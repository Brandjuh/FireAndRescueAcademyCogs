from __future__ import annotations

import asyncio
import io
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import discord
from redbot.core import commands

log = logging.getLogger("red.cog.messagemanager")

BASE_URL = "https://www.missionchief.com"
MESSAGES_URL = f"{BASE_URL}/messages"
NEW_MESSAGE_URL = f"{BASE_URL}/messages/new"
SUCCESS_MARKER = "Message Sent."
MESSAGE_MANAGER_ROLE_ID = 544117282167586836


@dataclass
class MessageField:
    name: str
    tag: str
    field_type: str = ""
    value: str = ""
    required: bool = False


@dataclass
class MessageForm:
    action: str
    method: str
    fields: List[MessageField] = field(default_factory=list)
    recipient_field: Optional[str] = None
    subject_field: Optional[str] = None
    body_field: Optional[str] = None
    submit_name: Optional[str] = None
    submit_value: Optional[str] = None


Payload = List[Tuple[str, str]]


class MemberResolutionError(ValueError):
    """Raised when an alliance member name cannot be resolved safely."""


def _text(element) -> str:
    return " ".join(element.get_text(" ", strip=True).split()) if element else ""


def _label_for_field(soup: BeautifulSoup, field) -> str:
    field_id = field.get("id")
    if field_id:
        label = soup.find("label", attrs={"for": field_id})
        if label:
            return _text(label)
    parent = field.find_parent("label")
    if parent:
        return _text(parent)
    return ""


def _field_identity(field: MessageField, labels: Dict[str, str]) -> str:
    label = labels.get(field.name, "")
    return " ".join([field.name, field.field_type, label]).lower()


def _looks_like_recipient(field: MessageField, labels: Dict[str, str]) -> bool:
    identity = _field_identity(field, labels)
    return any(token in identity for token in ("recipient", "receiver", "username", "user name", "to]"))


def _looks_like_subject(field: MessageField, labels: Dict[str, str]) -> bool:
    return "subject" in _field_identity(field, labels)


def _looks_like_body(field: MessageField, labels: Dict[str, str]) -> bool:
    identity = _field_identity(field, labels)
    return any(token in identity for token in ("body", "content", "message", "text"))


def parse_message_form(html: str, page_url: str = NEW_MESSAGE_URL) -> MessageForm:
    """Parse the MissionChief new-message form without assuming exact field names."""
    soup = BeautifulSoup(html, "html.parser")
    forms = soup.find_all("form")
    if not forms:
        raise ValueError("No form found on the MissionChief messages page.")

    form = None
    for candidate in forms:
        action = str(candidate.get("action") or "")
        if "message" in action.lower():
            form = candidate
            break
    form = form or forms[0]

    action = urljoin(page_url, form.get("action") or MESSAGES_URL)
    method = (form.get("method") or "get").lower()
    fields: List[MessageField] = []
    labels: Dict[str, str] = {}
    submit_name = None
    submit_value = None

    for input_el in form.find_all("input"):
        name = input_el.get("name")
        field_type = (input_el.get("type") or "text").lower()
        if field_type in {"button", "image", "reset"}:
            continue
        if field_type == "submit":
            if name and submit_name is None:
                submit_name = name
                submit_value = input_el.get("value") or ""
            continue
        if not name:
            continue
        field = MessageField(
            name=name,
            tag="input",
            field_type=field_type,
            value=input_el.get("value") or "",
            required=input_el.has_attr("required"),
        )
        fields.append(field)
        labels[name] = _label_for_field(soup, input_el)

    for textarea in form.find_all("textarea"):
        name = textarea.get("name")
        if not name:
            continue
        field = MessageField(
            name=name,
            tag="textarea",
            value=textarea.get_text() or "",
            required=textarea.has_attr("required"),
        )
        fields.append(field)
        labels[name] = _label_for_field(soup, textarea)

    for select in form.find_all("select"):
        name = select.get("name")
        if not name:
            continue
        selected = select.find("option", selected=True) or select.find("option")
        field = MessageField(
            name=name,
            tag="select",
            value=selected.get("value") if selected else "",
            required=select.has_attr("required"),
        )
        fields.append(field)
        labels[name] = _label_for_field(soup, select)

    recipient_field = next((field.name for field in fields if _looks_like_recipient(field, labels)), None)
    subject_field = next((field.name for field in fields if _looks_like_subject(field, labels)), None)
    body_field = next((field.name for field in fields if field.tag == "textarea" and _looks_like_body(field, labels)), None)
    body_field = body_field or next((field.name for field in fields if field.tag == "textarea"), None)

    return MessageForm(
        action=action,
        method=method,
        fields=fields,
        recipient_field=recipient_field,
        subject_field=subject_field,
        body_field=body_field,
        submit_name=submit_name,
        submit_value=submit_value,
    )


def build_message_payload(form: MessageForm, username: str, subject: str, body: str) -> Payload:
    """Build the MissionChief message POST payload."""
    missing = []
    if not form.recipient_field:
        missing.append("recipient/username")
    if not form.subject_field:
        missing.append("subject")
    if not form.body_field:
        missing.append("body")
    if missing:
        raise ValueError(f"Could not identify required message fields: {', '.join(missing)}.")

    replacements = {
        form.recipient_field: username,
        form.subject_field: subject,
        form.body_field: body,
    }
    payload: Payload = []
    used_names = set()
    for field_info in form.fields:
        value = replacements.get(field_info.name, field_info.value)
        payload.append((field_info.name, str(value or "")))
        used_names.add(field_info.name)
        if _visible_text_field_is_empty(field_info, value):
            raise ValueError(f"Visible message field `{field_info.name}` is empty.")

    for field_name, value in replacements.items():
        if field_name not in used_names:
            payload.append((field_name, value))

    if form.submit_name:
        payload.append((form.submit_name, form.submit_value or ""))
    return payload


def _visible_text_field_is_empty(field_info: MessageField, value: str) -> bool:
    if field_info.tag == "textarea":
        return not str(value or "").strip()
    if field_info.tag != "input":
        return False
    if field_info.field_type in {"hidden", "submit", "button", "checkbox", "radio"}:
        return False
    return not str(value or "").strip()


def parse_send_spec(spec: str) -> Tuple[str, str, str]:
    """Parse `<username> | <subject> | <body>` without changing username casing."""
    parts = [part.strip() for part in str(spec or "").split("|", 2)]
    if len(parts) != 3 or not all(parts):
        raise ValueError("Use `<username> | <subject> | <body>`.")
    return parts[0], parts[1], parts[2]


def _normalized_member_query(value: str) -> str:
    return " ".join(str(value or "").split()).casefold()


def resolve_alliance_member_name(query: str, members: Iterable[Dict[str, object]]) -> str:
    """Resolve a typed MissionChief member query to the exact alliance username casing."""
    normalized_query = _normalized_member_query(query)
    if not normalized_query:
        raise MemberResolutionError("MissionChief username is required.")

    matches = []
    for member in members:
        name = str(member.get("name") or member.get("username") or "").strip()
        if not name:
            continue

        member_ids = {
            str(member.get("user_id") or "").strip(),
            str(member.get("mc_user_id") or "").strip(),
            str(member.get("member_id") or "").strip(),
            str(member.get("mc_id") or "").strip(),
        }
        if _normalized_member_query(name) == normalized_query or normalized_query in member_ids:
            matches.append(name)

    unique_matches = sorted(set(matches), key=str.casefold)
    if not unique_matches:
        raise MemberResolutionError(f"No current alliance member found for `{query}`.")
    if len(unique_matches) > 1:
        raise MemberResolutionError(
            f"`{query}` matched multiple alliance members: {', '.join(unique_matches[:5])}."
        )
    return unique_matches[0]


def _redact_value(name: str, value: str) -> str:
    lowered = str(name or "").lower()
    if "token" in lowered or "cookie" in lowered or lowered == "authorization":
        return "REDACTED"
    return str(value or "")


def safe_payload_summary(payload: Payload) -> str:
    if not payload:
        return "none"
    return "\n".join(f"{name}={_redact_value(name, value)}" for name, value in payload)


def summarize_message_form(form: MessageForm) -> str:
    lines = [
        f"Action: {form.action}",
        f"Method: {form.method.upper()}",
        f"Fields: {len(form.fields)}",
        f"Recipient field: {form.recipient_field or 'NOT FOUND'}",
        f"Subject field: {form.subject_field or 'NOT FOUND'}",
        f"Body field: {form.body_field or 'NOT FOUND'}",
    ]
    if form.submit_name:
        lines.append(f"Submit: {form.submit_name}={form.submit_value or ''}")
    for field_info in form.fields:
        required = " required" if field_info.required else ""
        field_type = f":{field_info.field_type}" if field_info.field_type else ""
        value = _redact_value(field_info.name, field_info.value)
        suffix = f" = {value}" if value else ""
        lines.append(f"- {field_info.name} ({field_info.tag}{field_type}{required}){suffix}")
    return "\n".join(lines)


def message_was_sent(html: str) -> bool:
    soup = BeautifulSoup(html or "", "html.parser")
    text = soup.get_text(" ", strip=True)
    return SUCCESS_MARKER.lower() in text.lower()


def summarize_response(text: str, *, limit: int = 350) -> str:
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", str(text or ""), flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"authenticity_token[^\s&<>\"]+", "authenticity_token=REDACTED", text, flags=re.IGNORECASE)
    text = " ".join(text.split())
    return text[:limit]


class MessageComposeModal(discord.ui.Modal, title="MissionChief Message"):
    username = discord.ui.TextInput(
        label="MissionChief username",
        placeholder="Case does not matter, but the member must be in the alliance",
        max_length=100,
    )
    subject = discord.ui.TextInput(
        label="Title",
        placeholder="Message title",
        max_length=120,
    )
    body = discord.ui.TextInput(
        label="Body",
        placeholder="Message body",
        style=discord.TextStyle.paragraph,
        max_length=1800,
    )

    def __init__(self, manager: "MessageManager"):
        super().__init__()
        self.manager = manager

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        username = str(self.username.value).strip()
        subject = str(self.subject.value).strip()
        body = str(self.body.value).strip()
        try:
            ok, reason, resolved_username = await self.manager._send_message(username, subject, body)
        except MemberResolutionError as exc:
            await interaction.followup.send(str(exc), ephemeral=True)
            return
        except Exception as exc:
            log.exception("MessageManager modal send failed")
            await interaction.followup.send(f"Could not send MissionChief message: {exc}", ephemeral=True)
            return

        if ok:
            await interaction.followup.send(f"MissionChief message sent to `{resolved_username}`.", ephemeral=True)
        else:
            await interaction.followup.send(
                f"MissionChief message was not confirmed for `{resolved_username}`: {reason}",
                ephemeral=True,
            )


class MessageManagerLaunchView(discord.ui.View):
    """Short-lived launcher used because text commands cannot open Discord modals directly."""

    def __init__(self, manager: "MessageManager", allowed_user_id: Optional[int] = None):
        super().__init__(timeout=300)
        self.manager = manager
        self.allowed_user_id = allowed_user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self.allowed_user_id is not None:
            if interaction.user.id == self.allowed_user_id:
                return True
            await interaction.response.send_message(
                "This MessageManager launcher is not for you.",
                ephemeral=True,
            )
            return False

        if await self.manager._interaction_can_manage_messages(interaction):
            return True
        await interaction.response.send_message(
            f"You need role `{MESSAGE_MANAGER_ROLE_ID}` to use MessageManager.",
            ephemeral=True,
        )
        return False

    @discord.ui.button(label="Open MessageManager", style=discord.ButtonStyle.primary)
    async def open_message_form(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        del button
        await interaction.response.send_modal(MessageComposeModal(self.manager))


class MessageManager(commands.Cog):
    """Admin-only MissionChief direct message manager."""

    def __init__(self, bot):
        self.bot = bot

    def _cookie_manager(self):
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager or not hasattr(cookie_manager, "get_session"):
            return None
        return cookie_manager

    async def _member_can_manage_messages(self, user) -> bool:
        roles = getattr(user, "roles", None)
        if not roles:
            return False
        return any(getattr(role, "id", None) == MESSAGE_MANAGER_ROLE_ID for role in roles)

    async def _interaction_can_manage_messages(self, interaction: discord.Interaction) -> bool:
        try:
            return await self._member_can_manage_messages(interaction.user)
        except Exception:
            log.exception("Failed to check MessageManager interaction permissions")
            return False

    async def _get_alliance_members(self) -> List[Dict[str, object]]:
        members_scraper = self.bot.get_cog("MembersScraper")
        if not members_scraper or not hasattr(members_scraper, "get_members"):
            raise MemberResolutionError("MembersScraper is not loaded, so alliance usernames cannot be verified.")
        members = await members_scraper.get_members()
        if not members:
            raise MemberResolutionError("No current alliance members are available from MembersScraper.")
        return members

    async def _resolve_alliance_username(self, username: str) -> str:
        members = await self._get_alliance_members()
        return await asyncio.to_thread(resolve_alliance_member_name, username, members)

    async def _get_session(self):
        cookie_manager = self._cookie_manager()
        if not cookie_manager:
            raise RuntimeError("CookieManager is not loaded.")
        session = await cookie_manager.get_session()
        if not session:
            raise RuntimeError("CookieManager did not return a session.")
        return session

    async def _fetch_form(self) -> MessageForm:
        session = await self._get_session()
        async with session.get(NEW_MESSAGE_URL, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            raise RuntimeError(f"MissionChief returned HTTP {status}.")
        return parse_message_form(html, NEW_MESSAGE_URL)

    async def _send_message(self, username: str, subject: str, body: str) -> Tuple[bool, str, str]:
        if not username or not subject or not body:
            raise ValueError("Username, subject, and body are required.")

        resolved_username = await self._resolve_alliance_username(username)
        form = await self._fetch_form()
        if form.method != "post":
            raise RuntimeError(f"Unexpected message form method `{form.method}`.")
        payload = build_message_payload(form, resolved_username, subject, body)
        session = await self._get_session()
        headers = {
            "Origin": BASE_URL,
            "Referer": NEW_MESSAGE_URL,
        }
        async with session.post(form.action, data=payload, allow_redirects=True, headers=headers) as response:
            status = getattr(response, "status", None)
            response_text = await response.text()

        if message_was_sent(response_text):
            return True, "Message Sent.", resolved_username
        response_summary = summarize_response(response_text)
        log.warning("MessageManager send failed with HTTP %s. Response: %s", status, response_summary)
        return (
            False,
            f"MissionChief did not confirm delivery. HTTP {status}. Response: {response_summary or 'empty'}",
            resolved_username,
        )

    @commands.command(name="mm")
    async def message_manager_shortcut(self, ctx: commands.Context):
        """Send a private button launcher for the MissionChief message form."""
        if not ctx.guild:
            await ctx.send("Use this command in the server so your MessageManager role can be checked.")
            return

        if not await self._member_can_manage_messages(ctx.author):
            await ctx.send(f"You need role `{MESSAGE_MANAGER_ROLE_ID}` to use MessageManager.")
            return

        embed = discord.Embed(
            title="MessageManager",
            description=(
                "Open a private form to send a MissionChief message to a current alliance member.\n"
                "Usernames are checked against the alliance member list, so capitalization does not matter."
            ),
            color=discord.Color.blue(),
        )
        embed.set_footer(text="Only you can use this launcher.")
        try:
            await ctx.author.send(embed=embed, view=MessageManagerLaunchView(self, allowed_user_id=ctx.author.id))
        except discord.Forbidden:
            await ctx.send(
                "I could not send you a private MessageManager launcher. Enable DMs from this server and try again.",
                delete_after=20,
            )
            return

        try:
            await ctx.message.delete()
        except Exception:
            log.debug("Could not delete MessageManager shortcut command message", exc_info=True)

    @commands.group(name="messagemanager", aliases=["messages"], invoke_without_command=True)
    @commands.admin()
    async def messagemanager(self, ctx: commands.Context):
        """Manage MissionChief direct messages."""
        await ctx.send_help()

    @messagemanager.command(name="inspect")
    @commands.admin()
    async def inspect_form(self, ctx: commands.Context):
        """Inspect the live MissionChief new-message form."""
        try:
            form = await self._fetch_form()
        except Exception as exc:
            await ctx.send(f"Could not inspect message form: {exc}")
            return
        data = io.BytesIO(summarize_message_form(form).encode("utf-8"))
        await ctx.send(
            "MissionChief message form inspection:",
            file=discord.File(data, filename="messagemanager-form.txt"),
        )

    @messagemanager.command(name="payload")
    @commands.admin()
    async def debug_payload(self, ctx: commands.Context, *, spec: str):
        """Build a safe no-submit payload for `<username> | <subject> | <body>`."""
        try:
            username, subject, body = parse_send_spec(spec)
            username = await self._resolve_alliance_username(username)
            form = await self._fetch_form()
            payload = build_message_payload(form, username, subject, body)
        except Exception as exc:
            await ctx.send(f"Could not build message payload: {exc}")
            return
        data = io.BytesIO(safe_payload_summary(payload).encode("utf-8"))
        await ctx.send(
            "Safe MessageManager payload generated. No message was sent.",
            file=discord.File(data, filename="messagemanager-payload.txt"),
        )

    @messagemanager.command(name="send")
    @commands.admin()
    async def send_message(self, ctx: commands.Context, *, spec: str):
        """Send a MissionChief DM: `<username> | <subject> | <body>`."""
        try:
            username, subject, body = parse_send_spec(spec)
        except ValueError as exc:
            await ctx.send(str(exc))
            return

        async with ctx.typing():
            try:
                ok, reason, resolved_username = await self._send_message(username, subject, body)
            except Exception as exc:
                await ctx.send(f"Could not send MissionChief message: {exc}")
                return

        if ok:
            await ctx.send(f"MissionChief message sent to `{resolved_username}`.")
        else:
            await ctx.send(f"MissionChief message was not confirmed for `{resolved_username}`: {reason}")
