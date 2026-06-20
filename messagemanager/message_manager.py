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
from redbot.core import Config, commands

log = logging.getLogger("red.cog.messagemanager")

BASE_URL = "https://www.missionchief.com"
MESSAGES_URL = f"{BASE_URL}/messages"
NEW_MESSAGE_URL = f"{BASE_URL}/messages/new"
SUCCESS_MARKER = "Message Sent."
MESSAGE_MANAGER_ROLE_ID = 544117282167586836
DEFAULT_PANEL_CHANNEL_ID = 1421242306136113254
DEFAULT_FORUM_CHANNEL_ID = 1517694938501087342
INBOX_SCAN_INTERVAL_SECONDS = 3600
MAX_THREAD_TITLE_LENGTH = 100


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


@dataclass
class InboxMessage:
    conversation_id: str
    sender: str
    subject: str
    url: str
    is_new: bool = False


@dataclass
class ConversationMessage:
    author: str
    body: str
    timestamp: str = ""


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


def parse_inbox_messages(html: str, page_url: str = MESSAGES_URL) -> List[InboxMessage]:
    """Parse regular inbox messages and ignore MissionChief system messages."""
    soup = BeautifulSoup(html or "", "html.parser")
    inbox_form = None
    for form in soup.find_all("form"):
        current_box = form.find("input", attrs={"name": "current_box"})
        if current_box and (current_box.get("value") or "").lower() == "inbox":
            inbox_form = form
            break
    if not inbox_form:
        return []

    messages: List[InboxMessage] = []
    for row in inbox_form.find_all("tr"):
        checkbox = row.find("input", attrs={"name": "conversations[]"})
        if not checkbox:
            continue

        conversation_id = str(checkbox.get("value") or "").strip()
        cells = row.find_all("td")
        if len(cells) < 4 or not conversation_id:
            continue

        status = _text(cells[1])
        sender_link = cells[2].find("a", href=True)
        subject_link = cells[3].find("a", href=True)
        if not subject_link:
            continue

        href = str(subject_link.get("href") or "")
        if "/messages/system_message/" in href:
            continue

        messages.append(
            InboxMessage(
                conversation_id=conversation_id,
                sender=_text(sender_link) if sender_link else _text(cells[2]),
                subject=_text(subject_link),
                url=urljoin(page_url, href),
                is_new=status.casefold() == "new",
            )
        )
    return messages


def build_reply_payload(html: str, body: str, page_url: str) -> Tuple[str, Payload]:
    """Build a reply payload for an existing MissionChief conversation page."""
    if not str(body or "").strip():
        raise ValueError("Reply body is required.")

    soup = BeautifulSoup(html or "", "html.parser")
    form = None
    for candidate in soup.find_all("form"):
        if candidate.find(attrs={"name": "message[conversation_id]"}) and candidate.find(
            attrs={"name": "message[body]"}
        ):
            form = candidate
            break
    if not form:
        raise ValueError("No reply form found on this MissionChief message page.")

    action = urljoin(page_url, form.get("action") or MESSAGES_URL)
    payload: Payload = []
    body_field_name = "message[body]"
    submit_name = None
    submit_value = None

    for input_el in form.find_all("input"):
        name = input_el.get("name")
        if not name:
            continue
        field_type = (input_el.get("type") or "text").lower()
        if field_type in {"button", "image", "reset"}:
            continue
        if field_type == "submit":
            submit_name = name
            submit_value = input_el.get("value") or ""
            continue
        payload.append((name, input_el.get("value") or ""))

    body_seen = False
    for textarea in form.find_all("textarea"):
        name = textarea.get("name")
        if not name:
            continue
        value = str(body or "") if name == body_field_name else textarea.get_text() or ""
        if name == body_field_name:
            body_seen = True
        payload.append((name, value))

    if not body_seen:
        raise ValueError("Could not identify the reply body field.")
    if submit_name:
        payload.append((submit_name, submit_value or ""))
    return action, payload


def extract_conversation_id(html: str, page_url: str = "") -> Optional[str]:
    """Extract a MissionChief conversation ID from a message page URL or HTML body."""
    url_match = re.search(r"/messages/(\d+)(?:\D|$)", str(page_url or ""))
    if url_match:
        return url_match.group(1)

    soup = BeautifulSoup(html or "", "html.parser")
    conversation_input = soup.find(attrs={"name": "message[conversation_id]"})
    if conversation_input:
        value = str(conversation_input.get("value") or "").strip()
        if value.isdigit():
            return value

    for link in soup.find_all("a", href=True):
        match = re.search(r"/messages/(\d+)(?:\D|$)", str(link.get("href") or ""))
        if match:
            return match.group(1)
    return None


def parse_conversation_messages(html: str) -> List[ConversationMessage]:
    """Parse visible messages from a MissionChief conversation page, newest first."""
    soup = BeautifulSoup(html or "", "html.parser")
    messages: List[ConversationMessage] = []
    for well in soup.find_all("div", class_=lambda value: value and "well" in str(value).split()):
        author_link = well.find("a", href=re.compile(r"/profile/\d+"))
        body = "\n".join(_text(paragraph) for paragraph in well.find_all("p") if _text(paragraph)).strip()
        if not author_link or not body:
            continue
        messages.append(
            ConversationMessage(
                author=_text(author_link),
                body=body,
                timestamp=str(well.get("data-message-time") or "").strip(),
            )
        )
    return messages


def build_forum_thread_title(username: str, subject: str) -> str:
    """Build `Username - Title name of DM` while respecting Discord forum title length."""
    title = f"{str(username or 'Unknown').strip()} - {str(subject or 'Untitled').strip()}"
    title = " ".join(title.split())
    if len(title) <= MAX_THREAD_TITLE_LENGTH:
        return title
    return title[: MAX_THREAD_TITLE_LENGTH - 1].rstrip() + "…"


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
            ok, reason, resolved_username, conversation_id = await self.manager._send_message(username, subject, body)
        except MemberResolutionError as exc:
            await interaction.followup.send(str(exc), ephemeral=True)
            return
        except Exception as exc:
            log.exception("MessageManager modal send failed")
            await interaction.followup.send(f"Could not send MissionChief message: {exc}", ephemeral=True)
            return

        if ok:
            await interaction.followup.send(
                f"MissionChief message sent to `{resolved_username}`."
                + (" A forum thread will be linked in the background." if conversation_id else ""),
                ephemeral=True,
            )
            if conversation_id:
                asyncio.create_task(
                    self.manager._link_sent_message_to_forum(
                        conversation_id=conversation_id,
                        username=resolved_username,
                        subject=subject,
                        body=body,
                    )
                )
        else:
            await interaction.followup.send(
                f"MissionChief message was not confirmed for `{resolved_username}`: {reason}",
                ephemeral=True,
            )


class MessageReplyModal(discord.ui.Modal, title="Reply to MissionChief Message"):
    conversation_id = discord.ui.TextInput(
        label="Conversation ID",
        placeholder="Example: 238264",
        max_length=30,
    )
    body = discord.ui.TextInput(
        label="Reply",
        placeholder="Reply body",
        style=discord.TextStyle.paragraph,
        max_length=1800,
    )

    def __init__(self, manager: "MessageManager"):
        super().__init__()
        self.manager = manager

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        conversation_id = str(self.conversation_id.value).strip()
        body = str(self.body.value).strip()
        try:
            ok, reason = await self.manager._send_reply(conversation_id, body)
        except Exception as exc:
            log.exception("MessageManager reply failed")
            await interaction.followup.send(f"Could not send MissionChief reply: {exc}", ephemeral=True)
            return

        if ok:
            await interaction.followup.send(f"MissionChief reply sent in conversation `{conversation_id}`.", ephemeral=True)
        else:
            await interaction.followup.send(
                f"MissionChief reply was not confirmed for conversation `{conversation_id}`: {reason}",
                ephemeral=True,
            )


class MessageManagerPanelView(discord.ui.View):
    """Persistent MessageManager panel."""

    def __init__(self, manager: "MessageManager"):
        super().__init__(timeout=None)
        self.manager = manager

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if await self.manager._interaction_can_manage_messages(interaction):
            return True
        await interaction.response.send_message(
            f"You need role `{MESSAGE_MANAGER_ROLE_ID}` to use MessageManager.",
            ephemeral=True,
        )
        return False

    @discord.ui.button(
        label="Send Message",
        style=discord.ButtonStyle.primary,
        custom_id="messagemanager:send_message",
    )
    async def send_message(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        del button
        await interaction.response.send_modal(MessageComposeModal(self.manager))

    @discord.ui.button(
        label="Check Inbox",
        style=discord.ButtonStyle.secondary,
        custom_id="messagemanager:check_inbox",
    )
    async def check_inbox(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        del button
        await interaction.response.send_message(
            "MissionChief inbox check started. I will post a private summary when it finishes.",
            ephemeral=True,
        )
        asyncio.create_task(self.manager._scan_inbox_and_report(interaction))

    @discord.ui.button(
        label="Reply",
        style=discord.ButtonStyle.success,
        custom_id="messagemanager:reply",
    )
    async def reply(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        del button
        await interaction.response.send_modal(MessageReplyModal(self.manager))


class MessageManager(commands.Cog):
    """Admin-only MissionChief direct message manager."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA12A6E5, force_registration=True)
        self.config.register_global(
            panel_channel_id=DEFAULT_PANEL_CHANNEL_ID,
            panel_message_id=None,
            forum_channel_id=DEFAULT_FORUM_CHANNEL_ID,
            conversation_threads={},
            inbox_scan_enabled=True,
        )
        self._panel_task: Optional[asyncio.Task] = None
        self._inbox_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        add_view = getattr(self.bot, "add_view", None)
        if add_view:
            add_view(MessageManagerPanelView(self))
        self._panel_task = asyncio.create_task(self._delayed_panel_start())
        self._inbox_task = asyncio.create_task(self._inbox_scan_loop())

    async def cog_unload(self):
        if self._panel_task:
            self._panel_task.cancel()
            try:
                await self._panel_task
            except asyncio.CancelledError:
                pass
        if self._inbox_task:
            self._inbox_task.cancel()
            try:
                await self._inbox_task
            except asyncio.CancelledError:
                pass

    async def _delayed_panel_start(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)
        await self._ensure_panel_message()

    async def _inbox_scan_loop(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(60)
        while True:
            try:
                if await self.config.inbox_scan_enabled():
                    await self._scan_new_inbox_messages()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("MessageManager hourly inbox scan failed: %s", exc)
            await asyncio.sleep(INBOX_SCAN_INTERVAL_SECONDS)

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

    def _build_panel_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="MessageManager",
            description=(
                "Send MissionChief messages to current alliance members and route conversations through "
                f"forum channel `{DEFAULT_FORUM_CHANNEL_ID}`.\n\n"
                "The inbox is checked automatically every hour. System messages are ignored."
            ),
            color=discord.Color.blue(),
        )
        embed.add_field(
            name="Available actions",
            value="Send Message\nCheck Inbox\nReply by conversation ID",
            inline=False,
        )
        embed.set_footer(text=f"Required role: {MESSAGE_MANAGER_ROLE_ID}")
        return embed

    async def _send_panel_message(self, channel: discord.TextChannel) -> discord.Message:
        message = await channel.send(embed=self._build_panel_embed(), view=MessageManagerPanelView(self))
        await self.config.panel_message_id.set(message.id)
        return message

    async def _ensure_panel_message(self) -> None:
        channel_id = await self.config.panel_channel_id()
        if not channel_id:
            return

        channel = self.bot.get_channel(int(channel_id))
        if not channel:
            log.warning("MessageManager panel channel not found: %s", channel_id)
            return

        message_id = await self.config.panel_message_id()
        if message_id:
            try:
                await channel.fetch_message(int(message_id))
                return
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                log.info("Stored MessageManager panel message is unavailable; reposting")

        try:
            await self._send_panel_message(channel)
            log.info("MessageManager panel posted in channel %s", channel_id)
        except Exception as exc:
            log.exception("Failed to post MessageManager panel: %s", exc)

    async def _get_forum_channel(self) -> Optional[discord.ForumChannel]:
        channel_id = await self.config.forum_channel_id()
        if not channel_id:
            return None
        channel = self.bot.get_channel(int(channel_id))
        if not channel:
            try:
                channel = await self.bot.fetch_channel(int(channel_id))
            except Exception:
                log.exception("MessageManager forum channel not found: %s", channel_id)
                return None
        if not isinstance(channel, discord.ForumChannel):
            log.warning("MessageManager forum channel %s is not a Discord forum channel", channel_id)
            return None
        return channel

    async def _get_thread_for_conversation(self, conversation_id: str) -> Optional[discord.Thread]:
        mapping = await self.config.conversation_threads()
        entry = mapping.get(str(conversation_id)) or {}
        thread_id = entry.get("thread_id")
        if not thread_id:
            return None

        thread = self.bot.get_channel(int(thread_id))
        if isinstance(thread, discord.Thread):
            return thread
        for guild in getattr(self.bot, "guilds", []):
            guild_thread = guild.get_thread(int(thread_id))
            if isinstance(guild_thread, discord.Thread):
                return guild_thread
        try:
            fetched = await self.bot.fetch_channel(int(thread_id))
        except Exception:
            log.debug("Could not fetch MessageManager forum thread %s", thread_id, exc_info=True)
            return None
        return fetched if isinstance(fetched, discord.Thread) else None

    async def _save_conversation_thread(
        self,
        conversation_id: str,
        *,
        thread_id: int,
        username: str,
        subject: str,
        last_message_time: str = "",
    ) -> None:
        async with self.config.conversation_threads() as mapping:
            current = dict(mapping.get(str(conversation_id)) or {})
            current.update(
                {
                    "thread_id": int(thread_id),
                    "username": str(username or ""),
                    "subject": str(subject or ""),
                }
            )
            if last_message_time:
                current["last_message_time"] = str(last_message_time)
            mapping[str(conversation_id)] = current

    async def _ensure_conversation_thread(
        self,
        *,
        conversation_id: str,
        username: str,
        subject: str,
        preview: str = "",
        last_message_time: str = "",
    ) -> Optional[discord.Thread]:
        conversation_id = str(conversation_id or "").strip()
        if not conversation_id:
            return None

        mapping = await self.config.conversation_threads()
        mapped_entry = mapping.get(str(conversation_id)) or {}
        mapped_thread_id = mapped_entry.get("thread_id")
        existing = await self._get_thread_for_conversation(conversation_id)
        if existing:
            if last_message_time:
                await self._save_conversation_thread(
                    conversation_id,
                    thread_id=existing.id,
                    username=username,
                    subject=subject,
                    last_message_time=last_message_time,
                )
            return existing

        if mapped_thread_id:
            log.warning(
                "MessageManager conversation %s is already mapped to forum thread %s, "
                "but the thread could not be fetched. Skipping instead of creating a duplicate.",
                conversation_id,
                mapped_thread_id,
            )
            return None

        forum = await self._get_forum_channel()
        if not forum:
            return None

        title = build_forum_thread_title(username, subject)
        content = self._build_forum_thread_opening(
            conversation_id=conversation_id,
            username=username,
            subject=subject,
            preview=preview,
        )
        created = await forum.create_thread(name=title, content=content)
        thread = getattr(created, "thread", created)
        if isinstance(created, tuple):
            thread = created[0]
        if not isinstance(thread, discord.Thread):
            log.warning("MessageManager forum thread create returned unexpected result: %r", created)
            return None

        await self._save_conversation_thread(
            conversation_id,
            thread_id=thread.id,
            username=username,
            subject=subject,
            last_message_time=last_message_time,
        )
        return thread

    def _build_forum_thread_opening(
        self,
        *,
        conversation_id: str,
        username: str,
        subject: str,
        preview: str = "",
    ) -> str:
        lines = [
            f"MissionChief conversation: `{conversation_id}`",
            f"Member: **{discord.utils.escape_markdown(str(username or 'Unknown'))}**",
            f"Title: **{discord.utils.escape_markdown(str(subject or 'Untitled'))}**",
            f"Link: {MESSAGES_URL}/{conversation_id}",
        ]
        if preview:
            lines.extend(["", "Latest message:", discord.utils.escape_markdown(str(preview))[:1200]])
        return "\n".join(lines)[:1900]

    async def _post_inbound_to_forum(
        self,
        *,
        conversation_id: str,
        username: str,
        subject: str,
        body: str,
        timestamp: str = "",
    ) -> Optional[discord.Thread]:
        mapping = await self.config.conversation_threads()
        existing_entry = mapping.get(str(conversation_id)) or {}
        existing_thread_id = existing_entry.get("thread_id")
        previous_time = existing_entry.get("last_message_time")

        thread = await self._ensure_conversation_thread(
            conversation_id=conversation_id,
            username=username,
            subject=subject,
            preview=body,
            last_message_time="" if existing_thread_id else timestamp,
        )
        if not thread:
            return None

        if existing_thread_id and previous_time == timestamp and timestamp:
            return thread
        if not existing_thread_id:
            return thread

        content = (
            f"New MissionChief reply from **{discord.utils.escape_markdown(str(username or 'Unknown'))}**"
            + (f" (`{timestamp}`)" if timestamp else "")
            + ":\n"
            + discord.utils.escape_markdown(str(body or ""))[:1600]
        )
        await thread.send(content[:1900])
        await self._save_conversation_thread(
            conversation_id,
            thread_id=thread.id,
            username=username,
            subject=subject,
            last_message_time=timestamp,
        )
        return thread

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

    async def _fetch_new_inbox_messages(self) -> List[InboxMessage]:
        session = await self._get_session()
        async with session.get(MESSAGES_URL, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            raise RuntimeError(f"MissionChief returned HTTP {status}.")
        return [message for message in parse_inbox_messages(html, MESSAGES_URL) if message.is_new]

    async def _fetch_conversation_messages(self, conversation_id: str) -> List[ConversationMessage]:
        conversation_id = str(conversation_id or "").strip()
        if not conversation_id.isdigit():
            raise ValueError("Conversation ID must be numeric.")

        session = await self._get_session()
        url = f"{MESSAGES_URL}/{conversation_id}"
        async with session.get(url, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            raise RuntimeError(f"MissionChief returned HTTP {status} while opening the conversation.")
        return parse_conversation_messages(html)

    async def _scan_new_inbox_messages(self) -> Tuple[int, int, int]:
        created = 0
        updated = 0
        skipped = 0
        messages = await self._fetch_new_inbox_messages()
        mapping = await self.config.conversation_threads()
        seen_conversation_ids = set()

        for inbox_message in messages:
            if inbox_message.conversation_id in seen_conversation_ids:
                skipped += 1
                continue
            seen_conversation_ids.add(inbox_message.conversation_id)

            entry = mapping.get(str(inbox_message.conversation_id)) or {}
            latest_messages = await self._fetch_conversation_messages(inbox_message.conversation_id)
            latest = latest_messages[0] if latest_messages else None
            body = latest.body if latest else ""
            timestamp = latest.timestamp if latest else ""

            if entry.get("thread_id") and timestamp and entry.get("last_message_time") == timestamp:
                skipped += 1
                continue
            if entry.get("thread_id") and not timestamp:
                skipped += 1
                continue

            thread = await self._post_inbound_to_forum(
                conversation_id=inbox_message.conversation_id,
                username=inbox_message.sender,
                subject=inbox_message.subject,
                body=body or inbox_message.subject,
                timestamp=timestamp,
            )
            if not thread:
                skipped += 1
                continue

            if entry.get("thread_id"):
                updated += 1
            else:
                created += 1
            await asyncio.sleep(1)

        return created, updated, skipped

    async def _scan_inbox_and_report(self, interaction: discord.Interaction) -> None:
        try:
            created, updated, skipped = await asyncio.wait_for(
                self._scan_new_inbox_messages(),
                timeout=180,
            )
            message = (
                "MissionChief inbox checked.\n"
                f"New forum threads: `{created}`\n"
                f"Updated threads: `{updated}`\n"
                f"Already handled: `{skipped}`"
            )
        except Exception as exc:
            log.exception("MessageManager inbox check failed")
            message = f"Could not check MissionChief inbox: {exc}"

        try:
            await interaction.followup.send(message, ephemeral=True)
        except Exception:
            log.debug("Could not send MessageManager inbox scan follow-up", exc_info=True)

    async def _link_sent_message_to_forum(
        self,
        *,
        conversation_id: str,
        username: str,
        subject: str,
        body: str,
    ) -> None:
        try:
            await asyncio.wait_for(
                self._ensure_conversation_thread(
                    conversation_id=conversation_id,
                    username=username,
                    subject=subject,
                    preview=body,
                ),
                timeout=60,
            )
        except Exception:
            log.exception("Failed to create MessageManager forum thread for sent message")

    async def _send_message(self, username: str, subject: str, body: str) -> Tuple[bool, str, str, Optional[str]]:
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
            response_url = str(getattr(response, "url", "") or "")
            response_text = await response.text()

        if message_was_sent(response_text):
            conversation_id = extract_conversation_id(response_text, response_url)
            return True, "Message Sent.", resolved_username, conversation_id
        response_summary = summarize_response(response_text)
        log.warning("MessageManager send failed with HTTP %s. Response: %s", status, response_summary)
        return (
            False,
            f"MissionChief did not confirm delivery. HTTP {status}. Response: {response_summary or 'empty'}",
            resolved_username,
            None,
        )

    async def _send_reply(self, conversation_id: str, body: str) -> Tuple[bool, str]:
        conversation_id = str(conversation_id or "").strip()
        if not conversation_id.isdigit():
            raise ValueError("Conversation ID must be numeric.")
        if not body:
            raise ValueError("Reply body is required.")

        url = f"{MESSAGES_URL}/{conversation_id}"
        session = await self._get_session()
        async with session.get(url, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            raise RuntimeError(f"MissionChief returned HTTP {status} while opening the conversation.")

        action, payload = build_reply_payload(html, body, url)
        headers = {
            "Origin": BASE_URL,
            "Referer": url,
        }
        async with session.post(action, data=payload, allow_redirects=True, headers=headers) as response:
            status = getattr(response, "status", None)
            response_text = await response.text()

        if message_was_sent(response_text):
            return True, "Message Sent."
        response_summary = summarize_response(response_text)
        log.warning("MessageManager reply failed with HTTP %s. Response: %s", status, response_summary)
        return False, f"MissionChief did not confirm delivery. HTTP {status}. Response: {response_summary or 'empty'}"

    async def _conversation_id_for_thread(self, thread_id: int) -> Optional[str]:
        mapping = await self.config.conversation_threads()
        for conversation_id, entry in mapping.items():
            if str(entry.get("thread_id")) == str(thread_id):
                return str(conversation_id)
        return None

    def _build_reply_body_from_discord_message(self, message: discord.Message) -> str:
        parts = []
        content = str(getattr(message, "content", "") or "").strip()
        if content:
            parts.append(content)
        attachments = getattr(message, "attachments", []) or []
        for attachment in attachments:
            url = getattr(attachment, "url", "")
            if url:
                parts.append(str(url))
        return "\n".join(parts).strip()

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Forward staff replies in linked forum threads back to MissionChief."""
        if getattr(message.author, "bot", False):
            return
        channel = getattr(message, "channel", None)
        if not isinstance(channel, discord.Thread):
            return
        parent_id = getattr(channel, "parent_id", None)
        forum_channel_id = await self.config.forum_channel_id()
        if str(parent_id) != str(forum_channel_id):
            return
        if not await self._member_can_manage_messages(message.author):
            return

        conversation_id = await self._conversation_id_for_thread(channel.id)
        if not conversation_id:
            return

        body = self._build_reply_body_from_discord_message(message)
        if not body:
            return

        try:
            ok, reason = await self._send_reply(conversation_id, body)
        except Exception as exc:
            log.exception("Failed to forward Discord forum reply to MissionChief")
            try:
                await message.reply(f"Could not send MissionChief reply: {exc}", mention_author=False)
            except Exception:
                log.debug("Could not notify MessageManager forum reply failure", exc_info=True)
            return

        try:
            if ok:
                await message.add_reaction("✅")
            else:
                await message.add_reaction("⚠️")
                await message.reply(
                    f"MissionChief did not confirm this reply: {reason}",
                    mention_author=False,
                )
        except Exception:
            log.debug("Could not add MessageManager forum reply feedback", exc_info=True)

    @commands.command(name="mm")
    async def message_manager_shortcut(self, ctx: commands.Context):
        """Ensure the MessageManager panel exists."""
        if not ctx.guild:
            await ctx.send("Use this command in the server so your MessageManager role can be checked.")
            return

        if not await self._member_can_manage_messages(ctx.author):
            await ctx.send(f"You need role `{MESSAGE_MANAGER_ROLE_ID}` to use MessageManager.")
            return

        await self._ensure_panel_message()
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
                ok, reason, resolved_username, conversation_id = await self._send_message(username, subject, body)
            except Exception as exc:
                await ctx.send(f"Could not send MissionChief message: {exc}")
                return

        if ok:
            suffix = f" Conversation `{conversation_id}` linked to forum." if conversation_id else ""
            await ctx.send(f"MissionChief message sent to `{resolved_username}`.{suffix}")
        else:
            await ctx.send(f"MissionChief message was not confirmed for `{resolved_username}`: {reason}")
