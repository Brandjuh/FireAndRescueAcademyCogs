from __future__ import annotations

import asyncio
import io
import logging
import random
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
INBOX_SCAN_INTERVAL_SECONDS = 15 * 60
INBOX_SCAN_JITTER_SECONDS = 5 * 60
MAX_THREAD_TITLE_LENGTH = 100
MAX_DISCORD_CONTENT_LENGTH = 1900
TAX_WARNING_SCAN_INTERVAL_SECONDS = 6 * 3600
TAX_WARNING_SCAN_JITTER_SECONDS = 10 * 60
TAX_WARNING_MIN_RATE = 5.0
TAX_WARNING_MIN_DAYS_BETWEEN = 7
TAX_WARNING_SEND_DELAY_SECONDS = 90
TAX_WARNING_MAX_PER_RUN = 5
TAX_WARNING_REASON_CATEGORY = "Contribution"
TAX_WARNING_REASON_DETAIL = "4.1. 5% donation to alliance - Minimum 5% donation required."
SANCTION_MANAGER_COG_NAMES = ("SanctionsManager", "SanctionManager")
TAX_WARNING_SANCTION_TYPES = {
    1: "Warning - Official 1st warning",
    2: "Warning - Official 2nd warning",
    3: "Warning - Official 3rd and last warning",
}
TAX_WARNING_PRESETS = {
    1: (
        "Reminder: Please set your alliance donation to 5%",
        "Hello {username},\n\n"
        "This is a friendly reminder that your alliance donation is currently not set to the required minimum of 5%.\n\n"
        "According to our Code of Conduct, rule 4.1, every member must set their alliance donation to at least 5%. "
        "These funds are used to build hospitals, prisons, and academies that benefit all alliance members.\n\n"
        "It is possible that you simply forgot to set this or were not sure where to find it. No problem, but please "
        "update it as soon as possible.\n\n"
        "How to update your alliance donation:\n\n"
        "1. Open the menu.\n"
        "2. Click on Show Alliance.\n"
        "3. Go to Alliance Funds.\n"
        "4. Set your donation percentage to at least 5%.\n\n"
        "A higher percentage is always appreciated, but 5% is the minimum requirement.\n\n"
        "Thank you for taking care of this.",
    ),
    2: (
        "Warning: Alliance donation below required minimum",
        "Hello {username},\n\n"
        "This is an official warning regarding your alliance donation.\n\n"
        "Your alliance donation is still not set to the required minimum of 5%, even though this is mandatory under "
        "our Code of Conduct, rule 4.1.\n\n"
        "All members are required to contribute at least 5% to the alliance. These contributions are important because "
        "they allow the alliance to build hospitals, prisons, and academies that support every member.\n\n"
        "Please update your alliance donation to at least 5% as soon as possible.\n\n"
        "How to update your alliance donation:\n\n"
        "1. Open the menu.\n"
        "2. Click on Show Alliance.\n"
        "3. Go to Alliance Funds.\n"
        "4. Set your donation percentage to at least 5%.\n\n"
        "Failure to correct this may result in further action.\n\n"
        "Please make sure this is fixed.",
    ),
    3: (
        "Final warning: Alliance donation requirement not met",
        "Hello {username},\n\n"
        "This is a final warning regarding your alliance donation.\n\n"
        "Your alliance donation is still not set to the required minimum of 5%, despite previous reminders and "
        "warnings. This is a direct violation of our Code of Conduct, rule 4.1.\n\n"
        "All members are required to set their alliance donation to at least 5%. This rule exists to make sure everyone "
        "contributes fairly to the growth and support of the alliance.\n\n"
        "You must update your alliance donation to at least 5% immediately.\n\n"
        "How to update your alliance donation:\n\n"
        "1. Open the menu.\n"
        "2. Click on Show Alliance.\n"
        "3. Go to Alliance Funds.\n"
        "4. Set your donation percentage to at least 5%.\n\n"
        "If this is not corrected, sanctions will follow in accordance with the alliance rules.\n\n"
        "This is your final opportunity to fix the issue before action is taken.",
    ),
}


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


def build_forum_thread_title(username: str, subject: str, conversation_id: str = "") -> str:
    """Build `Username - Title of DM (Conversation ID)` within Discord's title length."""
    title = f"{str(username or 'Unknown').strip()} - {str(subject or 'Untitled').strip()}"
    conversation_id = str(conversation_id or "").strip()
    if conversation_id:
        title = f"{title} ({conversation_id})"
    title = " ".join(title.split())
    if len(title) <= MAX_THREAD_TITLE_LENGTH:
        return title
    return title[: MAX_THREAD_TITLE_LENGTH - 1].rstrip() + "…"


def inbox_scan_delay_seconds(rng=random) -> float:
    """Return the next inbox scan delay with jitter."""
    return INBOX_SCAN_INTERVAL_SECONDS + float(rng.uniform(0, INBOX_SCAN_JITTER_SECONDS))


def tax_warning_scan_delay_seconds(rng=random) -> float:
    """Return the next tax-warning scan delay with jitter."""
    return TAX_WARNING_SCAN_INTERVAL_SECONDS + float(rng.uniform(0, TAX_WARNING_SCAN_JITTER_SECONDS))


def tax_warning_level(existing_warning_count: int) -> Optional[int]:
    """Return the next tax warning level, capped at three warnings."""
    count = max(0, int(existing_warning_count or 0))
    if count >= 3:
        return None
    return count + 1


def tax_warning_is_due(
    *,
    existing_warning_count: int,
    last_warning_at: Optional[int],
    now: int,
    min_days_between: int,
) -> bool:
    """Return whether another tax warning may be sent without rushing the member."""
    if tax_warning_level(existing_warning_count) is None:
        return False
    if not last_warning_at:
        return True
    min_gap = max(0, int(min_days_between or 0)) * 86400
    return int(now) - int(last_warning_at) >= min_gap


def tax_warning_member_identity(member: Dict[str, object]) -> Tuple[str, str, float]:
    """Extract the MissionChief id, username, and contribution rate from a member record."""
    mc_id = str(
        member.get("mc_user_id")
        or member.get("user_id")
        or member.get("member_id")
        or member.get("id")
        or ""
    ).strip()
    username = str(
        member.get("name")
        or member.get("username")
        or member.get("mc_username")
        or ""
    ).strip()
    try:
        rate = float(member.get("contribution_rate") or 0.0)
    except (TypeError, ValueError):
        rate = 0.0
    return mc_id, username, rate


def get_sanction_manager_cog(bot):
    """Return the loaded SanctionManager cog, accepting historic cog name variants."""
    get_cog = getattr(bot, "get_cog", None)
    if get_cog:
        for cog_name in SANCTION_MANAGER_COG_NAMES:
            cog = get_cog(cog_name)
            if cog:
                return cog

    loaded_cogs = getattr(bot, "cogs", None)
    if isinstance(loaded_cogs, dict):
        for cog in loaded_cogs.values():
            if hasattr(cog, "create_sanction_for_member") and hasattr(cog, "get_member_sanctions"):
                return cog
    elif loaded_cogs:
        for cog in loaded_cogs:
            if hasattr(cog, "create_sanction_for_member") and hasattr(cog, "get_member_sanctions"):
                return cog

    return None


def get_loaded_cog_names(bot) -> List[str]:
    loaded_cogs = getattr(bot, "cogs", None)
    if isinstance(loaded_cogs, dict):
        return sorted(str(name) for name in loaded_cogs)
    if loaded_cogs:
        names = []
        for cog in loaded_cogs:
            name = getattr(cog, "qualified_name", None) or cog.__class__.__name__
            names.append(str(name))
        return sorted(names)
    return []


def tax_warning_sanction_manager_error(bot) -> str:
    loaded_names = get_loaded_cog_names(bot)
    if loaded_names:
        preview = ", ".join(loaded_names[:20])
        if len(loaded_names) > 20:
            preview += f", ... +{len(loaded_names) - 20} more"
        return (
            "SanctionManager is not loaded, so this warning was not sent. "
            f"Loaded cogs: {preview}"
        )
    return "SanctionManager is not loaded, so this warning was not sent."


def discord_timestamp_from_iso(value: str, style: str = "F") -> str:
    """Convert an ISO timestamp with timezone to a Discord timestamp."""
    raw = str(value or "").strip()
    if not raw:
        return "Unknown"
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw
    return f"<t:{int(parsed.timestamp())}:{style}>"


def split_discord_content(text: str, limit: int = MAX_DISCORD_CONTENT_LENGTH) -> List[str]:
    """Split long message text into Discord-safe chunks, preferring paragraph and line breaks."""
    remaining = str(text or "").strip()
    if not remaining:
        return ["No message body found."]

    chunks = []
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break

        window = remaining[:limit]
        split_at = max(window.rfind("\n\n"), window.rfind("\n"), window.rfind(" "))
        if split_at < max(1, limit // 2):
            split_at = limit

        chunk = remaining[:split_at].strip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[split_at:].strip()

    return chunks


def format_duration(seconds: float) -> str:
    """Format a short operational duration for status messages."""
    total_seconds = max(0, int(seconds or 0))
    minutes = total_seconds // 60
    if minutes <= 0:
        return f"{total_seconds}s"
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if minutes:
        return f"{hours}h {minutes}m"
    return f"{hours}h"


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
            result = await self.manager._send_message_and_link(username, subject, body)
        except MemberResolutionError as exc:
            await interaction.followup.send(str(exc), ephemeral=True)
            return
        except Exception as exc:
            log.exception("MessageManager modal send failed")
            await interaction.followup.send(f"Could not send MissionChief message: {exc}", ephemeral=True)
            return

        if result["ok"]:
            await interaction.followup.send(self.manager._format_send_result(result), ephemeral=True)
        else:
            await interaction.followup.send(
                f"MissionChief message was not confirmed for `{result['resolved_username']}`: {result['reason']}",
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
            tax_warning_enabled=False,
            tax_warning_min_rate=TAX_WARNING_MIN_RATE,
            tax_warning_min_days_between=TAX_WARNING_MIN_DAYS_BETWEEN,
            tax_warning_send_delay_seconds=TAX_WARNING_SEND_DELAY_SECONDS,
            tax_warning_max_per_run=TAX_WARNING_MAX_PER_RUN,
            tax_warning_state={},
        )
        self._panel_task: Optional[asyncio.Task] = None
        self._inbox_task: Optional[asyncio.Task] = None
        self._tax_warning_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        add_view = getattr(self.bot, "add_view", None)
        if add_view:
            add_view(MessageManagerPanelView(self))
        self._panel_task = asyncio.create_task(self._delayed_panel_start())
        self._inbox_task = asyncio.create_task(self._inbox_scan_loop())
        self._tax_warning_task = asyncio.create_task(self._tax_warning_loop())

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
        if self._tax_warning_task:
            self._tax_warning_task.cancel()
            try:
                await self._tax_warning_task
            except asyncio.CancelledError:
                pass

    async def _delayed_panel_start(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)
        await self._ensure_panel_message()

    async def _report_bot_status(self, detail: str, *, priority: int = 70, ttl_seconds: int = 300) -> None:
        botstatus = self.bot.get_cog("BotStatus")
        report_activity = getattr(botstatus, "report_activity", None) if botstatus else None
        if not report_activity:
            return
        try:
            await report_activity(
                "MessageManager",
                detail,
                priority=priority,
                ttl_seconds=ttl_seconds,
            )
        except Exception:
            log.debug("Could not report MessageManager BotStatus activity", exc_info=True)

    async def _inbox_scan_loop(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(60)
        while True:
            delay = inbox_scan_delay_seconds()
            try:
                if await self.config.inbox_scan_enabled():
                    await self._scan_new_inbox_messages_with_status(
                        trigger="automatic",
                        next_delay=delay,
                    )
                else:
                    await self._report_bot_status(
                        "MissionChief DM checker is disabled",
                        priority=40,
                        ttl_seconds=int(delay),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("MessageManager inbox scan failed: %s", exc)
                await self._report_bot_status(
                    f"MissionChief DM check failed: {exc}",
                    priority=90,
                    ttl_seconds=600,
                )
            await asyncio.sleep(delay)

    async def _tax_warning_loop(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(120)
        while True:
            try:
                if await self.config.tax_warning_enabled():
                    guild = self._default_guild()
                    if guild:
                        await self._process_tax_warning_run(guild)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("MessageManager tax warning scan failed: %s", exc)
            await asyncio.sleep(tax_warning_scan_delay_seconds())

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

    def _default_guild(self) -> Optional[discord.Guild]:
        guilds = list(getattr(self.bot, "guilds", []) or [])
        return guilds[0] if guilds else None

    def _build_panel_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="MessageManager",
            description=(
                "Send MissionChief messages to current alliance members and route conversations through "
                f"forum channel `{DEFAULT_FORUM_CHANNEL_ID}`.\n\n"
                "The inbox is checked automatically every 15-20 minutes. System messages are ignored."
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
                message = await channel.fetch_message(int(message_id))
                await message.edit(embed=self._build_panel_embed(), view=MessageManagerPanelView(self))
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
        opening_title: str = "MissionChief Conversation",
        opening_timestamp: str = "",
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

        title = build_forum_thread_title(username, subject, conversation_id)
        created = await forum.create_thread(
            name=title,
            embed=self._build_conversation_embed(
                title=opening_title,
                conversation_id=conversation_id,
                username=username,
                subject=subject,
                timestamp=opening_timestamp,
            ),
            allowed_mentions=discord.AllowedMentions.none(),
        )
        thread = getattr(created, "thread", created)
        if isinstance(created, tuple):
            thread = created[0]
        if not isinstance(thread, discord.Thread):
            log.warning("MessageManager forum thread create returned unexpected result: %r", created)
            return None

        await self._send_body_chunks(thread, preview or "Conversation linked.")

        await self._save_conversation_thread(
            conversation_id,
            thread_id=thread.id,
            username=username,
            subject=subject,
            last_message_time=last_message_time,
        )
        return thread

    def _build_conversation_embed(
        self,
        *,
        title: str,
        conversation_id: str,
        username: str,
        subject: str,
        timestamp: str = "",
    ) -> discord.Embed:
        embed = discord.Embed(
            title=title,
            color=discord.Color.blue(),
        )
        embed.add_field(
            name="Member",
            value=discord.utils.escape_markdown(str(username or "Unknown")),
            inline=True,
        )
        embed.add_field(
            name="Conversation ID",
            value=f"`{conversation_id}`",
            inline=True,
        )
        embed.add_field(
            name="Time",
            value=discord_timestamp_from_iso(timestamp),
            inline=False,
        )
        if subject:
            embed.add_field(
                name="Title",
                value=discord.utils.escape_markdown(str(subject))[:1024],
                inline=False,
            )
        return embed

    def _build_inbound_reply_embed(
        self,
        *,
        conversation_id: str,
        username: str,
        subject: str,
        timestamp: str = "",
    ) -> discord.Embed:
        return self._build_conversation_embed(
            title="New MissionChief Reply",
            conversation_id=conversation_id,
            username=username,
            subject=subject,
            timestamp=timestamp,
        )

    async def _send_body_chunks(self, thread: discord.Thread, body: str) -> None:
        """Send plain message body chunks after the metadata embed."""
        for chunk in split_discord_content(body):
            await thread.send(
                content=chunk,
                allowed_mentions=discord.AllowedMentions.none(),
            )

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
            last_message_time=timestamp,
            opening_title="New MissionChief Reply",
            opening_timestamp=timestamp,
        )
        if not thread:
            return None

        if existing_thread_id and previous_time == timestamp and timestamp:
            return thread
        if not existing_thread_id:
            return thread

        await thread.send(
            embed=self._build_inbound_reply_embed(
                conversation_id=conversation_id,
                username=username,
                subject=subject,
                timestamp=timestamp,
            ),
            allowed_mentions=discord.AllowedMentions.none(),
        )
        await self._send_body_chunks(thread, body)
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

    def _sanction_manager(self):
        return get_sanction_manager_cog(self.bot)

    async def _tax_warning_history(self, guild_id: int, mc_user_id: str) -> Tuple[int, Optional[int]]:
        count = 0
        latest_at = None
        sanction_manager = self._sanction_manager()
        get_member_sanctions = getattr(sanction_manager, "get_member_sanctions", None) if sanction_manager else None
        if get_member_sanctions:
            sanctions = get_member_sanctions(
                guild_id=guild_id,
                mc_user_id=str(mc_user_id),
            )
            for sanction in sanctions:
                if "Warning" not in str(sanction.get("sanction_type") or ""):
                    continue
                if str(sanction.get("reason_detail") or "").strip() != TAX_WARNING_REASON_DETAIL:
                    continue
                if sanction.get("effective_status", sanction.get("status")) == "removed":
                    continue
                count += 1
                created_at = sanction.get("created_at")
                if created_at:
                    latest_at = max(int(created_at), int(latest_at or 0))

        state = await self.config.tax_warning_state()
        state_entry = state.get(str(mc_user_id)) or {}
        state_count = int(state_entry.get("count") or 0)
        state_latest_at = state_entry.get("last_warning_at")
        if state_latest_at:
            latest_at = max(int(state_latest_at), int(latest_at or 0))
        return max(count, state_count), latest_at

    async def _tax_warning_candidates(self, guild: discord.Guild) -> List[dict]:
        members = await self._get_alliance_members()
        min_rate = float(await self.config.tax_warning_min_rate())
        min_days_between = int(await self.config.tax_warning_min_days_between())
        now = int(time.time())
        candidates = []

        for member in members:
            if member.get("suspicious"):
                continue
            mc_id, username, rate = tax_warning_member_identity(member)
            if not mc_id or not username or rate >= min_rate:
                continue
            warning_count, last_warning_at = await self._tax_warning_history(guild.id, mc_id)
            next_level = tax_warning_level(warning_count)
            due = tax_warning_is_due(
                existing_warning_count=warning_count,
                last_warning_at=last_warning_at,
                now=now,
                min_days_between=min_days_between,
            )
            candidates.append(
                {
                    "mc_user_id": mc_id,
                    "username": username,
                    "rate": rate,
                    "warning_count": warning_count,
                    "next_level": next_level,
                    "last_warning_at": last_warning_at,
                    "due": due,
                }
            )

        candidates.sort(key=lambda item: (not item["due"], item["rate"], item["username"].casefold()))
        return candidates

    async def _save_tax_warning_state(self, mc_user_id: str, *, count: int, warning_at: int) -> None:
        async with self.config.tax_warning_state() as state:
            state[str(mc_user_id)] = {
                "count": int(count),
                "last_warning_at": int(warning_at),
            }

    async def _record_tax_warning_sanction(
        self,
        *,
        guild: discord.Guild,
        candidate: dict,
        level: int,
    ) -> Optional[int]:
        sanction_manager = self._sanction_manager()
        create_sanction = getattr(sanction_manager, "create_sanction_for_member", None) if sanction_manager else None
        if not create_sanction:
            raise RuntimeError(tax_warning_sanction_manager_error(self.bot))

        bot_user = getattr(self.bot, "user", None)
        admin_user_id = int(getattr(bot_user, "id", 0) or 0)
        return create_sanction(
            guild_id=guild.id,
            discord_user_id=None,
            mc_user_id=str(candidate["mc_user_id"]),
            mc_username=str(candidate["username"]),
            admin_user_id=admin_user_id,
            admin_username="MessageManager Auto Tax Warning",
            sanction_type=TAX_WARNING_SANCTION_TYPES[level],
            reason_category=TAX_WARNING_REASON_CATEGORY,
            reason_detail=TAX_WARNING_REASON_DETAIL,
            additional_notes=(
                f"Automatic TAX warning {level}/3 sent by MessageManager. "
                f"Contribution rate at send time: {candidate['rate']:.1f}%."
            ),
            status="active",
        )

    async def _send_tax_warning(self, guild: discord.Guild, candidate: dict) -> dict:
        level = int(candidate.get("next_level") or 0)
        if level not in TAX_WARNING_PRESETS:
            return {"sent": False, "reason": "Maximum warning count reached."}
        sanction_manager = self._sanction_manager()
        if not getattr(sanction_manager, "create_sanction_for_member", None):
            return {"sent": False, "reason": tax_warning_sanction_manager_error(self.bot)}

        subject_template, body_template = TAX_WARNING_PRESETS[level]
        body = body_template.format(
            username=candidate["username"],
        )
        send_result = await self._send_message_and_link(
            str(candidate["username"]),
            subject_template,
            body,
        )
        if not send_result["ok"]:
            return {"sent": False, "reason": send_result["reason"]}

        await self._record_tax_warning_sanction(
            guild=guild,
            candidate={**candidate, "username": send_result["resolved_username"]},
            level=level,
        )
        warning_at = int(time.time())
        await self._save_tax_warning_state(
            str(candidate["mc_user_id"]),
            count=level,
            warning_at=warning_at,
        )
        return {
            "sent": True,
            "level": level,
            "conversation_id": send_result["conversation_id"],
            "thread": send_result["thread"],
        }

    async def _process_tax_warning_run(self, guild: discord.Guild, *, limit: Optional[int] = None) -> dict:
        candidates = await self._tax_warning_candidates(guild)
        due_candidates = [candidate for candidate in candidates if candidate["due"] and candidate["next_level"]]
        max_per_run = int(await self.config.tax_warning_max_per_run())
        if limit is not None:
            max_per_run = min(max_per_run, max(0, int(limit)))
        send_delay = max(0, int(await self.config.tax_warning_send_delay_seconds()))

        sent = 0
        failed = 0
        errors = []
        for candidate in due_candidates[:max_per_run]:
            result = await self._send_tax_warning(guild, candidate)
            if result.get("sent"):
                sent += 1
            else:
                failed += 1
                errors.append(f"{candidate['username']}: {result.get('reason', 'unknown error')}")
            if sent + failed < min(len(due_candidates), max_per_run) and send_delay:
                await asyncio.sleep(send_delay)

        return {
            "candidates": len(candidates),
            "due": len(due_candidates),
            "sent": sent,
            "failed": failed,
            "skipped": max(0, len(due_candidates) - sent - failed),
            "errors": errors[:5],
        }

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

    async def _scan_new_inbox_messages_with_status(
        self,
        *,
        trigger: str,
        next_delay: Optional[float] = None,
    ) -> Tuple[int, int, int]:
        await self._report_bot_status(
            f"Checking MissionChief DMs ({trigger})",
            priority=80,
            ttl_seconds=180,
        )
        try:
            created, updated, skipped = await self._scan_new_inbox_messages()
        except Exception as exc:
            await self._report_bot_status(
                f"MissionChief DM check failed ({trigger}): {exc}",
                priority=90,
                ttl_seconds=600,
            )
            raise

        next_text = f"; next check in ~{format_duration(next_delay)}" if next_delay is not None else ""
        await self._report_bot_status(
            (
                f"MissionChief DMs checked ({trigger}): "
                f"{created} new, {updated} updated, {skipped} skipped{next_text}"
            ),
            priority=65,
            ttl_seconds=int(next_delay or 900),
        )
        return created, updated, skipped

    async def _scan_inbox_and_report(self, interaction: discord.Interaction) -> None:
        try:
            created, updated, skipped = await asyncio.wait_for(
                self._scan_new_inbox_messages_with_status(trigger="manual"),
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
        sent_at: str = "",
    ) -> Optional[discord.Thread]:
        try:
            return await asyncio.wait_for(
                self._ensure_conversation_thread(
                    conversation_id=conversation_id,
                    username=username,
                    subject=subject,
                    preview=body,
                    last_message_time=sent_at,
                    opening_title="MissionChief Message Sent",
                    opening_timestamp=sent_at,
                ),
                timeout=60,
            )
        except Exception:
            log.exception("Failed to create MessageManager forum thread for sent message")
            return None

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

    async def _send_message_and_link(self, username: str, subject: str, body: str) -> dict:
        ok, reason, resolved_username, conversation_id = await self._send_message(username, subject, body)
        thread = None
        sent_at = ""
        if ok and conversation_id:
            sent_at = datetime.now(timezone.utc).isoformat()
            thread = await self._link_sent_message_to_forum(
                conversation_id=conversation_id,
                username=resolved_username,
                subject=subject,
                body=body,
                sent_at=sent_at,
            )
        return {
            "ok": ok,
            "reason": reason,
            "resolved_username": resolved_username,
            "conversation_id": conversation_id,
            "thread": thread,
            "sent_at": sent_at,
        }

    @staticmethod
    def _format_send_result(result: dict) -> str:
        resolved_username = result.get("resolved_username") or "Unknown"
        conversation_id = result.get("conversation_id")
        thread = result.get("thread")
        message = f"MissionChief message sent to `{resolved_username}`."
        if thread:
            return f"{message} Conversation `{conversation_id}` linked to forum: {thread.mention}"
        if conversation_id:
            return f"{message} Conversation `{conversation_id}` was sent, but the forum thread could not be linked."
        return message

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

    @messagemanager.command(name="inboxstatus")
    @commands.admin()
    async def inbox_status(self, ctx: commands.Context):
        """Show the MissionChief inbox checker status."""
        enabled = await self.config.inbox_scan_enabled()
        task_running = bool(self._inbox_task and not self._inbox_task.done())
        await ctx.send(
            "MessageManager inbox checker:\n"
            f"- Enabled: `{enabled}`\n"
            f"- Background task running: `{task_running}`\n"
            f"- Interval: `15 minutes + 0-5 minutes jitter`\n"
            "- BotStatus: `MessageManager`"
        )

    @messagemanager.command(name="inboxscan")
    @commands.admin()
    async def inbox_scan_enabled(self, ctx: commands.Context, enabled: Optional[bool] = None):
        """Show or set whether automatic MissionChief inbox scans are enabled."""
        if enabled is None:
            current = await self.config.inbox_scan_enabled()
            await ctx.send(f"Automatic MissionChief inbox scans are currently `{current}`.")
            return
        await self.config.inbox_scan_enabled.set(bool(enabled))
        await ctx.send(f"Automatic MissionChief inbox scans set to `{bool(enabled)}`.")

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
                result = await self._send_message_and_link(username, subject, body)
            except Exception as exc:
                await ctx.send(f"Could not send MissionChief message: {exc}")
                return

        if result["ok"]:
            await ctx.send(self._format_send_result(result))
        else:
            await ctx.send(
                f"MissionChief message was not confirmed for `{result['resolved_username']}`: {result['reason']}"
            )

    @messagemanager.group(name="taxwarnings", invoke_without_command=True)
    @commands.admin()
    @commands.guild_only()
    async def taxwarnings(self, ctx: commands.Context):
        """Manage automatic TAX warning messages."""
        await ctx.send_help()

    @taxwarnings.command(name="settings")
    @commands.admin()
    @commands.guild_only()
    async def taxwarnings_settings(self, ctx: commands.Context):
        """Show TAX warning automation settings."""
        enabled = await self.config.tax_warning_enabled()
        min_rate = await self.config.tax_warning_min_rate()
        min_days = await self.config.tax_warning_min_days_between()
        delay = await self.config.tax_warning_send_delay_seconds()
        max_per_run = await self.config.tax_warning_max_per_run()
        await ctx.send(
            "MessageManager TAX warning settings:\n"
            f"- Enabled: `{enabled}`\n"
            f"- Minimum contribution: `{float(min_rate):.1f}%`\n"
            f"- Minimum days between warnings: `{min_days}`\n"
            f"- Send delay between members: `{delay}` seconds\n"
            f"- Max warnings per run: `{max_per_run}`\n"
            "- Kick automation: `not implemented`"
        )

    @taxwarnings.command(name="enable")
    @commands.admin()
    @commands.guild_only()
    async def taxwarnings_enable(self, ctx: commands.Context, enabled: bool):
        """Enable or disable automatic TAX warning scans."""
        await self.config.tax_warning_enabled.set(bool(enabled))
        state = "enabled" if enabled else "disabled"
        await ctx.send(f"Automatic TAX warning scans are now `{state}`.")

    @taxwarnings.command(name="minrate")
    @commands.admin()
    @commands.guild_only()
    async def taxwarnings_minrate(self, ctx: commands.Context, rate: float):
        """Set the minimum required contribution percentage."""
        if rate < 0 or rate > 100:
            await ctx.send("Minimum contribution rate must be between 0 and 100.")
            return
        await self.config.tax_warning_min_rate.set(float(rate))
        await ctx.send(f"TAX warning minimum contribution set to `{rate:.1f}%`.")

    @taxwarnings.command(name="gap")
    @commands.admin()
    @commands.guild_only()
    async def taxwarnings_gap(self, ctx: commands.Context, days: int):
        """Set the minimum days between warning 1, 2, and 3."""
        if days < 1:
            await ctx.send("Minimum days between warnings must be at least 1.")
            return
        await self.config.tax_warning_min_days_between.set(int(days))
        await ctx.send(f"TAX warning minimum gap set to `{days}` day(s).")

    @taxwarnings.command(name="delay")
    @commands.admin()
    @commands.guild_only()
    async def taxwarnings_delay(self, ctx: commands.Context, seconds: int):
        """Set the delay between warning messages to different members."""
        if seconds < 30:
            await ctx.send("Delay must be at least 30 seconds to avoid message bursts.")
            return
        await self.config.tax_warning_send_delay_seconds.set(int(seconds))
        await ctx.send(f"TAX warning send delay set to `{seconds}` seconds.")

    @taxwarnings.command(name="maxperrun")
    @commands.admin()
    @commands.guild_only()
    async def taxwarnings_maxperrun(self, ctx: commands.Context, count: int):
        """Set the maximum TAX warnings sent per automatic run."""
        if count < 1 or count > 25:
            await ctx.send("Max per run must be between 1 and 25.")
            return
        await self.config.tax_warning_max_per_run.set(int(count))
        await ctx.send(f"TAX warning max per run set to `{count}`.")

    @taxwarnings.command(name="preview")
    @commands.admin()
    @commands.guild_only()
    async def taxwarnings_preview(self, ctx: commands.Context):
        """Preview low-TAX members and warning eligibility without sending messages."""
        async with ctx.typing():
            try:
                candidates = await self._tax_warning_candidates(ctx.guild)
            except Exception as exc:
                await ctx.send(f"Could not build TAX warning preview: {exc}")
                return

        due = [candidate for candidate in candidates if candidate["due"] and candidate["next_level"]]
        blocked = [candidate for candidate in candidates if not candidate["due"] or not candidate["next_level"]]
        lines = [
            f"Low-TAX members: `{len(candidates)}`",
            f"Due now: `{len(due)}`",
            "",
        ]
        for candidate in due[:10]:
            lines.append(
                f"- `{candidate['username']}` ({candidate['rate']:.1f}%) -> warning "
                f"`{candidate['next_level']}/3`"
            )
        if blocked:
            lines.append("")
            lines.append(f"Not due yet or already at max warnings: `{len(blocked)}`")
        if len(due) > 10:
            lines.append(f"... and `{len(due) - 10}` more due members.")
        await ctx.send("\n".join(lines)[:1900])

    @taxwarnings.command(name="run")
    @commands.admin()
    @commands.guild_only()
    async def taxwarnings_run(self, ctx: commands.Context, limit: Optional[int] = None):
        """Send due TAX warnings with rate limiting."""
        async with ctx.typing():
            try:
                result = await self._process_tax_warning_run(ctx.guild, limit=limit)
            except Exception as exc:
                await ctx.send(f"Could not process TAX warnings: {exc}")
                return

        lines = [
            "TAX warning run complete.",
            f"- Low-TAX candidates: `{result['candidates']}`",
            f"- Due now: `{result['due']}`",
            f"- Sent: `{result['sent']}`",
            f"- Failed: `{result['failed']}`",
            f"- Still queued/skipped this run: `{result['skipped']}`",
        ]
        if result["errors"]:
            lines.append("")
            lines.append("Errors:")
            lines.extend(f"- {error}" for error in result["errors"])
        await ctx.send("\n".join(lines)[:1900])
