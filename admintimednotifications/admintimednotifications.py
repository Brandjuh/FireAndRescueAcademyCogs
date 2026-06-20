from __future__ import annotations

import asyncio
import calendar
import logging
import re
import unicodedata
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import pagify

log = logging.getLogger("red.fara.admintimednotifications")

DEFAULT_REMINDER_CHANNEL_ID = 1421625293130567690
DEFAULT_MANAGEMENT_CHANNEL_ID = 1426226521231589507
DEFAULT_LOG_CHANNEL_ID = 668919729762730004
LOCAL_TIMEZONE_NAME = "Europe/Amsterdam"
MANAGEMENT_PANEL_TITLE = "Admin timer management"

try:
    LOCAL_TIMEZONE = ZoneInfo(LOCAL_TIMEZONE_NAME)
except Exception:  # pragma: no cover - zoneinfo is available in supported runtimes
    LOCAL_TIMEZONE = timezone.utc

RECURRENCES = {
    "daily": "Daily",
    "weekly": "Weekly",
    "monthly": "Monthly",
    "yearly": "Yearly",
}

WEEKDAY_ALIASES = {
    "monday": 0,
    "mon": 0,
    "maandag": 0,
    "ma": 0,
    "tuesday": 1,
    "tue": 1,
    "dinsdag": 1,
    "di": 1,
    "wednesday": 2,
    "wed": 2,
    "woensdag": 2,
    "wo": 2,
    "thursday": 3,
    "thu": 3,
    "donderdag": 3,
    "do": 3,
    "friday": 4,
    "fri": 4,
    "vrijdag": 4,
    "vr": 4,
    "saturday": 5,
    "sat": 5,
    "zaterdag": 5,
    "za": 5,
    "sunday": 6,
    "sun": 6,
    "zondag": 6,
    "zo": 6,
}

MONTH_ALIASES = {
    "january": 1,
    "jan": 1,
    "januari": 1,
    "february": 2,
    "feb": 2,
    "februari": 2,
    "march": 3,
    "mar": 3,
    "maart": 3,
    "mrt": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "mei": 5,
    "june": 6,
    "jun": 6,
    "juni": 6,
    "july": 7,
    "jul": 7,
    "juli": 7,
    "august": 8,
    "aug": 8,
    "augustus": 8,
    "september": 9,
    "sep": 9,
    "sept": 9,
    "oktober": 10,
    "october": 10,
    "okt": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}

TODAY_ALIASES = {"today", "vandaag", "nu", "now"}
TOMORROW_ALIASES = {"tomorrow", "morgen"}
DAY_FILLER_WORDS = {
    "de",
    "den",
    "het",
    "op",
    "om",
    "elke",
    "iedere",
    "ieder",
    "dag",
    "datum",
    "van",
    "aankomende",
    "komende",
    "volgende",
    "next",
    "on",
    "the",
    "every",
}

DEFAULT_GUILD = {
    "admin_channel_id": DEFAULT_REMINDER_CHANNEL_ID,
    "admin_role_id": None,
    "management_channel_id": DEFAULT_MANAGEMENT_CHANNEL_ID,
    "management_message_id": None,
    "log_channel_id": DEFAULT_LOG_CHANNEL_ID,
    "reminders": [],
}


def _embed_title(embed: Any) -> Optional[str]:
    title = getattr(embed, "title", None)
    if title:
        return str(title)
    kwargs = getattr(embed, "kwargs", None)
    if isinstance(kwargs, dict) and kwargs.get("title"):
        return str(kwargs["title"])
    return None


def is_management_panel_message(message: Any, *, bot_user_id: Optional[int] = None) -> bool:
    author_id = getattr(getattr(message, "author", None), "id", None)
    if bot_user_id is not None and author_id is not None and int(author_id) != int(bot_user_id):
        return False
    return any(_embed_title(embed) == MANAGEMENT_PANEL_TITLE for embed in getattr(message, "embeds", []) or [])


def format_channel_reference(channel: Any, fallback_id: Optional[int]) -> str:
    if channel:
        channel_id = getattr(channel, "id", fallback_id)
        name = getattr(channel, "name", None)
        if name:
            return f"#{name} (`{channel_id}`)"
        mention = getattr(channel, "mention", None)
        if mention:
            return f"{mention} (`{channel_id}`)"
    return f"Missing channel `{fallback_id}`"


def parse_title_body(raw: str, *, default_title: str = "Admin reminder") -> tuple[str, str]:
    value = (raw or "").strip()
    if "|" not in value:
        return default_title, value
    title, body = value.split("|", 1)
    return title.strip() or default_title, body.strip()


def next_run(interval_minutes: int, *, now: Optional[datetime] = None) -> int:
    if interval_minutes < 1:
        raise ValueError("interval must be at least 1 minute")
    base = now or datetime.now(timezone.utc)
    return int((base + timedelta(minutes=interval_minutes)).timestamp())


def split_due_reminders(
    reminders: list[dict[str, Any]], *, now_ts: Optional[int] = None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    current = int(datetime.now(timezone.utc).timestamp()) if now_ts is None else int(now_ts)
    due = []
    pending = []
    for reminder in reminders:
        scheduled_due = int(reminder.get("next_run", 0)) <= current
        snooze_until = int(reminder.get("snooze_until") or 0)
        snooze_due = bool(snooze_until and snooze_until <= current)
        snooze_active = bool(snooze_until and snooze_until > current)
        if snooze_active:
            pending.append(reminder)
        elif scheduled_due or snooze_due:
            due.append(reminder)
        else:
            pending.append(reminder)
    return due, pending


def normalize_recurrence(value: str) -> str:
    recurrence = (value or "").strip().lower()
    aliases = {
        "dagelijks": "daily",
        "day": "daily",
        "daily": "daily",
        "wekelijks": "weekly",
        "week": "weekly",
        "weekly": "weekly",
        "maandelijks": "monthly",
        "month": "monthly",
        "monthly": "monthly",
        "jaarlijks": "yearly",
        "year": "yearly",
        "yearly": "yearly",
    }
    normalized = aliases.get(recurrence)
    if not normalized:
        raise ValueError("repeat must be daily, weekly, monthly, or yearly")
    return normalized


def parse_time_text(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"\s*(\d{1,2})[:.](\d{2})\s*", value or "")
    if not match:
        raise ValueError("time must be HH:MM")
    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour > 23 or minute > 59:
        raise ValueError("time must be a valid 24-hour time")
    return hour, minute


def _local_now(now: Optional[datetime] = None) -> datetime:
    current = now or datetime.now(LOCAL_TIMEZONE)
    if current.tzinfo is None:
        return current.replace(tzinfo=LOCAL_TIMEZONE)
    return current.astimezone(LOCAL_TIMEZONE)


def _timestamp_from_local(value: datetime) -> int:
    return int(value.astimezone(timezone.utc).timestamp())


def _normalize_day_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.casefold()
    text = text.replace(",", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip(" .")


def _tokens(value: str) -> list[str]:
    return re.findall(r"[a-z]+|\d{1,4}", _normalize_day_text(value))


def _without_fillers(value: str) -> str:
    return " ".join(token for token in _tokens(value) if token not in DAY_FILLER_WORDS)


def _parse_day_number(value: str) -> Optional[int]:
    text = _normalize_day_text(value)
    match = re.search(r"\b(\d{1,2})(?:e|de|ste|st|nd|rd|th)?\b", text)
    if not match:
        return None
    day = int(match.group(1))
    if not 1 <= day <= 31:
        return None
    return day


def _weekday_from_text(value: str) -> Optional[int]:
    text = _without_fillers(value)
    if not text:
        return None
    if text in WEEKDAY_ALIASES:
        return WEEKDAY_ALIASES[text]
    for token in _tokens(text):
        if token in WEEKDAY_ALIASES:
            return WEEKDAY_ALIASES[token]
        if token.isdigit():
            value_int = int(token)
            if 1 <= value_int <= 7:
                return value_int - 1
            if value_int == 0:
                return 6
    return None


def _relative_date_from_text(value: str, *, now: Optional[datetime] = None) -> Optional[date]:
    text = _without_fillers(value)
    if not text:
        return None
    current = _local_now(now)
    if text in TODAY_ALIASES:
        return current.date()
    if text in TOMORROW_ALIASES:
        return current.date() + timedelta(days=1)
    return None


def _day_month_from_text(value: str) -> Optional[tuple[int, int]]:
    text = _without_fillers(value)
    if not text:
        return None

    numeric = re.fullmatch(r"(\d{1,2})[-/. ](\d{1,2})(?:[-/. ]\d{2,4})?", text)
    if numeric:
        day = int(numeric.group(1))
        month = int(numeric.group(2))
        if 1 <= month <= 12 and 1 <= day <= 31:
            return day, month
        return None

    day_month = re.fullmatch(
        r"(\d{1,2})(?:e|de|ste|st|nd|rd|th)?\s+([a-z]+)(?:\s+\d{2,4})?",
        text,
    )
    if day_month:
        day = int(day_month.group(1))
        month = MONTH_ALIASES.get(day_month.group(2))
        if month and 1 <= day <= 31:
            return day, month
        return None

    month_day = re.fullmatch(
        r"([a-z]+)\s+(\d{1,2})(?:e|de|ste|st|nd|rd|th)?(?:\s+\d{2,4})?",
        text,
    )
    if month_day:
        month = MONTH_ALIASES.get(month_day.group(1))
        day = int(month_day.group(2))
        if month and 1 <= day <= 31:
            return day, month
    return None


def _date_from_text(value: str, *, now: Optional[datetime] = None) -> Optional[date]:
    text = _without_fillers(value)
    if not text:
        return None
    relative = _relative_date_from_text(text, now=now)
    if relative:
        return relative

    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y", "%d %m %Y"):
        with suppress(ValueError):
            return datetime.strptime(text, fmt).date()

    day_month = _day_month_from_text(text)
    if not day_month:
        return None
    day, month = day_month
    current = _local_now(now)
    candidate = _safe_date(current.year, month, day)
    if candidate < current.date():
        candidate = _safe_date(current.year + 1, month, day)
    return candidate


def _safe_date(year: int, month: int, day: int) -> date:
    max_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(day, max_day))


def _date_from_weekday(value: str, *, now: datetime, hour: int, minute: int) -> Optional[datetime]:
    weekday = _weekday_from_text(value)
    if weekday is None:
        return None
    days_ahead = (weekday - now.weekday()) % 7
    candidate_date = now.date() + timedelta(days=days_ahead)
    candidate = datetime.combine(candidate_date, time(hour, minute), tzinfo=LOCAL_TIMEZONE)
    if candidate <= now:
        candidate += timedelta(days=7)
    return candidate


def _month_day_from_reminder(reminder: dict[str, Any], fallback: int) -> int:
    day_text = str(reminder.get("day") or "").strip()
    full_date = _date_from_text(day_text)
    if full_date:
        return full_date.day
    day_month = _day_month_from_text(day_text)
    if day_month:
        return day_month[0]
    day_number = _parse_day_number(day_text)
    if day_number is not None:
        return day_number
    return fallback


def _year_day_month_from_reminder(reminder: dict[str, Any], fallback: datetime) -> tuple[int, int]:
    day_text = str(reminder.get("day") or "").strip()
    full_date = _date_from_text(day_text)
    if full_date:
        return full_date.day, full_date.month
    day_month = _day_month_from_text(day_text)
    if day_month:
        return day_month
    return fallback.day, fallback.month


def _safe_local_datetime(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    max_day = calendar.monthrange(year, month)[1]
    return datetime(
        year,
        month,
        min(day, max_day),
        hour,
        minute,
        tzinfo=LOCAL_TIMEZONE,
    )


def first_scheduled_run(
    recurrence: str,
    day_text: str,
    time_text: str,
    *,
    now: Optional[datetime] = None,
) -> int:
    recurrence = normalize_recurrence(recurrence)
    hour, minute = parse_time_text(time_text)
    current = _local_now(now)
    day_text = (day_text or "").strip()

    if recurrence == "daily":
        explicit_date = _date_from_text(day_text, now=current)
        weekday_candidate = _date_from_weekday(day_text, now=current, hour=hour, minute=minute)
        if weekday_candidate:
            candidate = weekday_candidate
        else:
            target_date = explicit_date or current.date()
            candidate = datetime.combine(target_date, time(hour, minute), tzinfo=LOCAL_TIMEZONE)
        if candidate <= current:
            candidate += timedelta(days=1)
        return _timestamp_from_local(candidate)

    if recurrence == "weekly":
        explicit_date = _date_from_text(day_text, now=current)
        if explicit_date:
            candidate = datetime.combine(explicit_date, time(hour, minute), tzinfo=LOCAL_TIMEZONE)
            if candidate <= current:
                candidate += timedelta(days=7)
            return _timestamp_from_local(candidate)

        candidate = _date_from_weekday(day_text, now=current, hour=hour, minute=minute)
        if not candidate:
            raise ValueError(
                "I do not recognize that weekday. Use for example `maandag`, `ma`, `Monday`, or `1`."
            )
        return _timestamp_from_local(candidate)

    if recurrence == "monthly":
        explicit_date = _date_from_text(day_text, now=current)
        day_month = _day_month_from_text(day_text)
        day_number = day_month[0] if day_month else _parse_day_number(day_text)
        if explicit_date:
            candidate = datetime.combine(explicit_date, time(hour, minute), tzinfo=LOCAL_TIMEZONE)
            day_number = explicit_date.day
        elif day_number:
            candidate = _safe_local_datetime(current.year, current.month, day_number, hour, minute)
        else:
            raise ValueError(
                "I do not recognize that monthly day. Use for example `15`, `15e`, or `15 juni`."
            )
        while candidate <= current:
            candidate = _advance_monthly(candidate, day_number or candidate.day)
        return _timestamp_from_local(candidate)

    day_month = _day_month_from_text(day_text)
    explicit_date = _date_from_text(day_text, now=current)
    if explicit_date:
        day = explicit_date.day
        month = explicit_date.month
    elif day_month:
        day, month = day_month
    else:
        raise ValueError(
            "I do not recognize that yearly date. Use for example `25-12`, `25 december`, or `december 25`."
        )

    candidate = _safe_local_datetime(current.year, month, day, hour, minute)
    while candidate <= current:
        candidate = _safe_local_datetime(candidate.year + 1, month, day, hour, minute)
    return _timestamp_from_local(candidate)


def _advance_monthly(current: datetime, target_day: int) -> datetime:
    month = current.month + 1
    year = current.year
    if month > 12:
        month = 1
        year += 1
    return _safe_local_datetime(year, month, target_day, current.hour, current.minute)


def next_scheduled_run(reminder: dict[str, Any], *, now_ts: Optional[int] = None) -> int:
    recurrence = reminder.get("recurrence")
    if not recurrence:
        interval = int(reminder.get("interval_minutes") or 0)
        return next_run(interval)

    recurrence = normalize_recurrence(str(recurrence))
    current_ts = int(reminder.get("next_run") or 0)
    if current_ts <= 0:
        return first_scheduled_run(
            recurrence,
            str(reminder.get("day") or ""),
            str(reminder.get("time") or "09:00"),
        )

    after_ts = int(datetime.now(timezone.utc).timestamp()) if now_ts is None else int(now_ts)
    candidate = datetime.fromtimestamp(current_ts, tz=timezone.utc).astimezone(LOCAL_TIMEZONE)

    while int(candidate.astimezone(timezone.utc).timestamp()) <= after_ts:
        if recurrence == "daily":
            candidate += timedelta(days=1)
        elif recurrence == "weekly":
            candidate += timedelta(days=7)
        elif recurrence == "monthly":
            target_day = _month_day_from_reminder(reminder, candidate.day)
            candidate = _advance_monthly(candidate, target_day)
        else:
            target_day, target_month = _year_day_month_from_reminder(reminder, candidate)
            candidate = _safe_local_datetime(
                candidate.year + 1,
                target_month,
                target_day,
                candidate.hour,
                candidate.minute,
            )

    return _timestamp_from_local(candidate)


def make_reminder_id() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


@dataclass
class TimerWizardState:
    recurrence: Optional[str] = None


class AdminTimerPanelView(discord.ui.View):
    def __init__(self, cog: "AdminTimedNotifications"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Timer management",
        style=discord.ButtonStyle.primary,
        custom_id="fara_admintimer:management",
    )
    async def open_management(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        if not await self.cog.can_manage(interaction.guild, interaction.user):
            await interaction.response.send_message("You do not have permission.", ephemeral=True)
            return
        await interaction.response.send_message(
            await self.cog.management_menu_text(interaction.guild),
            view=TimerManagementView(self.cog, interaction.user.id),
            ephemeral=True,
        )


class TimerManagementView(discord.ui.View):
    def __init__(self, cog: "AdminTimedNotifications", user_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id

    @discord.ui.button(label="Add timer", style=discord.ButtonStyle.success)
    async def add_timer(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        await interaction.response.edit_message(
            content="Timer wizard\n\nStep 1: choose how often the reminder repeats.",
            view=TimerWizardView(self.cog, interaction.user.id),
        )

    @discord.ui.button(label="List timers", style=discord.ButtonStyle.secondary)
    async def list_timers(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        await interaction.response.edit_message(
            content=await self.cog.format_reminder_list(interaction.guild),
            view=self,
        )

    @discord.ui.button(label="Remove timer", style=discord.ButtonStyle.danger)
    async def remove_timer(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        reminders = await self.cog.config.guild(interaction.guild).reminders()
        if not reminders:
            await interaction.response.edit_message(content="No admin reminders configured.", view=self)
            return
        await interaction.response.edit_message(
            content="Choose the timer to remove.",
            view=TimerRemoveSelectView(self.cog, reminders, interaction.user.id),
        )

    @discord.ui.button(label="Back to menu", style=discord.ButtonStyle.secondary)
    async def back_to_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        await interaction.response.edit_message(
            content=await self.cog.management_menu_text(interaction.guild),
            view=TimerManagementView(self.cog, interaction.user.id),
        )


class TimerRecurrenceSelect(discord.ui.Select):
    def __init__(self, wizard_view: "TimerWizardView"):
        self.wizard_view = wizard_view
        options = [
            discord.SelectOption(label="Daily", value="daily", description="Every day at the chosen time"),
            discord.SelectOption(label="Weekly", value="weekly", description="Every week on the chosen weekday"),
            discord.SelectOption(label="Monthly", value="monthly", description="Every month on the chosen day"),
            discord.SelectOption(label="Yearly", value="yearly", description="Every year on the chosen date"),
        ]
        super().__init__(
            placeholder="Repeat schedule",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        self.wizard_view.state.recurrence = normalize_recurrence(self.values[0])
        await interaction.response.edit_message(
            content=(
                "Timer wizard\n\n"
                f"Repeat: {RECURRENCES[self.wizard_view.state.recurrence]}\n"
                "Step 2: press Details and enter the day/date, time, and message."
            ),
            view=self.wizard_view,
        )


class TimerWizardView(discord.ui.View):
    def __init__(self, cog: "AdminTimedNotifications", user_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.user_id = user_id
        self.state = TimerWizardState()
        self.add_item(TimerRecurrenceSelect(self))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id

    @discord.ui.button(label="Details", style=discord.ButtonStyle.primary)
    async def details(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        if not self.state.recurrence:
            await interaction.response.send_message("Choose a repeat schedule first.", ephemeral=True)
            return
        await interaction.response.send_modal(TimerDetailsModal(self.cog, self.state))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        await interaction.response.edit_message(
            content="Cancelled. No timer was created.",
            view=TimerManagementView(self.cog, interaction.user.id),
        )


class TimerDetailsModal(discord.ui.Modal, title="Admin timer"):
    title_input = discord.ui.TextInput(
        label="Title",
        max_length=100,
        required=False,
        placeholder="Optional, for example: Weekly report",
    )
    day_input = discord.ui.TextInput(
        label="Day or date",
        max_length=30,
        required=False,
        placeholder="Examples: maandag, ma, 15e, 15 juni, 25-12, morgen.",
    )
    time_input = discord.ui.TextInput(
        label="Time",
        max_length=5,
        required=True,
        placeholder="HH:MM, for example 19:30",
    )
    message_input = discord.ui.TextInput(
        label="Message",
        style=discord.TextStyle.paragraph,
        max_length=1900,
        required=True,
        placeholder="What should admins do?",
    )

    def __init__(self, cog: "AdminTimedNotifications", state: TimerWizardState):
        super().__init__()
        self.cog = cog
        self.state = state
        if state.recurrence == "daily":
            self.day_input.placeholder = "Optional: vandaag, morgen, maandag, or 2026-06-15."
        elif state.recurrence == "weekly":
            self.day_input.required = True
            self.day_input.placeholder = "Weekday: maandag, ma, Monday, mon, or 1-7."
        elif state.recurrence == "monthly":
            self.day_input.required = True
            self.day_input.placeholder = "Day: 1, 15, 15e, de 15e, or 15 juni."
        elif state.recurrence == "yearly":
            self.day_input.required = True
            self.day_input.placeholder = "Date: 25-12, 25 december, december 25, or 2026-12-25."

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        if not await self.cog.can_manage(interaction.guild, interaction.user):
            await interaction.response.send_message("You do not have permission.", ephemeral=True)
            return

        recurrence = self.state.recurrence
        title = str(self.title_input.value or "").strip() or "Admin reminder"
        day_text = str(self.day_input.value or "").strip()
        time_text = str(self.time_input.value or "").strip()
        body = str(self.message_input.value or "").strip()
        if not recurrence:
            await interaction.response.send_message("Choose a repeat schedule first.", ephemeral=True)
            return
        if not body:
            await interaction.response.send_message("Give me reminder text.", ephemeral=True)
            return

        try:
            first_run = first_scheduled_run(recurrence, day_text, time_text)
        except ValueError as exc:
            await interaction.response.send_message(str(exc), ephemeral=True)
            return

        reminder = {
            "id": make_reminder_id(),
            "title": title[:100],
            "body": body[:1900],
            "recurrence": recurrence,
            "day": day_text,
            "time": time_text,
            "next_run": first_run,
            "created_by": interaction.user.id,
            "created_at": int(datetime.now(timezone.utc).timestamp()),
            "snooze_until": None,
        }
        await self.cog.add_reminder(interaction.guild, reminder, actor=interaction.user)
        await interaction.response.send_message(
            (
                f"Timer `{reminder['id']}` created.\n"
                f"Next reminder: <t:{first_run}:F>\n"
                "It will post in the configured admin reminder channel."
            ),
            ephemeral=True,
        )


class TimerRemoveSelect(discord.ui.Select):
    def __init__(
        self,
        cog: "AdminTimedNotifications",
        reminders: list[dict[str, Any]],
        user_id: int,
    ):
        self.cog = cog
        self.user_id = user_id
        options = [
            discord.SelectOption(
                label=str(item.get("title") or "Admin reminder")[:100],
                value=str(item.get("id")),
                description=f"Next: {item.get('next_run', 0)}"[:100],
            )
            for item in reminders[:25]
        ]
        super().__init__(placeholder="Choose a timer", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This menu is not for you.", ephemeral=True)
            return
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        reminder_id = int(self.values[0])
        removed = await self.cog.remove_reminder(interaction.guild, reminder_id, actor=interaction.user)
        await interaction.response.edit_message(
            content=(
                f"Timer `{reminder_id}` removed."
                if removed
                else "That timer was already removed."
            ),
            view=TimerManagementView(self.cog, interaction.user.id),
        )


class TimerRemoveSelectView(discord.ui.View):
    def __init__(
        self,
        cog: "AdminTimedNotifications",
        reminders: list[dict[str, Any]],
        user_id: int,
    ):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.add_item(TimerRemoveSelect(cog, reminders, user_id))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id


class ReminderActionButton(discord.ui.Button):
    def __init__(
        self,
        cog: "AdminTimedNotifications",
        reminder_id: int,
        action: str,
        label: str,
        style: discord.ButtonStyle,
    ):
        super().__init__(
            label=label,
            style=style,
            custom_id=f"fara_admintimer:{int(reminder_id)}:{action}",
        )
        self.cog = cog
        self.reminder_id = int(reminder_id)
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        await self.cog.handle_reminder_action(interaction, self.reminder_id, self.action)


class ReminderActionView(discord.ui.View):
    def __init__(self, cog: "AdminTimedNotifications", reminder_id: int):
        super().__init__(timeout=None)
        self.add_item(
            ReminderActionButton(
                cog,
                reminder_id,
                "accepted",
                "Accepted",
                discord.ButtonStyle.success,
            )
        )
        self.add_item(
            ReminderActionButton(
                cog,
                reminder_id,
                "ignore",
                "Ignore",
                discord.ButtonStyle.danger,
            )
        )
        self.add_item(
            ReminderActionButton(
                cog,
                reminder_id,
                "snooze",
                "Snooze 1 hour",
                discord.ButtonStyle.secondary,
            )
        )


class AdminTimedNotifications(commands.Cog):
    """Repeated reminders for admins."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFACA_D001, force_registration=True)
        self.config.register_guild(**DEFAULT_GUILD)
        self._task: Optional[asyncio.Task] = None
        self._restore_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        self.bot.add_view(AdminTimerPanelView(self))
        self._task = asyncio.create_task(self.reminder_loop())
        self._restore_task = asyncio.create_task(self.restore_views_and_panels())

    def cog_unload(self):
        if self._task:
            self._task.cancel()
        if self._restore_task:
            self._restore_task.cancel()

    @asynccontextmanager
    async def _bot_status(self, detail: str, *, priority: int = 70):
        bot = getattr(self, "bot", None)
        botstatus = bot.get_cog("BotStatus") if bot else None
        if botstatus and hasattr(botstatus, "track_activity"):
            async with botstatus.track_activity("AdminTimedNotifications", detail, priority=priority):
                yield
        else:
            yield

    async def restore_views_and_panels(self):
        await self.bot.wait_until_red_ready()
        for guild in self.bot.guilds:
            reminders = await self.config.guild(guild).reminders()
            for reminder in reminders:
                reminder_id = reminder.get("id")
                if reminder_id:
                    self.bot.add_view(ReminderActionView(self, int(reminder_id)))
            await self.ensure_management_panel(guild, create=True)

    async def reminder_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                await self.run_due_reminders()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Admin reminder loop failed")
            await asyncio.sleep(60)

    async def can_manage(self, guild: discord.Guild, user: Any) -> bool:
        if not guild:
            return False
        if getattr(getattr(user, "guild_permissions", None), "administrator", False):
            return True
        role_id = await self.config.guild(guild).admin_role_id()
        if not role_id:
            return False
        return any(role.id == role_id for role in getattr(user, "roles", []))

    async def configured_channel_id(self, guild: discord.Guild, key: str, fallback: int) -> int:
        value = await getattr(self.config.guild(guild), key)()
        return int(value or fallback)

    def resolve_channel(self, guild: discord.Guild, channel_id: Optional[int]):
        if not channel_id:
            return None
        return guild.get_channel(int(channel_id)) or self.bot.get_channel(int(channel_id))

    async def reminder_channel(self, guild: discord.Guild):
        channel_id = await self.configured_channel_id(
            guild,
            "admin_channel_id",
            DEFAULT_REMINDER_CHANNEL_ID,
        )
        return self.resolve_channel(guild, channel_id)

    async def log_channel(self, guild: discord.Guild):
        channel_id = await self.configured_channel_id(guild, "log_channel_id", DEFAULT_LOG_CHANNEL_ID)
        return self.resolve_channel(guild, channel_id)

    async def management_channel(self, guild: discord.Guild):
        channel_id = await self.configured_channel_id(
            guild,
            "management_channel_id",
            DEFAULT_MANAGEMENT_CHANNEL_ID,
        )
        return self.resolve_channel(guild, channel_id)

    async def management_menu_text(self, guild: discord.Guild) -> str:
        reminders = await self.config.guild(guild).reminders()
        reminder_channel_id = await self.configured_channel_id(
            guild,
            "admin_channel_id",
            DEFAULT_REMINDER_CHANNEL_ID,
        )
        log_channel_id = await self.configured_channel_id(guild, "log_channel_id", DEFAULT_LOG_CHANNEL_ID)
        reminder_channel = await self.reminder_channel(guild)
        log_channel = await self.log_channel(guild)
        return "\n".join(
            [
                "Timer management",
                "",
                f"Active timers: {len(reminders)}",
                f"Reminder channel: {format_channel_reference(reminder_channel, reminder_channel_id)}",
                f"Log channel: {format_channel_reference(log_channel, log_channel_id)}",
            ]
        )

    async def build_management_embed(
        self,
        guild: discord.Guild,
        reminders: list[dict[str, Any]],
    ) -> discord.Embed:
        reminder_channel_id = await self.configured_channel_id(
            guild,
            "admin_channel_id",
            DEFAULT_REMINDER_CHANNEL_ID,
        )
        log_channel_id = await self.configured_channel_id(guild, "log_channel_id", DEFAULT_LOG_CHANNEL_ID)
        reminder_channel = await self.reminder_channel(guild)
        log_channel = await self.log_channel(guild)
        embed = discord.Embed(
            title=MANAGEMENT_PANEL_TITLE,
            description="Use the button below to create, list, or remove admin timers.",
            color=discord.Color.orange(),
        )
        embed.add_field(name="Active timers", value=str(len(reminders)), inline=True)
        embed.add_field(
            name="Reminder channel",
            value=format_channel_reference(reminder_channel, reminder_channel_id),
            inline=True,
        )
        embed.add_field(
            name="Log channel",
            value=format_channel_reference(log_channel, log_channel_id),
            inline=True,
        )
        return embed

    async def find_existing_management_panel(self, channel: Any):
        history = getattr(channel, "history", None)
        if not history:
            return None

        bot_user_id = getattr(getattr(self.bot, "user", None), "id", None)
        found = []
        try:
            async for message in channel.history(limit=50):
                if is_management_panel_message(message, bot_user_id=bot_user_id):
                    found.append(message)
        except (discord.Forbidden, discord.HTTPException):
            return None

        if not found:
            return None

        keep = found[0]
        for duplicate in found[1:]:
            with suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                await duplicate.delete()
        return keep

    async def ensure_management_panel(self, guild: discord.Guild, *, create: bool = True):
        channel = await self.management_channel(guild)
        if not channel:
            return None
        reminders = await self.config.guild(guild).reminders()
        embed = await self.build_management_embed(guild, reminders)
        view = AdminTimerPanelView(self)
        message_id = await self.config.guild(guild).management_message_id()
        if message_id and hasattr(channel, "fetch_message"):
            with suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                message = await channel.fetch_message(int(message_id))
                await message.edit(embed=embed, view=view)
                return message

        existing = await self.find_existing_management_panel(channel)
        if existing:
            await existing.edit(embed=embed, view=view)
            await self.config.guild(guild).management_message_id.set(existing.id)
            return existing

        if not create:
            return None

        message = await channel.send(embed=embed, view=view)
        await self.config.guild(guild).management_message_id.set(message.id)
        return message

    def reminder_schedule_label(self, reminder: dict[str, Any]) -> str:
        if reminder.get("recurrence"):
            recurrence = RECURRENCES.get(str(reminder.get("recurrence")), str(reminder.get("recurrence")))
            day = str(reminder.get("day") or "").strip()
            time_text = str(reminder.get("time") or "").strip()
            return f"{recurrence} {day} {time_text}".strip()
        return f"Every {reminder.get('interval_minutes')} minutes"

    async def format_reminder_list(self, guild: discord.Guild) -> str:
        reminders = await self.config.guild(guild).reminders()
        if not reminders:
            return "No admin reminders configured."
        lines = ["Configured admin timers:"]
        for item in reminders:
            next_at = int(item.get("next_run", 0))
            snooze_until = int(item.get("snooze_until") or 0)
            suffix = f" | snoozed until <t:{snooze_until}:R>" if snooze_until else ""
            lines.append(
                f"`{item['id']}` {item.get('title', 'Admin reminder')} - "
                f"{self.reminder_schedule_label(item)} - next <t:{next_at}:F>{suffix}"
            )
        return "\n".join(lines)

    def build_reminder_embed(
        self,
        guild: discord.Guild,
        reminder: dict[str, Any],
        *,
        status: Optional[str] = None,
    ) -> discord.Embed:
        del guild
        embed = discord.Embed(
            title=reminder.get("title") or "Admin reminder",
            description=reminder.get("body") or "",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        next_at = int(reminder.get("next_run", 0))
        embed.add_field(name="Schedule", value=self.reminder_schedule_label(reminder), inline=False)
        if next_at:
            embed.add_field(name="Next scheduled reminder", value=f"<t:{next_at}:F>", inline=False)
        if status:
            embed.add_field(name="Status", value=status, inline=False)
        embed.set_footer(text=f"Timer ID: {reminder.get('id')}")
        return embed

    async def get_reminder(self, guild: discord.Guild, reminder_id: int) -> Optional[dict[str, Any]]:
        reminders = await self.config.guild(guild).reminders()
        for reminder in reminders:
            if int(reminder.get("id", 0)) == int(reminder_id):
                return dict(reminder)
        return None

    async def add_reminder(self, guild: discord.Guild, reminder: dict[str, Any], *, actor: Any):
        async with self.config.guild(guild).reminders() as reminders:
            reminders.append(reminder)
        self.bot.add_view(ReminderActionView(self, int(reminder["id"])))
        await self.ensure_management_panel(guild, create=False)
        await self.log_timer_action(guild, "created", reminder, actor=actor)

    async def remove_reminder(self, guild: discord.Guild, reminder_id: int, *, actor: Any) -> bool:
        removed = None
        async with self.config.guild(guild).reminders() as reminders:
            kept = []
            for reminder in reminders:
                if int(reminder.get("id", 0)) == int(reminder_id):
                    removed = reminder
                else:
                    kept.append(reminder)
            reminders[:] = kept
        if removed:
            await self.ensure_management_panel(guild, create=False)
            await self.log_timer_action(guild, "removed", removed, actor=actor)
            return True
        return False

    async def set_reminder_snooze(
        self,
        guild: discord.Guild,
        reminder_id: int,
        snooze_until: int,
    ) -> dict[str, Any]:
        updated = None
        async with self.config.guild(guild).reminders() as reminders:
            for reminder in reminders:
                if int(reminder.get("id", 0)) == int(reminder_id):
                    reminder["snooze_until"] = int(snooze_until)
                    updated = dict(reminder)
                    break
        return updated or {}

    async def send_reminder_message(
        self,
        guild: discord.Guild,
        reminder: dict[str, Any],
        *,
        status: Optional[str] = None,
    ):
        channel = await self.reminder_channel(guild)
        if not channel:
            return None
        role_id = await self.config.guild(guild).admin_role_id()
        role = guild.get_role(int(role_id)) if role_id else None
        embed = self.build_reminder_embed(guild, reminder, status=status)
        message = await channel.send(
            content=role.mention if role else None,
            embed=embed,
            view=ReminderActionView(self, int(reminder["id"])),
        )
        return message

    def disabled_reminder_view(self, reminder_id: int) -> ReminderActionView:
        view = ReminderActionView(self, reminder_id)
        for child in view.children:
            child.disabled = True
        return view

    async def handle_reminder_action(
        self,
        interaction: discord.Interaction,
        reminder_id: int,
        action: str,
    ):
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        if not await self.can_manage(interaction.guild, interaction.user):
            await interaction.response.send_message("You do not have permission.", ephemeral=True)
            return

        reminder = await self.get_reminder(interaction.guild, reminder_id)
        if not reminder:
            await interaction.response.send_message("This timer no longer exists.", ephemeral=True)
            return

        if action == "accepted":
            await interaction.response.defer(ephemeral=True)
            await self.log_timer_action(
                interaction.guild,
                "accepted",
                reminder,
                actor=interaction.user,
            )
            with suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                await interaction.message.delete()
            await interaction.followup.send("Reminder accepted and removed.", ephemeral=True)
            return

        if action == "ignore":
            await interaction.response.defer(ephemeral=True)
            await self.log_timer_action(
                interaction.guild,
                "ignored",
                reminder,
                actor=interaction.user,
            )
            with suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                await interaction.message.delete()
            await interaction.followup.send("Reminder ignored and removed.", ephemeral=True)
            return

        if action == "snooze":
            await interaction.response.defer(ephemeral=True)
            snooze_until = int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())
            reminder = await self.set_reminder_snooze(interaction.guild, reminder_id, snooze_until)
            await self.log_timer_action(
                interaction.guild,
                "snoozed",
                reminder,
                actor=interaction.user,
                details=f"Until <t:{snooze_until}:F>",
            )
            with suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                await interaction.message.delete()
            await interaction.followup.send(
                f"Reminder snoozed until <t:{snooze_until}:F> and removed.",
                ephemeral=True,
            )

    async def run_due_reminders(self):
        current_ts = int(datetime.now(timezone.utc).timestamp())
        for guild in self.bot.guilds:
            reminders = await self.config.guild(guild).reminders()
            due_items = []
            for reminder in reminders:
                scheduled_due = int(reminder.get("next_run", 0)) <= current_ts
                snooze_until = int(reminder.get("snooze_until") or 0)
                snooze_due = bool(snooze_until and snooze_until <= current_ts)
                snooze_active = bool(snooze_until and snooze_until > current_ts)
                if snooze_active:
                    continue
                if scheduled_due or snooze_due:
                    due_items.append((reminder, scheduled_due, snooze_due))

            if not due_items:
                continue

            async with self._bot_status(f"posting {len(due_items)} admin reminders in {guild.name}"):
                for reminder, scheduled_due, snooze_due in due_items:
                    status = "Snoozed reminder" if snooze_due and not scheduled_due else None
                    message = await self.send_reminder_message(guild, reminder, status=status)
                    if message:
                        reminder["last_message_id"] = message.id
                        await self.log_timer_action(
                            guild,
                            "posted",
                            reminder,
                            details="Snooze due" if snooze_due and not scheduled_due else "Scheduled due",
                        )
                    if snooze_due:
                        reminder["snooze_until"] = None
                    if scheduled_due:
                        reminder["next_run"] = next_scheduled_run(reminder, now_ts=current_ts)

            await self.config.guild(guild).reminders.set(reminders)

    async def log_timer_action(
        self,
        guild: discord.Guild,
        action: str,
        reminder: dict[str, Any],
        *,
        actor: Any = None,
        details: Optional[str] = None,
    ):
        actor_text = actor.mention if actor else "System"
        title = reminder.get("title") or "Admin reminder"
        channel = await self.log_channel(guild)
        if channel:
            embed = discord.Embed(
                title=f"Admin timer {action}",
                color=discord.Color.orange(),
                timestamp=datetime.now(timezone.utc),
            )
            embed.add_field(name="Timer", value=f"`{reminder.get('id')}` {title}", inline=False)
            embed.add_field(name="Actor", value=actor_text, inline=True)
            embed.add_field(name="Schedule", value=self.reminder_schedule_label(reminder), inline=True)
            if details:
                embed.add_field(name="Details", value=details, inline=False)
            with suppress(discord.Forbidden, discord.HTTPException):
                await channel.send(embed=embed)

        await self._record_membermanager_timer_event(
            guild,
            f"admin_timer_{action}",
            reminder,
            actor_id=getattr(actor, "id", None),
            details=details,
        )

    async def _record_membermanager_timer_event(
        self,
        guild: discord.Guild,
        event_type: str,
        reminder: dict[str, Any],
        *,
        actor_id: Optional[int] = None,
        details: Optional[str] = None,
    ):
        try:
            member_manager = self.bot.get_cog("MemberManager")
            member_db = getattr(member_manager, "db", None) if member_manager else None
            add_event = getattr(member_db, "add_event", None)
            if not add_event:
                return
            await add_event(
                guild_id=guild.id,
                discord_id=actor_id,
                mc_user_id=None,
                event_type=event_type,
                event_data={
                    "reminder_id": reminder.get("id"),
                    "title": reminder.get("title"),
                    "recurrence": reminder.get("recurrence") or reminder.get("interval_minutes"),
                    "scheduled_for": reminder.get("next_run"),
                    "status": event_type.replace("admin_timer_", ""),
                    "note": details,
                },
                triggered_by="admintimednotifications",
                actor_id=actor_id,
            )
        except Exception:
            log.exception("Failed to record MemberManager audit event for AdminTimedNotifications")

    @commands.group(name="admintimerset")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def admintimerset(self, ctx: commands.Context):
        """Configure admin timed notifications."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @admintimerset.command(name="channel")
    async def admintimerset_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where reminders are posted."""
        await self.config.guild(ctx.guild).admin_channel_id.set(channel.id)
        await ctx.send(f"Admin reminder channel set to {channel.mention}.")

    @admintimerset.command(name="role")
    async def admintimerset_role(self, ctx: commands.Context, role: discord.Role):
        """Set the role to ping for reminders."""
        await self.config.guild(ctx.guild).admin_role_id.set(role.id)
        await ctx.send(f"Admin reminder role set to {role.mention}.")

    @admintimerset.command(name="managementchannel")
    async def admintimerset_management_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where the timer management panel is posted."""
        await self.config.guild(ctx.guild).management_channel_id.set(channel.id)
        await ctx.send(f"Admin timer management channel set to {channel.mention}.")
        await self.ensure_management_panel(ctx.guild)

    @admintimerset.command(name="logchannel")
    async def admintimerset_log_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where timer audit logs are posted."""
        await self.config.guild(ctx.guild).log_channel_id.set(channel.id)
        await ctx.send(f"Admin timer log channel set to {channel.mention}.")

    @commands.group(name="admintimer")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def admintimer(self, ctx: commands.Context):
        """Manage repeated admin reminders."""
        if ctx.invoked_subcommand is None:
            await ctx.send(await self.management_menu_text(ctx.guild), view=TimerManagementView(self, ctx.author.id))

    @admintimer.command(name="manage")
    async def admintimer_manage(self, ctx: commands.Context):
        """Open the timer management menu."""
        await ctx.send(await self.management_menu_text(ctx.guild), view=TimerManagementView(self, ctx.author.id))

    @admintimer.command(name="panel")
    async def admintimer_panel(self, ctx: commands.Context):
        """Post or refresh the persistent timer management panel."""
        message = await self.ensure_management_panel(ctx.guild)
        if message:
            await ctx.send(f"Timer management panel ready: {message.jump_url}")
        else:
            await ctx.send("Timer management channel is not available.")

    @admintimer.command(name="add")
    async def admintimer_add(self, ctx: commands.Context, interval_minutes: int, *, content: str):
        """Add a legacy repeated reminder. Format: `<minutes> Title | message`."""
        if interval_minutes < 1:
            await ctx.send("Interval must be at least 1 minute.")
            return
        title, body = parse_title_body(content)
        if not body:
            await ctx.send("Give me reminder text.")
            return
        item = {
            "id": make_reminder_id(),
            "title": title,
            "body": body,
            "interval_minutes": interval_minutes,
            "next_run": next_run(interval_minutes),
            "created_by": ctx.author.id,
            "created_at": int(datetime.now(timezone.utc).timestamp()),
            "snooze_until": None,
        }
        await self.add_reminder(ctx.guild, item, actor=ctx.author)
        await ctx.send(f"Admin reminder `{item['id']}` added.")

    @admintimer.command(name="list")
    async def admintimer_list(self, ctx: commands.Context):
        """List configured reminders."""
        for page in pagify(await self.format_reminder_list(ctx.guild), page_length=1800):
            await ctx.send(page)

    @admintimer.command(name="remove")
    async def admintimer_remove(self, ctx: commands.Context, reminder_id: int):
        """Remove a reminder by ID."""
        removed = await self.remove_reminder(ctx.guild, reminder_id, actor=ctx.author)
        await ctx.send("Admin reminder removed." if removed else "Admin reminder not found.")

    @admintimer.command(name="run")
    async def admintimer_run(self, ctx: commands.Context, reminder_id: int):
        """Post a reminder immediately without changing its schedule."""
        reminder = await self.get_reminder(ctx.guild, reminder_id)
        if not reminder:
            await ctx.send("Admin reminder not found.")
            return
        message = await self.send_reminder_message(ctx.guild, reminder)
        if not message:
            await ctx.send("No admin reminder channel configured.")
            return
        await self.log_timer_action(ctx.guild, "posted", reminder, actor=ctx.author, details="Manual run")
        await ctx.send("Admin reminder posted.")
