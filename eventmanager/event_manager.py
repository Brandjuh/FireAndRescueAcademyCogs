from __future__ import annotations

import asyncio
import io
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import discord
from redbot.core import Config, commands
from redbot.core.utils.chat_formatting import box

log = logging.getLogger("red.cog.eventmanager")

BASE_URL = "https://www.missionchief.com"
EVENT_KINDS = {
    "large": {
        "label": "Large scale alliance mission",
        "url": f"{BASE_URL}/missionAllianceNew",
        "schedule": "daily",
    },
    "event": {
        "label": "Alliance event",
        "url": f"{BASE_URL}/missionAllianceEventNew",
        "schedule": "weekly",
    },
}
DEFAULT_TIMEZONE = "America/New_York"
WEEKDAYS = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}


@dataclass
class FormOption:
    value: str
    label: str
    selected: bool = False
    field_type: str = ""


@dataclass
class FormField:
    name: str
    tag: str
    field_type: str = ""
    value: str = ""
    required: bool = False
    options: List[FormOption] = field(default_factory=list)


@dataclass
class EventForm:
    action: str
    method: str
    fields: List[FormField]
    submit_name: Optional[str] = None
    submit_value: Optional[str] = None


@dataclass
class EventStartResult:
    ok: bool
    reason: str
    status: Optional[int] = None
    post_url: Optional[str] = None


Payload = List[Tuple[str, str]]


def normalize_kind(kind: str) -> str:
    normalized = (kind or "").strip().lower()
    aliases = {
        "large_mission": "large",
        "large-mission": "large",
        "mission": "large",
        "alliance_mission": "large",
        "alliance_event": "event",
        "weekly": "event",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in EVENT_KINDS:
        raise ValueError("Use `large` or `event`.")
    return normalized


def _text(element) -> str:
    return " ".join(element.get_text(" ", strip=True).split())


def _inside_custom_mission_creator(element) -> bool:
    return element.find_parent(id="custom_mission_creator") is not None


def _input_option_label(input_el, fallback: str) -> str:
    parent_label = input_el.find_parent("label")
    if parent_label:
        label = _text(parent_label)
        if label:
            return label
    return input_el.get("title") or input_el.get("aria-label") or fallback


def _input_option_value(input_el) -> str:
    return input_el.get("value") or input_el.get("data-event-id") or ""


def parse_event_form(html: str, page_url: str) -> EventForm:
    """Parse the MissionChief alliance mission/event form."""
    soup = BeautifulSoup(html, "html.parser")
    forms = soup.find_all("form")
    if not forms:
        raise ValueError("No form found on the MissionChief page.")

    form = None
    for candidate in forms:
        if candidate.find("input", attrs={"name": "authenticity_token"}):
            form = candidate
            break
    form = form or forms[0]

    action = form.get("action") or page_url
    method = (form.get("method") or "post").lower()
    fields: List[FormField] = []
    submit_name = None
    submit_value = None

    grouped_inputs: Dict[str, List] = {}
    for input_el in form.find_all("input"):
        if _inside_custom_mission_creator(input_el):
            continue
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
        if field_type in {"radio", "checkbox"}:
            grouped_inputs.setdefault(name, []).append(input_el)
            continue
        fields.append(
            FormField(
                name=name,
                tag="input",
                field_type=field_type,
                value=input_el.get("value") or "",
                required=input_el.has_attr("required"),
            )
        )

    for name, input_group in grouped_inputs.items():
        options: List[FormOption] = []
        selected_values: List[str] = []
        field_type = (input_group[0].get("type") or "").lower()
        for index, input_el in enumerate(input_group, start=1):
            value = _input_option_value(input_el)
            if name == "mission_position[mission_type_id]" and value == "-1":
                continue
            option = FormOption(
                value=value,
                label=_input_option_label(input_el, f"option {index}"),
                selected=input_el.has_attr("checked"),
                field_type=field_type,
            )
            options.append(option)
            if option.selected:
                selected_values.append(value)

        if not options:
            continue

        if field_type == "radio":
            field_value = selected_values[0] if selected_values else ""
        else:
            field_value = ",".join(selected_values)

        fields.append(
            FormField(
                name=name,
                tag="input",
                field_type=field_type,
                value=field_value,
                required=any(input_el.has_attr("required") for input_el in input_group),
                options=options,
            )
        )

    for select_el in form.find_all("select"):
        if _inside_custom_mission_creator(select_el):
            continue
        name = select_el.get("name")
        if not name:
            continue
        options: List[FormOption] = []
        selected_value = ""
        for option_el in select_el.find_all("option"):
            option = FormOption(
                value=option_el.get("value") or "",
                label=_text(option_el),
                selected=option_el.has_attr("selected"),
            )
            options.append(option)
            if option.selected:
                selected_value = option.value
        if not selected_value and options:
            selected_value = options[0].value
        fields.append(
            FormField(
                name=name,
                tag="select",
                value=selected_value,
                required=select_el.has_attr("required"),
                options=options,
            )
        )

    for textarea in form.find_all("textarea"):
        if _inside_custom_mission_creator(textarea):
            continue
        name = textarea.get("name")
        if not name:
            continue
        fields.append(
            FormField(
                name=name,
                tag="textarea",
                value=textarea.get_text() or "",
                required=textarea.has_attr("required"),
            )
        )

    return EventForm(
        action=urljoin(page_url, action),
        method=method,
        fields=fields,
        submit_name=submit_name,
        submit_value=submit_value,
    )


def _append_payload_value(payload: Payload, name: str, value: str):
    payload.append((name, str(value or "")))


def _normalize_overrides(form: EventForm, overrides: Dict[str, str]) -> Dict[str, str]:
    normalized = {str(key): str(value) for key, value in overrides.items()}
    field_names = {field_info.name for field_info in form.fields}
    mission_type_name = "mission_position[mission_type_id]"
    event_radio_name = "event_radio_group"

    if event_radio_name in field_names:
        if event_radio_name in normalized and mission_type_name not in normalized:
            normalized[mission_type_name] = normalized[event_radio_name]
        elif mission_type_name in normalized and event_radio_name not in normalized:
            normalized[event_radio_name] = normalized[mission_type_name]

    return normalized


def build_payload(form: EventForm, overrides: Dict[str, str]) -> Payload:
    """Build a POST payload from form defaults plus configured overrides."""
    overrides = _normalize_overrides(form, overrides)
    payload: Payload = []
    used_names = set()
    for field_info in form.fields:
        override_present = field_info.name in overrides
        value = str(overrides[field_info.name] if override_present else field_info.value or "")

        if field_info.field_type == "checkbox":
            selected_values = {
                item.strip()
                for item in value.split(",")
                if item.strip()
            }
            for option in field_info.options:
                if option.value in selected_values:
                    _append_payload_value(payload, field_info.name, option.value)
        elif field_info.field_type == "radio":
            if value:
                _append_payload_value(payload, field_info.name, value)
        else:
            _append_payload_value(payload, field_info.name, value)
        used_names.add(field_info.name)

    for key, value in overrides.items():
        if key not in used_names:
            _append_payload_value(payload, str(key), str(value))

    if form.submit_name and form.submit_name not in used_names and not any(name == form.submit_name for name, _ in payload):
        _append_payload_value(payload, form.submit_name, form.submit_value or "")
    return payload


def summarize_form(form: EventForm, *, limit: int = 15) -> str:
    """Return a compact admin-facing form summary."""
    lines = [
        f"Action: {form.action}",
        f"Method: {form.method.upper()}",
        f"Fields: {len(form.fields)}",
    ]
    if form.submit_name:
        lines.append(f"Submit: {form.submit_name}={form.submit_value or ''}")

    for field_info in form.fields[:limit]:
        required = " required" if field_info.required else ""
        field_type = f":{field_info.field_type}" if field_info.field_type else ""
        value = f" = {field_info.value}" if field_info.value else ""
        lines.append(f"- {field_info.name} ({field_info.tag}{field_type}{required}){value}")
        if field_info.options:
            option_preview = ", ".join(
                f"{'*' if option.selected else ''}{option.value}:{option.label}" for option in field_info.options[:5]
            )
            lines.append(f"  options: {option_preview}")
    if len(form.fields) > limit:
        lines.append(f"... {len(form.fields) - limit} more fields")
    return "\n".join(lines)


def valid_time(value: str) -> Tuple[int, int]:
    try:
        hour_text, minute_text = value.strip().split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except Exception as exc:
        raise ValueError("Time must use HH:MM format.") from exc
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError("Time must use HH:MM format.")
    return hour, minute


def parse_profile_names(value: str) -> List[str]:
    """Parse comma or whitespace separated profile names."""
    names = [
        item.strip().lower()
        for chunk in (value or "").split(",")
        for item in chunk.split()
        if item.strip()
    ]
    if not names:
        raise ValueError("At least one profile name is required.")
    return names


def select_scheduled_profile(schedule: dict) -> Tuple[Optional[str], int]:
    """Return the profile for this run and the next rotation index."""
    profiles = schedule.get("profiles") or []
    if not profiles and schedule.get("profile"):
        profiles = [schedule["profile"]]
    profiles = [str(profile).strip().lower() for profile in profiles if str(profile).strip()]
    if not profiles:
        return None, int(schedule.get("rotation_index") or 0)

    index = int(schedule.get("rotation_index") or 0)
    profile = profiles[index % len(profiles)]
    next_index = (index + 1) % len(profiles)
    return profile, next_index


class EventManager(commands.Cog):
    """Start and schedule MissionChief alliance missions and alliance events."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFADAEE01, force_registration=True)
        self.config.register_global(
            profiles={"large": {}, "event": {}},
            schedules={
                "large": {
                    "enabled": False,
                    "profile": None,
                    "profiles": [],
                    "rotation_index": 0,
                    "time": "23:55",
                    "timezone": DEFAULT_TIMEZONE,
                    "weekday": None,
                },
                "event": {
                    "enabled": False,
                    "profile": None,
                    "profiles": [],
                    "rotation_index": 0,
                    "time": "23:55",
                    "timezone": DEFAULT_TIMEZONE,
                    "weekday": "monday",
                },
            },
            last_runs={},
            log_channel_id=None,
        )
        self._task: Optional[asyncio.Task] = None
        self._start_lock = asyncio.Lock()

    async def cog_load(self):
        self._task = asyncio.create_task(self._scheduler_loop())

    async def cog_unload(self):
        if self._task:
            self._task.cancel()

    async def _scheduler_loop(self):
        await self.bot.wait_until_ready()
        while True:
            try:
                await self._run_due_schedules()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("EventManager scheduler failed")
            await asyncio.sleep(60)

    def _cookie_manager(self):
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager or not hasattr(cookie_manager, "get_session"):
            return None
        return cookie_manager

    async def _get_session(self):
        cookie_manager = self._cookie_manager()
        if not cookie_manager:
            raise RuntimeError("CookieManager is not loaded.")
        session = await cookie_manager.get_session()
        if not session:
            raise RuntimeError("CookieManager did not return a session.")
        return session

    async def _fetch_form(self, kind: str) -> EventForm:
        kind = normalize_kind(kind)
        page_url = EVENT_KINDS[kind]["url"]
        session = await self._get_session()
        async with session.get(page_url, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            raise RuntimeError(f"MissionChief returned HTTP {status}.")
        return parse_event_form(html, page_url)

    async def _start_from_profile(self, kind: str, profile_name: str) -> EventStartResult:
        kind = normalize_kind(kind)
        profile_name = profile_name.strip().lower()
        profiles = await self.config.profiles()
        profile = profiles.get(kind, {}).get(profile_name)
        if not profile:
            return EventStartResult(False, f"Profile `{profile_name}` was not found.")

        async with self._start_lock:
            try:
                form = await self._fetch_form(kind)
            except Exception as exc:
                return EventStartResult(False, f"Could not fetch form: {exc}")

            if form.method != "post":
                return EventStartResult(False, f"Unexpected form method `{form.method}`.")

            payload = build_payload(form, profile.get("fields", {}))
            session = await self._get_session()
            try:
                async with session.post(form.action, data=payload, allow_redirects=True) as response:
                    status = getattr(response, "status", None)
                    await response.text()
            except Exception as exc:
                return EventStartResult(False, f"MissionChief POST failed: {exc}", post_url=form.action)

        if status is None or int(status) >= 400:
            return EventStartResult(False, f"MissionChief returned HTTP {status}.", status=status, post_url=form.action)

        await self._log_run(kind, profile_name, status)
        return EventStartResult(True, "Started successfully.", status=status, post_url=form.action)

    async def _log_run(self, kind: str, profile_name: str, status: Optional[int]):
        channel_id = await self.config.log_channel_id()
        if not channel_id:
            return
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return
        embed = discord.Embed(
            title="EventManager started an alliance item",
            color=discord.Color.green(),
            timestamp=datetime.utcnow(),
        )
        embed.add_field(name="Type", value=EVENT_KINDS[kind]["label"], inline=True)
        embed.add_field(name="Profile", value=profile_name, inline=True)
        embed.add_field(name="HTTP Status", value=str(status), inline=True)
        await channel.send(embed=embed)

    async def _run_due_schedules(self):
        schedules = await self.config.schedules()
        last_runs = await self.config.last_runs()
        changed = False

        for kind, schedule in schedules.items():
            if not schedule.get("enabled"):
                continue
            profile_name, next_rotation_index = select_scheduled_profile(schedule)
            if not profile_name:
                continue
            timezone_name = schedule.get("timezone") or DEFAULT_TIMEZONE
            now = datetime.now(ZoneInfo(timezone_name))
            hour, minute = valid_time(schedule.get("time") or "23:55")
            if (now.hour, now.minute) < (hour, minute):
                continue

            if kind == "event":
                weekday = (schedule.get("weekday") or "monday").lower()
                if now.weekday() != WEEKDAYS.get(weekday, 0):
                    continue
                run_key = f"{now:%G-W%V}"
            else:
                run_key = now.strftime("%Y-%m-%d")

            if last_runs.get(kind) == run_key:
                continue

            result = await self._start_from_profile(kind, profile_name)
            if result.ok:
                last_runs[kind] = run_key
                schedule["profile"] = profile_name
                schedule["rotation_index"] = next_rotation_index
                changed = True
            else:
                log.warning("Scheduled %s failed: %s", kind, result.reason)

        if changed:
            await self.config.last_runs.set(last_runs)

    @commands.group(name="eventmanager", aliases=["eventmgr"], invoke_without_command=True)
    @commands.admin()
    async def eventmanager(self, ctx: commands.Context):
        """Manage MissionChief alliance missions and alliance events."""
        await ctx.send_help()

    @eventmanager.command(name="inspect")
    @commands.admin()
    async def inspect_form(self, ctx: commands.Context, kind: str, limit: int = 20):
        """Inspect the live MissionChief form for `large` or `event`."""
        try:
            kind = normalize_kind(kind)
            form = await self._fetch_form(kind)
        except Exception as exc:
            await ctx.send(f"Could not inspect form: {exc}")
            return
        limit = max(1, min(int(limit), 40))
        await ctx.send(box(summarize_form(form, limit=limit), lang="ini"))

    @eventmanager.command(name="inspectfile")
    @commands.admin()
    async def inspect_form_file(self, ctx: commands.Context, kind: str):
        """Send the complete live MissionChief form inspection as a text file."""
        try:
            kind = normalize_kind(kind)
            form = await self._fetch_form(kind)
        except Exception as exc:
            await ctx.send(f"Could not inspect form: {exc}")
            return

        summary = summarize_form(form, limit=len(form.fields))
        data = io.BytesIO(summary.encode("utf-8"))
        await ctx.send(
            f"Full {EVENT_KINDS[kind]['label']} form inspection:",
            file=discord.File(data, filename=f"eventmanager-{kind}-form.txt"),
        )

    @eventmanager.command(name="start")
    @commands.admin()
    async def start_profile(self, ctx: commands.Context, kind: str, profile_name: str):
        """Start a large alliance mission or alliance event using a saved profile."""
        try:
            kind = normalize_kind(kind)
        except ValueError as exc:
            await ctx.send(str(exc))
            return
        result = await self._start_from_profile(kind, profile_name)
        if result.ok:
            await ctx.send(f"Started {EVENT_KINDS[kind]['label']} with profile `{profile_name}`.")
        else:
            await ctx.send(f"Could not start {EVENT_KINDS[kind]['label']}: {result.reason}")

    @eventmanager.group(name="profile", invoke_without_command=True)
    @commands.admin()
    async def profile(self, ctx: commands.Context):
        """Manage start profiles."""
        await ctx.send_help()

    @profile.command(name="set")
    @commands.admin()
    async def profile_set(self, ctx: commands.Context, kind: str, profile_name: str, field_name: str, *, value: str):
        """Set one MissionChief form field override for a profile."""
        try:
            kind = normalize_kind(kind)
        except ValueError as exc:
            await ctx.send(str(exc))
            return
        profile_name = profile_name.strip().lower()
        async with self.config.profiles() as profiles:
            profile = profiles.setdefault(kind, {}).setdefault(profile_name, {"fields": {}})
            profile.setdefault("fields", {})[field_name] = value
        await ctx.tick()

    @profile.command(name="remove")
    @commands.admin()
    async def profile_remove(self, ctx: commands.Context, kind: str, profile_name: str, field_name: str):
        """Remove one field override from a profile."""
        try:
            kind = normalize_kind(kind)
        except ValueError as exc:
            await ctx.send(str(exc))
            return
        profile_name = profile_name.strip().lower()
        async with self.config.profiles() as profiles:
            profile = profiles.get(kind, {}).get(profile_name)
            if not profile or field_name not in profile.get("fields", {}):
                await ctx.send("That field was not configured.")
                return
            del profile["fields"][field_name]
        await ctx.tick()

    @profile.command(name="delete")
    @commands.admin()
    async def profile_delete(self, ctx: commands.Context, kind: str, profile_name: str):
        """Delete a profile."""
        try:
            kind = normalize_kind(kind)
        except ValueError as exc:
            await ctx.send(str(exc))
            return
        profile_name = profile_name.strip().lower()
        async with self.config.profiles() as profiles:
            if profile_name not in profiles.get(kind, {}):
                await ctx.send("Profile not found.")
                return
            del profiles[kind][profile_name]
        await ctx.tick()

    @profile.command(name="show")
    @commands.admin()
    async def profile_show(self, ctx: commands.Context, kind: str, profile_name: str):
        """Show one configured profile."""
        try:
            kind = normalize_kind(kind)
        except ValueError as exc:
            await ctx.send(str(exc))
            return
        profiles = await self.config.profiles()
        profile = profiles.get(kind, {}).get(profile_name.strip().lower())
        if not profile:
            await ctx.send("Profile not found.")
            return
        lines = [f"{kind}/{profile_name.strip().lower()}"]
        for field_name, value in sorted(profile.get("fields", {}).items()):
            lines.append(f"{field_name} = {value}")
        await ctx.send(box("\n".join(lines), lang="ini"))

    @profile.command(name="list")
    @commands.admin()
    async def profile_list(self, ctx: commands.Context):
        """List configured profiles."""
        profiles = await self.config.profiles()
        lines = []
        for kind in ("large", "event"):
            names = sorted(profiles.get(kind, {}).keys())
            lines.append(f"{kind}: {', '.join(names) if names else 'none'}")
        await ctx.send(box("\n".join(lines), lang="ini"))

    @eventmanager.group(name="schedule", invoke_without_command=True)
    @commands.admin()
    async def schedule(self, ctx: commands.Context):
        """Manage automatic schedules."""
        schedules = await self.config.schedules()
        lines = []
        for kind, schedule in schedules.items():
            profiles = schedule.get("profiles") or [schedule.get("profile")]
            profiles = [profile for profile in profiles if profile]
            lines.append(
                f"{kind}: enabled={schedule.get('enabled')} profiles={profiles or 'none'} "
                f"time={schedule.get('time')} timezone={schedule.get('timezone')} "
                f"weekday={schedule.get('weekday')} rotation_index={schedule.get('rotation_index', 0)}"
            )
        await ctx.send(box("\n".join(lines), lang="ini"))

    @schedule.command(name="daily")
    @commands.admin()
    async def schedule_daily(self, ctx: commands.Context, time: str, *, profiles: str):
        """Schedule the free daily large alliance mission with rotating profiles."""
        try:
            valid_time(time)
            profile_names = parse_profile_names(profiles)
        except ValueError as exc:
            await ctx.send(str(exc))
            return
        async with self.config.schedules() as schedules:
            schedules["large"].update(
                enabled=True,
                profile=profile_names[0],
                profiles=profile_names,
                rotation_index=0,
                time=time,
                timezone=DEFAULT_TIMEZONE,
                weekday=None,
            )
        await ctx.tick()

    @schedule.command(name="weekly")
    @commands.admin()
    async def schedule_weekly(self, ctx: commands.Context, weekday: str, time: str, *, profiles: str):
        """Schedule the free weekly alliance event with rotating profiles."""
        weekday = weekday.strip().lower()
        if weekday not in WEEKDAYS:
            await ctx.send(f"Weekday must be one of: {', '.join(WEEKDAYS)}")
            return
        try:
            valid_time(time)
            profile_names = parse_profile_names(profiles)
        except ValueError as exc:
            await ctx.send(str(exc))
            return
        async with self.config.schedules() as schedules:
            schedules["event"].update(
                enabled=True,
                profile=profile_names[0],
                profiles=profile_names,
                rotation_index=0,
                time=time,
                timezone=DEFAULT_TIMEZONE,
                weekday=weekday,
            )
        await ctx.tick()

    @schedule.command(name="off")
    @commands.admin()
    async def schedule_off(self, ctx: commands.Context, kind: str):
        """Disable one automatic schedule."""
        try:
            kind = normalize_kind(kind)
        except ValueError as exc:
            await ctx.send(str(exc))
            return
        async with self.config.schedules() as schedules:
            schedules[kind]["enabled"] = False
        await ctx.tick()

    @eventmanager.command(name="logchannel")
    @commands.admin()
    async def logchannel(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        """Set or clear the EventManager log channel."""
        if channel is None:
            await self.config.log_channel_id.set(None)
            await ctx.send("EventManager log channel cleared.")
            return
        await self.config.log_channel_id.set(channel.id)
        await ctx.tick()
