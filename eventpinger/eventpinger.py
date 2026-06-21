from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Callable

import discord
from redbot.core import commands

log = logging.getLogger("red.fara.eventpinger")

SOURCE_CHANNEL_ID = 544461383358480385
MISSIONCHIEF_APP_ID = 743939319122886657
NOTIFY_EVENT_ROLE_ID = 669496241591418890

MISSION_PREFIX = "start alliance mission!"
EVENT_PREFIX = "alliance event started!"


US_REGION_NAMES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}

REGION_ROLE_NAMES = {
    **{code: f"{name} ({code})" for code, name in US_REGION_NAMES.items()},
    "BM": "Bermuda (BM)",
}

# ZIP prefixes are intentionally state-level only. Ambiguous or non-state US territories are omitted.
US_ZIP3_RANGES = (
    (10, 27, "MA"),
    (28, 29, "RI"),
    (30, 38, "NH"),
    (39, 49, "ME"),
    (50, 59, "VT"),
    (60, 62, "CT"),
    (64, 69, "CT"),
    (70, 89, "NJ"),
    (100, 149, "NY"),
    (150, 196, "PA"),
    (197, 199, "DE"),
    (200, 200, "DC"),
    (201, 201, "VA"),
    (202, 205, "DC"),
    (206, 219, "MD"),
    (220, 246, "VA"),
    (247, 268, "WV"),
    (270, 289, "NC"),
    (290, 299, "SC"),
    (300, 319, "GA"),
    (320, 349, "FL"),
    (350, 369, "AL"),
    (370, 385, "TN"),
    (386, 397, "MS"),
    (398, 399, "GA"),
    (400, 427, "KY"),
    (430, 459, "OH"),
    (460, 479, "IN"),
    (480, 499, "MI"),
    (500, 528, "IA"),
    (530, 549, "WI"),
    (550, 567, "MN"),
    (570, 577, "SD"),
    (580, 588, "ND"),
    (590, 599, "MT"),
    (600, 629, "IL"),
    (630, 658, "MO"),
    (660, 679, "KS"),
    (680, 693, "NE"),
    (700, 714, "LA"),
    (716, 729, "AR"),
    (730, 749, "OK"),
    (750, 799, "TX"),
    (800, 816, "CO"),
    (820, 831, "WY"),
    (832, 838, "ID"),
    (840, 849, "UT"),
    (850, 865, "AZ"),
    (870, 884, "NM"),
    (885, 885, "TX"),
    (889, 898, "NV"),
    (900, 961, "CA"),
    (967, 968, "HI"),
    (970, 979, "OR"),
    (980, 994, "WA"),
    (995, 999, "AK"),
)

US_PLACE_ALIASES = {
    "new york": "NY",
    "the bronx": "NY",
    "bronx": "NY",
    "manhattan": "NY",
    "brooklyn": "NY",
    "queens": "NY",
    "staten island": "NY",
    "los angeles": "CA",
    "san francisco": "CA",
    "san diego": "CA",
    "miami": "FL",
    "orlando": "FL",
    "tampa": "FL",
    "jacksonville": "FL",
    "houston": "TX",
    "dallas": "TX",
    "austin": "TX",
    "san antonio": "TX",
    "chicago": "IL",
    "philadelphia": "PA",
    "washington dc": "DC",
    "washington, dc": "DC",
    "district of columbia": "DC",
}

BERMUDA_POSTAL_PREFIXES = {"CR", "DV", "FL", "GE", "HA", "HM", "HS", "MA", "PG", "SB", "SN", "WK"}
BERMUDA_PLACE_ALIASES = {
    "bermuda",
    "flatts",
    "hamilton",
    "devonshire",
    "paget",
    "pembroke",
    "sandys",
    "smiths",
    "smith's",
    "southampton",
    "st george",
    "st. george",
    "st georges",
    "st. georges",
    "warwick",
}

ZIP_RE = re.compile(r"(?<!\d)(\d{5})(?:-\d{4})?(?!\d)")
BERMUDA_POSTAL_RE = re.compile(r"\b([A-Z]{2})\s?\d{2}\b", re.IGNORECASE)


@dataclass(frozen=True)
class EventAnnouncement:
    kind: str
    name: str
    address: str


@dataclass(frozen=True)
class RegionMatch:
    code: str
    name: str
    source: str


Resolver = Callable[[str], RegionMatch | None]


def normalize_text(value: str) -> str:
    return " ".join(str(value or "").replace("\n", " ").split())


def extract_announcement_from_message(message: Any) -> EventAnnouncement | None:
    for title, body in iter_message_blocks(message):
        announcement = extract_announcement(title, body)
        if announcement:
            return announcement
    return None


def iter_message_blocks(message: Any):
    for embed in getattr(message, "embeds", []) or []:
        title = normalize_text(getattr(embed, "title", "") or "")
        parts = []
        description = getattr(embed, "description", None)
        if description:
            parts.append(str(description))
        for field in getattr(embed, "fields", []) or []:
            value = getattr(field, "value", None)
            if value is None and isinstance(field, dict):
                value = field.get("value")
            if value:
                parts.append(str(value))
        yield title, "\n".join(parts)

    content = getattr(message, "content", "") or ""
    if content:
        lines = [line.strip() for line in str(content).splitlines() if line.strip()]
        if lines:
            yield lines[0], "\n".join(lines[1:])


def extract_announcement(title: str, body: str) -> EventAnnouncement | None:
    normalized_title = normalize_text(title)
    lowered = normalized_title.casefold()
    if lowered.startswith(MISSION_PREFIX):
        return EventAnnouncement(
            kind="mission",
            name=normalized_title[len(MISSION_PREFIX) :].strip(" -:"),
            address=first_address_line(body),
        )
    if lowered.startswith(EVENT_PREFIX):
        return EventAnnouncement(
            kind="event",
            name=normalized_title[len(EVENT_PREFIX) :].strip(" -:"),
            address=first_address_line(body),
        )
    return None


def first_address_line(body: str) -> str:
    for line in str(body or "").splitlines():
        clean = normalize_text(line)
        if clean:
            return clean
    return ""


def resolve_region(address: str) -> RegionMatch | None:
    text = normalize_text(address)
    if not text:
        return None

    for resolver in (resolve_bermuda, resolve_us):
        match = resolver(text)
        if match:
            return match
    return None


def resolve_bermuda(address: str) -> RegionMatch | None:
    for match in BERMUDA_POSTAL_RE.finditer(address):
        prefix = match.group(1).upper()
        if prefix in BERMUDA_POSTAL_PREFIXES:
            return RegionMatch("BM", REGION_ROLE_NAMES["BM"], "bermuda_postal_code")

    lowered = address.casefold()
    for alias in BERMUDA_PLACE_ALIASES:
        if re.search(rf"\b{re.escape(alias)}\b", lowered):
            return RegionMatch("BM", REGION_ROLE_NAMES["BM"], "bermuda_place")
    return None


def resolve_us(address: str) -> RegionMatch | None:
    zip_match = ZIP_RE.search(address)
    if zip_match:
        state_code = state_from_zip(zip_match.group(1))
        if state_code:
            return RegionMatch(state_code, REGION_ROLE_NAMES[state_code], "us_zip")

    lowered = address.casefold()
    for alias, code in sorted(US_PLACE_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        if re.search(rf"\b{re.escape(alias)}\b", lowered):
            return RegionMatch(code, REGION_ROLE_NAMES[code], "us_place")

    for code, name in US_REGION_NAMES.items():
        if re.search(rf"\b{re.escape(name.casefold())}\b", lowered):
            return RegionMatch(code, REGION_ROLE_NAMES[code], "us_state_name")

    return None


def state_from_zip(zip_code: str) -> str | None:
    try:
        prefix = int(str(zip_code)[:3])
    except (TypeError, ValueError):
        return None

    for start, end, state_code in US_ZIP3_RANGES:
        if start <= prefix <= end:
            return state_code
    return None


def find_role_by_name(guild: Any, expected_name: str):
    expected = expected_name.casefold()
    for role in getattr(guild, "roles", []) or []:
        if getattr(role, "name", "").casefold() == expected:
            return role
    return None


def find_region_role(guild: Any, region_code: str | None):
    if not region_code:
        return None

    expected_name = REGION_ROLE_NAMES.get(region_code)
    if not expected_name:
        return None

    role = find_role_by_name(guild, expected_name)
    if role:
        return role

    suffix = f"({region_code})".casefold()
    for role in getattr(guild, "roles", []) or []:
        if getattr(role, "name", "").casefold().endswith(suffix):
            return role
    return None


def format_notification(
    announcement: EventAnnouncement,
    region: RegionMatch | None,
    notify_role_mention: str,
    region_role_mention: str | None,
) -> str:
    mentions = [notify_role_mention]
    if region_role_mention:
        mentions.append(region_role_mention)

    label = "Alliance mission" if announcement.kind == "mission" else "Alliance event"
    lines = [
        " ".join(mentions),
        f"{label}: {announcement.name or 'Unknown'}",
        f"Location: {announcement.address or 'Unknown'}",
    ]
    if region:
        lines.append(f"Region: {region.name}")
    else:
        lines.append("Region: Unresolved, Notify-Event only")
    return "\n".join(lines)


class EventPinger(commands.Cog):
    """Ping event notification roles when MissionChief announces alliance events."""

    def __init__(self, bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        if not self._is_source_message(message):
            return

        announcement = extract_announcement_from_message(message)
        if announcement is None:
            return

        await self._send_notification(message, announcement)

    def _is_source_message(self, message: Any) -> bool:
        channel = getattr(message, "channel", None)
        author = getattr(message, "author", None)
        return (
            getattr(channel, "id", None) == SOURCE_CHANNEL_ID
            and getattr(author, "id", None) == MISSIONCHIEF_APP_ID
        )

    async def _send_notification(self, message: Any, announcement: EventAnnouncement) -> None:
        guild = getattr(message, "guild", None)
        channel = getattr(message, "channel", None)
        if guild is None or channel is None:
            return

        region = resolve_region(announcement.address)
        notify_role = getattr(guild, "get_role", lambda role_id: None)(NOTIFY_EVENT_ROLE_ID)
        region_role = find_region_role(guild, region.code if region else None)

        notify_mention = getattr(notify_role, "mention", f"<@&{NOTIFY_EVENT_ROLE_ID}>")
        region_mention = getattr(region_role, "mention", None)
        content = format_notification(announcement, region, notify_mention, region_mention)

        kwargs = {}
        if hasattr(discord, "AllowedMentions"):
            kwargs["allowed_mentions"] = discord.AllowedMentions(roles=True, users=False, everyone=False)

        try:
            await channel.send(content, **kwargs)
        except (discord.Forbidden, discord.HTTPException):
            log.exception("Failed to send event notification for MissionChief announcement")

    @commands.group(name="eventpinger")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def eventpinger(self, ctx: commands.Context) -> None:
        """Inspect the hardcoded MissionChief event pinger."""
        pass

    @eventpinger.command(name="status")
    async def eventpinger_status(self, ctx: commands.Context) -> None:
        """Show the hardcoded listener configuration."""
        await ctx.send(
            "eventpinger status\n"
            f"Source channel: {SOURCE_CHANNEL_ID}\n"
            f"MissionChief app ID: {MISSIONCHIEF_APP_ID}\n"
            f"Notify role: {NOTIFY_EVENT_ROLE_ID}\n"
            f"Known region roles: {len(REGION_ROLE_NAMES)}"
        )

    @eventpinger.command(name="resolve")
    async def eventpinger_resolve(self, ctx: commands.Context, *, address: str) -> None:
        """Test address resolution without sending role pings."""
        match = resolve_region(address)
        if not match:
            await ctx.send("No confident region match. Only Notify-Event would be used.")
            return
        role = find_region_role(ctx.guild, match.code)
        await ctx.send(
            f"Resolved: {match.name}\n"
            f"Code: {match.code}\n"
            f"Source: {match.source}\n"
            f"Role: {getattr(role, 'mention', 'not found')}"
        )
