from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

import aiohttp
import discord
from redbot.core import Config, commands

log = logging.getLogger("red.fara.eventpinger")

SOURCE_CHANNEL_ID = 544461383358480385
MISSIONCHIEF_APP_ID = 743939319122886657
NOTIFY_EVENT_ROLE_ID = 669496241591418890

MISSION_PREFIX = "start alliance mission!"
EVENT_PREFIX = "alliance event started!"
GEOCODE_SEARCH_URL = "https://geocode.maps.co/search"
GEOCODE_TIMEOUT_SECONDS = 5
GEOCODE_CACHE_TTL_SECONDS = 90 * 24 * 60 * 60
GEOCODE_MIN_INTERVAL_SECONDS = 1.1


def normalize_geocode_api_key(api_key: str) -> str:
    """Return the raw geocode.maps.co key, accepting values pasted with a Bearer prefix."""
    key = str(api_key or "").strip().strip("'\"")
    if key.casefold().startswith("bearer "):
        key = key[7:].strip().strip("'\"")
    return key


def geocode_search_params(address: str, api_key: str) -> dict[str, str]:
    """Build geocode.maps.co search params with the documented API key parameter."""
    key = normalize_geocode_api_key(api_key)
    params = {
        "q": address,
        "format": "json",
        "addressdetails": "1",
        "limit": "3",
        "accept-language": "en",
    }
    if key:
        params["api_key"] = key
    return params


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
US_STATE_NAME_TO_CODE = {name.casefold(): code for code, name in US_REGION_NAMES.items()}

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
    role_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class GeocodeOutcome:
    match: RegionMatch | None
    authoritative: bool = False


Resolver = Callable[[str], RegionMatch | None]


def normalize_text(value: str) -> str:
    return " ".join(str(value or "").replace("\n", " ").split())


def cache_key_for_address(address: str) -> str:
    return normalize_text(address).casefold()


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


def region_from_geocode_results(results: Any) -> RegionMatch | None:
    return geocode_outcome_from_results(results).match


def geocode_outcome_from_results(results: Any) -> GeocodeOutcome:
    if not isinstance(results, list):
        return GeocodeOutcome(None, authoritative=False)

    saw_non_us_country = False
    for result in results[:3]:
        match = region_from_geocode_result(result)
        if match:
            return GeocodeOutcome(match, authoritative=True)

        country_code = country_code_from_geocode_result(result)
        if country_code and country_code not in {"us", "bm"}:
            saw_non_us_country = True

    if saw_non_us_country:
        return GeocodeOutcome(None, authoritative=True)
    return GeocodeOutcome(None, authoritative=False)


def region_from_geocode_result(result: Any) -> RegionMatch | None:
    if not isinstance(result, dict):
        return None

    address = result.get("address")
    if not isinstance(address, dict):
        return None

    country_code = normalize_text(address.get("country_code", "")).casefold()
    country_name = normalize_text(address.get("country", ""))
    country = country_name.casefold()
    if country_code == "bm" or country == "bermuda":
        return RegionMatch("BM", REGION_ROLE_NAMES["BM"], "geocode_country")

    if country_code == "us" or country == "united states":
        state_name = normalize_text(address.get("state", "")).casefold()
        state_code = normalize_text(address.get("state_code", "")).upper()
        if state_code in REGION_ROLE_NAMES:
            return RegionMatch(state_code, REGION_ROLE_NAMES[state_code], "geocode_state_code")

        resolved_code = US_STATE_NAME_TO_CODE.get(state_name)
        if resolved_code:
            return RegionMatch(resolved_code, REGION_ROLE_NAMES[resolved_code], "geocode_state")

        return None

    if country_code or country_name:
        return country_region_match(country_name, country_code, "geocode_country")

    return None


def country_code_from_geocode_result(result: Any) -> str:
    if not isinstance(result, dict):
        return ""

    address = result.get("address")
    if not isinstance(address, dict):
        return ""

    country_code = normalize_text(address.get("country_code", "")).casefold()
    if country_code:
        return country_code

    country = normalize_text(address.get("country", "")).casefold()
    if country == "bermuda":
        return "bm"
    if country == "united states":
        return "us"
    return country


def country_region_match(country_name: str, country_code: str, source: str) -> RegionMatch | None:
    clean_country = normalize_text(country_name)
    clean_code = normalize_text(country_code).upper()
    if not clean_country and not clean_code:
        return None

    display_name = f"{clean_country} ({clean_code})" if clean_country and clean_code else clean_country or clean_code
    role_names = tuple(dict.fromkeys(name for name in (display_name, clean_country) if name))
    code = f"COUNTRY:{clean_code}" if clean_code else f"COUNTRY:{display_name.casefold()}"
    return RegionMatch(code, display_name, source, role_names)


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
    has_context = has_us_context(address)
    zip_match = ZIP_RE.search(address)
    if zip_match and has_context:
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


def has_us_context(address: str) -> bool:
    lowered = address.casefold()
    if re.search(r"\b(?:united states|usa|u\.s\.a\.|u\.s\.)\b", lowered):
        return True

    for alias in US_PLACE_ALIASES:
        if re.search(rf"\b{re.escape(alias)}\b", lowered):
            return True

    for name in US_REGION_NAMES.values():
        if re.search(rf"\b{re.escape(name.casefold())}\b", lowered):
            return True

    return False


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


def find_region_role(guild: Any, region: RegionMatch | str | None):
    if not region:
        return None

    if isinstance(region, RegionMatch):
        region_code = region.code
        candidates = list(region.role_names)
        if region_code in REGION_ROLE_NAMES:
            candidates.append(REGION_ROLE_NAMES[region_code])
        candidates.append(region.name)
    else:
        region_code = region
        candidates = [REGION_ROLE_NAMES.get(region_code, "")]

    seen = set()
    for expected_name in candidates:
        normalized = normalize_text(expected_name)
        if not normalized or normalized.casefold() in seen:
            continue
        seen.add(normalized.casefold())

        role = find_role_by_name(guild, normalized)
        if role:
            return role

    if region_code not in REGION_ROLE_NAMES:
        return None

    suffix = f"({region_code})".casefold()
    for role in getattr(guild, "roles", []) or []:
        if getattr(role, "name", "").casefold().endswith(suffix):
            return role
    return None


def format_notification_mentions(
    notify_role_mention: str,
    region_role_mention: str | None,
) -> str:
    mentions = [notify_role_mention]
    if region_role_mention:
        mentions.append(region_role_mention)
    return " ".join(mentions)


def announcement_label(kind: str) -> str:
    return "Alliance Mission" if kind == "mission" else "Alliance Event"


def default_next_type(kind: str) -> str:
    return "Surprise mission" if kind == "mission" else "Surprise event"


def parse_next_summary(summary: str | None, kind: str) -> dict[str, str]:
    details: dict[str, str] = {}
    for raw_line in str(summary or "").splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        normalized_key = key.strip().casefold()
        if normalized_key in {"location", "type"}:
            details[normalized_key] = value.strip()
    if summary and "type" not in details:
        details["type"] = default_next_type(kind)
    return details


def discord_timestamp(value: Any) -> str:
    if not value:
        return "Unknown"
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return str(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    unix_timestamp = int(parsed.timestamp())
    return f"<t:{unix_timestamp}:F>"


def build_notification_embed(
    announcement: EventAnnouncement,
    region: RegionMatch | None,
    next_details: dict[str, Any] | None = None,
) -> discord.Embed:
    label = announcement_label(announcement.kind)
    embed = discord.Embed(
        title=f"MissionChief {label}",
        color=discord.Color.orange() if announcement.kind == "event" else discord.Color.blue(),
    )
    embed.add_field(name=label, value=announcement.name or "Unknown", inline=False)
    embed.add_field(name="Location", value=announcement.address or "Unknown", inline=False)
    embed.add_field(
        name="Region",
        value=region.name if region else "Unresolved, Notify-Event only",
        inline=False,
    )

    if next_details:
        next_label = f"Next {label}"
        next_location = str(next_details.get("location") or "").strip() or "Unknown"
        next_type = str(next_details.get("type") or "").strip() or default_next_type(announcement.kind)
        scheduled_time = discord_timestamp(next_details.get("scheduled_at"))
        embed.add_field(
            name=next_label,
            value="\n".join(
                [
                    f"Location: {next_location}",
                    f"Type: {next_type}",
                    f"Scheduled time: {scheduled_time}",
                ]
            ),
            inline=False,
        )
    return embed


class EventPinger(commands.Cog):
    """Ping event notification roles when MissionChief announces alliance events."""

    def __init__(self, bot):
        self.bot = bot
        self.config = None
        if hasattr(Config, "get_conf"):
            self.config = Config.get_conf(self, identifier=0xFA20260622, force_registration=True)
            self.config.register_global(
                geocode_api_key="",
                geocode_enabled=True,
                geocode_cache={},
            )
        self._session: aiohttp.ClientSession | None = None
        self._memory_cache: dict[str, dict[str, Any]] = {}
        self._geocode_lock = asyncio.Lock()
        self._last_geocode_at = 0.0

    async def cog_unload(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

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

        region = await self.resolve_region_for_address(announcement.address)
        notify_role = getattr(guild, "get_role", lambda role_id: None)(NOTIFY_EVENT_ROLE_ID)
        region_role = find_region_role(guild, region)

        notify_mention = getattr(notify_role, "mention", f"<@&{NOTIFY_EVENT_ROLE_ID}>")
        region_mention = getattr(region_role, "mention", None)
        next_details = await self._eventmanager_next_details(announcement)
        content = format_notification_mentions(notify_mention, region_mention)
        embed = build_notification_embed(announcement, region, next_details)

        kwargs = {}
        if hasattr(discord, "AllowedMentions"):
            kwargs["allowed_mentions"] = discord.AllowedMentions(roles=True, users=False, everyone=False)

        try:
            await channel.send(content, embed=embed, **kwargs)
        except (discord.Forbidden, discord.HTTPException):
            log.exception("Failed to send event notification for MissionChief announcement")

    async def _eventmanager_next_summary(self, announcement: EventAnnouncement) -> str | None:
        """Read EventManager's next route summary when that cog started this item."""
        details = await self._eventmanager_next_details(announcement)
        summary = str((details or {}).get("summary") or "").strip()
        return summary or None

    async def _eventmanager_next_details(self, announcement: EventAnnouncement) -> dict[str, Any] | None:
        """Read EventManager's next route details when that cog started this item."""
        get_cog = getattr(self.bot, "get_cog", None)
        if not callable(get_cog):
            return None
        event_manager = get_cog("EventManager")
        if not event_manager:
            return None
        kind = "large" if announcement.kind == "mission" else "event"

        detail_method = getattr(event_manager, "get_next_notification_details", None)
        if callable(detail_method):
            try:
                result = detail_method(kind)
                if asyncio.iscoroutine(result):
                    result = await result
            except Exception:
                log.exception("Could not read EventManager next notification details")
                return None
            if isinstance(result, dict):
                details = dict(result)
                if not details.get("location") or not details.get("type"):
                    parsed = parse_next_summary(details.get("summary"), announcement.kind)
                    details.setdefault("location", parsed.get("location", ""))
                    details.setdefault("type", parsed.get("type", ""))
                return details

        summary_method = getattr(event_manager, "get_next_notification_summary", None)
        if not callable(summary_method):
            return None
        try:
            result = summary_method(kind)
            if asyncio.iscoroutine(result):
                result = await result
        except Exception:
            log.exception("Could not read EventManager next notification summary")
            return None
        summary = str(result or "").strip()
        if not summary:
            return None
        details = parse_next_summary(summary, announcement.kind)
        details["summary"] = summary
        return details

    async def resolve_region_for_address(self, address: str) -> RegionMatch | None:
        geocode_outcome = await self._resolve_region_with_geocode(address)
        if geocode_outcome:
            if geocode_outcome.match:
                return geocode_outcome.match
            if geocode_outcome.authoritative:
                return None
        return resolve_region(address)

    async def _resolve_region_with_geocode(self, address: str) -> GeocodeOutcome | None:
        text = normalize_text(address)
        if not text:
            return None

        api_key = await self._get_geocode_api_key()
        if not api_key or not await self._geocode_enabled():
            return None

        cached = await self._get_cached_geocode_outcome(text)
        if cached:
            return cached

        try:
            results = await self._fetch_geocode_results(text, api_key)
        except Exception:
            log.exception("Geocode lookup failed for event address")
            return None

        outcome = geocode_outcome_from_results(results)
        if outcome.match:
            await self._set_cached_geocode_outcome(text, outcome)
        return outcome

    async def _get_geocode_api_key(self) -> str:
        if self.config is None:
            return ""
        return str(await self.config.geocode_api_key() or "").strip()

    async def _geocode_enabled(self) -> bool:
        if self.config is None:
            return False
        return bool(await self.config.geocode_enabled())

    async def _get_cached_geocode_outcome(self, address: str) -> GeocodeOutcome | None:
        key = cache_key_for_address(address)
        now = int(time.time())
        cache = await self._read_cache()
        entry = cache.get(key)
        if not isinstance(entry, dict):
            return None

        if int(entry.get("expires_at", 0) or 0) <= now:
            await self._delete_cached_region(key)
            return None

        code = str(entry.get("code") or "")
        name = str(entry.get("name") or REGION_ROLE_NAMES.get(code, ""))
        source = str(entry.get("source") or "geocode_cache")
        role_names = tuple(str(name) for name in entry.get("role_names", []) or [] if str(name))
        if not code or not name:
            return None
        return GeocodeOutcome(RegionMatch(code, name, source, role_names), authoritative=True)

    async def _set_cached_geocode_outcome(self, address: str, outcome: GeocodeOutcome) -> None:
        key = cache_key_for_address(address)
        if outcome.match:
            entry = {
                "code": outcome.match.code,
                "name": outcome.match.name,
                "source": f"{outcome.match.source}_cache",
                "role_names": list(outcome.match.role_names),
                "expires_at": int(time.time()) + GEOCODE_CACHE_TTL_SECONDS,
            }
        else:
            return
        if self.config is None:
            self._memory_cache[key] = entry
            return

        async with self.config.geocode_cache() as cache:
            cache[key] = entry

    async def _delete_cached_region(self, key: str) -> None:
        if self.config is None:
            self._memory_cache.pop(key, None)
            return

        async with self.config.geocode_cache() as cache:
            cache.pop(key, None)

    async def _read_cache(self) -> dict[str, Any]:
        if self.config is None:
            return dict(self._memory_cache)
        return dict(await self.config.geocode_cache() or {})

    async def _fetch_geocode_results(self, address: str, api_key: str) -> Any:
        async with self._geocode_lock:
            elapsed = time.monotonic() - self._last_geocode_at
            if elapsed < GEOCODE_MIN_INTERVAL_SECONDS:
                await asyncio.sleep(GEOCODE_MIN_INTERVAL_SECONDS - elapsed)

            session = await self._get_session()
            params = geocode_search_params(address, api_key)
            timeout = aiohttp.ClientTimeout(total=GEOCODE_TIMEOUT_SECONDS)
            async with session.get(
                GEOCODE_SEARCH_URL,
                params=params,
                timeout=timeout,
            ) as response:
                self._last_geocode_at = time.monotonic()
                if int(response.status) >= 400:
                    if int(response.status) == 401:
                        raise RuntimeError("Geocode API rejected the configured API key (HTTP 401).")
                    raise RuntimeError(f"Geocode API returned HTTP {response.status}")
                return await response.json(content_type=None)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    @commands.group(name="eventpinger")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def eventpinger(self, ctx: commands.Context) -> None:
        """Inspect the hardcoded MissionChief event pinger."""
        pass

    @eventpinger.command(name="status")
    async def eventpinger_status(self, ctx: commands.Context) -> None:
        """Show the hardcoded listener configuration."""
        geocode_enabled = await self._geocode_enabled()
        geocode_key = await self._get_geocode_api_key()
        cache_size = len(await self._read_cache())
        await ctx.send(
            "eventpinger status\n"
            f"Source channel: {SOURCE_CHANNEL_ID}\n"
            f"MissionChief app ID: {MISSIONCHIEF_APP_ID}\n"
            f"Notify role: {NOTIFY_EVENT_ROLE_ID}\n"
            f"Known region roles: {len(REGION_ROLE_NAMES)}\n"
            f"Geocode enabled: {geocode_enabled}\n"
            f"Geocode API key: {'set' if geocode_key else 'not set'}\n"
            f"Geocode cache entries: {cache_size}"
        )

    @eventpinger.command(name="resolve")
    async def eventpinger_resolve(self, ctx: commands.Context, *, address: str) -> None:
        """Test address resolution without sending role pings."""
        match = await self.resolve_region_for_address(address)
        if not match:
            await ctx.send("No confident region match. Only Notify-Event would be used.")
            return
        role = find_region_role(ctx.guild, match)
        await ctx.send(
            f"Resolved: {match.name}\n"
            f"Code: {match.code}\n"
            f"Source: {match.source}\n"
            f"Role: {getattr(role, 'mention', 'not found')}"
        )

    @eventpinger.command(name="apikey")
    @commands.is_owner()
    async def eventpinger_apikey(self, ctx: commands.Context, api_key: str = "") -> None:
        """Set or clear the geocode.maps.co API key. Owner only."""
        if self.config is None:
            await ctx.send("Config is not available in this runtime.")
            return

        try:
            await ctx.message.delete()
        except (discord.Forbidden, discord.HTTPException, AttributeError):
            pass

        clean_key = str(api_key or "").strip()
        await self.config.geocode_api_key.set(clean_key)
        await ctx.send("Geocode API key updated." if clean_key else "Geocode API key cleared.")

    @eventpinger.command(name="geocode")
    @commands.is_owner()
    async def eventpinger_geocode(self, ctx: commands.Context, state: str = "") -> None:
        """Enable or disable geocode fallback. Use on/off. Owner only."""
        normalized = state.casefold().strip()
        if normalized not in {"on", "off"}:
            await ctx.send(f"Use `{ctx.prefix}eventpinger geocode on` or `{ctx.prefix}eventpinger geocode off`.")
            return
        if self.config is None:
            await ctx.send("Config is not available in this runtime.")
            return

        enabled = normalized == "on"
        await self.config.geocode_enabled.set(enabled)
        await ctx.send(f"Geocode fallback {'enabled' if enabled else 'disabled'}.")

    @eventpinger.command(name="clearcache")
    @commands.is_owner()
    async def eventpinger_clearcache(self, ctx: commands.Context) -> None:
        """Clear cached geocode results. Owner only."""
        if self.config is None:
            self._memory_cache.clear()
        else:
            await self.config.geocode_cache.set({})
        await ctx.send("Geocode cache cleared.")
