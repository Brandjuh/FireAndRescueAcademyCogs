from __future__ import annotations

import asyncio
import io
import logging
import re
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import discord
from redbot.core import Config, commands
from redbot.core.utils.chat_formatting import box

log = logging.getLogger("red.cog.eventmanager")

BASE_URL = "https://www.missionchief.com"
REVERSE_ADDRESS_URL = f"{BASE_URL}/reverse_address"
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
DEFAULT_PANEL_CHANNEL_ID = 1426226521231589507
PANEL_TITLE = "EventManager Control Panel"
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
LATITUDE_FIELD = "mission_position[latitude]"
LONGITUDE_FIELD = "mission_position[longitude]"
ADDRESS_FIELD = "mission_position[address]"
COINS_FIELD = "mission_position[coins]"
MISSION_TYPE_FIELD = "mission_position[mission_type_id]"
EVENT_RADIO_FIELD = "event_radio_group"
AUTHENTICITY_TOKEN_FIELD = "authenticity_token"
POI_TYPE_FIELD = "mission_position[poi_type]"
SIZE_FIELD = "mission_position[size]"
SHAPE_FIELD = "mission_position[shape]"
AMOUNT_FIELD = "mission_position[amount]"
MISSION_POSITION_DEFAULT_OVERRIDES = {
    POI_TYPE_FIELD: "0",
    SHAPE_FIELD: "",
    SIZE_FIELD: "1",
    AMOUNT_FIELD: "1",
    COINS_FIELD: "0",
}
EVENT_DEFAULT_OVERRIDES = {
    SIZE_FIELD: "2",
    SHAPE_FIELD: "circle",
    AMOUNT_FIELD: "0",
}
RANDOM_LOCATION_KEY = "random_location"
RANDOM_LOCATION_ANCHORS = {
    "nyc": [
        (40.7295, -73.9972, "70 Washington Square South, 10012 New York, Manhattan"),
    ],
    "bermuda": [
        (32.2948, -64.7814, "Hamilton, Bermuda"),
    ],
}
RANDOM_LOCATION_ALIASES = {
    "newyork": "nyc",
    "new_york": "nyc",
    "new-york": "nyc",
    "new york": "nyc",
    "new york city": "nyc",
    "bermuda islands": "bermuda",
    "bermuda_islands": "bermuda",
    "bermuda-islands": "bermuda",
    "nyc_or_bermuda": "nyc_or_bermuda",
    "nyc-or-bermuda": "nyc_or_bermuda",
    "nyc/bermuda": "nyc_or_bermuda",
    "both": "nyc_or_bermuda",
}
CUSTOM_LOCATION_LABELS = {
    "nyc": "Fixed New York City",
    "bermuda": "Fixed Bermuda",
    "nyc_or_bermuda": "Fixed New York City",
}


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


def _submit_button_value(button_el) -> str:
    return button_el.get("value") or _text(button_el)


def profile_name_from_label(label: str, prefix: str = "") -> str:
    """Create a stable profile name from a MissionChief option label."""
    name = re.sub(r"[^a-z0-9]+", "_", (label or "").strip().lower()).strip("_")
    if not name:
        name = "profile"
    return f"{prefix}{name}"


def normalize_random_location_region(region: str) -> str:
    """Normalize configured random location regions."""
    normalized = " ".join(str(region or "").strip().lower().replace("_", " ").replace("-", " ").split())
    normalized = RANDOM_LOCATION_ALIASES.get(normalized, normalized)
    normalized = normalized.replace(" ", "_")
    if normalized not in {"nyc", "bermuda", "nyc_or_bermuda"}:
        raise ValueError("Random location must be `nyc`, `bermuda`, or `nyc_or_bermuda`.")
    return normalized


def random_location_for_region(region: str, *, rng=None) -> Tuple[str, str, str, str]:
    """Return latitude, longitude, address, and the concrete region used for a fixed start location."""
    normalized = normalize_random_location_region(region)
    concrete_region = "nyc" if normalized == "nyc_or_bermuda" else normalized
    latitude, longitude, address = RANDOM_LOCATION_ANCHORS[concrete_region][0]
    return f"{latitude:.6f}", f"{longitude:.6f}", address, concrete_region


def profile_fields_for_start(profile: dict, *, rng=None) -> Dict[str, str]:
    """Resolve runtime-only profile options into MissionChief form fields."""
    fields = dict(profile.get("fields", {}))
    random_region = profile.get(RANDOM_LOCATION_KEY)
    if random_region:
        latitude, longitude, address, _region = random_location_for_region(random_region, rng=rng)
        fields[LATITUDE_FIELD] = latitude
        fields[LONGITUDE_FIELD] = longitude
        fields[ADDRESS_FIELD] = address
    return fields


def field_options_for_kind(form: EventForm, kind: str) -> List[FormOption]:
    """Return the user-selectable MissionChief type options for an event kind."""
    kind = normalize_kind(kind)
    field_name = EVENT_RADIO_FIELD if kind == "event" else MISSION_TYPE_FIELD
    field_info = next((field for field in form.fields if field.name == field_name), None)
    if not field_info:
        return []
    return field_info.options


def truncate_discord_text(value: str, limit: int) -> str:
    """Trim text to a Discord component limit."""
    value = str(value or "")
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 1)] + "…"


def fields_for_selection(
    kind: str,
    selected_value: str,
    *,
    random_region: Optional[str] = None,
    latitude: Optional[str] = None,
    longitude: Optional[str] = None,
    address: Optional[str] = None,
) -> dict:
    """Build profile-like fields for a one-off panel start."""
    kind = normalize_kind(kind)
    profile: Dict[str, Any] = {"fields": {MISSION_TYPE_FIELD: selected_value}}
    if kind == "event":
        profile["fields"][EVENT_RADIO_FIELD] = selected_value
        profile["fields"].update(EVENT_DEFAULT_OVERRIDES)

    if random_region:
        profile[RANDOM_LOCATION_KEY] = normalize_random_location_region(random_region)
    elif latitude is not None and longitude is not None:
        profile["fields"][LATITUDE_FIELD] = latitude
        profile["fields"][LONGITUDE_FIELD] = longitude
        profile["fields"][ADDRESS_FIELD] = address or "Custom EventManager location"
    return profile


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
            if name and submit_name is None and not input_el.has_attr("disabled"):
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

    for button_el in form.find_all("button"):
        if _inside_custom_mission_creator(button_el):
            continue
        button_type = (button_el.get("type") or "submit").lower()
        if button_type != "submit" or button_el.has_attr("disabled"):
            continue
        name = button_el.get("name")
        if name and submit_name is None:
            submit_name = name
            submit_value = _submit_button_value(button_el)

    return EventForm(
        action=urljoin(page_url, action),
        method=method,
        fields=fields,
        submit_name=submit_name,
        submit_value=submit_value,
    )


def _append_payload_value(payload: Payload, name: str, value: str):
    payload.append((name, str(value or "")))


def _payload_value(payload: Payload, name: str) -> Optional[str]:
    for key, value in payload:
        if key == name:
            return value
    return None


def _replace_payload_value(payload: Payload, name: str, value: str) -> Payload:
    replaced = False
    updated: Payload = []
    for key, current_value in payload:
        if key == name:
            if not replaced:
                updated.append((key, str(value or "")))
                replaced = True
            continue
        updated.append((key, current_value))
    if not replaced:
        updated.append((name, str(value or "")))
    return updated


def _form_position_params(fields: Dict[str, str]) -> Dict[str, str]:
    latitude = fields.get(LATITUDE_FIELD)
    longitude = fields.get(LONGITUDE_FIELD)
    if not latitude or not longitude:
        return {}
    return {"tlat": latitude, "tlng": longitude}


def _ajax_submit_headers(kind: str, payload: Payload) -> Dict[str, str]:
    headers = {
        "Accept": "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01",
        "Origin": BASE_URL,
        "Referer": EVENT_KINDS[kind]["url"],
        "X-Requested-With": "XMLHttpRequest",
    }
    csrf_token = _payload_value(payload, AUTHENTICITY_TOKEN_FIELD)
    if csrf_token:
        headers["X-CSRF-Token"] = csrf_token
    return headers


def _normalize_overrides(form: EventForm, overrides: Dict[str, str]) -> Dict[str, str]:
    normalized = {str(key): str(value) for key, value in overrides.items()}
    field_names = {field_info.name for field_info in form.fields}

    if MISSION_TYPE_FIELD in field_names or EVENT_RADIO_FIELD in field_names:
        for key, value in MISSION_POSITION_DEFAULT_OVERRIDES.items():
            normalized.setdefault(key, value)

    if EVENT_RADIO_FIELD in field_names:
        for key, value in EVENT_DEFAULT_OVERRIDES.items():
            normalized[key] = value
        if EVENT_RADIO_FIELD in normalized and MISSION_TYPE_FIELD not in normalized:
            normalized[MISSION_TYPE_FIELD] = normalized[EVENT_RADIO_FIELD]
        elif MISSION_TYPE_FIELD in normalized and EVENT_RADIO_FIELD not in normalized:
            normalized[EVENT_RADIO_FIELD] = normalized[MISSION_TYPE_FIELD]

    return normalized


def _validate_free_submit(form: EventForm, payload: Payload) -> Optional[str]:
    submit_text = (form.submit_value or "").lower()
    if "free" not in submit_text:
        return f"Refusing to submit non-free action `{form.submit_value or 'unknown'}`."

    for name, value in payload:
        if name == COINS_FIELD and str(value or "0") not in {"", "0"}:
            return "Refusing to submit a payload that would spend coins."
        if name == "commit" and "coin" in str(value).lower():
            return f"Refusing to submit coin action `{value}`."
    return None


def build_payload(form: EventForm, overrides: Dict[str, str]) -> Payload:
    """Build a POST payload from form defaults plus configured overrides."""
    overrides = _normalize_overrides(form, overrides)
    payload: Payload = []
    used_names = set()
    for field_info in form.fields:
        override_present = field_info.name in overrides
        value = str(overrides[field_info.name] if override_present else field_info.value or "")

        if field_info.field_type == "checkbox":
            selected_values = {item.strip() for item in value.split(",")}
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


def summarize_payload_for_debug(payload: Payload, *, limit: int = 900) -> str:
    """Summarize non-sensitive MissionChief POST fields."""
    safe_names = {
        MISSION_TYPE_FIELD,
        EVENT_RADIO_FIELD,
        LATITUDE_FIELD,
        LONGITUDE_FIELD,
        ADDRESS_FIELD,
        POI_TYPE_FIELD,
        SIZE_FIELD,
        SHAPE_FIELD,
        AMOUNT_FIELD,
        COINS_FIELD,
        "commit",
    }
    parts = [f"{name}={value}" for name, value in payload if name in safe_names]
    summary = "; ".join(parts) if parts else "no safe payload fields"
    return summary[:limit]


def summarize_response_for_debug(text: str, *, limit: int = 350) -> str:
    """Summarize a MissionChief error response without leaking form tokens."""
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", str(text or ""), flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"authenticity_token[^\s&<>\"]+", "authenticity_token=REDACTED", text, flags=re.IGNORECASE)
    text = " ".join(text.split())
    if not text:
        return ""
    return text[:limit]


def parse_location_value(value: str) -> Tuple[str, str]:
    """Parse `lat, lon` input for profile location shortcuts."""
    parts = [part.strip() for part in (value or "").replace(";", ",").split(",")]
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("Location must be formatted as `latitude, longitude`.")
    try:
        latitude = float(parts[0])
        longitude = float(parts[1])
    except ValueError as exc:
        raise ValueError("Location must contain numeric latitude and longitude.") from exc
    if not -90 <= latitude <= 90:
        raise ValueError("Latitude must be between -90 and 90.")
    if not -180 <= longitude <= 180:
        raise ValueError("Longitude must be between -180 and 180.")
    return str(latitude), str(longitude)


def parse_location_or_random_region(value: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse coordinates or a random-location region."""
    try:
        latitude, longitude = parse_location_value(value)
    except ValueError:
        return None, None, normalize_random_location_region(value)
    return latitude, longitude, None


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


class EventManagerPanelView(discord.ui.View):
    """Persistent admin entry point for EventManager."""

    def __init__(self, cog: "EventManager"):
        super().__init__(timeout=None)
        self.cog = cog

    async def _require_admin(self, interaction: discord.Interaction) -> bool:
        if await self.cog.can_manage(interaction):
            return True
        await interaction.response.send_message("You do not have permission to use EventManager.", ephemeral=True)
        return False

    async def _quick_start(self, interaction: discord.Interaction, kind: str):
        if not await self._require_admin(interaction):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        result = await self.cog.start_quick(kind)
        if result.ok:
            await interaction.followup.send(f"Started {EVENT_KINDS[kind]['label']} with quick settings.", ephemeral=True)
        else:
            await interaction.followup.send(f"Could not start {EVENT_KINDS[kind]['label']}: {result.reason}", ephemeral=True)

    async def _custom_start(self, interaction: discord.Interaction, kind: str):
        if not await self._require_admin(interaction):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            view = await self.cog.build_custom_start_view(kind)
        except Exception as exc:
            await interaction.followup.send(f"Could not load MissionChief options: {exc}", ephemeral=True)
            return
        await interaction.followup.send(embed=view.build_embed(), view=view, ephemeral=True)

    @discord.ui.button(
        label="Alliance Event (Quick)",
        style=discord.ButtonStyle.success,
        custom_id="eventmanager:quick:event",
        row=0,
    )
    async def event_quick(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._quick_start(interaction, "event")

    @discord.ui.button(
        label="Alliance Event (Custom)",
        style=discord.ButtonStyle.primary,
        custom_id="eventmanager:custom:event",
        row=0,
    )
    async def event_custom(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._custom_start(interaction, "event")

    @discord.ui.button(
        label="Large Scale Mission (Quick)",
        style=discord.ButtonStyle.success,
        custom_id="eventmanager:quick:large",
        row=1,
    )
    async def large_quick(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._quick_start(interaction, "large")

    @discord.ui.button(
        label="Large Scale Mission (Custom)",
        style=discord.ButtonStyle.primary,
        custom_id="eventmanager:custom:large",
        row=1,
    )
    async def large_custom(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._custom_start(interaction, "large")


class CustomTypeSelect(discord.ui.Select):
    """Select the MissionChief event or mission type for a custom start."""

    def __init__(self, parent: "CustomStartView"):
        self.parent_view = parent
        options = [
            discord.SelectOption(
                label=truncate_discord_text(option.label, 100),
                value=option.value,
                default=option.value == parent.selected_value,
            )
            for option in parent.options[:25]
        ]
        super().__init__(
            placeholder="Choose MissionChief type",
            min_values=1,
            max_values=1,
            options=options,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction):
        self.parent_view.selected_value = self.values[0]
        await interaction.response.edit_message(embed=self.parent_view.build_embed(), view=self.parent_view.rebuild())


class CustomLocationSelect(discord.ui.Select):
    """Select how the custom start location should be chosen."""

    def __init__(self, parent: "CustomStartView"):
        self.parent_view = parent
        allowed_regions = ["nyc", "bermuda", "nyc_or_bermuda"] if parent.kind == "event" else ["nyc"]
        options = [
            discord.SelectOption(
                label=CUSTOM_LOCATION_LABELS[region],
                value=f"random:{region}",
                default=parent.random_region == region,
            )
            for region in allowed_regions
        ]
        if parent.latitude and parent.longitude:
            options.append(
                discord.SelectOption(
                    label="Manual coordinates",
                    value="manual",
                    description=f"{parent.latitude}, {parent.longitude}",
                    default=not parent.random_region,
                )
            )
        super().__init__(
            placeholder="Choose location",
            min_values=1,
            max_values=1,
            options=options[:25],
            row=1,
        )

    async def callback(self, interaction: discord.Interaction):
        value = self.values[0]
        if value.startswith("random:"):
            self.parent_view.random_region = value.split(":", 1)[1]
            self.parent_view.latitude = None
            self.parent_view.longitude = None
        await interaction.response.edit_message(embed=self.parent_view.build_embed(), view=self.parent_view.rebuild())


class CustomLocationModal(discord.ui.Modal, title="Custom start location"):
    """Modal for manual latitude/longitude input."""

    location = discord.ui.TextInput(
        label="Latitude, longitude",
        placeholder="Example: 40.7295, -73.9972",
        required=True,
        max_length=60,
    )
    address = discord.ui.TextInput(
        label="Address or map label",
        placeholder="Example: Washington Square Park, New York",
        required=False,
        max_length=120,
    )

    def __init__(self, parent: "CustomStartView"):
        super().__init__()
        self.parent_view = parent

    async def on_submit(self, interaction: discord.Interaction):
        try:
            latitude, longitude = parse_location_value(str(self.location.value))
        except ValueError as exc:
            await interaction.response.send_message(str(exc), ephemeral=True)
            return
        self.parent_view.latitude = latitude
        self.parent_view.longitude = longitude
        self.parent_view.address = str(self.address.value or "").strip() or "Custom EventManager location"
        self.parent_view.random_region = None
        await interaction.response.send_message(
            f"Location set to `{latitude}, {longitude}`. Return to the custom start message and press Start.",
            ephemeral=True,
        )


class CustomStartView(discord.ui.View):
    """Transient private flow for one custom MissionChief start."""

    def __init__(self, cog: "EventManager", kind: str, options: List[FormOption]):
        super().__init__(timeout=300)
        self.cog = cog
        self.kind = normalize_kind(kind)
        self.options = options
        self.selected_value = options[0].value if options else ""
        self.random_region = "nyc_or_bermuda" if self.kind == "event" else "nyc"
        self.latitude: Optional[str] = None
        self.longitude: Optional[str] = None
        self.address: Optional[str] = None
        self.rebuild()

    def selected_label(self) -> str:
        selected = next((option for option in self.options if option.value == self.selected_value), None)
        return selected.label if selected else self.selected_value or "None"

    def location_label(self) -> str:
        if self.random_region:
            return CUSTOM_LOCATION_LABELS.get(self.random_region, self.random_region)
        if self.latitude and self.longitude:
            address = f" - {self.address}" if self.address else ""
            return f"{self.latitude}, {self.longitude}{address}"
        return "Not configured"

    def rebuild(self):
        self.clear_items()
        if self.options:
            self.add_item(CustomTypeSelect(self))
        self.add_item(CustomLocationSelect(self))
        self.add_item(CustomLocationButton(self))
        self.add_item(CustomStartButton(self))
        return self

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=f"Custom {EVENT_KINDS[self.kind]['label']}",
            description="Choose the MissionChief option and location, then press Start.",
            color=discord.Color.blue(),
        )
        embed.add_field(name="Selected type", value=self.selected_label(), inline=False)
        embed.add_field(name="Location", value=self.location_label(), inline=False)
        if self.kind == "event":
            embed.add_field(name="Defaults", value="Large area • Circle • Every 30 seconds", inline=False)
        return embed

    async def start(self, interaction: discord.Interaction):
        if not await self.cog.can_manage(interaction):
            await interaction.response.send_message("You do not have permission to use EventManager.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        profile = fields_for_selection(
            self.kind,
            self.selected_value,
            random_region=self.random_region,
            latitude=self.latitude,
            longitude=self.longitude,
            address=self.address,
        )
        result = await self.cog.start_one_off(self.kind, profile, f"custom:{self.selected_label()}")
        if result.ok:
            await interaction.followup.send(f"Started {EVENT_KINDS[self.kind]['label']}: {self.selected_label()}", ephemeral=True)
        else:
            await interaction.followup.send(f"Could not start {EVENT_KINDS[self.kind]['label']}: {result.reason}", ephemeral=True)


class CustomLocationButton(discord.ui.Button):
    """Open the manual coordinate modal."""

    def __init__(self, parent: CustomStartView):
        super().__init__(label="Use Coordinates", style=discord.ButtonStyle.secondary, row=2)
        self.parent_view = parent

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CustomLocationModal(self.parent_view))


class CustomStartButton(discord.ui.Button):
    """Start the selected custom item."""

    def __init__(self, parent: CustomStartView):
        super().__init__(label="Start", style=discord.ButtonStyle.success, row=2)
        self.parent_view = parent

    async def callback(self, interaction: discord.Interaction):
        await self.parent_view.start(interaction)


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
            panel_channel_id=DEFAULT_PANEL_CHANNEL_ID,
            panel_message_id=None,
        )
        self._task: Optional[asyncio.Task] = None
        self._panel_task: Optional[asyncio.Task] = None
        self._start_lock = asyncio.Lock()

    async def cog_load(self):
        self.bot.add_view(EventManagerPanelView(self))
        self._task = asyncio.create_task(self._scheduler_loop())
        self._panel_task = asyncio.create_task(self._ensure_panels_after_ready())

    async def cog_unload(self):
        if self._task:
            self._task.cancel()
        if self._panel_task:
            self._panel_task.cancel()

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

    async def _fetch_form(self, kind: str, fields: Optional[Dict[str, str]] = None) -> EventForm:
        kind = normalize_kind(kind)
        page_url = EVENT_KINDS[kind]["url"]
        params = _form_position_params(fields or {})
        session = await self._get_session()
        async with session.get(page_url, allow_redirects=True, params=params or None) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            raise RuntimeError(f"MissionChief returned HTTP {status}.")
        return parse_event_form(html, page_url)

    async def _resolve_reverse_address(self, session, kind: str, payload: Payload) -> Payload:
        """Use MissionChief's marker reverse-address endpoint before submitting the form."""
        latitude = _payload_value(payload, LATITUDE_FIELD)
        longitude = _payload_value(payload, LONGITUDE_FIELD)
        if not latitude or not longitude:
            return payload

        try:
            async with session.get(
                REVERSE_ADDRESS_URL,
                params={"latitude": latitude, "longitude": longitude},
                headers={"Referer": EVENT_KINDS[kind]["url"], "X-Requested-With": "XMLHttpRequest"},
            ) as response:
                status = getattr(response, "status", None)
                address = (await response.text()).strip()
        except Exception:
            log.exception("EventManager reverse-address lookup failed")
            return payload

        if status is not None and int(status) >= 400:
            log.warning("EventManager reverse-address lookup returned HTTP %s", status)
            return payload
        if not address:
            log.warning("EventManager reverse-address lookup returned an empty address")
            return payload
        return _replace_payload_value(payload, ADDRESS_FIELD, address)

    async def _start_profile_data(self, kind: str, profile_name: str, profile: dict) -> EventStartResult:
        kind = normalize_kind(kind)
        async with self._start_lock:
            start_fields = profile_fields_for_start(profile)
            try:
                form = await self._fetch_form(kind, start_fields)
            except Exception as exc:
                return EventStartResult(False, f"Could not fetch form: {exc}")

            if form.method != "post":
                return EventStartResult(False, f"Unexpected form method `{form.method}`.")

            session = await self._get_session()
            payload = build_payload(form, start_fields)
            payload = await self._resolve_reverse_address(session, kind, payload)
            validation_error = _validate_free_submit(form, payload)
            if validation_error:
                return EventStartResult(False, validation_error, post_url=form.action)

            try:
                async with session.post(
                    form.action,
                    data=payload,
                    allow_redirects=False,
                    headers=_ajax_submit_headers(kind, payload),
                ) as response:
                    status = getattr(response, "status", None)
                    response_text = await response.text()
            except Exception as exc:
                return EventStartResult(False, f"MissionChief POST failed: {exc}", post_url=form.action)

        if status is None or int(status) >= 400:
            debug = summarize_payload_for_debug(payload)
            response_debug = summarize_response_for_debug(response_text)
            log.warning(
                "EventManager %s POST failed with HTTP %s. Payload: %s. Response: %s",
                kind,
                status,
                debug,
                response_debug,
            )
            response_suffix = f" Response: {response_debug}" if response_debug else ""
            return EventStartResult(
                False,
                f"MissionChief returned HTTP {status}. Safe payload: {debug}{response_suffix}",
                status=status,
                post_url=form.action,
            )

        await self._log_run(kind, profile_name, status)
        return EventStartResult(True, "Started successfully.", status=status, post_url=form.action)

    async def _start_from_profile(self, kind: str, profile_name: str) -> EventStartResult:
        kind = normalize_kind(kind)
        profile_name = profile_name.strip().lower()
        profiles = await self.config.profiles()
        profile = profiles.get(kind, {}).get(profile_name)
        if not profile:
            return EventStartResult(False, f"Profile `{profile_name}` was not found.")
        return await self._start_profile_data(kind, profile_name, profile)

    async def start_one_off(self, kind: str, profile: dict, label: str) -> EventStartResult:
        """Start an item from an in-memory profile."""
        return await self._start_profile_data(kind, label, profile)

    async def start_quick(self, kind: str) -> EventStartResult:
        """Start using configured rotation when available, otherwise fall back to live defaults."""
        kind = normalize_kind(kind)
        profiles = await self.config.profiles()
        schedules = await self.config.schedules()
        schedule = schedules.get(kind, {})
        profile_name, next_rotation_index = select_scheduled_profile(schedule)
        if profile_name and profile_name in profiles.get(kind, {}):
            result = await self._start_from_profile(kind, profile_name)
            if result.ok:
                async with self.config.schedules() as saved_schedules:
                    saved_schedules[kind]["profile"] = profile_name
                    saved_schedules[kind]["rotation_index"] = next_rotation_index
            return result

        form = await self._fetch_form(kind)
        options = field_options_for_kind(form, kind)
        if not options:
            return EventStartResult(False, "No MissionChief options were found on the live form.")
        random_region = "nyc_or_bermuda" if kind == "event" else "nyc"
        profile = fields_for_selection(kind, options[0].value, random_region=random_region)
        return await self._start_profile_data(kind, f"quick:{options[0].label}", profile)

    async def build_custom_start_view(self, kind: str) -> CustomStartView:
        """Build a custom start view from the live MissionChief form."""
        kind = normalize_kind(kind)
        form = await self._fetch_form(kind)
        options = field_options_for_kind(form, kind)
        if not options:
            raise RuntimeError("No selectable MissionChief options were found.")
        return CustomStartView(self, kind, options)

    async def can_manage(self, interaction: discord.Interaction) -> bool:
        """Check if an interaction user can use EventManager."""
        guild = interaction.guild
        user = interaction.user
        if guild is None or not isinstance(user, discord.Member):
            return False
        if user.guild_permissions.administrator:
            return True
        is_admin = getattr(self.bot, "is_admin", None)
        if is_admin:
            with suppress(Exception):
                return bool(await is_admin(user))
        return False

    async def _ensure_panels_after_ready(self):
        await self.bot.wait_until_ready()
        for guild in self.bot.guilds:
            with suppress(Exception):
                await self.ensure_panel_message(guild, create=True)

    async def panel_channel(self, guild: discord.Guild):
        channel_id = int(await self.config.panel_channel_id() or DEFAULT_PANEL_CHANNEL_ID)
        return guild.get_channel(channel_id) or self.bot.get_channel(channel_id)

    def is_panel_message(self, message: discord.Message) -> bool:
        bot_user_id = getattr(getattr(self.bot, "user", None), "id", None)
        if bot_user_id and getattr(getattr(message, "author", None), "id", None) != bot_user_id:
            return False
        for embed in getattr(message, "embeds", []) or []:
            if getattr(embed, "title", None) == PANEL_TITLE:
                return True
        return False

    async def build_panel_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=PANEL_TITLE,
            description=(
                "Start free MissionChief alliance missions and events.\n\n"
                "Quick uses configured/default settings. Custom lets admins choose the live MissionChief option "
                "and location before starting."
            ),
            color=discord.Color.orange(),
        )
        embed.add_field(name="Safety", value="Free starts only. Coin actions are refused.", inline=False)
        embed.add_field(name="Visibility", value="Button actions are private to the admin using them.", inline=False)
        return embed

    async def find_existing_panel_message(self, channel: Any):
        history = getattr(channel, "history", None)
        if not history:
            return None
        found = []
        with suppress(discord.Forbidden, discord.HTTPException):
            async for message in channel.history(limit=50):
                if self.is_panel_message(message):
                    found.append(message)
        if not found:
            return None
        keep = found[0]
        for duplicate in found[1:]:
            with suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                await duplicate.delete()
        return keep

    async def ensure_panel_message(self, guild: discord.Guild, *, create: bool = True):
        channel = await self.panel_channel(guild)
        if not channel:
            return None
        embed = await self.build_panel_embed()
        view = EventManagerPanelView(self)
        message_id = await self.config.panel_message_id()
        if message_id and hasattr(channel, "fetch_message"):
            with suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                message = await channel.fetch_message(int(message_id))
                await message.edit(embed=embed, view=view)
                return message

        existing = await self.find_existing_panel_message(channel)
        if existing:
            await existing.edit(embed=embed, view=view)
            await self.config.panel_message_id.set(existing.id)
            return existing
        if not create:
            return None
        message = await channel.send(embed=embed, view=view)
        await self.config.panel_message_id.set(message.id)
        return message

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

    @eventmanager.command(name="debugpayload")
    @commands.admin()
    async def debug_payload(self, ctx: commands.Context, kind: str, profile_name: Optional[str] = None):
        """Show the safe POST payload EventManager would submit."""
        try:
            kind = normalize_kind(kind)
            form = await self._fetch_form(kind)
        except Exception as exc:
            await ctx.send(f"Could not build debug payload: {exc}")
            return

        profile = None
        label = "quick"
        if profile_name:
            profiles = await self.config.profiles()
            profile = profiles.get(kind, {}).get(profile_name.strip().lower())
            label = profile_name.strip().lower()
            if not profile:
                await ctx.send(f"Profile `{label}` was not found.")
                return
        else:
            options = field_options_for_kind(form, kind)
            if not options:
                await ctx.send("No MissionChief options were found on the live form.")
                return
            random_region = "nyc_or_bermuda" if kind == "event" else "nyc"
            profile = fields_for_selection(kind, options[0].value, random_region=random_region)

        try:
            session = await self._get_session()
            start_fields = profile_fields_for_start(profile)
            form = await self._fetch_form(kind, start_fields)
            payload = build_payload(form, start_fields)
            payload = await self._resolve_reverse_address(session, kind, payload)
        except Exception as exc:
            await ctx.send(f"Could not build debug payload: {exc}")
            return

        summary = "\n".join(
            [
                f"Kind: {kind}",
                f"Profile: {label}",
                f"Action: {form.action}",
                f"Submit: {form.submit_name}={form.submit_value or ''}",
                "",
                summarize_payload_for_debug(payload, limit=1800),
            ]
        )
        data = io.BytesIO(summary.encode("utf-8"))
        await ctx.send(
            f"Safe EventManager payload debug for `{kind}`:",
            file=discord.File(data, filename=f"eventmanager-{kind}-payload.txt"),
        )

    @eventmanager.command(name="panel")
    @commands.admin()
    @commands.guild_only()
    async def panel(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        """Post or refresh the EventManager control panel."""
        if channel is not None:
            await self.config.panel_channel_id.set(channel.id)
            await self.config.panel_message_id.set(None)
        message = await self.ensure_panel_message(ctx.guild, create=True)
        if message:
            await ctx.send(f"EventManager panel ready in {message.channel.mention}: {message.jump_url}")
        else:
            await ctx.send("Could not post the EventManager panel. Check the configured channel and bot permissions.")

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

    @profile.command(name="location")
    @commands.admin()
    async def profile_location(self, ctx: commands.Context, kind: str, profile_name: str, *, location: str):
        """Set profile coordinates as `latitude, longitude`; address is left for MissionChief."""
        try:
            kind = normalize_kind(kind)
            latitude, longitude = parse_location_value(location)
        except ValueError as exc:
            await ctx.send(str(exc))
            return

        profile_name = profile_name.strip().lower()
        async with self.config.profiles() as profiles:
            profile = profiles.setdefault(kind, {}).setdefault(profile_name, {"fields": {}})
            fields = profile.setdefault("fields", {})
            fields[LATITUDE_FIELD] = latitude
            fields[LONGITUDE_FIELD] = longitude
            fields[ADDRESS_FIELD] = "Custom EventManager location"
            profile.pop(RANDOM_LOCATION_KEY, None)
        await ctx.tick()

    @profile.command(name="randomlocation")
    @commands.admin()
    async def profile_random_location(self, ctx: commands.Context, kind: str, profile_name: str, region: str):
        """Set a profile to choose a random start location from `nyc`, `bermuda`, or `nyc_or_bermuda`."""
        try:
            kind = normalize_kind(kind)
            region = normalize_random_location_region(region)
        except ValueError as exc:
            await ctx.send(str(exc))
            return

        profile_name = profile_name.strip().lower()
        async with self.config.profiles() as profiles:
            profile = profiles.setdefault(kind, {}).setdefault(profile_name, {"fields": {}})
            profile[RANDOM_LOCATION_KEY] = region
            fields = profile.setdefault("fields", {})
            fields.pop(LATITUDE_FIELD, None)
            fields.pop(LONGITUDE_FIELD, None)
            fields.pop(ADDRESS_FIELD, None)
        await ctx.tick()

    @profile.command(name="seeddailymissions")
    @commands.admin()
    async def profile_seed_daily_missions(self, ctx: commands.Context):
        """Create one daily large-scale mission profile per available mission type with random NYC locations."""
        try:
            form = await self._fetch_form("large")
        except Exception as exc:
            await ctx.send(f"Could not seed daily mission profiles: {exc}")
            return

        mission_type = next((field_info for field_info in form.fields if field_info.name == MISSION_TYPE_FIELD), None)
        if not mission_type or not mission_type.options:
            await ctx.send("No large-scale mission options were found on the MissionChief form.")
            return

        created = []
        async with self.config.profiles() as profiles:
            large_profiles = profiles.setdefault("large", {})
            for option in mission_type.options:
                profile_name = profile_name_from_label(option.label, prefix="large_")
                large_profiles[profile_name] = {
                    RANDOM_LOCATION_KEY: "nyc",
                    "fields": {
                        MISSION_TYPE_FIELD: option.value,
                    },
                }
                created.append(profile_name)

        await ctx.send(
            "Created daily large-scale mission profiles with random New York City locations:\n"
            + box(", ".join(created), lang="ini")
        )

    @profile.command(name="seedweeklyevents")
    @commands.admin()
    async def profile_seed_weekly_events(self, ctx: commands.Context, *, location: str = "nyc_or_bermuda"):
        """Create one weekly event profile per event type with coordinates or random NYC/Bermuda locations."""
        try:
            latitude, longitude, random_region = parse_location_or_random_region(location)
            form = await self._fetch_form("event")
        except Exception as exc:
            await ctx.send(f"Could not seed event profiles: {exc}")
            return

        event_group = next((field_info for field_info in form.fields if field_info.name == EVENT_RADIO_FIELD), None)
        if not event_group or not event_group.options:
            await ctx.send("No event options were found on the MissionChief form.")
            return

        created = []
        async with self.config.profiles() as profiles:
            event_profiles = profiles.setdefault("event", {})
            for option in event_group.options:
                profile_name = profile_name_from_label(option.label)
                profile = {
                    "fields": {
                        EVENT_RADIO_FIELD: option.value,
                        MISSION_TYPE_FIELD: option.value,
                        **EVENT_DEFAULT_OVERRIDES,
                    }
                }
                if random_region:
                    profile[RANDOM_LOCATION_KEY] = random_region
                else:
                    profile["fields"][LATITUDE_FIELD] = latitude
                    profile["fields"][LONGITUDE_FIELD] = longitude
                    profile["fields"][ADDRESS_FIELD] = "Custom EventManager location"
                event_profiles[profile_name] = profile
                created.append(profile_name)

        location_note = (
            f"random `{random_region}` locations" if random_region else f"fixed location `{latitude}, {longitude}`"
        )
        await ctx.send(
            f"Created weekly event profiles with Large/Circle/Every 30 seconds and {location_note}:\n"
            + box(", ".join(created), lang="ini")
        )

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
        if profile.get(RANDOM_LOCATION_KEY):
            lines.append(f"{RANDOM_LOCATION_KEY} = {profile[RANDOM_LOCATION_KEY]}")
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
