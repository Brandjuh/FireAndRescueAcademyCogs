from __future__ import annotations

import asyncio
import io
import json
import logging
import random
import re
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
import discord
from redbot.core import Config, commands
from redbot.core.utils.chat_formatting import box

log = logging.getLogger("red.cog.eventmanager")

BASE_URL = "https://www.missionchief.com"
MISSIONCHIEF_HOME_URL = f"{BASE_URL}/"
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
DEFAULT_PANEL_CHANNEL_ID = 1421256548977606827
PANEL_TITLE = "EventManager Control Panel"
REQUEST_PANEL_TITLE = "Event Requests"
SCHEDULE_RETRY_SECONDS = 90
COOLDOWN_GRACE_SECONDS = 75
PLAYWRIGHT_SETUP_MESSAGE = (
    "Playwright browser automation is not ready. Install the EventManager requirements and run "
    "`python -m playwright install chromium` in the same Python environment as Redbot."
)
BROWSER_CAPTURE_SCRIPT = r"""
(() => {
  const form = document.querySelector("#new_mission_position")
    || [...document.querySelectorAll("form")].find((item) => (item.action || "").includes("missionAlliance"));
  if (!form) {
    console.log("EventManager capture: no MissionChief alliance form found.");
    return;
  }

  const redact = (name, value) => {
    const lowered = String(name || "").toLowerCase();
    if (lowered.includes("token") || lowered.includes("cookie") || lowered === "authorization") {
      return "REDACTED";
    }
    return value;
  };

  const fields = [...form.querySelectorAll("input, select, textarea")].map((field) => ({
    tag: field.tagName.toLowerCase(),
    type: field.type || "",
    name: field.name || "",
    id: field.id || "",
    value: redact(field.name || field.id, field.value || ""),
    checked: field.checked || false,
    disabled: field.disabled || false,
    readonly: field.readOnly || false,
  }));

  const submitButtons = [...form.querySelectorAll("input[type='submit'], button[type='submit'], button:not([type])")].map((button) => ({
    tag: button.tagName.toLowerCase(),
    type: button.type || "",
    name: button.name || "",
    id: button.id || "",
    value: redact(button.name || button.id, button.value || button.textContent.trim() || ""),
    disabled: button.disabled || false,
    className: button.className || "",
  }));

  const report = {
    note: "No submit was sent. This only reads the current browser DOM.",
    pageUrl: location.href,
    formAction: form.action,
    formMethod: form.method,
    formClass: form.className,
    fields,
    submitButtons,
  };

  const output = JSON.stringify(report, null, 2);
  console.log(output);
  if (typeof copy === "function") {
    copy(output);
    console.log("EventManager capture copied to clipboard.");
  } else if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(output).then(
      () => console.log("EventManager capture copied to clipboard."),
      () => console.log("EventManager capture could not be copied automatically.")
    );
  }
})();
""".strip()
BROWSER_EVENT_START_SCRIPT = r"""
(() => {
  const config = __CONFIG__;
  const fail = (message) => {
    console.error(`EventManager browser start: ${message}`);
    alert(`EventManager browser start failed:\n${message}`);
    throw new Error(message);
  };
  const dispatch = (field) => {
    for (const eventName of ["input", "change"]) {
      field.dispatchEvent(new Event(eventName, { bubbles: true }));
    }
  };
  const cssEscape = (value) => {
    if (window.CSS && typeof CSS.escape === "function") return CSS.escape(value);
    return String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
  };
  const setValue = (name, value, required = false) => {
    const field = document.querySelector(`[name="${cssEscape(name)}"]`);
    if (!field) {
      if (required) fail(`Required field not found: ${name}`);
      return false;
    }
    field.value = value;
    dispatch(field);
    return true;
  };
  const visibleText = (element) => [element.value, element.textContent, element.getAttribute("title")]
    .filter(Boolean)
    .join(" ")
    .replace(/\s+/g, " ")
    .trim();

  if (!location.pathname.includes("/missionAllianceEventNew")) {
    fail("Open https://www.missionchief.com/missionAllianceEventNew first.");
  }

  const form = document.querySelector("#new_mission_position")
    || [...document.querySelectorAll("form")].find((item) => (item.action || "").includes("missionAllianceEventCreate"));
  if (!form) {
    fail("MissionChief alliance event form was not found.");
  }
  if (!(form.action || "").includes("missionAllianceEventCreate")) {
    fail(`Unexpected form action: ${form.action || "unknown"}`);
  }

  const eventValue = String(config.fields["event_radio_group"] || config.fields["mission_position[mission_type_id]"] || "");
  if (!eventValue) {
    fail("No event type was configured.");
  }

  const radios = [...form.querySelectorAll('input[name="event_radio_group"]')];
  const eventRadio = radios.find((radio) => {
    const candidates = [
      radio.value,
      radio.dataset.eventId,
      (radio.id || "").replace(/^event_/, ""),
    ].filter(Boolean).map(String);
    return candidates.includes(eventValue);
  });
  if (!eventRadio) {
    fail(`Could not find event option ${eventValue} on this page.`);
  }
  eventRadio.checked = true;
  eventRadio.click();
  dispatch(eventRadio);

  if (!document.querySelector('[name="mission_position[mission_type_id]"]')?.value) {
    setValue("mission_position[mission_type_id]", eventRadio.dataset.eventId || eventValue, true);
  }
  for (const [name, value] of Object.entries(config.fields)) {
    if (name === "event_radio_group" || name === "mission_position[mission_type_id]") continue;
    setValue(name, String(value), false);
  }

  const allowCoins = config.allowCoins === true;
  const coinValue = document.querySelector('[name="mission_position[coins]"]')?.value || "0";
  if (!allowCoins && !["", "0"].includes(String(coinValue))) {
    fail(`Refusing to continue because coins is ${coinValue}.`);
  }

  const buttons = [...form.querySelectorAll('input[type="submit"], button[type="submit"], button:not([type])')];
  const startButton = buttons.find((button) => {
    const text = visibleText(button).toLowerCase();
    if (button.disabled) return false;
    if (allowCoins) return text.includes("start") && text.includes("event") && (text.includes("free") || text.includes("coin"));
    return text.includes("free") && !text.includes("coin");
  });
  if (!startButton) {
    const wanted = allowCoins ? "free or coin Start Event" : "free Start Event";
    fail(`The enabled ${wanted} button was not found.`);
  }

  const buttonText = visibleText(startButton);
  const usesCoins = buttonText.toLowerCase().includes("coin") || !["", "0"].includes(String(coinValue));
  if (usesCoins && !allowCoins) {
    fail(`Refusing to click coin action: ${buttonText}.`);
  }
  const summary = [
    `Event: ${config.label}`,
    `Latitude: ${config.fields["mission_position[latitude]"] || "not set"}`,
    `Longitude: ${config.fields["mission_position[longitude]"] || "not set"}`,
    `Address: ${config.fields["mission_position[address]"] || "not set"}`,
    `Button: ${buttonText}`,
  ].join("\n");

  if (!confirm(`Start this MissionChief alliance event?\n\n${summary}`)) {
    console.log("EventManager browser start cancelled by user.");
    return;
  }

  if (usesCoins) {
    const confirmation = prompt(
      `This action can spend MissionChief coins.\n\n${summary}\n\nType SPEND COINS to continue.`
    );
    if (confirmation !== "SPEND COINS") {
      console.log("EventManager browser start cancelled before spending coins.");
      return;
    }
  }

  startButton.click();
})();
""".strip()
BROWSER_PREPARE_START_SCRIPT = r"""
async (config) => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const visibleText = (element) => [element?.value, element?.textContent, element?.getAttribute?.("title")]
    .filter(Boolean)
    .join(" ")
    .replace(/\s+/g, " ")
    .trim();
  const fieldByName = (name) => document.querySelector(`[name="${window.CSS.escape(name)}"]`);
  const fieldValue = (name) => fieldByName(name)?.value || "";
  const dispatch = (field) => {
    if (!field) return;
    for (const eventName of ["input", "change"]) {
      field.dispatchEvent(new Event(eventName, { bubbles: true }));
    }
  };
  const buttons = () => [...document.querySelectorAll('input[type="submit"], button[type="submit"], button:not([type])')];
  const buttonDiagnostics = () => buttons().map((button, index) => ({
    index,
    text: visibleText(button) || "(blank)",
    disabled: Boolean(button.disabled || button.hasAttribute("disabled")),
    id: button.id || "",
    name: button.name || "",
    className: button.className || "",
  }));
  const lastFreeText = () => {
    const candidates = [
      document.querySelector("#alliance_event_last_free_mission"),
      ...document.querySelectorAll(".alert, .alert-info, .well"),
    ].filter(Boolean);
    const node = candidates.find((item) => visibleText(item).toLowerCase().includes("last free mission"));
    return visibleText(node);
  };
  const snapshot = () => ({
    url: location.href,
    latitude: fieldValue("mission_position[latitude]"),
    longitude: fieldValue("mission_position[longitude]"),
    address: fieldValue("mission_position[address]"),
    missionType: fieldValue("mission_position[mission_type_id]"),
    eventRadio: [...document.querySelectorAll('input[name="event_radio_group"]')]
      .find((radio) => radio.checked)?.dataset?.eventId || "",
    coins: fieldValue("mission_position[coins]"),
    lastFreeText: lastFreeText(),
    submitButtons: buttonDiagnostics(),
  });
  const fail = (reason) => ({ ok: false, reason, snapshot: snapshot() });
  const clickIfPresent = (selector) => {
    const element = document.querySelector(selector);
    if (!element) return false;
    element.click();
    return true;
  };
  const setField = (name, value) => {
    const field = fieldByName(name);
    if (!field) return false;
    field.value = String(value);
    dispatch(field);
    return true;
  };

  const form = document.querySelector("#new_mission_position");
  if (!form) return fail("MissionChief start form was not loaded.");

  if (config.kind === "event") {
    const eventValue = String(config.eventValue || "");
    const radios = [...form.querySelectorAll('input[name="event_radio_group"]')];
    const radio = radios.find((item) => {
      const candidates = [item.value, item.dataset.eventId, (item.id || "").replace(/^event_/, "")]
        .filter(Boolean)
        .map(String);
      return candidates.includes(eventValue);
    });
    if (!radio) return fail(`Could not find event option ${eventValue}.`);
    radio.checked = true;
    radio.click();
    dispatch(radio);
    if (!fieldValue("mission_position[mission_type_id]")) {
      setField("mission_position[mission_type_id]", radio.dataset.eventId || eventValue);
    }
    clickIfPresent(`.btn-event_expansion[expansion_id="${config.size || "2"}"]`);
    clickIfPresent(`.btn-event_shape[data-shape="${config.shape || "circle"}"]`);
    clickIfPresent(`.btn-event_amount[amount_id="${config.amount || "0"}"]`);
    if (config.duration !== undefined && config.duration !== null && config.duration !== "") {
      clickIfPresent(`.btn-event_duration[duration_id="${config.duration}"]`);
    }
  } else {
    const missionValue = String(config.missionType || "");
    const radios = [...form.querySelectorAll('input[name="mission_position[mission_type_id]"]')];
    const radio = radios.find((item) => String(item.value || "") === missionValue);
    if (!radio) return fail(`Could not find large mission option ${missionValue}.`);
    radio.checked = true;
    radio.click();
    dispatch(radio);
    if (config.amount) {
      clickIfPresent(`.btn-event_amount[data-amount="${config.amount}"]`);
    }
    if (config.size) {
      clickIfPresent(`.btn-event_expansion[expansion_id="${config.size}"]`);
    }
    if (config.shape) {
      clickIfPresent(`.btn-event_shape[data-shape="${config.shape}"]`);
    }
  }

  const latitude = Number(config.latitude);
  const longitude = Number(config.longitude);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
    return fail("No valid latitude/longitude was configured.");
  }

  if (typeof mission_position_new_marker === "undefined") {
    if (typeof isLeaflet === "function" && isLeaflet() && typeof L !== "undefined" && typeof map !== "undefined") {
      mission_position_new_marker = L.marker([latitude, longitude], { draggable: true, zIndexOffset: 100000 }).addTo(map);
    } else if (typeof mapkit !== "undefined" && typeof map !== "undefined") {
      mission_position_new_marker = new mapkit.MarkerAnnotation(new mapkit.Coordinate(latitude, longitude), {
        draggable: true,
        color: "#000000",
      });
      map.addAnnotation(mission_position_new_marker);
    } else {
      return fail("MissionChief map marker is not available.");
    }
  }

  if (typeof isLeaflet === "function" && isLeaflet()) {
    if (typeof mission_position_new_marker.setLatLng !== "function") {
      return fail("Leaflet marker cannot be moved.");
    }
    mission_position_new_marker.setLatLng([latitude, longitude]);
    if (typeof map !== "undefined" && typeof map.setView === "function") {
      map.setView([latitude, longitude], map.getZoom ? map.getZoom() : undefined);
    }
  } else if (typeof mapkit !== "undefined") {
    mission_position_new_marker.coordinate = new mapkit.Coordinate(latitude, longitude);
  }

  if (typeof mission_position_new_dragend === "function") {
    mission_position_new_dragend();
  } else {
    setField("mission_position[latitude]", latitude);
    setField("mission_position[longitude]", longitude);
    if (typeof updateAddress === "function") updateAddress();
  }

  const expectedLatitude = String(latitude);
  const expectedLongitude = String(longitude);
  for (let attempt = 0; attempt < 30; attempt += 1) {
    if (fieldValue("mission_position[latitude]") && fieldValue("mission_position[longitude]")) break;
    await sleep(100);
  }
  if (!fieldValue("mission_position[latitude]") || !fieldValue("mission_position[longitude]")) {
    return fail("MissionChief did not accept the marker coordinates.");
  }
  if (!fieldValue("mission_position[address]") && typeof updateAddress === "function") {
    updateAddress();
  }
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (fieldValue("mission_position[address]")) break;
    await sleep(100);
  }
  if (!fieldValue("mission_position[address]")) {
    return fail("MissionChief did not resolve an address for the marker.");
  }

  const availableButtons = buttons();
  const startIndex = availableButtons.findIndex((button) => {
    const text = visibleText(button).toLowerCase();
    if (button.disabled || button.hasAttribute("disabled")) return false;
    if (!text.includes("start")) return false;
    if (config.allowCoins) return text.includes("free") || text.includes("coin");
    return text.includes("free") && !text.includes("coin");
  });
  if (startIndex < 0) {
    return fail(`No enabled ${config.allowCoins ? "free or coin" : "free"} start button was found.`);
  }

  const buttonText = visibleText(availableButtons[startIndex]);
  const usesCoins = buttonText.toLowerCase().includes("coin") || fieldValue("mission_position[coins]") !== "0";
  if (usesCoins && !config.allowCoins) {
    return fail(`Refusing to spend coins with button: ${buttonText}`);
  }

  return {
    ok: true,
    submitIndex: startIndex,
    usesCoins,
    buttonText,
    expectedLatitude,
    expectedLongitude,
    snapshot: snapshot(),
  };
}
""".strip()
BROWSER_CLICK_START_SCRIPT = r"""
(submitIndex) => {
  const buttons = [...document.querySelectorAll('input[type="submit"], button[type="submit"], button:not([type])')];
  const button = buttons[submitIndex];
  if (!button) return false;
  button.click();
  return true;
}
""".strip()
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
    details: Dict[str, Any] = field(default_factory=dict)


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
    SHAPE_FIELD: "circle",
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
RANDOM_TYPE_KEY = "random_type"
EVENT_ROUTE_PROFILE_PREFIX = "route_"
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
EVENT_ROUTE_LOCATIONS = [
    {
        "label": "New York City",
        "latitude": "40.712800",
        "longitude": "-74.006000",
        "address": "New York City, NY, USA",
    },
    {
        "label": "Portland, OR",
        "latitude": "45.515200",
        "longitude": "-122.678400",
        "address": "Portland, OR, USA",
    },
    {
        "label": "Los Angeles",
        "latitude": "34.052200",
        "longitude": "-118.243700",
        "address": "Los Angeles, CA, USA",
    },
    {
        "label": "Houma, Louisiana",
        "input_note": "User request said Houba, Louisiana; configured as Houma, Louisiana.",
        "latitude": "29.587614",
        "longitude": "-90.716108",
        "address": "Houma, LA, USA",
    },
    {
        "label": "San Francisco",
        "latitude": "37.774900",
        "longitude": "-122.419400",
        "address": "San Francisco, CA, USA",
    },
    {
        "label": "Sacramento",
        "latitude": "38.581600",
        "longitude": "-121.494400",
        "address": "Sacramento, CA, USA",
    },
    {
        "label": "Bermuda Islands",
        "latitude": "32.294800",
        "longitude": "-64.781400",
        "address": "Hamilton, Bermuda",
    },
    {
        "label": "Glasgow, UK",
        "input_note": "User request said Glowglow UK; configured as Glasgow, UK.",
        "latitude": "55.864200",
        "longitude": "-4.251800",
        "address": "Glasgow, Scotland, UK",
    },
    {
        "label": "Kobenhavn, Denmark",
        "latitude": "55.676100",
        "longitude": "12.568300",
        "address": "Copenhagen, Denmark",
    },
    {
        "label": "Beersheba, Israel",
        "latitude": "31.252973",
        "longitude": "34.791462",
        "address": "Beersheba, Israel",
    },
]


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


def route_profile_name(location: Dict[str, str]) -> str:
    """Return a stable profile name for a configured route location."""
    return profile_name_from_label(location.get("label", "location"), prefix=EVENT_ROUTE_PROFILE_PREFIX)


def route_profile_for_location(kind: str, location: Dict[str, str]) -> dict:
    """Build a saved profile that keeps location fixed and chooses MissionChief type at runtime."""
    kind = normalize_kind(kind)
    fields = {
        LATITUDE_FIELD: str(location["latitude"]),
        LONGITUDE_FIELD: str(location["longitude"]),
        ADDRESS_FIELD: str(location["address"]),
    }
    if kind == "event":
        fields.update(EVENT_DEFAULT_OVERRIDES)
    return {
        RANDOM_TYPE_KEY: True,
        "location_label": str(location["label"]),
        "fields": fields,
    }


def route_profile_names() -> List[str]:
    """Return route profile names in the exact rotation order."""
    return [route_profile_name(location) for location in EVENT_ROUTE_LOCATIONS]


def profile_with_selected_type(kind: str, profile: dict, option: FormOption) -> dict:
    """Return a copy of a profile with a concrete live MissionChief option selected."""
    kind = normalize_kind(kind)
    selected = dict(profile)
    fields = dict(selected.get("fields", {}))
    fields[MISSION_TYPE_FIELD] = option.value
    if kind == "event":
        fields[EVENT_RADIO_FIELD] = option.value
        fields.update(EVENT_DEFAULT_OVERRIDES)
    selected["fields"] = fields
    selected.pop(RANDOM_TYPE_KEY, None)
    selected["selected_type_label"] = option.label
    return selected


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


def _ajax_get_headers(kind: str) -> Dict[str, str]:
    return {
        "Accept": "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01",
        "Referer": BASE_URL,
        "X-Requested-With": "XMLHttpRequest",
    }


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


def _redact_debug_value(name: str, value: str) -> str:
    lowered = str(name or "").lower()
    if "token" in lowered or "cookie" in lowered or lowered in {"authorization", "x-csrf-token"}:
        return "REDACTED"
    return str(value or "")


def safe_debug_mapping(mapping: Dict[str, str]) -> str:
    if not mapping:
        return "none"
    return "\n".join(f"{key}: {_redact_debug_value(key, value)}" for key, value in sorted(mapping.items()))


def safe_debug_payload(payload: Payload) -> str:
    if not payload:
        return "none"
    return "\n".join(f"{name}={_redact_debug_value(name, value)}" for name, value in payload)


def summarize_browser_snapshot(snapshot: Optional[Dict[str, Any]], *, limit: int = 900) -> str:
    """Summarize non-sensitive browser state after a Playwright start attempt."""
    if not snapshot:
        return ""
    lines = []
    for key in ["url", "missionType", "eventRadio", "latitude", "longitude", "address", "coins", "lastFreeText"]:
        value = snapshot.get(key)
        if value:
            lines.append(f"{key}: {value}")
    buttons = snapshot.get("submitButtons") or []
    if buttons:
        lines.append("buttons:")
        for button in buttons[:5]:
            lines.append(
                "- {text} | disabled={disabled}".format(
                    text=button.get("text") or "(blank)",
                    disabled=button.get("disabled"),
                )
            )
    text = "; ".join(lines)
    if len(text) > limit:
        return text[: max(0, limit - 1)] + "..."
    return text


LAST_FREE_RE = re.compile(
    r"Last free mission:\s*([A-Za-z]{3},\s+\d{1,2}\s+[A-Za-z]{3}\s+\d{4}\s+\d{2}:\d{2}:\d{2}\s+[+-]\d{4})",
    re.IGNORECASE,
)


def parse_last_free_mission_time(value: str) -> Optional[datetime]:
    """Parse MissionChief's visible `Last free mission` timestamp."""
    match = LAST_FREE_RE.search(value or "")
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%a, %d %b %Y %H:%M:%S %z")
    except ValueError:
        return None


def next_free_start_from_text(kind: str, value: str) -> Optional[datetime]:
    """Return the next free start time implied by MissionChief's last-free text."""
    last_free = parse_last_free_mission_time(value)
    if not last_free:
        return None
    interval = timedelta(days=7 if normalize_kind(kind) == "event" else 1)
    return last_free + interval + timedelta(seconds=COOLDOWN_GRACE_SECONDS)


def parse_config_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO datetime from Config as an aware UTC datetime."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def config_datetime(value: datetime) -> str:
    """Serialize a datetime for Config storage in UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def schedule_run_key(kind: str, when: datetime) -> str:
    """Return the daily/weekly run key used to prevent duplicate scheduled starts."""
    if normalize_kind(kind) == "event":
        return f"{when:%G-W%V}"
    return when.strftime("%Y-%m-%d")


def next_nominal_schedule_time(kind: str, schedule: dict, last_runs: dict, now: datetime) -> Optional[datetime]:
    """Return the next intended scheduler attempt before cooldown retry overrides."""
    kind = normalize_kind(kind)
    if not schedule.get("enabled"):
        return None
    profile_name, _ = select_scheduled_profile(schedule)
    if not profile_name:
        return None

    hour, minute = valid_time(schedule.get("time") or "23:55")
    target_today = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    if kind == "large":
        current_key = schedule_run_key(kind, now)
        if now < target_today:
            return target_today
        if last_runs.get(kind) != current_key:
            return now
        return target_today + timedelta(days=1)

    weekday = WEEKDAYS.get((schedule.get("weekday") or "monday").lower(), 0)
    days_since_target = (now.weekday() - weekday) % 7
    current_week_target = (now - timedelta(days=days_since_target)).replace(
        hour=hour,
        minute=minute,
        second=0,
        microsecond=0,
    )
    current_key = schedule_run_key(kind, current_week_target)
    if now.date() == current_week_target.date():
        if now < current_week_target:
            return current_week_target
        if last_runs.get(kind) != current_key:
            return now
    days_until_target = (weekday - now.weekday()) % 7
    if days_until_target == 0:
        days_until_target = 7
    return target_today + timedelta(days=days_until_target)


def next_schedule_attempt_time(
    kind: str,
    schedule: dict,
    last_runs: dict,
    retry_after: dict,
    now: datetime,
) -> Optional[datetime]:
    """Return the next scheduler attempt, including cooldown retry throttling."""
    nominal = next_nominal_schedule_time(kind, schedule, last_runs, now)
    if not nominal:
        return None
    retry_at = parse_config_datetime(retry_after.get(kind))
    if retry_at and retry_at > datetime.now(timezone.utc):
        retry_local = retry_at.astimezone(now.tzinfo)
        if retry_local > nominal:
            return retry_local
    return nominal


def build_browser_event_start_script(
    fields: Dict[str, str],
    *,
    label: str = "EventManager event",
    allow_coins: bool = False,
) -> str:
    """Build a browser-console script that starts an event through the live MissionChief DOM."""
    allowed_fields = {
        EVENT_RADIO_FIELD,
        MISSION_TYPE_FIELD,
        LATITUDE_FIELD,
        LONGITUDE_FIELD,
        ADDRESS_FIELD,
        POI_TYPE_FIELD,
        SIZE_FIELD,
        SHAPE_FIELD,
        AMOUNT_FIELD,
        COINS_FIELD,
    }
    safe_fields = {
        str(name): str(value)
        for name, value in fields.items()
        if str(name) in allowed_fields
    }
    safe_fields.setdefault(COINS_FIELD, "0")
    config = {
        "label": str(label or "EventManager event"),
        "fields": safe_fields,
        "allowCoins": bool(allow_coins),
    }
    return BROWSER_EVENT_START_SCRIPT.replace("__CONFIG__", json.dumps(config, ensure_ascii=False, indent=2))


def build_browser_start_config(
    kind: str,
    profile: dict,
    *,
    label: str,
    allow_coins: bool = False,
) -> Dict[str, Any]:
    """Build the Playwright start configuration from an EventManager profile."""
    kind = normalize_kind(kind)
    fields = profile_fields_for_start(profile)
    latitude = fields.get(LATITUDE_FIELD)
    longitude = fields.get(LONGITUDE_FIELD)
    if not latitude or not longitude:
        raise ValueError("Browser starts require latitude and longitude.")

    config: Dict[str, Any] = {
        "kind": kind,
        "label": str(label or "EventManager"),
        "allowCoins": bool(allow_coins),
        "latitude": str(latitude),
        "longitude": str(longitude),
        "address": str(fields.get(ADDRESS_FIELD) or ""),
        "size": str(fields.get(SIZE_FIELD) or ""),
        "shape": str(fields.get(SHAPE_FIELD) or ""),
        "amount": str(fields.get(AMOUNT_FIELD) or ""),
        "duration": str(fields.get("mission_position[duration]") or ""),
    }
    if kind == "event":
        event_value = fields.get(EVENT_RADIO_FIELD) or fields.get(MISSION_TYPE_FIELD)
        if not event_value:
            raise ValueError("Browser event starts require an event type.")
        config["eventValue"] = str(event_value)
        config["missionType"] = str(fields.get(MISSION_TYPE_FIELD) or event_value)
        config["size"] = config["size"] or EVENT_DEFAULT_OVERRIDES[SIZE_FIELD]
        config["shape"] = config["shape"] or EVENT_DEFAULT_OVERRIDES[SHAPE_FIELD]
        config["amount"] = config["amount"] or EVENT_DEFAULT_OVERRIDES[AMOUNT_FIELD]
    else:
        mission_type = fields.get(MISSION_TYPE_FIELD)
        if not mission_type:
            raise ValueError("Browser large mission starts require a mission type.")
        config["missionType"] = str(mission_type)
        config["size"] = config["size"] or MISSION_POSITION_DEFAULT_OVERRIDES[SIZE_FIELD]
        config["shape"] = config["shape"] or MISSION_POSITION_DEFAULT_OVERRIDES[SHAPE_FIELD]
        config["amount"] = config["amount"] or MISSION_POSITION_DEFAULT_OVERRIDES[AMOUNT_FIELD]
    return config


def browser_result_details(
    kind: str,
    profile_name: str,
    prepare_result: Optional[Dict[str, Any]],
    *,
    status: Optional[int] = None,
    post_url: Optional[str] = None,
) -> Dict[str, Any]:
    """Build non-sensitive details from the browser start result for logs and scheduling."""
    prepare_result = prepare_result or {}
    snapshot = prepare_result.get("snapshot") or {}
    last_free_text = str(snapshot.get("lastFreeText") or "")
    next_eligible_at = next_free_start_from_text(kind, last_free_text)
    details: Dict[str, Any] = {
        "kind": normalize_kind(kind),
        "profile": profile_name,
        "status": status,
        "post_url": post_url,
        "button_text": prepare_result.get("buttonText"),
        "uses_coins": prepare_result.get("usesCoins"),
        "latitude": snapshot.get("latitude") or prepare_result.get("expectedLatitude"),
        "longitude": snapshot.get("longitude") or prepare_result.get("expectedLongitude"),
        "address": snapshot.get("address"),
        "mission_type": snapshot.get("missionType"),
        "event_radio": snapshot.get("eventRadio"),
        "last_free_text": last_free_text,
        "snapshot_summary": summarize_browser_snapshot(snapshot),
    }
    if next_eligible_at:
        details["next_eligible_at"] = next_eligible_at
    return {key: value for key, value in details.items() if value not in (None, "", [])}


def normalize_optional_profile_arg(value: Optional[str]) -> Optional[str]:
    """Normalize optional command profile arguments and ignore documented placeholders."""
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized in {"", "profile", "[profile]", "<profile>", "{profile}"}:
        return None
    return normalized


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
        safe_value = _redact_debug_value(field_info.name, field_info.value)
        value = f" = {safe_value}" if safe_value else ""
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


class EventRequestPanelView(discord.ui.View):
    """Persistent member entry point for requesting an alliance event or mission."""

    def __init__(self, cog: "EventManager"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Request Event / Mission",
        style=discord.ButtonStyle.primary,
        custom_id="eventmanager:request:create",
    )
    async def request_item(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message("Event requests can only be created in the server.", ephemeral=True)
            return
        await interaction.response.send_modal(EventRequestModal(self.cog))


class EventRequestModal(discord.ui.Modal, title="Request Event / Mission"):
    """Collect a lightweight member request without starting anything automatically."""

    request_type = discord.ui.TextInput(
        label="Request type",
        placeholder="Alliance event or large scale mission",
        required=True,
        max_length=80,
    )
    preferred_type = discord.ui.TextInput(
        label="Preferred MissionChief type",
        placeholder="Example: Storm, Major fire, Bomb Explosion",
        required=False,
        max_length=100,
    )
    location = discord.ui.TextInput(
        label="Location preference",
        placeholder="Example: New York City, Bermuda, no preference",
        required=False,
        max_length=160,
    )
    notes = discord.ui.TextInput(
        label="Reason / timing notes",
        placeholder="Optional details for admins",
        required=False,
        style=discord.TextStyle.paragraph,
        max_length=700,
    )

    def __init__(self, cog: "EventManager"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        request_id = await self.cog.create_event_request(
            interaction,
            request_type=str(self.request_type.value or "").strip(),
            preferred_type=str(self.preferred_type.value or "").strip(),
            location=str(self.location.value or "").strip(),
            notes=str(self.notes.value or "").strip(),
        )
        await interaction.response.send_message(
            f"Your event request has been submitted for admin review. Reference: `{request_id}`",
            ephemeral=True,
        )


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
            schedule_retry_after={},
            log_channel_id=None,
            panel_channel_id=DEFAULT_PANEL_CHANNEL_ID,
            panel_message_id=None,
            request_panel_channel_id=None,
            request_panel_message_id=None,
            request_log_channel_id=None,
            event_requests=[],
        )
        self._task: Optional[asyncio.Task] = None
        self._panel_task: Optional[asyncio.Task] = None
        self._start_lock = asyncio.Lock()

    async def cog_load(self):
        self.bot.add_view(EventManagerPanelView(self))
        self.bot.add_view(EventRequestPanelView(self))
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

    async def _fetch_form(self, kind: str, fields: Optional[Dict[str, str]] = None, *, ajax: bool = False) -> EventForm:
        kind = normalize_kind(kind)
        page_url = EVENT_KINDS[kind]["url"]
        params = _form_position_params(fields or {})
        session = await self._get_session()
        async with session.get(
            page_url,
            allow_redirects=True,
            params=params or None,
            headers=_ajax_get_headers(kind) if ajax else None,
        ) as response:
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

    async def _build_safe_diagnostics(self, kind: str, profile_name: Optional[str] = None) -> Tuple[str, str]:
        """Build a safe no-submit diagnostics report for a MissionChief start flow."""
        kind = normalize_kind(kind)
        initial_form = await self._fetch_form(kind)
        label = "quick"
        profile = None
        if profile_name:
            label = profile_name.strip().lower()
            profiles = await self.config.profiles()
            profile = profiles.get(kind, {}).get(label)
            if not profile:
                raise ValueError(f"Profile `{label}` was not found.")
        else:
            options = field_options_for_kind(initial_form, kind)
            if not options:
                raise ValueError("No MissionChief options were found on the live form.")
            random_region = "nyc_or_bermuda" if kind == "event" else "nyc"
            profile = fields_for_selection(kind, options[0].value, random_region=random_region)

        start_fields = profile_fields_for_start(profile)
        form_params = _form_position_params(start_fields)
        form = await self._fetch_form(kind, start_fields)
        try:
            ajax_form = await self._fetch_form(kind, start_fields, ajax=True)
            ajax_form_summary = summarize_form(ajax_form, limit=len(ajax_form.fields))
        except Exception as exc:
            ajax_form_summary = f"AJAX form fetch failed or did not parse: {exc}"
        payload_before_address = build_payload(form, start_fields)
        payload = await self._resolve_reverse_address(await self._get_session(), kind, payload_before_address)
        headers = _ajax_submit_headers(kind, payload)
        reverse_address_changed = _payload_value(payload_before_address, ADDRESS_FIELD) != _payload_value(payload, ADDRESS_FIELD)

        lines = [
            "EventManager Safe Diagnostics",
            "NO POST WAS SENT. NO MISSION OR EVENT WAS STARTED.",
            "",
            f"Kind: {kind}",
            f"Profile: {label}",
            f"Form URL: {EVENT_KINDS[kind]['url']}",
            f"Form GET params: {form_params or 'none'}",
            "",
            "[Form GET headers if AJAX]",
            safe_debug_mapping(_ajax_get_headers(kind)),
            "",
            f"Action: {form.action}",
            f"Method: {form.method}",
            f"Submit: {form.submit_name}={form.submit_value or ''}",
            f"Reverse address changed: {reverse_address_changed}",
            "",
            "[Start fields]",
            safe_debug_mapping(start_fields),
            "",
            "[POST headers]",
            safe_debug_mapping(headers),
            "",
            "[POST payload before reverse_address]",
            safe_debug_payload(payload_before_address),
            "",
            "[POST payload after reverse_address]",
            safe_debug_payload(payload),
            "",
            "[Safe payload summary]",
            summarize_payload_for_debug(payload, limit=4000),
            "",
            "[Parsed form]",
            summarize_form(form, limit=len(form.fields)),
            "",
            "[Parsed AJAX form comparison]",
            ajax_form_summary,
        ]
        return "\n".join(lines), f"eventmanager-{kind}-diagnostics.txt"

    async def _playwright_cookies(self) -> List[Dict[str, str]]:
        """Copy CookieManager's aiohttp cookies into Playwright cookie format."""
        session = await self._get_session()
        cookie_jar = getattr(session, "cookie_jar", None)
        if not cookie_jar:
            return []
        cookies = cookie_jar.filter_cookies(BASE_URL)
        playwright_cookies = []
        for name, morsel in cookies.items():
            value = getattr(morsel, "value", str(morsel))
            if not name or not value:
                continue
            playwright_cookies.append({"name": str(name), "value": str(value), "url": BASE_URL})
        return playwright_cookies

    async def _resolve_profile_runtime_options(self, kind: str, profile: dict) -> dict:
        """Resolve runtime markers like random MissionChief type before starting."""
        kind = normalize_kind(kind)
        if not profile.get(RANDOM_TYPE_KEY):
            return profile
        form = await self._fetch_form(kind)
        options = field_options_for_kind(form, kind)
        if not options:
            raise RuntimeError(f"No {EVENT_KINDS[kind]['label']} options were found on the live MissionChief form.")
        return profile_with_selected_type(kind, profile, random.choice(options))

    async def _start_profile_data_browser(
        self,
        kind: str,
        profile_name: str,
        profile: dict,
        *,
        allow_coins: bool = False,
    ) -> EventStartResult:
        """Start a MissionChief item by driving the live map/form in a headless browser."""
        kind = normalize_kind(kind)
        try:
            profile = await self._resolve_profile_runtime_options(kind, profile)
            config = build_browser_start_config(kind, profile, label=profile_name, allow_coins=allow_coins)
        except Exception as exc:
            return EventStartResult(False, str(exc))

        try:
            from playwright.async_api import TimeoutError as PlaywrightTimeoutError
            from playwright.async_api import async_playwright
        except Exception:
            return EventStartResult(False, PLAYWRIGHT_SETUP_MESSAGE)

        async with self._start_lock:
            try:
                cookies = await self._playwright_cookies()
            except Exception as exc:
                return EventStartResult(False, f"Could not load MissionChief cookies: {exc}")
            if not cookies:
                return EventStartResult(False, "No MissionChief cookies are available from CookieManager.")

            open_selector = "#btn-alliance-new-event" if kind == "event" else "#btn-alliance-new-mission"
            create_path = "missionAllianceEventCreate" if kind == "event" else "missionAllianceCreate"
            status: Optional[int] = None
            response_text = ""
            prepare_result: Dict[str, Any] = {}
            try:
                async with async_playwright() as playwright:
                    browser = await playwright.chromium.launch(headless=True)
                    try:
                        context = await browser.new_context(viewport={"width": 1440, "height": 1000})
                        await context.add_cookies(cookies)
                        page = await context.new_page()
                        page.set_default_timeout(30000)
                        await page.goto(MISSIONCHIEF_HOME_URL, wait_until="domcontentloaded")

                        open_button = page.locator(open_selector)
                        if await open_button.count() == 0:
                            login_fields = await page.locator("input[type='password']").count()
                            if login_fields:
                                return EventStartResult(False, "MissionChief session is not logged in.")
                            return EventStartResult(False, f"MissionChief start button `{open_selector}` was not found.")

                        await open_button.nth(0).click()
                        await page.wait_for_selector("#new_mission_position", state="attached")
                        with suppress(Exception):
                            await page.wait_for_function(
                                "typeof mission_position_new_marker !== 'undefined' || typeof map !== 'undefined'",
                                timeout=15000,
                            )

                        prepare_result = await page.evaluate(BROWSER_PREPARE_START_SCRIPT, config)
                        if not prepare_result.get("ok"):
                            snapshot = summarize_browser_snapshot(prepare_result.get("snapshot"))
                            suffix = f" Snapshot: {snapshot}" if snapshot else ""
                            details = browser_result_details(kind, profile_name, prepare_result, post_url=f"{BASE_URL}/{create_path}")
                            return EventStartResult(False, f"{prepare_result.get('reason')}{suffix}", details=details)

                        async with page.expect_response(lambda response: create_path in response.url, timeout=30000) as response_info:
                            clicked = await page.evaluate(BROWSER_CLICK_START_SCRIPT, prepare_result.get("submitIndex"))
                            if not clicked:
                                details = browser_result_details(kind, profile_name, prepare_result, post_url=f"{BASE_URL}/{create_path}")
                                return EventStartResult(False, "Browser could not click the MissionChief start button.", details=details)
                        response = await response_info.value
                        status = response.status
                        with suppress(Exception):
                            response_text = await response.text()
                    finally:
                        await browser.close()
            except PlaywrightTimeoutError as exc:
                snapshot = summarize_browser_snapshot(prepare_result.get("snapshot"))
                suffix = f" Snapshot: {snapshot}" if snapshot else ""
                details = browser_result_details(kind, profile_name, prepare_result, status=status, post_url=f"{BASE_URL}/{create_path}")
                return EventStartResult(False, f"MissionChief browser flow timed out: {exc}{suffix}", status=status, post_url=f"{BASE_URL}/{create_path}", details=details)
            except Exception as exc:
                message = str(exc)
                if "Executable doesn't exist" in message or "playwright install" in message:
                    return EventStartResult(False, PLAYWRIGHT_SETUP_MESSAGE)
                return EventStartResult(False, f"MissionChief browser flow failed: {message}")

        if status is None or int(status) >= 400:
            response_debug = summarize_response_for_debug(response_text)
            response_suffix = f" Response: {response_debug}" if response_debug else ""
            details = browser_result_details(kind, profile_name, prepare_result, status=status, post_url=f"{BASE_URL}/{create_path}")
            return EventStartResult(
                False,
                f"MissionChief returned HTTP {status} from browser start.{response_suffix}",
                status=status,
                post_url=f"{BASE_URL}/{create_path}",
                details=details,
            )

        details = browser_result_details(kind, profile_name, prepare_result, status=status, post_url=f"{BASE_URL}/{create_path}")
        await self._log_run(kind, profile_name, status, details=details)
        return EventStartResult(
            True,
            "Started successfully through browser automation.",
            status=status,
            post_url=f"{BASE_URL}/{create_path}",
            details=details,
        )

    async def _start_profile_data_http(self, kind: str, profile_name: str, profile: dict) -> EventStartResult:
        kind = normalize_kind(kind)
        async with self._start_lock:
            try:
                profile = await self._resolve_profile_runtime_options(kind, profile)
                start_fields = profile_fields_for_start(profile)
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

    async def _start_profile_data(self, kind: str, profile_name: str, profile: dict) -> EventStartResult:
        return await self._start_profile_data_browser(kind, profile_name, profile)

    async def _start_from_profile(self, kind: str, profile_name: str, *, allow_coins: bool = False) -> EventStartResult:
        kind = normalize_kind(kind)
        profile_name = profile_name.strip().lower()
        profiles = await self.config.profiles()
        profile = profiles.get(kind, {}).get(profile_name)
        if not profile:
            return EventStartResult(False, f"Profile `{profile_name}` was not found.")
        if allow_coins:
            return await self._start_profile_data_browser(kind, profile_name, profile, allow_coins=True)
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
                "Start MissionChief alliance missions and events through the live map.\n\n"
                "Quick uses configured/default settings. Custom lets admins choose the live MissionChief option "
                "and location before starting."
            ),
            color=discord.Color.orange(),
        )
        embed.add_field(name="Safety", value="Default starts use the browser backend and refuse coin actions.", inline=False)
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

    async def request_panel_channel(self, guild: discord.Guild):
        channel_id = await self.config.request_panel_channel_id()
        if not channel_id:
            channel_id = await self.config.panel_channel_id() or DEFAULT_PANEL_CHANNEL_ID
        return guild.get_channel(int(channel_id)) or self.bot.get_channel(int(channel_id))

    def is_request_panel_message(self, message: discord.Message) -> bool:
        bot_user_id = getattr(getattr(self.bot, "user", None), "id", None)
        if bot_user_id and getattr(getattr(message, "author", None), "id", None) != bot_user_id:
            return False
        for embed in getattr(message, "embeds", []) or []:
            if getattr(embed, "title", None) == REQUEST_PANEL_TITLE:
                return True
        return False

    async def build_request_panel_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=REQUEST_PANEL_TITLE,
            description=(
                "Request a large scale alliance mission or alliance event for admin review.\n\n"
                "This does not start anything automatically. Staff will review the request first."
            ),
            color=discord.Color.blurple(),
        )
        embed.add_field(name="Privacy", value="Your request confirmation is private to you.", inline=False)
        return embed

    async def find_existing_request_panel_message(self, channel: Any):
        history = getattr(channel, "history", None)
        if not history:
            return None
        found = []
        with suppress(discord.Forbidden, discord.HTTPException):
            async for message in channel.history(limit=50):
                if self.is_request_panel_message(message):
                    found.append(message)
        if not found:
            return None
        keep = found[0]
        for duplicate in found[1:]:
            with suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                await duplicate.delete()
        return keep

    async def ensure_request_panel_message(self, guild: discord.Guild, *, create: bool = True):
        channel = await self.request_panel_channel(guild)
        if not channel:
            return None
        embed = await self.build_request_panel_embed()
        view = EventRequestPanelView(self)
        message_id = await self.config.request_panel_message_id()
        if message_id and hasattr(channel, "fetch_message"):
            with suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                message = await channel.fetch_message(int(message_id))
                await message.edit(embed=embed, view=view)
                return message

        existing = await self.find_existing_request_panel_message(channel)
        if existing:
            await existing.edit(embed=embed, view=view)
            await self.config.request_panel_message_id.set(existing.id)
            return existing
        if not create:
            return None
        message = await channel.send(embed=embed, view=view)
        await self.config.request_panel_message_id.set(message.id)
        return message

    async def create_event_request(
        self,
        interaction: discord.Interaction,
        *,
        request_type: str,
        preferred_type: str,
        location: str,
        notes: str,
    ) -> str:
        created_at = datetime.now(timezone.utc)
        user = interaction.user
        request_id = f"EVT-{created_at:%Y%m%d%H%M%S}-{getattr(user, 'id', 0) % 10000:04d}"
        record = {
            "id": request_id,
            "created_at": config_datetime(created_at),
            "status": "pending",
            "discord_user_id": getattr(user, "id", None),
            "discord_name": str(user),
            "display_name": getattr(user, "display_name", str(user)),
            "request_type": request_type,
            "preferred_type": preferred_type,
            "location": location,
            "notes": notes,
        }
        async with self.config.event_requests() as requests:
            requests.append(record)
        await self._log_event_request(interaction.guild, record)
        return request_id

    async def _request_log_channel(self, guild: Optional[discord.Guild]):
        channel_id = await self.config.request_log_channel_id()
        if not channel_id:
            channel_id = await self.config.log_channel_id()
        if not channel_id:
            return None
        if guild:
            return guild.get_channel(int(channel_id)) or self.bot.get_channel(int(channel_id))
        return self.bot.get_channel(int(channel_id))

    async def _log_event_request(self, guild: Optional[discord.Guild], record: Dict[str, Any]):
        channel = await self._request_log_channel(guild)
        if not channel:
            return
        embed = discord.Embed(
            title="New EventManager request",
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc),
        )
        user_id = record.get("discord_user_id")
        requested_by = f"<@{user_id}>" if user_id else record.get("display_name") or "Unknown"
        embed.add_field(name="Reference", value=str(record.get("id")), inline=True)
        embed.add_field(name="Requested by", value=requested_by, inline=True)
        embed.add_field(name="Request type", value=truncate_discord_text(record.get("request_type") or "Not specified", 1024), inline=False)
        embed.add_field(name="Preferred type", value=truncate_discord_text(record.get("preferred_type") or "No preference", 1024), inline=False)
        embed.add_field(name="Location", value=truncate_discord_text(record.get("location") or "No preference", 1024), inline=False)
        if record.get("notes"):
            embed.add_field(name="Notes", value=truncate_discord_text(record["notes"], 1024), inline=False)
        await channel.send(embed=embed)

    async def _log_run(
        self,
        kind: str,
        profile_name: str,
        status: Optional[int],
        *,
        details: Optional[Dict[str, Any]] = None,
    ):
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
        details = details or {}
        button_text = details.get("button_text")
        if button_text:
            embed.add_field(name="Button", value=truncate_discord_text(str(button_text), 1024), inline=False)
        location = details.get("address") or "Unknown"
        latitude = details.get("latitude")
        longitude = details.get("longitude")
        if latitude and longitude:
            location = f"{location}\n`{latitude}, {longitude}`"
        embed.add_field(name="Location", value=truncate_discord_text(str(location), 1024), inline=False)
        uses_coins = details.get("uses_coins")
        if uses_coins is not None:
            embed.add_field(name="Used Coins", value="Yes" if uses_coins else "No", inline=True)
        await channel.send(embed=embed)

    async def _run_due_schedules(self):
        schedules = await self.config.schedules()
        last_runs = await self.config.last_runs()
        retry_after = await self.config.schedule_retry_after()
        schedule_changed = False
        last_runs_changed = False
        retry_changed = False

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

            retry_at = parse_config_datetime(retry_after.get(kind))
            if retry_at and retry_at > datetime.now(timezone.utc):
                continue

            result = await self._start_from_profile(kind, profile_name)
            if result.ok:
                last_runs[kind] = run_key
                schedule["profile"] = profile_name
                schedule["rotation_index"] = next_rotation_index
                retry_after.pop(kind, None)
                last_runs_changed = True
                schedule_changed = True
                retry_changed = True
            else:
                next_retry = result.details.get("next_eligible_at") if result.details else None
                if not isinstance(next_retry, datetime) or next_retry <= datetime.now(timezone.utc):
                    next_retry = datetime.now(timezone.utc) + timedelta(seconds=SCHEDULE_RETRY_SECONDS)
                retry_after[kind] = config_datetime(next_retry)
                retry_changed = True
                log.warning("Scheduled %s failed: %s", kind, result.reason)

        if last_runs_changed:
            await self.config.last_runs.set(last_runs)
        if schedule_changed:
            await self.config.schedules.set(schedules)
        if retry_changed:
            await self.config.schedule_retry_after.set(retry_after)

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

    @eventmanager.command(name="diagnose")
    @commands.admin()
    async def diagnose_start_flow(self, ctx: commands.Context, kind: str, profile_name: Optional[str] = None):
        """Create a safe no-submit diagnostics report for a MissionChief start flow."""
        try:
            report, filename = await self._build_safe_diagnostics(kind, profile_name)
        except Exception as exc:
            await ctx.send(f"Could not build diagnostics: {exc}")
            return

        data = io.BytesIO(report.encode("utf-8"))
        await ctx.send(
            "Safe EventManager diagnostics generated. No mission/event was started.",
            file=discord.File(data, filename=filename),
        )

    @eventmanager.command(name="browsercapture")
    @commands.admin()
    async def browser_capture_script(self, ctx: commands.Context):
        """Send a no-submit browser snippet for capturing the live MissionChief form DOM."""
        instructions = (
            "Open the MissionChief large mission/event form in your browser, move the marker, "
            "then run this snippet in the browser console. It only reads the DOM and does not submit the form."
        )
        data = io.BytesIO(BROWSER_CAPTURE_SCRIPT.encode("utf-8"))
        await ctx.send(
            instructions,
            file=discord.File(data, filename="eventmanager-browser-capture.js"),
        )

    async def _send_browser_event_start_script(
        self,
        ctx: commands.Context,
        profile_name: Optional[str],
        *,
        allow_coins: bool,
    ):
        try:
            form = await self._fetch_form("event")
        except Exception as exc:
            await ctx.send(f"Could not load the live MissionChief event form: {exc}")
            return

        label = "quick"
        normalized_profile = normalize_optional_profile_arg(profile_name)
        if normalized_profile:
            label = normalized_profile
            profiles = await self.config.profiles()
            profile = profiles.get("event", {}).get(label)
            if not profile:
                await ctx.send(f"Event profile `{label}` was not found.")
                return
        else:
            options = field_options_for_kind(form, "event")
            if not options:
                await ctx.send("No MissionChief event options were found on the live form.")
                return
            profile = fields_for_selection("event", options[0].value, random_region="nyc_or_bermuda")
            label = f"quick:{options[0].label}"

        start_fields = _normalize_overrides(form, profile_fields_for_start(profile))
        script = build_browser_event_start_script(start_fields, label=label, allow_coins=allow_coins)
        if allow_coins:
            instructions = (
                "Open https://www.missionchief.com/missionAllianceEventNew in the same MissionChief account, "
                "paste this script in the browser console, and review every confirmation. "
                "This script may click an enabled coin Start Event button, but only after you type SPEND COINS."
            )
            filename_suffix = f"{normalized_profile or 'quick'}-coins"
        else:
            instructions = (
                "Open https://www.missionchief.com/missionAllianceEventNew in the same MissionChief account, "
                "paste this script in the browser console, review the confirmation, and only then approve it. "
                "This script clicks only the free Start Event button and refuses coin payloads."
            )
            filename_suffix = normalized_profile or "quick"
        data = io.BytesIO(script.encode("utf-8"))
        await ctx.send(
            instructions,
            file=discord.File(data, filename=f"eventmanager-browser-event-{filename_suffix}.js"),
        )

    @eventmanager.command(name="browsereventscript")
    @commands.admin()
    async def browser_event_start_script(self, ctx: commands.Context, profile_name: Optional[str] = None):
        """Send a browser-console script that starts an alliance event through the live DOM."""
        await self._send_browser_event_start_script(ctx, profile_name, allow_coins=False)

    @eventmanager.command(name="browsereventcoinscript")
    @commands.admin()
    async def browser_event_coin_start_script(self, ctx: commands.Context, profile_name: Optional[str] = None):
        """Send a browser-console script that may start an alliance event with coins."""
        await self._send_browser_event_start_script(ctx, profile_name, allow_coins=True)

    @eventmanager.command(name="browsercheck")
    @commands.admin()
    async def browser_backend_check(self, ctx: commands.Context):
        """Check whether Playwright browser automation is installed and launchable."""
        try:
            from playwright.async_api import async_playwright
        except Exception:
            await ctx.send(PLAYWRIGHT_SETUP_MESSAGE)
            return

        try:
            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=True)
                await browser.close()
        except Exception as exc:
            message = str(exc)
            if "Executable doesn't exist" in message or "playwright install" in message:
                await ctx.send(PLAYWRIGHT_SETUP_MESSAGE)
                return
            await ctx.send(f"Playwright is installed, but Chromium could not launch: {message}")
            return

        await ctx.send("EventManager browser backend is ready.")

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

    @eventmanager.command(name="startcoins")
    @commands.admin()
    async def start_profile_with_coins(self, ctx: commands.Context, kind: str, profile_name: str):
        """Start a saved profile and explicitly allow MissionChief coin actions."""
        try:
            kind = normalize_kind(kind)
        except ValueError as exc:
            await ctx.send(str(exc))
            return
        await ctx.send(
            "Coin start requested. EventManager will only continue if MissionChief shows an enabled start button."
        )
        result = await self._start_from_profile(kind, profile_name, allow_coins=True)
        if result.ok:
            await ctx.send(f"Started {EVENT_KINDS[kind]['label']} with profile `{profile_name}` using the browser backend.")
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

    @profile.command(name="seedrouteschedule")
    @commands.admin()
    async def profile_seed_route_schedule(
        self,
        ctx: commands.Context,
        daily_time: str = "07:00",
        weekly_day: str = "saturday",
        weekly_time: str = "07:00",
    ):
        """Create the fixed location rotation for daily missions and weekly events."""
        weekly_day = weekly_day.strip().lower()
        try:
            valid_time(daily_time)
            valid_time(weekly_time)
        except ValueError as exc:
            await ctx.send(str(exc))
            return
        if weekly_day not in WEEKDAYS:
            await ctx.send(f"Weekday must be one of: {', '.join(WEEKDAYS)}")
            return

        profile_names = route_profile_names()
        async with self.config.profiles() as profiles:
            for kind in ("large", "event"):
                kind_profiles = profiles.setdefault(kind, {})
                for location in EVENT_ROUTE_LOCATIONS:
                    kind_profiles[route_profile_name(location)] = route_profile_for_location(kind, location)

        async with self.config.schedules() as schedules:
            schedules["large"].update(
                enabled=True,
                profile=profile_names[0],
                profiles=profile_names,
                rotation_index=0,
                time=daily_time,
                timezone=DEFAULT_TIMEZONE,
                weekday=None,
            )
            schedules["event"].update(
                enabled=True,
                profile=profile_names[0],
                profiles=profile_names,
                rotation_index=0,
                time=weekly_time,
                timezone=DEFAULT_TIMEZONE,
                weekday=weekly_day,
            )
        async with self.config.schedule_retry_after() as retry_after:
            retry_after.pop("large", None)
            retry_after.pop("event", None)

        notes = [location["input_note"] for location in EVENT_ROUTE_LOCATIONS if location.get("input_note")]
        lines = [
            "Created route profiles for both large missions and alliance events.",
            f"Large scale mission schedule: daily at {daily_time} {DEFAULT_TIMEZONE}.",
            f"Alliance event schedule: {weekly_day} at {weekly_time} {DEFAULT_TIMEZONE}.",
            "Alliance event defaults: Large area, Circle, Every 30 seconds.",
            "MissionChief type: random live option at start time.",
            "",
            "Rotation order:",
            ", ".join(profile_names),
        ]
        if notes:
            lines.extend(["", "Location notes:", *notes])
        await ctx.send(box("\n".join(lines), lang="ini"))

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
        if profile.get(RANDOM_TYPE_KEY):
            lines.append(f"{RANDOM_TYPE_KEY} = true")
        if profile.get("location_label"):
            lines.append(f"location_label = {profile['location_label']}")
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
        last_runs = await self.config.last_runs()
        retry_after = await self.config.schedule_retry_after()
        lines = []
        for kind, schedule in schedules.items():
            profiles = schedule.get("profiles") or [schedule.get("profile")]
            profiles = [profile for profile in profiles if profile]
            timezone_name = schedule.get("timezone") or DEFAULT_TIMEZONE
            now = datetime.now(ZoneInfo(timezone_name))
            next_attempt = next_schedule_attempt_time(kind, schedule, last_runs, retry_after, now)
            next_attempt_text = next_attempt.isoformat() if next_attempt else "not scheduled"
            retry_at = parse_config_datetime(retry_after.get(kind))
            retry_text = retry_at.isoformat() if retry_at else "none"
            lines.append(
                f"{kind}: enabled={schedule.get('enabled')} profiles={profiles or 'none'} "
                f"time={schedule.get('time')} timezone={schedule.get('timezone')} "
                f"weekday={schedule.get('weekday')} rotation_index={schedule.get('rotation_index', 0)} "
                f"last_run={last_runs.get(kind, 'none')} retry_after={retry_text} next_attempt={next_attempt_text}"
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

    @schedule.command(name="clearretry")
    @commands.admin()
    async def schedule_clear_retry(self, ctx: commands.Context, kind: str):
        """Clear a stored cooldown retry for one automatic schedule."""
        try:
            kind = normalize_kind(kind)
        except ValueError as exc:
            await ctx.send(str(exc))
            return
        async with self.config.schedule_retry_after() as retry_after:
            retry_after.pop(kind, None)
        await ctx.tick()

    @eventmanager.group(name="request", invoke_without_command=True)
    @commands.admin()
    async def request(self, ctx: commands.Context):
        """Manage member EventManager requests."""
        requests = await self.config.event_requests()
        pending = [item for item in requests if item.get("status") == "pending"]
        if not pending:
            await ctx.send("No pending EventManager requests.")
            return
        lines = []
        for item in pending[-15:]:
            lines.append(
                "{id}: {display_name} | {request_type} | {preferred_type} | {location}".format(
                    id=item.get("id", "unknown"),
                    display_name=item.get("display_name") or item.get("discord_name") or "Unknown",
                    request_type=item.get("request_type") or "not specified",
                    preferred_type=item.get("preferred_type") or "no preference",
                    location=item.get("location") or "no preference",
                )
            )
        await ctx.send(box("\n".join(lines), lang="ini"))

    @request.command(name="panel")
    @commands.admin()
    async def request_panel(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        """Post or refresh the member EventManager request panel."""
        if channel is not None:
            await self.config.request_panel_channel_id.set(channel.id)
            await self.config.request_panel_message_id.set(None)
        message = await self.ensure_request_panel_message(ctx.guild, create=True)
        if message:
            await ctx.send(f"EventManager request panel ready in {message.channel.mention}: {message.jump_url}")
        else:
            await ctx.send("Could not post the EventManager request panel. Check the configured channel and bot permissions.")

    @request.command(name="logchannel")
    @commands.admin()
    async def request_logchannel(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        """Set or clear the EventManager request notification channel."""
        if channel is None:
            await self.config.request_log_channel_id.set(None)
            await ctx.send("EventManager request log channel cleared.")
            return
        await self.config.request_log_channel_id.set(channel.id)
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
