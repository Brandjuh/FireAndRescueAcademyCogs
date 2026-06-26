from __future__ import annotations

import asyncio
import aiohttp
import contextlib
from io import BytesIO
import json
import logging
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, quote, unquote, urlparse

import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box

log = logging.getLogger("red.cog.building_manager")

BASE_URL = "https://www.missionchief.com"
MISSIONCHIEF_HOME_URL = BASE_URL
MISSIONCHIEF_NEW_BUILDING_URL = f"{BASE_URL}/buildings/new"
MISSIONCHIEF_ALLIANCE_BUILDINGS_URL = f"{BASE_URL}/verband/gebauede"
DEFAULT_REQUEST_PANEL_CHANNEL_ID = 1421627971831070730
ALLIANCE_BUILDING_TYPE_IDS = {
    "Hospital": "2",
    "Prison": "10",
}
ALLIANCE_BUILDING_TARGET_TAX = 20
BUILDING_AUTOMATION_RETRY_SECONDS = 6 * 60 * 60
BUILDING_AUTOMATION_LOOP_SECONDS = 15 * 60
BUILDING_AUTOMATION_MAX_ACTIONS_PER_RUN = 24
BUILDING_AUTOMATION_MAX_EXTENSION_STARTS_PER_RUN = 3
BUILDING_AUTOMATION_EXCLUDED_EXTENSIONS = {
    "large hospital",
    "large prison",
}
PLAYWRIGHT_SETUP_MESSAGE = (
    "Playwright browser automation is not ready. Install the BuildingManager requirements and run "
    "`python -m playwright install chromium` in the same Python environment as Redbot."
)
REQUEST_PANEL_TITLE = "🏢 Building Request System"

BUILDING_DIAGNOSTICS_SCRIPT = r"""
() => {
  const visibleText = (element) => [element?.value, element?.textContent, element?.getAttribute?.("title"), element?.getAttribute?.("aria-label")]
    .filter(Boolean)
    .join(" ")
    .replace(/\s+/g, " ")
    .trim();
  const truncate = (value, limit = 180) => {
    const text = String(value || "").replace(/\s+/g, " ").trim();
    return text.length > limit ? `${text.slice(0, limit)}...` : text;
  };
  const redactedValue = (field) => {
    const name = String(field?.name || field?.id || "").toLowerCase();
    const type = String(field?.type || "").toLowerCase();
    if (name.includes("token") || name.includes("authenticity") || type === "password") return "REDACTED";
    return truncate(field?.value || "");
  };
  const keywords = /(alliance|verband|building|build|hospital|prison|jail|correction|detention|gebouw|gevangen|ziekenhuis)/i;
  const elementInfo = (element, index) => ({
    index,
    tag: element.tagName?.toLowerCase() || "",
    text: truncate(visibleText(element)),
    href: element.href || element.getAttribute?.("href") || "",
    id: element.id || "",
    name: element.name || "",
    type: element.type || "",
    className: String(element.className || ""),
    disabled: Boolean(element.disabled || element.hasAttribute?.("disabled")),
  });
  const candidateElements = [...document.querySelectorAll("a, button, input[type='button'], input[type='submit']")]
    .map(elementInfo)
    .filter((item) => keywords.test([item.text, item.href, item.id, item.name, item.className].join(" ")))
    .slice(0, 100);
  const forms = [...document.querySelectorAll("form")].slice(0, 20).map((form, formIndex) => {
    const fields = [...form.querySelectorAll("input, select, textarea, button")].slice(0, 140).map((field, fieldIndex) => ({
      index: fieldIndex,
      tag: field.tagName?.toLowerCase() || "",
      type: field.type || "",
      name: field.name || "",
      id: field.id || "",
      value: redactedValue(field),
      placeholder: truncate(field.placeholder || ""),
      text: truncate(visibleText(field)),
      checked: Boolean(field.checked),
      disabled: Boolean(field.disabled || field.hasAttribute?.("disabled")),
      options: field.tagName?.toLowerCase() === "select"
        ? [...field.options].slice(0, 30).map((option) => ({
            value: truncate(option.value, 80),
            text: truncate(option.textContent, 120),
            selected: Boolean(option.selected),
          }))
        : [],
    }));
    return {
      index: formIndex,
      id: form.id || "",
      action: form.action || form.getAttribute("action") || "",
      method: form.method || form.getAttribute("method") || "",
      className: String(form.className || ""),
      text: truncate(visibleText(form), 300),
      fields,
    };
  });
  const headings = [...document.querySelectorAll("h1, h2, h3, .page-header, .headline, .panel-heading")]
    .map((element) => truncate(visibleText(element), 160))
    .filter(Boolean)
    .slice(0, 30);
  return {
    url: location.href,
    title: document.title || "",
    headings,
    candidates: candidateElements,
    forms,
  };
}
""".strip()
BUILDING_CREATE_SCRIPT = r"""
async (config) => {
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const visibleText = (element) => [element?.value, element?.textContent, element?.getAttribute?.("title"), element?.getAttribute?.("aria-label")]
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
  const isVisible = (element) => {
    if (!element) return false;
    const style = window.getComputedStyle(element);
    if (style.display === "none" || style.visibility === "hidden" || Number(style.opacity) === 0) return false;
    const rect = element.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  };
  const setField = (name, value) => {
    const field = fieldByName(name);
    if (!field) return false;
    field.value = String(value);
    dispatch(field);
    return true;
  };
  const buttonDiagnostics = () => [...document.querySelectorAll('input[type="submit"], button[type="submit"], button:not([type])')]
    .map((button, index) => ({
      index,
      text: visibleText(button),
      name: button.name || "",
      id: button.id || "",
      disabled: Boolean(button.disabled || button.hasAttribute("disabled")),
      visible: isVisible(button),
      allianceContext: Boolean(allianceContext(button)),
    }));
  const fail = (reason) => ({
    ok: false,
    reason,
    snapshot: {
      url: location.href,
      buildingType: fieldValue("building[building_type]"),
      name: fieldValue("building[name]"),
      latitude: fieldValue("building[latitude]"),
      longitude: fieldValue("building[longitude]"),
      address: fieldValue("building[address]"),
      buildAsAlliance: fieldValue("build_as_alliance"),
      buildWithCoins: fieldValue("build_with_coins"),
      buttons: buttonDiagnostics(),
    },
  });
  function allianceContext(button) {
    for (let node = button?.parentElement; node && node !== document.body; node = node.parentElement) {
      const text = visibleText(node).toLowerCase();
      if (text.includes("build as alliance building")) return node;
      if (node.matches?.("form")) return null;
    }
    return null;
  }

  const form = document.querySelector("#new_building") || document.querySelector('form[action*="/buildings"]');
  if (!form) return fail("MissionChief building form was not loaded.");

  const typeSelect = fieldByName("building[building_type]");
  if (!typeSelect) return fail("MissionChief building type field was not found.");
  typeSelect.value = String(config.buildingTypeId || "");
  dispatch(typeSelect);
  await sleep(300);

  if (fieldValue("building[building_type]") !== String(config.buildingTypeId || "")) {
    return fail(`MissionChief did not accept building type ${config.buildingTypeId}.`);
  }
  if (!setField("building[name]", config.name || "")) return fail("MissionChief building name field was not found.");
  if (!setField("building[latitude]", config.latitude || "")) return fail("MissionChief latitude field was not found.");
  if (!setField("building[longitude]", config.longitude || "")) return fail("MissionChief longitude field was not found.");
  setField("building[address]", config.address || "");
  setField("build_with_coins", "0");
  setField("build_as_alliance", "1");
  const buildAnother = fieldByName("build_another");
  if (buildAnother) {
    buildAnother.checked = false;
    dispatch(buildAnother);
  }

  const buttons = [...document.querySelectorAll('input[type="submit"], button[type="submit"], button:not([type])')];
  const candidates = buttons
    .map((button, index) => ({ button, index, text: visibleText(button), context: allianceContext(button) }))
    .filter((item) => {
      const text = item.text.toLowerCase();
      return item.context
        && isVisible(item.button)
        && !item.button.disabled
        && !item.button.hasAttribute("disabled")
        && text.includes("build")
        && text.includes("credits")
        && !text.includes("coins");
    });

  if (candidates.length < 1) {
    return fail("No enabled alliance build button was found.");
  }
  const selected = candidates[0];
  return {
    ok: true,
    submitIndex: selected.index,
    snapshot: {
      url: location.href,
      buildingType: fieldValue("building[building_type]"),
      name: fieldValue("building[name]"),
      latitude: fieldValue("building[latitude]"),
      longitude: fieldValue("building[longitude]"),
      address: fieldValue("building[address]"),
      buildAsAlliance: fieldValue("build_as_alliance"),
      buildWithCoins: fieldValue("build_with_coins"),
      buildAnother: buildAnother ? Boolean(buildAnother.checked) : null,
    },
  };
}
""".strip()
BUILDING_CLICK_CREATE_SCRIPT = r"""
(submitIndex) => {
  const buttons = [...document.querySelectorAll('input[type="submit"], button[type="submit"], button:not([type])')];
  const button = buttons[submitIndex];
  if (!button) return false;
  button.click();
  return true;
}
""".strip()
BUILDING_FETCH_API_SCRIPT = r"""
async () => {
  const response = await fetch("/api/buildings", {
    credentials: "same-origin",
    headers: { "Accept": "application/json" },
  });
  const status = response.status;
  if (!response.ok) {
    let text = "";
    try {
      text = await response.text();
    } catch (_) {}
    return { ok: false, status, text: String(text || "").slice(0, 500) };
  }
  const buildings = await response.json();
  return { ok: true, status, buildings };
}
""".strip()
BUILDING_FETCH_ALLIANCE_LIST_SCRIPT = r"""
async (config) => {
  const maxPages = Number(config.maxPages || 6);
  const startPath = "/verband/gebauede";
  const seenPages = new Set();
  const seenIds = new Set();
  const candidates = [];
  const textOf = (element) => String(element?.textContent || "").replace(/\s+/g, " ").trim();
  const absolutePath = (href) => {
    try {
      const url = new URL(href, location.origin);
      return `${url.pathname}${url.search || ""}`;
    } catch (_) {
      return "";
    }
  };
  const collectCandidate = (id, source, element, pagePath) => {
    if (!id || seenIds.has(`${id}:${pagePath}:${source}`)) return;
    seenIds.add(`${id}:${pagePath}:${source}`);
    const row = element?.closest?.("tr, .panel, .well, .building-list-entry, li, div") || element;
    const imageSources = [...(row?.querySelectorAll?.("img") || [])].map((img) => img.src || img.getAttribute("src") || "");
    candidates.push({
      id: Number(id),
      pagePath,
      source,
      text: textOf(element),
      rowText: textOf(row),
      searchAttribute: row?.getAttribute?.("search_attribute") || "",
      imageSources,
    });
  };
  const parsePage = (html, pagePath) => {
    const doc = new DOMParser().parseFromString(html, "text/html");
    for (const img of doc.querySelectorAll("[building_id]")) {
      const id = img.getAttribute("building_id");
      collectCandidate(id, "building_id", img, pagePath);
    }
    for (const link of doc.querySelectorAll('a[href*="/buildings/"]')) {
      const match = String(link.getAttribute("href") || "").match(/\/buildings\/(\d+)/);
      if (match) collectCandidate(match[1], "link", link, pagePath);
    }
    const next = [...doc.querySelectorAll("a[href]")]
      .find((link) => {
        const label = textOf(link).toLowerCase();
        const className = String(link.getAttribute("class") || "").toLowerCase();
        const rel = String(link.getAttribute("rel") || "").toLowerCase();
        const href = String(link.getAttribute("href") || "");
        return href.includes("/verband/gebauede")
          && (label.includes("next") || label.includes(">") || className.includes("next") || rel.includes("next"));
      });
    return next ? absolutePath(next.getAttribute("href")) : "";
  };

  let pagePath = startPath;
  let lastStatus = null;
  for (let pageIndex = 0; pageIndex < maxPages && pagePath && !seenPages.has(pagePath); pageIndex += 1) {
    seenPages.add(pagePath);
    const response = await fetch(pagePath, { credentials: "same-origin", headers: { "Accept": "text/html" } });
    lastStatus = response.status;
    if (!response.ok) {
      let text = "";
      try {
        text = await response.text();
      } catch (_) {}
      return { ok: false, status: response.status, pages: [...seenPages], candidates, text: String(text || "").slice(0, 500) };
    }
    const html = await response.text();
    pagePath = parsePage(html, pagePath);
  }
  return { ok: true, status: lastStatus, pages: [...seenPages], candidates };
}
""".strip()
BUILDING_AUTOMATION_PREPARE_SCRIPT = r"""
(config) => {
  const targetTax = String(config.targetTax || "20");
  const maxExtensionStarts = Number(config.maxExtensionStarts || 3);
  const extensionsStartedThisRun = Number(config.extensionsStartedThisRun || 0);
  const excludedLabels = (config.excludedLabels || []).map((item) => String(item || "").toLowerCase());
  const visibleText = (element) => [element?.value, element?.textContent, element?.getAttribute?.("title"), element?.getAttribute?.("aria-label")]
    .filter(Boolean)
    .join(" ")
    .replace(/\s+/g, " ")
    .trim();
  const normalized = (value) => String(value || "").replace(/\s+/g, " ").trim().toLowerCase();
  const isVisible = (element) => {
    if (!element) return false;
    const style = window.getComputedStyle(element);
    if (style.display === "none" || style.visibility === "hidden" || Number(style.opacity) === 0) return false;
    const rect = element.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  };
  const isEnabled = (element) => element
    && !element.disabled
    && !element.hasAttribute("disabled")
    && !String(element.className || "").toLowerCase().includes("disabled")
    && String(element.getAttribute("aria-disabled") || "").toLowerCase() !== "true";
  const textFor = (element) => normalized([
    visibleText(element),
    element?.href || "",
    element?.name || "",
    element?.id || "",
    element?.className || "",
    element?.getAttribute?.("data-method") || "",
  ].join(" "));
  const isDangerous = (text) => /(coin|coins|delete|remove|demolish|sell|cancel|back|abort)/i.test(text);
  const isExcludedLarge = (text) => excludedLabels.some((label) => label && text.includes(label));
  const tagAction = (element) => {
    const token = `bm-action-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    element.setAttribute("data-bm-action", token);
    return `[data-bm-action="${token}"]`;
  };
  const fieldLabel = (field) => {
    const id = field?.id ? document.querySelector(`label[for="${window.CSS.escape(field.id)}"]`) : null;
    const wrappingLabel = field?.closest?.("label");
    const fieldGroup = field?.closest?.(".form-group, .control-group, .field, .row, p, td, tr");
    return normalized([
      field?.name || "",
      field?.id || "",
      visibleText(id),
      visibleText(wrappingLabel),
      visibleText(fieldGroup),
    ].join(" "));
  };
  const safeSubmitInForm = (form) => {
    if (!form) return null;
    return [...form.querySelectorAll('input[type="submit"], button[type="submit"], button:not([type])')]
      .find((button) => {
        const text = textFor(button);
        return isVisible(button) && isEnabled(button) && !isDangerous(text);
      }) || null;
  };
  const setTaxField = (field) => {
    const tag = normalized(field.tagName);
    if (tag === "select") {
      const option = [...field.options].find((item) => String(item.value || "") === targetTax)
        || [...field.options].find((item) => normalized(item.textContent).includes(`${targetTax}%`));
      if (!option) return false;
      if (String(field.value || "") === String(option.value || "")) return "already";
      field.value = option.value;
    } else {
      const type = normalized(field.type);
      if (!["number", "text", "range"].includes(type)) return false;
      if (String(field.value || "") === targetTax) return "already";
      field.value = targetTax;
    }
    field.dispatchEvent(new Event("input", { bubbles: true }));
    field.dispatchEvent(new Event("change", { bubbles: true }));
    return true;
  };
  const taxKeywords = /(tax|fee|percentage|percent|share|alliance|patient|prisoner|charge|abgabe|steuer|beitrag)/i;
  const taxFields = [...document.querySelectorAll("select, input")]
    .filter((field) => !["hidden", "submit", "button", "checkbox", "radio"].includes(normalized(field.type)))
    .map((field) => ({ field, label: fieldLabel(field) }))
    .filter((item) => taxKeywords.test(item.label));
  if (!config.taxComplete) {
    for (const item of taxFields) {
      const state = setTaxField(item.field);
      if (state === "already") {
        return {
          ok: true,
          action: "tax_already_set",
          completed: false,
          label: `Tax already set to ${targetTax}%`,
          taxState: "already_target",
          snapshot: { url: location.href, taxField: item.label },
        };
      }
      if (state === true) {
        const submit = safeSubmitInForm(item.field.closest("form"));
        if (!submit) {
          return {
            ok: false,
            action: null,
            completed: false,
            reason: "Tax field was found, but no safe submit button was available.",
            taxState: "submit_missing",
            snapshot: { url: location.href, taxField: item.label },
          };
        }
        return {
          ok: true,
          action: "set_tax",
          selector: tagAction(submit),
          label: `Set tax to ${targetTax}%`,
          taxState: "updating",
          completed: false,
          snapshot: { url: location.href, taxField: item.label },
        };
      }
    }
  }

  const actionElements = [...document.querySelectorAll('a, input[type="submit"], button[type="submit"], button:not([type])')]
    .map((element) => ({
      element,
      text: textFor(element),
      label: visibleText(element) || element.href || element.name || element.id || "MissionChief action",
      visible: isVisible(element),
      enabled: isEnabled(element),
    }))
    .filter((item) => item.visible && !isDangerous(item.text) && !isExcludedLarge(item.text));
  const levelCandidates = actionElements.filter((item) =>
    /(level|upgrade|expand|capacity|bed|cell)/i.test(item.text)
    && !/(extension|department|large hospital|large prison)/i.test(item.text)
  );
  const extensionCandidates = actionElements.filter((item) =>
    /(extension|department|ward|clinic|surgery|icu|intensive|psychiatry|cell|detention)/i.test(item.text)
    && !/(large hospital|large prison)/i.test(item.text)
  );

  const nextEnabled = (items) => items.find((item) => item.enabled);
  const level = nextEnabled(levelCandidates);
  if (!config.levelComplete && level) {
    return {
      ok: true,
      action: "start_level_upgrade",
      selector: tagAction(level.element),
      label: level.label,
      completed: false,
      snapshot: { url: location.href },
    };
  }

  if (extensionsStartedThisRun >= maxExtensionStarts) {
    return {
      ok: true,
      action: null,
      completed: false,
      reason: `Started ${extensionsStartedThisRun} extension(s) this run; waiting before starting more.`,
      wait: true,
      snapshot: { url: location.href },
    };
  }

  const extension = nextEnabled(extensionCandidates);
  if (!config.extensionsComplete && extension) {
    return {
      ok: true,
      action: "start_extension",
      selector: tagAction(extension.element),
      label: extension.label,
      completed: false,
      snapshot: { url: location.href },
    };
  }

  const blockedTargets = [...levelCandidates, ...extensionCandidates]
    .filter((item) => !item.enabled)
    .map((item) => item.label)
    .slice(0, 10);
  const taxState = config.taxComplete ? "complete" : taxFields.length ? "unhandled" : "not_found";
  const fallbackReason = taxState === "unhandled"
    ? `Tax field was found, but no safe ${targetTax}% update action was available.`
    : "No remaining eligible upgrade or extension actions were found.";
  return {
    ok: true,
    action: null,
    completed: blockedTargets.length === 0 && taxState !== "unhandled" && taxState !== "not_found",
    wait: blockedTargets.length > 0 || taxState === "unhandled" || taxState === "not_found",
    reason: blockedTargets.length
      ? "MissionChief shows upgrade/extension targets, but they are not available yet."
      : fallbackReason,
    taxState,
    snapshot: {
      url: location.href,
      blockedTargets,
      excludedLabels,
      taxFields: taxFields.map((item) => item.label).slice(0, 5),
    },
  };
}
""".strip()

# ---------- Utilities ----------

def ts() -> int:
    """Get current unix timestamp."""
    return int(datetime.now(timezone.utc).timestamp())

def fmt_dt(timestamp: int) -> str:
    """Format unix timestamp to Discord timestamp."""
    return f"<t:{timestamp}:F>"

def _truncate_text(value: Any, limit: int = 180) -> str:
    """Return a compact one-line text value for diagnostics."""
    text = " ".join(str(value or "").split())
    if len(text) > limit:
        return f"{text[:limit]}..."
    return text

def _normalize_missionchief_url(value: Optional[str]) -> str:
    """Normalize a user-supplied MissionChief URL or path."""
    if not value:
        return MISSIONCHIEF_NEW_BUILDING_URL
    value = value.strip()
    if value.startswith("/"):
        return f"{BASE_URL}{value}"
    parsed = urlparse(value)
    if parsed.scheme in {"http", "https"} and parsed.netloc.lower() in {"missionchief.com", "www.missionchief.com"}:
        return value
    raise ValueError("Only missionchief.com URLs or relative MissionChief paths are supported.")

def _building_type_id(building_type: str) -> str:
    """Return the MissionChief building type id for supported alliance buildings."""
    try:
        return ALLIANCE_BUILDING_TYPE_IDS[building_type]
    except KeyError as exc:
        allowed = ", ".join(ALLIANCE_BUILDING_TYPE_IDS)
        raise ValueError(f"Alliance building automation only supports: {allowed}.") from exc

def _parse_coordinate_pair(value: Optional[str]) -> Tuple[str, str]:
    """Parse a stored `lat, lon` coordinate pair."""
    if not value:
        raise ValueError("No coordinates are available for this request.")
    parts = [part.strip() for part in str(value).split(",", 1)]
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError("Coordinates must be formatted as `latitude, longitude`.")
    try:
        latitude = float(parts[0])
        longitude = float(parts[1])
    except ValueError as exc:
        raise ValueError("Coordinates must contain numeric latitude and longitude.") from exc
    if not -90 <= latitude <= 90:
        raise ValueError("Latitude must be between -90 and 90.")
    if not -180 <= longitude <= 180:
        raise ValueError("Longitude must be between -180 and 180.")
    return f"{latitude:.7f}", f"{longitude:.7f}"

def build_alliance_building_config(
    *,
    building_type: str,
    building_name: str,
    coordinates: Optional[str],
    address: Optional[str],
) -> Dict[str, str]:
    """Build a browser automation config for supported alliance building creation."""
    latitude, longitude = _parse_coordinate_pair(coordinates)
    name = _truncate_text(building_name, 100).strip()
    if not name:
        raise ValueError("Building name is required.")
    return {
        "buildingType": building_type,
        "buildingTypeId": _building_type_id(building_type),
        "name": name,
        "latitude": latitude,
        "longitude": longitude,
        "address": _truncate_text(address, 180),
    }

def extract_missionchief_building_id(*values: Any) -> Optional[int]:
    """Extract a MissionChief building id from URLs, response text, or snapshots."""
    for value in values:
        if value is None:
            continue
        if isinstance(value, dict):
            nested_values = []
            for key in ("buildingId", "building_id", "url", "responseUrl", "finalUrl", "postUrl"):
                nested_values.append(value.get(key))
            found = extract_missionchief_building_id(*nested_values)
            if found:
                return found
            continue
        text = str(value)
        for pattern in (
            r"/buildings/(\d+)(?:\D|$)",
            r"buildings%2F(\d+)(?:\D|$)",
            r"building[_-]?id[\"'\s:=]+(\d+)",
        ):
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return int(match.group(1))
    return None

def _normalize_match_text(value: Any) -> str:
    """Normalize text for conservative MissionChief API matching."""
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()

def _coerce_float(value: Any) -> Optional[float]:
    """Return a float when MissionChief API data contains a numeric value."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _coerce_int(value: Any) -> Optional[int]:
    """Return an int when MissionChief API data contains a numeric id/type."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def _api_value(record: Dict[str, Any], *keys: str) -> Any:
    """Read the first present value from a MissionChief API record."""
    for key in keys:
        if key in record:
            return record.get(key)
    return None

def find_created_alliance_building_id(
    buildings: Iterable[Dict[str, Any]],
    config: Dict[str, str],
) -> Optional[int]:
    """Find a newly created building in MissionChief `/api/buildings` data.

    The match intentionally requires type and coordinates. Name is used only as
    a preference, because MissionChief names can be edited or normalized.
    """
    target_type = _coerce_int(config.get("buildingTypeId"))
    target_lat = _coerce_float(config.get("latitude"))
    target_lon = _coerce_float(config.get("longitude"))
    target_name = _normalize_match_text(config.get("name"))
    if target_type is None or target_lat is None or target_lon is None:
        return None

    candidates: List[Tuple[int, int]] = []
    for record in buildings or []:
        if not isinstance(record, dict):
            continue
        building_id = _coerce_int(_api_value(record, "id", "building_id", "buildingId"))
        building_type = _coerce_int(_api_value(record, "building_type", "building_type_id", "buildingType"))
        latitude = _coerce_float(_api_value(record, "latitude", "lat"))
        longitude = _coerce_float(_api_value(record, "longitude", "lon", "lng"))
        if building_id is None or building_type != target_type or latitude is None or longitude is None:
            continue
        if abs(latitude - target_lat) > 0.0002 or abs(longitude - target_lon) > 0.0002:
            continue

        record_name = _normalize_match_text(_api_value(record, "caption", "name", "building_name", "buildingName"))
        score = 100
        if record_name and target_name and record_name == target_name:
            score += 50
        candidates.append((score, building_id))

    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return candidates[0][1]

def _alliance_list_candidate_text(record: Dict[str, Any]) -> str:
    """Return searchable text from one MissionChief alliance building list candidate."""
    image_sources = " ".join(str(item or "") for item in record.get("imageSources") or [])
    return _normalize_match_text(
        " ".join(
            str(record.get(key) or "")
            for key in ("text", "rowText", "searchAttribute", "pagePath", "source")
        )
        + " "
        + image_sources
    )

def _alliance_list_type_score(record: Dict[str, Any], building_type: str) -> int:
    """Score whether a list candidate looks like the requested alliance building type."""
    text = _alliance_list_candidate_text(record)
    requested = _normalize_match_text(building_type)
    hospital_tokens = (
        "hospital",
        "ziekenhuis",
        "medical",
        "health",
        "clinic",
        "building_hospital",
        "building_rettung",
    )
    prison_tokens = (
        "prison",
        "jail",
        "detention",
        "correction",
        "gevangen",
        "polizeigefaengnis",
        "building_prison",
    )
    if requested == "hospital":
        if any(token in text for token in prison_tokens):
            return -100
        return 20 if any(token in text for token in hospital_tokens) else 0
    if requested == "prison":
        if any(token in text for token in hospital_tokens):
            return -100
        return 20 if any(token in text for token in prison_tokens) else 0
    return 0

def find_created_alliance_building_id_from_list(
    candidates: Iterable[Dict[str, Any]],
    config: Dict[str, str],
) -> Optional[int]:
    """Find a created alliance building from `/verband/gebauede` list data.

    The alliance list does not expose coordinates, so this fallback only trusts
    candidates whose visible/search text contains the requested building name.
    When multiple exact matches exist, the newest/highest id is preferred.
    """
    target_name = _normalize_match_text(config.get("name"))
    if not target_name:
        return None

    matches: List[Tuple[int, int]] = []
    for record in candidates or []:
        if not isinstance(record, dict):
            continue
        building_id = _coerce_int(record.get("id"))
        if building_id is None:
            continue
        candidate_text = _alliance_list_candidate_text(record)
        if not candidate_text or target_name not in candidate_text:
            continue

        score = 100
        if _normalize_match_text(record.get("text")) == target_name:
            score += 40
        if _normalize_match_text(record.get("searchAttribute")) == target_name:
            score += 30
        type_score = _alliance_list_type_score(record, str(config.get("buildingType") or ""))
        if type_score < 0:
            continue
        score += type_score
        matches.append((score, building_id))

    if not matches:
        return None
    matches.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return matches[0][1]

def _diagnostic_lines_for_fields(fields: Iterable[Dict[str, Any]]) -> List[str]:
    """Format no-submit browser field diagnostics."""
    lines = []
    for field in fields:
        name = field.get("name") or field.get("id") or "(unnamed)"
        field_type = field.get("type") or field.get("tag") or "field"
        flags = []
        if field.get("checked"):
            flags.append("checked")
        if field.get("disabled"):
            flags.append("disabled")
        flag_text = f" [{' '.join(flags)}]" if flags else ""
        value = field.get("value") or field.get("text") or ""
        value_text = f" = {_truncate_text(value, 140)}" if value else ""
        lines.append(f"- {name} ({field_type}){flag_text}{value_text}")
        for option in field.get("options") or []:
            selected = "*" if option.get("selected") else ""
            lines.append(f"  option: {selected}{_truncate_text(option.get('value'), 80)} - {_truncate_text(option.get('text'), 100)}")
    return lines

def build_browser_diagnostics_report(snapshot: Dict[str, Any]) -> str:
    """Build an admin-facing no-submit report from the live MissionChief page."""
    lines = [
        "BuildingManager Browser Diagnostics",
        "NO FORM WAS SUBMITTED. NO BUILDING WAS CREATED.",
        "",
        f"URL: {snapshot.get('url') or ''}",
        f"Title: {snapshot.get('title') or ''}",
    ]
    headings = [heading for heading in snapshot.get("headings") or [] if heading]
    if headings:
        lines.extend(["", "[Headings]"])
        lines.extend(f"- {_truncate_text(heading, 180)}" for heading in headings)

    candidates = snapshot.get("candidates") or []
    lines.extend(["", f"[Building-related links/buttons: {len(candidates)}]"])
    if candidates:
        for item in candidates:
            disabled = " disabled" if item.get("disabled") else ""
            target = item.get("href") or item.get("id") or item.get("name") or item.get("className") or ""
            lines.append(
                f"- #{item.get('index')} {item.get('tag')} {item.get('type') or ''}{disabled}: "
                f"{_truncate_text(item.get('text'), 140)} | {_truncate_text(target, 220)}"
            )
    else:
        lines.append("- No obvious building/alliance/hospital/prison controls found on this page.")

    forms = snapshot.get("forms") or []
    lines.extend(["", f"[Forms: {len(forms)}]"])
    if forms:
        for form in forms:
            lines.extend(
                [
                    "",
                    f"Form #{form.get('index')}",
                    f"Action: {form.get('action') or ''}",
                    f"Method: {form.get('method') or ''}",
                    f"ID/Class: {_truncate_text(form.get('id'), 80)} / {_truncate_text(form.get('className'), 120)}",
                    f"Text: {_truncate_text(form.get('text'), 260)}",
                    "Fields:",
                ]
            )
            field_lines = _diagnostic_lines_for_fields(form.get("fields") or [])
            lines.extend(field_lines or ["- No fields found."])
    else:
        lines.append("- No forms found.")
    return "\n".join(lines)

@dataclass
class BuildingCreateResult:
    """Result of attempting to create an alliance building in MissionChief."""

    ok: bool
    reason: str
    status: Optional[int] = None
    post_url: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

    @property
    def building_id(self) -> Optional[int]:
        return extract_missionchief_building_id(self.details or {}, self.post_url)


@dataclass
class BuildingAutomationJob:
    """Stored post-creation automation job for an alliance building."""

    job_id: int
    request_id: int
    guild_id: int
    building_id: int
    building_type: str
    building_name: str
    status: str
    target_tax: int
    tax_complete: bool
    level_complete: bool
    extensions_complete: bool
    extensions_started: int
    attempts: int
    next_run_at: int
    last_result: Optional[str] = None


@dataclass
class BuildingAutomationResult:
    """Result of one post-creation automation pass."""

    ok: bool
    completed: bool
    wait: bool
    reason: str
    actions: List[str]
    tax_complete: bool = False
    level_complete: bool = False
    extensions_complete: bool = False
    extensions_started: int = 0
    status: Optional[int] = None
    details: Optional[Dict[str, Any]] = None

async def safe_update(interaction: discord.Interaction, *, content=None, embed=None, view=None):
    """Robust message updater for component/modal callbacks."""
    try:
        if not interaction.response.is_done():
            await interaction.response.edit_message(content=content, embed=embed, view=view)
            return
    except Exception as e:
        log.debug("safe_update: response.edit_message failed: %r", e)
    try:
        if getattr(interaction, "message", None) is not None:
            await interaction.message.edit(content=content, embed=embed, view=view)
            return
    except Exception as e:
        log.debug("safe_update: message.edit failed: %r", e)
    try:
        await interaction.followup.send(content or "Updated.", embed=embed, view=view, ephemeral=True)
    except Exception as e:
        log.exception("safe_update completely failed: %r", e)
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(content or "Updated.", embed=embed, view=view, ephemeral=True)
        except Exception:
            pass

# ---------- Location Parser ----------

@dataclass
class LocationDetails:
    """Resolved location details for a building request."""

    original_input: str
    resolved_input: str
    place_name: Optional[str] = None
    coordinates: Optional[str] = None
    address: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None
    maps_url: Optional[str] = None
    provider: Optional[str] = None
    facility_warning: Optional[str] = None
    detected_facility_type: Optional[str] = None


class LocationParser:
    """Parse and geocode location inputs."""
    
    # Rate limiting for Nominatim (1 req/sec)
    _last_nominatim_call = 0
    _nominatim_delay = 1.0
    _health_keywords = {
        "hospital",
        "medical",
        "clinic",
        "health",
        "healthcare",
        "ziekenhuis",
        "medisch",
        "kliniek",
        "zorg",
        "mc",
    }
    _justice_keywords = {
        "prison",
        "jail",
        "detention",
        "correctional",
        "justice",
        "courthouse",
        "gevangenis",
        "justitie",
        "penitentiaire",
        "inrichting",
        "detentie",
    }
    
    @staticmethod
    def extract_coordinates(text: str) -> Optional[Tuple[float, float]]:
        """Extract coordinates from various formats."""
        decoded_text = unquote(text)

        # Pattern 0: Google Maps place data !3dlat!4dlon. This is usually the
        # exact place marker and is better than the viewport /@ coordinates.
        pattern0 = r'!3d(-?\d+\.?\d*)!4d(-?\d+\.?\d*)'
        match = re.search(pattern0, decoded_text)
        if match:
            return (float(match.group(1)), float(match.group(2)))

        # Pattern 1: Google Maps ?q=lat,lon
        pattern1 = r'[?&]q=(-?\d+\.?\d*),\s*(-?\d+\.?\d*)'
        match = re.search(pattern1, decoded_text)
        if match:
            return (float(match.group(1)), float(match.group(2)))
        
        # Pattern 2: Google Maps /@lat,lon
        pattern2 = r'/@(-?\d+\.?\d*),\s*(-?\d+\.?\d*)'
        match = re.search(pattern2, decoded_text)
        if match:
            return (float(match.group(1)), float(match.group(2)))

        # Pattern 2b: Supported map query parameters
        parsed = urlparse(decoded_text.strip())
        query = parse_qs(parsed.query)
        value = (query.get("query") or [None])[0]
        if value:
            match = re.match(r"\s*(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)\s*$", value)
            if match:
                return (float(match.group(1)), float(match.group(2)))
        
        # Pattern 3: Direct coordinates "lat, lon" or "lat,lon"
        pattern3 = r'^(-?\d+\.?\d*)[,\s]+(-?\d+\.?\d*)$'
        match = re.search(pattern3, decoded_text.strip())
        if match:
            return (float(match.group(1)), float(match.group(2)))
        
        # Pattern 4: X: lat, Y: lon format
        pattern4 = r'X:\s*(-?\d+\.?\d*)[,\s]+Y:\s*(-?\d+\.?\d*)'
        match = re.search(pattern4, decoded_text, re.IGNORECASE)
        if match:
            return (float(match.group(1)), float(match.group(2)))
        
        return None

    @staticmethod
    def is_maps_short_url(text: str) -> bool:
        """Return whether the input is a Google Maps short URL."""
        try:
            host = urlparse(text.strip()).netloc.lower()
        except Exception:
            return False
        return host in {"maps.app.goo.gl", "goo.gl"} or host.endswith(".goo.gl")

    @staticmethod
    def make_maps_url(coordinates: Optional[str], fallback_query: str) -> str:
        """Build a stable Google Maps URL for admins."""
        if coordinates:
            return f"https://www.google.com/maps/search/?api=1&query={quote(coordinates)}"
        return f"https://www.google.com/maps/search/?api=1&query={quote(fallback_query)}"

    @staticmethod
    def extract_place_name(text: str) -> Optional[str]:
        """Extract the visible place name from supported map URLs."""
        decoded_text = unquote(text)
        match = re.search(r"/maps/place/([^/@?]+)", decoded_text)
        if match:
            return match.group(1).replace("+", " ").strip() or None

        parsed = urlparse(decoded_text.strip())
        host = parsed.netloc.lower()
        query = parse_qs(parsed.query)

        if "google." in host or "maps.app.goo.gl" in host:
            for key in ("query", "q"):
                value = (query.get(key) or [None])[0]
                cleaned = LocationParser._clean_place_query(value)
                if cleaned:
                    return cleaned

        return None

    @staticmethod
    def _clean_place_query(value: Optional[str]) -> Optional[str]:
        """Return a human place query, ignoring coordinate-only values."""
        if not value:
            return None
        cleaned = unquote(value).replace("+", " ").strip()
        if not cleaned:
            return None
        if re.fullmatch(r"-?\d+\.?\d*\s*[,~]\s*-?\d+\.?\d*", cleaned):
            return None
        return cleaned

    @staticmethod
    def derive_building_name(building_type: str, location_details: LocationDetails) -> str:
        """Return the best building name from resolved location details."""
        if location_details.place_name:
            return location_details.place_name

        if location_details.address:
            first_part = location_details.address.split(",", 1)[0].strip()
            if first_part:
                return first_part

        place_name = LocationParser.extract_place_name(location_details.resolved_input)
        if place_name:
            return place_name

        return f"{building_type} location"

    @classmethod
    async def expand_maps_url(cls, text: str) -> str:
        """Resolve Google Maps short URLs to their final URL when possible."""
        if not cls.is_maps_short_url(text):
            return text

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(text, allow_redirects=True, timeout=10) as resp:
                    return str(resp.url)
        except Exception as e:
            log.warning("Google Maps URL expansion failed: %r", e)
            return text

    @classmethod
    async def resolve_location(cls, location_input: str) -> LocationDetails:
        """Resolve user-provided location input into admin-facing location details."""
        cleaned_input = location_input.strip()
        resolved_input = await cls.expand_maps_url(cleaned_input)
        coords = cls.extract_coordinates(resolved_input)

        address = None
        country = None
        region = None
        provider = None
        place_name = cls.extract_place_name(resolved_input)
        detected_facility_type = None

        if coords:
            lat, lon = coords
            coordinates_str = f"{lat}, {lon}"
            geocode = await cls.reverse_geocode_details(lat, lon)
        else:
            geocode = await cls.forward_geocode_nominatim_details(place_name or resolved_input)
            coordinates_str = geocode.get("coordinates") if geocode else None

        if geocode:
            address = geocode.get("address")
            country = geocode.get("country")
            region = geocode.get("region")
            provider = geocode.get("provider")
            place_name = place_name or geocode.get("place_name")
            detected_facility_type = geocode.get("facility_type")

        maps_url = cls.make_maps_url(coordinates_str, resolved_input)
        return LocationDetails(
            original_input=cleaned_input,
            resolved_input=resolved_input,
            place_name=place_name,
            coordinates=coordinates_str,
            address=address,
            country=country,
            region=region,
            maps_url=maps_url,
            provider=provider,
            detected_facility_type=detected_facility_type,
        )
    
    @classmethod
    async def geocode_nominatim(cls, lat: float, lon: float) -> Optional[str]:
        """Reverse geocode using Nominatim."""
        # Rate limiting
        now = time.time()
        elapsed = now - cls._last_nominatim_call
        if elapsed < cls._nominatim_delay:
            await asyncio.sleep(cls._nominatim_delay - elapsed)
        
        cls._last_nominatim_call = time.time()
        
        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            "lat": lat,
            "lon": lon,
            "format": "json",
            "addressdetails": 1
        }
        headers = {
            "User-Agent": "DiscordBot-BuildingManager/1.0"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        display_name = data.get("display_name")
                        return display_name
        except Exception as e:
            log.warning("Nominatim geocoding failed: %r", e)
        
        return None

    @classmethod
    async def reverse_geocode_details(
        cls,
        lat: float,
        lon: float,
        *,
        google_key: Optional[str] = None,
    ) -> Optional[dict]:
        """Reverse geocode and return structured address details."""
        details = await cls.reverse_geocode_nominatim_details(lat, lon)
        if details:
            return details

        if google_key:
            return await cls.reverse_geocode_google_details(lat, lon, google_key)
        return None

    @classmethod
    async def reverse_geocode_nominatim_details(cls, lat: float, lon: float) -> Optional[dict]:
        """Reverse geocode using Nominatim with country and region details."""
        now = time.time()
        elapsed = now - cls._last_nominatim_call
        if elapsed < cls._nominatim_delay:
            await asyncio.sleep(cls._nominatim_delay - elapsed)

        cls._last_nominatim_call = time.time()

        url = "https://nominatim.openstreetmap.org/reverse"
        params = {
            "lat": lat,
            "lon": lon,
            "format": "json",
            "addressdetails": 1,
        }
        headers = {
            "User-Agent": "DiscordBot-BuildingManager/1.0"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=headers, timeout=10) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
        except Exception as e:
            log.warning("Nominatim structured reverse geocoding failed: %r", e)
            return None

        return cls._nominatim_result_to_details(data)

    @classmethod
    async def forward_geocode_details(cls, query: str, *, google_key: Optional[str] = None) -> Optional[dict]:
        """Forward geocode text or place names."""
        if google_key:
            details = await cls.forward_geocode_google_details(query, google_key)
            if details:
                return details
        return await cls.forward_geocode_nominatim_details(query)

    @classmethod
    async def forward_geocode_nominatim_details(cls, query: str) -> Optional[dict]:
        """Forward geocode using Nominatim for addresses or place names."""
        now = time.time()
        elapsed = now - cls._last_nominatim_call
        if elapsed < cls._nominatim_delay:
            await asyncio.sleep(cls._nominatim_delay - elapsed)

        cls._last_nominatim_call = time.time()

        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": query,
            "format": "json",
            "addressdetails": 1,
            "namedetails": 1,
            "limit": 1,
        }
        headers = {
            "User-Agent": "DiscordBot-BuildingManager/1.0"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=headers, timeout=10) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
        except Exception as e:
            log.warning("Nominatim structured forward geocoding failed: %r", e)
            return None

        if not data:
            return None
        return cls._nominatim_result_to_details(data[0])

    @staticmethod
    async def reverse_geocode_google_details(lat: float, lon: float, api_key: str) -> Optional[dict]:
        """Reverse geocode using Google and return structured address details."""
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "latlng": f"{lat},{lon}",
            "key": api_key,
        }
        return await LocationParser._google_geocode_details(url, params)

    @staticmethod
    async def forward_geocode_google_details(query: str, api_key: str) -> Optional[dict]:
        """Forward geocode using Google and return structured address details."""
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "address": query,
            "key": api_key,
        }
        return await LocationParser._google_geocode_details(url, params)

    @staticmethod
    async def _google_geocode_details(url: str, params: dict) -> Optional[dict]:
        """Run a Google geocoding request and normalize the first result."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=10) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
        except Exception as e:
            log.warning("Google structured geocoding failed: %r", e)
            return None

        if data.get("status") != "OK" or not data.get("results"):
            return None
        return LocationParser._google_result_to_details(data["results"][0])

    @staticmethod
    def _nominatim_result_to_details(data: dict) -> dict:
        """Normalize a Nominatim response."""
        address_data = data.get("address") or {}
        namedetails = data.get("namedetails") or {}
        lat = data.get("lat")
        lon = data.get("lon")
        region = (
            address_data.get("state")
            or address_data.get("region")
            or address_data.get("province")
            or address_data.get("county")
        )
        facility_type = " ".join(
            str(value)
            for value in (
                data.get("class"),
                data.get("type"),
                address_data.get("amenity"),
                address_data.get("healthcare"),
            )
            if value
        ).strip() or None

        return {
            "coordinates": f"{float(lat)}, {float(lon)}" if lat and lon else None,
            "address": data.get("display_name"),
            "place_name": data.get("name") or namedetails.get("name") or (data.get("display_name") or "").split(",", 1)[0],
            "country": address_data.get("country"),
            "region": region,
            "provider": "nominatim",
            "facility_type": facility_type,
        }

    @staticmethod
    def _google_result_to_details(result: dict) -> dict:
        """Normalize a Google geocoding result."""
        components = result.get("address_components") or []

        def component(*types: str) -> Optional[str]:
            for item in components:
                item_types = set(item.get("types") or [])
                if item_types.intersection(types):
                    return item.get("long_name")
            return None

        location = (result.get("geometry") or {}).get("location") or {}
        lat = location.get("lat")
        lon = location.get("lng")
        region = component("administrative_area_level_1", "administrative_area_level_2")
        facility_type = " ".join(result.get("types") or []) or None
        formatted_address = result.get("formatted_address")

        return {
            "coordinates": f"{float(lat)}, {float(lon)}" if lat is not None and lon is not None else None,
            "address": formatted_address,
            "place_name": result.get("name") or (formatted_address or "").split(",", 1)[0],
            "country": component("country"),
            "region": region,
            "provider": "google",
            "facility_type": facility_type,
        }

    @classmethod
    def facility_warning(
        cls,
        building_type: str,
        building_name: str,
        location_details: LocationDetails,
    ) -> Optional[str]:
        """Return a warning if the location does not clearly match the requested type."""
        searchable = " ".join(
            value
            for value in (
                building_name,
                location_details.address,
                cls.extract_place_name(location_details.resolved_input),
                location_details.resolved_input,
                location_details.detected_facility_type,
            )
            if value
        )
        searchable = unquote(searchable).replace("+", " ").lower()

        if building_type == "Hospital":
            if not any(keyword in searchable for keyword in cls._health_keywords):
                return "This location does not clearly look like a health facility."
        elif building_type == "Prison":
            if not any(keyword in searchable for keyword in cls._justice_keywords):
                return "This location does not clearly look like a justice or correctional facility."
        return None
    
    @staticmethod
    async def geocode_google(lat: float, lon: float, api_key: str) -> Optional[str]:
        """Reverse geocode using Google Geocoding API."""
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "latlng": f"{lat},{lon}",
            "key": api_key
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "OK" and data.get("results"):
                            return data["results"][0].get("formatted_address")
        except Exception as e:
            log.warning("Google geocoding failed: %r", e)
        
        return None

# ---------- Database ----------

class BuildingDatabase:
    """SQLite database for building requests."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize database tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Building requests table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_requests (
                request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                building_type TEXT NOT NULL,
                building_name TEXT NOT NULL,
                location_input TEXT NOT NULL,
                coordinates TEXT,
                address TEXT,
                notes TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        ''')
        
        # Building actions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_actions (
                action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                guild_id INTEGER NOT NULL,
                admin_user_id INTEGER,
                admin_username TEXT,
                action_type TEXT NOT NULL,
                denial_reason TEXT,
                previous_values TEXT,
                timestamp INTEGER NOT NULL,
                FOREIGN KEY (request_id) REFERENCES building_requests(request_id)
            )
        ''')
        
        # Geocoding cache table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS geocoding_cache (
                location_input TEXT PRIMARY KEY,
                coordinates TEXT,
                address TEXT,
                provider TEXT,
                cached_at INTEGER
            )
        ''')
        
        # Building types table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_types (
                type_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                type_name TEXT NOT NULL,
                emoji TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                created_at INTEGER NOT NULL,
                UNIQUE(guild_id, type_name)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_automation_jobs (
                job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL UNIQUE,
                guild_id INTEGER NOT NULL,
                building_id INTEGER NOT NULL,
                building_type TEXT NOT NULL,
                building_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                target_tax INTEGER NOT NULL DEFAULT 20,
                tax_complete INTEGER NOT NULL DEFAULT 0,
                level_complete INTEGER NOT NULL DEFAULT 0,
                extensions_complete INTEGER NOT NULL DEFAULT 0,
                extensions_started INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_run_at INTEGER NOT NULL,
                last_result TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def add_request(self, guild_id: int, user_id: int, username: str, building_type: str,
                   building_name: str, location_input: str, coordinates: Optional[str],
                   address: Optional[str], notes: Optional[str]) -> int:
        """Add a new building request."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = ts()
        cursor.execute('''
            INSERT INTO building_requests 
            (guild_id, user_id, username, building_type, building_name, location_input,
             coordinates, address, notes, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
        ''', (guild_id, user_id, username, building_type, building_name, location_input,
              coordinates, address, notes, now, now))
        
        request_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return request_id
    
    def update_request_status(self, request_id: int, status: str):
        """Update request status."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE building_requests 
            SET status = ?, updated_at = ?
            WHERE request_id = ?
        ''', (status, ts(), request_id))
        
        conn.commit()
        conn.close()

    def get_request_by_id(self, request_id: int) -> Optional[Dict[str, Any]]:
        """Return one stored building request by id."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT *
            FROM building_requests
            WHERE request_id = ?
            LIMIT 1
            ''',
            (int(request_id),),
        )
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def add_action(self, request_id: int, guild_id: int, admin_user_id: Optional[int],
                  admin_username: Optional[str], action_type: str, denial_reason: Optional[str] = None,
                  previous_values: Optional[str] = None):
        """Log an action on a request."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO building_actions
            (request_id, guild_id, admin_user_id, admin_username, action_type, 
             denial_reason, previous_values, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (request_id, guild_id, admin_user_id, admin_username, action_type,
              denial_reason, previous_values, ts()))
        
        conn.commit()
        conn.close()
    
    def get_cached_geocode(self, location_input: str) -> Optional[Tuple[str, str, str]]:
        """Get cached geocoding result."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT coordinates, address, provider 
            FROM geocoding_cache 
            WHERE location_input = ?
        ''', (location_input,))
        
        result = cursor.fetchone()
        conn.close()
        
        return result if result else None
    
    def cache_geocode(self, location_input: str, coordinates: str, address: str, provider: str):
        """Cache geocoding result."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO geocoding_cache
            (location_input, coordinates, address, provider, cached_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (location_input, coordinates, address, provider, ts()))
        
        conn.commit()
        conn.close()

    @staticmethod
    def _automation_row_to_job(row: sqlite3.Row) -> BuildingAutomationJob:
        """Convert an automation job row to a typed model."""
        return BuildingAutomationJob(
            job_id=int(row["job_id"]),
            request_id=int(row["request_id"]),
            guild_id=int(row["guild_id"]),
            building_id=int(row["building_id"]),
            building_type=str(row["building_type"]),
            building_name=str(row["building_name"]),
            status=str(row["status"]),
            target_tax=int(row["target_tax"]),
            tax_complete=bool(row["tax_complete"]),
            level_complete=bool(row["level_complete"]),
            extensions_complete=bool(row["extensions_complete"]),
            extensions_started=int(row["extensions_started"]),
            attempts=int(row["attempts"]),
            next_run_at=int(row["next_run_at"]),
            last_result=row["last_result"],
        )

    def add_or_update_automation_job(
        self,
        *,
        request_id: int,
        guild_id: int,
        building_id: int,
        building_type: str,
        building_name: str,
        target_tax: int = ALLIANCE_BUILDING_TARGET_TAX,
        next_run_at: Optional[int] = None,
    ) -> int:
        """Queue post-creation automation for a MissionChief alliance building."""
        now = ts()
        if next_run_at is None:
            next_run_at = now
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO building_automation_jobs
            (request_id, guild_id, building_id, building_type, building_name, status,
             target_tax, next_run_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?)
            ON CONFLICT(request_id) DO UPDATE SET
                building_id = excluded.building_id,
                building_type = excluded.building_type,
                building_name = excluded.building_name,
                status = CASE
                    WHEN building_automation_jobs.status = 'completed' THEN 'completed'
                    ELSE 'queued'
                END,
                target_tax = excluded.target_tax,
                next_run_at = excluded.next_run_at,
                updated_at = excluded.updated_at
            ''',
            (
                int(request_id),
                int(guild_id),
                int(building_id),
                str(building_type),
                str(building_name),
                int(target_tax),
                int(next_run_at),
                now,
                now,
            ),
        )
        cursor.execute("SELECT job_id FROM building_automation_jobs WHERE request_id = ?", (int(request_id),))
        job_id = int(cursor.fetchone()[0])
        conn.commit()
        conn.close()
        return job_id

    def get_automation_job(self, job_id: int) -> Optional[BuildingAutomationJob]:
        """Return one automation job by internal job id."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM building_automation_jobs WHERE job_id = ?", (int(job_id),))
        row = cursor.fetchone()
        conn.close()
        return self._automation_row_to_job(row) if row else None

    def get_automation_job_by_request_or_building(self, identifier: int) -> Optional[BuildingAutomationJob]:
        """Return one automation job by request id or MissionChief building id."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM building_automation_jobs
            WHERE request_id = ? OR building_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            ''',
            (int(identifier), int(identifier)),
        )
        row = cursor.fetchone()
        conn.close()
        return self._automation_row_to_job(row) if row else None

    def get_due_automation_jobs(self, *, now_ts: Optional[int] = None, limit: int = 5) -> List[BuildingAutomationJob]:
        """Return queued automation jobs that are ready to run."""
        if now_ts is None:
            now_ts = ts()
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM building_automation_jobs
            WHERE status IN ('queued', 'waiting', 'failed')
              AND next_run_at <= ?
            ORDER BY next_run_at ASC, job_id ASC
            LIMIT ?
            ''',
            (int(now_ts), int(limit)),
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._automation_row_to_job(row) for row in rows]

    def get_recent_automation_jobs(self, guild_id: int, *, limit: int = 10) -> List[BuildingAutomationJob]:
        """Return recent automation jobs for admin status output."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT * FROM building_automation_jobs
            WHERE guild_id = ?
            ORDER BY updated_at DESC
            LIMIT ?
            ''',
            (int(guild_id), int(limit)),
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._automation_row_to_job(row) for row in rows]

    def update_automation_job(self, job_id: int, result: BuildingAutomationResult):
        """Persist the latest automation result."""
        now = ts()
        if result.completed:
            status = "completed"
            next_run_at = now
            completed_at = now
        elif result.ok and result.wait:
            status = "waiting"
            next_run_at = now + BUILDING_AUTOMATION_RETRY_SECONDS
            completed_at = None
        elif result.ok:
            status = "queued"
            next_run_at = now + BUILDING_AUTOMATION_LOOP_SECONDS
            completed_at = None
        else:
            status = "failed"
            next_run_at = now + BUILDING_AUTOMATION_RETRY_SECONDS
            completed_at = None

        last_result = _truncate_text(
            json.dumps(
                {
                    "reason": result.reason,
                    "actions": result.actions,
                    "details": result.details or {},
                },
                ensure_ascii=True,
                default=str,
            ),
            1800,
        )

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE building_automation_jobs
            SET status = ?,
                tax_complete = CASE WHEN ? THEN 1 ELSE tax_complete END,
                level_complete = CASE WHEN ? THEN 1 ELSE level_complete END,
                extensions_complete = CASE WHEN ? THEN 1 ELSE extensions_complete END,
                extensions_started = extensions_started + ?,
                attempts = attempts + 1,
                next_run_at = ?,
                last_result = ?,
                updated_at = ?,
                completed_at = COALESCE(?, completed_at)
            WHERE job_id = ?
            ''',
            (
                status,
                bool(result.tax_complete),
                bool(result.level_complete),
                bool(result.extensions_complete),
                int(result.extensions_started),
                int(next_run_at),
                last_result,
                now,
                completed_at,
                int(job_id),
            ),
        )
        conn.commit()
        conn.close()
    
    def get_stats_overall(self, guild_id: int) -> dict:
        """Get overall statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total counts by status
        cursor.execute('''
            SELECT status, COUNT(*) 
            FROM building_requests 
            WHERE guild_id = ?
            GROUP BY status
        ''', (guild_id,))
        status_counts = dict(cursor.fetchall())
        
        # By building type
        cursor.execute('''
            SELECT building_type, status, COUNT(*)
            FROM building_requests
            WHERE guild_id = ?
            GROUP BY building_type, status
        ''', (guild_id,))
        type_stats = cursor.fetchall()
        
        # Top requesters
        cursor.execute('''
            SELECT username, COUNT(*) as count
            FROM building_requests
            WHERE guild_id = ?
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 5
        ''', (guild_id,))
        top_requesters = cursor.fetchall()
        
        # Top admins
        cursor.execute('''
            SELECT admin_username, COUNT(*) as count
            FROM building_actions
            WHERE guild_id = ? AND admin_user_id IS NOT NULL
            GROUP BY admin_user_id
            ORDER BY count DESC
            LIMIT 5
        ''', (guild_id,))
        top_admins = cursor.fetchall()
        
        # Average response time
        cursor.execute('''
            SELECT AVG(ba.timestamp - br.created_at) as avg_time
            FROM building_requests br
            JOIN building_actions ba ON br.request_id = ba.request_id
            WHERE br.guild_id = ? AND ba.action_type IN ('approved', 'denied')
        ''', (guild_id,))
        avg_response_result = cursor.fetchone()
        avg_response_time = avg_response_result[0] if avg_response_result[0] else 0
        
        conn.close()
        
        return {
            "status_counts": status_counts,
            "type_stats": type_stats,
            "top_requesters": top_requesters,
            "top_admins": top_admins,
            "avg_response_time": avg_response_time
        }
    
    def get_stats_user(self, guild_id: int, user_id: int) -> dict:
        """Get user statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total counts by status
        cursor.execute('''
            SELECT status, COUNT(*) 
            FROM building_requests 
            WHERE guild_id = ? AND user_id = ?
            GROUP BY status
        ''', (guild_id, user_id))
        status_counts = dict(cursor.fetchall())
        
        # By building type
        cursor.execute('''
            SELECT building_type, status, COUNT(*)
            FROM building_requests
            WHERE guild_id = ? AND user_id = ?
            GROUP BY building_type, status
        ''', (guild_id, user_id))
        type_stats = cursor.fetchall()
        
        # Denial reasons
        cursor.execute('''
            SELECT ba.denial_reason, COUNT(*) as count
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE br.guild_id = ? AND br.user_id = ? AND ba.action_type = 'denied'
            GROUP BY ba.denial_reason
            ORDER BY count DESC
        ''', (guild_id, user_id))
        denial_reasons = cursor.fetchall()
        
        # Recent requests
        cursor.execute('''
            SELECT building_type, building_name, status, created_at
            FROM building_requests
            WHERE guild_id = ? AND user_id = ?
            ORDER BY created_at DESC
            LIMIT 5
        ''', (guild_id, user_id))
        recent_requests = cursor.fetchall()
        
        conn.close()
        
        return {
            "status_counts": status_counts,
            "type_stats": type_stats,
            "denial_reasons": denial_reasons,
            "recent_requests": recent_requests
        }
    
    def get_stats_admin(self, guild_id: int, admin_user_id: int) -> dict:
        """Get admin statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total actions by type
        cursor.execute('''
            SELECT action_type, COUNT(*)
            FROM building_actions
            WHERE guild_id = ? AND admin_user_id = ?
            GROUP BY action_type
        ''', (guild_id, admin_user_id))
        action_counts = dict(cursor.fetchall())
        
        # By building type
        cursor.execute('''
            SELECT br.building_type, ba.action_type, COUNT(*)
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE ba.guild_id = ? AND ba.admin_user_id = ?
            GROUP BY br.building_type, ba.action_type
        ''', (guild_id, admin_user_id))
        type_stats = cursor.fetchall()
        
        # Denial reasons breakdown
        cursor.execute('''
            SELECT denial_reason, COUNT(*) as count
            FROM building_actions
            WHERE guild_id = ? AND admin_user_id = ? AND action_type = 'denied'
            GROUP BY denial_reason
            ORDER BY count DESC
        ''', (guild_id, admin_user_id))
        denial_breakdown = cursor.fetchall()
        
        # Response times
        cursor.execute('''
            SELECT 
                AVG(ba.timestamp - br.created_at) as avg_time,
                MIN(ba.timestamp - br.created_at) as min_time,
                MAX(ba.timestamp - br.created_at) as max_time
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE ba.guild_id = ? AND ba.admin_user_id = ? AND ba.action_type IN ('approved', 'denied')
        ''', (guild_id, admin_user_id))
        response_times = cursor.fetchone()
        
        # Recent actions
        cursor.execute('''
            SELECT ba.action_type, br.building_type, br.username, ba.timestamp
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE ba.guild_id = ? AND ba.admin_user_id = ?
            ORDER BY ba.timestamp DESC
            LIMIT 5
        ''', (guild_id, admin_user_id))
        recent_actions = cursor.fetchall()
        
        conn.close()
        
        return {
            "action_counts": action_counts,
            "type_stats": type_stats,
            "denial_breakdown": denial_breakdown,
            "response_times": response_times or (0, 0, 0),
            "recent_actions": recent_actions
        }
    
    def get_stats_type(self, guild_id: int, building_type: str) -> dict:
        """Get building type statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total counts by status
        cursor.execute('''
            SELECT status, COUNT(*)
            FROM building_requests
            WHERE guild_id = ? AND building_type = ?
            GROUP BY status
        ''', (guild_id, building_type))
        status_counts = dict(cursor.fetchall())
        
        # Top requesters for this type
        cursor.execute('''
            SELECT username, COUNT(*) as count
            FROM building_requests
            WHERE guild_id = ? AND building_type = ?
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 5
        ''', (guild_id, building_type))
        top_requesters = cursor.fetchall()
        
        # Most common denial reason
        cursor.execute('''
            SELECT ba.denial_reason, COUNT(*) as count
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE br.guild_id = ? AND br.building_type = ? AND ba.action_type = 'denied'
            GROUP BY ba.denial_reason
            ORDER BY count DESC
            LIMIT 1
        ''', (guild_id, building_type))
        common_denial = cursor.fetchone()
        
        # Approval rate by admin
        cursor.execute('''
            SELECT 
                ba.admin_username,
                SUM(CASE WHEN ba.action_type = 'approved' THEN 1 ELSE 0 END) as approved,
                COUNT(*) as total
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE br.guild_id = ? AND br.building_type = ? 
              AND ba.action_type IN ('approved', 'denied')
              AND ba.admin_user_id IS NOT NULL
            GROUP BY ba.admin_user_id
        ''', (guild_id, building_type))
        admin_rates = cursor.fetchall()
        
        conn.close()
        
        return {
            "status_counts": status_counts,
            "top_requesters": top_requesters,
            "common_denial": common_denial,
            "admin_rates": admin_rates
        }

# ---------- Models ----------

class BuildingRequest:
    def __init__(
        self,
        user_id: int,
        username: str,
        building_type: str,
        building_name: str,
        location_input: str,
        coordinates: Optional[str] = None,
        address: Optional[str] = None,
        country: Optional[str] = None,
        region: Optional[str] = None,
        maps_url: Optional[str] = None,
        facility_warning: Optional[str] = None,
        notes: Optional[str] = None,
        request_id: Optional[int] = None,
    ):
        self.user_id = user_id
        self.username = username
        self.building_type = building_type
        self.building_name = building_name
        self.location_input = location_input
        self.coordinates = coordinates
        self.address = address
        self.country = country
        self.region = region
        self.maps_url = maps_url
        self.facility_warning = facility_warning
        self.notes = notes
        self.request_id = request_id

# ---------- Views ----------

class StartView(discord.ui.View):
    def __init__(self, cog: "BuildingManager"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Request Building", style=discord.ButtonStyle.primary, custom_id="bm:start")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Select the type of building you want to request.",
            view=BuildingTypeView(self.cog),
            ephemeral=True,
        )

class BuildingTypeView(discord.ui.View):
    def __init__(self, cog: "BuildingManager"):
        super().__init__(timeout=600)
        self.cog = cog
        self.add_item(BuildingTypeSelect(self.cog))

class BuildingTypeSelect(discord.ui.Select):
    def __init__(self, cog: "BuildingManager"):
        self.cog = cog
        # Default types
        options = [
            discord.SelectOption(label="Hospital", emoji="🏥", description="Medical facility"),
            discord.SelectOption(label="Prison", emoji="🔒", description="Correctional facility"),
        ]
        super().__init__(placeholder="Choose a building type", min_values=1, max_values=1, options=options, custom_id="bm:type")

    async def callback(self, interaction: discord.Interaction):
        building_type = self.values[0]
        modal = BuildingRequestModal(self.cog, building_type)
        await interaction.response.send_modal(modal)

class BuildingRequestModal(discord.ui.Modal, title="Building Request"):
    location = discord.ui.TextInput(
        label="Google Maps link",
        style=discord.TextStyle.short,
        max_length=500,
        required=True,
        placeholder="Paste a Google Maps place link or maps.app.goo.gl short link",
    )
    
    notes = discord.ui.TextInput(
        label="Additional Notes (Optional)",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=False,
        placeholder="Any additional information...",
    )

    def __init__(self, cog: "BuildingManager", building_type: str):
        super().__init__()
        self.cog = cog
        self.building_type = building_type

    async def on_submit(self, interaction: discord.Interaction):
        # Parse location
        await interaction.response.defer(ephemeral=True)
        
        location_input = str(self.location)
        location_details = await LocationParser.resolve_location(location_input)
        building_name = LocationParser.derive_building_name(self.building_type, location_details)
        location_details.facility_warning = LocationParser.facility_warning(
            self.building_type,
            building_name,
            location_details,
        )
        
        # Create request object
        req = BuildingRequest(
            user_id=interaction.user.id,
            username=str(interaction.user),
            building_type=self.building_type,
            building_name=building_name,
            location_input=location_input,
            coordinates=location_details.coordinates,
            address=location_details.address,
            country=location_details.country,
            region=location_details.region,
            maps_url=location_details.maps_url,
            facility_warning=location_details.facility_warning,
            notes=str(self.notes) if self.notes.value else None,
        )
        
        # Show summary
        view = SummaryView(self.cog, req)
        await view.send_summary(interaction)

class SummaryView(discord.ui.View):
    def __init__(self, cog: "BuildingManager", req: BuildingRequest):
        super().__init__(timeout=600)
        self.cog = cog
        self.req = req

    async def send_summary(self, interaction: discord.Interaction):
        """Display the summary embed."""
        embed = self._create_embed(interaction.user)
        await safe_update(
            interaction,
            content="⚠️ **Warning**: Once submitted, you cannot edit this request!\n\nReview your request:",
            embed=embed,
            view=self
        )

    def _add_location_details(self, embed: discord.Embed, *, warning_title: str = "Facility Check"):
        """Add resolved location fields to a request embed."""
        region_text = ", ".join(part for part in (self.req.region, self.req.country) if part)
        if region_text:
            embed.add_field(name="Country / Region", value=region_text[:200], inline=True)

        if self.req.address:
            embed.add_field(name="Address", value=self.req.address[:300], inline=False)

        if self.req.maps_url:
            embed.add_field(name="Maps URL", value=f"[Open in Google Maps]({self.req.maps_url})", inline=False)

        if self.req.facility_warning:
            embed.add_field(
                name=warning_title,
                value=f"⚠️ {self.req.facility_warning}",
                inline=False,
            )

    def _create_embed(self, user: discord.User) -> discord.Embed:
        """Create summary embed."""
        emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
        emoji = emoji_map.get(self.req.building_type, "🏢")
        
        embed = discord.Embed(
            title=f"{emoji} Building Request Summary",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        
        embed.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Building Type", value=self.req.building_type, inline=True)
        embed.add_field(name="Building Name", value=self.req.building_name, inline=True)
        embed.add_field(name="Location Input", value=self.req.location_input[:100], inline=False)
        
        if self.req.coordinates:
            embed.add_field(name="📍 Coordinates", value=self.req.coordinates, inline=True)
        else:
            embed.add_field(name="📍 Coordinates", value="Not detected", inline=True)

        self._add_location_details(embed)
        
        if self.req.notes:
            embed.add_field(name="Notes", value=self.req.notes[:200], inline=False)
        
        return embed

    @discord.ui.button(label="✏️ Edit", style=discord.ButtonStyle.secondary, custom_id="bm:edit")
    async def edit(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = BuildingRequestModal(self.cog, self.req.building_type)
        modal.location.default = self.req.location_input
        if self.req.notes:
            modal.notes.default = self.req.notes
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="📤 Submit to Admin", style=discord.ButtonStyle.success, custom_id="bm:submit")
    async def submit(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This only works inside a server.", ephemeral=True)
            return

        conf = await self.cog.config.guild(guild).all()
        admin_channel_id = conf.get("admin_channel_id")
        log_channel_id = conf.get("log_channel_id")

        if not admin_channel_id or not log_channel_id:
            await interaction.response.send_message(
                "Admin/Log channels are not configured yet. Ask an admin to use [p]buildset.",
                ephemeral=True,
            )
            return

        admin_channel = guild.get_channel(admin_channel_id)
        log_channel = guild.get_channel(log_channel_id)

        if not admin_channel or not log_channel:
            await interaction.response.send_message("One or more configured channels could not be found.", ephemeral=True)
            return

        # Save to database
        request_id = self.cog.db.add_request(
            guild_id=guild.id,
            user_id=self.req.user_id,
            username=self.req.username,
            building_type=self.req.building_type,
            building_name=self.req.building_name,
            location_input=self.req.location_input,
            coordinates=self.req.coordinates,
            address=self.req.address,
            notes=self.req.notes
        )
        
        self.req.request_id = request_id

        # Send to admin channel
        emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
        emoji = emoji_map.get(self.req.building_type, "🏢")
        
        emb = discord.Embed(
            title=f"{emoji} New Building Request",
            color=discord.Color.yellow(),
            timestamp=datetime.now(timezone.utc),
        )
        
        user = interaction.user
        emb.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        emb.add_field(name="Building Type", value=self.req.building_type, inline=True)
        emb.add_field(name="Building Name", value=self.req.building_name, inline=True)
        emb.add_field(name="Location Input", value=self.req.location_input[:100], inline=False)
        
        if self.req.coordinates:
            emb.add_field(name="📍 Coordinates", value=self.req.coordinates, inline=True)
        else:
            emb.add_field(name="📍 Coordinates", value="Not detected", inline=True)

        self._add_location_details(emb, warning_title="Facility Check Warning")
        
        if self.req.notes:
            emb.add_field(name="Notes", value=self.req.notes[:200], inline=False)
        
        emb.set_footer(text=f"Request ID: {request_id}")

        view = AdminDecisionView(self.cog, requester_id=user.id, req=self.req)
        await admin_channel.send(embed=emb, view=view)

        # Log to log channel
        log_emb = discord.Embed(
            title="Request submitted",
            description=f"By {user.mention}",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        log_emb.add_field(name="Building", value=f"{self.req.building_type} - {self.req.building_name}", inline=False)
        if self.req.coordinates:
            log_emb.add_field(name="Coordinates", value=self.req.coordinates, inline=True)
        if self.req.maps_url:
            log_emb.add_field(name="Maps", value=f"[Open]({self.req.maps_url})", inline=True)
        log_emb.add_field(name="Request ID", value=str(request_id), inline=True)
        await log_channel.send(embed=log_emb)

        # Disable all buttons
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

        await safe_update(interaction, content="✅ Request submitted to Admin. You'll be notified of any updates.", embed=None, view=self)

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary, custom_id="bm:cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True
        
        await safe_update(interaction, content="❌ Request cancelled.", embed=None, view=self)

# ---------- Admin Decision ----------

class AdminDecisionView(discord.ui.View):
    def __init__(self, cog: "BuildingManager", requester_id: int, req: BuildingRequest):
        super().__init__(timeout=None)
        self.cog = cog
        self.requester_id = requester_id
        self.req = req

    async def _is_admin(self, interaction: discord.Interaction) -> bool:
        guild = interaction.guild
        if guild is None or not isinstance(interaction.user, discord.Member):
            return False
        role_id = await self.cog.config.guild(guild).admin_role_id()
        if role_id is None:
            return False
        role = guild.get_role(role_id)
        return role in interaction.user.roles if role else False

    @discord.ui.button(label="✅ Approve", style=discord.ButtonStyle.success, custom_id="bm:approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._is_admin(interaction):
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        guild = interaction.guild
        conf = await self.cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None
        create_result = await self.cog._create_alliance_building_browser(self.req)
        final_status = "created" if create_result.ok else "approved_pending_manual"
        automation_message = None
        if create_result.ok:
            building_id = create_result.building_id
            if building_id:
                job_id = self.cog.db.add_or_update_automation_job(
                    request_id=self.req.request_id,
                    guild_id=guild.id,
                    building_id=building_id,
                    building_type=self.req.building_type,
                    building_name=self.req.building_name,
                )
                automation_message = (
                    f"Queued post-creation automation for MissionChief building `{building_id}`: "
                    f"tax {ALLIANCE_BUILDING_TARGET_TAX}%, max level, and allowed extensions."
                )
                self.cog.bot.loop.create_task(self.cog._process_building_automation_job(job_id))
            else:
                automation_message = (
                    "Created in MissionChief, but the building ID was not detected. "
                    "Post-creation automation was not queued."
                )

        # Update database
        self.cog.db.update_request_status(self.req.request_id, final_status)
        self.cog.db.add_action(
            request_id=self.req.request_id,
            guild_id=guild.id,
            admin_user_id=interaction.user.id,
            admin_username=str(interaction.user),
            action_type="approved"
        )
        self.cog.db.add_action(
            request_id=self.req.request_id,
            guild_id=guild.id,
            admin_user_id=interaction.user.id,
            admin_username=str(interaction.user),
            action_type="created" if create_result.ok else "create_failed",
            previous_values=None if create_result.ok else create_result.reason[:900],
        )

        user = guild.get_member(self.requester_id) if guild else None
        emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
        emoji = emoji_map.get(self.req.building_type, "🏢")
        
        ok_text = (
            f"✅ Your building request has been **APPROVED**.\n\n"
            f"{emoji} **{self.req.building_type}**: {self.req.building_name}\n"
        )
        if self.req.coordinates:
            ok_text += f"📍 Coordinates: {self.req.coordinates}\n"
        if self.req.maps_url:
            ok_text += f"Maps: {self.req.maps_url}\n"
        if self.req.address:
            ok_text += f"📫 Address: {self.req.address}\n"
        if self.req.notes:
            ok_text += f"\nNotes: {self.req.notes}"

        if user:
            try:
                await user.send(ok_text)
            except discord.Forbidden:
                pass

        if log_channel:
            emb = discord.Embed(
                title="Building request approved and created" if create_result.ok else "Building request approved - manual creation needed",
                color=discord.Color.green() if create_result.ok else discord.Color.orange(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Requester", value=requester, inline=False)
            emb.add_field(name="Building", value=f"{self.req.building_type} - {self.req.building_name}", inline=False)
            if self.req.coordinates:
                emb.add_field(name="Coordinates", value=self.req.coordinates, inline=True)
            if self.req.maps_url:
                emb.add_field(name="Maps", value=f"[Open]({self.req.maps_url})", inline=True)
            region_text = ", ".join(part for part in (self.req.region, self.req.country) if part)
            if region_text:
                emb.add_field(name="Country / Region", value=region_text[:200], inline=True)
            emb.add_field(name="Approved by", value=f"{interaction.user.mention} ({interaction.user.id})", inline=False)
            emb.add_field(name="Auto Creation", value="Created in MissionChief" if create_result.ok else create_result.reason[:900], inline=False)
            if automation_message:
                emb.add_field(name="Post-Creation Automation", value=automation_message[:900], inline=False)
            if create_result.status is not None:
                emb.add_field(name="MissionChief HTTP Status", value=str(create_result.status), inline=True)
            emb.add_field(name="Request ID", value=str(self.req.request_id), inline=True)
            await log_channel.send(embed=emb)

        if create_result.ok:
            try:
                await interaction.message.delete()
            except Exception:
                pass

        if create_result.ok:
            if automation_message:
                await interaction.followup.send(
                    f"Request approved and alliance building created. {automation_message}",
                    ephemeral=True,
                )
            else:
                await interaction.followup.send("Request approved and alliance building created.", ephemeral=True)
        else:
            await interaction.followup.send(
                f"Request approved, but automatic creation failed: {create_result.reason}",
                ephemeral=True,
            )

    @discord.ui.button(label="❌ Reject", style=discord.ButtonStyle.danger, custom_id="bm:deny")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._is_admin(interaction):
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return
        
        # Show denial reason selector
        await interaction.response.send_message(
            "Select a denial reason:",
            view=DenialReasonView(self.cog, self.requester_id, self.req, interaction.message, interaction.user),
            ephemeral=True
        )

class DenialReasonView(discord.ui.View):
    def __init__(self, cog: "BuildingManager", requester_id: int, req: BuildingRequest, admin_msg: discord.Message, admin_user: discord.User):
        super().__init__(timeout=600)
        self.cog = cog
        self.requester_id = requester_id
        self.req = req
        self.admin_msg = admin_msg
        self.admin_user = admin_user
        self.add_item(DenialReasonSelect(self.cog, requester_id, req, admin_msg, admin_user))

class DenialReasonSelect(discord.ui.Select):
    def __init__(self, cog: "BuildingManager", requester_id: int, req: BuildingRequest, admin_msg: discord.Message, admin_user: discord.User):
        self.cog = cog
        self.requester_id = requester_id
        self.req = req
        self.admin_msg = admin_msg
        self.admin_user = admin_user
        
        options = [
            discord.SelectOption(label="Location not found", value="Location not found"),
            discord.SelectOption(label="Not a real-life location", value="Not a real-life location"),
            discord.SelectOption(label="Duplicate building already exists", value="Duplicate building already exists"),
            discord.SelectOption(label="Insufficient detail provided", value="Insufficient detail provided"),
            discord.SelectOption(label="Other (custom reason)", value="custom"),
        ]
        super().__init__(placeholder="Choose a denial reason", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        reason = self.values[0]
        
        if reason == "custom":
            modal = CustomDenialModal(self.cog, self.requester_id, self.req, self.admin_msg, self.admin_user)
            await interaction.response.send_modal(modal)
        else:
            await self._process_denial(interaction, reason)

    async def _process_denial(self, interaction: discord.Interaction, reason: str):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Internal error: no guild.", ephemeral=True)
            return
        
        conf = await self.cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None

        self.cog.db.update_request_status(self.req.request_id, "denied")
        self.cog.db.add_action(
            request_id=self.req.request_id,
            guild_id=guild.id,
            admin_user_id=self.admin_user.id,
            admin_username=str(self.admin_user),
            action_type="denied",
            denial_reason=reason
        )

        user = guild.get_member(self.requester_id)
        text = (
            f"❌ Your building request has been **DENIED**.\n\n"
            f"**Building**: {self.req.building_type} - {self.req.building_name}\n"
            f"**Reason**: {reason}"
        )
        
        if user:
            try:
                await user.send(text)
            except discord.Forbidden:
                pass

        if log_channel:
            emb = discord.Embed(
                title="Building request denied",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Requester", value=requester, inline=False)
            emb.add_field(name="Building", value=f"{self.req.building_type} - {self.req.building_name}", inline=False)
            emb.add_field(name="Reason", value=reason, inline=False)
            emb.add_field(name="Denied by", value=f"{self.admin_user.mention} ({self.admin_user.id})", inline=False)
            emb.add_field(name="Request ID", value=str(self.req.request_id), inline=True)
            await log_channel.send(embed=emb)

        try:
            await self.admin_msg.delete()
        except Exception:
            pass

        await interaction.response.send_message("Denial processed and logged.", ephemeral=True)

class CustomDenialModal(discord.ui.Modal, title="Custom Denial Reason"):
    reason = discord.ui.TextInput(
        label="Reason",
        style=discord.TextStyle.paragraph,
        max_length=400,
        required=True,
        placeholder="Explain why this request is denied...",
    )

    def __init__(self, cog: "BuildingManager", requester_id: int, req: BuildingRequest, admin_msg: discord.Message, admin_user: discord.User):
        super().__init__()
        self.cog = cog
        self.requester_id = requester_id
        self.req = req
        self.admin_msg = admin_msg
        self.admin_user = admin_user

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Internal error: no guild.", ephemeral=True)
            return
        
        conf = await self.cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None

        reason_text = f"Other: {self.reason}"

        self.cog.db.update_request_status(self.req.request_id, "denied")
        self.cog.db.add_action(
            request_id=self.req.request_id,
            guild_id=guild.id,
            admin_user_id=self.admin_user.id,
            admin_username=str(self.admin_user),
            action_type="denied",
            denial_reason=reason_text
        )

        user = guild.get_member(self.requester_id)
        text = (
            f"❌ Your building request has been **DENIED**.\n\n"
            f"**Building**: {self.req.building_type} - {self.req.building_name}\n"
            f"**Reason**: {reason_text}"
        )
        
        if user:
            try:
                await user.send(text)
            except discord.Forbidden:
                pass

        if log_channel:
            emb = discord.Embed(
                title="Building request denied",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Requester", value=requester, inline=False)
            emb.add_field(name="Building", value=f"{self.req.building_type} - {self.req.building_name}", inline=False)
            emb.add_field(name="Reason", value=reason_text, inline=False)
            emb.add_field(name="Denied by", value=f"{self.admin_user.mention} ({self.admin_user.id})", inline=False)
            emb.add_field(name="Request ID", value=str(self.req.request_id), inline=True)
            await log_channel.send(embed=emb)

        try:
            await self.admin_msg.delete()
        except Exception:
            pass

        await interaction.response.send_message("Denial processed and logged.", ephemeral=True)

# ---------- Cog ----------

class BuildingManager(commands.Cog):
    """Building request system with location parsing and statistics."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xDEADBEEF, force_registration=True)
        default_guild = {
            "request_channel_id": None,
            "admin_channel_id": None,
            "log_channel_id": None,
            "admin_role_id": None,
            "button_message": None,
        }
        default_global = {
            "google_api_key": None,
            "default_request_panel_message_id": None,
        }
        self.config.register_guild(**default_guild)
        self.config.register_global(**default_global)

        # Initialize database
        from redbot.core import data_manager
        db_path = str(data_manager.cog_data_path(self) / "building_manager.db")
        self.db = BuildingDatabase(db_path)

        self._panel_task = None
        self._automation_task = None
        self._browser_lock = asyncio.Lock()
        self._persistent_view_registered = False
        self._register_persistent_views()
        self._start_panel_task()
        self._start_automation_task()

    def cog_unload(self):  # <-- Let op: 4 spaties inspringing, zelfde niveau als __init__
        if getattr(self, "_panel_task", None):
            self._panel_task.cancel()
        if getattr(self, "_automation_task", None):
            self._automation_task.cancel()

    async def cog_load(self):
        """Register persistent views and ensure the default request panel exists."""
        self._register_persistent_views()
        self._start_panel_task()
        self._start_automation_task()

    def _register_persistent_views(self):
        """Register persistent component views once per cog instance."""
        if self._persistent_view_registered:
            return
        self.bot.add_view(StartView(self))
        self._persistent_view_registered = True

    def _start_panel_task(self):
        """Start the default panel task if it is not already running."""
        if self._panel_task and not self._panel_task.done():
            return
        self._panel_task = self.bot.loop.create_task(self._ensure_default_request_panel())

    def _start_automation_task(self):
        """Start the post-creation automation worker if it is not already running."""
        if self._automation_task and not self._automation_task.done():
            return
        self._automation_task = self.bot.loop.create_task(self._building_automation_loop())

    def _cookie_manager(self):
        """Return the CookieManager cog when it exposes a MissionChief session."""
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager or not hasattr(cookie_manager, "get_session"):
            return None
        return cookie_manager

    async def _get_session(self):
        """Return the MissionChief aiohttp session from CookieManager."""
        cookie_manager = self._cookie_manager()
        if not cookie_manager:
            raise RuntimeError("CookieManager is not loaded.")
        session = await cookie_manager.get_session()
        if not session:
            raise RuntimeError("CookieManager did not return a session.")
        return session

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

    async def _browser_diagnostics(self, target_url: str) -> str:
        """Inspect a MissionChief page in a logged-in browser without submitting anything."""
        try:
            from playwright.async_api import async_playwright
        except Exception:
            raise RuntimeError(PLAYWRIGHT_SETUP_MESSAGE)

        cookies = await self._playwright_cookies()
        if not cookies:
            raise RuntimeError("No MissionChief cookies are available from CookieManager.")

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            try:
                context = await browser.new_context(viewport={"width": 1440, "height": 1000})
                await context.add_cookies(cookies)
                page = await context.new_page()
                page.set_default_timeout(30000)
                await page.goto(target_url, wait_until="domcontentloaded")
                login_fields = await page.locator("input[type='password']").count()
                if login_fields:
                    raise RuntimeError("MissionChief session is not logged in.")
                snapshot = await page.evaluate(BUILDING_DIAGNOSTICS_SCRIPT)
            finally:
                await browser.close()

        return build_browser_diagnostics_report(snapshot or {})

    async def _create_alliance_building_browser(self, req: BuildingRequest) -> BuildingCreateResult:
        """Create an approved Hospital or Prison as an alliance building through the live browser form."""
        try:
            config = build_alliance_building_config(
                building_type=req.building_type,
                building_name=req.building_name,
                coordinates=req.coordinates,
                address=req.address,
            )
        except ValueError as exc:
            return BuildingCreateResult(False, str(exc))

        try:
            from playwright.async_api import TimeoutError as PlaywrightTimeoutError
            from playwright.async_api import async_playwright
        except Exception:
            return BuildingCreateResult(False, PLAYWRIGHT_SETUP_MESSAGE)

        try:
            cookies = await self._playwright_cookies()
        except Exception as exc:
            return BuildingCreateResult(False, f"Could not load MissionChief cookies: {exc}")
        if not cookies:
            return BuildingCreateResult(False, "No MissionChief cookies are available from CookieManager.")

        async with self._browser_lock:
            status: Optional[int] = None
            response_text = ""
            prepare_result: Dict[str, Any] = {}
            response_url = ""
            final_url = ""
            api_lookup: Dict[str, Any] = {}
            alliance_list_lookup: Dict[str, Any] = {}
            try:
                async with async_playwright() as playwright:
                    browser = await playwright.chromium.launch(headless=True)
                    try:
                        context = await browser.new_context(viewport={"width": 1440, "height": 1000})
                        await context.add_cookies(cookies)
                        page = await context.new_page()
                        page.set_default_timeout(30000)
                        await page.goto(MISSIONCHIEF_NEW_BUILDING_URL, wait_until="domcontentloaded")

                        login_fields = await page.locator("input[type='password']").count()
                        if login_fields:
                            return BuildingCreateResult(False, "MissionChief session is not logged in.")

                        prepare_result = await page.evaluate(BUILDING_CREATE_SCRIPT, config)
                        if not prepare_result.get("ok"):
                            return BuildingCreateResult(
                                False,
                                str(prepare_result.get("reason") or "MissionChief building form could not be prepared."),
                                details=prepare_result.get("snapshot") or {},
                            )

                        async with page.expect_response(
                            lambda response: "/buildings" in response.url and response.request.method.upper() == "POST",
                            timeout=30000,
                        ) as response_info:
                            clicked = await page.evaluate(BUILDING_CLICK_CREATE_SCRIPT, prepare_result.get("submitIndex"))
                            if not clicked:
                                return BuildingCreateResult(
                                    False,
                                    "Browser could not click the alliance build button.",
                                    details=prepare_result.get("snapshot") or {},
                                )
                        response = await response_info.value
                        status = response.status
                        response_url = str(response.url or "")
                        with contextlib.suppress(Exception):
                            response_text = await response.text()
                        with contextlib.suppress(Exception):
                            await page.wait_for_load_state("domcontentloaded", timeout=10000)
                        final_url = str(page.url or "")
                        detected_id = extract_missionchief_building_id(response_url, final_url, response_text)
                        if not detected_id and status is not None and int(status) < 400:
                            with contextlib.suppress(Exception):
                                api_lookup = await page.evaluate(BUILDING_FETCH_API_SCRIPT)
                                if api_lookup.get("ok"):
                                    detected_id = find_created_alliance_building_id(
                                        api_lookup.get("buildings") or [],
                                        config,
                                    )
                                    api_lookup = {
                                        "ok": True,
                                        "status": api_lookup.get("status"),
                                        "count": len(api_lookup.get("buildings") or []),
                                        "matchedBuildingId": detected_id,
                                    }
                        if not detected_id and status is not None and int(status) < 400:
                            with contextlib.suppress(Exception):
                                alliance_list_lookup = await page.evaluate(
                                    BUILDING_FETCH_ALLIANCE_LIST_SCRIPT,
                                    {"maxPages": 8},
                                )
                                if alliance_list_lookup.get("ok"):
                                    candidates = alliance_list_lookup.get("candidates") or []
                                    detected_id = find_created_alliance_building_id_from_list(candidates, config)
                                    alliance_list_lookup = {
                                        "ok": True,
                                        "status": alliance_list_lookup.get("status"),
                                        "pages": alliance_list_lookup.get("pages") or [],
                                        "count": len(candidates),
                                        "matchedBuildingId": detected_id,
                                    }
                    finally:
                        await browser.close()
            except PlaywrightTimeoutError as exc:
                details = dict(prepare_result.get("snapshot") or {})
                details.update(
                    {
                        "responseUrl": response_url,
                        "finalUrl": final_url,
                        "apiLookup": api_lookup,
                        "allianceListLookup": alliance_list_lookup,
                    }
                )
                return BuildingCreateResult(
                    False,
                    f"MissionChief browser building flow timed out: {exc}",
                    status=status,
                    post_url=f"{BASE_URL}/buildings",
                    details=details,
                )
            except Exception as exc:
                message = str(exc)
                if "Executable doesn't exist" in message or "playwright install" in message:
                    return BuildingCreateResult(False, PLAYWRIGHT_SETUP_MESSAGE)
                return BuildingCreateResult(False, f"MissionChief browser building flow failed: {message}")

        details = dict(prepare_result.get("snapshot") or {})
        building_id = extract_missionchief_building_id(response_url, final_url, response_text)
        if not building_id:
            building_id = _coerce_int(api_lookup.get("matchedBuildingId"))
        if not building_id:
            building_id = _coerce_int(alliance_list_lookup.get("matchedBuildingId"))
        details.update(
            {
                "responseUrl": response_url,
                "finalUrl": final_url,
                "apiLookup": api_lookup,
                "allianceListLookup": alliance_list_lookup,
                "buildingId": building_id,
            }
        )

        if status is None or int(status) >= 400:
            response_summary = _truncate_text(re.sub(r"<[^>]+>", " ", response_text), 300)
            suffix = f" Response: {response_summary}" if response_summary else ""
            return BuildingCreateResult(
                False,
                f"MissionChief returned HTTP {status} while creating the alliance building.{suffix}",
                status=status,
                post_url=f"{BASE_URL}/buildings",
                details=details,
            )

        return BuildingCreateResult(
            True,
            "Alliance building created through browser automation.",
            status=status,
            post_url=final_url or response_url or f"{BASE_URL}/buildings",
            details=details,
        )

    async def _building_automation_loop(self):
        """Process post-creation automation jobs for alliance buildings."""
        try:
            await self.bot.wait_until_ready()
            await asyncio.sleep(30)
            while True:
                try:
                    due_jobs = self.db.get_due_automation_jobs(limit=5)
                    for job in due_jobs:
                        await self._process_building_automation_job(job.job_id)
                        await asyncio.sleep(5)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("BuildingManager post-creation automation loop failed")
                await asyncio.sleep(BUILDING_AUTOMATION_LOOP_SECONDS)
        except asyncio.CancelledError:
            raise

    async def _process_building_automation_job(self, job_id: int) -> Optional[BuildingAutomationResult]:
        """Run and persist one post-creation automation job."""
        job = self.db.get_automation_job(job_id)
        if not job or job.status == "completed":
            return None

        result = await self._upgrade_alliance_building_browser(job)
        self.db.update_automation_job(job.job_id, result)
        action_type = "automation_completed" if result.completed else "automation_waiting" if result.wait else "automation_run"
        if not result.ok:
            action_type = "automation_failed"
        self.db.add_action(
            request_id=job.request_id,
            guild_id=job.guild_id,
            admin_user_id=None,
            admin_username="BuildingManager",
            action_type=action_type,
            previous_values=_truncate_text(result.reason, 900),
        )
        await self._log_building_automation_result(job, result)
        return result

    async def _upgrade_alliance_building_browser(self, job: BuildingAutomationJob) -> BuildingAutomationResult:
        """Set alliance building tax, level, and extensions through the live browser page."""
        if job.building_type not in ALLIANCE_BUILDING_TYPE_IDS:
            return BuildingAutomationResult(
                ok=False,
                completed=False,
                wait=False,
                reason=f"Unsupported automation building type: {job.building_type}",
                actions=[],
            )

        try:
            from playwright.async_api import TimeoutError as PlaywrightTimeoutError
            from playwright.async_api import async_playwright
        except Exception:
            return BuildingAutomationResult(False, False, True, PLAYWRIGHT_SETUP_MESSAGE, [])

        try:
            cookies = await self._playwright_cookies()
        except Exception as exc:
            return BuildingAutomationResult(False, False, True, f"Could not load MissionChief cookies: {exc}", [])
        if not cookies:
            return BuildingAutomationResult(False, False, True, "No MissionChief cookies are available from CookieManager.", [])

        building_url = f"{BASE_URL}/buildings/{job.building_id}"
        actions: List[str] = []
        details: Dict[str, Any] = {"building_url": building_url}
        tax_complete = job.tax_complete
        level_complete = job.level_complete
        extensions_complete = job.extensions_complete
        extensions_started_this_run = 0
        last_status: Optional[int] = None
        last_prepare: Dict[str, Any] = {}

        async with self._browser_lock:
            try:
                async with async_playwright() as playwright:
                    browser = await playwright.chromium.launch(headless=True)
                    try:
                        context = await browser.new_context(viewport={"width": 1440, "height": 1000})
                        await context.add_cookies(cookies)
                        page = await context.new_page()
                        page.set_default_timeout(30000)

                        def _accept_dialog(dialog):
                            self.bot.loop.create_task(dialog.accept())

                        page.on("dialog", _accept_dialog)

                        await page.goto(building_url, wait_until="domcontentloaded")
                        login_fields = await page.locator("input[type='password']").count()
                        if login_fields:
                            return BuildingAutomationResult(
                                False,
                                False,
                                True,
                                "MissionChief session is not logged in.",
                                actions,
                                details=details,
                            )

                        for _ in range(BUILDING_AUTOMATION_MAX_ACTIONS_PER_RUN):
                            prepare_config = {
                                "targetTax": str(job.target_tax),
                                "taxComplete": bool(tax_complete),
                                "levelComplete": bool(level_complete),
                                "extensionsComplete": bool(extensions_complete),
                                "extensionsStartedThisRun": extensions_started_this_run,
                                "maxExtensionStarts": BUILDING_AUTOMATION_MAX_EXTENSION_STARTS_PER_RUN,
                                "excludedLabels": sorted(BUILDING_AUTOMATION_EXCLUDED_EXTENSIONS),
                            }
                            last_prepare = await page.evaluate(BUILDING_AUTOMATION_PREPARE_SCRIPT, prepare_config)
                            details["last_prepare"] = last_prepare
                            if not last_prepare.get("ok"):
                                return BuildingAutomationResult(
                                    False,
                                    False,
                                    True,
                                    str(last_prepare.get("reason") or "MissionChief building automation could not prepare an action."),
                                    actions,
                                    details=details,
                                )

                            action = str(last_prepare.get("action") or "")
                            label = _truncate_text(last_prepare.get("label") or action or "MissionChief action", 160)
                            if action == "tax_already_set":
                                tax_complete = True
                                if label not in actions:
                                    actions.append(label)
                                continue

                            if not action:
                                completed = bool(last_prepare.get("completed")) and tax_complete
                                wait = bool(last_prepare.get("wait")) or not completed
                                reason = str(last_prepare.get("reason") or "No remaining eligible actions were found.")
                                if not tax_complete and last_prepare.get("taxState") == "not_found":
                                    reason = "Tax field was not found on the MissionChief building page; retrying later."
                                if completed:
                                    level_complete = True
                                    extensions_complete = True
                                return BuildingAutomationResult(
                                    True,
                                    completed,
                                    wait,
                                    reason,
                                    actions,
                                    tax_complete=tax_complete,
                                    level_complete=level_complete,
                                    extensions_complete=extensions_complete,
                                    extensions_started=extensions_started_this_run,
                                    status=last_status,
                                    details=details,
                                )

                            selector = last_prepare.get("selector")
                            if not selector:
                                return BuildingAutomationResult(
                                    False,
                                    False,
                                    True,
                                    f"MissionChief action `{action}` did not expose a safe selector.",
                                    actions,
                                    details=details,
                                )

                            try:
                                await page.locator(str(selector)).click(timeout=15000)
                                with contextlib.suppress(PlaywrightTimeoutError):
                                    await page.wait_for_load_state("domcontentloaded", timeout=5000)
                                await page.wait_for_timeout(1200)
                                with contextlib.suppress(Exception):
                                    response = await page.goto(building_url, wait_until="domcontentloaded", timeout=30000)
                                    if response:
                                        last_status = response.status
                            except PlaywrightTimeoutError as exc:
                                return BuildingAutomationResult(
                                    False,
                                    False,
                                    True,
                                    f"MissionChief building automation timed out while running `{label}`: {exc}",
                                    actions,
                                    status=last_status,
                                    details=details,
                                )

                            actions.append(label)
                            if action == "set_tax":
                                tax_complete = True
                            elif action == "start_extension":
                                extensions_started_this_run += 1

                        return BuildingAutomationResult(
                            True,
                            False,
                            True,
                            "Action limit reached for this run; queued for the next pass.",
                            actions,
                            tax_complete=tax_complete,
                            level_complete=level_complete,
                            extensions_complete=extensions_complete,
                            extensions_started=extensions_started_this_run,
                            status=last_status,
                            details=details,
                        )
                    finally:
                        await browser.close()
            except PlaywrightTimeoutError as exc:
                return BuildingAutomationResult(
                    False,
                    False,
                    True,
                    f"MissionChief building automation timed out: {exc}",
                    actions,
                    status=last_status,
                    details=details,
                )
            except Exception as exc:
                message = str(exc)
                if "Executable doesn't exist" in message or "playwright install" in message:
                    return BuildingAutomationResult(False, False, True, PLAYWRIGHT_SETUP_MESSAGE, actions, details=details)
                return BuildingAutomationResult(
                    False,
                    False,
                    True,
                    f"MissionChief building automation failed: {message}",
                    actions,
                    status=last_status,
                    details=details,
                )

    async def _log_building_automation_result(self, job: BuildingAutomationJob, result: BuildingAutomationResult):
        """Send a compact admin log for one automation pass when useful."""
        if not result.actions and result.ok and result.wait:
            return
        guild = self.bot.get_guild(job.guild_id)
        if guild is None:
            return
        conf = await self.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None
        if log_channel is None:
            return

        if result.completed:
            color = discord.Color.green()
            title = "Alliance building automation completed"
        elif result.ok:
            color = discord.Color.blue()
            title = "Alliance building automation updated"
        else:
            color = discord.Color.orange()
            title = "Alliance building automation needs attention"

        embed = discord.Embed(title=title, color=color, timestamp=datetime.now(timezone.utc))
        embed.add_field(name="Building", value=f"{job.building_type} - {job.building_name}", inline=False)
        embed.add_field(name="MissionChief", value=f"[Open]({BASE_URL}/buildings/{job.building_id})", inline=True)
        embed.add_field(name="Request ID", value=str(job.request_id), inline=True)
        if result.actions:
            embed.add_field(name="Actions", value="\n".join(f"- {action}" for action in result.actions)[:1000], inline=False)
        embed.add_field(name="Status", value=result.reason[:900], inline=False)
        if result.wait and not result.completed:
            embed.add_field(
                name="Next check",
                value=fmt_dt(ts() + BUILDING_AUTOMATION_RETRY_SECONDS),
                inline=True,
            )
        await log_channel.send(embed=embed)

    def _request_panel_embed(self, description: Optional[str] = None) -> discord.Embed:
        """Build the persistent request panel embed."""
        if description is None:
            description = (
                "Request a new Hospital or Prison placement by clicking the button below.\n\n"
                "Accepted location formats:\n"
                "- Google Maps place link\n"
                "- Google Maps short link, for example `https://maps.app.goo.gl/...`\n\n"
                "The building name is detected automatically from the location when possible. "
                "Your request will be reviewed by admins."
            )

        return discord.Embed(
            title=REQUEST_PANEL_TITLE,
            description=description,
            color=discord.Color.blue(),
        )

    async def _ensure_default_request_panel(self):
        """Post or update the default request panel after the cog loads."""
        try:
            await self.bot.wait_until_ready()
            channel = self.bot.get_channel(DEFAULT_REQUEST_PANEL_CHANNEL_ID)
            if channel is None:
                try:
                    channel = await self.bot.fetch_channel(DEFAULT_REQUEST_PANEL_CHANNEL_ID)
                except Exception:
                    log.warning("Default BuildingManager request panel channel was not found.")
                    return

            embed = self._request_panel_embed()
            message_id = await self.config.default_request_panel_message_id()
            if message_id:
                try:
                    message = await channel.fetch_message(message_id)
                    await message.edit(embed=embed, view=StartView(self))
                    return
                except discord.NotFound:
                    await self.config.default_request_panel_message_id.set(None)
                except Exception:
                    log.warning("Stored BuildingManager request panel could not be edited; trying history scan.")

            try:
                async for message in channel.history(limit=50):
                    if getattr(message.author, "id", None) != getattr(self.bot.user, "id", None):
                        continue
                    if any(getattr(existing, "title", None) == REQUEST_PANEL_TITLE for existing in message.embeds):
                        await message.edit(embed=embed, view=StartView(self))
                        await self.config.default_request_panel_message_id.set(message.id)
                        return
            except Exception:
                log.warning("Could not scan for an existing BuildingManager request panel; posting a new one.")

            sent = await channel.send(embed=embed, view=StartView(self))
            await self.config.default_request_panel_message_id.set(sent.id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Failed to ensure default BuildingManager request panel")

    @commands.group(name="buildset", invoke_without_command=True)
    @commands.admin()
    @commands.guild_only()
    async def buildset(self, ctx: commands.Context):
        """Configure Building Manager."""
        conf = await self.config.guild(ctx.guild).all()
        txt = (
            f"Request channel: {ctx.guild.get_channel(conf['request_channel_id']).mention if conf.get('request_channel_id') else '—'}\n"
            f"Admin channel: {ctx.guild.get_channel(conf['admin_channel_id']).mention if conf.get('admin_channel_id') else '—'}\n"
            f"Log channel: {ctx.guild.get_channel(conf['log_channel_id']).mention if conf.get('log_channel_id') else '—'}\n"
            f"Admin role: {ctx.guild.get_role(conf['admin_role_id']).mention if conf.get('admin_role_id') else '—'}\n"
            f"Custom button message: {'Set' if conf.get('button_message') else 'Not set (using default)'}\n"
        )
        await ctx.send(box(txt, lang="ini"))

    @buildset.command(name="requestchannel")
    @commands.admin()
    @commands.guild_only()
    async def requestchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where users can request buildings."""
        await self.config.guild(ctx.guild).request_channel_id.set(channel.id)
        await ctx.tick()

    @buildset.command(name="adminchannel")
    @commands.admin()
    @commands.guild_only()
    async def adminchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where admin approval requests are sent."""
        await self.config.guild(ctx.guild).admin_channel_id.set(channel.id)
        await ctx.tick()

    @buildset.command(name="logchannel")
    @commands.admin()
    @commands.guild_only()
    async def logchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where all actions are logged."""
        await self.config.guild(ctx.guild).log_channel_id.set(channel.id)
        await ctx.tick()

    @buildset.command(name="adminrole")
    @commands.admin()
    @commands.guild_only()
    async def adminrole(self, ctx: commands.Context, role: discord.Role):
        """Set the role that can approve/deny building requests."""
        await self.config.guild(ctx.guild).admin_role_id.set(role.id)
        await ctx.tick()

    @buildset.command(name="buttonmessage")
    @commands.admin()
    @commands.guild_only()
    async def buttonmessage(self, ctx: commands.Context, *, message: str = None):
        """Set a custom message above the Request Building button."""
        if message:
            await self.config.guild(ctx.guild).button_message.set(message)
            await ctx.send(f"Custom button message set. Use `{ctx.prefix}buildset post` to update the message.")
        else:
            await self.config.guild(ctx.guild).button_message.set(None)
            await ctx.send(f"Button message reset to default. Use `{ctx.prefix}buildset post` to update the message.")

    @buildset.command(name="post")
    @commands.admin()
    @commands.guild_only()
    async def post(self, ctx: commands.Context):
        """Post the Request Building button."""
        request_channel_id = await self.config.guild(ctx.guild).request_channel_id()
        if not request_channel_id:
            await ctx.send("Set the request channel first with `[p]buildset requestchannel #channel`.")
            return
        ch = ctx.guild.get_channel(request_channel_id)
        if not ch:
            await ctx.send("The configured request channel was not found.")
            return
        
        custom_msg = await self.config.guild(ctx.guild).button_message()
        if custom_msg:
            description = custom_msg
        else:
            description = None
        
        emb = self._request_panel_embed(description)
        await ch.send(embed=emb, view=StartView(self))
        await ctx.tick()

    @buildset.command(name="browsercheck")
    @commands.admin()
    @commands.guild_only()
    async def browsercheck(self, ctx: commands.Context):
        """Check whether BuildingManager browser automation is ready."""
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
            await ctx.send(f"BuildingManager browser backend is not ready: {exc}")
            return

        await ctx.send("BuildingManager browser backend is ready.")

    @buildset.command(name="browserinspect")
    @commands.admin()
    @commands.guild_only()
    async def browserinspect(self, ctx: commands.Context, *, url: Optional[str] = None):
        """Inspect a MissionChief building page without submitting anything."""
        try:
            target_url = _normalize_missionchief_url(url)
        except ValueError as exc:
            await ctx.send(str(exc))
            return

        async with ctx.typing():
            try:
                report = await self._browser_diagnostics(target_url)
            except Exception as exc:
                await ctx.send(f"BuildingManager browser diagnostics failed: {exc}")
                return

        data = BytesIO(report.encode("utf-8"))
        data.seek(0)
        await ctx.send(
            "Safe BuildingManager diagnostics generated. No building was created.",
            file=discord.File(data, filename="buildingmanager-browser-diagnostics.txt"),
        )

    @buildset.command(name="automationstatus")
    @commands.admin()
    @commands.guild_only()
    async def automationstatus(self, ctx: commands.Context):
        """Show recent post-creation building automation jobs."""
        jobs = self.db.get_recent_automation_jobs(ctx.guild.id, limit=10)
        if not jobs:
            await ctx.send("No BuildingManager post-creation automation jobs are stored.")
            return

        lines = ["BuildingManager post-creation automation:"]
        for job in jobs:
            flags = []
            flags.append("tax" if job.tax_complete else "tax pending")
            flags.append("level" if job.level_complete else "level pending")
            flags.append("extensions" if job.extensions_complete else "extensions pending")
            lines.append(
                f"- Request {job.request_id} / Building {job.building_id}: {job.status} "
                f"({', '.join(flags)}; extensions started by bot: {job.extensions_started}; "
                f"next: {fmt_dt(job.next_run_at)})"
            )
        await ctx.send(box("\n".join(lines), lang="text"))

    @buildset.command(name="automationqueue")
    @commands.admin()
    @commands.guild_only()
    async def automationqueue(self, ctx: commands.Context, request_id: int, building_id: int):
        """Queue automation for an already-created MissionChief alliance building."""
        request = self.db.get_request_by_id(int(request_id))
        if not request:
            await ctx.send("No building request was found with that request id.")
            return
        if int(request["guild_id"]) != ctx.guild.id:
            await ctx.send("That building request belongs to a different guild.")
            return
        if request["building_type"] not in ALLIANCE_BUILDING_TYPE_IDS:
            allowed = ", ".join(ALLIANCE_BUILDING_TYPE_IDS)
            await ctx.send(f"Post-creation automation only supports: {allowed}.")
            return

        job_id = self.db.add_or_update_automation_job(
            request_id=int(request["request_id"]),
            guild_id=ctx.guild.id,
            building_id=int(building_id),
            building_type=str(request["building_type"]),
            building_name=str(request["building_name"]),
        )
        self.db.update_request_status(int(request["request_id"]), "created")
        self.db.add_action(
            request_id=int(request["request_id"]),
            guild_id=ctx.guild.id,
            admin_user_id=ctx.author.id,
            admin_username=str(ctx.author),
            action_type="automation_queued",
            previous_values=f"MissionChief building {int(building_id)}",
        )

        async with ctx.typing():
            result = await self._process_building_automation_job(job_id)

        if result is None:
            await ctx.send("Automation was queued, but no runnable job was found.")
            return

        action_text = "\n".join(f"- {action}" for action in result.actions) if result.actions else "- No actions were started."
        status = "completed" if result.completed else "waiting" if result.wait else "queued"
        await ctx.send(
            box(
                "\n".join(
                    [
                        f"Queued automation for request {int(request['request_id'])} / building {int(building_id)}",
                        f"Result: {'OK' if result.ok else 'FAILED'} ({status})",
                        f"Reason: {result.reason}",
                        "Actions:",
                        action_text,
                    ]
                ),
                lang="text",
            )
        )

    @buildset.command(name="automationrun")
    @commands.admin()
    @commands.guild_only()
    async def automationrun(self, ctx: commands.Context, request_or_building_id: int):
        """Run post-creation automation now for a request id or MissionChief building id."""
        job = self.db.get_automation_job_by_request_or_building(int(request_or_building_id))
        if not job:
            await ctx.send("No automation job was found for that request/building id.")
            return
        if job.guild_id != ctx.guild.id:
            await ctx.send("That automation job belongs to a different guild.")
            return

        async with ctx.typing():
            result = await self._process_building_automation_job(job.job_id)
        if result is None:
            await ctx.send("That automation job is already completed or no longer exists.")
            return

        action_text = "\n".join(f"- {action}" for action in result.actions) if result.actions else "- No actions were started."
        status = "completed" if result.completed else "waiting" if result.wait else "queued"
        await ctx.send(
            box(
                "\n".join(
                    [
                        f"Automation run for request {job.request_id} / building {job.building_id}",
                        f"Result: {'OK' if result.ok else 'FAILED'} ({status})",
                        f"Reason: {result.reason}",
                        "Actions:",
                        action_text,
                    ]
                ),
                lang="text",
            )
        )

    @commands.hybrid_group(name="buildstats", invoke_without_command=True)
    @commands.guild_only()
    async def buildstats(self, ctx: commands.Context):
        """View building request statistics."""
        stats = self.db.get_stats_overall(ctx.guild.id)
        
        status_counts = stats["status_counts"]
        total = sum(status_counts.values())
        
        if total == 0:
            await ctx.send("No building requests have been made yet.")
            return
        
        approved = status_counts.get("approved", 0)
        denied = status_counts.get("denied", 0)
        cancelled = status_counts.get("cancelled", 0)
        pending = status_counts.get("pending", 0)
        
        embed = discord.Embed(
            title="📊 Building Request Statistics",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        
        summary = (
            f"**Total Requests**: {total}\n"
            f"├─ ✅ Approved: {approved} ({approved*100//total if total else 0}%)\n"
            f"├─ ❌ Denied: {denied} ({denied*100//total if total else 0}%)\n"
            f"├─ ⏳ Pending: {pending} ({pending*100//total if total else 0}%)\n"
            f"└─ 🚫 Cancelled: {cancelled} ({cancelled*100//total if total else 0}%)\n"
        )
        embed.add_field(name="Overview", value=summary, inline=False)
        
        type_stats = stats["type_stats"]
        type_summary = {}
        for building_type, status, count in type_stats:
            if building_type not in type_summary:
                type_summary[building_type] = {"approved": 0, "denied": 0, "total": 0}
            type_summary[building_type][status] = count
            type_summary[building_type]["total"] += count
        
        if type_summary:
            type_text = ""
            emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
            for building_type, counts in type_summary.items():
                emoji = emoji_map.get(building_type, "🏢")
                approved_count = counts.get("approved", 0)
                total_count = counts["total"]
                type_text += f"{emoji} **{building_type}**: {total_count} requests ({approved_count} approved)\n"
            embed.add_field(name="By Building Type", value=type_text, inline=False)
        
        top_requesters = stats["top_requesters"]
        if top_requesters:
            requester_text = "\n".join([f"{i+1}. {username} - {count} requests" 
                                       for i, (username, count) in enumerate(top_requesters[:5])])
            embed.add_field(name="Top Requesters", value=requester_text, inline=True)
        
        top_admins = stats["top_admins"]
        if top_admins:
            admin_text = "\n".join([f"{i+1}. {username} - {count} actions" 
                                   for i, (username, count) in enumerate(top_admins[:5])])
            embed.add_field(name="Most Active Admins", value=admin_text, inline=True)
        
        avg_time = stats["avg_response_time"]
        if avg_time:
            hours = int(avg_time // 3600)
            minutes = int((avg_time % 3600) // 60)
            embed.add_field(name="Average Response Time", value=f"{hours}h {minutes}m", inline=False)
        
        await ctx.send(embed=embed)

    @buildstats.command(name="user")
    @commands.guild_only()
    async def buildstats_user(self, ctx: commands.Context, user: discord.Member = None):
        """View statistics for a specific user."""
        if user is None:
            user = ctx.author
        
        stats = self.db.get_stats_user(ctx.guild.id, user.id)
        
        status_counts = stats["status_counts"]
        total = sum(status_counts.values())
        
        if total == 0:
            await ctx.send(f"{user.mention} has not made any building requests yet.")
            return
        
        approved = status_counts.get("approved", 0)
        denied = status_counts.get("denied", 0)
        cancelled = status_counts.get("cancelled", 0)
        pending = status_counts.get("pending", 0)
        
        embed = discord.Embed(
            title=f"📊 Building Statistics for {user.display_name}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_thumbnail(url=user.display_avatar.url)
        
        summary = (
            f"**Total Requests**: {total}\n"
            f"├─ ✅ Approved: {approved} ({approved*100//total if total else 0}%)\n"
            f"├─ ❌ Denied: {denied} ({denied*100//total if total else 0}%)\n"
            f"├─ ⏳ Pending: {pending}\n"
            f"└─ 🚫 Cancelled: {cancelled}\n"
        )
        embed.add_field(name="Overview", value=summary, inline=False)
        
        type_stats = stats["type_stats"]
        type_summary = {}
        for building_type, status, count in type_stats:
            if building_type not in type_summary:
                type_summary[building_type] = {"approved": 0, "denied": 0}
            type_summary[building_type][status] = count
        
        if type_summary:
            type_text = ""
            emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
            for building_type, counts in type_summary.items():
                emoji = emoji_map.get(building_type, "🏢")
                approved_count = counts.get("approved", 0)
                denied_count = counts.get("denied", 0)
                type_text += f"{emoji} **{building_type}**: {approved_count} approved, {denied_count} denied\n"
            embed.add_field(name="By Building Type", value=type_text, inline=False)
        
        denial_reasons = stats["denial_reasons"]
        if denial_reasons:
            denial_text = "\n".join([f"└─ {reason}: {count}" for reason, count in denial_reasons[:5]])
            embed.add_field(name="Denial Reasons", value=denial_text, inline=False)
        
        recent_requests = stats["recent_requests"]
        if recent_requests:
            recent_text = ""
            status_emoji = {"approved": "✅", "denied": "❌", "pending": "⏳", "cancelled": "🚫"}
            for building_type, building_name, status, created_at in recent_requests[:5]:
                emoji = status_emoji.get(status, "")
                time_ago = fmt_dt(created_at)
                recent_text += f"{emoji} {building_type} - {building_name[:30]} ({time_ago})\n"
            embed.add_field(name="Recent Requests (last 5)", value=recent_text, inline=False)
        
        await ctx.send(embed=embed)

    @buildstats.command(name="admin")
    @commands.guild_only()
    async def buildstats_admin(self, ctx: commands.Context, admin: discord.Member = None):
        """View statistics for a specific admin."""
        if admin is None:
            admin = ctx.author
        
        stats = self.db.get_stats_admin(ctx.guild.id, admin.id)
        
        action_counts = stats["action_counts"]
        total = sum(action_counts.values())
        
        if total == 0:
            await ctx.send(f"{admin.mention} has not taken any admin actions yet.")
            return
        
        approved = action_counts.get("approved", 0)
        denied = action_counts.get("denied", 0)
        
        embed = discord.Embed(
            title=f"📊 Admin Statistics for {admin.display_name}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_thumbnail(url=admin.display_avatar.url)
        
        summary = (
            f"**Total Actions**: {total}\n"
            f"├─ ✅ Approvals: {approved} ({approved*100//total if total else 0}%)\n"
            f"└─ ❌ Denials: {denied} ({denied*100//total if total else 0}%)\n"
        )
        embed.add_field(name="Overview", value=summary, inline=False)
        
        type_stats = stats["type_stats"]
        type_summary = {}
        for building_type, action_type, count in type_stats:
            if building_type not in type_summary:
                type_summary[building_type] = {"approved": 0, "denied": 0}
            type_summary[building_type][action_type] = count
        
        if type_summary:
            type_text = ""
            emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
            for building_type, counts in type_summary.items():
                emoji = emoji_map.get(building_type, "🏢")
                approved_count = counts.get("approved", 0)
                denied_count = counts.get("denied", 0)
                type_text += f"{emoji} **{building_type}**: {approved_count} approved, {denied_count} denied\n"
            embed.add_field(name="By Building Type", value=type_text, inline=False)
        
        denial_breakdown = stats["denial_breakdown"]
        if denial_breakdown:
            total_denials = sum([count for reason, count in denial_breakdown])
            denial_text = ""
            for reason, count in denial_breakdown[:5]:
                percentage = (count * 100 // total_denials) if total_denials else 0
                denial_text += f"├─ {reason}: {count} ({percentage}%)\n"
            embed.add_field(name="Denial Breakdown", value=denial_text, inline=False)
        
        response_times = stats["response_times"]
        avg_time, min_time, max_time = response_times
        if avg_time:
            avg_hours = int(avg_time // 3600)
            avg_minutes = int((avg_time % 3600) // 60)
            min_minutes = int(min_time // 60)
            max_hours = int(max_time // 3600)
            max_minutes = int((max_time % 3600) // 60)
            
            time_text = (
                f"Average: {avg_hours}h {avg_minutes}m\n"
                f"Fastest: {min_minutes} minutes\n"
                f"Slowest: {max_hours}h {max_minutes}m"
            )
            embed.add_field(name="Response Times", value=time_text, inline=False)
        
        recent_actions = stats["recent_actions"]
        if recent_actions:
            recent_text = ""
            action_emoji = {"approved": "✅", "denied": "❌"}
            for action_type, building_type, username, timestamp in recent_actions[:5]:
                emoji = action_emoji.get(action_type, "")
                time_ago = fmt_dt(timestamp)
                recent_text += f"{emoji} {action_type.capitalize()} {building_type} by {username} ({time_ago})\n"
            embed.add_field(name="Recent Actions (last 5)", value=recent_text, inline=False)
        
        await ctx.send(embed=embed)

    @buildstats.command(name="type")
    @commands.guild_only()
    async def buildstats_type(self, ctx: commands.Context, building_type: str):
        """View statistics for a specific building type."""
        building_type = building_type.capitalize()
        
        stats = self.db.get_stats_type(ctx.guild.id, building_type)
        
        status_counts = stats["status_counts"]
        total = sum(status_counts.values())
        
        if total == 0:
            await ctx.send(f"No requests found for building type: {building_type}")
            return
        
        approved = status_counts.get("approved", 0)
        denied = status_counts.get("denied", 0)
        cancelled = status_counts.get("cancelled", 0)
        pending = status_counts.get("pending", 0)
        
        emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
        emoji = emoji_map.get(building_type, "🏢")
        
        embed = discord.Embed(
            title=f"📊 Statistics for {emoji} {building_type}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        
        summary = (
            f"**Total Requests**: {total}\n"
            f"├─ ✅ Approved: {approved} ({approved*100//total if total else 0}%)\n"
            f"├─ ❌ Denied: {denied} ({denied*100//total if total else 0}%)\n"
            f"├─ ⏳ Pending: {pending}\n"
            f"└─ 🚫 Cancelled: {cancelled}\n"
        )
        embed.add_field(name="Overview", value=summary, inline=False)
        
        top_requesters = stats["top_requesters"]
        if top_requesters:
            requester_text = "\n".join([f"{i+1}. {username} - {count} requests" 
                                       for i, (username, count) in enumerate(top_requesters[:5])])
            embed.add_field(name="Top Requesters", value=requester_text, inline=False)
        
        common_denial = stats["common_denial"]
        if common_denial:
            reason, count = common_denial
            embed.add_field(name="Most Common Denial Reason", value=f"{reason} ({count} times)", inline=False)
        
        admin_rates = stats["admin_rates"]
        if admin_rates:
            admin_text = ""
            for admin_username, approved_count, total_count in admin_rates[:5]:
                rate = (approved_count * 100 // total_count) if total_count else 0
                admin_text += f"├─ {admin_username}: {rate}% ({approved_count}/{total_count})\n"
            embed.add_field(name="Approval Rate by Admin", value=admin_text, inline=False)
        
        await ctx.send(embed=embed)


async def setup(bot: Red):
    await bot.add_cog(BuildingManager(bot))
