from __future__ import annotations

import asyncio
import aiohttp
import contextlib
import difflib
import hashlib
import html as html_lib
import math
import os
import struct
import tempfile
from html.parser import HTMLParser
from io import BytesIO
import json
import logging
import re
import sqlite3
import time
import unicodedata
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse
from zoneinfo import ZoneInfo

import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box

log = logging.getLogger("red.cog.building_manager")

BASE_URL = "https://www.missionchief.com"
MISSIONCHIEF_HOME_URL = BASE_URL
MISSIONCHIEF_NEW_BUILDING_URL = f"{BASE_URL}/buildings/new"
MISSIONCHIEF_ALLIANCE_BUILDINGS_URL = f"{BASE_URL}/verband/gebauede"
MISSIONCHIEF_ALLIANCE_FUNDS_URL = f"{BASE_URL}/verband/kasse"
DEFAULT_REQUEST_PANEL_CHANNEL_ID = 1421627971831070730
BOARD_THREAD_ID = 6165
BOARD_POLL_SECONDS = 5 * 60
BOARD_GUIDE_SYNC_SECONDS = 60 * 60
BOARD_CLEANUP_SECONDS = 10 * 60
BOARD_POST_DELETE_AFTER_SECONDS = 12 * 60 * 60
BOARD_PROCESSED_POST_ID_LIMIT = 1000
BOARD_PENDING_DELETE_LIMIT = 1000
BOARD_GUIDE_MAX_SCAN_PAGES = 25
BOARD_GUIDE_MARKER = "[BM-GUIDE:overview]"
BOARD_REPLY_MARKER = "[BM-REPLY]"
BUILDING_BOARD_MIN_AUTO_ACCEPT_TAX = 5.0
GEOCODE_MAPS_SEARCH_URL = "https://geocode.maps.co/search"
GEOCODE_MAPS_TIMEOUT_SECONDS = 8
ALLIANCE_BUILDING_TYPE_IDS = {
    "Hospital": "2",
    "Prison": "10",
}
ALLIANCE_BUILDING_TARGET_TAX = 20
ALLIANCE_BUILDING_TARGET_HOSPITAL_LEVEL = 20
ALLIANCE_BUILDING_MIN_FUNDS = 2_000_000
MISSIONCHIEF_BUILDING_NAME_LIMIT = 40
BUILDING_APPROVAL_FUNDS_TIMEOUT_SECONDS = 45
BUILDING_APPROVAL_CREATE_TIMEOUT_SECONDS = 180
BUILDING_APPROVAL_RECOVERY_TIMEOUT_SECONDS = 75
BUILDING_LOOKUP_MAX_ALLIANCE_LIST_PAGES = 8
BUILDING_AUTOMATION_RETRY_SECONDS = 6 * 60 * 60
BUILDING_AUTOMATION_LOOP_SECONDS = 15 * 60
BUILDING_CREATION_QUEUE_LOOP_SECONDS = 15 * 60
BUILDING_AUTOMATION_MAX_ACTIONS_PER_RUN = 30
BUILDING_AUTOMATION_MAX_EXTENSION_STARTS_PER_RUN = BUILDING_AUTOMATION_MAX_ACTIONS_PER_RUN
BUILDING_AUTOMATION_MAX_SCRIPT_STEPS_PER_RUN = BUILDING_AUTOMATION_MAX_EXTENSION_STARTS_PER_RUN + 10
BUILDING_AUTOMATION_EXCLUDED_EXTENSIONS = {
    "large hospital",
    "large prison",
}
AUTO_CANDIDATE_MIN_FUNDS = 5_000_000
AUTO_CANDIDATE_LOOP_SECONDS = 15 * 60
AUTO_CANDIDATE_DEFAULT_TIME = "07:00"
AUTO_CANDIDATE_DEFAULT_TIMEZONE = "America/New_York"
AUTO_CANDIDATE_DUPLICATE_RADIUS_METERS = 250
AUTO_CANDIDATE_SELECTION_POOL = 50
AUTO_CANDIDATE_REFILL_MIN_AVAILABLE = 10
AUTO_CANDIDATE_REFILL_REGIONS_PER_RUN = 2
AUTO_CANDIDATE_REFILL_TIMEOUT_SECONDS = 300
AUTO_CANDIDATE_REFILL_MAX_EXTRACT_BYTES = 350 * 1024 * 1024
GEOFABRIK_INDEX_URL = "https://download.geofabrik.de/index-v1.json"
OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_IMPORT_AREA_WARNING_DEGREES = 0.35
PLAYWRIGHT_SETUP_MESSAGE = (
    "Playwright browser automation is not ready. Install the BuildingManager requirements and run "
    "`python -m playwright install chromium` in the same Python environment as Redbot."
)
REQUEST_PANEL_TITLE = "🏢 Building Request System"

BOARD_BUILDING_TYPE_ALIASES = {
    "Hospital": (
        "hospital",
        "hospitaal",
        "medical center",
        "medical centre",
        "health center",
        "health centre",
        "healthcare",
        "clinic",
        "ziekenhuis",
        "ziekenhuiz",
    ),
    "Prison": (
        "prison",
        "prision",
        "jail",
        "detention",
        "correctional",
        "correctional facility",
        "gevangenis",
        "gevangeniss",
    ),
}

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
  const safeDecode = (value) => {
    let text = String(value || "");
    for (let i = 0; i < 4; i += 1) {
      try {
        const decoded = decodeURIComponent(text);
        if (decoded === text) break;
        text = decoded;
      } catch (_) {
        break;
      }
    }
    return text.replace(/\+/g, " ");
  };
  const normalized = (value) => safeDecode(value).replace(/\s+/g, " ").trim().toLowerCase();
  const targetName = normalized(config.targetName || "");
  const startPath = "/verband/gebauede";
  const seenPages = new Set();
  const seenIds = new Set();
  const candidates = [];
  const textOf = (element) => safeDecode(element?.textContent || "").replace(/\s+/g, " ").trim();
  const candidateText = (candidate) => [
    candidate.text,
    candidate.rowText,
    candidate.searchAttribute,
    ...(candidate.imageSources || []),
  ].map((value) => normalized(value)).join(" ");
  const hasTargetCandidate = () => targetName && candidates.some((candidate) => candidateText(candidate).includes(targetName));
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
      searchAttribute: safeDecode(row?.getAttribute?.("search_attribute") || ""),
      imageSources: imageSources.map(safeDecode),
    });
  };
  const parsePage = (html, pagePath) => {
    const doc = new DOMParser().parseFromString(html, "text/html");
    const beforeCount = candidates.length;
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
        const aria = String(link.getAttribute("aria-label") || "").toLowerCase();
        const href = absolutePath(link.getAttribute("href") || "");
        return (href.startsWith("/verband/gebauede") || href.startsWith("?") || href.includes("page="))
          && (label.includes("next") || label.includes(">") || className.includes("next") || rel.includes("next") || aria.includes("next"));
      });
    return {
      nextPath: next ? absolutePath(next.getAttribute("href")) : "",
      foundCandidates: candidates.length > beforeCount,
    };
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
    const parsed = parsePage(html, pagePath);
    if (hasTargetCandidate()) break;
    if (parsed.nextPath) {
      pagePath = parsed.nextPath;
    } else if (parsed.foundCandidates) {
      pagePath = `${startPath}?page=${pageIndex + 2}`;
    } else {
      pagePath = "";
    }
  }
  return { ok: true, status: lastStatus, pages: [...seenPages], candidates };
}
""".strip()

BUILDING_FETCH_ALLIANCE_LOGS_SCRIPT = r"""
async () => {
  const safeDecode = (value) => {
    let text = String(value || "");
    for (let i = 0; i < 4; i += 1) {
      try {
        const decoded = decodeURIComponent(text);
        if (decoded === text) break;
        text = decoded;
      } catch (_) {
        break;
      }
    }
    return text.replace(/\+/g, " ");
  };
  const textOf = (element) => safeDecode(element?.textContent || "").replace(/\s+/g, " ").trim();
  const response = await fetch("/alliance_logfiles", {
    credentials: "same-origin",
    headers: { "Accept": "text/html" },
  });
  const status = response.status;
  const html = await response.text();
  if (!response.ok) {
    return { ok: false, status, candidates: [], text: String(html || "").slice(0, 500) };
  }
  const doc = new DOMParser().parseFromString(html, "text/html");
  const candidates = [];
  for (const row of doc.querySelectorAll("tr")) {
    const links = [...row.querySelectorAll('a[href*="/buildings/"]')];
    if (!links.length) continue;
    const link = links[links.length - 1];
    const href = link.getAttribute("href") || "";
    const match = href.match(/\/buildings\/(\d+)/);
    if (!match) continue;
    const cells = [...row.querySelectorAll("td")].map((cell) => textOf(cell));
    candidates.push({
      id: Number(match[1]),
      href,
      affectedName: textOf(link),
      rowText: textOf(row),
      cells,
    });
  }
  return { ok: true, status, candidates: candidates.slice(0, 40) };
}
""".strip()

BUILDING_AUTOMATION_PREPARE_SCRIPT = r"""
(config) => {
  const targetTax = String(config.targetTax || "20");
  const maxExtensionStarts = Number(config.maxExtensionStarts || 30);
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

BUILDING_AUTOMATION_DIRECT_SCRIPT = r"""
async (config) => {
  const buildingId = String(config.buildingId || "").trim();
  const buildingType = String(config.buildingType || "").trim().toLowerCase();
  const targetTax = Number(config.targetTax || 20);
  const maxHospitalLevel = Number(config.maxHospitalLevel || 20);
  const maxExtensionStarts = Number(config.maxExtensionStarts || 30);
  const extensionsStartedThisRun = Number(config.extensionsStartedThisRun || 0);
  const targetTaxIds = { 0: 0, 10: 1, 20: 2, 30: 3, 40: 4, 50: 5 };
  const targetTaxId = targetTaxIds[targetTax];

  const normalize = (value) => String(value || "").replace(/\s+/g, " ").trim().toLowerCase();
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const csrf = () => document.querySelector('meta[name="csrf-token"]')?.content || "";

  const fail = (reason, extra = {}) => ({
    ok: false,
    action: null,
    completed: false,
    wait: true,
    reason,
    snapshot: { url: location.href, buildingId, buildingType, ...extra },
  });

  if (!buildingId || !/^\d+$/.test(buildingId)) {
    return fail("MissionChief building id is missing or invalid.");
  }
  if (!["hospital", "prison"].includes(buildingType)) {
    return fail(`Unsupported alliance building automation type: ${buildingType || "unknown"}.`);
  }
  if (targetTaxId === undefined) {
    return fail("Invalid target tax percentage. Supported values are 0, 10, 20, 30, 40, and 50.");
  }

  async function fetchText(path) {
    const response = await fetch(path, {
      credentials: "same-origin",
      headers: { "Accept": "text/html" },
    });
    const text = await response.text();
    return { ok: response.ok, status: response.status, url: response.url, text };
  }

  async function doGet(path) {
    const response = await fetch(path, {
      credentials: "same-origin",
      headers: { "Accept": "text/html", "X-Requested-With": "XMLHttpRequest" },
    });
    const text = await response.text();
    return { ok: response.ok, status: response.status, url: response.url, text };
  }

  async function doPost(path) {
    const token = csrf();
    const headers = {
      "Accept": "text/html",
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      "X-Requested-With": "XMLHttpRequest",
    };
    if (token) headers["X-CSRF-Token"] = token;

    const body = new URLSearchParams();
    if (token) body.set("authenticity_token", token);

    const response = await fetch(path, {
      method: "POST",
      credentials: "same-origin",
      headers,
      body,
    });
    const text = await response.text();
    return { ok: response.ok, status: response.status, url: response.url, text };
  }

  function parseCredits(text) {
    const match = String(text || "").match(/([\d.,\s]+)/);
    if (!match) return null;
    const digits = match[1].replace(/[^\d]/g, "");
    return digits ? Number.parseInt(digits, 10) : null;
  }

  function parseDoc(html) {
    return new DOMParser().parseFromString(String(html || ""), "text/html");
  }

  function extractCurrentLevel(html) {
    const match = String(html || "").match(/<dt><strong>Level:<\/strong><\/dt>\s*<dd>\s*(\d+)/i);
    return match ? Number.parseInt(match[1], 10) : null;
  }

  function isTaxAlreadyTarget(html) {
    const doc = parseDoc(html);
    const selector = `a.btn.btn-alliance_costs[href="/buildings/${buildingId}/alliance_costs/${targetTaxId}"]`;
    const link = doc.querySelector(selector);
    if (!link) return false;
    return normalize(link.textContent).includes(`${targetTax}%`) && normalize(link.className).includes("btn-success");
  }

  function extractExtensionOffers(html) {
    const doc = parseDoc(html);
    const links = [...doc.querySelectorAll(`a[href^="/buildings/${buildingId}/extension/credits/"]`)];
    const offers = [];
    for (const link of links) {
      const href = link.getAttribute("href") || "";
      const match = href.match(new RegExp(`^/buildings/${buildingId}/extension/credits/(\\d+)`));
      if (!match) continue;
      const extId = Number.parseInt(match[1], 10);
      offers.push({
        extId,
        price: parseCredits(link.textContent || ""),
        href,
        label: (link.textContent || href).replace(/\s+/g, " ").trim(),
      });
    }
    const unique = new Map();
    for (const offer of offers) {
      if (!unique.has(offer.extId)) unique.set(offer.extId, offer);
    }
    return [...unique.values()].sort((a, b) => a.extId - b.extId);
  }

  const detailPath = `/buildings/${buildingId}`;
  const detail = await fetchText(detailPath);
  if (!detail.ok) {
    return fail(`MissionChief returned HTTP ${detail.status} for building ${buildingId}.`, {
      status: detail.status,
      response: String(detail.text || "").slice(0, 500),
    });
  }

  if (!config.taxComplete) {
    if (isTaxAlreadyTarget(detail.text)) {
      return {
        ok: true,
        action: "tax_already_set",
        completed: false,
        label: `Tax already set to ${targetTax}%`,
        status: detail.status,
        snapshot: { url: detail.url || detailPath },
      };
    }

    const taxResult = await doGet(`/buildings/${buildingId}/alliance_costs/${targetTaxId}`);
    if (!taxResult.ok) {
      return fail(`MissionChief returned HTTP ${taxResult.status} while setting tax to ${targetTax}%.`, {
        status: taxResult.status,
        response: String(taxResult.text || "").slice(0, 500),
      });
    }
    await sleep(500);
    return {
      ok: true,
      action: "set_tax",
      completed: false,
      label: `Set tax to ${targetTax}%`,
      status: taxResult.status,
      snapshot: { url: taxResult.url || detailPath },
    };
  }

  if (buildingType === "prison" && !config.levelComplete) {
    return {
      ok: true,
      action: "level_not_applicable",
      completed: false,
      label: "Prison level not applicable",
      status: detail.status,
      snapshot: { url: detail.url || detailPath },
    };
  }

  if (buildingType === "hospital" && !config.levelComplete) {
    const currentLevel = extractCurrentLevel(detail.text);
    if (currentLevel !== null && currentLevel >= maxHospitalLevel) {
      return {
        ok: true,
        action: "level_already_max",
        completed: false,
        label: `Hospital level already ${maxHospitalLevel}`,
        status: detail.status,
        snapshot: { url: detail.url || detailPath, currentLevel },
      };
    }
    if (currentLevel === null) {
      return {
        ok: true,
        action: null,
        completed: false,
        wait: true,
        reason: "Hospital level could not be read from the MissionChief building page.",
        status: detail.status,
        snapshot: { url: detail.url || detailPath },
      };
    }

    const levelTarget = Math.max(0, maxHospitalLevel - 1);
    const levelResult = await doGet(`/buildings/${buildingId}/expand_do/credits?level=${levelTarget}`);
    if (!levelResult.ok) {
      return fail(`MissionChief returned HTTP ${levelResult.status} while setting hospital level.`, {
        status: levelResult.status,
        response: String(levelResult.text || "").slice(0, 500),
      });
    }
    await sleep(500);
    return {
      ok: true,
      action: "start_level_upgrade",
      completed: false,
      label: `Set hospital level to ${maxHospitalLevel}`,
      status: levelResult.status,
      snapshot: { url: levelResult.url || detailPath, previousLevel: currentLevel },
    };
  }

  let offers = extractExtensionOffers(detail.text);
  if (buildingType === "hospital") {
    offers = offers.filter((offer) => offer.extId !== 9);
  }
  if (buildingType === "prison") {
    offers = offers.filter((offer) => offer.extId !== 30 && offer.price !== 200000);
  }

  if (!offers.length) {
    return {
      ok: true,
      action: null,
      completed: true,
      wait: false,
      reason: "Tax, level, and eligible extensions are complete.",
      status: detail.status,
      snapshot: { url: detail.url || detailPath },
    };
  }

  if (extensionsStartedThisRun >= maxExtensionStarts) {
    return {
      ok: true,
      action: null,
      completed: false,
      wait: true,
      reason: `Started ${extensionsStartedThisRun} extension(s) this run; waiting before starting more.`,
      status: detail.status,
      snapshot: { url: detail.url || detailPath, remainingExtensions: offers.map((offer) => offer.extId).slice(0, 10) },
    };
  }

  const next = offers[0];
  const extensionResult = await doPost(next.href);
  if (!extensionResult.ok) {
    return fail(`MissionChief returned HTTP ${extensionResult.status} while starting extension ${next.extId}.`, {
      status: extensionResult.status,
      extensionId: next.extId,
      response: String(extensionResult.text || "").slice(0, 500),
    });
  }
  await sleep(500);
  return {
    ok: true,
    action: "start_extension",
    completed: false,
    label: `Started extension ${next.extId}${next.price ? ` (${next.price} credits)` : ""}`,
    status: extensionResult.status,
    snapshot: { url: extensionResult.url || detailPath, extensionId: next.extId, price: next.price },
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

def _truncate_discord_text(value: Any, limit: int = 1024) -> str:
    """Return text that fits within Discord content or embed field limits."""
    text = str(value or "").strip()
    if len(text) <= limit:
        return text or "N/A"
    if limit <= 3:
        return text[:limit]
    return f"{text[: limit - 3]}..."

async def safe_ephemeral_complete(interaction: discord.Interaction, content: str):
    """Finish an interaction, including a deferred thinking response."""
    message = _truncate_discord_text(content, 1900)
    try:
        if interaction.response.is_done():
            try:
                await interaction.edit_original_response(content=message, embed=None, view=None)
                return
            except Exception as exc:
                log.debug("safe_ephemeral_complete: edit_original_response failed: %r", exc)
            await interaction.followup.send(message, ephemeral=True)
            return
        await interaction.response.send_message(message, ephemeral=True)
    except Exception as exc:
        log.exception("safe_ephemeral_complete failed: %r", exc)

async def send_ephemeral_followup(interaction: discord.Interaction, content: str):
    """Send a private result without leaving a deferred thinking response behind."""
    message = _truncate_discord_text(content, 1900)
    try:
        if interaction.response.is_done():
            try:
                await interaction.edit_original_response(content=message, embed=None, view=None)
                return
            except Exception as exc:
                log.debug("send_ephemeral_followup: edit_original_response failed: %r", exc)
            await interaction.followup.send(message, ephemeral=True)
            return
        await interaction.response.send_message(message, ephemeral=True)
    except Exception as exc:
        log.exception("send_ephemeral_followup failed: %r", exc)

def _decode_url_text(value: Any) -> str:
    """Decode URL-encoded text, including values encoded more than once."""
    text = str(value or "")
    for _ in range(4):
        decoded = unquote(text)
        if decoded == text:
            break
        text = decoded
    return text.replace("+", " ").strip()

def _clean_building_name(value: Any) -> str:
    """Normalize a building name before storing or submitting it."""
    text = _decode_url_text(value)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def _missionchief_building_name(value: Any) -> str:
    """Return a building name that fits MissionChief's 40-character limit."""
    return _clean_building_name(value)[:MISSIONCHIEF_BUILDING_NAME_LIMIT].strip()

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
    name = _missionchief_building_name(building_name)
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

def _haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return approximate distance between two coordinates in meters."""
    radius_m = 6_371_000
    phi1 = math.radians(float(lat1))
    phi2 = math.radians(float(lat2))
    delta_phi = math.radians(float(lat2) - float(lat1))
    delta_lambda = math.radians(float(lon2) - float(lon1))
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return radius_m * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def _osm_address_from_tags(tags: Dict[str, Any]) -> Optional[str]:
    """Build a compact address from OSM addr:* tags."""
    street = " ".join(
        part
        for part in (
            tags.get("addr:housenumber"),
            tags.get("addr:street"),
        )
        if part
    ).strip()
    parts = [
        street,
        tags.get("addr:postcode"),
        tags.get("addr:city") or tags.get("addr:town") or tags.get("addr:village"),
        tags.get("addr:state") or tags.get("addr:province"),
        tags.get("addr:country"),
    ]
    text = ", ".join(str(part).strip() for part in parts if str(part or "").strip())
    return text or None

def _osm_candidate_type_from_tags(tags: Dict[str, Any]) -> Optional[str]:
    """Return supported BuildingManager type from OSM tags."""
    amenity = str(tags.get("amenity") or "").casefold()
    healthcare = str(tags.get("healthcare") or "").casefold()
    if amenity == "prison":
        return "Prison"
    if amenity == "hospital" or healthcare == "hospital":
        return "Hospital"
    return None

def _osm_location_details(
    *,
    element: Dict[str, Any],
    tags: Dict[str, Any],
    name: str,
    lat: float,
    lon: float,
) -> LocationDetails:
    """Build LocationDetails used by the existing facility validator."""
    source_id = f"{element.get('type')}/{element.get('id')}"
    address = _osm_address_from_tags(tags)
    facility_type = " ".join(
        str(tags.get(key) or "")
        for key in (
            "amenity",
            "healthcare",
            "building",
            "operator:type",
            "disused:amenity",
            "historic",
            "tourism",
        )
    )
    return LocationDetails(
        original_input=f"osm:{source_id}",
        resolved_input=f"osm:{source_id} {json.dumps(tags, ensure_ascii=False, sort_keys=True)}",
        place_name=name,
        coordinates=f"{float(lat):.7f}, {float(lon):.7f}",
        address=address,
        country=tags.get("addr:country") or tags.get("is_in:country"),
        region=tags.get("addr:state") or tags.get("addr:province") or tags.get("is_in:state"),
        maps_url=f"https://www.openstreetmap.org/{source_id}",
        provider="openstreetmap",
        detected_facility_type=facility_type,
    )

def _overpass_element_to_candidate_record(element: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Convert one Overpass element into a database candidate record."""
    if not isinstance(element, dict):
        return None, "Element is not an object."
    tags = element.get("tags") or {}
    if not isinstance(tags, dict):
        return None, "Element has no tags."

    building_type = _osm_candidate_type_from_tags(tags)
    if not building_type:
        return None, "Element is not a supported hospital/prison tag."

    name = _clean_building_name(
        tags.get("name")
        or tags.get("official_name")
        or tags.get("operator")
        or tags.get("brand")
        or ""
    )
    if not name:
        return None, "Element has no usable name."

    lat = element.get("lat")
    lon = element.get("lon")
    if (lat is None or lon is None) and isinstance(element.get("center"), dict):
        lat = element["center"].get("lat")
        lon = element["center"].get("lon")
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return None, "Element has no usable coordinates."
    if not -90 <= lat <= 90 or not -180 <= lon <= 180:
        return None, "Element coordinates are out of range."

    details = _osm_location_details(element=element, tags=tags, name=name, lat=lat, lon=lon)
    name_address_text = _normalize_facility_text(" ".join(part for part in (name, details.address) if part))
    if (
        building_type == "Hospital"
        and LocationParser._contains_facility_term(name_address_text, LocationParser._hospital_reject_terms)
        and not LocationParser._contains_facility_term(name_address_text, LocationParser._hospital_positive_terms)
    ):
        return None, "Element looks like a clinic, doctor office, pharmacy, or other non-hospital facility."
    if (
        building_type == "Prison"
        and LocationParser._contains_facility_term(name_address_text, LocationParser._prison_reject_terms)
        and not LocationParser._contains_facility_term(name_address_text, LocationParser._prison_positive_terms)
    ):
        return None, "Element looks like a courthouse, police station, or other non-prison facility."
    detected_type, reason = LocationParser.detect_supported_building_type(details, name)
    if detected_type != building_type:
        return None, reason

    source_id = f"{element.get('type')}/{element.get('id')}"
    return {
        "source": "openstreetmap",
        "source_id": source_id,
        "building_type": building_type,
        "name": name,
        "lat": lat,
        "lon": lon,
        "address": details.address,
        "country": details.country,
        "region": details.region,
        "raw_tags_json": json.dumps(tags, ensure_ascii=False, sort_keys=True),
    }, None

def parse_overpass_auto_build_candidates(data: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Parse Overpass JSON into clean automatic building candidates."""
    candidates: List[Dict[str, Any]] = []
    stats = {
        "source_elements": 0,
        "accepted": 0,
        "rejected": 0,
    }
    for element in data.get("elements") or []:
        stats["source_elements"] += 1
        record, _reason = _overpass_element_to_candidate_record(element)
        if record:
            candidates.append(record)
            stats["accepted"] += 1
        else:
            stats["rejected"] += 1
    return candidates, stats

def build_overpass_candidate_query(
    south: float,
    west: float,
    north: float,
    east: float,
    building_type: str = "both",
) -> str:
    """Build an Overpass query for hospitals and prisons in a bounding box."""
    if south >= north:
        raise ValueError("South latitude must be lower than north latitude.")
    if west >= east:
        raise ValueError("West longitude must be lower than east longitude.")
    for latitude in (south, north):
        if not -90 <= latitude <= 90:
            raise ValueError("Latitude must be between -90 and 90.")
    for longitude in (west, east):
        if not -180 <= longitude <= 180:
            raise ValueError("Longitude must be between -180 and 180.")
    bbox = f"{south:.7f},{west:.7f},{north:.7f},{east:.7f}"
    normalized = str(building_type or "both").casefold().strip()
    clauses = []
    if normalized in {"both", "all", "hospital", "hospitals"}:
        clauses.extend(
            [
                f'  nwr["amenity"="hospital"]({bbox});',
                f'  nwr["healthcare"="hospital"]({bbox});',
            ]
        )
    if normalized in {"both", "all", "prison", "prisons", "jail", "jails"}:
        clauses.append(f'  nwr["amenity"="prison"]({bbox});')
    if not clauses:
        raise ValueError("Building type must be `hospital`, `prison`, or `both`.")
    return "\n".join(
        [
            "[out:json][timeout:180];",
            "(",
            *clauses,
            ");",
            "out center tags;",
        ]
    )

def overpass_import_area_notice(south: float, west: float, north: float, east: float) -> Optional[str]:
    """Return a warning when a bounding box is likely too large for public Overpass."""
    height = abs(float(north) - float(south))
    width = abs(float(east) - float(west))
    if height > OVERPASS_IMPORT_AREA_WARNING_DEGREES or width > OVERPASS_IMPORT_AREA_WARNING_DEGREES:
        return (
            "This area is fairly large for the public Overpass server. If it returns HTTP 504, "
            "split the import into smaller boxes or import `hospital` and `prison` separately."
        )
    return None

def format_overpass_http_error(status: int, body: str, *, building_type: str) -> str:
    """Return a short admin-facing error for an Overpass failure."""
    text = html_lib.unescape(re.sub(r"<[^>]+>", " ", str(body or "")))
    text = re.sub(r"\s+", " ", text).strip()
    if int(status) == 504:
        return (
            f"Overpass returned HTTP 504 while importing `{building_type}` candidates. "
            "The public Overpass server timed out or rejected the query as too heavy. "
            "Try a smaller bounding box, or import `hospital` and `prison` separately."
        )
    detail = f" Detail: {_truncate_discord_text(text, 300)}" if text else ""
    return f"Overpass returned HTTP {int(status)} while importing `{building_type}` candidates.{detail}"

def _decode_dbf_text(value: bytes) -> str:
    """Decode DBF text from Geofabrik shapefile extracts."""
    raw = bytes(value or b"").replace(b"\x00", b"").strip()
    if not raw:
        return ""
    for encoding in ("utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding).strip()
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", "ignore").strip()

def _read_geofabrik_dbf_rows(data: bytes) -> List[Dict[str, str]]:
    """Read the small DBF subset needed from a Geofabrik shapefile layer."""
    if len(data) < 33:
        return []
    record_count = struct.unpack("<I", data[4:8])[0]
    header_length = struct.unpack("<H", data[8:10])[0]
    record_length = struct.unpack("<H", data[10:12])[0]
    fields: List[Tuple[str, int, int]] = []
    pos = 32
    offset = 1
    while pos + 32 <= len(data) and data[pos] != 0x0D:
        descriptor = data[pos : pos + 32]
        name = descriptor[:11].split(b"\x00", 1)[0].decode("ascii", "ignore").strip()
        length = int(descriptor[16])
        if name and length:
            fields.append((name, offset, length))
        offset += length
        pos += 32
    rows: List[Dict[str, str]] = []
    for index in range(int(record_count)):
        start = header_length + index * record_length
        end = start + record_length
        if end > len(data):
            break
        record = data[start:end]
        if not record or record[:1] == b"*":
            continue
        rows.append(
            {
                name: _decode_dbf_text(record[field_offset : field_offset + field_length])
                for name, field_offset, field_length in fields
            }
        )
    return rows

def _read_geofabrik_shp_points(data: bytes) -> List[Optional[Tuple[float, float]]]:
    """Read point or polygon-center coordinates from a Geofabrik SHP layer."""
    points: List[Optional[Tuple[float, float]]] = []
    if len(data) < 100:
        return points
    pos = 100
    while pos + 8 <= len(data):
        try:
            _record_number, content_words = struct.unpack(">2i", data[pos : pos + 8])
        except struct.error:
            break
        pos += 8
        content_length = int(content_words) * 2
        content = data[pos : pos + content_length]
        pos += content_length
        if len(content) < 4:
            points.append(None)
            continue
        shape_type = struct.unpack("<i", content[:4])[0]
        if shape_type == 0:
            points.append(None)
        elif shape_type == 1 and len(content) >= 20:
            lon, lat = struct.unpack("<2d", content[4:20])
            points.append((float(lat), float(lon)))
        elif shape_type in {3, 5, 13, 15, 23, 25, 31} and len(content) >= 36:
            xmin, ymin, xmax, ymax = struct.unpack("<4d", content[4:36])
            points.append((float((ymin + ymax) / 2), float((xmin + xmax) / 2)))
        else:
            points.append(None)
    return points

def _normalize_candidate_facility_text(value: Any) -> str:
    """Normalize a facility name for conservative Geofabrik filtering."""
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text.casefold())
    return re.sub(r"\s+", " ", text).strip()

def _candidate_name_contains(text: str, terms: Iterable[str]) -> bool:
    searchable = f" {text} "
    return any(f" {_normalize_candidate_facility_text(term)} " in searchable for term in terms)

GEOFABRIK_STRONG_HOSPITAL_TERMS = {
    "hospital",
    "hospitals",
    "hopital",
    "hopital general",
    "hospicio",
    "ospedale",
    "ziekenhuis",
    "krankenhaus",
    "klinikum",
    "universitatsklinikum",
    "universitaetsklinikum",
    "sairaala",
    "sjukhus",
    "sygehus",
    "nosocomio",
    "lazarett",
    "centre hospitalier",
    "centro hospitalario",
    "hospital universitario",
    "regional hospital",
    "general hospital",
    "medical center",
    "medical centre",
    "university medical center",
}

GEOFABRIK_HOSPITAL_REJECT_TERMS = {
    "centro de salud",
    "centro salud",
    "centre de sante",
    "centre de santé",
    "health center",
    "health centre",
    "health post",
    "health station",
    "clinic",
    "clinique",
    "clinica",
    "kliniek",
    "polyclinic",
    "policlinico",
    "poli clinico",
    "dispensary",
    "pharmacy",
    "farmacia",
    "doctor",
    "doctors",
    "medical office",
    "physician",
    "dentist",
    "veterinary",
    "rehab center",
    "rehabilitation center",
    "planta de gas",
    "gas plant",
    "plant",
    "factory",
    "fabrica",
}

GEOFABRIK_STRONG_PRISON_TERMS = {
    "prison",
    "jail",
    "gaol",
    "correctional",
    "correctional facility",
    "correctional institution",
    "detention",
    "detention center",
    "detention centre",
    "penitentiary",
    "penal",
    "penal unit",
    "unidad penal",
    "carcel",
    "prision",
    "prisao",
    "presidio",
    "penitenciario",
    "gevangenis",
    "justizvollzugsanstalt",
    "jva",
    "centre penitentiaire",
    "maison d arret",
}

GEOFABRIK_PRISON_REJECT_TERMS = {
    "courthouse",
    "court house",
    "police station",
    "sheriff office",
    "sheriff s office",
    "police department",
    "law office",
    "museum",
    "historic",
    "historical",
}

def _geofabrik_candidate_building_type(fclass: str, name: str) -> Optional[str]:
    """Return a supported building type only when the Geofabrik name is clear enough."""
    normalized_name = _normalize_candidate_facility_text(name)
    if not normalized_name:
        return None
    if fclass == "hospital":
        if _candidate_name_contains(normalized_name, GEOFABRIK_HOSPITAL_REJECT_TERMS):
            return None
        if _candidate_name_contains(normalized_name, GEOFABRIK_STRONG_HOSPITAL_TERMS):
            return "Hospital"
        return None
    if fclass == "prison":
        if _candidate_name_contains(normalized_name, GEOFABRIK_PRISON_REJECT_TERMS):
            return None
        if _candidate_name_contains(normalized_name, GEOFABRIK_STRONG_PRISON_TERMS):
            return "Prison"
        return None
    return None

def parse_geofabrik_shp_auto_build_candidates(
    zip_path: str,
    *,
    extract_id: str,
    extract_name: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Parse Geofabrik free shapefile ZIP into local auto-build candidates."""
    candidates: List[Dict[str, Any]] = []
    stats = {
        "source_elements": 0,
        "accepted": 0,
        "rejected": 0,
    }
    layers = ("gis_osm_pois_free_1", "gis_osm_pois_a_free_1")
    with zipfile.ZipFile(zip_path) as archive:
        names = set(archive.namelist())
        for layer in layers:
            dbf_name = f"{layer}.dbf"
            shp_name = f"{layer}.shp"
            if dbf_name not in names or shp_name not in names:
                continue
            rows = _read_geofabrik_dbf_rows(archive.read(dbf_name))
            points = _read_geofabrik_shp_points(archive.read(shp_name))
            for index, row in enumerate(rows):
                stats["source_elements"] += 1
                fclass = str(row.get("fclass") or "").casefold().strip()
                point = points[index] if index < len(points) else None
                name = _clean_building_name(row.get("name") or "")
                building_type = _geofabrik_candidate_building_type(fclass, name)
                if not building_type:
                    stats["rejected"] += 1
                    continue
                if not point or not name:
                    stats["rejected"] += 1
                    continue
                lat, lon = point
                if not -90 <= lat <= 90 or not -180 <= lon <= 180:
                    stats["rejected"] += 1
                    continue
                osm_id = str(row.get("osm_id") or "").strip()
                source_id = f"{extract_id}:{layer}:{osm_id or index}"
                raw_tags = {
                    "source": "geofabrik",
                    "extract_id": extract_id,
                    "extract_name": extract_name,
                    "layer": layer,
                    "osm_id": osm_id,
                    "fclass": fclass,
                }
                candidates.append(
                    {
                        "source": "geofabrik",
                        "source_id": source_id,
                        "building_type": building_type,
                        "name": name,
                        "lat": lat,
                        "lon": lon,
                        "address": None,
                        "country": extract_name,
                        "region": extract_name,
                        "raw_tags_json": json.dumps(raw_tags, ensure_ascii=False, sort_keys=True),
                    }
                )
                stats["accepted"] += 1
    return candidates, stats

def extract_missionchief_building_id(*values: Any) -> Optional[int]:
    """Extract a MissionChief building id from URLs, response text, or snapshots."""
    for value in values:
        if value is None:
            continue
        if isinstance(value, dict):
            for key in ("buildingId", "building_id", "matchedBuildingId"):
                found_id = _coerce_int(value.get(key))
                if found_id:
                    return found_id
            nested_values = []
            for key in (
                "buildingId",
                "building_id",
                "matchedBuildingId",
                "url",
                "responseUrl",
                "finalUrl",
                "postUrl",
                "location",
                "Location",
                "redirectLocation",
                "apiLookup",
                "allianceListLookup",
                "allianceLogLookup",
                "beforeAllianceListLookup",
            ):
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
    return re.sub(r"\s+", " ", _decode_url_text(value)).strip().casefold()

def _normalize_loose_match_text(value: Any) -> str:
    """Normalize text for matching names across punctuation differences."""
    text = _normalize_match_text(value)
    return re.sub(r"[^0-9a-zA-ZÀ-ž]+", " ", text).strip()

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

def _parse_credit_amount(value: Any) -> Optional[int]:
    """Parse a visible MissionChief credit amount."""
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None

def parse_alliance_funds_from_html(html: str) -> Optional[int]:
    """Parse the current alliance funds balance from the MissionChief treasury page."""
    text = html_lib.unescape(re.sub(r"<[^>]+>", " ", str(html or "")))
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None

    lowered = text.casefold()
    marker = -1
    for marker_text in (
        "alliance funds",
        "alliance fund",
        "alliance treasury",
        "verband funds",
    ):
        marker = lowered.find(marker_text)
        if marker >= 0:
            break
    if marker < 0:
        return None

    window = text[max(0, marker - 500) : marker + 1000]
    for pattern in (
        r"Alliance\s+Funds\s+(\d[\d,\.\s]*)\s+Credits",
        r"Alliance\s+Treasury\s+(\d[\d,\.\s]*)\s+Credits",
        r"(\d[\d,\.\s]*)\s+Credits",
    ):
        match = re.search(pattern, window, re.IGNORECASE)
        if match:
            return _parse_credit_amount(match.group(1))
    return None

def alliance_funds_allow_auto_build(
    funds: Optional[int],
    source: str,
    minimum: int = ALLIANCE_BUILDING_MIN_FUNDS,
) -> bool:
    """Return whether automatic building is allowed by the funds safety rule."""
    return funds is not None and funds >= int(minimum) and source.startswith("live MissionChief")

def building_create_result_needs_recovery(result: BuildingCreateResult) -> bool:
    """Return whether a failed create result may still have created a building."""
    if result.ok:
        return False
    reason = str(result.reason or "").casefold()
    return "timed out" in reason or "timeout" in reason

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
    target_name_loose = _normalize_loose_match_text(config.get("name"))
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
        candidate_text_loose = _normalize_loose_match_text(candidate_text)
        if not candidate_text or (
            target_name not in candidate_text
            and (not target_name_loose or target_name_loose not in candidate_text_loose)
        ):
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

def find_new_created_alliance_building_id_from_list(
    before_candidates: Iterable[Dict[str, Any]],
    after_candidates: Iterable[Dict[str, Any]],
    config: Dict[str, str],
) -> Optional[int]:
    """Find a newly created alliance building by comparing list snapshots.

    This is more reliable than name matching when MissionChief displays a
    URL-encoded or localized name differently from the request data.
    """
    before_ids = {
        building_id
        for record in before_candidates or []
        if isinstance(record, dict)
        for building_id in [_coerce_int(record.get("id"))]
        if building_id is not None
    }
    target_name = _normalize_match_text(config.get("name"))
    target_name_loose = _normalize_loose_match_text(config.get("name"))
    requested_type = str(config.get("buildingType") or "")

    matches: List[Tuple[int, int]] = []
    for record in after_candidates or []:
        if not isinstance(record, dict):
            continue
        building_id = _coerce_int(record.get("id"))
        if building_id is None or building_id in before_ids:
            continue

        type_score = _alliance_list_type_score(record, requested_type)
        if type_score < 0:
            continue

        candidate_text = _alliance_list_candidate_text(record)
        candidate_text_loose = _normalize_loose_match_text(candidate_text)
        score = 100 + type_score
        if target_name and target_name in candidate_text:
            score += 50
        elif target_name_loose and target_name_loose in candidate_text_loose:
            score += 35
        if _normalize_match_text(record.get("text")) == target_name:
            score += 40
        if _normalize_match_text(record.get("searchAttribute")) == target_name:
            score += 30
        matches.append((score, building_id))

    if not matches:
        return None
    matches.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return matches[0][1]

def find_created_alliance_building_id_from_logs(
    candidates: Iterable[Dict[str, Any]],
    config: Dict[str, str],
) -> Optional[int]:
    """Find a newly created alliance building from the newest alliance log page.

    This fallback intentionally requires the requested building name to appear
    in the log row. Alliance logs are newest-first, but other admins can build
    around the same time, so a generic "first building log" match is not safe.
    """
    target_name = _normalize_match_text(config.get("name"))
    target_name_loose = _normalize_loose_match_text(config.get("name"))
    if not target_name:
        return None

    matches: List[Tuple[int, int, int]] = []
    for index, record in enumerate(candidates or []):
        if not isinstance(record, dict):
            continue
        building_id = _coerce_int(record.get("id"))
        if building_id is None:
            continue

        affected_name = _normalize_match_text(record.get("affectedName"))
        row_text = _normalize_match_text(
            " ".join(
                str(record.get(key) or "")
                for key in ("affectedName", "rowText", "href")
            )
        )
        row_text_loose = _normalize_loose_match_text(row_text)
        exact_match = target_name in affected_name or target_name in row_text
        loose_match = bool(target_name_loose and target_name_loose in row_text_loose)
        if not exact_match and not loose_match:
            continue

        score = 100
        if exact_match:
            score += 50
        if "building constructed" in row_text:
            score += 25
        matches.append((score, -index, building_id))

    if not matches:
        return None
    matches.sort(reverse=True)
    return matches[0][2]

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


@dataclass
class AutoBuildCandidate:
    """Local OSM-derived candidate for automatic alliance building creation."""

    candidate_id: int
    source: str
    source_id: str
    building_type: str
    name: str
    lat: float
    lon: float
    address: Optional[str] = None
    country: Optional[str] = None
    region: Optional[str] = None
    raw_tags: Optional[Dict[str, Any]] = None
    status: str = "available"
    status_reason: Optional[str] = None
    missionchief_building_id: Optional[int] = None

    @property
    def coordinates(self) -> str:
        return f"{self.lat:.7f}, {self.lon:.7f}"

    @property
    def location_input(self) -> str:
        return f"{self.source}:{self.source_id}"

    @property
    def osm_url(self) -> str:
        element_type, _, element_id = self.source_id.partition("/")
        if element_type and element_id:
            return f"https://www.openstreetmap.org/{element_type}/{element_id}"
        return f"https://www.openstreetmap.org/?mlat={self.lat:.7f}&mlon={self.lon:.7f}#map=18/{self.lat:.7f}/{self.lon:.7f}"


@dataclass
class AutoBuildPlan:
    """Dry-run result for one automatic candidate build slot."""

    building_type: str
    candidate: Optional[AutoBuildCandidate]
    blocked_reason: Optional[str] = None
    duplicate_distance_m: Optional[float] = None
    duplicate_building_id: Optional[int] = None
    duplicate_check_source: str = "not checked"

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


@dataclass
class BoardBuildingPost:
    post_id: int
    author_id: Optional[str]
    author_name: str
    created_at: str
    content: str


@dataclass
class BoardPage:
    posts: List[BoardBuildingPost]
    last_page: int = 1
    current_user_id: Optional[str] = None
    reply_action: Optional[str] = None
    reply_token: Optional[str] = None


@dataclass(frozen=True)
class BoardBuildingRequestSpec:
    building_type: Optional[str]
    location_input: str
    matched_alias: str


@dataclass
class MissionChiefForm:
    action: Optional[str]
    method: str
    fields: Dict[str, str]


class BuildingBoardPageParser(HTMLParser):
    """Parse MissionChief alliance board pages for building requests and reply form data."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.posts: List[BoardBuildingPost] = []
        self.page_numbers: List[int] = []
        self.current_user_id: Optional[str] = None
        self.reply_action: Optional[str] = None
        self.reply_token: Optional[str] = None
        self._post: Optional[dict] = None
        self._post_depth = 0
        self._content_depth = 0
        self._capture_author = False
        self._capture_content = False
        self._capture_page_number = False
        self._capture_active_page = False

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        attr = {key: value for key, value in attrs}

        if tag == "div" and str(attr.get("id") or "").startswith("post-on-page-"):
            self._post = {
                "post_id": None,
                "author_id": None,
                "author_name": "",
                "created_at": "",
                "content": [],
            }
            self._post_depth = 1
            return

        if self._post is not None and tag == "div":
            self._post_depth += 1
            classes = str(attr.get("class") or "")
            if "col-md-11" in classes:
                self._content_depth = self._post_depth
                self._capture_content = True

        if self._post is not None and tag == "a":
            href = str(attr.get("href") or "")
            profile_match = re.search(r"/profile/(\d+)", href)
            if profile_match and not self._post.get("author_id"):
                self._post["author_id"] = profile_match.group(1)
                self._capture_author = True

            post_match = re.search(r"/alliance_posts/(\d+)", href)
            if post_match:
                self._post["post_id"] = int(post_match.group(1))

        if self._post is not None and tag == "span":
            title = attr.get("title")
            if title and not self._post.get("created_at"):
                self._post["created_at"] = str(title)

        if self._post is not None and self._capture_content and tag == "br":
            self._post["content"].append("\n")

        if tag == "a":
            href = str(attr.get("href") or "")
            page_match = re.search(r"[?&]page=(\d+)", href)
            if page_match:
                self.page_numbers.append(int(page_match.group(1)))
            self._capture_page_number = bool(page_match)

        if tag == "li" and "active" in str(attr.get("class") or ""):
            self._capture_active_page = True

        if tag == "form" and str(attr.get("id") or "") == "new_alliance_post":
            self.reply_action = attr.get("action")

        if tag == "input" and attr.get("name") == "authenticity_token":
            token = attr.get("value")
            if token:
                self.reply_token = token

    def handle_data(self, data: str):
        if "user_id =" in data:
            match = re.search(r"user_id\s*=\s*(\d+)", data)
            if match:
                self.current_user_id = match.group(1)

        if self._post is not None and self._capture_author:
            text = re.sub(r"\s+", " ", data).strip()
            if text:
                self._post["author_name"] = text

        if self._post is not None and self._capture_content:
            self._post["content"].append(data)

        if self._capture_page_number:
            try:
                self.page_numbers.append(int(data.strip()))
            except ValueError:
                pass

        if self._capture_active_page:
            try:
                self.page_numbers.append(int(data.strip()))
            except ValueError:
                pass

    def handle_endtag(self, tag: str):
        if self._capture_author and tag == "a":
            self._capture_author = False

        if self._capture_page_number and tag == "a":
            self._capture_page_number = False

        if self._capture_active_page and tag == "li":
            self._capture_active_page = False

        if self._post is not None and tag == "div":
            if self._capture_content and self._post_depth == self._content_depth:
                self._capture_content = False
                self._content_depth = 0

            self._post_depth -= 1
            if self._post_depth <= 0:
                self._finish_post()

    def _finish_post(self) -> None:
        if self._post is None:
            return

        post_id = self._post.get("post_id")
        if post_id is None:
            self._post = None
            return

        content = "".join(self._post.get("content") or [])
        content = re.sub(r"\n\s*\n+", "\n", content)
        content = re.sub(r"[ \t]+", " ", content).strip()
        self.posts.append(
            BoardBuildingPost(
                post_id=int(post_id),
                author_id=self._post.get("author_id"),
                author_name=str(self._post.get("author_name") or "Unknown"),
                created_at=str(self._post.get("created_at") or ""),
                content=content,
            )
        )
        self._post = None

    def page(self) -> BoardPage:
        return BoardPage(
            posts=self.posts,
            last_page=max(self.page_numbers or [1]),
            current_user_id=self.current_user_id,
            reply_action=self.reply_action,
            reply_token=self.reply_token,
        )


def parse_building_board_page(html: str) -> BoardPage:
    parser = BuildingBoardPageParser()
    parser.feed(html or "")
    return parser.page()


class MissionChiefFormParser(HTMLParser):
    """Small generic parser for MissionChief Rails forms."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.forms: List[MissionChiefForm] = []
        self._form: Optional[dict] = None
        self._textarea_name: Optional[str] = None
        self._textarea_text: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        attr = {key: value for key, value in attrs}
        if tag == "form":
            self._form = {
                "action": attr.get("action"),
                "method": str(attr.get("method") or "get").lower(),
                "fields": {},
            }
            return

        if self._form is None:
            return

        if tag == "input":
            name = attr.get("name")
            if name:
                self._form["fields"][name] = attr.get("value") or ""
        elif tag == "textarea":
            name = attr.get("name")
            if name:
                self._textarea_name = name
                self._textarea_text = []
        elif tag == "select":
            name = attr.get("name")
            if name and name not in self._form["fields"]:
                self._form["fields"][name] = ""

    def handle_data(self, data: str):
        if self._form is not None and self._textarea_name:
            self._textarea_text.append(data)

    def handle_endtag(self, tag: str):
        if self._form is None:
            return
        if tag == "textarea" and self._textarea_name:
            self._form["fields"][self._textarea_name] = "".join(self._textarea_text)
            self._textarea_name = None
            self._textarea_text = []
        elif tag == "form":
            self.forms.append(
                MissionChiefForm(
                    action=self._form.get("action"),
                    method=str(self._form.get("method") or "get").lower(),
                    fields=dict(self._form.get("fields") or {}),
                )
            )
            self._form = None


def parse_missionchief_forms(html: str) -> List[MissionChiefForm]:
    parser = MissionChiefFormParser()
    parser.feed(html or "")
    return parser.forms


def _is_supported_board_maps_url(value: str) -> bool:
    try:
        parsed = urlparse(value.strip())
    except Exception:
        return False
    host = parsed.netloc.lower()
    return (
        "google." in host
        or host in {"maps.app.goo.gl", "goo.gl"}
        or host.endswith(".goo.gl")
    )


def _extract_board_maps_url(text: str) -> Optional[str]:
    for match in re.finditer(r"https?://[^\s<>\]\)\"']+", text or "", flags=re.IGNORECASE):
        url = match.group(0).rstrip(".,;")
        if _is_supported_board_maps_url(url):
            return url
    return None


def _strip_urls_for_board_type_match(text: str) -> str:
    """Remove URLs before matching the requested building type."""
    return re.sub(r"https?://[^\s<>\]\)\"']+", " ", text or "", flags=re.IGNORECASE)


def _normalize_board_type_text(text: str) -> str:
    """Normalize board type text for exact and fuzzy matching."""
    text = html_lib.unescape(text or "").casefold()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_facility_text(text: str) -> str:
    """Normalize facility text for conservative building-type detection."""
    text = html_lib.unescape(_decode_url_text(text or "")).casefold()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _levenshtein_distance(left: str, right: str) -> int:
    """Return a small edit distance for short request words."""
    if left == right:
        return 0
    if not left:
        return len(right)
    if not right:
        return len(left)
    previous = list(range(len(right) + 1))
    for left_index, left_char in enumerate(left, start=1):
        current = [left_index]
        for right_index, right_char in enumerate(right, start=1):
            current.append(
                min(
                    current[right_index - 1] + 1,
                    previous[right_index] + 1,
                    previous[right_index - 1] + (0 if left_char == right_char else 1),
                )
            )
        previous = current
    return previous[-1]


def _is_fuzzy_board_type_match(candidate: str, alias: str) -> bool:
    """Return whether a user word is a likely typo for a supported building type."""
    candidate = _normalize_board_type_text(candidate).replace(" ", "")
    alias = _normalize_board_type_text(alias).replace(" ", "")
    if len(candidate) < 4 or len(alias) < 4:
        return False
    if abs(len(candidate) - len(alias)) > max(1, len(alias) // 4):
        return False
    distance = _levenshtein_distance(candidate, alias)
    if distance <= 1:
        return True
    if len(alias) >= 8 and distance <= 2:
        return difflib.SequenceMatcher(None, candidate, alias).ratio() >= 0.78
    return difflib.SequenceMatcher(None, candidate, alias).ratio() >= 0.86


def _match_board_building_type(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    matches: List[Tuple[str, str]] = []
    type_text = _strip_urls_for_board_type_match(text)
    normalized = _normalize_board_type_text(type_text)
    searchable = f" {normalized} "
    for building_type, aliases in BOARD_BUILDING_TYPE_ALIASES.items():
        for alias in aliases:
            normalized_alias = _normalize_board_type_text(alias)
            if not normalized_alias:
                continue
            if f" {normalized_alias} " in searchable:
                matches.append((building_type, alias))
                break

    if not matches:
        tokens = normalized.split()
        for building_type, aliases in BOARD_BUILDING_TYPE_ALIASES.items():
            for alias in aliases:
                alias_words = _normalize_board_type_text(alias).split()
                if not alias_words:
                    continue
                size = len(alias_words)
                for index in range(0, max(0, len(tokens) - size + 1)):
                    candidate = " ".join(tokens[index : index + size])
                    if _is_fuzzy_board_type_match(candidate, " ".join(alias_words)):
                        matches.append((building_type, alias))
                        break
                if any(match_type == building_type for match_type, _alias in matches):
                    break

    unique_types = list(dict.fromkeys(building_type for building_type, _alias in matches))
    if len(unique_types) > 1:
        return None, None, "The request mentions both Hospital and Prison. Please submit one building per post."
    if not matches:
        return None, None, None
    return matches[0][0], matches[0][1], None


def extract_building_board_request(text: str) -> Tuple[Optional[BoardBuildingRequestSpec], Optional[str]]:
    """Extract one BuildingManager board request from a MissionChief post."""
    cleaned = html_lib.unescape(str(text or "")).strip()
    if not cleaned:
        return None, "The post is empty."

    maps_url = _extract_board_maps_url(cleaned)
    if not maps_url:
        return None, "No supported Google Maps link was found. Paste a Google Maps place link or maps.app.goo.gl link."

    building_type, alias, error = _match_board_building_type(cleaned)
    if error:
        return None, error

    return BoardBuildingRequestSpec(
        building_type=building_type,
        location_input=maps_url,
        matched_alias=str(alias or building_type or ""),
    ), None


def build_building_board_guide_content(thread_id: int = BOARD_THREAD_ID) -> str:
    """Build the maintained MissionChief board guide post."""
    return "\n".join(
        [
            BOARD_GUIDE_MARKER,
            "[b]Building Request Guide[/b]",
            "",
            "This post is maintained automatically by the Fire & Rescue Academy.",
            "",
            "[b]Request in this topic[/b]",
            f"Thread: https://www.missionchief.com/alliance_threads/{int(thread_id)}",
            "",
            "[b]What can be requested[/b]",
            "- Hospital",
            "- Prison",
            "",
            "[b]How to request[/b]",
            "- Create a new post in this topic.",
            "- Paste a Google Maps link to a real hospital or prison/jail.",
            "- You do not need to type Hospital or Prison. Fire & Rescue Academy detects the type from the location.",
            "- Clinics, doctor offices, museums, historic sites, courthouses, and police stations are rejected.",
            "- You do not need to type a building name. The name is detected from Google Maps.",
            "- Requests are reviewed by admins before they are built.",
            "- Your request post and the Fire & Rescue Academy reply are removed after 12 hours.",
            "",
            "[b]Formats[/b]",
            "<Google Maps link>",
            "Hospital: <Google Maps link> and Prison: <Google Maps link> still work, but the location decides the type.",
            "",
            "[b]Examples[/b]",
            "https://www.google.com/maps/place/Example+Hospital/@40.0,-73.0",
            "https://maps.app.goo.gl/example",
        ]
    )

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
    _hospital_positive_terms = {
        "hospital",
        "general hospital",
        "regional hospital",
        "university hospital",
        "childrens hospital",
        "children s hospital",
        "medical center",
        "medical centre",
        "ziekenhuis",
        "hopital",
        "krankenhaus",
        "klinikum",
        "ospedale",
        "hospital universitario",
        "centre hospitalier",
        "centro hospitalario",
        "sairaala",
        "sjukhus",
        "sygehus",
    }
    _hospital_reject_terms = {
        "clinic",
        "clinique",
        "kliniek",
        "doctor",
        "doctors",
        "medical office",
        "physician",
        "gp practice",
        "urgent care",
        "pharmacy",
        "dentist",
        "veterinary",
        "rehab center",
        "rehabilitation center",
        "health center",
        "health centre",
    }
    _prison_positive_terms = {
        "prison",
        "jail",
        "correctional facility",
        "correctional institution",
        "detention center",
        "detention centre",
        "penitentiary",
        "remand center",
        "remand centre",
        "gevangenis",
        "carcel",
        "jva",
        "justizvollzugsanstalt",
        "maison d arret",
        "centre penitentiaire",
    }
    _prison_reject_terms = {
        "courthouse",
        "court house",
        "police station",
        "sheriff office",
        "sheriff s office",
        "police department",
        "law office",
    }
    _inactive_facility_terms = {
        "museum",
        "historic",
        "historical",
        "heritage",
        "monument",
        "tourist attraction",
        "former hospital",
        "former prison",
        "former jail",
        "old jail museum",
    }
    
    @staticmethod
    def extract_coordinates(text: str) -> Optional[Tuple[float, float]]:
        """Extract coordinates from various formats."""
        decoded_text = _decode_url_text(text)

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
        decoded_text = _decode_url_text(text)
        match = re.search(r"/maps/place/([^/@?]+)", decoded_text)
        if match:
            return _clean_building_name(match.group(1)) or None

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
        cleaned = _decode_url_text(value)
        if not cleaned:
            return None
        if re.fullmatch(r"-?\d+\.?\d*\s*[,~]\s*-?\d+\.?\d*", cleaned):
            return None
        return cleaned

    @staticmethod
    def derive_building_name(building_type: str, location_details: LocationDetails) -> str:
        """Return the best building name from resolved location details."""
        if location_details.place_name:
            return _clean_building_name(location_details.place_name)

        if location_details.address:
            first_part = location_details.address.split(",", 1)[0].strip()
            if first_part:
                return _clean_building_name(first_part)

        place_name = LocationParser.extract_place_name(location_details.resolved_input)
        if place_name:
            return _clean_building_name(place_name)

        return f"{building_type} location"

    @classmethod
    async def expand_maps_url(cls, text: str) -> str:
        """Resolve Google Maps short URLs to their final URL when possible."""
        if not cls.is_maps_short_url(text):
            return text

        try:
            headers = {
                "User-Agent": "DiscordBot-BuildingManager/1.0",
                "Accept-Language": "en-US,en;q=0.9",
            }
            async with aiohttp.ClientSession() as session:
                async with session.get(text, allow_redirects=True, timeout=10, headers=headers) as resp:
                    return str(resp.url)
        except Exception as e:
            log.warning("Google Maps URL expansion failed: %r", e)
            return text

    @staticmethod
    def _normalize_geocode_query(value: Optional[str]) -> Optional[str]:
        """Normalize a Maps-derived place query before sending it to geocoders."""
        if not value:
            return None
        text = _decode_url_text(value)
        text = html_lib.unescape(text)
        text = text.replace("،", ",")
        replacements = {
            "egypte": "Egypt",
            "nederland": "Netherlands",
            "duitsland": "Germany",
            "belgie": "Belgium",
            "belgië": "Belgium",
            "spanje": "Spain",
            "italie": "Italy",
            "italië": "Italy",
            "frankrijk": "France",
            "verenigd koninkrijk": "United Kingdom",
        }
        for source, target in replacements.items():
            text = re.sub(rf"\b{re.escape(source)}\b", target, text, flags=re.IGNORECASE)
        text = re.sub(r"\s+", " ", text).strip(" ,")
        return text or None

    @classmethod
    def _location_query_candidates(cls, place_name: Optional[str], resolved_input: str) -> List[str]:
        """Return conservative geocode queries from a Maps place URL."""
        raw_values = [place_name, cls.extract_place_name(resolved_input)]
        candidates: List[str] = []

        for raw in raw_values:
            cleaned = cls._normalize_geocode_query(raw)
            if not cleaned:
                continue
            candidates.append(cleaned)
            parts = [part.strip() for part in cleaned.split(",") if part.strip()]
            if len(parts) >= 3:
                candidates.append(" ".join([parts[0], parts[-2], parts[-1]]))
            if len(parts) >= 2:
                candidates.append(" ".join([parts[0], parts[-1]]))
            candidates.append(parts[0] if parts else cleaned)

        normalized_resolved = cls._normalize_geocode_query(resolved_input)
        if normalized_resolved and not cls.extract_coordinates(normalized_resolved):
            parsed = urlparse(normalized_resolved)
            if parsed.scheme not in {"http", "https"}:
                candidates.append(normalized_resolved)

        deduped: List[str] = []
        seen = set()
        for candidate in candidates:
            candidate = re.sub(r"\s+", " ", candidate or "").strip()
            if not candidate:
                continue
            key = candidate.casefold()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(candidate)
        return deduped

    @classmethod
    async def resolve_location(
        cls,
        location_input: str,
        *,
        google_key: Optional[str] = None,
        mapsco_key: Optional[str] = None,
    ) -> LocationDetails:
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
            geocode = await cls.reverse_geocode_details(lat, lon, google_key=google_key)
        else:
            geocode = None
            for query in cls._location_query_candidates(place_name, resolved_input):
                geocode = await cls.forward_geocode_details(
                    query,
                    google_key=google_key,
                    mapsco_key=mapsco_key,
                )
                if geocode and geocode.get("coordinates"):
                    break
            coordinates_str = geocode.get("coordinates") if geocode else None

        if geocode:
            address = geocode.get("address")
            country = geocode.get("country")
            region = geocode.get("region")
            provider = geocode.get("provider")
            place_name = _clean_building_name(place_name or geocode.get("place_name"))
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
    async def forward_geocode_details(
        cls,
        query: str,
        *,
        google_key: Optional[str] = None,
        mapsco_key: Optional[str] = None,
    ) -> Optional[dict]:
        """Forward geocode text or place names."""
        if google_key:
            details = await cls.forward_geocode_google_details(query, google_key)
            if details:
                return details
        if mapsco_key:
            details = await cls.forward_geocode_mapsco_details(query, mapsco_key)
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

    @classmethod
    async def forward_geocode_mapsco_details(cls, query: str, api_key: str) -> Optional[dict]:
        """Forward geocode using geocode.maps.co for Google Maps place fallbacks."""
        params = {
            "q": query,
            "format": "json",
            "addressdetails": "1",
            "namedetails": "1",
            "limit": "1",
            "accept-language": "en",
        }
        headers = {
            "User-Agent": "DiscordBot-BuildingManager/1.0",
            "Authorization": f"Bearer {api_key}",
        }
        timeout = aiohttp.ClientTimeout(total=GEOCODE_MAPS_TIMEOUT_SECONDS)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    GEOCODE_MAPS_SEARCH_URL,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                ) as resp:
                    if resp.status != 200:
                        log.warning("geocode.maps.co returned HTTP %s for BuildingManager query %r", resp.status, query)
                        return None
                    data = await resp.json(content_type=None)
        except Exception as e:
            log.warning("geocode.maps.co forward geocoding failed: %r", e)
            return None

        if not data:
            return None
        details = cls._nominatim_result_to_details(data[0])
        details["provider"] = "geocode.maps.co"
        return details

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
    def _facility_search_text(
        cls,
        location_details: LocationDetails,
        building_name: Optional[str] = None,
    ) -> Tuple[str, str]:
        values = [
            building_name,
            location_details.place_name,
            location_details.address,
            cls.extract_place_name(location_details.resolved_input),
            location_details.resolved_input,
            location_details.detected_facility_type,
        ]
        text = _normalize_facility_text(" ".join(value for value in values if value))
        facility_type = _normalize_facility_text(location_details.detected_facility_type or "")
        return text, facility_type

    @staticmethod
    def _contains_facility_term(text: str, terms: Iterable[str]) -> bool:
        searchable = f" {text} "
        return any(f" {_normalize_facility_text(term)} " in searchable for term in terms)

    @classmethod
    def detect_supported_building_type(
        cls,
        location_details: LocationDetails,
        building_name: Optional[str] = None,
    ) -> Tuple[Optional[str], str]:
        """Detect whether a location is clearly a supported alliance building type."""
        text, facility_type = cls._facility_search_text(location_details, building_name)
        if not text:
            return None, "The Google Maps link did not provide enough location details."

        if cls._contains_facility_term(text, cls._inactive_facility_terms):
            return None, "This location looks like a museum, historic site, or inactive facility."

        hospital_facility = cls._contains_facility_term(facility_type, {"hospital"})
        prison_facility = cls._contains_facility_term(
            facility_type,
            {"prison", "jail", "correctional", "detention", "penitentiary"},
        )
        hospital_name = cls._contains_facility_term(text, cls._hospital_positive_terms)
        prison_name = cls._contains_facility_term(text, cls._prison_positive_terms)
        hospital_reject = cls._contains_facility_term(text, cls._hospital_reject_terms)
        prison_reject = cls._contains_facility_term(text, cls._prison_reject_terms)

        hospital_score = (3 if hospital_facility else 0) + (2 if hospital_name else 0)
        prison_score = (3 if prison_facility else 0) + (2 if prison_name else 0)

        if hospital_reject and not hospital_facility:
            hospital_score = 0
        if prison_reject and not prison_facility:
            prison_score = 0

        if hospital_score > 0 and prison_score > 0:
            if hospital_score > prison_score:
                return "Hospital", "Detected as a hospital."
            if prison_score > hospital_score:
                return "Prison", "Detected as a prison or jail."
            return None, "This location has conflicting hospital and prison/jail signals."
        if hospital_score > 0:
            return "Hospital", "Detected as a hospital."
        if prison_score > 0:
            return "Prison", "Detected as a prison or jail."
        if hospital_reject:
            return None, "This location looks like a clinic, doctor office, pharmacy, or other non-hospital facility."
        if prison_reject:
            return None, "This location looks like a courthouse, police station, or other non-prison facility."
        return None, "This location does not clearly look like a real hospital or prison/jail."

    @classmethod
    def facility_warning(
        cls,
        building_type: str,
        building_name: str,
        location_details: LocationDetails,
    ) -> Optional[str]:
        """Return a warning if the location does not clearly match the requested type."""
        detected_type, reason = cls.detect_supported_building_type(location_details, building_name)
        if detected_type != building_type:
            return reason
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

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_auto_candidates (
                candidate_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                source_id TEXT NOT NULL,
                building_type TEXT NOT NULL,
                name TEXT NOT NULL,
                lat REAL NOT NULL,
                lon REAL NOT NULL,
                address TEXT,
                country TEXT,
                region TEXT,
                raw_tags_json TEXT,
                status TEXT NOT NULL DEFAULT 'available',
                status_reason TEXT,
                missionchief_building_id INTEGER,
                imported_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                used_at INTEGER,
                last_attempt_at INTEGER,
                UNIQUE(source, source_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_auto_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                run_date TEXT NOT NULL,
                building_type TEXT NOT NULL,
                candidate_id INTEGER,
                funds INTEGER,
                funds_source TEXT,
                result TEXT NOT NULL,
                request_id INTEGER,
                missionchief_building_id INTEGER,
                reason TEXT,
                created_at INTEGER NOT NULL,
                UNIQUE(guild_id, run_date, building_type)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_auto_extract_imports (
                extract_id TEXT PRIMARY KEY,
                extract_name TEXT,
                url TEXT,
                status TEXT NOT NULL,
                bytes_downloaded INTEGER,
                inserted INTEGER NOT NULL DEFAULT 0,
                updated INTEGER NOT NULL DEFAULT 0,
                skipped INTEGER NOT NULL DEFAULT 0,
                accepted INTEGER NOT NULL DEFAULT 0,
                rejected INTEGER NOT NULL DEFAULT 0,
                reason TEXT,
                imported_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
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

    def get_requests_by_status(self, status: str, *, limit: int = 10) -> List[Dict[str, Any]]:
        """Return stored building requests with a specific status."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT *
            FROM building_requests
            WHERE status = ?
            ORDER BY updated_at ASC, request_id ASC
            LIMIT ?
            ''',
            (str(status), int(limit)),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
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
    def _candidate_row_to_model(row: sqlite3.Row) -> AutoBuildCandidate:
        """Convert a candidate row to a typed model."""
        raw_tags = None
        if row["raw_tags_json"]:
            with contextlib.suppress(json.JSONDecodeError, TypeError):
                raw_tags = json.loads(row["raw_tags_json"])
        return AutoBuildCandidate(
            candidate_id=int(row["candidate_id"]),
            source=str(row["source"]),
            source_id=str(row["source_id"]),
            building_type=str(row["building_type"]),
            name=str(row["name"]),
            lat=float(row["lat"]),
            lon=float(row["lon"]),
            address=row["address"],
            country=row["country"],
            region=row["region"],
            raw_tags=raw_tags,
            status=str(row["status"]),
            status_reason=row["status_reason"],
            missionchief_building_id=_coerce_int(row["missionchief_building_id"]),
        )

    def upsert_auto_candidates(self, records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
        """Insert or refresh automatic build candidates."""
        now = ts()
        inserted = 0
        updated = 0
        skipped = 0
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for record in records:
            try:
                source = str(record["source"])
                source_id = str(record["source_id"])
                building_type = str(record["building_type"])
                name = _clean_building_name(record["name"])
                lat = float(record["lat"])
                lon = float(record["lon"])
            except (KeyError, TypeError, ValueError):
                skipped += 1
                continue
            if building_type not in ALLIANCE_BUILDING_TYPE_IDS or not name:
                skipped += 1
                continue

            cursor.execute(
                "SELECT candidate_id, status FROM building_auto_candidates WHERE source = ? AND source_id = ?",
                (source, source_id),
            )
            existing = cursor.fetchone()
            cursor.execute(
                '''
                INSERT INTO building_auto_candidates
                (source, source_id, building_type, name, lat, lon, address, country, region,
                 raw_tags_json, status, imported_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'available', ?, ?)
                ON CONFLICT(source, source_id) DO UPDATE SET
                    building_type = excluded.building_type,
                    name = excluded.name,
                    lat = excluded.lat,
                    lon = excluded.lon,
                    address = excluded.address,
                    country = excluded.country,
                    region = excluded.region,
                    raw_tags_json = excluded.raw_tags_json,
                    status = CASE
                        WHEN building_auto_candidates.status IN ('used', 'duplicate') THEN building_auto_candidates.status
                        ELSE 'available'
                    END,
                    status_reason = CASE
                        WHEN building_auto_candidates.status IN ('used', 'duplicate') THEN building_auto_candidates.status_reason
                        ELSE NULL
                    END,
                    updated_at = excluded.updated_at
                ''',
                (
                    source,
                    source_id,
                    building_type,
                    name,
                    lat,
                    lon,
                    record.get("address"),
                    record.get("country"),
                    record.get("region"),
                    record.get("raw_tags_json"),
                    now,
                    now,
                ),
            )
            if existing:
                updated += 1
            else:
                inserted += 1
        conn.commit()
        conn.close()
        return {"inserted": inserted, "updated": updated, "skipped": skipped}

    def get_auto_candidate_stats(self) -> Dict[str, int]:
        """Return candidate counts by type and status."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT building_type, status, COUNT(*)
            FROM building_auto_candidates
            GROUP BY building_type, status
            '''
        )
        stats: Dict[str, int] = {}
        for building_type, status, count in cursor.fetchall():
            stats[f"{building_type}:{status}"] = int(count)
            stats[f"{building_type}:total"] = stats.get(f"{building_type}:total", 0) + int(count)
            stats[f"total:{status}"] = stats.get(f"total:{status}", 0) + int(count)
            stats["total"] = stats.get("total", 0) + int(count)
        conn.close()
        return stats

    def get_random_auto_candidates(self, building_type: str, *, limit: int = 25) -> List[AutoBuildCandidate]:
        """Return random available candidates for one building type."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT *
            FROM building_auto_candidates
            WHERE building_type = ?
              AND status = 'available'
            ORDER BY RANDOM()
            LIMIT ?
            ''',
            (str(building_type), int(limit)),
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._candidate_row_to_model(row) for row in rows]

    def get_auto_candidate(self, candidate_id: int) -> Optional[AutoBuildCandidate]:
        """Return one automatic build candidate."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM building_auto_candidates WHERE candidate_id = ?",
            (int(candidate_id),),
        )
        row = cursor.fetchone()
        conn.close()
        return self._candidate_row_to_model(row) if row else None

    def get_auto_extract_import_statuses(self) -> Dict[str, str]:
        """Return Geofabrik extract import status by extract id."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT extract_id, status FROM building_auto_extract_imports")
        rows = cursor.fetchall()
        conn.close()
        return {str(extract_id): str(status) for extract_id, status in rows}

    def record_auto_extract_import(
        self,
        *,
        extract_id: str,
        extract_name: str,
        url: str,
        status: str,
        bytes_downloaded: Optional[int] = None,
        inserted: int = 0,
        updated: int = 0,
        skipped: int = 0,
        accepted: int = 0,
        rejected: int = 0,
        reason: Optional[str] = None,
    ) -> None:
        """Record one Geofabrik extract import attempt."""
        now = ts()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO building_auto_extract_imports
            (extract_id, extract_name, url, status, bytes_downloaded, inserted, updated,
             skipped, accepted, rejected, reason, imported_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(extract_id) DO UPDATE SET
                extract_name = excluded.extract_name,
                url = excluded.url,
                status = excluded.status,
                bytes_downloaded = excluded.bytes_downloaded,
                inserted = excluded.inserted,
                updated = excluded.updated,
                skipped = excluded.skipped,
                accepted = excluded.accepted,
                rejected = excluded.rejected,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            ''',
            (
                str(extract_id),
                str(extract_name or extract_id),
                str(url or ""),
                str(status),
                bytes_downloaded,
                int(inserted),
                int(updated),
                int(skipped),
                int(accepted),
                int(rejected),
                _truncate_text(reason, 900) if reason else None,
                now,
                now,
            ),
        )
        conn.commit()
        conn.close()

    def purge_geofabrik_auto_candidates(self, *, include_used: bool = False) -> Dict[str, int]:
        """Remove imported Geofabrik candidates and reset extract import history."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT status, COUNT(*)
            FROM building_auto_candidates
            WHERE source = 'geofabrik'
            GROUP BY status
            '''
        )
        before = {f"candidates_{status}": int(count) for status, count in cursor.fetchall()}
        if include_used:
            cursor.execute("DELETE FROM building_auto_candidates WHERE source = 'geofabrik'")
        else:
            cursor.execute("DELETE FROM building_auto_candidates WHERE source = 'geofabrik' AND status != 'used'")
        deleted_candidates = int(cursor.rowcount if cursor.rowcount is not None else 0)
        cursor.execute("DELETE FROM building_auto_extract_imports")
        deleted_extracts = int(cursor.rowcount if cursor.rowcount is not None else 0)
        conn.commit()
        conn.close()
        return {
            **before,
            "deleted_candidates": deleted_candidates,
            "deleted_extract_imports": deleted_extracts,
        }

    def mark_auto_candidate(
        self,
        candidate_id: int,
        status: str,
        *,
        reason: Optional[str] = None,
        missionchief_building_id: Optional[int] = None,
    ) -> None:
        """Update candidate state after duplicate detection or build attempts."""
        now = ts()
        used_at = now if status == "used" else None
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            UPDATE building_auto_candidates
            SET status = ?,
                status_reason = ?,
                missionchief_building_id = COALESCE(?, missionchief_building_id),
                used_at = COALESCE(?, used_at),
                last_attempt_at = ?,
                updated_at = ?
            WHERE candidate_id = ?
            ''',
            (
                str(status),
                reason,
                missionchief_building_id,
                used_at,
                now,
                now,
                int(candidate_id),
            ),
        )
        conn.commit()
        conn.close()

    def get_auto_run(self, guild_id: int, run_date: str, building_type: str) -> Optional[Dict[str, Any]]:
        """Return an automatic build run for one guild/date/type."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT *
            FROM building_auto_runs
            WHERE guild_id = ?
              AND run_date = ?
              AND building_type = ?
            LIMIT 1
            ''',
            (int(guild_id), str(run_date), str(building_type)),
        )
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None

    def record_auto_run(
        self,
        *,
        guild_id: int,
        run_date: str,
        building_type: str,
        candidate_id: Optional[int],
        funds: Optional[int],
        funds_source: str,
        result: str,
        request_id: Optional[int] = None,
        missionchief_building_id: Optional[int] = None,
        reason: Optional[str] = None,
    ) -> None:
        """Persist the result of one automatic daily build slot."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO building_auto_runs
            (guild_id, run_date, building_type, candidate_id, funds, funds_source, result,
             request_id, missionchief_building_id, reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, run_date, building_type) DO UPDATE SET
                candidate_id = excluded.candidate_id,
                funds = excluded.funds,
                funds_source = excluded.funds_source,
                result = excluded.result,
                request_id = excluded.request_id,
                missionchief_building_id = excluded.missionchief_building_id,
                reason = excluded.reason,
                created_at = excluded.created_at
            ''',
            (
                int(guild_id),
                str(run_date),
                str(building_type),
                candidate_id,
                funds,
                str(funds_source),
                str(result),
                request_id,
                missionchief_building_id,
                reason,
                ts(),
            ),
        )
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

    def get_request_stats(
        self,
        guild_id: int,
        period_start_ts: Optional[int] = None,
        period_end_ts: Optional[int] = None,
    ) -> dict:
        """Public contract: building request statistics for reports."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        period_clause = ""
        period_params = []
        if period_start_ts is not None and period_end_ts is not None:
            period_clause = "AND updated_at >= ? AND updated_at < ?"
            period_params = [int(period_start_ts), int(period_end_ts)]

        cursor.execute(
            f'''
                SELECT status, COUNT(*)
                FROM building_requests
                WHERE guild_id = ?
                {period_clause}
                GROUP BY status
            ''',
            (guild_id, *period_params),
        )
        status_counts = dict(cursor.fetchall())

        cursor.execute(
            f'''
                SELECT building_type, COUNT(*)
                FROM building_requests
                WHERE guild_id = ?
                AND status = 'approved'
                {period_clause}
                GROUP BY building_type
            ''',
            (guild_id, *period_params),
        )
        by_type = dict(cursor.fetchall())

        cursor.execute(
            '''
                SELECT COUNT(*)
                FROM building_requests
                WHERE guild_id = ? AND status = 'pending'
            ''',
            (guild_id,),
        )
        pending = cursor.fetchone()[0]
        conn.close()

        return {
            "approved": status_counts.get("approved", 0),
            "denied": status_counts.get("denied", 0),
            "pending": pending,
            "by_type": by_type,
        }

    def get_admin_activity_stats(
        self,
        guild_id: int,
        period_start_ts: Optional[int] = None,
        period_end_ts: Optional[int] = None,
    ) -> dict:
        """Public contract: building admin activity statistics for reports."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        period_clause = ""
        period_params = []
        if period_start_ts is not None and period_end_ts is not None:
            period_clause = "AND timestamp >= ? AND timestamp < ?"
            period_params = [int(period_start_ts), int(period_end_ts)]

        cursor.execute(
            f'''
                SELECT COUNT(*)
                FROM building_actions
                WHERE guild_id = ?
                {period_clause}
            ''',
            (guild_id, *period_params),
        )
        total_actions = cursor.fetchone()[0]

        cursor.execute(
            f'''
                SELECT admin_username, COUNT(*) as count
                FROM building_actions
                WHERE guild_id = ?
                {period_clause}
                GROUP BY admin_username
                ORDER BY count DESC
                LIMIT 1
            ''',
            (guild_id, *period_params),
        )
        result = cursor.fetchone()
        conn.close()

        return {
            "building_reviews": total_actions,
            "most_active_admin": result[0] if result else "N/A",
            "most_active_admin_count": result[1] if result else 0,
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
        self.building_name = _clean_building_name(building_name)
        self.location_input = location_input
        self.coordinates = coordinates
        self.address = address
        self.country = country
        self.region = region
        self.maps_url = maps_url
        self.facility_warning = facility_warning
        self.notes = notes
        self.request_id = request_id

def building_request_from_row(row: Dict[str, Any]) -> BuildingRequest:
    """Build a request model from a stored database row."""
    return BuildingRequest(
        user_id=int(row["user_id"]),
        username=str(row["username"]),
        building_type=str(row["building_type"]),
        building_name=str(row["building_name"]),
        location_input=str(row["location_input"]),
        coordinates=row.get("coordinates"),
        address=row.get("address"),
        notes=row.get("notes"),
        request_id=int(row["request_id"]),
    )

# ---------- Views ----------

class StartView(discord.ui.View):
    def __init__(self, cog: "BuildingManager"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Request Building", style=discord.ButtonStyle.primary, custom_id="bm:start")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(BuildingRequestModal(self.cog))

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

    def __init__(self, cog: "BuildingManager", building_type: Optional[str] = None):
        super().__init__()
        self.cog = cog
        self.building_type = building_type

    async def on_submit(self, interaction: discord.Interaction):
        # Parse location
        await interaction.response.defer(ephemeral=True)
        
        location_input = str(self.location)
        location_details = await LocationParser.resolve_location(location_input)
        detected_type, rejection_reason = LocationParser.detect_supported_building_type(location_details)
        if not detected_type:
            await interaction.followup.send(
                (
                    "This Google Maps link was not accepted.\n\n"
                    f"Reason: {rejection_reason}\n\n"
                    "Use a link to a real hospital or prison/jail. Clinics, doctor offices, museums, "
                    "historic sites, courthouses, and police stations are not accepted."
                ),
                ephemeral=True,
            )
            return

        building_name = LocationParser.derive_building_name(detected_type, location_details)
        location_details.facility_warning = LocationParser.facility_warning(
            detected_type,
            building_name,
            location_details,
        )
        
        # Create request object
        req = BuildingRequest(
            user_id=interaction.user.id,
            username=str(interaction.user),
            building_type=detected_type,
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
        self.submitted = False

    def _disable_actions(self) -> None:
        """Prevent duplicate submit/cancel clicks after processing starts."""
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

    @staticmethod
    def _is_public_request_panel_message(message) -> bool:
        """Return True when a stale interaction points at the public request panel."""
        if message is None:
            return False
        for embed in getattr(message, "embeds", []) or []:
            if getattr(embed, "title", None) == REQUEST_PANEL_TITLE:
                return True
        return False

    async def send_summary(self, interaction: discord.Interaction):
        """Display the summary embed privately without touching the public request panel."""
        embed = self._create_embed(interaction.user)
        content = "Warning: Once submitted, you cannot edit this request!\n\nReview your request:"
        try:
            if interaction.response.is_done():
                await interaction.followup.send(content=content, embed=embed, view=self, ephemeral=True)
            else:
                await interaction.response.send_message(content=content, embed=embed, view=self, ephemeral=True)
        except Exception as exc:
            log.exception("BuildingManager failed to send private request summary: %r", exc)
            await send_ephemeral_followup(interaction, "Could not open the private request summary. Please try again.")

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

        if self.submitted:
            await interaction.response.send_message(
                "This request is already being processed in the background.",
                ephemeral=True,
            )
            return

        self.submitted = True
        self._disable_actions()
        submitted_embed = self._create_embed(interaction.user)
        submitted_embed.title = "Submitted Building Request"
        submitted_embed.color = discord.Color.green()
        submitted_embed.add_field(
            name="Status",
            value=(
                "BuildingManager is processing this request in the background. "
                "You will be notified when there is an update."
            ),
            inline=False,
        )
        submitted_content = "Request submitted. You can dismiss this private message."

        updated_review = False
        if not self._is_public_request_panel_message(getattr(interaction, "message", None)):
            with contextlib.suppress(Exception):
                await interaction.response.edit_message(
                    content=submitted_content,
                    embed=submitted_embed,
                    view=None,
                )
                updated_review = True
        if not updated_review:
            if not interaction.response.is_done():
                await interaction.response.send_message(submitted_content, ephemeral=True)
            else:
                await interaction.followup.send(submitted_content, ephemeral=True)

        self._schedule_background_submission(interaction, guild, interaction.user)
        return

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, custom_id="bm:cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.submitted:
            await interaction.response.send_message(
                "This request has already been submitted and cannot be cancelled from this panel.",
                ephemeral=True,
            )
            return
        self._disable_actions()
        cancelled_content = "Request cancelled. You can dismiss this private message."
        if not self._is_public_request_panel_message(getattr(interaction, "message", None)):
            with contextlib.suppress(Exception):
                await interaction.response.edit_message(content=cancelled_content, embed=None, view=None)
                return
        await send_ephemeral_followup(interaction, cancelled_content)
        return

    def _schedule_background_submission(
        self,
        interaction: discord.Interaction,
        guild: discord.Guild,
        requester: discord.abc.User,
    ) -> None:
        """Run the slow MissionChief work outside the interaction callback."""
        coroutine = self._finish_background_submission(interaction, guild, requester)
        loop = getattr(getattr(self.cog, "bot", None), "loop", None)
        if loop and loop.is_running():
            loop.create_task(coroutine)
        else:
            asyncio.create_task(coroutine)

    async def _finish_background_submission(
        self,
        interaction: discord.Interaction,
        guild: discord.Guild,
        requester: discord.abc.User,
    ) -> None:
        """Complete a submitted request and update the private response when possible."""
        try:
            result_message = await self.cog._process_discord_building_panel_request(
                guild,
                self.req,
                requester,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("BuildingManager background Discord request processing failed: %s", exc)
            result_message = (
                "Your building request hit an internal error while processing. "
                "Staff have been notified through the bot logs."
            )
        await send_ephemeral_followup(interaction, result_message)

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

    async def _delete_action_required_message(self, interaction: discord.Interaction):
        """Remove the admin action-required message after a successful auto-build."""
        with contextlib.suppress(Exception):
            if getattr(interaction, "message", None) is not None:
                await interaction.message.delete()

    async def on_error(
        self,
        interaction: discord.Interaction,
        error: Exception,
        item: discord.ui.Item,
    ) -> None:
        log.error(
            "BuildingManager admin decision view failed for request %s on item %s",
            getattr(self.req, "request_id", "unknown"),
            getattr(item, "custom_id", type(item).__name__),
            exc_info=(type(error), error, error.__traceback__),
        )
        await safe_ephemeral_complete(
            interaction,
            "This BuildingManager action hit an internal error. "
            f"Check the bot logs. Error: {_truncate_discord_text(error, 300)}",
        )

    @discord.ui.button(label="Auto build", style=discord.ButtonStyle.success, custom_id="bm:approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._is_admin(interaction):
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        for item in self.children:
            item.disabled = True
        with contextlib.suppress(Exception):
            await interaction.message.edit(view=self)
        await safe_ephemeral_complete(
            interaction,
            "Approval accepted. BuildingManager is creating the alliance building now. "
            "The final result will be posted here and in the configured log channel.",
        )
        guild = interaction.guild
        conf = await self.cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None
        minimum_funds = await self.cog._get_min_alliance_funds(guild)
        try:
            current_funds, funds_source = await asyncio.wait_for(
                self.cog._get_current_alliance_funds(),
                timeout=BUILDING_APPROVAL_FUNDS_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            current_funds, funds_source = None, "live funds check timed out"
        if not alliance_funds_allow_auto_build(current_funds, funds_source, minimum_funds):
            queue_message = await self.cog._queue_request_waiting_for_funds(
                guild=guild,
                req=self.req,
                requester_id=self.requester_id,
                admin_user=interaction.user,
                funds=current_funds,
                source=funds_source,
                minimum=minimum_funds,
                log_channel=log_channel,
            )
            with contextlib.suppress(Exception):
                await interaction.message.delete()
            await safe_ephemeral_complete(interaction, queue_message)
            return

        try:
            create_result = await asyncio.wait_for(
                self.cog._create_alliance_building_browser(self.req),
                timeout=BUILDING_APPROVAL_CREATE_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            try:
                create_result = await asyncio.wait_for(
                    self.cog._find_created_alliance_building_browser(
                        self.req,
                        reason=(
                            "MissionChief building creation timed out, but the created building "
                            "was found afterwards."
                        ),
                    ),
                    timeout=BUILDING_APPROVAL_RECOVERY_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                create_result = BuildingCreateResult(
                    False,
                    "MissionChief building creation timed out and the recovery lookup also timed out. "
                    "No post-creation automation was queued.",
                )
        if building_create_result_needs_recovery(create_result):
            try:
                recovered_result = await asyncio.wait_for(
                    self.cog._find_created_alliance_building_browser(
                        self.req,
                        reason=(
                            "MissionChief building creation returned a timeout, but the created building "
                            "was found afterwards."
                        ),
                    ),
                    timeout=BUILDING_APPROVAL_RECOVERY_TIMEOUT_SECONDS,
                )
                if recovered_result.ok:
                    create_result = recovered_result
            except asyncio.TimeoutError:
                pass
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

        if create_result.ok:
            await self._delete_action_required_message(interaction)

        user = guild.get_member(self.requester_id) if guild and self.requester_id else None
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
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await user.send(_truncate_discord_text(ok_text, 1900))

        await self.cog._send_building_request_game_update(
            self.req,
            subject="Building request approved",
            body=self.cog._build_game_approval_message(
                self.req,
                status=(
                    "Your building has been created in MissionChief."
                    if create_result.ok
                    else "Your request was approved, but staff need to complete the build manually."
                ),
            ),
        )

        if log_channel:
            emb = discord.Embed(
                title="Building request approved and created" if create_result.ok else "Building request approved - manual creation needed",
                color=discord.Color.green() if create_result.ok else discord.Color.orange(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = self.cog._requester_label(guild, self.requester_id, self.req.username)
            emb.add_field(name="Requester", value=requester, inline=False)
            emb.add_field(
                name="Building",
                value=_truncate_discord_text(f"{self.req.building_type} - {self.req.building_name}"),
                inline=False,
            )
            if self.req.coordinates:
                emb.add_field(name="Coordinates", value=_truncate_discord_text(self.req.coordinates, 200), inline=True)
            if self.req.maps_url:
                emb.add_field(name="Maps", value=_truncate_discord_text(f"[Open]({self.req.maps_url})"), inline=True)
            region_text = ", ".join(part for part in (self.req.region, self.req.country) if part)
            if region_text:
                emb.add_field(name="Country / Region", value=_truncate_discord_text(region_text, 200), inline=True)
            emb.add_field(name="Approved by", value=f"{interaction.user.mention} ({interaction.user.id})", inline=False)
            emb.add_field(
                name="Auto Creation",
                value=_truncate_discord_text("Created in MissionChief" if create_result.ok else create_result.reason, 900),
                inline=False,
            )
            if automation_message:
                emb.add_field(
                    name="Post-Creation Automation",
                    value=_truncate_discord_text(automation_message, 900),
                    inline=False,
                )
            if create_result.status is not None:
                emb.add_field(name="MissionChief HTTP Status", value=str(create_result.status), inline=True)
            emb.add_field(name="Request ID", value=str(self.req.request_id), inline=True)
            await log_channel.send(embed=emb)

        if create_result.ok:
            if automation_message:
                await safe_ephemeral_complete(
                    interaction,
                    f"Request approved and alliance building created. {automation_message}",
                )
            else:
                await safe_ephemeral_complete(interaction, "Request approved and alliance building created.")
        else:
            await safe_ephemeral_complete(
                interaction,
                f"Request approved, but automatic creation failed: {create_result.reason}",
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

        user = guild.get_member(self.requester_id) if self.requester_id else None
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

        await self.cog._send_building_request_game_update(
            self.req,
            subject="Building request rejected",
            body=self.cog._build_game_rejection_message(self.req, reason=reason),
        )

        if log_channel:
            emb = discord.Embed(
                title="Building request denied",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = self.cog._requester_label(guild, self.requester_id, self.req.username)
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

        user = guild.get_member(self.requester_id) if self.requester_id else None
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

        await self.cog._send_building_request_game_update(
            self.req,
            subject="Building request rejected",
            body=self.cog._build_game_rejection_message(self.req, reason=reason_text),
        )

        if log_channel:
            emb = discord.Embed(
                title="Building request denied",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = self.cog._requester_label(guild, self.requester_id, self.req.username)
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
            "min_alliance_funds": ALLIANCE_BUILDING_MIN_FUNDS,
            "board_poll_enabled": True,
            "board_thread_id": BOARD_THREAD_ID,
            "board_last_seen_post_id": None,
            "board_processed_post_ids": [],
            "board_pending_deletions": [],
            "board_guide_enabled": True,
            "board_guide_thread_id": None,
            "board_guide_post_id": None,
            "board_guide_content_hash": None,
            "board_auto_accept_enabled": True,
            "auto_candidate_build_enabled": False,
            "auto_candidate_min_funds": AUTO_CANDIDATE_MIN_FUNDS,
            "auto_candidate_time": AUTO_CANDIDATE_DEFAULT_TIME,
            "auto_candidate_timezone": AUTO_CANDIDATE_DEFAULT_TIMEZONE,
            "auto_candidate_duplicate_radius_m": AUTO_CANDIDATE_DUPLICATE_RADIUS_METERS,
            "auto_candidate_refill_enabled": True,
            "auto_candidate_refill_min_available": AUTO_CANDIDATE_REFILL_MIN_AVAILABLE,
            "auto_candidate_refill_regions_per_run": AUTO_CANDIDATE_REFILL_REGIONS_PER_RUN,
            "auto_candidate_refill_max_extract_mb": AUTO_CANDIDATE_REFILL_MAX_EXTRACT_BYTES // (1024 * 1024),
            "auto_candidate_refill_next_region_index": 0,
        }
        default_global = {
            "google_api_key": None,
            "geocode_maps_api_key": None,
            "default_request_panel_message_id": None,
            "board_thread_states": {},
        }
        self.config.register_guild(**default_guild)
        self.config.register_global(**default_global)

        # Initialize database
        from redbot.core import data_manager
        db_path = str(data_manager.cog_data_path(self) / "building_manager.db")
        self.db = BuildingDatabase(db_path)

        self._panel_task = None
        self._automation_task = None
        self._creation_queue_task = None
        self._board_poll_task = None
        self._board_guide_task = None
        self._board_cleanup_task = None
        self._auto_candidate_task = None
        self._browser_lock = asyncio.Lock()
        self._persistent_view_registered = False
        self._register_persistent_views()
        self._start_panel_task()
        self._start_automation_task()
        self._start_creation_queue_task()
        self._start_board_tasks()
        self._start_auto_candidate_task()

    def cog_unload(self):  # <-- Let op: 4 spaties inspringing, zelfde niveau als __init__
        if getattr(self, "_panel_task", None):
            self._panel_task.cancel()
        if getattr(self, "_automation_task", None):
            self._automation_task.cancel()
        if getattr(self, "_creation_queue_task", None):
            self._creation_queue_task.cancel()
        if getattr(self, "_board_poll_task", None):
            self._board_poll_task.cancel()
        if getattr(self, "_board_guide_task", None):
            self._board_guide_task.cancel()
        if getattr(self, "_board_cleanup_task", None):
            self._board_cleanup_task.cancel()
        if getattr(self, "_auto_candidate_task", None):
            self._auto_candidate_task.cancel()

    def get_request_stats(
        self,
        guild_id: int,
        period_start_ts: Optional[int] = None,
        period_end_ts: Optional[int] = None,
    ) -> dict:
        """Public contract for report cogs to read building request stats."""
        return self.db.get_request_stats(
            guild_id,
            period_start_ts=period_start_ts,
            period_end_ts=period_end_ts,
        )

    def get_admin_activity_stats(
        self,
        guild_id: int,
        period_start_ts: Optional[int] = None,
        period_end_ts: Optional[int] = None,
    ) -> dict:
        """Public contract for report cogs to read building admin activity stats."""
        return self.db.get_admin_activity_stats(
            guild_id,
            period_start_ts=period_start_ts,
            period_end_ts=period_end_ts,
        )

    async def cog_load(self):
        """Register persistent views and ensure the default request panel exists."""
        self._register_persistent_views()
        self._start_panel_task()
        self._start_automation_task()
        self._start_creation_queue_task()
        self._start_board_tasks()
        self._start_auto_candidate_task()

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

    def _start_creation_queue_task(self):
        """Start the approved-but-waiting-for-funds creation worker."""
        if self._creation_queue_task and not self._creation_queue_task.done():
            return
        self._creation_queue_task = self.bot.loop.create_task(self._building_creation_queue_loop())

    def _start_board_tasks(self):
        """Start MissionChief board polling, guide sync, and cleanup workers."""
        if not self._board_poll_task or self._board_poll_task.done():
            self._board_poll_task = self.bot.loop.create_task(self._board_poll_loop())
        if not self._board_guide_task or self._board_guide_task.done():
            self._board_guide_task = self.bot.loop.create_task(self._board_guide_loop())
        if not self._board_cleanup_task or self._board_cleanup_task.done():
            self._board_cleanup_task = self.bot.loop.create_task(self._board_cleanup_loop())

    def _start_auto_candidate_task(self):
        """Start the daily candidate auto-build worker."""
        if self._auto_candidate_task and not self._auto_candidate_task.done():
            return
        self._auto_candidate_task = self.bot.loop.create_task(self._auto_candidate_build_loop())

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

    async def _get_google_api_key(self) -> Optional[str]:
        """Return the optional Google geocoding key for BuildingManager."""
        try:
            value = await self.config.google_api_key()
        except Exception:
            return None
        value = str(value or "").strip()
        return value or None

    async def _get_geocode_maps_api_key(self) -> Optional[str]:
        """Return the optional geocode.maps.co key, reusing EventPinger when available."""
        try:
            value = str(await self.config.geocode_maps_api_key() or "").strip()
        except Exception:
            value = ""
        if value:
            return value

        get_cog = getattr(self.bot, "get_cog", None)
        if not callable(get_cog):
            return None
        eventpinger = get_cog("EventPinger")
        method = getattr(eventpinger, "_get_geocode_api_key", None) if eventpinger else None
        if not callable(method):
            return None
        try:
            result = method()
            if asyncio.iscoroutine(result):
                result = await result
        except Exception:
            return None
        result = str(result or "").strip()
        return result or None

    async def _resolve_building_location(self, location_input: str) -> LocationDetails:
        """Resolve a request location using all configured geocoding fallbacks."""
        return await LocationParser.resolve_location(
            location_input,
            google_key=await self._get_google_api_key(),
            mapsco_key=await self._get_geocode_maps_api_key(),
        )

    def _message_manager(self):
        """Return MessageManager when it exposes the MissionChief send contract."""
        message_manager = self.bot.get_cog("MessageManager")
        if not message_manager or not hasattr(message_manager, "_send_message_and_link"):
            return None
        return message_manager

    @staticmethod
    def _extract_contribution_rate(member: Optional[dict]) -> Optional[float]:
        """Read a contribution rate from a MembersScraper member shape."""
        if not member:
            return None
        for key in ("contribution_rate", "contribution", "tax_rate", "tax"):
            value = member.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    async def _get_board_request_contribution_rate(self, post: BoardBuildingPost) -> Tuple[Optional[float], str]:
        """Return the latest known alliance contribution rate for a board author."""
        get_cog = getattr(self.bot, "get_cog", None)
        if not callable(get_cog):
            return None, "Bot cog registry is unavailable"
        members_scraper = get_cog("MembersScraper") or get_cog("membersscraper")
        if not members_scraper:
            return None, "MembersScraper is not loaded"

        author_id = str(post.author_id or "").strip()
        if author_id and hasattr(members_scraper, "get_member_snapshot"):
            try:
                snapshot = await members_scraper.get_member_snapshot(author_id)
            except Exception:
                snapshot = None
            rate = self._extract_contribution_rate(snapshot)
            if rate is not None:
                return rate, f"MembersScraper snapshot for MC ID {author_id}"

        if hasattr(members_scraper, "get_members"):
            try:
                members = await members_scraper.get_members()
            except Exception:
                members = []
            author_name = str(post.author_name or "").casefold()
            for member in members or []:
                ids = {
                    str(member.get(key) or "").strip()
                    for key in ("member_id", "mc_user_id", "user_id", "id")
                }
                names = {
                    str(member.get(key) or "").strip().casefold()
                    for key in ("username", "name", "mc_username", "mc_name")
                }
                if (author_id and author_id in ids) or (author_name and author_name in names):
                    rate = self._extract_contribution_rate(member)
                    if rate is not None:
                        return rate, "MembersScraper current members"

        return None, "MembersScraper has no usable contribution snapshot"

    async def _get_discord_request_contribution_rate(
        self,
        user: discord.abc.User,
    ) -> Tuple[Optional[float], str]:
        """Return the latest known alliance contribution rate for a Discord requester."""
        get_cog = getattr(self.bot, "get_cog", None)
        if not callable(get_cog):
            return None, "Bot cog registry is unavailable"

        member_sync = get_cog("MemberSync") or get_cog("membersync")
        members_scraper = get_cog("MembersScraper") or get_cog("membersscraper")
        if not member_sync:
            return None, "MemberSync is not loaded"
        if not members_scraper:
            return None, "MembersScraper is not loaded"

        link = None
        method = getattr(member_sync, "get_link_for_discord", None)
        if callable(method):
            try:
                link = method(int(user.id))
                if asyncio.iscoroutine(link):
                    link = await link
            except Exception:
                link = None
        if not link:
            return None, "No approved MemberSync link for this Discord user"

        mc_user_id = str(link.get("mc_user_id") or link.get("user_id") or "").strip()
        if not mc_user_id:
            return None, "MemberSync link has no MissionChief ID"

        if hasattr(members_scraper, "get_member_snapshot"):
            try:
                snapshot = await members_scraper.get_member_snapshot(mc_user_id)
            except Exception:
                snapshot = None
            rate = self._extract_contribution_rate(snapshot)
            if rate is not None:
                return rate, f"MembersScraper snapshot for MC ID {mc_user_id}"

        if hasattr(members_scraper, "get_members"):
            try:
                members = await members_scraper.get_members()
            except Exception:
                members = []
            for member in members or []:
                ids = {
                    str(member.get(key) or "").strip()
                    for key in ("member_id", "mc_user_id", "user_id", "id")
                }
                if mc_user_id in ids:
                    rate = self._extract_contribution_rate(member)
                    if rate is not None:
                        return rate, "MembersScraper current members"

        return None, f"MembersScraper has no contribution snapshot for MC ID {mc_user_id}"

    async def _board_auto_accept_enabled(self, guild: discord.Guild) -> bool:
        """Return whether MissionChief board requests may auto-build."""
        try:
            conf = await self.config.guild(guild).all()
        except Exception:
            return False
        return bool(conf.get("board_auto_accept_enabled", True))

    def _missionchief_board_request_username(self, req: BuildingRequest) -> Optional[str]:
        """Return the MissionChief username for board requests, avoiding Discord-name guesses."""
        try:
            user_id = int(req.user_id or 0)
        except (TypeError, ValueError):
            user_id = 0
        if user_id:
            return None
        username = str(req.username or "").strip()
        if not username or username.casefold() == "unknown":
            return None
        return username

    def _build_game_approval_message(self, req: BuildingRequest, *, status: str) -> str:
        """Build a MissionChief DM body for approved building requests."""
        lines = [
            f"Hello {req.username},",
            "",
            "Your building request has been approved.",
            "",
            f"Building: {req.building_type} - {req.building_name}",
            f"Status: {status}",
        ]
        lines.extend(
            [
                "",
                "Thank you for helping improve the alliance infrastructure.",
                "",
                "Fire & Rescue Academy",
            ]
        )
        return "\n".join(lines)

    def _build_game_rejection_message(self, req: BuildingRequest, *, reason: str) -> str:
        """Build a MissionChief DM body for rejected building requests."""
        lines = [
            f"Hello {req.username},",
            "",
            "Your building request has been rejected.",
            "",
            f"Building: {req.building_type} - {req.building_name}",
            f"Reason: {reason}",
        ]
        if req.request_id:
            lines.append(f"Request ID: {req.request_id}")
        lines.extend(
            [
                "",
                "You can submit a new request with a corrected Google Maps link if needed.",
                "",
                "Fire & Rescue Academy",
            ]
        )
        return "\n".join(lines)

    async def _send_building_request_game_update(
        self,
        req: BuildingRequest,
        *,
        subject: str,
        body: str,
    ) -> Optional[dict]:
        """Send a MissionChief DM for board requests without breaking the admin flow."""
        username = self._missionchief_board_request_username(req)
        if not username:
            return None
        message_manager = self._message_manager()
        if not message_manager:
            log.info("BuildingManager skipped MissionChief DM for %s: MessageManager is not loaded", username)
            return None
        try:
            result = await message_manager._send_message_and_link(username, subject, body)
        except Exception as exc:
            log.warning("BuildingManager could not send MissionChief DM to %s: %s", username, exc, exc_info=True)
            return None
        if not result.get("ok"):
            log.warning(
                "BuildingManager MissionChief DM to %s was not confirmed: %s",
                username,
                result.get("reason"),
            )
        return result

    def _requester_label(self, guild: Optional[discord.Guild], user_id: Optional[int], username: str) -> str:
        """Return a human requester label for Discord and MissionChief board users."""
        try:
            numeric_id = int(user_id or 0)
        except (TypeError, ValueError):
            numeric_id = 0
        if numeric_id > 0:
            member = guild.get_member(numeric_id) if guild else None
            if member:
                return f"{member.mention} ({member.id})"
            return f"<@{numeric_id}> ({numeric_id})"
        return f"{username or 'Unknown'} (MissionChief board)"

    async def _resolve_channel(self, guild: discord.Guild, channel_id: Optional[int]) -> Optional[discord.abc.Messageable]:
        """Resolve a configured Discord channel from cache or API."""
        if not channel_id:
            return None
        try:
            numeric_id = int(channel_id)
        except (TypeError, ValueError):
            return None
        channel = guild.get_channel(numeric_id)
        if channel:
            return channel
        fetch_channel = getattr(self.bot, "fetch_channel", None)
        if fetch_channel:
            with contextlib.suppress(Exception):
                return await fetch_channel(numeric_id)
        return None

    def _add_request_location_fields(
        self,
        embed: discord.Embed,
        req: BuildingRequest,
        *,
        warning_title: str = "Facility Check Warning",
    ) -> None:
        """Add normalized location fields for admin-facing request embeds."""
        if req.coordinates:
            embed.add_field(name="Coordinates", value=req.coordinates, inline=True)
        else:
            embed.add_field(name="Coordinates", value="Not detected", inline=True)

        region_text = ", ".join(part for part in (req.region, req.country) if part)
        if region_text:
            embed.add_field(name="Country / Region", value=_truncate_discord_text(region_text, 200), inline=True)
        if req.address:
            embed.add_field(name="Address", value=_truncate_discord_text(req.address, 300), inline=False)
        if req.maps_url:
            embed.add_field(name="Maps URL", value=f"[Open in Google Maps]({req.maps_url})", inline=False)
        if req.facility_warning:
            embed.add_field(name=warning_title, value=f"Warning: {req.facility_warning}", inline=False)

    def _store_building_request(self, guild: discord.Guild, req: BuildingRequest) -> int:
        """Persist a building request and attach the request id to the model."""
        request_id = self.db.add_request(
            guild_id=guild.id,
            user_id=int(req.user_id or 0),
            username=req.username,
            building_type=req.building_type,
            building_name=req.building_name,
            location_input=req.location_input,
            coordinates=req.coordinates,
            address=req.address,
            notes=req.notes,
        )
        req.request_id = int(request_id)
        return int(request_id)

    async def _submit_building_request_to_admins(
        self,
        guild: discord.Guild,
        req: BuildingRequest,
        *,
        source: str,
        board_post: Optional[BoardBuildingPost] = None,
    ) -> int:
        """Store a building request and send it to the configured admin channel."""
        conf = await self.config.guild(guild).all()
        admin_channel = await self._resolve_channel(guild, conf.get("admin_channel_id"))
        log_channel = await self._resolve_channel(guild, conf.get("log_channel_id"))
        if not admin_channel:
            raise RuntimeError("Admin channel must be configured before board requests can be processed.")

        request_id = self._store_building_request(guild, req)

        requester = self._requester_label(guild, req.user_id, req.username)

        embed = discord.Embed(
            title="New Building Request",
            color=discord.Color.yellow(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Requester", value=requester, inline=False)
        embed.add_field(name="Source", value=source, inline=True)
        if board_post:
            embed.add_field(name="Board Post", value=f"#{board_post.post_id}", inline=True)
            if board_post.created_at:
                embed.add_field(name="Posted at", value=board_post.created_at, inline=True)
        embed.add_field(name="Building Type", value=req.building_type, inline=True)
        embed.add_field(name="Building Name", value=req.building_name, inline=True)
        embed.add_field(name="Location Input", value=req.location_input[:100], inline=False)
        self._add_request_location_fields(embed, req)
        if req.notes:
            embed.add_field(name="Notes", value=req.notes[:300], inline=False)
        embed.set_footer(text=f"Request ID: {request_id}")

        await admin_channel.send(embed=embed, view=AdminDecisionView(self, requester_id=int(req.user_id or 0), req=req))

        log_embed = discord.Embed(
            title="Building request submitted",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        log_embed.add_field(name="Requester", value=requester, inline=False)
        log_embed.add_field(name="Source", value=source, inline=True)
        log_embed.add_field(name="Building", value=f"{req.building_type} - {req.building_name}", inline=False)
        if req.coordinates:
            log_embed.add_field(name="Coordinates", value=req.coordinates, inline=True)
        if req.maps_url:
            log_embed.add_field(name="Maps", value=f"[Open]({req.maps_url})", inline=True)
        log_embed.add_field(name="Request ID", value=str(request_id), inline=True)
        if log_channel:
            await log_channel.send(embed=log_embed)
        return int(request_id)

    async def _create_and_queue_approved_building(
        self,
        guild: discord.Guild,
        req: BuildingRequest,
    ) -> Tuple[BuildingCreateResult, Optional[str]]:
        """Create an approved building and queue post-creation automation when possible."""
        try:
            create_result = await asyncio.wait_for(
                self._create_alliance_building_browser(req),
                timeout=BUILDING_APPROVAL_CREATE_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            try:
                create_result = await asyncio.wait_for(
                    self._find_created_alliance_building_browser(
                        req,
                        reason=(
                            "MissionChief building creation timed out, but the created building "
                            "was found afterwards."
                        ),
                    ),
                    timeout=BUILDING_APPROVAL_RECOVERY_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                create_result = BuildingCreateResult(
                    False,
                    "MissionChief building creation timed out and the recovery lookup also timed out. "
                    "No post-creation automation was queued.",
                )

        if building_create_result_needs_recovery(create_result):
            try:
                recovered_result = await asyncio.wait_for(
                    self._find_created_alliance_building_browser(
                        req,
                        reason=(
                            "MissionChief building creation returned a timeout, but the created building "
                            "was found afterwards."
                        ),
                    ),
                    timeout=BUILDING_APPROVAL_RECOVERY_TIMEOUT_SECONDS,
                )
                if recovered_result.ok:
                    create_result = recovered_result
            except asyncio.TimeoutError:
                pass

        automation_message = None
        if create_result.ok:
            building_id = create_result.building_id
            if building_id:
                job_id = self.db.add_or_update_automation_job(
                    request_id=int(req.request_id),
                    guild_id=guild.id,
                    building_id=building_id,
                    building_type=req.building_type,
                    building_name=req.building_name,
                )
                automation_message = (
                    f"Queued post-creation automation for MissionChief building `{building_id}`: "
                    f"tax {ALLIANCE_BUILDING_TARGET_TAX}%, max level, and allowed extensions."
                )
                self.bot.loop.create_task(self._process_building_automation_job(job_id))
            else:
                automation_message = (
                    "Created in MissionChief, but the building ID was not detected. "
                    "Post-creation automation was not queued."
                )
        return create_result, automation_message

    async def _send_board_auto_build_log(
        self,
        guild: discord.Guild,
        log_channel: Optional[discord.abc.Messageable],
        req: BuildingRequest,
        post: BoardBuildingPost,
        *,
        contribution_rate: Optional[float],
        create_result: Optional[BuildingCreateResult] = None,
        automation_message: Optional[str] = None,
        status: str,
        color: discord.Color,
    ) -> None:
        """Log one automatic board decision to Discord."""
        if not log_channel:
            return
        embed = discord.Embed(
            title=status,
            color=color,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Requester", value=f"{post.author_name} ({post.author_id or 'unknown ID'})", inline=False)
        if contribution_rate is not None:
            embed.add_field(name="Contribution", value=f"{contribution_rate:.1f}%", inline=True)
        embed.add_field(name="Board Post", value=f"#{post.post_id}", inline=True)
        embed.add_field(name="Request ID", value=str(req.request_id), inline=True)
        embed.add_field(
            name="Building",
            value=_truncate_discord_text(f"{req.building_type} - {req.building_name}"),
            inline=False,
        )
        self._add_request_location_fields(embed, req)
        if create_result:
            embed.add_field(
                name="Auto Creation",
                value=_truncate_discord_text(
                    "Created in MissionChief" if create_result.ok else create_result.reason,
                    900,
                ),
                inline=False,
            )
            if create_result.status is not None:
                embed.add_field(name="MissionChief HTTP Status", value=str(create_result.status), inline=True)
        if automation_message:
            embed.add_field(
                name="Post-Creation Automation",
                value=_truncate_discord_text(automation_message, 900),
                inline=False,
            )
        await log_channel.send(embed=embed)

    async def _send_discord_auto_build_log(
        self,
        guild: discord.Guild,
        log_channel: Optional[discord.abc.Messageable],
        req: BuildingRequest,
        requester: discord.abc.User,
        *,
        contribution_rate: Optional[float],
        create_result: Optional[BuildingCreateResult] = None,
        automation_message: Optional[str] = None,
        status: str,
        color: discord.Color,
    ) -> None:
        """Log one automatic Discord request decision to Discord."""
        if not log_channel:
            return
        embed = discord.Embed(
            title=status,
            color=color,
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Requester", value=self._requester_label(guild, req.user_id, req.username), inline=False)
        embed.add_field(name="Source", value="Discord request panel", inline=True)
        if contribution_rate is not None:
            embed.add_field(name="Contribution", value=f"{contribution_rate:.1f}%", inline=True)
        embed.add_field(name="Request ID", value=str(req.request_id), inline=True)
        embed.add_field(
            name="Building",
            value=_truncate_discord_text(f"{req.building_type} - {req.building_name}"),
            inline=False,
        )
        self._add_request_location_fields(embed, req)
        if create_result:
            embed.add_field(
                name="Auto Creation",
                value=_truncate_discord_text(
                    "Created in MissionChief" if create_result.ok else create_result.reason,
                    900,
                ),
                inline=False,
            )
            if create_result.status is not None:
                embed.add_field(name="MissionChief HTTP Status", value=str(create_result.status), inline=True)
        if automation_message:
            embed.add_field(
                name="Post-Creation Automation",
                value=_truncate_discord_text(automation_message, 900),
                inline=False,
            )
        await log_channel.send(embed=embed)

    async def _process_discord_building_panel_request(
        self,
        guild: discord.Guild,
        req: BuildingRequest,
        requester: discord.abc.User,
    ) -> str:
        """Process a Discord panel request after the interaction is acknowledged."""
        conf = await self.config.guild(guild).all()
        admin_channel_id = conf.get("admin_channel_id")
        log_channel_id = conf.get("log_channel_id")

        if not admin_channel_id or not log_channel_id:
            return "Admin/Log channels are not configured yet. Ask an admin to use [p]buildset."

        admin_channel = await self._resolve_channel(guild, admin_channel_id)
        log_channel = await self._resolve_channel(guild, log_channel_id)
        if not admin_channel or not log_channel:
            return "One or more configured BuildingManager channels could not be found."

        contribution_rate, contribution_source = await self._get_discord_request_contribution_rate(requester)
        if contribution_rate is None:
            req.notes = (
                f"{req.notes or ''}\n"
                f"Auto-accept skipped: contribution rate unknown ({contribution_source})."
            ).strip()
            request_id = await self._submit_building_request_to_admins(
                guild,
                req,
                source="Discord request - contribution unknown",
            )
            return (
                "Your request was submitted to admins for review because your latest alliance "
                f"donation could not be verified. Request ID: {request_id}."
            )

        if contribution_rate < BUILDING_BOARD_MIN_AUTO_ACCEPT_TAX:
            return await self._reject_discord_request_for_low_tax(
                guild,
                req,
                requester,
                contribution_rate=contribution_rate,
                log_channel=log_channel,
            )

        return await self._auto_accept_discord_building_request(
            guild,
            req,
            requester,
            contribution_rate=contribution_rate,
            log_channel=log_channel,
        )

    async def _reject_discord_request_for_low_tax(
        self,
        guild: discord.Guild,
        req: BuildingRequest,
        requester: discord.abc.User,
        *,
        contribution_rate: float,
        log_channel: Optional[discord.abc.Messageable],
    ) -> str:
        """Store and reject a Discord request when the requester has insufficient tax."""
        request_id = self._store_building_request(guild, req)
        reason = (
            f"Latest alliance donation is {contribution_rate:.1f}%, below the required "
            f"{BUILDING_BOARD_MIN_AUTO_ACCEPT_TAX:.1f}% minimum."
        )
        self.db.update_request_status(request_id, "denied")
        self.db.add_action(
            request_id=request_id,
            guild_id=guild.id,
            admin_user_id=None,
            admin_username="BuildingManager",
            action_type="auto_denied_low_tax",
            denial_reason=reason,
        )
        await self._send_discord_auto_build_log(
            guild,
            log_channel,
            req,
            requester,
            contribution_rate=contribution_rate,
            status="Discord building request rejected",
            color=discord.Color.orange(),
        )
        return (
            "Your building request was rejected automatically because your latest known alliance "
            f"donation is {contribution_rate:.1f}%. The minimum is "
            f"{BUILDING_BOARD_MIN_AUTO_ACCEPT_TAX:.1f}%."
        )

    async def _auto_accept_discord_building_request(
        self,
        guild: discord.Guild,
        req: BuildingRequest,
        requester: discord.abc.User,
        *,
        contribution_rate: float,
        log_channel: Optional[discord.abc.Messageable],
    ) -> str:
        """Automatically approve and build a Discord request."""
        request_id = self._store_building_request(guild, req)
        minimum_funds = await self._get_min_alliance_funds(guild)
        try:
            current_funds, funds_source = await asyncio.wait_for(
                self._get_current_alliance_funds(),
                timeout=BUILDING_APPROVAL_FUNDS_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            current_funds, funds_source = None, "live funds check timed out"

        if not alliance_funds_allow_auto_build(current_funds, funds_source, minimum_funds):
            return await self._queue_request_waiting_for_funds(
                guild=guild,
                req=req,
                requester_id=int(req.user_id or 0),
                admin_user=None,
                funds=current_funds,
                source=funds_source,
                minimum=minimum_funds,
                log_channel=log_channel,
                record_approval=False,
            )

        create_result, automation_message = await self._create_and_queue_approved_building(guild, req)
        final_status = "created" if create_result.ok else "approved_pending_manual"
        self.db.update_request_status(request_id, final_status)
        self.db.add_action(
            request_id=request_id,
            guild_id=guild.id,
            admin_user_id=None,
            admin_username="BuildingManager",
            action_type="auto_approved",
            previous_values=f"Discord request contribution: {contribution_rate:.1f}%",
        )
        self.db.add_action(
            request_id=request_id,
            guild_id=guild.id,
            admin_user_id=None,
            admin_username="BuildingManager",
            action_type="created" if create_result.ok else "create_failed",
            previous_values=None if create_result.ok else create_result.reason[:900],
        )
        await self._send_discord_auto_build_log(
            guild,
            log_channel,
            req,
            requester,
            contribution_rate=contribution_rate,
            create_result=create_result,
            automation_message=automation_message,
            status=(
                "Discord building request auto-built"
                if create_result.ok
                else "Discord building request needs manual creation"
            ),
            color=discord.Color.green() if create_result.ok else discord.Color.orange(),
        )
        if create_result.ok:
            return "Your building request was approved and automatically created in MissionChief."
        return f"Your building request was approved, but automatic creation needs staff follow-up: {create_result.reason}"

    async def _reject_board_request_for_low_tax(
        self,
        guild: discord.Guild,
        req: BuildingRequest,
        post: BoardBuildingPost,
        *,
        contribution_rate: float,
        log_channel: Optional[discord.abc.Messageable],
    ) -> int:
        """Store and reject a board request when the requester has insufficient tax."""
        request_id = self._store_building_request(guild, req)
        reason = (
            f"Latest alliance donation is {contribution_rate:.1f}%, below the required "
            f"{BUILDING_BOARD_MIN_AUTO_ACCEPT_TAX:.1f}% minimum."
        )
        self.db.update_request_status(request_id, "denied")
        self.db.add_action(
            request_id=request_id,
            guild_id=guild.id,
            admin_user_id=None,
            admin_username="BuildingManager",
            action_type="auto_denied_low_tax",
            denial_reason=reason,
        )
        await self._send_building_request_game_update(
            req,
            subject="Building request rejected",
            body=self._build_game_rejection_message(req, reason=reason),
        )
        await self._send_board_auto_build_log(
            guild,
            log_channel,
            req,
            post,
            contribution_rate=contribution_rate,
            status="MissionChief board building request rejected",
            color=discord.Color.orange(),
        )
        return request_id

    async def _auto_accept_board_building_request(
        self,
        guild: discord.Guild,
        req: BuildingRequest,
        post: BoardBuildingPost,
        *,
        contribution_rate: float,
        log_channel: Optional[discord.abc.Messageable],
    ) -> str:
        """Automatically approve and build a board request."""
        request_id = self._store_building_request(guild, req)
        minimum_funds = await self._get_min_alliance_funds(guild)
        try:
            current_funds, funds_source = await asyncio.wait_for(
                self._get_current_alliance_funds(),
                timeout=BUILDING_APPROVAL_FUNDS_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            current_funds, funds_source = None, "live funds check timed out"

        if not alliance_funds_allow_auto_build(current_funds, funds_source, minimum_funds):
            message = await self._queue_request_waiting_for_funds(
                guild=guild,
                req=req,
                requester_id=0,
                admin_user=None,
                funds=current_funds,
                source=funds_source,
                minimum=minimum_funds,
                log_channel=log_channel,
                record_approval=False,
            )
            return message

        create_result, automation_message = await self._create_and_queue_approved_building(guild, req)
        final_status = "created" if create_result.ok else "approved_pending_manual"
        self.db.update_request_status(request_id, final_status)
        self.db.add_action(
            request_id=request_id,
            guild_id=guild.id,
            admin_user_id=None,
            admin_username="BuildingManager",
            action_type="auto_approved",
            previous_values=f"Contribution: {contribution_rate:.1f}%",
        )
        self.db.add_action(
            request_id=request_id,
            guild_id=guild.id,
            admin_user_id=None,
            admin_username="BuildingManager",
            action_type="created" if create_result.ok else "create_failed",
            previous_values=None if create_result.ok else create_result.reason[:900],
        )

        await self._send_building_request_game_update(
            req,
            subject="Building request approved",
            body=self._build_game_approval_message(
                req,
                status=(
                    "Your building has been created in MissionChief."
                    if create_result.ok
                    else "Your request was approved, but staff need to complete the build manually."
                ),
            ),
        )
        await self._send_board_auto_build_log(
            guild,
            log_channel,
            req,
            post,
            contribution_rate=contribution_rate,
            create_result=create_result,
            automation_message=automation_message,
            status=(
                "MissionChief board building request auto-built"
                if create_result.ok
                else "MissionChief board building request needs manual creation"
            ),
            color=discord.Color.green() if create_result.ok else discord.Color.orange(),
        )
        if create_result.ok:
            return "Building request approved and automatically created in MissionChief."
        return f"Building request approved, but automatic creation needs staff follow-up: {create_result.reason}"

    async def _board_poll_loop(self) -> None:
        """Poll configured MissionChief board topics for new building requests."""
        await self.bot.wait_until_ready()
        while True:
            try:
                guild_configs: List[Tuple[discord.Guild, dict]] = []
                for guild in self.bot.guilds:
                    conf = await self.config.guild(guild).all()
                    guild_configs.append((guild, conf))

                poll_targets = self._select_board_poll_targets(guild_configs)
                for guild, conf in poll_targets.values():
                    await self._poll_building_board_for_guild(guild, conf)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("BuildingManager board poll loop error: %s", exc)
            await asyncio.sleep(BOARD_POLL_SECONDS)

    def _select_board_poll_targets(
        self,
        guild_configs: Iterable[Tuple[discord.Guild, dict]],
    ) -> Dict[int, Tuple[discord.Guild, dict]]:
        """Select one properly configured Discord guild per MissionChief board thread."""
        poll_targets: Dict[int, Tuple[discord.Guild, dict]] = {}
        for guild, conf in guild_configs:
            if not conf.get("board_poll_enabled"):
                continue
            if not conf.get("admin_channel_id"):
                log.info(
                    "Building board poll skipped for guild %s: admin channel is not configured",
                    getattr(guild, "id", "unknown"),
                )
                continue
            try:
                thread_id = int(conf.get("board_thread_id") or BOARD_THREAD_ID)
            except (TypeError, ValueError):
                thread_id = BOARD_THREAD_ID

            current = poll_targets.get(thread_id)
            if current is None or self._board_poll_target_score(conf) > self._board_poll_target_score(current[1]):
                poll_targets[thread_id] = (guild, conf)
        return poll_targets

    @staticmethod
    def _board_poll_target_score(conf: dict) -> int:
        """Prefer board targets with both admin and log channels configured."""
        score = 0
        if conf.get("admin_channel_id"):
            score += 10
        if conf.get("log_channel_id"):
            score += 1
        return score

    async def _poll_building_board_for_guild(self, guild: discord.Guild, conf: dict) -> None:
        if not conf.get("admin_channel_id"):
            log.info("Building board poll skipped: admin channel is not configured for guild %s", guild.id)
            return

        cookie_manager = self._cookie_manager()
        if not cookie_manager:
            log.info("Building board poll skipped: CookieManager is not loaded")
            return

        thread_id = int(conf.get("board_thread_id") or BOARD_THREAD_ID)
        session = await cookie_manager.get_session()
        page, status = await self._fetch_building_board_latest_page(session, thread_id)
        if status is not None and int(status) >= 400:
            log.warning("Building board poll failed: thread %s returned HTTP %s", thread_id, status)
            return
        if not page.posts:
            return

        latest_post_id = max(post.post_id for post in page.posts)
        thread_state = await self._get_board_thread_state(thread_id)
        last_seen_raw = thread_state.get("last_seen_post_id") or conf.get("board_last_seen_post_id")
        if not last_seen_raw:
            await self._set_board_thread_last_seen(thread_id, latest_post_id)
            await self.config.guild(guild).board_last_seen_post_id.set(latest_post_id)
            log.info("Building board baseline set to post %s for guild %s", latest_post_id, guild.id)
            return

        try:
            last_seen = int(last_seen_raw)
        except (TypeError, ValueError):
            await self._set_board_thread_last_seen(thread_id, latest_post_id)
            await self.config.guild(guild).board_last_seen_post_id.set(latest_post_id)
            return

        guild_processed_post_ids = self._normalize_board_post_ids(conf.get("board_processed_post_ids") or [])
        processed_post_ids = self._normalize_board_post_ids(thread_state.get("processed_post_ids") or [])
        if guild_processed_post_ids:
            await self._merge_board_thread_processed_ids(thread_id, guild_processed_post_ids)
            processed_post_ids = list(dict.fromkeys([*processed_post_ids, *guild_processed_post_ids]))

        new_posts = [
            post
            for post in sorted(page.posts, key=lambda item: item.post_id)
            if post.post_id > last_seen
            and post.post_id not in processed_post_ids
            and post.author_id != page.current_user_id
            and not self._is_board_system_post(post)
        ]

        for post in new_posts:
            if not await self._mark_board_post_processing(thread_id, post.post_id):
                continue
            try:
                await self._handle_building_board_post(guild, session, thread_id, page, post)
            except Exception as exc:
                log.exception("Building board post %s processing failed: %s", post.post_id, exc)
                notified = await self._send_board_processing_error_log(guild, conf, post, exc)
                reason = (
                    "An internal error occurred while processing this request. Staff has been notified."
                    if notified
                    else "An internal error occurred while processing this request. Please contact staff."
                )
                reply = self._build_building_board_error_reply(
                    post,
                    reason,
                )
                with contextlib.suppress(Exception):
                    _status, reply_post_id = await self._post_building_board_reply_with_id(
                        session,
                        thread_id,
                        page,
                        reply,
                    )
                    await self._schedule_board_post_deletion(guild, thread_id, post.post_id, "failed request")
                    if reply_post_id:
                        await self._schedule_board_post_deletion(guild, thread_id, reply_post_id, "failure reply")

        if latest_post_id > last_seen:
            await self._set_board_thread_last_seen(thread_id, latest_post_id)
            await self.config.guild(guild).board_last_seen_post_id.set(latest_post_id)

    async def _fetch_building_board_latest_page(self, session, thread_id: int) -> Tuple[BoardPage, Optional[int]]:
        base_url = f"{BASE_URL}/alliance_threads/{int(thread_id)}"
        async with session.get(base_url, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        page = parse_building_board_page(html)
        if status is not None and int(status) >= 400:
            return page, status

        if page.last_page > 1:
            last_url = f"{base_url}?page={page.last_page}"
            async with session.get(last_url, allow_redirects=True) as response:
                status = getattr(response, "status", None)
                html = await response.text()
            page = parse_building_board_page(html)
        return page, status

    def _normalize_board_post_ids(self, values) -> List[int]:
        post_ids: List[int] = []
        for value in values or []:
            try:
                post_ids.append(int(value))
            except (TypeError, ValueError):
                continue
        return post_ids

    async def _get_board_thread_state(self, thread_id: int) -> dict:
        states = await self.config.board_thread_states()
        return dict((states or {}).get(str(int(thread_id))) or {})

    async def _merge_board_thread_processed_ids(self, thread_id: int, post_ids: List[int]) -> None:
        if not post_ids:
            return
        key = str(int(thread_id))
        async with self.config.board_thread_states() as states:
            state = dict(states.get(key) or {})
            current = self._normalize_board_post_ids(state.get("processed_post_ids") or [])
            merged = list(dict.fromkeys([*current, *post_ids]))
            del merged[:-BOARD_PROCESSED_POST_ID_LIMIT]
            state["processed_post_ids"] = merged
            states[key] = state

    async def _set_board_thread_last_seen(self, thread_id: int, post_id: int) -> None:
        key = str(int(thread_id))
        async with self.config.board_thread_states() as states:
            state = dict(states.get(key) or {})
            state["last_seen_post_id"] = int(post_id)
            states[key] = state

    async def _mark_board_post_processing(self, thread_id: int, post_id: int) -> bool:
        key = str(int(thread_id))
        post_id = int(post_id)
        async with self.config.board_thread_states() as states:
            state = dict(states.get(key) or {})
            current = self._normalize_board_post_ids(state.get("processed_post_ids") or [])
            if post_id in current:
                return False
            current.append(post_id)
            del current[:-BOARD_PROCESSED_POST_ID_LIMIT]
            state["processed_post_ids"] = current
            states[key] = state
        return True

    async def _handle_building_board_post(
        self,
        guild: discord.Guild,
        session,
        thread_id: int,
        page: BoardPage,
        post: BoardBuildingPost,
    ) -> None:
        if self._is_board_system_post(post):
            return

        spec, error = extract_building_board_request(post.content)
        if error or not spec:
            reply = self._build_building_board_error_reply(post, error or "Could not read this request.")
            _status, reply_post_id = await self._post_building_board_reply_with_id(session, thread_id, page, reply)
            await self._schedule_board_post_deletion(guild, thread_id, post.post_id, "unrecognized request")
            if reply_post_id:
                await self._schedule_board_post_deletion(guild, thread_id, reply_post_id, "unrecognized reply")
            return

        location_details = await self._resolve_building_location(spec.location_input)
        if not location_details.coordinates:
            reason = (
                "The location could not be resolved to GPS coordinates. "
                "Please use a Google Maps place link with a visible marker."
            )
            reply = self._build_building_board_error_reply(post, reason)
            _status, reply_post_id = await self._post_building_board_reply_with_id(session, thread_id, page, reply)
            await self._schedule_board_post_deletion(guild, thread_id, post.post_id, "unresolved request")
            if reply_post_id:
                await self._schedule_board_post_deletion(guild, thread_id, reply_post_id, "unresolved reply")
            return

        detected_type, rejection_reason = LocationParser.detect_supported_building_type(location_details)
        if not detected_type:
            reply = self._build_building_board_error_reply(post, rejection_reason)
            _status, reply_post_id = await self._post_building_board_reply_with_id(session, thread_id, page, reply)
            await self._schedule_board_post_deletion(guild, thread_id, post.post_id, "unsupported facility request")
            if reply_post_id:
                await self._schedule_board_post_deletion(guild, thread_id, reply_post_id, "unsupported facility reply")
            return

        building_name = LocationParser.derive_building_name(detected_type, location_details)
        location_details.facility_warning = LocationParser.facility_warning(
            detected_type,
            building_name,
            location_details,
        )
        req = BuildingRequest(
            user_id=0,
            username=post.author_name,
            building_type=detected_type,
            building_name=building_name,
            location_input=spec.location_input,
            coordinates=location_details.coordinates,
            address=location_details.address,
            country=location_details.country,
            region=location_details.region,
            maps_url=location_details.maps_url,
            facility_warning=location_details.facility_warning,
            notes=f"MissionChief board post #{post.post_id}",
        )

        conf = await self.config.guild(guild).all()
        log_channel = await self._resolve_channel(guild, conf.get("log_channel_id"))
        if log_channel is None:
            log_channel = await self._resolve_channel(guild, conf.get("admin_channel_id"))

        if await self._board_auto_accept_enabled(guild):
            contribution_rate, contribution_source = await self._get_board_request_contribution_rate(post)
            if contribution_rate is None:
                req.notes = (
                    f"{req.notes or ''}\n"
                    f"Auto-accept skipped: contribution rate unknown ({contribution_source})."
                ).strip()
                request_id = await self._submit_building_request_to_admins(
                    guild,
                    req,
                    source="MissionChief board - contribution unknown",
                    board_post=post,
                )
                reply = self._build_building_board_success_reply(
                    post,
                    req,
                    request_id,
                    status="Admins will review this request because the requester contribution rate is unknown.",
                )
            elif contribution_rate < BUILDING_BOARD_MIN_AUTO_ACCEPT_TAX:
                request_id = await self._reject_board_request_for_low_tax(
                    guild,
                    req,
                    post,
                    contribution_rate=contribution_rate,
                    log_channel=log_channel,
                )
                reply = self._build_building_board_rejection_reply(
                    post,
                    request_id,
                    (
                        f"Your latest alliance donation is {contribution_rate:.1f}%, below the required "
                        f"{BUILDING_BOARD_MIN_AUTO_ACCEPT_TAX:.1f}% minimum."
                    ),
                )
            else:
                status = await self._auto_accept_board_building_request(
                    guild,
                    req,
                    post,
                    contribution_rate=contribution_rate,
                    log_channel=log_channel,
                )
                reply = self._build_building_board_success_reply(
                    post,
                    req,
                    int(req.request_id or 0),
                    status=status,
                )
        else:
            request_id = await self._submit_building_request_to_admins(
                guild,
                req,
                source="MissionChief board",
                board_post=post,
            )
            reply = self._build_building_board_success_reply(post, req, request_id)

        reply_status, reply_post_id = await self._post_building_board_reply_with_id(session, thread_id, page, reply)
        if reply_status is None or int(reply_status) >= 400:
            log.warning("Building board reply for post %s returned HTTP %s", post.post_id, reply_status)
        await self._schedule_board_post_deletion(guild, thread_id, post.post_id, "processed request")
        if reply_post_id:
            await self._schedule_board_post_deletion(guild, thread_id, reply_post_id, "processed reply")

    def _build_building_board_success_reply(
        self,
        post: BoardBuildingPost,
        req: BuildingRequest,
        request_id: int,
        *,
        status: str = "Admins will review this request.",
    ) -> str:
        lines = [
            BOARD_REPLY_MARKER,
            f"Building request received for {post.author_name}.",
            "",
            f"Request ID: {int(request_id)}",
            f"Type: {req.building_type}",
            f"Detected name: {req.building_name}",
        ]
        if req.coordinates:
            lines.append(f"Coordinates: {req.coordinates}")
        if req.facility_warning:
            lines.extend(["", f"Admin review note: {req.facility_warning}"])
        lines.extend(["", status])
        return "\n".join(lines)

    def _build_building_board_rejection_reply(
        self,
        post: BoardBuildingPost,
        request_id: int,
        reason: str,
    ) -> str:
        return "\n".join(
            [
                BOARD_REPLY_MARKER,
                f"Building request rejected for {post.author_name}.",
                "",
                f"Request ID: {int(request_id)}",
                f"Reason: {reason}",
                "",
                "Please correct this before submitting another building request.",
            ]
        )

    def _build_building_board_error_reply(self, post: BoardBuildingPost, reason: str) -> str:
        return "\n".join(
            [
                BOARD_REPLY_MARKER,
                f"Building request could not be processed for {post.author_name}.",
                "",
                f"Reason: {reason}",
                "",
                "Use one of these formats:",
                "Hospital: <Google Maps link>",
                "Prison: <Google Maps link>",
            ]
        )

    async def _send_board_request_error_log(self, guild: discord.Guild, post: BoardBuildingPost, reason: str) -> None:
        conf = await self.config.guild(guild).all()
        log_channel = await self._resolve_channel(guild, conf.get("admin_channel_id"))
        if not log_channel:
            log_channel = await self._resolve_channel(guild, conf.get("log_channel_id"))
        if not log_channel:
            return
        embed = discord.Embed(
            title="MissionChief board building request rejected",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Requester", value=f"{post.author_name} ({post.author_id or 'unknown ID'})", inline=False)
        embed.add_field(name="Board post", value=f"#{post.post_id}", inline=True)
        if post.created_at:
            embed.add_field(name="Posted at", value=post.created_at, inline=True)
        embed.add_field(name="Reason", value=_truncate_discord_text(reason), inline=False)
        embed.add_field(name="Original text", value=_truncate_discord_text(post.content or "-", 1024), inline=False)
        try:
            await log_channel.send(embed=embed)
        except Exception:
            log.exception("Could not send BuildingManager board request error log for post %s", post.post_id)

    async def _send_board_processing_error_log(
        self,
        guild: discord.Guild,
        conf: dict,
        post: BoardBuildingPost,
        error: Exception,
    ) -> bool:
        log_channel = await self._resolve_channel(guild, conf.get("admin_channel_id"))
        if not log_channel:
            log_channel = await self._resolve_channel(guild, conf.get("log_channel_id"))
        if not log_channel:
            return False
        embed = discord.Embed(
            title="MissionChief board building request failed",
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Requester", value=f"{post.author_name} ({post.author_id or 'unknown ID'})", inline=False)
        embed.add_field(name="Board post", value=f"#{post.post_id}", inline=True)
        if post.created_at:
            embed.add_field(name="Posted at", value=post.created_at, inline=True)
        embed.add_field(name="Error", value=_truncate_discord_text(str(error)), inline=False)
        embed.add_field(name="Original text", value=_truncate_discord_text(post.content or "-", 1024), inline=False)
        try:
            await log_channel.send(embed=embed)
            return True
        except Exception:
            log.exception("Could not send BuildingManager board processing error log for post %s", post.post_id)
            return False

    async def _post_building_board_reply(
        self,
        session,
        thread_id: int,
        page: BoardPage,
        content: str,
    ) -> Optional[int]:
        action = page.reply_action or f"/alliance_posts?alliance_thread_id={int(thread_id)}"
        post_url = urljoin(BASE_URL, action)
        payload = {
            "utf8": "\u2713",
            "alliance_post[content]": content,
            "commit": "Save",
        }
        if page.reply_token:
            payload["authenticity_token"] = page.reply_token

        async with session.post(post_url, data=payload, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            await response.text()
        return status

    async def _post_building_board_reply_with_id(
        self,
        session,
        thread_id: int,
        page: BoardPage,
        content: str,
    ) -> Tuple[Optional[int], Optional[int]]:
        status = await self._post_building_board_reply(session, thread_id, page, content)
        if status is None or int(status) >= 400:
            return status, None

        refreshed_page, refreshed_status = await self._fetch_building_board_latest_page(session, thread_id)
        if refreshed_status is not None and int(refreshed_status) >= 400:
            return status, None

        for post in sorted(refreshed_page.posts, key=lambda item: item.post_id, reverse=True):
            if BOARD_REPLY_MARKER not in str(post.content or ""):
                continue
            if refreshed_page.current_user_id and post.author_id and post.author_id != refreshed_page.current_user_id:
                continue
            return status, int(post.post_id)
        return status, None

    async def _schedule_board_post_deletion(
        self,
        guild: discord.Guild,
        thread_id: int,
        post_id: int,
        reason: str,
    ) -> None:
        due_ts = ts() + BOARD_POST_DELETE_AFTER_SECONDS
        post_id = int(post_id)
        thread_id = int(thread_id)
        async with self.config.guild(guild).board_pending_deletions() as pending:
            normalized = []
            exists = False
            for item in pending or []:
                try:
                    item_thread_id = int(item.get("thread_id"))
                    item_post_id = int(item.get("post_id"))
                except (AttributeError, TypeError, ValueError):
                    continue
                if item_thread_id == thread_id and item_post_id == post_id:
                    exists = True
                    item = dict(item)
                    item["due_ts"] = min(int(item.get("due_ts") or due_ts), due_ts)
                    item["reason"] = str(item.get("reason") or reason)
                normalized.append(item)

            if not exists:
                normalized.append(
                    {
                        "thread_id": thread_id,
                        "post_id": post_id,
                        "due_ts": due_ts,
                        "reason": str(reason),
                        "attempts": 0,
                    }
                )

            normalized = sorted(normalized, key=lambda item: int(item.get("due_ts") or 0))
            del normalized[:-BOARD_PENDING_DELETE_LIMIT]
            pending.clear()
            pending.extend(normalized)

    async def _board_cleanup_loop(self) -> None:
        await self.bot.wait_until_ready()
        while True:
            try:
                for guild in self.bot.guilds:
                    await self._cleanup_board_deletions_for_guild(guild)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("BuildingManager board cleanup loop error: %s", exc)
            await asyncio.sleep(BOARD_CLEANUP_SECONDS)

    async def _cleanup_board_deletions_for_guild(self, guild: discord.Guild) -> None:
        conf = await self.config.guild(guild).all()
        pending = list(conf.get("board_pending_deletions") or [])
        if not pending:
            return

        now_ts = ts()
        due_items = []
        remaining = []
        for item in pending:
            try:
                due_ts = int(item.get("due_ts") or 0)
            except (AttributeError, TypeError, ValueError):
                continue
            if due_ts <= now_ts:
                due_items.append(dict(item))
            else:
                remaining.append(dict(item))

        if not due_items:
            return

        cookie_manager = self._cookie_manager()
        if not cookie_manager:
            log.info("Building board cleanup skipped: CookieManager is not loaded")
            return

        session = await cookie_manager.get_session()
        for item in due_items:
            try:
                thread_id = int(item.get("thread_id"))
                post_id = int(item.get("post_id"))
            except (TypeError, ValueError):
                continue

            deleted, reason = await self._delete_board_post(session, thread_id, post_id)
            if deleted:
                await asyncio.sleep(1)
                continue

            attempts = int(item.get("attempts") or 0) + 1
            if attempts < 6:
                item["attempts"] = attempts
                item["last_error"] = reason[:200]
                item["due_ts"] = now_ts + min(BOARD_CLEANUP_SECONDS * attempts, 60 * 60)
                remaining.append(item)
            else:
                log.warning("Dropping building board post cleanup for %s after %s attempts: %s", post_id, attempts, reason)

        remaining = sorted(remaining, key=lambda item: int(item.get("due_ts") or 0))
        del remaining[:-BOARD_PENDING_DELETE_LIMIT]
        await self.config.guild(guild).board_pending_deletions.set(remaining)

    async def _delete_board_post(self, session, thread_id: int, post_id: int) -> Tuple[bool, str]:
        page, status = await self._fetch_building_board_latest_page(session, int(thread_id))
        if status is not None and int(status) >= 400:
            return False, f"thread returned HTTP {status}"

        payload = {
            "utf8": "\u2713",
            "_method": "delete",
            "commit": "Delete",
        }
        if page.reply_token:
            payload["authenticity_token"] = page.reply_token

        url = f"{BASE_URL}/alliance_posts/{int(post_id)}"
        async with session.post(url, data=payload, allow_redirects=True) as response:
            delete_status = getattr(response, "status", None)
            await response.text()
        if delete_status is None:
            return False, "delete returned no HTTP status"
        if int(delete_status) in {404, 410}:
            return True, "already deleted"
        if int(delete_status) >= 400:
            return False, f"delete returned HTTP {delete_status}"
        return True, "deleted"

    def _board_guide_hash(self, content: str) -> str:
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def _is_board_guide_post(self, post: BoardBuildingPost) -> bool:
        return BOARD_GUIDE_MARKER in str(post.content or "")

    def _is_board_system_post(self, post: BoardBuildingPost) -> bool:
        text = str(post.content or "").strip()
        if self._is_board_guide_post(post):
            return True
        if BOARD_REPLY_MARKER in text:
            return True
        lowered = text.casefold()
        return (
            lowered.startswith("building request received for ")
            or lowered.startswith("building request could not be processed for ")
        )

    def _set_form_value(
        self,
        payload: Dict[str, str],
        candidates: Tuple[str, ...],
        value: str,
        fallback_name: str,
    ) -> None:
        for name in list(payload.keys()):
            lowered = name.casefold()
            if any(candidate in lowered for candidate in candidates):
                payload[name] = value
                return
        payload[fallback_name] = value

    def _select_post_edit_form(self, forms: List[MissionChiefForm], post_id: int) -> Optional[MissionChiefForm]:
        wanted = f"/alliance_posts/{int(post_id)}"
        for form in forms:
            if wanted in str(form.action or ""):
                return form
        for form in forms:
            if "/alliance_posts" in str(form.action or ""):
                return form
        return forms[0] if forms else None

    async def _submit_missionchief_form(self, session, form: MissionChiefForm, payload: Dict[str, str]) -> Tuple[Optional[int], str, str]:
        action = form.action or ""
        url = urljoin(BASE_URL, action)
        if form.method == "get":
            async with session.get(url, params=payload, allow_redirects=True) as response:
                status = getattr(response, "status", None)
                html = await response.text()
                final_url = str(getattr(response, "url", url))
        else:
            async with session.post(url, data=payload, allow_redirects=True) as response:
                status = getattr(response, "status", None)
                html = await response.text()
                final_url = str(getattr(response, "url", url))
        return status, html, final_url

    async def _find_existing_board_guide_post(self, session, thread_id: int) -> Optional[int]:
        base_url = f"{BASE_URL}/alliance_threads/{int(thread_id)}"
        async with session.get(base_url, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            return None

        first_page = parse_building_board_page(html)
        max_page = min(first_page.last_page, BOARD_GUIDE_MAX_SCAN_PAGES)
        for page_number in range(1, max_page + 1):
            page_url = f"{base_url}?page={page_number}"
            async with session.get(page_url, allow_redirects=True) as response:
                status = getattr(response, "status", None)
                page_html = await response.text()
            if status is not None and int(status) >= 400:
                continue
            page = parse_building_board_page(page_html)
            for post in page.posts:
                if self._is_board_guide_post(post):
                    return int(post.post_id)
        return None

    async def _create_board_guide_post(self, session, thread_id: int, content: str) -> Tuple[Optional[int], str]:
        page, status = await self._fetch_building_board_latest_page(session, thread_id)
        if status is not None and int(status) >= 400:
            return None, f"request thread returned HTTP {status}"

        post_status = await self._post_building_board_reply(session, thread_id, page, content)
        if post_status is None or int(post_status) >= 400:
            return None, f"create guide post returned HTTP {post_status}"

        page, status = await self._fetch_building_board_latest_page(session, thread_id)
        if status is not None and int(status) >= 400:
            return None, f"could not refetch request thread after create: HTTP {status}"
        for post in sorted(page.posts, key=lambda item: item.post_id, reverse=True):
            if self._is_board_guide_post(post):
                return int(post.post_id), "created"
        return None, "created guide post but could not resolve new post ID"

    async def _edit_board_guide_post(self, session, post_id: int, content: str) -> Tuple[bool, str]:
        edit_url = f"{BASE_URL}/alliance_posts/{int(post_id)}/edit"
        async with session.get(edit_url, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            return False, f"edit form returned HTTP {status}"

        form = self._select_post_edit_form(parse_missionchief_forms(html), post_id)
        if not form:
            return False, "edit form not found"

        payload = dict(form.fields)
        payload.setdefault("utf8", "\u2713")
        payload.setdefault("commit", "Save")
        if "_method" not in payload and f"/alliance_posts/{int(post_id)}" in str(form.action or ""):
            payload["_method"] = "patch"
        self._set_form_value(
            payload,
            ("[content]", "[text]", "[body]", "content", "text", "body"),
            content,
            "alliance_post[content]",
        )
        status, _html, _final_url = await self._submit_missionchief_form(session, form, payload)
        if status is None or int(status) >= 400:
            return False, f"edit post returned HTTP {status}"
        return True, "updated"

    async def _sync_building_board_guide_for_guild(self, guild: discord.Guild, *, force: bool = False) -> bool:
        conf = await self.config.guild(guild).all()
        if not conf.get("board_guide_enabled"):
            return False
        cookie_manager = self._cookie_manager()
        if not cookie_manager:
            log.info("Building board guide skipped: CookieManager is not loaded")
            return False

        thread_id = int(conf.get("board_thread_id") or BOARD_THREAD_ID)
        content = build_building_board_guide_content(thread_id)
        content_hash = self._board_guide_hash(content)
        post_id = conf.get("board_guide_post_id")
        stored_hash = conf.get("board_guide_content_hash")
        if post_id and not force and stored_hash == content_hash:
            return False

        session = await cookie_manager.get_session()
        discovered_post_id = await self._find_existing_board_guide_post(session, thread_id)
        if discovered_post_id:
            post_id = discovered_post_id

        changed = False
        if post_id:
            updated, reason = await self._edit_board_guide_post(session, int(post_id), content)
            if updated:
                changed = True
            else:
                log.warning("Building board guide post %s update failed: %s", post_id, reason)
                post_id = None

        if not post_id:
            created_post_id, reason = await self._create_board_guide_post(session, thread_id, content)
            if not created_post_id:
                log.warning("Building board guide could not be created: %s", reason)
                return False
            post_id = int(created_post_id)
            changed = True

        await self.config.guild(guild).board_guide_thread_id.set(int(thread_id))
        await self.config.guild(guild).board_guide_post_id.set(int(post_id))
        await self.config.guild(guild).board_guide_content_hash.set(content_hash)
        return changed

    async def _board_guide_loop(self) -> None:
        await self.bot.wait_until_ready()
        while True:
            try:
                for guild in self.bot.guilds:
                    await self._sync_building_board_guide_for_guild(guild)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("BuildingManager board guide loop error: %s", exc)
            await asyncio.sleep(BOARD_GUIDE_SYNC_SECONDS)

    async def _get_min_alliance_funds(self, guild: discord.Guild) -> int:
        """Return the configured minimum alliance funds before auto-building."""
        value = await self.config.guild(guild).min_alliance_funds()
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return ALLIANCE_BUILDING_MIN_FUNDS

    def _read_alliance_funds_from_income_db(self) -> Optional[int]:
        """Read the latest known alliance funds from IncomeScraper storage when available."""
        income_scraper = self.bot.get_cog("IncomeScraper")
        db_path = getattr(income_scraper, "db_path", None) if income_scraper else None
        if not db_path:
            return None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT total_funds FROM treasury_balance ORDER BY scraped_at DESC LIMIT 1")
            row = cursor.fetchone()
            conn.close()
        except sqlite3.Error:
            return None
        return _coerce_int(row[0]) if row else None

    async def _get_alliance_funds_from_contract(self) -> Optional[int]:
        """Read alliance funds through a public scraper contract when one is available."""
        income_scraper = self.bot.get_cog("IncomeScraper")
        if not income_scraper:
            return None
        for method_name in ("get_current_alliance_funds", "get_alliance_funds"):
            method = getattr(income_scraper, method_name, None)
            if not callable(method):
                continue
            try:
                result = method()
                if asyncio.iscoroutine(result):
                    result = await result
            except Exception:
                continue
            if isinstance(result, dict):
                result = result.get("total_funds", result.get("funds", result.get("balance")))
            funds = _coerce_int(result)
            if funds is not None:
                return funds
        return None

    async def _fetch_live_alliance_funds(self) -> Optional[int]:
        """Fetch and parse the live MissionChief alliance funds page."""
        session = await self._get_session()
        async with session.get(MISSIONCHIEF_ALLIANCE_FUNDS_URL, timeout=30) as response:
            if response.status != 200:
                raise RuntimeError(f"MissionChief returned HTTP {response.status} for alliance funds.")
            html = await response.text()
        return parse_alliance_funds_from_html(html)

    async def _fetch_live_alliance_funds_browser(self) -> Optional[int]:
        """Fetch the live MissionChief alliance funds page through a logged-in browser."""
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
                await page.goto(MISSIONCHIEF_ALLIANCE_FUNDS_URL, wait_until="domcontentloaded")
                login_fields = await page.locator("input[type='password']").count()
                if login_fields:
                    raise RuntimeError("MissionChief session is not logged in.")

                html_funds = parse_alliance_funds_from_html(await page.content())
                if html_funds is not None:
                    return html_funds

                body_text = await page.locator("body").inner_text(timeout=5000)
                return parse_alliance_funds_from_html(body_text)
            finally:
                await browser.close()

    async def _get_current_alliance_funds(self) -> Tuple[Optional[int], str]:
        """Return current alliance funds and the source used."""
        errors = []
        for source, fetcher in (
            ("live MissionChief", self._fetch_live_alliance_funds),
            ("live MissionChief browser", self._fetch_live_alliance_funds_browser),
        ):
            try:
                funds = await fetcher()
                if funds is not None:
                    return funds, source
                errors.append(f"{source}: no alliance funds amount found")
            except Exception as exc:
                errors.append(f"{source}: {type(exc).__name__}: {_truncate_discord_text(exc, 180)}")

        funds = await self._get_alliance_funds_from_contract()
        if funds is not None:
            return funds, "IncomeScraper contract"

        funds = self._read_alliance_funds_from_income_db()
        if funds is not None:
            return funds, "income_v2.db"

        if errors:
            return None, _truncate_discord_text("unavailable; " + "; ".join(errors), 900)
        return None, "unavailable"

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

    async def _queue_request_waiting_for_funds(
        self,
        *,
        guild: discord.Guild,
        req: BuildingRequest,
        requester_id: int,
        admin_user: Optional[discord.abc.User],
        funds: Optional[int],
        source: str,
        minimum: int,
        log_channel: Optional[discord.abc.Messageable],
        record_approval: bool = True,
    ):
        """Mark an approved request as waiting until alliance funds are high enough."""
        self.db.update_request_status(int(req.request_id), "awaiting_funds")
        if record_approval:
            self.db.add_action(
                request_id=int(req.request_id),
                guild_id=guild.id,
                admin_user_id=getattr(admin_user, "id", None),
                admin_username=str(admin_user) if admin_user else "BuildingManager",
                action_type="approved",
            )
        self.db.add_action(
            request_id=int(req.request_id),
            guild_id=guild.id,
            admin_user_id=getattr(admin_user, "id", None),
            admin_username=str(admin_user) if admin_user else "BuildingManager",
            action_type="awaiting_funds",
            previous_values=(
                f"Funds: {funds:,}" if funds is not None else f"Funds unavailable via {source}"
            ),
        )

        current = f"{funds:,} credits" if funds is not None else "unknown"
        message = (
            f"Queued for automatic build. No building was created yet. Current alliance funds are {current}; "
            f"minimum required before auto-building is {minimum:,} credits."
        )
        user = guild.get_member(requester_id) if requester_id else None
        if user:
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await user.send(
                    _truncate_discord_text(
                        "Your building request is queued, but no building has been created yet. "
                        "It is waiting until alliance funds can be verified above the configured minimum.\n\n"
                        f"{req.building_type}: {req.building_name}",
                        1900,
                    )
                )

        await self._send_building_request_game_update(
            req,
            subject="Building request queued",
            body=self._build_game_approval_message(
                req,
                status=(
                    "Your building request is queued, but no building has been created yet. "
                    "It is waiting until alliance funds can be verified above the configured minimum."
                ),
            ),
        )

        if log_channel:
            embed = discord.Embed(
                title="Building request queued - waiting for alliance funds",
                color=discord.Color.orange(),
                timestamp=datetime.now(timezone.utc),
            )
            embed.add_field(name="Requester", value=self._requester_label(guild, requester_id, req.username), inline=False)
            embed.add_field(
                name="Building",
                value=_truncate_discord_text(f"{req.building_type} - {req.building_name}"),
                inline=False,
            )
            embed.add_field(name="Current Funds", value=current, inline=True)
            embed.add_field(name="Required Minimum", value=f"{minimum:,} credits", inline=True)
            embed.add_field(name="Funds Source", value=_truncate_discord_text(source, 900), inline=False)
            if admin_user:
                embed.add_field(name="Queued by", value=f"{admin_user.mention} ({admin_user.id})", inline=False)
            embed.add_field(
                name="Auto Creation",
                value="Not started. Waiting until alliance funds can be verified above the required minimum.",
                inline=False,
            )
            embed.add_field(name="Request ID", value=str(req.request_id), inline=True)
            await log_channel.send(embed=embed)

        return message

    async def _find_created_alliance_building_browser(
        self,
        req: BuildingRequest,
        *,
        reason: str = "Created alliance building was found after lookup.",
    ) -> BuildingCreateResult:
        """Find an already-created alliance building and return its MissionChief id."""
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
            api_lookup: Dict[str, Any] = {}
            alliance_list_lookup: Dict[str, Any] = {}
            log_lookup: Dict[str, Any] = {}
            detected_id: Optional[int] = None
            last_status: Optional[int] = None

            try:
                async with async_playwright() as playwright:
                    browser = await playwright.chromium.launch(headless=True)
                    try:
                        context = await browser.new_context(viewport={"width": 1440, "height": 1000})
                        await context.add_cookies(cookies)
                        page = await context.new_page()
                        page.set_default_timeout(30000)
                        await page.goto(MISSIONCHIEF_HOME_URL, wait_until="domcontentloaded")

                        login_fields = await page.locator("input[type='password']").count()
                        if login_fields:
                            return BuildingCreateResult(False, "MissionChief session is not logged in.")

                        with contextlib.suppress(Exception):
                            api_lookup = await page.evaluate(BUILDING_FETCH_API_SCRIPT)
                            if api_lookup.get("ok"):
                                detected_id = find_created_alliance_building_id(
                                    api_lookup.get("buildings") or [],
                                    config,
                                )
                                last_status = _coerce_int(api_lookup.get("status"))
                                api_lookup = {
                                    "ok": True,
                                    "status": api_lookup.get("status"),
                                    "count": len(api_lookup.get("buildings") or []),
                                    "matchedBuildingId": detected_id,
                                }

                        if not detected_id:
                            with contextlib.suppress(Exception):
                                alliance_list_lookup = await page.evaluate(
                                    BUILDING_FETCH_ALLIANCE_LIST_SCRIPT,
                                    {
                                        "maxPages": BUILDING_LOOKUP_MAX_ALLIANCE_LIST_PAGES,
                                        "targetName": config.get("name") or "",
                                    },
                                )
                                if alliance_list_lookup.get("ok"):
                                    candidates = alliance_list_lookup.get("candidates") or []
                                    detected_id = find_created_alliance_building_id_from_list(candidates, config)
                                    last_status = _coerce_int(alliance_list_lookup.get("status")) or last_status
                                    alliance_list_lookup = {
                                        "ok": True,
                                        "status": alliance_list_lookup.get("status"),
                                        "pages": alliance_list_lookup.get("pages") or [],
                                        "count": len(candidates),
                                        "matchedBuildingId": detected_id,
                                    }

                        if not detected_id:
                            with contextlib.suppress(Exception):
                                log_lookup = await page.evaluate(BUILDING_FETCH_ALLIANCE_LOGS_SCRIPT)
                                if log_lookup.get("ok"):
                                    candidates = log_lookup.get("candidates") or []
                                    detected_id = find_created_alliance_building_id_from_logs(candidates, config)
                                    last_status = _coerce_int(log_lookup.get("status")) or last_status
                                    log_lookup = {
                                        "ok": True,
                                        "status": log_lookup.get("status"),
                                        "count": len(candidates),
                                        "matchedBuildingId": detected_id,
                                    }
                    finally:
                        await browser.close()
            except Exception as exc:
                message = str(exc)
                if "Executable doesn't exist" in message or "playwright install" in message:
                    return BuildingCreateResult(False, PLAYWRIGHT_SETUP_MESSAGE)
                return BuildingCreateResult(False, f"MissionChief building lookup failed: {message}")

        details = {
            "apiLookup": api_lookup,
            "allianceListLookup": alliance_list_lookup,
            "allianceLogLookup": log_lookup,
            "buildingId": detected_id,
        }
        if not detected_id:
            return BuildingCreateResult(
                False,
                "MissionChief building was not found after creation lookup. "
                "No post-creation automation was queued.",
                status=last_status,
                post_url=f"{BASE_URL}/buildings",
                details=details,
            )
        return BuildingCreateResult(
            True,
            reason,
            status=last_status,
            post_url=f"{BASE_URL}/buildings/{detected_id}",
            details=details,
        )

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
            redirect_location = ""
            api_lookup: Dict[str, Any] = {}
            before_alliance_list_lookup: Dict[str, Any] = {}
            alliance_list_lookup: Dict[str, Any] = {}
            log_lookup: Dict[str, Any] = {}
            before_alliance_candidates: List[Dict[str, Any]] = []
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

                        with contextlib.suppress(Exception):
                            before_alliance_list_lookup = await page.evaluate(
                                BUILDING_FETCH_ALLIANCE_LIST_SCRIPT,
                                {
                                    "maxPages": BUILDING_LOOKUP_MAX_ALLIANCE_LIST_PAGES,
                                    "targetName": config.get("name") or "",
                                },
                            )
                            if before_alliance_list_lookup.get("ok"):
                                before_alliance_candidates = before_alliance_list_lookup.get("candidates") or []
                                before_alliance_list_lookup = {
                                    "ok": True,
                                    "status": before_alliance_list_lookup.get("status"),
                                    "pages": before_alliance_list_lookup.get("pages") or [],
                                    "count": len(before_alliance_candidates),
                                }

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
                        response_headers = response.headers or {}
                        redirect_location = str(
                            response_headers.get("location")
                            or response_headers.get("Location")
                            or ""
                        )
                        with contextlib.suppress(Exception):
                            response_text = await response.text()
                        with contextlib.suppress(Exception):
                            await page.wait_for_load_state("domcontentloaded", timeout=10000)
                        final_url = str(page.url or "")
                        detected_id = extract_missionchief_building_id(
                            response_url,
                            final_url,
                            redirect_location,
                            response_text,
                        )
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
                                    {
                                        "maxPages": BUILDING_LOOKUP_MAX_ALLIANCE_LIST_PAGES,
                                        "targetName": config.get("name") or "",
                                    },
                                )
                                if alliance_list_lookup.get("ok"):
                                    candidates = alliance_list_lookup.get("candidates") or []
                                    detected_id = find_new_created_alliance_building_id_from_list(
                                        before_alliance_candidates,
                                        candidates,
                                        config,
                                    )
                                    if not detected_id:
                                        detected_id = find_created_alliance_building_id_from_list(candidates, config)
                                    alliance_list_lookup = {
                                        "ok": True,
                                        "status": alliance_list_lookup.get("status"),
                                        "pages": alliance_list_lookup.get("pages") or [],
                                        "count": len(candidates),
                                        "beforeCount": len(before_alliance_candidates),
                                        "matchedBuildingId": detected_id,
                                    }
                        if not detected_id and status is not None and int(status) < 400:
                            with contextlib.suppress(Exception):
                                log_lookup = await page.evaluate(BUILDING_FETCH_ALLIANCE_LOGS_SCRIPT)
                                if log_lookup.get("ok"):
                                    candidates = log_lookup.get("candidates") or []
                                    detected_id = find_created_alliance_building_id_from_logs(candidates, config)
                                    log_lookup = {
                                        "ok": True,
                                        "status": log_lookup.get("status"),
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
                        "redirectLocation": redirect_location,
                        "apiLookup": api_lookup,
                        "beforeAllianceListLookup": before_alliance_list_lookup,
                        "allianceListLookup": alliance_list_lookup,
                        "allianceLogLookup": log_lookup,
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
        building_id = extract_missionchief_building_id(response_url, final_url, redirect_location, response_text)
        if not building_id:
            building_id = _coerce_int(api_lookup.get("matchedBuildingId"))
        if not building_id:
            building_id = _coerce_int(alliance_list_lookup.get("matchedBuildingId"))
        if not building_id:
            building_id = _coerce_int(log_lookup.get("matchedBuildingId"))
        details.update(
            {
                "responseUrl": response_url,
                "finalUrl": final_url,
                "redirectLocation": redirect_location,
                "apiLookup": api_lookup,
                "beforeAllianceListLookup": before_alliance_list_lookup,
                "allianceListLookup": alliance_list_lookup,
                "allianceLogLookup": log_lookup,
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

    async def _building_creation_queue_loop(self):
        """Create approved building requests once live alliance funds are high enough."""
        try:
            await self.bot.wait_until_ready()
            await asyncio.sleep(60)
            while True:
                try:
                    await self._process_waiting_for_funds_queue()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("BuildingManager funds queue loop failed")
                await asyncio.sleep(BUILDING_CREATION_QUEUE_LOOP_SECONDS)
        except asyncio.CancelledError:
            raise

    async def _process_waiting_for_funds_queue(self, guild_id: Optional[int] = None) -> bool:
        """Process at most one approved request waiting for sufficient funds."""
        rows = self.db.get_requests_by_status("awaiting_funds", limit=20)
        if guild_id is not None:
            rows = [row for row in rows if int(row["guild_id"]) == int(guild_id)]
        if not rows:
            return False

        row = rows[0]
        guild = self.bot.get_guild(int(row["guild_id"]))
        if guild is None:
            return False

        minimum_funds = await self._get_min_alliance_funds(guild)
        current_funds, funds_source = await self._get_current_alliance_funds()
        if not alliance_funds_allow_auto_build(current_funds, funds_source, minimum_funds):
            return False

        req = building_request_from_row(row)
        conf = await self.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None
        create_result = await self._create_alliance_building_browser(req)
        final_status = "created" if create_result.ok else "approved_pending_manual"
        self.db.update_request_status(int(req.request_id), final_status)

        self.db.add_action(
            request_id=int(req.request_id),
            guild_id=guild.id,
            admin_user_id=None,
            admin_username="BuildingManager",
            action_type="created_from_funds_queue" if create_result.ok else "create_failed_from_funds_queue",
            previous_values=None if create_result.ok else create_result.reason[:900],
        )

        automation_message = None
        if create_result.ok:
            building_id = create_result.building_id
            if building_id:
                job_id = self.db.add_or_update_automation_job(
                    request_id=int(req.request_id),
                    guild_id=guild.id,
                    building_id=building_id,
                    building_type=req.building_type,
                    building_name=req.building_name,
                )
                automation_message = (
                    f"Queued post-creation automation for MissionChief building `{building_id}`: "
                    f"tax {ALLIANCE_BUILDING_TARGET_TAX}%, max level, and allowed extensions."
                )
                self.bot.loop.create_task(self._process_building_automation_job(job_id))
            else:
                automation_message = (
                    "Created in MissionChief, but the building ID was not detected. "
                    "Post-creation automation was not queued."
                )

        user_id = int(row["user_id"])
        user = guild.get_member(user_id) if user_id else None
        if user and create_result.ok:
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await user.send(
                    "Your approved building request has now been built automatically because alliance funds "
                    f"are above {minimum_funds:,} credits.\n\n{req.building_type}: {req.building_name}"
                )

        if create_result.ok:
            await self._send_building_request_game_update(
                req,
                subject="Approved building request created",
                body=self._build_game_approval_message(
                    req,
                    status=(
                        "Your approved building request has now been built automatically because alliance funds "
                        f"are above {minimum_funds:,} credits."
                    ),
                ),
            )

        if log_channel:
            embed = discord.Embed(
                title="Queued building request created" if create_result.ok else "Queued building request needs manual creation",
                color=discord.Color.green() if create_result.ok else discord.Color.orange(),
                timestamp=datetime.now(timezone.utc),
            )
            embed.add_field(name="Requester", value=self._requester_label(guild, user_id, str(row["username"])), inline=False)
            embed.add_field(name="Building", value=f"{req.building_type} - {req.building_name}", inline=False)
            embed.add_field(name="Funds at Build Time", value=f"{current_funds:,} credits", inline=True)
            embed.add_field(name="Required Minimum", value=f"{minimum_funds:,} credits", inline=True)
            embed.add_field(
                name="Auto Creation",
                value="Created in MissionChief" if create_result.ok else create_result.reason[:900],
                inline=False,
            )
            if automation_message:
                embed.add_field(name="Post-Creation Automation", value=automation_message[:900], inline=False)
            if create_result.status is not None:
                embed.add_field(name="MissionChief HTTP Status", value=str(create_result.status), inline=True)
            embed.add_field(name="Request ID", value=str(req.request_id), inline=True)
            await log_channel.send(embed=embed)

        return create_result.ok

    async def _auto_candidate_build_loop(self):
        """Run the daily automatic candidate build scheduler."""
        try:
            await self.bot.wait_until_ready()
            await asyncio.sleep(90)
            while True:
                try:
                    for guild in self.bot.guilds:
                        await self._maybe_run_daily_auto_candidates(guild)
                        await asyncio.sleep(2)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("BuildingManager candidate auto-build loop failed")
                await asyncio.sleep(AUTO_CANDIDATE_LOOP_SECONDS)
        except asyncio.CancelledError:
            raise

    async def _maybe_run_daily_auto_candidates(self, guild: discord.Guild) -> None:
        """Run automatic candidate builds when enabled and due for the guild."""
        conf = await self.config.guild(guild).all()
        if not conf.get("auto_candidate_build_enabled"):
            return
        timezone_name = str(conf.get("auto_candidate_timezone") or AUTO_CANDIDATE_DEFAULT_TIMEZONE)
        time_text = str(conf.get("auto_candidate_time") or AUTO_CANDIDATE_DEFAULT_TIME)
        try:
            zone = ZoneInfo(timezone_name)
        except Exception:
            zone = ZoneInfo(AUTO_CANDIDATE_DEFAULT_TIMEZONE)
            timezone_name = AUTO_CANDIDATE_DEFAULT_TIMEZONE
        now_local = datetime.now(zone)
        try:
            hour_text, minute_text = time_text.split(":", 1)
            due_hour = int(hour_text)
            due_minute = int(minute_text)
        except (TypeError, ValueError):
            due_hour, due_minute = (7, 0)
            time_text = AUTO_CANDIDATE_DEFAULT_TIME
        if now_local.hour < due_hour or (now_local.hour == due_hour and now_local.minute < due_minute):
            return

        run_date = now_local.date().isoformat()
        for building_type in ("Hospital", "Prison"):
            if self.db.get_auto_run(guild.id, run_date, building_type):
                continue
            await self._run_auto_candidate_build(
                guild,
                building_type,
                run_date=run_date,
                timezone_name=timezone_name,
                scheduled=True,
            )
            await asyncio.sleep(5)

    async def _fetch_existing_missionchief_buildings(self) -> Tuple[List[Dict[str, Any]], str]:
        """Fetch current MissionChief buildings for duplicate checks."""
        session = await self._get_session()
        async with session.get(f"{BASE_URL}/api/buildings", timeout=45) as response:
            if response.status != 200:
                raise RuntimeError(f"MissionChief returned HTTP {response.status} for /api/buildings.")
            data = await response.json(content_type=None)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)], "live MissionChief /api/buildings"
        if isinstance(data, dict):
            for key in ("buildings", "result", "data"):
                value = data.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)], "live MissionChief /api/buildings"
        return [], "live MissionChief /api/buildings returned no list"

    def _nearest_duplicate_building(
        self,
        candidate: AutoBuildCandidate,
        existing_buildings: Iterable[Dict[str, Any]],
        *,
        radius_m: int,
    ) -> Tuple[Optional[float], Optional[int]]:
        """Return nearest same-type existing building within the configured radius."""
        target_type_id = _coerce_int(ALLIANCE_BUILDING_TYPE_IDS.get(candidate.building_type))
        best_distance = None
        best_id = None
        for record in existing_buildings or []:
            building_type = _coerce_int(_api_value(record, "building_type", "building_type_id", "buildingType"))
            if target_type_id is not None and building_type is not None and building_type != target_type_id:
                continue
            lat = _coerce_float(_api_value(record, "latitude", "lat"))
            lon = _coerce_float(_api_value(record, "longitude", "lon", "lng"))
            if lat is None or lon is None:
                continue
            distance = _haversine_meters(candidate.lat, candidate.lon, lat, lon)
            if distance <= radius_m and (best_distance is None or distance < best_distance):
                best_distance = distance
                best_id = _coerce_int(_api_value(record, "id", "building_id", "buildingId"))
        return best_distance, best_id

    async def _candidate_duplicate_context(
        self,
        guild: discord.Guild,
    ) -> Tuple[List[Dict[str, Any]], str, int]:
        """Return existing buildings and configured duplicate radius for candidate selection."""
        conf = await self.config.guild(guild).all()
        radius = int(conf.get("auto_candidate_duplicate_radius_m") or AUTO_CANDIDATE_DUPLICATE_RADIUS_METERS)
        try:
            buildings, source = await self._fetch_existing_missionchief_buildings()
            return buildings, source, max(0, radius)
        except Exception as exc:
            return [], f"duplicate check unavailable: {type(exc).__name__}: {_truncate_text(exc, 180)}", max(0, radius)

    @staticmethod
    def _available_auto_candidate_count(stats: Dict[str, int], building_type: str) -> int:
        """Return locally available candidate count for one building type."""
        return int(stats.get(f"{building_type}:available", 0) or 0)

    async def _fetch_geofabrik_extract_index(self) -> List[Dict[str, str]]:
        """Fetch the public Geofabrik extract index and return shapefile-capable extracts."""
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(GEOFABRIK_INDEX_URL) as response:
                if response.status != 200:
                    raise RuntimeError(f"Geofabrik index returned HTTP {response.status}.")
                data = await response.json(content_type=None)
        extracts: List[Dict[str, str]] = []
        for feature in data.get("features") or []:
            if not isinstance(feature, dict):
                continue
            props = feature.get("properties") or {}
            urls = props.get("urls") or {}
            extract_id = str(props.get("id") or "").strip()
            url = str(urls.get("shp") or "").strip()
            if not extract_id or not url:
                continue
            extracts.append(
                {
                    "id": extract_id,
                    "name": str(props.get("name") or extract_id).strip(),
                    "url": url,
                }
            )
        return extracts

    @staticmethod
    def _extract_content_length(headers: Any) -> Optional[int]:
        """Return Content-Length from aiohttp headers when available."""
        value = None
        with contextlib.suppress(Exception):
            value = headers.get("Content-Length")
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    async def _download_and_import_geofabrik_extract(
        self,
        extract: Dict[str, str],
        *,
        max_bytes: int,
    ) -> str:
        """Download one Geofabrik shapefile extract, import candidates, then delete the ZIP."""
        extract_id = str(extract.get("id") or "").strip()
        extract_name = str(extract.get("name") or extract_id).strip()
        url = str(extract.get("url") or "").strip()
        if not extract_id or not url:
            return "Skipped malformed Geofabrik extract record."

        temp_path = None
        downloaded = 0
        try:
            timeout = aiohttp.ClientTimeout(total=AUTO_CANDIDATE_REFILL_TIMEOUT_SECONDS)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        reason = f"Geofabrik returned HTTP {response.status}."
                        self.db.record_auto_extract_import(
                            extract_id=extract_id,
                            extract_name=extract_name,
                            url=url,
                            status="failed",
                            reason=reason,
                        )
                        return f"- {extract_name}: failed: {reason}"
                    content_length = self._extract_content_length(response.headers)
                    if content_length is not None and content_length > max_bytes:
                        reason = f"extract is {content_length / (1024 * 1024):.1f} MB; limit is {max_bytes / (1024 * 1024):.1f} MB."
                        self.db.record_auto_extract_import(
                            extract_id=extract_id,
                            extract_name=extract_name,
                            url=url,
                            status="skipped_large",
                            bytes_downloaded=content_length,
                            reason=reason,
                        )
                        return f"- {extract_name}: skipped large extract ({reason})"

                    with tempfile.NamedTemporaryFile(delete=False, suffix=".shp.zip") as handle:
                        temp_path = handle.name
                        async for chunk in response.content.iter_chunked(1024 * 1024):
                            if not chunk:
                                continue
                            downloaded += len(chunk)
                            if downloaded > max_bytes:
                                reason = f"download exceeded {max_bytes / (1024 * 1024):.1f} MB."
                                self.db.record_auto_extract_import(
                                    extract_id=extract_id,
                                    extract_name=extract_name,
                                    url=url,
                                    status="skipped_large",
                                    bytes_downloaded=downloaded,
                                    reason=reason,
                                )
                                return f"- {extract_name}: skipped large extract ({reason})"
                            handle.write(chunk)

            candidates, parse_stats = await asyncio.to_thread(
                parse_geofabrik_shp_auto_build_candidates,
                temp_path,
                extract_id=extract_id,
                extract_name=extract_name,
            )
            db_stats = self.db.upsert_auto_candidates(candidates)
            status = "completed"
            self.db.record_auto_extract_import(
                extract_id=extract_id,
                extract_name=extract_name,
                url=url,
                status=status,
                bytes_downloaded=downloaded,
                inserted=db_stats.get("inserted", 0),
                updated=db_stats.get("updated", 0),
                skipped=db_stats.get("skipped", 0),
                accepted=parse_stats.get("accepted", 0),
                rejected=parse_stats.get("rejected", 0),
                reason=None,
            )
            return (
                f"- {extract_name}: {parse_stats.get('accepted', 0):,} accepted, "
                f"{db_stats.get('inserted', 0):,} inserted, {db_stats.get('updated', 0):,} updated "
                f"({downloaded / (1024 * 1024):.1f} MB downloaded)."
            )
        except Exception as exc:
            reason = _truncate_text(exc, 500)
            self.db.record_auto_extract_import(
                extract_id=extract_id,
                extract_name=extract_name,
                url=url,
                status="failed",
                bytes_downloaded=downloaded or None,
                reason=reason,
            )
            return f"- {extract_name}: failed: {reason}"
        finally:
            if temp_path:
                with contextlib.suppress(OSError):
                    os.unlink(temp_path)

    async def _import_next_geofabrik_extracts(self, guild: discord.Guild, *, max_extracts: int) -> List[str]:
        """Import the next unprocessed Geofabrik extracts into the local candidate database."""
        conf = await self.config.guild(guild).all()
        try:
            max_mb = max(1, int(conf.get("auto_candidate_refill_max_extract_mb") or (AUTO_CANDIDATE_REFILL_MAX_EXTRACT_BYTES // (1024 * 1024))))
        except (TypeError, ValueError):
            max_mb = AUTO_CANDIDATE_REFILL_MAX_EXTRACT_BYTES // (1024 * 1024)
        max_bytes = max_mb * 1024 * 1024
        extracts = await self._fetch_geofabrik_extract_index()
        statuses = self.db.get_auto_extract_import_statuses()
        if not extracts:
            return ["Geofabrik index returned no shapefile extracts."]

        try:
            next_index = int(conf.get("auto_candidate_refill_next_region_index") or 0)
        except (TypeError, ValueError):
            next_index = 0

        lines = [
            (
                "Automatic candidate refill source: Geofabrik OSM extracts "
                f"({len(extracts):,} extracts indexed, max {max_mb:,} MB per extract)."
            )
        ]
        imported = 0
        scanned = 0
        while imported < max(1, int(max_extracts)) and scanned < len(extracts):
            extract = extracts[next_index % len(extracts)]
            next_index += 1
            scanned += 1
            status = statuses.get(str(extract.get("id") or ""))
            if status in {"completed", "skipped_large"}:
                continue
            lines.append(await self._download_and_import_geofabrik_extract(extract, max_bytes=max_bytes))
            imported += 1
            await asyncio.sleep(1)

        await self.config.guild(guild).auto_candidate_refill_next_region_index.set(next_index % len(extracts))
        if imported == 0:
            lines.append("No unprocessed Geofabrik extracts were available within this scan window.")
        return lines

    async def _refill_auto_candidates_if_needed(
        self,
        guild: discord.Guild,
        building_types: Iterable[str],
    ) -> List[str]:
        """Autonomously refill local OSM candidates when the local stock is low."""
        try:
            conf = await self.config.guild(guild).all()
        except Exception:
            conf = {}
        if not conf.get("auto_candidate_refill_enabled", True):
            return []

        normalized_types = [
            building_type
            for building_type in dict.fromkeys(str(item) for item in building_types)
            if building_type in ALLIANCE_BUILDING_TYPE_IDS
        ]
        if not normalized_types:
            return []

        try:
            minimum_available = max(
                0,
                int(conf.get("auto_candidate_refill_min_available") or AUTO_CANDIDATE_REFILL_MIN_AVAILABLE),
            )
        except (TypeError, ValueError):
            minimum_available = AUTO_CANDIDATE_REFILL_MIN_AVAILABLE
        if minimum_available <= 0:
            return []

        stats = self.db.get_auto_candidate_stats()
        needed = [
            building_type
            for building_type in normalized_types
            if self._available_auto_candidate_count(stats, building_type) < minimum_available
        ]
        if not needed:
            return []

        try:
            extracts_per_run = max(
                1,
                int(conf.get("auto_candidate_refill_regions_per_run") or AUTO_CANDIDATE_REFILL_REGIONS_PER_RUN),
            )
        except (TypeError, ValueError):
            extracts_per_run = AUTO_CANDIDATE_REFILL_REGIONS_PER_RUN

        lines = [
            f"Automatic candidate refill started: {', '.join(needed)} below {minimum_available} available."
        ]
        lines.extend(await self._import_next_geofabrik_extracts(guild, max_extracts=extracts_per_run))
        final_stats = self.db.get_auto_candidate_stats()
        lines.append(
            (
                "Local candidate stock after refill: "
                f"Hospitals {self._available_auto_candidate_count(final_stats, 'Hospital'):,}, "
                f"Prisons {self._available_auto_candidate_count(final_stats, 'Prison'):,}."
            )
        )
        return lines

    def _select_auto_candidate(
        self,
        building_type: str,
        *,
        existing_buildings: Iterable[Dict[str, Any]],
        duplicate_source: str,
        duplicate_radius_m: int,
        mark_duplicates: bool = True,
    ) -> AutoBuildPlan:
        """Choose one available candidate, skipping confirmed local duplicates."""
        candidates = self.db.get_random_auto_candidates(
            building_type,
            limit=AUTO_CANDIDATE_SELECTION_POOL,
        )
        if not candidates:
            return AutoBuildPlan(building_type=building_type, candidate=None, blocked_reason="No available candidates.")

        for candidate in candidates:
            duplicate_distance, duplicate_building_id = self._nearest_duplicate_building(
                candidate,
                existing_buildings,
                radius_m=duplicate_radius_m,
            )
            if duplicate_distance is not None:
                reason = (
                    f"Existing MissionChief building {duplicate_building_id or 'unknown'} is "
                    f"{duplicate_distance:.0f}m away."
                )
                if mark_duplicates:
                    self.db.mark_auto_candidate(candidate.candidate_id, "duplicate", reason=reason)
                continue
            return AutoBuildPlan(
                building_type=building_type,
                candidate=candidate,
                duplicate_check_source=duplicate_source,
            )

        return AutoBuildPlan(
            building_type=building_type,
            candidate=None,
            blocked_reason="All sampled candidates were confirmed duplicates.",
            duplicate_check_source=duplicate_source,
        )

    async def _build_auto_candidate_plan(
        self,
        guild: discord.Guild,
    ) -> Tuple[Optional[int], str, int, List[AutoBuildPlan], List[str]]:
        """Build a dry-run plan for both daily automatic building slots."""
        minimum = await self._get_auto_candidate_min_funds(guild)
        funds, funds_source = await self._get_current_alliance_funds()
        refill_lines: List[str] = []
        if alliance_funds_allow_auto_build(funds, funds_source, minimum):
            refill_lines = await self._refill_auto_candidates_if_needed(guild, ("Hospital", "Prison"))
        existing_buildings, duplicate_source, duplicate_radius = await self._candidate_duplicate_context(guild)
        plans = [
            self._select_auto_candidate(
                building_type,
                existing_buildings=existing_buildings,
                duplicate_source=duplicate_source,
                duplicate_radius_m=duplicate_radius,
                mark_duplicates=False,
            )
            for building_type in ("Hospital", "Prison")
        ]
        return funds, funds_source, minimum, plans, refill_lines

    async def _get_auto_candidate_min_funds(self, guild: discord.Guild) -> int:
        """Return the funds minimum for autonomous daily candidate builds."""
        value = await self.config.guild(guild).auto_candidate_min_funds()
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return AUTO_CANDIDATE_MIN_FUNDS

    def _format_candidate_autobuild_status(self, conf: Dict[str, Any], stats: Dict[str, int]) -> str:
        """Format candidate auto-build configuration and counts."""
        return "\n".join(
            [
                "Daily candidate auto-build",
                "Candidate source: local SQLite database",
                "Automatic refill: Geofabrik OSM extracts when local stock is low",
                f"Enabled: {bool(conf.get('auto_candidate_build_enabled'))}",
                f"Time: {conf.get('auto_candidate_time') or AUTO_CANDIDATE_DEFAULT_TIME}",
                f"Timezone: {conf.get('auto_candidate_timezone') or AUTO_CANDIDATE_DEFAULT_TIMEZONE}",
                f"Minimum funds: {int(conf.get('auto_candidate_min_funds') or AUTO_CANDIDATE_MIN_FUNDS):,} credits",
                f"Duplicate radius: {int(conf.get('auto_candidate_duplicate_radius_m') or AUTO_CANDIDATE_DUPLICATE_RADIUS_METERS)}m",
                f"Refill enabled: {bool(conf.get('auto_candidate_refill_enabled', True))}",
                (
                    "Refill minimum stock: "
                    f"{int(conf.get('auto_candidate_refill_min_available') or AUTO_CANDIDATE_REFILL_MIN_AVAILABLE):,} "
                    "available per type"
                ),
                (
                    "Refill extracts per run: "
                    f"{int(conf.get('auto_candidate_refill_regions_per_run') or AUTO_CANDIDATE_REFILL_REGIONS_PER_RUN):,}"
                ),
                (
                    "Refill max extract size: "
                    f"{int(conf.get('auto_candidate_refill_max_extract_mb') or (AUTO_CANDIDATE_REFILL_MAX_EXTRACT_BYTES // (1024 * 1024))):,} MB"
                ),
                "",
                "Candidates:",
                f"- Hospitals available: {stats.get('Hospital:available', 0):,} / {stats.get('Hospital:total', 0):,}",
                f"- Prisons available: {stats.get('Prison:available', 0):,} / {stats.get('Prison:total', 0):,}",
                f"- Used: {stats.get('total:used', 0):,}",
                f"- Duplicates: {stats.get('total:duplicate', 0):,}",
                f"- Failed: {stats.get('total:failed', 0):,}",
            ]
        )

    def _request_from_auto_candidate(self, candidate: AutoBuildCandidate) -> BuildingRequest:
        """Convert a candidate into a normal BuildingRequest model."""
        return BuildingRequest(
            user_id=0,
            username="BuildingManager AutoBuild",
            building_type=candidate.building_type,
            building_name=candidate.name,
            location_input=candidate.osm_url,
            coordinates=candidate.coordinates,
            address=candidate.address,
            country=candidate.country,
            region=candidate.region,
            maps_url=candidate.osm_url,
            notes=(
                "Automatic daily alliance building candidate. "
                f"Source: {candidate.source} {candidate.source_id}."
            ),
        )

    async def _run_auto_candidate_build(
        self,
        guild: discord.Guild,
        building_type: str,
        *,
        run_date: Optional[str] = None,
        timezone_name: Optional[str] = None,
        scheduled: bool = False,
        force: bool = False,
    ) -> str:
        """Run one automatic candidate build slot."""
        timezone_name = timezone_name or AUTO_CANDIDATE_DEFAULT_TIMEZONE
        if run_date is None:
            try:
                run_date = datetime.now(ZoneInfo(timezone_name)).date().isoformat()
            except Exception:
                run_date = datetime.now(timezone.utc).date().isoformat()

        if not force and self.db.get_auto_run(guild.id, run_date, building_type):
            return f"{building_type}: already processed for {run_date}."

        minimum = await self._get_auto_candidate_min_funds(guild)
        funds, funds_source = await self._get_current_alliance_funds()
        if not alliance_funds_allow_auto_build(funds, funds_source, minimum):
            current = f"{funds:,} credits" if funds is not None else "unknown"
            return (
                f"{building_type}: blocked by funds safety rule. Current funds: {current}; "
                f"source: {funds_source}; required: {minimum:,} credits."
            )

        refill_lines = await self._refill_auto_candidates_if_needed(guild, (building_type,))
        existing_buildings, duplicate_source, duplicate_radius = await self._candidate_duplicate_context(guild)
        plan = self._select_auto_candidate(
            building_type,
            existing_buildings=existing_buildings,
            duplicate_source=duplicate_source,
            duplicate_radius_m=duplicate_radius,
        )
        if not plan.candidate:
            reason = plan.blocked_reason or "No candidate selected."
            if refill_lines:
                reason = f"{reason} Automatic refill result: {' | '.join(refill_lines[-3:])}"
            self.db.record_auto_run(
                guild_id=guild.id,
                run_date=run_date,
                building_type=building_type,
                candidate_id=None,
                funds=funds,
                funds_source=funds_source,
                result="no_candidate",
                reason=reason,
            )
            await self._send_auto_candidate_build_log(
                guild,
                building_type=building_type,
                candidate=None,
                funds=funds,
                funds_source=funds_source,
                result="No candidate",
                reason=reason,
                scheduled=scheduled,
            )
            return f"{building_type}: {reason}"

        candidate = plan.candidate
        req = self._request_from_auto_candidate(candidate)
        request_id = self._store_building_request(guild, req)
        self.db.update_request_status(request_id, "auto_selected")
        self.db.add_action(
            request_id=request_id,
            guild_id=guild.id,
            admin_user_id=None,
            admin_username="BuildingManager",
            action_type="auto_candidate_selected",
            previous_values=f"{candidate.source} {candidate.source_id}",
        )

        create_result, automation_message = await self._create_and_queue_approved_building(guild, req)
        building_id = create_result.building_id
        final_status = "created" if create_result.ok else "auto_create_failed"
        self.db.update_request_status(request_id, final_status)
        self.db.add_action(
            request_id=request_id,
            guild_id=guild.id,
            admin_user_id=None,
            admin_username="BuildingManager",
            action_type="auto_candidate_created" if create_result.ok else "auto_candidate_create_failed",
            previous_values=None if create_result.ok else create_result.reason[:900],
        )

        if create_result.ok:
            self.db.mark_auto_candidate(
                candidate.candidate_id,
                "used",
                reason=f"Built automatically on {run_date}.",
                missionchief_building_id=building_id,
            )
        else:
            self.db.mark_auto_candidate(
                candidate.candidate_id,
                "failed",
                reason=_truncate_text(create_result.reason, 900),
            )

        self.db.record_auto_run(
            guild_id=guild.id,
            run_date=run_date,
            building_type=building_type,
            candidate_id=candidate.candidate_id,
            funds=funds,
            funds_source=funds_source,
            result="created" if create_result.ok else "failed",
            request_id=request_id,
            missionchief_building_id=building_id,
            reason=create_result.reason,
        )
        await self._send_auto_candidate_build_log(
            guild,
            building_type=building_type,
            candidate=candidate,
            funds=funds,
            funds_source=funds_source,
            result="Created" if create_result.ok else "Failed",
            reason=create_result.reason,
            scheduled=scheduled,
            request_id=request_id,
            missionchief_building_id=building_id,
            automation_message=automation_message,
        )

        if create_result.ok:
            return f"{building_type}: created {candidate.name}."
        return f"{building_type}: failed to create {candidate.name}: {create_result.reason}"

    async def _send_auto_candidate_build_log(
        self,
        guild: discord.Guild,
        *,
        building_type: str,
        candidate: Optional[AutoBuildCandidate],
        funds: Optional[int],
        funds_source: str,
        result: str,
        reason: str,
        scheduled: bool,
        request_id: Optional[int] = None,
        missionchief_building_id: Optional[int] = None,
        automation_message: Optional[str] = None,
    ) -> None:
        """Log one autonomous candidate build result."""
        conf = await self.config.guild(guild).all()
        log_channel = await self._resolve_channel(guild, conf.get("log_channel_id"))
        if log_channel is None:
            log_channel = await self._resolve_channel(guild, conf.get("admin_channel_id"))
        if log_channel is None:
            return

        ok = result.casefold() == "created"
        embed = discord.Embed(
            title=f"Automatic {building_type} build: {result}",
            color=discord.Color.green() if ok else discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Mode", value="Scheduled" if scheduled else "Manual", inline=True)
        funds_text = f"{funds:,} credits" if funds is not None else "unknown"
        embed.add_field(name="Alliance Funds", value=funds_text, inline=True)
        embed.add_field(name="Funds Source", value=_truncate_discord_text(funds_source, 200), inline=False)
        if candidate:
            embed.add_field(name="Candidate", value=_truncate_discord_text(candidate.name, 200), inline=False)
            embed.add_field(name="Source", value=f"{candidate.source} {candidate.source_id}", inline=True)
            embed.add_field(name="Coordinates", value=candidate.coordinates, inline=True)
            region_text = ", ".join(part for part in (candidate.region, candidate.country) if part)
            if region_text:
                embed.add_field(name="Region", value=_truncate_discord_text(region_text, 200), inline=True)
            embed.add_field(name="OSM", value=f"[Open]({candidate.osm_url})", inline=False)
        embed.add_field(name="Result", value=_truncate_discord_text(reason, 900), inline=False)
        if automation_message:
            embed.add_field(name="Post-Creation Automation", value=_truncate_discord_text(automation_message, 900), inline=False)
        if request_id:
            embed.add_field(name="Request ID", value=str(request_id), inline=True)
        if missionchief_building_id:
            embed.add_field(name="MissionChief Building ID", value=str(missionchief_building_id), inline=True)
        await log_channel.send(embed=embed)

    async def _process_building_automation_job(self, job_id: int) -> Optional[BuildingAutomationResult]:
        """Run and persist one post-creation automation job."""
        job = self.db.get_automation_job(job_id)
        if not job or job.status == "completed":
            return None

        guild = self.bot.get_guild(job.guild_id)
        if guild is None:
            result = BuildingAutomationResult(
                ok=True,
                completed=False,
                wait=True,
                reason="Guild is unavailable; waiting before running building automation.",
                actions=[],
            )
            self.db.update_automation_job(job.job_id, result)
            return result

        minimum_funds = await self._get_min_alliance_funds(guild)
        current_funds, funds_source = await self._get_current_alliance_funds()
        if not alliance_funds_allow_auto_build(current_funds, funds_source, minimum_funds):
            current = f"{current_funds:,} credits" if current_funds is not None else "unknown"
            result = BuildingAutomationResult(
                ok=True,
                completed=False,
                wait=True,
                reason=(
                    "Alliance funds safety hold: "
                    f"current funds are {current} from {funds_source}; "
                    f"required live minimum is {minimum_funds:,} credits."
                ),
                actions=[],
            )
            self.db.update_automation_job(job.job_id, result)
            self.db.add_action(
                request_id=job.request_id,
                guild_id=job.guild_id,
                admin_user_id=None,
                admin_username="BuildingManager",
                action_type="automation_waiting",
                previous_values=_truncate_text(result.reason, 900),
            )
            return result

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

                        for _ in range(BUILDING_AUTOMATION_MAX_SCRIPT_STEPS_PER_RUN):
                            prepare_config = {
                                "buildingId": str(job.building_id),
                                "buildingType": str(job.building_type),
                                "targetTax": str(job.target_tax),
                                "maxHospitalLevel": ALLIANCE_BUILDING_TARGET_HOSPITAL_LEVEL,
                                "taxComplete": bool(tax_complete),
                                "levelComplete": bool(level_complete),
                                "extensionsComplete": bool(extensions_complete),
                                "extensionsStartedThisRun": extensions_started_this_run,
                                "maxExtensionStarts": BUILDING_AUTOMATION_MAX_EXTENSION_STARTS_PER_RUN,
                            }
                            last_prepare = await page.evaluate(BUILDING_AUTOMATION_DIRECT_SCRIPT, prepare_config)
                            details["last_prepare"] = last_prepare
                            if last_prepare.get("status") is not None:
                                with contextlib.suppress(TypeError, ValueError):
                                    last_status = int(last_prepare.get("status"))
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
                            if action == "level_already_max" or action == "level_not_applicable":
                                level_complete = True
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

                            actions.append(label)
                            if action == "set_tax":
                                tax_complete = True
                            elif action == "start_level_upgrade":
                                level_complete = True
                            elif action == "start_extension":
                                extensions_started_this_run += 1
                            await page.wait_for_timeout(1200)

                        return BuildingAutomationResult(
                            True,
                            False,
                            True,
                            "Internal safety limit reached for this run; queued for the next pass.",
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
                "Request a new alliance Hospital or Prison/Jail placement by clicking the button below.\n\n"
                "Accepted location formats:\n"
                "- Google Maps place link\n"
                "- Google Maps short link, for example `https://maps.app.goo.gl/...`\n\n"
                "Only paste the link. The building type and name are detected automatically. "
                "Clinics, doctor offices, museums, historic sites, courthouses, and police stations are rejected. "
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
            f"Minimum alliance funds before auto-build: {int(conf.get('min_alliance_funds') or ALLIANCE_BUILDING_MIN_FUNDS):,} credits\n"
            f"MissionChief board polling: {'enabled' if conf.get('board_poll_enabled') else 'disabled'} "
            f"(thread {conf.get('board_thread_id') or BOARD_THREAD_ID})\n"
            f"MissionChief board auto-accept: {'enabled' if conf.get('board_auto_accept_enabled', True) else 'disabled'} "
            f"(requires known tax >= {BUILDING_BOARD_MIN_AUTO_ACCEPT_TAX:.1f}%)\n"
            f"Daily candidate auto-build: {'enabled' if conf.get('auto_candidate_build_enabled') else 'disabled'} "
            f"at {conf.get('auto_candidate_time') or AUTO_CANDIDATE_DEFAULT_TIME} "
            f"{conf.get('auto_candidate_timezone') or AUTO_CANDIDATE_DEFAULT_TIMEZONE} "
            f"(minimum {int(conf.get('auto_candidate_min_funds') or AUTO_CANDIDATE_MIN_FUNDS):,} credits)\n"
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

    @buildset.command(name="minfunds")
    @commands.admin()
    @commands.guild_only()
    async def minfunds(self, ctx: commands.Context, credits: int = ALLIANCE_BUILDING_MIN_FUNDS):
        """Set the minimum alliance funds required before auto-building."""
        if credits < 0:
            await ctx.send("Minimum funds cannot be negative.")
            return
        await self.config.guild(ctx.guild).min_alliance_funds.set(int(credits))
        await ctx.send(f"Minimum alliance funds before auto-build set to {int(credits):,} credits.")

    @buildset.command(name="fundscheck")
    @commands.admin()
    @commands.guild_only()
    async def fundscheck(self, ctx: commands.Context):
        """Check current alliance funds and both automatic building safety thresholds."""
        request_minimum = await self._get_min_alliance_funds(ctx.guild)
        autonomous_minimum = await self._get_auto_candidate_min_funds(ctx.guild)
        async with ctx.typing():
            funds, source = await self._get_current_alliance_funds()
        request_allowed = alliance_funds_allow_auto_build(funds, source, request_minimum)
        autonomous_allowed = alliance_funds_allow_auto_build(funds, source, autonomous_minimum)
        funds_text = f"{funds:,} credits" if funds is not None else "unknown"
        await ctx.send(
            box(
                "\n".join(
                    [
                        f"Current alliance funds: {funds_text}",
                        f"Source: {source}",
                        "",
                        "Member/admin request auto-build",
                        f"Required minimum: {request_minimum:,} credits",
                        f"Status: {'ALLOWED' if request_allowed else 'BLOCKED'}",
                        "",
                        "Autonomous daily candidate build",
                        f"Required minimum: {autonomous_minimum:,} credits",
                        f"Status: {'ALLOWED' if autonomous_allowed else 'BLOCKED'}",
                    ]
                ),
                lang="text",
            )
        )

    @buildset.command(name="boardauto")
    @commands.admin()
    @commands.guild_only()
    async def board_auto_accept(self, ctx: commands.Context, state: str = "status"):
        """Manage automatic MissionChief board building approval. Use on/off/status."""
        normalized = str(state or "status").casefold().strip()
        if normalized in {"on", "enable", "enabled"}:
            await self.config.guild(ctx.guild).board_auto_accept_enabled.set(True)
            await ctx.send(
                f"Building board auto-accept enabled. Known requester tax must be at least "
                f"{BUILDING_BOARD_MIN_AUTO_ACCEPT_TAX:.1f}%."
            )
            return
        if normalized in {"off", "disable", "disabled"}:
            await self.config.guild(ctx.guild).board_auto_accept_enabled.set(False)
            await ctx.send("Building board auto-accept disabled. Board requests will go to admin review.")
            return
        conf = await self.config.guild(ctx.guild).all()
        await ctx.send(
            box(
                "\n".join(
                    [
                        "Building board auto-accept status",
                        f"Enabled: {bool(conf.get('board_auto_accept_enabled', True))}",
                        f"Minimum known tax: {BUILDING_BOARD_MIN_AUTO_ACCEPT_TAX:.1f}%",
                        "Known tax below minimum: rejected automatically",
                        "Unknown tax: sent to admin review",
                    ]
                ),
                lang="text",
            )
        )

    @buildset.command(name="geocodeapikey")
    @commands.is_owner()
    async def geocodeapikey(self, ctx: commands.Context, api_key: str = ""):
        """Set or clear the optional geocode.maps.co API key for BuildingManager."""
        try:
            await ctx.message.delete()
        except (discord.Forbidden, discord.HTTPException, AttributeError):
            pass

        clean_key = str(api_key or "").strip()
        await self.config.geocode_maps_api_key.set(clean_key)
        await ctx.send("BuildingManager geocode API key updated." if clean_key else "BuildingManager geocode API key cleared.")

    @buildset.command(name="fundsqueue")
    @commands.admin()
    @commands.guild_only()
    async def fundsqueue(self, ctx: commands.Context):
        """Show approved building requests waiting for enough alliance funds."""
        rows = [
            row
            for row in self.db.get_requests_by_status("awaiting_funds", limit=10)
            if int(row["guild_id"]) == ctx.guild.id
        ]
        if not rows:
            await ctx.send("No approved building requests are waiting for alliance funds.")
            return
        lines = ["Building requests waiting for alliance funds:"]
        for row in rows:
            lines.append(
                f"- Request {int(row['request_id'])}: {row['building_type']} - {row['building_name']} "
                f"(requested by {row['username']})"
            )
        await ctx.send(box("\n".join(lines), lang="text"))

    @buildset.command(name="fundsrun")
    @commands.admin()
    @commands.guild_only()
    async def fundsrun(self, ctx: commands.Context):
        """Try to process one approved request waiting for alliance funds now."""
        async with ctx.typing():
            processed = await self._process_waiting_for_funds_queue(ctx.guild.id)
            minimum = await self._get_min_alliance_funds(ctx.guild)
            funds, source = await self._get_current_alliance_funds()

        if processed:
            await ctx.send("Processed one approved building request from the alliance funds queue.")
            return

        rows = [
            row
            for row in self.db.get_requests_by_status("awaiting_funds", limit=10)
            if int(row["guild_id"]) == ctx.guild.id
        ]
        allowed = alliance_funds_allow_auto_build(funds, source, minimum)
        funds_text = f"{funds:,} credits" if funds is not None else "unknown"
        await ctx.send(
            box(
                "\n".join(
                    [
                        "No building request was processed from the alliance funds queue.",
                        f"Queued requests in this guild: {len(rows)}",
                        f"Current alliance funds: {funds_text}",
                        f"Source: {source}",
                        f"Required minimum: {minimum:,} credits",
                        f"Auto-build allowed: {'yes' if allowed else 'no'}",
                    ]
                ),
                lang="text",
            )
        )

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

    @buildset.command(name="board")
    @commands.admin()
    @commands.guild_only()
    async def board_polling(self, ctx: commands.Context, state: str = "status"):
        """Manage MissionChief board building request polling. Use on/off/status/reset."""
        state = str(state or "status").casefold()
        if state in {"on", "enable", "enabled"}:
            await self.config.guild(ctx.guild).board_poll_enabled.set(True)
            await ctx.send("Building board polling enabled.")
            return
        if state in {"off", "disable", "disabled"}:
            await self.config.guild(ctx.guild).board_poll_enabled.set(False)
            await ctx.send("Building board polling disabled.")
            return
        if state == "reset":
            thread_id = int((await self.config.guild(ctx.guild).board_thread_id()) or BOARD_THREAD_ID)
            await self.config.guild(ctx.guild).board_last_seen_post_id.set(None)
            key = str(thread_id)
            async with self.config.board_thread_states() as states:
                state_data = dict(states.get(key) or {})
                state_data.pop("last_seen_post_id", None)
                state_data["processed_post_ids"] = []
                states[key] = state_data
            await ctx.send("Building board baseline reset. The next poll will baseline the latest post without processing older posts.")
            return

        conf = await self.config.guild(ctx.guild).all()
        await ctx.send(
            box(
                "\n".join(
                    [
                        "Building board polling status",
                        f"Enabled: {bool(conf.get('board_poll_enabled'))}",
                        f"Thread ID: {conf.get('board_thread_id') or BOARD_THREAD_ID}",
                        f"Last seen post ID: {conf.get('board_last_seen_post_id') or 'not set'}",
                        f"Pending cleanup posts: {len(conf.get('board_pending_deletions') or [])}",
                        f"Interval: {BOARD_POLL_SECONDS // 60} minutes",
                    ]
                ),
                lang="text",
            )
        )

    @buildset.command(name="boardthread")
    @commands.admin()
    @commands.guild_only()
    async def board_thread(self, ctx: commands.Context, thread_id: int = BOARD_THREAD_ID):
        """Set the MissionChief alliance thread ID used for board building requests."""
        await self.config.guild(ctx.guild).board_thread_id.set(int(thread_id))
        await self.config.guild(ctx.guild).board_last_seen_post_id.set(None)
        await ctx.send(
            f"Building board thread set to `{int(thread_id)}`. "
            "The next poll will baseline the latest post without processing older posts."
        )

    @buildset.command(name="boardguide")
    @commands.admin()
    @commands.guild_only()
    async def board_guide(self, ctx: commands.Context, state: str = "status"):
        """Manage the MissionChief building request guide post. Use on/off/status/reset/sync."""
        state = str(state or "status").casefold()
        if state in {"on", "enable", "enabled"}:
            await self.config.guild(ctx.guild).board_guide_enabled.set(True)
            await ctx.send("Building board guide sync enabled.")
            return
        if state in {"off", "disable", "disabled"}:
            await self.config.guild(ctx.guild).board_guide_enabled.set(False)
            await ctx.send("Building board guide sync disabled.")
            return
        if state == "reset":
            await self.config.guild(ctx.guild).board_guide_thread_id.set(None)
            await self.config.guild(ctx.guild).board_guide_post_id.set(None)
            await self.config.guild(ctx.guild).board_guide_content_hash.set(None)
            await ctx.send("Building board guide tracking reset. The next sync will find or create the guide post.")
            return
        if state == "sync":
            async with ctx.typing():
                changed = await self._sync_building_board_guide_for_guild(ctx.guild, force=True)
            await ctx.send("Building board guide synced." if changed else "Building board guide sync did not change anything.")
            return

        conf = await self.config.guild(ctx.guild).all()
        await ctx.send(
            box(
                "\n".join(
                    [
                        "Building board guide status",
                        f"Enabled: {bool(conf.get('board_guide_enabled'))}",
                        f"Request thread ID: {conf.get('board_thread_id') or BOARD_THREAD_ID}",
                        f"Managed thread ID: {conf.get('board_guide_thread_id') or 'not set'}",
                        f"Managed guide post ID: {conf.get('board_guide_post_id') or 'not set'}",
                        f"Sync interval: {BOARD_GUIDE_SYNC_SECONDS // 60} minutes",
                    ]
                ),
                lang="text",
            )
        )

    @buildset.group(name="autobuild", invoke_without_command=True)
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild(self, ctx: commands.Context):
        """Manage automatic daily hospital/prison building from local candidates."""
        conf = await self.config.guild(ctx.guild).all()
        stats = self.db.get_auto_candidate_stats()
        await ctx.send(box(self._format_candidate_autobuild_status(conf, stats), lang="text"))

    @candidate_autobuild.command(name="status")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_status(self, ctx: commands.Context):
        """Show automatic candidate build configuration and local candidate counts."""
        conf = await self.config.guild(ctx.guild).all()
        stats = self.db.get_auto_candidate_stats()
        await ctx.send(box(self._format_candidate_autobuild_status(conf, stats), lang="text"))

    @candidate_autobuild.command(name="enable")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_enable(self, ctx: commands.Context):
        """Enable daily candidate auto-building."""
        await self.config.guild(ctx.guild).auto_candidate_build_enabled.set(True)
        await ctx.send("Daily candidate auto-build enabled.")

    @candidate_autobuild.command(name="disable")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_disable(self, ctx: commands.Context):
        """Disable daily candidate auto-building."""
        await self.config.guild(ctx.guild).auto_candidate_build_enabled.set(False)
        await ctx.send("Daily candidate auto-build disabled.")

    @candidate_autobuild.command(name="time")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_time(
        self,
        ctx: commands.Context,
        time_text: str = AUTO_CANDIDATE_DEFAULT_TIME,
        timezone_name: str = AUTO_CANDIDATE_DEFAULT_TIMEZONE,
    ):
        """Set daily candidate auto-build time, for example 07:00 America/New_York."""
        if not re.fullmatch(r"\d{1,2}:\d{2}", str(time_text or "")):
            await ctx.send("Use HH:MM format, for example `07:00`.")
            return
        hour, minute = (int(part) for part in time_text.split(":", 1))
        if not 0 <= hour <= 23 or not 0 <= minute <= 59:
            await ctx.send("Time must be a valid 24-hour HH:MM value.")
            return
        try:
            ZoneInfo(timezone_name)
        except Exception:
            await ctx.send("Unknown timezone. Use an IANA timezone like `America/New_York`.")
            return
        await self.config.guild(ctx.guild).auto_candidate_time.set(f"{hour:02d}:{minute:02d}")
        await self.config.guild(ctx.guild).auto_candidate_timezone.set(timezone_name)
        await ctx.send(f"Daily candidate auto-build time set to {hour:02d}:{minute:02d} {timezone_name}.")

    @candidate_autobuild.command(name="minfunds")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_minfunds(self, ctx: commands.Context, credits: int = AUTO_CANDIDATE_MIN_FUNDS):
        """Set minimum alliance funds before autonomous daily building."""
        if credits < 0:
            await ctx.send("Minimum funds cannot be negative.")
            return
        await self.config.guild(ctx.guild).auto_candidate_min_funds.set(int(credits))
        await ctx.send(f"Daily candidate auto-build minimum funds set to {int(credits):,} credits.")

    @candidate_autobuild.command(name="fundscheck")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_fundscheck(self, ctx: commands.Context):
        """Check current funds against the autonomous daily build threshold."""
        minimum = await self._get_auto_candidate_min_funds(ctx.guild)
        async with ctx.typing():
            funds, source = await self._get_current_alliance_funds()
        allowed = alliance_funds_allow_auto_build(funds, source, minimum)
        funds_text = f"{funds:,} credits" if funds is not None else "unknown"
        await ctx.send(
            box(
                "\n".join(
                    [
                        "Autonomous daily candidate build funds check",
                        f"Current alliance funds: {funds_text}",
                        f"Source: {source}",
                        f"Required minimum: {minimum:,} credits",
                        f"Status: {'ALLOWED' if allowed else 'BLOCKED'}",
                    ]
                ),
                lang="text",
            )
        )

    @candidate_autobuild.command(name="radius")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_radius(
        self,
        ctx: commands.Context,
        meters: int = AUTO_CANDIDATE_DUPLICATE_RADIUS_METERS,
    ):
        """Set duplicate detection radius in meters."""
        if meters < 0:
            await ctx.send("Duplicate radius cannot be negative.")
            return
        await self.config.guild(ctx.guild).auto_candidate_duplicate_radius_m.set(int(meters))
        await ctx.send(f"Duplicate radius set to {int(meters):,} meters.")

    @candidate_autobuild.command(name="maxextractmb")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_max_extract_mb(self, ctx: commands.Context, megabytes: int = 350):
        """Set the maximum Geofabrik extract ZIP size for automatic refill."""
        if megabytes < 1:
            await ctx.send("Maximum extract size must be at least 1 MB.")
            return
        await self.config.guild(ctx.guild).auto_candidate_refill_max_extract_mb.set(int(megabytes))
        await ctx.send(f"Automatic candidate refill max extract size set to {int(megabytes):,} MB.")

    @candidate_autobuild.command(name="importjson")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_import_json(self, ctx: commands.Context):
        """Import candidates from an attached Overpass JSON export."""
        if not ctx.message.attachments:
            await ctx.send("Attach an Overpass JSON file to this command.")
            return
        attachment = ctx.message.attachments[0]
        async with ctx.typing():
            try:
                payload = await attachment.read()
                data = json.loads(payload.decode("utf-8-sig"))
            except Exception as exc:
                await ctx.send(f"Could not read JSON attachment: {exc}")
                return
            candidates, parse_stats = parse_overpass_auto_build_candidates(data)
            db_stats = self.db.upsert_auto_candidates(candidates)
        await ctx.send(
            box(
                "\n".join(
                    [
                        "Candidate import complete.",
                        f"Source elements: {parse_stats['source_elements']:,}",
                        f"Accepted candidates: {parse_stats['accepted']:,}",
                        f"Rejected source elements: {parse_stats['rejected']:,}",
                        f"Inserted: {db_stats['inserted']:,}",
                        f"Updated: {db_stats['updated']:,}",
                        f"Skipped: {db_stats['skipped']:,}",
                    ]
                ),
                lang="text",
            )
        )

    @candidate_autobuild.command(name="importoverpass")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_import_overpass(
        self,
        ctx: commands.Context,
        south: float,
        west: float,
        north: float,
        east: float,
        building_type: str = "both",
    ):
        """Download OSM hospital/prison candidates through public Overpass for one bounding box."""
        try:
            query = build_overpass_candidate_query(
                float(south),
                float(west),
                float(north),
                float(east),
                building_type,
            )
        except ValueError as exc:
            await ctx.send(str(exc))
            return

        notice = overpass_import_area_notice(float(south), float(west), float(north), float(east))
        async with ctx.typing():
            try:
                timeout = aiohttp.ClientTimeout(total=240)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(OVERPASS_API_URL, data={"data": query}) as response:
                        if response.status != 200:
                            text = await response.text()
                            await ctx.send(format_overpass_http_error(response.status, text, building_type=building_type))
                            return
                        data = await response.json(content_type=None)
            except Exception as exc:
                await ctx.send(f"Overpass import failed: {exc}")
                return

            candidates, parse_stats = parse_overpass_auto_build_candidates(data)
            db_stats = self.db.upsert_auto_candidates(candidates)

        lines = [
            "Overpass candidate import complete.",
            "Network source: public Overpass API",
            "Stored result: local SQLite candidate database",
            "Automatic dry-run/run uses local data first and refills from OSM only when local stock is low.",
            f"Source elements: {parse_stats['source_elements']:,}",
            f"Accepted candidates: {parse_stats['accepted']:,}",
            f"Rejected source elements: {parse_stats['rejected']:,}",
            f"Inserted: {db_stats['inserted']:,}",
            f"Updated: {db_stats['updated']:,}",
            f"Skipped: {db_stats['skipped']:,}",
            f"Import type: {building_type}",
        ]
        if notice:
            lines.append(f"Note: {notice}")
        await ctx.send(
            box(
                "\n".join(lines),
                lang="text",
            )
        )

    @candidate_autobuild.command(name="importextracts")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_import_extracts(self, ctx: commands.Context, max_extracts: int = 1):
        """Import the next unprocessed Geofabrik extracts into the local candidate database."""
        if max_extracts < 1:
            await ctx.send("Import at least one extract.")
            return
        max_extracts = min(int(max_extracts), 10)
        async with ctx.typing():
            lines = await self._import_next_geofabrik_extracts(ctx.guild, max_extracts=max_extracts)
            stats = self.db.get_auto_candidate_stats()
        lines.extend(
            [
                "",
                "Local candidate stock:",
                f"- Hospitals available: {stats.get('Hospital:available', 0):,} / {stats.get('Hospital:total', 0):,}",
                f"- Prisons available: {stats.get('Prison:available', 0):,} / {stats.get('Prison:total', 0):,}",
            ]
        )
        await ctx.send(box("\n".join(lines), lang="text"))

    @candidate_autobuild.command(name="purgegeofabrik")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_purge_geofabrik(
        self,
        ctx: commands.Context,
        confirmation: str = "",
        include_used: bool = False,
    ):
        """Remove Geofabrik candidates and reset extract import history."""
        if str(confirmation or "").casefold() != "confirm":
            await ctx.send(
                "This removes non-used Geofabrik candidates and resets Geofabrik extract import history. "
                f"Run `{ctx.clean_prefix}buildset autobuild purgegeofabrik confirm` to continue."
            )
            return
        stats = self.db.purge_geofabrik_auto_candidates(include_used=bool(include_used))
        remaining = self.db.get_auto_candidate_stats()
        lines = [
            "Geofabrik candidate cleanup complete.",
            f"Deleted candidates: {stats.get('deleted_candidates', 0):,}",
            f"Deleted extract import records: {stats.get('deleted_extract_imports', 0):,}",
            f"Included used candidates: {bool(include_used)}",
            "",
            "Before cleanup:",
            f"- Available: {stats.get('candidates_available', 0):,}",
            f"- Duplicate: {stats.get('candidates_duplicate', 0):,}",
            f"- Failed: {stats.get('candidates_failed', 0):,}",
            f"- Used: {stats.get('candidates_used', 0):,}",
            "",
            "Remaining local candidate stock:",
            f"- Hospitals available: {remaining.get('Hospital:available', 0):,} / {remaining.get('Hospital:total', 0):,}",
            f"- Prisons available: {remaining.get('Prison:available', 0):,} / {remaining.get('Prison:total', 0):,}",
        ]
        await ctx.send(box("\n".join(lines), lang="text"))

    @candidate_autobuild.command(name="dryrun")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_dryrun(self, ctx: commands.Context):
        """Show what daily candidate auto-build would do without creating buildings."""
        async with ctx.typing():
            funds, funds_source, minimum, plans, refill_lines = await self._build_auto_candidate_plan(ctx.guild)
        funds_text = f"{funds:,} credits" if funds is not None else "unknown"
        allowed = alliance_funds_allow_auto_build(funds, funds_source, minimum)
        lines = [
            "Daily candidate auto-build dry run",
            f"Current alliance funds: {funds_text}",
            f"Funds source: {funds_source}",
            f"Required minimum: {minimum:,} credits",
            f"Funds check: {'PASS' if allowed else 'BLOCKED'}",
            "",
        ]
        if refill_lines:
            lines.append("Automatic candidate refill:")
            lines.extend(refill_lines)
            lines.append("")
        for plan in plans:
            lines.append(f"{plan.building_type}:")
            if not plan.candidate:
                lines.append(f"- Blocked: {plan.blocked_reason or 'No candidate selected.'}")
                continue
            candidate = plan.candidate
            lines.extend(
                [
                    f"- Candidate: {candidate.name}",
                    f"- Source: {candidate.source} {candidate.source_id}",
                    f"- Coordinates: {candidate.coordinates}",
                    f"- Region: {', '.join(part for part in (candidate.region, candidate.country) if part) or 'unknown'}",
                    f"- Duplicate check: {plan.duplicate_check_source}",
                ]
            )
        await ctx.send(box("\n".join(lines), lang="text"))

    @candidate_autobuild.command(name="run")
    @commands.admin()
    @commands.guild_only()
    async def candidate_autobuild_run(
        self,
        ctx: commands.Context,
        building_type: str = "both",
        force: bool = False,
    ):
        """Run candidate auto-build now for hospital, prison, or both."""
        normalized = str(building_type or "both").casefold()
        if normalized in {"both", "all"}:
            building_types = ["Hospital", "Prison"]
        elif normalized in {"hospital", "hospitals"}:
            building_types = ["Hospital"]
        elif normalized in {"prison", "prisons", "jail", "jails"}:
            building_types = ["Prison"]
        else:
            await ctx.send("Use `hospital`, `prison`, or `both`.")
            return

        async with ctx.typing():
            results = [
                await self._run_auto_candidate_build(ctx.guild, item, scheduled=False, force=bool(force))
                for item in building_types
            ]
        await ctx.send(box("\n".join(results), lang="text"))

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
