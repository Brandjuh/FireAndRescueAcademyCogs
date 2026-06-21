"""
MissionChief possible mission fetcher.

The possible missions page is rendered from the public einsaetze.json endpoint. We use the
JSON endpoint as source of truth and keep the page URL only for user-facing links.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlencode

import aiohttp


MISSION_JSON_URL = "https://www.missionchief.com/einsaetze.json"
MISSION_DETAIL_BASE_URL = "https://www.missionchief.com/einsaetze"


class MissionFetcher:
    """Fetch and normalize MissionChief possible mission data."""

    def __init__(self) -> None:
        self.session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> None:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    async def fetch_missions(self) -> list[dict[str, Any]]:
        """Fetch and normalize all possible missions."""
        await self._ensure_session()
        assert self.session is not None

        async with self.session.get(MISSION_JSON_URL) as response:
            response.raise_for_status()
            payload = await response.json()

        return self.normalize_missions(payload)

    @staticmethod
    def normalize_missions(payload: Any) -> list[dict[str, Any]]:
        """Normalize MissionChief's list or dict payload into mission dictionaries."""
        if isinstance(payload, list):
            missions = payload
        elif isinstance(payload, dict):
            if isinstance(payload.get("missions"), list):
                missions = payload["missions"]
            else:
                missions = []
                for mission_id, mission_data in payload.items():
                    if not isinstance(mission_data, dict):
                        continue
                    mission = dict(mission_data)
                    mission.setdefault("id", str(mission_id))
                    missions.append(mission)
        else:
            raise ValueError(f"Unexpected mission JSON payload: {type(payload)!r}")

        normalized = []
        for index, mission_data in enumerate(missions):
            if not isinstance(mission_data, dict):
                continue
            mission = dict(mission_data)
            mission.setdefault("id", str(index))
            normalized.append(mission)

        MissionFetcher.add_related_mission_names(normalized)
        return sorted(normalized, key=MissionFetcher.sort_key)

    @staticmethod
    def add_related_mission_names(missions: list[dict[str, Any]]) -> None:
        """Add readable names for mission IDs referenced by the JSON payload."""
        names_by_id: dict[str, str] = {}
        for mission in missions:
            name = MissionFetcher.mission_name(mission)
            mission_id = mission.get("id")
            base_id = mission.get("base_mission_id")
            if mission_id not in (None, ""):
                names_by_id[str(mission_id)] = name
            if base_id not in (None, ""):
                names_by_id.setdefault(str(base_id), name)

        for mission in missions:
            additional = mission.get("additional")
            if not isinstance(additional, dict):
                continue

            expansion_ids = additional.get("expansion_missions_ids") or []
            if not isinstance(expansion_ids, list):
                continue

            additional["expansion_mission_names"] = [
                names_by_id.get(str(mission_id), f"Mission {mission_id}")
                for mission_id in expansion_ids
            ]

    @staticmethod
    def mission_key(mission_data: dict[str, Any]) -> str:
        """Return a stable mission key, including additive overlays when present."""
        overlay = str(mission_data.get("additive_overlays") or "").strip().lower()
        base_id = mission_data.get("base_mission_id")
        mission_id = mission_data.get("id")

        if base_id not in (None, "") and overlay:
            return f"{base_id}/{overlay}"

        if mission_id not in (None, ""):
            return str(mission_id)

        if base_id not in (None, ""):
            return str(base_id)

        return MissionFetcher.slugify(str(mission_data.get("name") or "unknown"))

    @staticmethod
    def detail_url(mission_data: dict[str, Any]) -> str:
        """Build the MissionChief detail URL for a mission or overlay."""
        mission_key = MissionFetcher.mission_key(mission_data)
        if "/" not in mission_key:
            return f"{MISSION_DETAIL_BASE_URL}/{mission_key}"

        base_id, overlay = mission_key.split("/", 1)
        return f"{MISSION_DETAIL_BASE_URL}/{base_id}?{urlencode({'additive_overlays': overlay})}"

    @staticmethod
    def calculate_hash(mission_data: dict[str, Any], *, format_version: str = "1") -> str:
        """Calculate a stable hash that also changes when the formatter changes."""
        payload = {
            "format_version": format_version,
            "mission": mission_data,
        }
        mission_json = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(mission_json.encode("utf-8")).hexdigest()

    @staticmethod
    def sort_key(mission_data: dict[str, Any]) -> tuple[int, str, str]:
        """Sort by base numeric mission ID first, then overlay."""
        mission_key = MissionFetcher.mission_key(mission_data)
        base = mission_key.split("/", 1)[0]
        try:
            numeric = int(base)
        except ValueError:
            numeric = 10**9
        return (numeric, base, mission_key)

    @staticmethod
    def mission_name(mission_data: dict[str, Any]) -> str:
        return str(mission_data.get("name") or mission_data.get("caption") or "Unknown Mission")

    @staticmethod
    def matches_query(mission_data: dict[str, Any], query: str | None) -> bool:
        """Match by mission key, name, category, requirement key, or detail URL."""
        if not query:
            return True

        needle = query.casefold().strip()
        if not needle:
            return True

        haystack: list[str] = [
            MissionFetcher.mission_key(mission_data),
            MissionFetcher.mission_name(mission_data),
            MissionFetcher.detail_url(mission_data),
        ]
        haystack.extend(str(value) for value in mission_data.get("mission_categories", []) or [])
        haystack.extend(MissionFetcher._flatten_keys(mission_data.get("requirements", {})))
        haystack.extend(MissionFetcher._flatten_keys(mission_data.get("prerequisites", {})))

        return any(needle in value.casefold() for value in haystack)

    @staticmethod
    def _flatten_keys(value: Any) -> Iterable[str]:
        if isinstance(value, dict):
            for key, nested in value.items():
                yield str(key)
                yield from MissionFetcher._flatten_keys(nested)
        elif isinstance(value, list):
            for item in value:
                yield from MissionFetcher._flatten_keys(item)

    @staticmethod
    def slugify(value: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
        return slug or "unknown"
