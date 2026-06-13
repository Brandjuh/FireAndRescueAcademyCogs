
from __future__ import annotations

import asyncio
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import discord
from redbot.core import commands, Config, bank

try:
    import yaml
except ImportError:  # pragma: no cover - dependency is declared in info.json
    yaml = None


class FireStationCommand(commands.Cog):
    """Fire station management & incident mini-game."""

    __version__ = "1.1.4"
    MISSION_SCHEMA_VERSION = 1
    MAX_COMMAND_LEVEL = 10
    STAGE_ALERT_CHOICE = "ALERT_CHOICE"
    STAGE_STAFF_TURNOUT = "STAFF_TURNOUT"
    STAGE_VEHICLE_SELECT = "VEHICLE_SELECT"
    STAGE_TRAVEL = "TRAVEL"
    STAGE_SCENE_WORK = "SCENE_WORK"
    ACTION_SHOW_TURNOUT_RESULT = "SHOW_TURNOUT_RESULT"
    ACTION_SHOW_TRAVEL_UPDATE = "SHOW_TRAVEL_UPDATE"
    ACTION_RESOLVE_INCIDENT = "RESOLVE_INCIDENT"

    def __init__(self, bot):
        self.bot = bot
        # fresh identifier
        self.config = Config.get_conf(self, identifier=0xF15704, force_registration=True)
        self.game_data = self._load_game_data()

        default_global = self._build_default_global_config()

        default_user: Dict[str, Any] = {
            "started": False,
            "credits": 0,
            "vehicles": [],
            "next_vehicle_id": 1,
            "equipment": [],
            "station_level": 1,
            "command_level": 1,
            "xp": 0,
            "missions_completed": 0,
            "station_type": "volunteer",  # volunteer | career
            "staff_total": 6,
            "staff_trained": 0,
            "active_mission": {},
        }

        self.config.register_global(**default_global)
        self.config.register_user(**default_user)

        self.vehicle_definitions = self._build_vehicle_definitions()
        self.equipment_definitions = self._equipment_definitions()
        self.INCIDENTS = self._build_incidents()
        self.VEHICLE_CATALOG = self._build_vehicle_catalog()
        self.EQUIPMENT_CATALOG = self._build_equipment_catalog()

    # --------------------------------------------------
    # Static fallback data
    # --------------------------------------------------

    @staticmethod
    def _fallback_incidents() -> List[Dict[str, Any]]:
        return [
            {
                "id": "house_fire",
                "name": "House Fire",
                "required_staff": 8,
                "base_credits": 1000,
                "hint": "Reports of smoke from a residential building.",
                "detail": "On approach you see smoke from the roof and people outside waving.",
                "dispatch_narrative": "Dispatch reports smoke from a residential roof. Neighbors are gathering outside and the first caller is worried people may still be inside.",
                "success_narrative": "The crew makes a fast attack and prevents the fire from spreading beyond the roof space.",
                "partial_narrative": "The fire is contained, but smoke damage spreads through part of the home.",
                "failure_narrative": "The response falls behind the fire growth and the house takes heavy damage before control is gained.",
                "image": None,
            },
            {
                "id": "car_crash",
                "name": "Traffic Collision",
                "required_staff": 6,
                "base_credits": 1000,
                "hint": "Multiple calls of a crash at an intersection.",
                "detail": "Police report two vehicles involved, possible entrapment.",
                "dispatch_narrative": "Several callers report a crash at an intersection. Dispatch notes possible entrapment and traffic backing up fast.",
                "success_narrative": "The scene is secured quickly and hazards are controlled before they can escalate.",
                "partial_narrative": "The incident is handled, but traffic and scene safety remain difficult throughout the response.",
                "failure_narrative": "The scene stays unstable too long, forcing additional resources to regain control.",
                "image": None,
            },
            {
                "id": "small_fire",
                "name": "Trash Fire",
                "required_staff": 4,
                "base_credits": 1000,
                "hint": "Caller reports a small fire near containers.",
                "detail": "On arrival, smoke visible but no exposures yet.",
                "dispatch_narrative": "A caller reports flames near a set of containers. It sounds small, but nearby exposures could make it grow quickly.",
                "success_narrative": "The fire is knocked down before it reaches anything important.",
                "partial_narrative": "The fire is handled, though it causes avoidable smoke and surface damage.",
                "failure_narrative": "The fire spreads from the containers and takes extra work to bring under control.",
                "image": None,
            },
        ]

    @staticmethod
    def _fallback_vehicle_catalog() -> Dict[str, Dict[str, Any]]:
        return {
            "pumper": {"name": "Fire Engine", "crew_capacity": 6, "price": 50_000},
            "ladder": {"name": "Aerial Ladder", "crew_capacity": 3, "price": 75_000},
            "rescue": {"name": "Rescue Unit", "crew_capacity": 4, "price": 65_000},
        }

    # --------------------------------------------------
    # Data loading
    # --------------------------------------------------

    def _load_yaml_config(self, filename: str) -> Dict[str, Any]:
        if yaml is None:
            return {}

        path = Path(__file__).parent / "data" / "config" / filename
        try:
            with path.open("r", encoding="utf-8") as fp:
                data = yaml.safe_load(fp) or {}
        except (OSError, yaml.YAMLError):
            return {}

        return data if isinstance(data, dict) else {}

    def _load_game_data(self) -> Dict[str, Dict[str, Any]]:
        return {
            "balance": self._load_yaml_config("balance.yaml"),
            "missions": self._load_yaml_config("missions.yaml"),
            "vehicles": self._load_yaml_config("vehicles.yaml"),
            "equipment": self._load_yaml_config("equipment.yaml"),
            "trainings": self._load_yaml_config("trainings.yaml"),
            "expansions": self._load_yaml_config("expansions.yaml"),
            "progression": self._load_yaml_config("progression.yaml"),
        }

    def _balance_config(self) -> Dict[str, Any]:
        balance = self.game_data.get("balance", {}).get("balance", {})
        return balance if isinstance(balance, dict) else {}

    def _balance_int(self, key: str, default: int) -> int:
        value = self._balance_config().get(key, default)
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _balance_float(self, key: str, default: float) -> float:
        value = self._balance_config().get(key, default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _balance_seconds_as_minutes(self, key: str, default: float) -> float:
        seconds = self._balance_float(key, default * 60)
        return max(0.0, seconds / 60)

    def _reward_multiplier(self) -> float:
        return max(0.0, self._balance_float("credits_reward_multiplier", 1.0))

    def _progression_config(self) -> Dict[str, Any]:
        progression = self.game_data.get("progression", {}).get("progression", {})
        return progression if isinstance(progression, dict) else {}

    def _level_xp_thresholds(self) -> Dict[int, int]:
        configured = self._progression_config().get("level_xp", {})
        defaults = {
            1: 0,
            2: 100,
            3: 275,
            4: 550,
            5: 975,
            6: 1600,
            7: 2500,
            8: 3750,
            9: 5450,
            10: 7700,
        }
        if not isinstance(configured, dict):
            return defaults

        thresholds: Dict[int, int] = {}
        for raw_level, raw_xp in configured.items():
            try:
                level = int(raw_level)
                xp = int(raw_xp)
            except (TypeError, ValueError):
                continue
            if level >= 1 and xp >= 0:
                thresholds[level] = xp
        return thresholds or defaults

    def _command_level_for_xp(self, xp: int) -> int:
        thresholds = self._level_xp_thresholds()
        level = 1
        for candidate, required_xp in sorted(thresholds.items()):
            if xp >= required_xp:
                level = candidate
        return max(1, min(self.MAX_COMMAND_LEVEL, level))

    def _xp_for_next_command_level(self, level: int) -> int | None:
        thresholds = self._level_xp_thresholds()
        next_level = level + 1
        if next_level > self.MAX_COMMAND_LEVEL:
            return None
        return thresholds.get(next_level)

    def _unlock_level(self, item: Dict[str, Any]) -> int:
        for key in ("unlock_level", "required_level", "recommended_level", "tier_min", "min_tier"):
            try:
                value = int(item.get(key, 1))
            except (TypeError, ValueError):
                continue
            return max(1, value)
        return 1

    @staticmethod
    def _capabilities_from(item: Dict[str, Any]) -> Dict[str, float]:
        capabilities = item.get("capabilities", {})
        if not isinstance(capabilities, dict):
            return {}

        parsed: Dict[str, float] = {}
        for name, raw_value in capabilities.items():
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            if isinstance(name, str) and name and value > 0:
                parsed[name] = value
        return parsed

    def _equipment_definitions(self) -> Dict[str, Dict[str, Any]]:
        equipment = self.game_data.get("equipment", {}).get("equipment", [])
        if not isinstance(equipment, list):
            return {}

        definitions: Dict[str, Dict[str, Any]] = {}
        for item in equipment:
            if not isinstance(item, dict):
                continue
            item_id = item.get("id")
            if isinstance(item_id, str) and item_id:
                definitions[item_id] = item
        return definitions

    def _equipment_inventory_counts(self, equipment_inventory: Any) -> Dict[str, int]:
        if isinstance(equipment_inventory, dict):
            counts: Dict[str, int] = {}
            for item_id, quantity in equipment_inventory.items():
                try:
                    count = int(quantity)
                except (TypeError, ValueError):
                    continue
                if count > 0:
                    counts[str(item_id)] = count
            return counts

        if not isinstance(equipment_inventory, list):
            return {}

        counts: Dict[str, int] = {}
        for item in equipment_inventory:
            if isinstance(item, str):
                item_id = item
                quantity = 1
            elif isinstance(item, dict):
                item_id = item.get("catalog_id") or item.get("id")
                quantity = item.get("quantity", 1)
            else:
                continue
            if not item_id:
                continue
            try:
                count = int(quantity)
            except (TypeError, ValueError):
                count = 1
            if count > 0:
                counts[str(item_id)] = counts.get(str(item_id), 0) + count
        return counts

    def _station_capabilities(self, vehicles: Any, equipment_inventory: Any = None) -> Dict[str, float]:
        if not isinstance(vehicles, list):
            vehicles = []

        equipment_counts = self._equipment_inventory_counts(equipment_inventory)
        totals: Dict[str, float] = {}
        for owned in vehicles:
            if not isinstance(owned, dict):
                continue
            vehicle_id = owned.get("catalog_id")
            vehicle = self.vehicle_definitions.get(str(vehicle_id))
            if not vehicle:
                continue

            for capability, value in self._capabilities_from(vehicle).items():
                totals[capability] = totals.get(capability, 0.0) + value

            equipment_slots = vehicle.get("equipment_slots", [])
            if isinstance(equipment_slots, list):
                for equipment_id in equipment_slots:
                    equipment_key = str(equipment_id)
                    if equipment_counts.get(equipment_key, 0) <= 0:
                        continue
                    equipment = self.equipment_definitions.get(equipment_key)
                    if not equipment:
                        continue
                    equipment_counts[equipment_key] -= 1
                    for capability, value in self._capabilities_from(equipment).items():
                        totals[capability] = totals.get(capability, 0.0) + value

        return totals

    def _readiness_score(self, mission: Dict[str, Any], data: Dict[str, Any]) -> int:
        vehicles = data.get("vehicles", [])
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        staff_total = int(data.get("staff_total", 0))
        required_staff = int(mission.get("required_staff", self._required_staff_for_mission(mission)))

        required_caps = self._capabilities_from(mission)
        station_caps = self._station_capabilities(vehicles, data.get("equipment", []))
        if required_caps:
            covered = 0.0
            needed = 0.0
            for capability, required_value in required_caps.items():
                needed += required_value
                covered += min(station_caps.get(capability, 0.0), required_value)
            capability_score = covered / needed if needed > 0 else 1.0
        else:
            capability_score = 1.0

        staff_score = min(1.0, staff_total / required_staff) if required_staff > 0 else 1.0
        missing_vehicles = self._missing_required_vehicle_ids(mission, vehicles)
        vehicle_score = 1.0 if not missing_vehicles else max(0.0, 1.0 - (len(missing_vehicles) * 0.5))
        level_required = self._unlock_level(mission)
        level_score = 1.0 if command_level >= level_required else max(0.0, command_level / level_required)

        score = (
            capability_score * 0.45
            + staff_score * 0.20
            + vehicle_score * 0.25
            + level_score * 0.10
        )
        return int(round(max(0.0, min(1.0, score)) * 100))

    def _mission_challenge_limit(self, command_level: int) -> int:
        roll = random.random()
        if roll < 0.05:
            return command_level + 2
        if roll < 0.20:
            return command_level + 1
        return command_level

    def _build_default_global_config(self) -> Dict[str, Any]:
        return {
            "volunteer_normal_minutes": 2.0,
            "volunteer_emergency_minutes": 0.5,
            "career_turnout_minutes": self._balance_seconds_as_minutes(
                "career_turnout_seconds", 0.0
            ),
            "realert_minutes_min": 0.25,
            "realert_minutes_max": 0.75,
            "travel_minutes_min": 1.0,
            "travel_minutes_max": 2.0,
            "scene_work_minutes_min": 0.5,
            "scene_work_minutes_max": 1.5,
            "staff_cost": 2000,
            "upgrade_base_cost": 50000,
            "career_convert_cost": self._balance_int("career_upgrade_cost", 250000),
            "max_station_level": 10,
        }

    def _utcnow(self) -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _format_timestamp(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _timestamp_after_minutes(self, minutes: float) -> str:
        delay = max(0.0, minutes)
        return self._format_timestamp(self._utcnow() + timedelta(minutes=delay))

    def _new_mission_state(
        self,
        incident: Dict[str, Any],
        channel_id: int,
        guild_id: int | None,
    ) -> Dict[str, Any]:
        now = self._format_timestamp(self._utcnow())
        return {
            "schema_version": self.MISSION_SCHEMA_VERSION,
            "id": incident["id"],
            "title": incident["name"],
            "required_staff": incident["required_staff"],
            "base_credits": incident.get("base_credits", 1000),
            "hint": incident["hint"],
            "detail": incident["detail"],
            "dispatch_narrative": incident.get("dispatch_narrative", incident["hint"]),
            "success_narrative": incident.get("success_narrative", ""),
            "partial_narrative": incident.get("partial_narrative", ""),
            "failure_narrative": incident.get("failure_narrative", ""),
            "image": incident.get("image"),
            "required_vehicles": incident.get("required_vehicles", []),
            "required_equipment": incident.get("required_equipment", []),
            "base_xp": incident.get("base_xp", self._balance_int("xp_per_mission_base", 50)),
            "tier": incident.get("tier", incident.get("min_tier", 1)),
            "recommended_level": incident.get("recommended_level", incident.get("min_tier", 1)),
            "capabilities": incident.get("capabilities", {}),
            "stage": self.STAGE_ALERT_CHOICE,
            "alert_mode": None,
            "channel_id": channel_id,
            "guild_id": guild_id,
            "created_at": now,
            "updated_at": now,
            "next_action": None,
            "next_action_at": None,
        }

    def _mission_stage(self, mission: Dict[str, Any]) -> str:
        stage = mission.get("stage", "")
        return stage if isinstance(stage, str) else ""

    def _mission_is_stage(self, mission: Dict[str, Any], stage: str) -> bool:
        return self._mission_stage(mission) == stage

    def _set_mission_stage(self, mission: Dict[str, Any], stage: str) -> None:
        mission["schema_version"] = self.MISSION_SCHEMA_VERSION
        mission["stage"] = stage
        mission["updated_at"] = self._format_timestamp(self._utcnow())

    def _set_mission_due(
        self,
        mission: Dict[str, Any],
        action: str,
        minutes: float,
    ) -> None:
        mission["next_action"] = action
        mission["next_action_at"] = self._timestamp_after_minutes(minutes)
        mission["updated_at"] = self._format_timestamp(self._utcnow())

    def _clear_mission_due(self, mission: Dict[str, Any]) -> None:
        mission["next_action"] = None
        mission["next_action_at"] = None
        mission["updated_at"] = self._format_timestamp(self._utcnow())

    def _build_vehicle_definitions(self) -> Dict[str, Dict[str, Any]]:
        vehicles = self.game_data.get("vehicles", {}).get("vehicles", [])
        if not isinstance(vehicles, list):
            return {}

        definitions: Dict[str, Dict[str, Any]] = {}
        for vehicle in vehicles:
            if not isinstance(vehicle, dict):
                continue
            vehicle_id = vehicle.get("id")
            if isinstance(vehicle_id, str) and vehicle_id:
                definitions[vehicle_id] = vehicle

        return definitions

    def _required_staff_for_mission(self, mission: Dict[str, Any]) -> int:
        explicit = mission.get("required_staff")
        if isinstance(explicit, int) and explicit > 0:
            return explicit

        required_vehicles = mission.get("required_vehicles", [])
        if not isinstance(required_vehicles, list):
            return 4

        total = 0
        for vehicle_id in required_vehicles:
            vehicle = self.vehicle_definitions.get(vehicle_id)
            if vehicle:
                total += int(vehicle.get("required_staff", 0))

        return total if total > 0 else 4

    def _equipment_display_text(self, equipment_ids: Any) -> str | None:
        if not isinstance(equipment_ids, list) or not equipment_ids:
            return None

        equipment = self.game_data.get("equipment", {}).get("equipment", [])
        names_by_id: Dict[str, str] = {}
        if isinstance(equipment, list):
            for item in equipment:
                if not isinstance(item, dict):
                    continue
                item_id = item.get("id")
                name = item.get("name")
                if isinstance(item_id, str) and isinstance(name, str):
                    names_by_id[item_id] = name

        names = [names_by_id.get(str(item_id), str(item_id)) for item_id in equipment_ids]
        return ", ".join(names)

    def _vehicle_requirement_display_text(self, vehicle_ids: Any) -> str | None:
        if not isinstance(vehicle_ids, list) or not vehicle_ids:
            return None

        names = []
        for vehicle_id in vehicle_ids:
            vehicle_key = str(vehicle_id)
            vehicle = self.vehicle_definitions.get(vehicle_key)
            name = vehicle.get("name") if vehicle else None
            names.append(name if isinstance(name, str) else vehicle_key)
        return ", ".join(names)

    def _missing_required_equipment_ids(self, mission: Dict[str, Any], owned_equipment: Any) -> List[str]:
        required = mission.get("required_equipment", [])
        if not isinstance(required, list) or not required:
            return []

        counts = self._equipment_inventory_counts(owned_equipment)
        missing: List[str] = []
        for equipment_id in required:
            equipment_key = str(equipment_id)
            if counts.get(equipment_key, 0) <= 0:
                missing.append(equipment_key)
            else:
                counts[equipment_key] -= 1
        return missing

    def _missing_required_vehicle_ids(self, mission: Dict[str, Any], owned_vehicles: Any) -> List[str]:
        required = mission.get("required_vehicles", [])
        if not isinstance(required, list) or not required:
            return []
        if not isinstance(owned_vehicles, list):
            owned_vehicles = []

        owned_catalog_ids = {
            str(vehicle.get("catalog_id"))
            for vehicle in owned_vehicles
            if isinstance(vehicle, dict) and vehicle.get("catalog_id")
        }
        return [str(vehicle_id) for vehicle_id in required if str(vehicle_id) not in owned_catalog_ids]

    def _add_mission_requirement_fields(self, embed: discord.Embed, mission: Dict[str, Any]) -> None:
        embed.add_field(name="Required staff", value=str(mission.get("required_staff", "Unknown")), inline=True)
        vehicle_text = self._vehicle_requirement_display_text(mission.get("required_vehicles"))
        if vehicle_text:
            embed.add_field(name="Required vehicles", value=vehicle_text, inline=False)
        equipment_text = self._equipment_display_text(mission.get("required_equipment"))
        if equipment_text:
            embed.add_field(name="Required equipment", value=equipment_text, inline=False)
        readiness = mission.get("readiness_score")
        if isinstance(readiness, int):
            embed.add_field(name="Readiness", value=f"{readiness} / 100", inline=True)
        missing_vehicles = mission.get("missing_required_vehicles", [])
        missing_text = self._vehicle_requirement_display_text(missing_vehicles)
        if missing_text:
            embed.add_field(
                name="Station readiness",
                value=f"Missing vehicle types: {missing_text}",
                inline=False,
            )
        missing_equipment = mission.get("missing_required_equipment", [])
        missing_equipment_text = self._equipment_display_text(missing_equipment)
        if missing_equipment_text:
            embed.add_field(
                name="Equipment readiness",
                value=f"Missing equipment: {missing_equipment_text}",
                inline=False,
            )

    def _build_incidents(self) -> List[Dict[str, Any]]:
        missions = self.game_data.get("missions", {}).get("missions", [])
        if not isinstance(missions, list):
            return self._fallback_incidents()

        incidents: List[Dict[str, Any]] = []
        for mission in missions:
            if not isinstance(mission, dict):
                continue
            mission_id = mission.get("id")
            name = mission.get("name")
            if not isinstance(mission_id, str) or not isinstance(name, str):
                continue

            description = mission.get("description", "No further details available.")
            incidents.append(
                {
                    "id": mission_id,
                    "name": name,
                    "required_staff": self._required_staff_for_mission(mission),
                    "base_credits": int(mission.get("base_credits", 1000)),
                    "hint": description,
                    "detail": mission.get("scene_narrative", description),
                    "dispatch_narrative": mission.get("dispatch_narrative", description),
                    "success_narrative": mission.get("success_narrative", ""),
                    "partial_narrative": mission.get("partial_narrative", ""),
                    "failure_narrative": mission.get("failure_narrative", ""),
                    "image": mission.get("image"),
                    "required_vehicles": mission.get("required_vehicles", []),
                    "required_equipment": mission.get("required_equipment", []),
                    "base_xp": int(mission.get("base_xp", self._balance_int("xp_per_mission_base", 50))),
                    "tier": int(mission.get("tier", mission.get("min_tier", 1))),
                    "recommended_level": int(mission.get("recommended_level", mission.get("min_tier", 1))),
                    "unlock_level": self._unlock_level(mission),
                    "capabilities": mission.get("capabilities", {}),
                }
            )

        return incidents or self._fallback_incidents()

    def _build_vehicle_catalog(self) -> Dict[str, Dict[str, Any]]:
        catalog: Dict[str, Dict[str, Any]] = {}
        for vehicle_id, vehicle in self.vehicle_definitions.items():
            name = vehicle.get("name")
            if not isinstance(name, str):
                continue
            catalog[vehicle_id] = {
                "name": name,
                "crew_capacity": int(vehicle.get("required_staff", 1)),
                "price": int(vehicle.get("base_cost", 0)),
                "image": vehicle.get("image"),
                "unlock_level": self._unlock_level(vehicle),
                "capabilities": vehicle.get("capabilities", {}),
            }

        return catalog or self._fallback_vehicle_catalog()

    def _build_equipment_catalog(self) -> Dict[str, Dict[str, Any]]:
        catalog: Dict[str, Dict[str, Any]] = {}
        for equipment_id, equipment in self.equipment_definitions.items():
            name = equipment.get("name")
            if not isinstance(name, str):
                continue
            catalog[equipment_id] = {
                "name": name,
                "price": int(equipment.get("base_cost", 0)),
                "unlock_level": self._unlock_level(equipment),
                "capabilities": equipment.get("capabilities", {}),
                "required_training": equipment.get("required_training", []),
            }
        return catalog

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------

    async def _ensure_started(self, ctx: commands.Context) -> bool:
        data = await self.config.user(ctx.author).all()
        if not data["started"]:
            await ctx.send("You have not started yet. Use `[p]fsc start` first.")
            return False
        return True

    async def _get_credits(self, user: discord.abc.User) -> int:
        try:
            return int(await bank.get_balance(user))
        except Exception:
            data = await self.config.user(user).all()
            return int(data.get("credits", 0))

    async def _give(self, user: discord.abc.User, amount: int) -> None:
        if amount <= 0:
            return
        try:
            await bank.deposit_credits(user, amount)
            return
        except Exception:
            pass
        user_conf = self.config.user(user)
        data = await user_conf.all()
        local = int(data.get("credits", 0))
        await user_conf.credits.set(local + amount)

    async def _spend(self, user: discord.abc.User, amount: int) -> bool:
        if amount <= 0:
            return True
        try:
            can = await bank.can_spend(user, amount)  # type: ignore[attr-defined]
            if can:
                await bank.withdraw_credits(user, amount)
                return True
        except Exception:
            pass
        user_conf = self.config.user(user)
        data = await user_conf.all()
        local = int(data.get("credits", 0))
        if local < amount:
            return False
        await user_conf.credits.set(local - amount)
        return True

    async def _get_user_vehicles(self, user: discord.abc.User) -> List[Dict[str, Any]]:
        data = await self.config.user(user).all()
        return data.get("vehicles", [])

    async def _award_mission_xp(
        self,
        user_conf,
        data: Dict[str, Any],
        mission: Dict[str, Any],
        outcome_key: str,
    ) -> Dict[str, Any]:
        xp = int(data.get("xp", 0))
        old_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        mission_level = int(mission.get("recommended_level", mission.get("tier", 1)))
        base_xp = int(mission.get("base_xp", self._balance_int("xp_per_mission_base", 50)))
        outcome_multiplier = {
            "success": 1.0,
            "partial": 0.6,
            "failure": 0.2,
        }.get(outcome_key, 0.0)

        level_delta = mission_level - old_level
        if level_delta <= -2:
            challenge_multiplier = 0.75
        elif level_delta <= 0:
            challenge_multiplier = 1.0
        elif level_delta == 1:
            challenge_multiplier = 1.15
        else:
            challenge_multiplier = 1.35

        earned = max(1, int(round(base_xp * outcome_multiplier * challenge_multiplier)))
        new_xp = xp + earned
        new_level = self._command_level_for_xp(new_xp)
        await user_conf.xp.set(new_xp)
        await user_conf.command_level.set(new_level)
        await user_conf.missions_completed.set(int(data.get("missions_completed", 0)) + 1)
        return {
            "earned": earned,
            "total": new_xp,
            "old_level": old_level,
            "new_level": new_level,
            "leveled_up": new_level > old_level,
        }

    async def _create_station(self, user: discord.abc.User) -> bool:
        user_conf = self.config.user(user)
        data = await user_conf.all()
        if data["started"]:
            return False

        starter_vehicle = {
            "id": 1,
            "catalog_id": "engine_basic",
            "name": "Starter Fire Engine",
            "crew_capacity": 6,
            "image": "Images/Vehicles/engine_basic.png",
        }
        await user_conf.started.set(True)
        await user_conf.vehicles.set([starter_vehicle])
        await user_conf.next_vehicle_id.set(2)
        await user_conf.equipment.set(
            [
                {"catalog_id": "hose", "quantity": 1},
                {"catalog_id": "basic_tools", "quantity": 1},
            ]
        )
        await user_conf.station_level.set(1)
        await user_conf.command_level.set(1)
        await user_conf.xp.set(0)
        await user_conf.missions_completed.set(0)
        await user_conf.station_type.set("volunteer")
        await user_conf.staff_total.set(6)
        await user_conf.staff_trained.set(0)
        await user_conf.active_mission.set({})

        await self._give(user, 100_000)
        return True

    async def _build_station_created_embed(self, user: discord.abc.User) -> discord.Embed:
        credits = await self._get_credits(user)
        embed = discord.Embed(
            title="Station created",
            description="You are now the commander of a small **volunteer** station.",
            color=discord.Color.red(),
        )
        embed.add_field(name="Starter vehicle", value="🚒 Starter Fire Engine (crew 6)", inline=False)
        embed.add_field(name="Staff", value="6 volunteers (untrained)", inline=True)
        embed.add_field(name="Credits", value=f"{credits:,}", inline=True)
        return embed

    def _max_staff(self, level: int) -> int:
        if level < 1:
            level = 1
        return 6 + (level - 1) * 2

    def _max_vehicles(self, level: int) -> int:
        if level < 1:
            level = 1
        return 1 + (level - 1)

    def _xp_progress_text(self, xp: int, command_level: int) -> str:
        next_xp = self._xp_for_next_command_level(command_level)
        if next_xp is None:
            return f"Level {command_level} - {xp:,} XP (max)"
        return f"Level {command_level} - {xp:,} / {next_xp:,} XP"

    def _vehicle_is_unlocked(self, vehicle: Dict[str, Any], command_level: int) -> bool:
        return command_level >= int(vehicle.get("unlock_level", 1))

    def _equipment_is_unlocked(self, equipment: Dict[str, Any], command_level: int) -> bool:
        return command_level >= int(equipment.get("unlock_level", 1))

    def _pick_random_incident(self, data: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if not data:
            return random.choice(self.INCIDENTS)

        command_level = int(data.get("command_level", 1))
        challenge_limit = self._mission_challenge_limit(command_level)
        eligible: List[Dict[str, Any]] = []
        challenge: List[Dict[str, Any]] = []
        fallback: List[Dict[str, Any]] = []

        for incident in self.INCIDENTS:
            unlock_level = self._unlock_level(incident)
            readiness = self._readiness_score(incident, data)
            if unlock_level <= command_level and readiness >= 50:
                eligible.append(incident)
            elif unlock_level <= challenge_limit and readiness >= 35:
                challenge.append(incident)
            elif unlock_level <= challenge_limit:
                fallback.append(incident)

        if eligible:
            return random.choice(eligible)
        if challenge:
            return random.choice(challenge)
        if fallback:
            return random.choice(fallback)
        return random.choice(self.INCIDENTS)

    def _make_relative_text(self, minutes: float) -> str:
        if minutes <= 0:
            return "now"
        mins = max(1, int(math.ceil(minutes)))
        if mins == 1:
            return "in 1 minute"
        return f"in {mins} minutes"

    def _simulate_emergency_turnout(self, available: int, required: int) -> Dict[str, int]:
        if available <= 0:
            return {"available": 0, "arrived": 0}
        arrived = 0
        base_no_show = random.uniform(0.10, 0.25)
        for _ in range(available):
            if random.random() > base_no_show:
                arrived += 1
        return {"available": available, "arrived": arrived}

    def _simulate_realert(self, current_total: int, available: int, required: int) -> Dict[str, int]:
        if current_total >= available:
            return {"second_arrived": 0, "total_arrived": current_total}
        remaining = available - current_total
        no_show_second = random.uniform(0.0, 0.10)
        second_arrived = 0
        for _ in range(remaining):
            if random.random() > no_show_second:
                second_arrived += 1
        return {
            "second_arrived": second_arrived,
            "total_arrived": current_total + second_arrived,
        }

    def _alert_narrative(self, mode: str, station_type: str, minutes: float) -> str:
        if station_type == "career":
            options = [
                "The watch room lights up as the call drops. Boots hit the floor almost immediately and the crew moves as one toward the bay.",
                "From the dispatch desk, the assignment is clean: tones, address, incident type. The career crew is already moving before the last detail is read out.",
            ]
        elif mode == "emergency":
            options = [
                "Across town, pagers cut through dinner, work, and quiet living rooms. Volunteers grab keys, leave half-finished tasks behind, and head for the station.",
                "The alert tone lands hard. One firefighter steps away from a workbench, another leaves the house still pulling on a jacket, all converging on the station.",
                "Dispatch marks it urgent. Phones buzz, pagers chirp, and the first volunteers start moving before the message has finished scrolling.",
            ]
        else:
            options = [
                "The call is toned as routine, but the station still begins to stir. Volunteers acknowledge from home, work, and the road as they make their way in.",
                "Dispatch sends the details calmly. Around the district, responders wrap up what they are doing and start toward the station.",
                "A steady alert rolls out over the pagers. It is not panic, but it is movement: doors closing, engines starting, and crew members checking in.",
            ]
        return f"{random.choice(options)} Turnout expected {self._make_relative_text(minutes)}."

    def _realert_narrative(self, minutes: float) -> str:
        options = [
            "The first turnout is not enough, so dispatch sends a second tone. The message is sharper this time: more hands are needed.",
            "A re-alert goes out across the district. Anyone who missed the first call gets a second chance to make the bay.",
            "The incident commander asks for more crew. Dispatch repeats the call, and late responders start checking in.",
        ]
        return f"{random.choice(options)} Additional turnout expected {self._make_relative_text(minutes)}."

    def _turnout_result_narrative(self, arrived: int, required: int, available: int) -> str:
        if arrived >= required:
            return (
                "The bay fills with enough crew to make a confident first move. "
                "The incident can proceed with the planned staffing."
            )
        if arrived > 0:
            return (
                "The turnout is thin. A small crew is ready, but the incident may stretch them "
                "unless more personnel arrive or the dispatch is kept tight."
            )
        if available > 0:
            return (
                "The station stays quiet after the tones. No one makes it in fast enough, "
                "leaving the call without a crew ready to roll."
            )
        return "There is no available staff to answer the call."

    def _travel_narrative(self, minutes: float) -> str:
        options = [
            "The bay doors lift and the unit rolls out into traffic. Dispatch keeps the channel open while the crew builds the first picture from the notes.",
            "The engine clears the station and turns toward the incident. In the cab, the crew reviews the call details and starts planning the first action.",
            "Sirens carry down the street as the unit goes en route. The address is set, the crew is briefed, and the scene is coming closer.",
        ]
        return f"{random.choice(options)} ETA {self._make_relative_text(minutes)}."

    async def _build_dashboard_embed(self, user: discord.abc.User) -> discord.Embed:
        data = await self.config.user(user).all()
        if not data["started"]:
            return discord.Embed(
                title="Fire Station Command",
                description="Create your first station to start managing incidents.",
                color=discord.Color.red(),
            )

        vehicles = data.get("vehicles", [])
        equipment_count = sum(self._equipment_inventory_counts(data.get("equipment", [])).values())
        credits = await self._get_credits(user)
        lvl = int(data.get("station_level", 1))
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        stype = data.get("station_type", "volunteer")
        staff_total = int(data.get("staff_total", 0))
        staff_trained = int(data.get("staff_trained", 0))
        active = data.get("active_mission", {}) or {}

        embed = discord.Embed(
            title="Fire Station Command",
            description="Station dashboard",
            color=discord.Color.red(),
        )
        embed.add_field(name="Credits", value=f"{credits:,}", inline=True)
        embed.add_field(name="Station level", value=str(lvl), inline=True)
        embed.add_field(name="Type", value=stype.capitalize(), inline=True)
        embed.add_field(name="Command XP", value=self._xp_progress_text(xp, command_level), inline=False)
        embed.add_field(
            name="Staff",
            value=f"{staff_total} total ({staff_trained} trained) / max {self._max_staff(lvl)}",
            inline=False,
        )
        embed.add_field(
            name="Vehicles",
            value=f"{len(vehicles)} / max {self._max_vehicles(lvl)}",
            inline=False,
        )
        embed.add_field(name="Equipment", value=f"{equipment_count} item(s)", inline=True)
        if active:
            embed.add_field(
                name="Active incident",
                value=f"{active.get('title', 'Unknown')} (stage: `{active.get('stage', 'unknown')}`)",
                inline=False,
            )
        else:
            embed.add_field(name="Active incident", value="None", inline=False)
        return embed

    def _build_mission_control_embed(self, mission: Dict[str, Any]) -> discord.Embed:
        stage = self._mission_stage(mission)
        next_action = mission.get("next_action")
        next_action_at = mission.get("next_action_at")

        if stage == self.STAGE_ALERT_CHOICE:
            guidance = "Choose how to alert your crew."
        elif stage == self.STAGE_STAFF_TURNOUT and next_action:
            guidance = "Crew turnout is in progress. Refresh this panel after the expected turnout time."
        elif stage == self.STAGE_STAFF_TURNOUT:
            guidance = "Review turnout and decide whether to re-alert, proceed, or cancel."
        elif stage == self.STAGE_VEHICLE_SELECT:
            guidance = "Select vehicles to dispatch with the available crew."
        elif stage == self.STAGE_TRAVEL:
            guidance = "Units are en route. Refresh this panel while waiting for the next update."
        elif stage == self.STAGE_SCENE_WORK:
            guidance = "Crews are working on scene. Refresh this panel while waiting for the incident result."
        else:
            guidance = "Review the current mission state."

        embed = discord.Embed(
            title=f"Mission control - {mission.get('title', 'Incident')}",
            description=mission.get("dispatch_narrative") or mission.get("hint", guidance),
            color=discord.Color.red(),
        )
        self._apply_mission_image(embed, mission)
        embed.add_field(name="Stage", value=stage or "Unknown", inline=True)
        self._add_mission_requirement_fields(embed, mission)
        embed.add_field(name="Guidance", value=guidance, inline=False)

        alert_mode = mission.get("alert_mode")
        if alert_mode:
            embed.add_field(name="Alert mode", value=str(alert_mode).capitalize(), inline=True)

        if "turnout_total_arrived" in mission:
            arrived = int(mission.get("turnout_total_arrived", 0))
            available = int(mission.get("turnout_available", 0))
            embed.add_field(name="Turnout", value=f"{arrived} / {available} arrived", inline=True)

        if next_action_at:
            embed.add_field(name="Next update", value=str(next_action_at), inline=False)

        selected = mission.get("selected_vehicle_ids", [])
        if selected:
            embed.add_field(name="Vehicles dispatched", value=str(len(selected)), inline=True)

        embed.set_footer(text="Use the buttons below to continue this mission.")
        return embed

    async def _build_mission_control_view(
        self,
        user: discord.abc.User,
        channel: discord.abc.Messageable,
        guild: discord.Guild | None,
        mission: Dict[str, Any],
    ) -> discord.ui.View:
        vehicles: List[Dict[str, Any]] = []
        if self._mission_is_stage(mission, self.STAGE_VEHICLE_SELECT):
            vehicles = await self._get_user_vehicles(user)
        return MissionControlView(self, user, channel, guild, mission, vehicles)

    async def _send_active_mission_control(
        self,
        send,
        channel: discord.abc.Messageable,
        guild: discord.Guild | None,
        user: discord.abc.User,
        *,
        ephemeral: bool = False,
    ) -> None:
        data = await self.config.user(user).all()
        mission = data.get("active_mission", {}) or {}
        kwargs = {"ephemeral": True} if ephemeral else {}
        if not mission:
            await send("You do not have an active incident.", **kwargs)
            return

        embed = self._build_mission_control_embed(mission)
        view = await self._build_mission_control_view(user, channel, guild, mission)
        await send(embed=embed, view=view, **kwargs)

    def _build_station_overview_embed(self, data: Dict[str, Any]) -> discord.Embed:
        lvl = int(data.get("station_level", 1))
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        stype = data.get("station_type", "volunteer")
        staff_total = int(data.get("staff_total", 0))
        staff_trained = int(data.get("staff_trained", 0))
        vehicles = data.get("vehicles", [])

        max_staff = self._max_staff(lvl)
        max_veh = self._max_vehicles(lvl)

        embed = discord.Embed(
            title="Station overview",
            color=discord.Color.dark_red(),
        )
        embed.add_field(name="Level", value=str(lvl), inline=True)
        embed.add_field(name="Command XP", value=self._xp_progress_text(xp, command_level), inline=False)
        embed.add_field(name="Type", value=stype.capitalize(), inline=True)
        embed.add_field(name="Vehicle capacity", value=f"{len(vehicles)} / {max_veh}", inline=True)

        embed.add_field(
            name="Staff",
            value=f"{staff_total} total ({staff_trained} trained) / max {max_staff}",
            inline=False,
        )

        if stype == "volunteer":
            embed.add_field(
                name="Turnout profile",
                value="Volunteer: slower turnout, chance of no-shows.",
                inline=False,
            )
        else:
            embed.add_field(
                name="Turnout profile",
                value="Career: instant turnout (in-game), full crew expected.",
                inline=False,
            )

        image_url = self._station_image_url(lvl)
        if image_url:
            embed.set_image(url=image_url)
        return embed

    async def _build_recruitment_embed(self, user: discord.abc.User) -> discord.Embed:
        data = await self.config.user(user).all()
        lvl = int(data.get("station_level", 1))
        staff_total = int(data.get("staff_total", 0))
        max_staff = self._max_staff(lvl)
        open_slots = max(0, max_staff - staff_total)

        glb = await self.config.all()
        cost_per = int(glb.get("staff_cost", 2000))
        credits = await self._get_credits(user)
        affordable = credits // cost_per if cost_per > 0 else open_slots
        hireable = min(open_slots, affordable)

        embed = discord.Embed(
            title="Recruitment desk",
            description="Hire extra station staff with the buttons below.",
            color=discord.Color.green(),
        )
        embed.add_field(name="Staff", value=f"{staff_total} / {max_staff}", inline=True)
        embed.add_field(name="Open positions", value=str(open_slots), inline=True)
        embed.add_field(name="Credits", value=f"{credits:,}", inline=True)
        embed.add_field(name="Cost per recruit", value=f"{cost_per:,} credits", inline=True)

        if open_slots <= 0:
            embed.add_field(
                name="Recruitment status",
                value="Your current station level is already at maximum staff capacity.",
                inline=False,
            )
        elif hireable <= 0:
            embed.add_field(
                name="Recruitment status",
                value="You do not have enough credits to hire another recruit yet.",
                inline=False,
            )
        else:
            embed.add_field(
                name="Available actions",
                value=f"You can currently hire up to **{hireable}** staff.",
                inline=False,
            )
        return embed

    @staticmethod
    def _station_image_url(level: int) -> str | None:
        if level < 1:
            level = 1
        if level > 10:
            level = 10
        return FireStationCommand._asset_image_url(f"Images/Stations/station_level_{level:02d}.png")

    @staticmethod
    def _asset_image_url(path: str | None) -> str | None:
        if not path or not isinstance(path, str):
            return None
        if path.startswith(("http://", "https://")):
            return path
        clean_path = path.removeprefix("FireStationCommand/").lstrip("/")
        return (
            "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
            f"refs/heads/main/FireStationCommand/{clean_path}"
        )

    def _mission_image_url(self, mission: Dict[str, Any]) -> str | None:
        image = mission.get("image")
        return self._asset_image_url(image if isinstance(image, str) else None)

    def _apply_mission_image(self, embed: discord.Embed, mission: Dict[str, Any]) -> None:
        image_url = self._mission_image_url(mission)
        if image_url:
            embed.set_image(url=image_url)

    def _vehicle_image_url(self, vehicle: Dict[str, Any]) -> str | None:
        image = vehicle.get("image")
        return self._asset_image_url(image if isinstance(image, str) else None)

    def _apply_vehicle_image(self, embed: discord.Embed, vehicle: Dict[str, Any]) -> None:
        image_url = self._vehicle_image_url(vehicle)
        if image_url:
            embed.set_image(url=image_url)

    def _build_vehicle_shop_embed(self, data: Dict[str, Any]) -> discord.Embed:
        lvl = int(data.get("station_level", 1))
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        vehicles = data.get("vehicles", [])
        equipment_count = sum(self._equipment_inventory_counts(data.get("equipment", [])).values())
        max_veh = self._max_vehicles(lvl)
        embed = discord.Embed(
            title="Vehicle shop",
            description="Select a vehicle to purchase from the menu below.",
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Capacity",
            value=f"{len(vehicles)} / {max_veh} vehicles currently in your station.",
            inline=False,
        )
        locked = [
            f"{vehicle['name']} (level {vehicle.get('unlock_level', 1)})"
            for vehicle in self.VEHICLE_CATALOG.values()
            if not self._vehicle_is_unlocked(vehicle, command_level)
        ]
        if locked:
            embed.add_field(name="Locked vehicles", value=", ".join(locked), inline=False)
        return embed

    def _build_equipment_shop_embed(self, data: Dict[str, Any]) -> discord.Embed:
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        counts = self._equipment_inventory_counts(data.get("equipment", []))
        owned = [
            f"{self.EQUIPMENT_CATALOG.get(item_id, {}).get('name', item_id)} x{quantity}"
            for item_id, quantity in sorted(counts.items())
        ]
        locked = [
            f"{equipment['name']} (level {equipment.get('unlock_level', 1)})"
            for equipment in self.EQUIPMENT_CATALOG.values()
            if not self._equipment_is_unlocked(equipment, command_level)
        ]

        embed = discord.Embed(
            title="Equipment shop",
            description="Buy equipment to improve mission readiness.",
            color=discord.Color.blue(),
        )
        embed.add_field(name="Owned equipment", value=", ".join(owned) if owned else "None", inline=False)
        embed.add_field(name="Command XP", value=self._xp_progress_text(int(data.get("xp", 0)), command_level), inline=False)
        if locked:
            embed.add_field(name="Locked equipment", value=", ".join(locked), inline=False)
        return embed

    async def _send_vehicle_shop(
        self,
        send,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        *,
        ephemeral: bool = False,
    ) -> None:
        data = await self.config.user(user).all()
        lvl = int(data.get("station_level", 1))
        vehicles = data.get("vehicles", [])
        max_veh = self._max_vehicles(lvl)
        if len(vehicles) >= max_veh:
            kwargs = {"ephemeral": True} if ephemeral else {}
            await send(
                "You are at maximum vehicle capacity. Upgrade your station to buy more vehicles.",
                **kwargs,
            )
            return

        view = VehicleShopView(self, channel, user, data=data)
        embed = self._build_vehicle_shop_embed(data)
        kwargs = {"ephemeral": True} if ephemeral else {}
        await send(embed=embed, view=view, **kwargs)

    async def _start_mission_for_user(
        self,
        send,
        channel: discord.abc.Messageable,
        guild: discord.Guild | None,
        user: discord.abc.User,
        *,
        ephemeral: bool = False,
    ) -> None:
        user_conf = self.config.user(user)
        data = await user_conf.all()
        active = data.get("active_mission", {}) or {}
        if active:
            kwargs = {"ephemeral": True} if ephemeral else {}
            await send(
                "You already have an active incident. Finish or cancel it first.",
                **kwargs,
            )
            return

        staff_total = int(data.get("staff_total", 0))
        if staff_total <= 0:
            kwargs = {"ephemeral": True} if ephemeral else {}
            await send(
                "You have no staff at your station. Recruit staff before taking incidents.",
                **kwargs,
            )
            return

        incident = self._pick_random_incident(data)
        mission = self._new_mission_state(
            incident,
            channel_id=channel.id,
            guild_id=guild.id if guild else None,
        )
        mission["missing_required_vehicles"] = self._missing_required_vehicle_ids(incident, data.get("vehicles", []))
        mission["missing_required_equipment"] = self._missing_required_equipment_ids(
            incident,
            data.get("equipment", []),
        )
        mission["readiness_score"] = self._readiness_score(incident, data)
        await user_conf.active_mission.set(mission)

        view = AlertChoiceView(self, channel, user)
        embed = discord.Embed(
            title=f"🚨 New incident: {incident['name']}",
            description=incident.get("dispatch_narrative", incident["hint"]),
            color=discord.Color.red(),
        )
        self._apply_mission_image(embed, mission)
        self._add_mission_requirement_fields(embed, mission)
        embed.add_field(name="Initial report", value=incident["hint"], inline=False)
        embed.set_footer(text="Choose how to alert your crew below.")
        kwargs = {"ephemeral": True} if ephemeral else {}
        await send(embed=embed, view=view, **kwargs)

    async def _send_dashboard(self, ctx: commands.Context) -> None:
        data = await self.config.user(ctx.author).all()
        embed = await self._build_dashboard_embed(ctx.author)
        if data["started"]:
            view = FscDashboardView(self, ctx.author, ctx.channel, ctx.guild)
        else:
            view = FscStartView(self, ctx.author)
        await ctx.send(embed=embed, view=view)

    # --------------------------------------------------
    # Commands
    # --------------------------------------------------

    @commands.group(name="fsc", invoke_without_command=True)
    async def fsc_group(self, ctx: commands.Context):
        """Fire Station Command main group."""
        if ctx.invoked_subcommand is None:
            await self._send_dashboard(ctx)

    @fsc_group.command(name="start")
    async def fsc_start(self, ctx: commands.Context):
        """Start your fire station career."""
        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        if data["started"]:
            await ctx.send("You already started.")
            return

        await self._create_station(ctx.author)
        embed = await self._build_station_created_embed(ctx.author)
        try:
            await ctx.send(embed=embed)
        except Exception:
            # Fallback if Discord temporarily disconnects
            await ctx.send("Station created. (Discord had trouble sending the embed.)")

    @fsc_group.command(name="status")
    async def fsc_status(self, ctx: commands.Context):
        """Show station, staff, vehicles, and active mission."""
        if not await self._ensure_started(ctx):
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        vehicles = data.get("vehicles", [])
        credits = await self._get_credits(ctx.author)

        lvl = int(data.get("station_level", 1))
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        stype = data.get("station_type", "volunteer")
        staff_total = int(data.get("staff_total", 0))
        staff_trained = int(data.get("staff_trained", 0))
        active = data.get("active_mission", {}) or {}

        max_staff = self._max_staff(lvl)
        max_veh = self._max_vehicles(lvl)

        embed = discord.Embed(
            title="Fire Station Status",
            color=discord.Color.red(),
        )
        embed.add_field(name="Credits", value=f"{credits:,}", inline=True)
        embed.add_field(name="Station level", value=str(lvl), inline=True)
        embed.add_field(name="Type", value=stype.capitalize(), inline=True)
        embed.add_field(name="Command XP", value=self._xp_progress_text(xp, command_level), inline=False)

        embed.add_field(
            name="Staff",
            value=f"{staff_total} total ({staff_trained} trained) / max {max_staff}",
            inline=False,
        )
        embed.add_field(
            name="Vehicles",
            value=f"{len(vehicles)} / max {max_veh}",
            inline=False,
        )

        if active:
            embed.add_field(
                name="Active incident",
                value=f"{active.get('title', 'Unknown')} (stage: `{active.get('stage', 'unknown')}`)",
                inline=False,
            )
        else:
            embed.add_field(name="Active incident", value="None", inline=False)

        await ctx.send(embed=embed)

    @fsc_group.command(name="station")
    async def fsc_station(self, ctx: commands.Context):
        """Detailed overview of your station (with image by bay count)."""
        if not await self._ensure_started(ctx):
            return

        data = await self.config.user(ctx.author).all()
        lvl = int(data.get("station_level", 1))
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        stype = data.get("station_type", "volunteer")
        staff_total = int(data.get("staff_total", 0))
        staff_trained = int(data.get("staff_trained", 0))
        vehicles = data.get("vehicles", [])

        max_staff = self._max_staff(lvl)
        max_veh = self._max_vehicles(lvl)

        embed = discord.Embed(
            title="Station overview",
            color=discord.Color.dark_red(),
        )
        embed.add_field(name="Level", value=str(lvl), inline=True)
        embed.add_field(name="Type", value=stype.capitalize(), inline=True)
        embed.add_field(name="Vehicle capacity", value=f"{len(vehicles)} / {max_veh}", inline=True)
        embed.add_field(name="Equipment", value=f"{equipment_count} item(s)", inline=True)
        embed.add_field(name="Command XP", value=self._xp_progress_text(xp, command_level), inline=False)

        embed.add_field(
            name="Staff",
            value=f"{staff_total} total ({staff_trained} trained) / max {max_staff}",
            inline=False,
        )

        if stype == "volunteer":
            embed.add_field(
                name="Turnout profile",
                value="Volunteer: slower turnout, chance of no-shows.",
                inline=False,
            )
        else:
            embed.add_field(
                name="Turnout profile",
                value="Career: instant turnout (in-game), full crew expected.",
                inline=False,
            )

        image_url = self._station_image_url(lvl)
        if image_url:
            embed.set_image(url=image_url)

        await ctx.send(embed=embed)

    @fsc_group.command(name="recruit")
    async def fsc_recruit(self, ctx: commands.Context, amount: int):
        """Recruit new staff (with confirmation)."""
        if not await self._ensure_started(ctx):
            return
        if amount <= 0:
            await ctx.send("Amount must be positive.")
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        lvl = int(data.get("station_level", 1))
        staff_total = int(data.get("staff_total", 0))

        max_staff = self._max_staff(lvl)
        if staff_total >= max_staff:
            await ctx.send("You are already at maximum staff for your current station level.")
            return

        if staff_total + amount > max_staff:
            amount = max_staff - staff_total

        glb = await self.config.all()
        cost_per = int(glb.get("staff_cost", 2000))
        total_cost = amount * cost_per

        credits = await self._get_credits(ctx.author)
        if credits < total_cost:
            await ctx.send(
                f"You do not have enough credits. Recruiting {amount} costs {total_cost:,}, "
                f"but you only have {credits:,}."
            )
            return

        embed = discord.Embed(
            title="Confirm recruitment",
            description=f"Recruit **{amount}** new staff for **{total_cost:,}** credits?",
            color=discord.Color.green(),
        )
        embed.add_field(name="After recruitment", value=f"{staff_total + amount} / {max_staff} staff", inline=False)

        view = ConfirmRecruitView(self, ctx.author, amount, total_cost)
        await ctx.send(embed=embed, view=view)

    async def _confirm_recruit(
        self,
        interaction: discord.Interaction,
        user: discord.abc.User,
        amount: int,
        total_cost: int,
        *,
        edit_message: bool = False,
        channel: discord.abc.Messageable | None = None,
        guild: discord.Guild | None = None,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        lvl = int(data.get("station_level", 1))
        staff_total = int(data.get("staff_total", 0))
        max_staff = self._max_staff(lvl)

        if staff_total >= max_staff:
            if edit_message:
                embed = await self._build_recruitment_embed(user)
                embed.add_field(name="Recruitment failed", value="Already at maximum staff.", inline=False)
                view = RecruitmentView(self, user, channel or interaction.channel, guild or interaction.guild)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message("Recruitment failed: already at maximum staff.", ephemeral=True)
            return

        if staff_total + amount > max_staff:
            amount = max_staff - staff_total
            total_cost = 0  # we will recompute
            glb = await self.config.all()
            cost_per = int(glb.get("staff_cost", 2000))
            total_cost = amount * cost_per

        credits = await self._get_credits(user)
        if credits < total_cost or not await self._spend(user, total_cost):
            if edit_message:
                embed = await self._build_recruitment_embed(user)
                embed.add_field(name="Recruitment failed", value="Not enough credits.", inline=False)
                view = RecruitmentView(self, user, channel or interaction.channel, guild or interaction.guild)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message("Recruitment failed: not enough credits.", ephemeral=True)
            return

        staff_total += amount
        await user_conf.staff_total.set(staff_total)

        embed = discord.Embed(
            title="Recruitment complete",
            description=f"Recruited **{amount}** new staff.",
            color=discord.Color.green(),
        )
        embed.add_field(name="Total staff", value=f"{staff_total} / {max_staff}", inline=True)
        embed.add_field(name="Cost", value=f"{total_cost:,} credits", inline=True)
        if edit_message:
            view = RecruitmentView(self, user, channel or interaction.channel, guild or interaction.guild)
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            return
        await interaction.response.send_message(embed=embed, ephemeral=False)

    @fsc_group.command(name="upgrade")
    async def fsc_upgrade(self, ctx: commands.Context):
        """Upgrade your station level (confirmation)."""
        if not await self._ensure_started(ctx):
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        lvl = int(data.get("station_level", 1))
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))

        glb = await self.config.all()
        max_lvl = int(glb.get("max_station_level", 10))
        if lvl >= max_lvl:
            await ctx.send("Your station is already at the maximum level.")
            return

        base = int(glb.get("upgrade_base_cost", 50000))
        cost = base * lvl

        credits = await self._get_credits(ctx.author)
        if credits < cost:
            await ctx.send(
                f"You do not have enough credits to upgrade. "
                f"Level {lvl} → {lvl + 1} costs {cost:,}, you have {credits:,}."
            )
            return

        new_lvl = lvl + 1
        if command_level < new_lvl:
            await ctx.send(
                f"You need command level {new_lvl} before upgrading this station. "
                f"Current progress: {self._xp_progress_text(xp, command_level)}."
            )
            return

        max_staff = self._max_staff(new_lvl)
        max_veh = self._max_vehicles(new_lvl)

        embed = discord.Embed(
            title="Confirm station upgrade",
            description=f"Upgrade station from level **{lvl}** to **{new_lvl}** for **{cost:,}** credits?",
            color=discord.Color.blue(),
        )
        embed.add_field(name="New staff capacity", value=f"max {max_staff}", inline=True)
        embed.add_field(name="New vehicle capacity", value=f"max {max_veh}", inline=True)
        embed.add_field(name="Required command level", value=str(new_lvl), inline=True)

        view = ConfirmUpgradeView(self, ctx.author, new_lvl, cost)
        await ctx.send(embed=embed, view=view)

    async def _confirm_upgrade(
        self,
        interaction: discord.Interaction,
        user: discord.abc.User,
        new_level: int,
        cost: int,
        *,
        edit_message: bool = False,
        channel: discord.abc.Messageable | None = None,
        guild: discord.Guild | None = None,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        current_lvl = int(data.get("station_level", 1))
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))

        if new_level <= current_lvl:
            if edit_message:
                embed = await self._build_dashboard_embed(user)
                embed.add_field(name="Upgrade cancelled", value="Station level already changed.", inline=False)
                view = FscDashboardView(self, user, channel or interaction.channel, guild or interaction.guild)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message("Upgrade cancelled: level already changed.", ephemeral=True)
            return

        if command_level < new_level:
            if edit_message:
                embed = await self._build_dashboard_embed(user)
                embed.add_field(
                    name="Upgrade locked",
                    value=(
                        f"You need command level {new_level} first. "
                        f"Current progress: {self._xp_progress_text(xp, command_level)}."
                    ),
                    inline=False,
                )
                view = FscDashboardView(self, user, channel or interaction.channel, guild or interaction.guild)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                f"Upgrade locked: command level {new_level} required.", ephemeral=True
            )
            return

        credits = await self._get_credits(user)
        if credits < cost or not await self._spend(user, cost):
            if edit_message:
                embed = await self._build_dashboard_embed(user)
                embed.add_field(name="Upgrade failed", value="Not enough credits.", inline=False)
                view = FscDashboardView(self, user, channel or interaction.channel, guild or interaction.guild)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message("Upgrade failed: not enough credits.", ephemeral=True)
            return

        await user_conf.station_level.set(new_level)

        max_staff = self._max_staff(new_level)
        max_veh = self._max_vehicles(new_level)

        embed = discord.Embed(
            title="Station upgraded",
            description=f"Your station is now level **{new_level}**.",
            color=discord.Color.blue(),
        )
        embed.add_field(name="Staff capacity", value=f"max {max_staff}", inline=True)
        embed.add_field(name="Vehicle capacity", value=f"max {max_veh}", inline=True)
        embed.add_field(name="Cost", value=f"{cost:,} credits", inline=True)
        if edit_message:
            view = FscDashboardView(self, user, channel or interaction.channel, guild or interaction.guild)
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            return
        await interaction.response.send_message(embed=embed, ephemeral=False)

    @fsc_group.command(name="career")
    async def fsc_career(self, ctx: commands.Context):
        """Convert your volunteer station to a career station (confirmation)."""
        if not await self._ensure_started(ctx):
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        stype = data.get("station_type", "volunteer")
        lvl = int(data.get("station_level", 1))

        if stype == "career":
            await ctx.send("Your station is already a career station.")
            return

        glb = await self.config.all()
        required_lvl = 2
        if lvl < required_lvl:
            await ctx.send(f"You must be at least station level {required_lvl} to convert to a career station.")
            return

        cost = int(glb.get("career_convert_cost", 250000))
        credits = await self._get_credits(ctx.author)
        if credits < cost:
            await ctx.send(
                f"You do not have enough credits. Converting to a career station costs {cost:,}, "
                f"but you only have {credits:,}."
            )
            return

        embed = discord.Embed(
            title="Confirm career conversion",
            description=f"Convert your station to **career** for **{cost:,}** credits?",
            color=discord.Color.gold(),
        )
        embed.add_field(
            name="Effect",
            value="Turnout becomes effectively instant and more reliable.",
            inline=False,
        )

        view = ConfirmCareerView(self, ctx.author, cost)
        await ctx.send(embed=embed, view=view)

    async def _confirm_career(
        self,
        interaction: discord.Interaction,
        user: discord.abc.User,
        cost: int,
        *,
        edit_message: bool = False,
        channel: discord.abc.Messageable | None = None,
        guild: discord.Guild | None = None,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        stype = data.get("station_type", "volunteer")
        if stype == "career":
            if edit_message:
                embed = await self._build_dashboard_embed(user)
                embed.add_field(name="Career conversion", value="Your station is already a career station.", inline=False)
                view = FscDashboardView(self, user, channel or interaction.channel, guild or interaction.guild)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message("Your station is already a career station.", ephemeral=True)
            return

        credits = await self._get_credits(user)
        if credits < cost or not await self._spend(user, cost):
            if edit_message:
                embed = await self._build_dashboard_embed(user)
                embed.add_field(name="Conversion failed", value="Not enough credits.", inline=False)
                view = FscDashboardView(self, user, channel or interaction.channel, guild or interaction.guild)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message("Conversion failed: not enough credits.", ephemeral=True)
            return

        await user_conf.station_type.set("career")

        embed = discord.Embed(
            title="Station converted",
            description="Your station is now a **career** station. Turnout is effectively instant.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Cost", value=f"{cost:,} credits", inline=True)
        if edit_message:
            view = FscDashboardView(self, user, channel or interaction.channel, guild or interaction.guild)
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            return
        await interaction.response.send_message(embed=embed, ephemeral=False)

    @fsc_group.command(name="shop")
    async def fsc_shop(self, ctx: commands.Context):
        """Open the vehicle shop (dropdown)."""
        if not await self._ensure_started(ctx):
            return

        data = await self.config.user(ctx.author).all()
        lvl = int(data.get("station_level", 1))
        vehicles = data.get("vehicles", [])
        max_veh = self._max_vehicles(lvl)
        if len(vehicles) >= max_veh:
            await ctx.send("You are at maximum vehicle capacity. Upgrade your station to buy more vehicles.")
            return

        view = VehicleShopView(self, ctx.channel, ctx.author, ctx.guild, data=data)
        embed = self._build_vehicle_shop_embed(data)
        await ctx.send(embed=embed, view=view)

    @fsc_group.command(name="equipment")
    async def fsc_equipment(self, ctx: commands.Context):
        """Open the equipment shop."""
        if not await self._ensure_started(ctx):
            return

        data = await self.config.user(ctx.author).all()
        embed = self._build_equipment_shop_embed(data)
        view = EquipmentShopView(self, ctx.channel, ctx.author, ctx.guild, data=data)
        await ctx.send(embed=embed, view=view)

    @fsc_group.command(name="mission")
    async def fsc_mission(self, ctx: commands.Context):
        """Start a new incident if none is active."""
        if not await self._ensure_started(ctx):
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        active = data.get("active_mission", {}) or {}
        if active:
            await self._send_active_mission_control(ctx.send, ctx.channel, ctx.guild, ctx.author)
            return

        staff_total = int(data.get("staff_total", 0))
        if staff_total <= 0:
            await ctx.send("You have no staff at your station. Recruit staff before taking incidents.")
            return

        incident = self._pick_random_incident(data)
        mission = self._new_mission_state(
            incident,
            channel_id=ctx.channel.id,
            guild_id=ctx.guild.id if ctx.guild else None,
        )
        mission["missing_required_vehicles"] = self._missing_required_vehicle_ids(incident, data.get("vehicles", []))
        mission["missing_required_equipment"] = self._missing_required_equipment_ids(
            incident,
            data.get("equipment", []),
        )
        mission["readiness_score"] = self._readiness_score(incident, data)
        await user_conf.active_mission.set(mission)

        view = AlertChoiceView(self, ctx.channel, ctx.author)

        embed = discord.Embed(
            title=f"🚨 New incident: {incident['name']}",
            description=incident["hint"],
            color=discord.Color.red(),
        )
        self._apply_mission_image(embed, mission)
        self._add_mission_requirement_fields(embed, mission)
        embed.set_footer(text="Choose how to alert your crew below.")
        await ctx.send(embed=embed, view=view)

    # --------------------------------------------------
    # Workflow handlers
    # --------------------------------------------------

    async def handle_alert_choice(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        mode: str,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if not self._mission_is_stage(mission, self.STAGE_ALERT_CHOICE):
            await interaction.response.send_message("This incident is no longer in the alert stage.", ephemeral=True)
            return

        glb = await self.config.all()
        stype = data.get("station_type", "volunteer")
        required = int(mission.get("required_staff", 6))
        staff_total = int(data.get("staff_total", 0))

        if staff_total <= 0:
            await interaction.response.send_message("You have no staff at your station.", ephemeral=True)
            await user_conf.active_mission.set({})
            return

        available = staff_total

        if stype == "career":
            minutes = float(glb.get("career_turnout_minutes", 0.0))
            first_arrived = min(available, required)
            total = first_arrived
        else:
            if mode == "normal":
                minutes = float(glb.get("volunteer_normal_minutes", 15.0))
                first_arrived = min(available, required)
                total = first_arrived
            else:
                minutes = float(glb.get("volunteer_emergency_minutes", 5.0))
                sim = self._simulate_emergency_turnout(available, required)
                first_arrived = sim["arrived"]
                total = first_arrived

        mission.update(
            {
                "alert_mode": mode,
                "turnout_required": required,
                "turnout_available": available,
                "turnout_first_arrived": first_arrived,
                "turnout_total_arrived": total,
            }
        )
        self._set_mission_stage(mission, self.STAGE_STAFF_TURNOUT)
        self._set_mission_due(mission, self.ACTION_SHOW_TURNOUT_RESULT, minutes)
        await user_conf.active_mission.set(mission)

        embed = discord.Embed(
            title="Crew alerted",
            description=self._alert_narrative(mode, stype, minutes),
            color=discord.Color.orange(),
        )
        embed.add_field(name="Alert mode", value=mode.capitalize(), inline=True)
        embed.add_field(name="Expected turnout", value=self._make_relative_text(minutes), inline=True)
        embed.add_field(name="Required staff", value=str(required), inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=False)

        if minutes > 0:
            await asyncio.sleep(int(minutes * 60))

        await self._show_turnout_result(channel, user)

    async def _show_turnout_result(self, channel: discord.abc.Messageable, user: discord.abc.User):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if not self._mission_is_stage(mission, self.STAGE_STAFF_TURNOUT):
            return
        self._clear_mission_due(mission)
        await user_conf.active_mission.set(mission)

        required = int(mission.get("turnout_required", 0))
        arrived = int(mission.get("turnout_total_arrived", 0))
        available = int(mission.get("turnout_available", 0))

        embed = discord.Embed(
            title="Turnout result",
            color=discord.Color.orange(),
        )
        embed.add_field(name="Required staff", value=str(required), inline=True)
        embed.add_field(name="Available staff", value=str(available), inline=True)
        embed.add_field(name="Arrived staff", value=str(arrived), inline=True)
        embed.add_field(
            name="Narrative",
            value=self._turnout_result_narrative(arrived, required, available),
            inline=False,
        )
        embed.set_footer(text="Re-alert for more crew, proceed with current crew, or cancel the call.")

        view = TurnoutDecisionView(self, channel, user)

        try:
            await channel.send(embed=embed, view=view)
        except Exception:
            try:
                await user.send(embed=embed, view=view)
            except Exception:
                pass

    async def handle_realert(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if not self._mission_is_stage(mission, self.STAGE_STAFF_TURNOUT):
            await interaction.response.send_message("This incident is no longer in turnout stage.", ephemeral=True)
            return

        glb = await self.config.all()
        required = int(mission.get("turnout_required", 0))
        available = int(mission.get("turnout_available", 0))
        current_total = int(mission.get("turnout_total_arrived", 0))

        sim = self._simulate_realert(current_total, available, required)
        mission["turnout_total_arrived"] = sim["total_arrived"]
        mission["turnout_second_arrived"] = sim["second_arrived"]

        min_minutes = float(glb.get("realert_minutes_min", 1.0))
        max_minutes = float(glb.get("realert_minutes_max", 3.0))
        minutes = random.uniform(min_minutes, max_minutes)
        self._set_mission_due(mission, self.ACTION_SHOW_TURNOUT_RESULT, minutes)
        await user_conf.active_mission.set(mission)

        embed = discord.Embed(
            title="Re-alert sent",
            description=self._realert_narrative(minutes),
            color=discord.Color.orange(),
        )
        embed.add_field(name="Current turnout", value=str(current_total), inline=True)
        embed.add_field(name="Available staff", value=str(available), inline=True)
        embed.add_field(name="Additional ETA", value=self._make_relative_text(minutes), inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=False)

        await asyncio.sleep(int(minutes * 60))
        await self._show_turnout_result(channel, user)

    async def handle_proceed_to_vehicles(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if not self._mission_is_stage(mission, self.STAGE_STAFF_TURNOUT):
            await interaction.response.send_message("This incident is no longer in turnout stage.", ephemeral=True)
            return

        arrived = int(mission.get("turnout_total_arrived", 0))
        if arrived <= 0:
            await interaction.response.send_message(
                "No one turned out. You cannot dispatch any units.", ephemeral=True
            )
            return

        self._set_mission_stage(mission, self.STAGE_VEHICLE_SELECT)
        await user_conf.active_mission.set(mission)

        vehicles = await self._get_user_vehicles(user)
        view = VehicleSelectView(self, channel, user, vehicles)

        embed = discord.Embed(
            title="Vehicle selection",
            description=(
                f"{arrived} personnel are ready in the bay. Select the vehicles that best match "
                "the incident picture and available crew."
            ),
            color=discord.Color.blue(),
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=False)

    async def handle_cancel_incident(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
    ):
        user_conf = self.config.user(user)
        await user_conf.active_mission.set({})
        await interaction.response.send_message("Incident cancelled.", ephemeral=False)

    async def handle_vehicle_selection(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        values: List[str],
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if not self._mission_is_stage(mission, self.STAGE_VEHICLE_SELECT):
            await interaction.response.send_message("This incident is not in vehicle selection stage.", ephemeral=True)
            return

        if not values:
            await interaction.response.send_message("No vehicles selected.", ephemeral=True)
            return

        glb = await self.config.all()
        min_minutes = float(glb.get("travel_minutes_min", 3.0))
        max_minutes = float(glb.get("travel_minutes_max", 8.0))
        minutes = random.uniform(min_minutes, max_minutes)
        mission["selected_vehicle_ids"] = [int(v) for v in values]
        self._set_mission_stage(mission, self.STAGE_TRAVEL)
        self._set_mission_due(mission, self.ACTION_SHOW_TRAVEL_UPDATE, minutes)
        await user_conf.active_mission.set(mission)

        embed = discord.Embed(
            title="Units en route",
            description=self._travel_narrative(minutes),
            color=discord.Color.blue(),
        )
        embed.add_field(name="ETA", value=self._make_relative_text(minutes), inline=True)
        embed.add_field(name="Vehicles dispatched", value=str(len(values)), inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=False)

        await asyncio.sleep(int(minutes * 60))
        await self._send_travel_update(channel, user)

    async def _send_travel_update(self, channel: discord.abc.Messageable, user: discord.abc.User):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if not self._mission_is_stage(mission, self.STAGE_TRAVEL):
            return
        glb = await self.config.all()
        min_minutes = float(glb.get("scene_work_minutes_min", 0.5))
        max_minutes = float(glb.get("scene_work_minutes_max", 1.5))
        minutes = random.uniform(min_minutes, max_minutes)
        self._set_mission_stage(mission, self.STAGE_SCENE_WORK)
        self._set_mission_due(mission, self.ACTION_RESOLVE_INCIDENT, minutes)
        await user_conf.active_mission.set(mission)

        title = mission.get("title", "Incident")
        detail = mission.get("detail", "Units report additional information en route.")

        embed = discord.Embed(
            title=f"On-scene update – {title}",
            description=detail,
            color=discord.Color.orange(),
        )
        self._apply_mission_image(embed, mission)
        embed.add_field(name="Scene work", value=f"Incident result expected {self._make_relative_text(minutes)}.", inline=False)
        embed.set_footer(text="Use this information to judge if your dispatch was sufficient.")

        try:
            await channel.send(embed=embed)
        except Exception:
            try:
                await user.send(embed=embed)
            except Exception:
                pass

        await asyncio.sleep(int(minutes * 60))
        await self._resolve_incident(channel, user)

    async def _resolve_incident(self, channel: discord.abc.Messageable, user: discord.abc.User):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if self._mission_stage(mission) not in {
            self.STAGE_TRAVEL,
            self.STAGE_VEHICLE_SELECT,
            self.STAGE_SCENE_WORK,
        }:
            return
        self._clear_mission_due(mission)

        required = int(mission.get("required_staff", 0))
        arrived = int(mission.get("turnout_total_arrived", 0))
        vehicles = await self._get_user_vehicles(user)
        selected_ids = mission.get("selected_vehicle_ids", [])
        selected = [v for v in vehicles if v["id"] in selected_ids]

        total_capacity = sum(v.get("crew_capacity", 0) for v in selected)

        ratio_staff = arrived / required if required else 1.0
        ratio_capacity = total_capacity / required if required else 1.0

        success_score = (ratio_staff * 0.6) + (ratio_capacity * 0.4)
        success_score = max(0.0, min(1.5, success_score))
        base_reward = int(mission.get("base_credits", 1000) * self._reward_multiplier())

        if success_score >= 1.0:
            outcome = "✅ Incident successfully handled."
            reward = int(base_reward * success_score)
            outcome_key = "success"
            narrative = mission.get("success_narrative") or "The incident is wrapped up cleanly."
        elif success_score >= 0.6:
            outcome = "⚠️ Incident handled with difficulties."
            reward = int(base_reward * 0.5 * success_score)
            outcome_key = "partial"
            narrative = mission.get("partial_narrative") or "The incident is handled, but the response was stretched."
        else:
            outcome = "❌ Incident not successfully handled."
            reward = int(base_reward * 0.1 * success_score)
            outcome_key = "failure"
            narrative = mission.get("failure_narrative") or "The incident outcome is poor and needs review."

        xp_result = await self._award_mission_xp(user_conf, data, mission, outcome_key)
        await self._give(user, reward)
        total_credits = await self._get_credits(user)
        await user_conf.active_mission.set({})

        embed = discord.Embed(
            title=f"Incident result – {mission.get('title', 'Unknown')}",
            color=discord.Color.green() if success_score >= 1.0 else discord.Color.orange(),
        )
        self._apply_mission_image(embed, mission)
        embed.add_field(name="Required staff", value=str(required), inline=True)
        embed.add_field(name="Arrived staff", value=str(arrived), inline=True)
        embed.add_field(name="Vehicles dispatched", value=f"{len(selected)} (cap {total_capacity})", inline=True)
        embed.add_field(name="Outcome", value=outcome, inline=False)
        embed.add_field(name="Narrative", value=narrative, inline=False)
        embed.add_field(name="Reward", value=f"{reward:,} credits", inline=True)
        embed.add_field(name="XP gained", value=f"{xp_result['earned']:,} XP", inline=True)
        embed.add_field(name="Total credits", value=f"{total_credits:,}", inline=True)
        embed.add_field(
            name="Command XP",
            value=self._xp_progress_text(xp_result["total"], xp_result["new_level"]),
            inline=False,
        )
        if xp_result["leveled_up"]:
            embed.add_field(
                name="Level up",
                value=f"Command level increased to {xp_result['new_level']}. New unlocks may be available.",
                inline=False,
            )

        try:
            await channel.send(embed=embed)
        except Exception:
            try:
                await user.send(embed=embed)
            except Exception:
                pass

    # --------------------------------------------------
    # Vehicle shop handling
    # --------------------------------------------------

    async def _confirm_vehicle_purchase(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        vehicle_id: str,
        *,
        edit_message: bool = False,
        guild: discord.Guild | None = None,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()

        catalog = self.VEHICLE_CATALOG
        if vehicle_id not in catalog:
            if edit_message:
                embed = self._build_vehicle_shop_embed(data)
                embed.add_field(name="Purchase failed", value="Unknown vehicle type.", inline=False)
                view = VehicleShopView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message("Unknown vehicle type.", ephemeral=True)
            return

        vdef = catalog[vehicle_id]
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        if not self._vehicle_is_unlocked(vdef, command_level):
            required_level = int(vdef.get("unlock_level", 1))
            if edit_message:
                embed = self._build_vehicle_shop_embed(data)
                embed.add_field(
                    name="Purchase locked",
                    value=f"{vdef['name']} requires command level {required_level}.",
                    inline=False,
                )
                view = VehicleShopView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                f"Purchase locked: command level {required_level} required.", ephemeral=True
            )
            return

        price = int(vdef["price"])

        # Check capacity again
        lvl = int(data.get("station_level", 1))
        vehicles = data.get("vehicles", [])
        max_veh = self._max_vehicles(lvl)
        if len(vehicles) >= max_veh:
            if edit_message:
                embed = await self._build_dashboard_embed(user)
                embed.add_field(
                    name="Vehicle shop",
                    value="Purchase failed: you are at maximum vehicle capacity.",
                    inline=False,
                )
                view = FscDashboardView(self, user, channel, guild or interaction.guild)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                "Purchase failed: you are at maximum vehicle capacity.", ephemeral=True
            )
            return

        credits = await self._get_credits(user)
        if credits < price or not await self._spend(user, price):
            if edit_message:
                embed = self._build_vehicle_shop_embed(data)
                embed.add_field(
                    name="Purchase failed",
                    value="You do not have enough credits to complete this purchase.",
                    inline=False,
                )
                view = VehicleShopView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                "You do not have enough credits to complete this purchase.",
                ephemeral=True,
            )
            return

        next_id = int(data.get("next_vehicle_id", 1))
        new_vehicle = {
            "id": next_id,
            "catalog_id": vehicle_id,
            "name": vdef["name"],
            "crew_capacity": int(vdef["crew_capacity"]),
            "image": vdef.get("image"),
        }
        vehicles.append(new_vehicle)

        await user_conf.vehicles.set(vehicles)
        await user_conf.next_vehicle_id.set(next_id + 1)

        embed = discord.Embed(
            title="Vehicle purchased",
            description=f"Purchased **{vdef['name']}** for **{price:,}** credits.",
            color=discord.Color.green(),
        )
        embed.add_field(name="Crew capacity", value=str(vdef["crew_capacity"]), inline=True)
        self._apply_vehicle_image(embed, vdef)
        if edit_message:
            embed.add_field(name="Vehicle capacity", value=f"{len(vehicles)} / {max_veh}", inline=True)
            if len(vehicles) >= max_veh:
                view = FscDashboardView(self, user, channel, guild or interaction.guild)
            else:
                view = VehicleShopView(self, channel, user, guild or interaction.guild, data=data)
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            return
        await interaction.response.send_message(embed=embed, ephemeral=False)

    async def _confirm_equipment_purchase(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        equipment_id: str,
        *,
        edit_message: bool = False,
        guild: discord.Guild | None = None,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()

        if equipment_id not in self.EQUIPMENT_CATALOG:
            if edit_message:
                embed = self._build_equipment_shop_embed(data)
                embed.add_field(name="Purchase failed", value="Unknown equipment type.", inline=False)
                view = EquipmentShopView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message("Unknown equipment type.", ephemeral=True)
            return

        equipment = self.EQUIPMENT_CATALOG[equipment_id]
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        if not self._equipment_is_unlocked(equipment, command_level):
            required_level = int(equipment.get("unlock_level", 1))
            if edit_message:
                embed = self._build_equipment_shop_embed(data)
                embed.add_field(
                    name="Purchase locked",
                    value=f"{equipment['name']} requires command level {required_level}.",
                    inline=False,
                )
                view = EquipmentShopView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                f"Purchase locked: command level {required_level} required.", ephemeral=True
            )
            return

        price = int(equipment.get("price", 0))
        credits = await self._get_credits(user)
        if credits < price or not await self._spend(user, price):
            if edit_message:
                embed = self._build_equipment_shop_embed(data)
                embed.add_field(
                    name="Purchase failed",
                    value="You do not have enough credits to complete this purchase.",
                    inline=False,
                )
                view = EquipmentShopView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                "You do not have enough credits to complete this purchase.",
                ephemeral=True,
            )
            return

        equipment_inventory = data.get("equipment", [])
        counts = self._equipment_inventory_counts(equipment_inventory)
        counts[equipment_id] = counts.get(equipment_id, 0) + 1
        updated_inventory = [
            {"catalog_id": item_id, "quantity": quantity}
            for item_id, quantity in sorted(counts.items())
        ]
        await user_conf.equipment.set(updated_inventory)

        updated_data = dict(data)
        updated_data["equipment"] = updated_inventory
        embed = discord.Embed(
            title="Equipment purchased",
            description=f"Purchased **{equipment['name']}** for **{price:,}** credits.",
            color=discord.Color.green(),
        )
        embed.add_field(name="Owned", value=f"{counts[equipment_id]} total", inline=True)
        if edit_message:
            view = EquipmentShopView(self, channel, user, guild or interaction.guild, data=updated_data)
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            return
        await interaction.response.send_message(embed=embed, ephemeral=False)


class FscStartView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, user: discord.abc.User):
        super().__init__(timeout=180)
        self.cog = cog
        self.user = user

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Create station", style=discord.ButtonStyle.success)
    async def create_station(self, interaction: discord.Interaction, button: discord.ui.Button):
        created = await self.cog._create_station(self.user)
        embed = await self.cog._build_dashboard_embed(self.user)
        view = FscDashboardView(self.cog, self.user, interaction.channel, interaction.guild)
        if created:
            await interaction.response.edit_message(embed=embed, view=view)
        else:
            await interaction.response.send_message("You already started.", ephemeral=True)
        self.stop()


class FscDashboardView(discord.ui.View):
    def __init__(
        self,
        cog: FireStationCommand,
        user: discord.abc.User,
        channel: discord.abc.Messageable,
        guild: discord.Guild | None,
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.user = user
        self.channel = channel
        self.guild = guild

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    def _dashboard_view(self) -> "FscDashboardView":
        return FscDashboardView(self.cog, self.user, self.channel, self.guild)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.primary)
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog._build_dashboard_embed(self.user)
        await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())

    @discord.ui.button(label="Station", style=discord.ButtonStyle.secondary)
    async def station(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        if not data["started"]:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action required", value="Create a station first.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=FscStartView(self.cog, self.user))
            return
        embed = self.cog._build_station_overview_embed(data)
        await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())

    @discord.ui.button(label="Recruit", style=discord.ButtonStyle.success)
    async def recruit(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        if not data["started"]:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action required", value="Create a station first.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=FscStartView(self.cog, self.user))
            return
        embed = await self.cog._build_recruitment_embed(self.user)
        view = RecruitmentView(self.cog, self.user, interaction.channel or self.channel, interaction.guild or self.guild)
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    @discord.ui.button(label="Shop", style=discord.ButtonStyle.secondary)
    async def shop(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        if not data["started"]:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action required", value="Create a station first.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=FscStartView(self.cog, self.user))
            return
        lvl = int(data.get("station_level", 1))
        vehicles = data.get("vehicles", [])
        max_veh = self.cog._max_vehicles(lvl)
        if len(vehicles) >= max_veh:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(
                name="Vehicle shop",
                value="You are at maximum vehicle capacity. Upgrade your station to buy more vehicles.",
                inline=False,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())
            return

        embed = self.cog._build_vehicle_shop_embed(data)
        await interaction.response.edit_message(
            content=None,
            embed=embed,
            view=VehicleShopView(self.cog, self.channel, self.user, self.guild, data=data),
        )

    @discord.ui.button(label="Equipment", style=discord.ButtonStyle.secondary)
    async def equipment(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        if not data["started"]:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action required", value="Create a station first.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=FscStartView(self.cog, self.user))
            return

        embed = self.cog._build_equipment_shop_embed(data)
        view = EquipmentShopView(
            self.cog,
            interaction.channel or self.channel,
            self.user,
            interaction.guild or self.guild,
            data=data,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    @discord.ui.button(label="Upgrade", style=discord.ButtonStyle.primary)
    async def upgrade(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        if not data["started"]:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action required", value="Create a station first.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=FscStartView(self.cog, self.user))
            return

        lvl = int(data.get("station_level", 1))
        glb = await self.cog.config.all()
        max_lvl = int(glb.get("max_station_level", 10))
        if lvl >= max_lvl:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Station upgrade", value="Your station is already at the maximum level.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())
            return

        base = int(glb.get("upgrade_base_cost", 50000))
        cost = base * lvl
        credits = await self.cog._get_credits(self.user)
        if credits < cost:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(
                name="Station upgrade",
                value=f"Level {lvl} to {lvl + 1} costs {cost:,} credits, but you only have {credits:,}.",
                inline=False,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())
            return

        new_lvl = lvl + 1
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self.cog._command_level_for_xp(xp)))
        if command_level < new_lvl:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(
                name="Station upgrade",
                value=(
                    f"You need command level {new_lvl} before upgrading this station. "
                    f"Current progress: {self.cog._xp_progress_text(xp, command_level)}."
                ),
                inline=False,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())
            return

        embed = discord.Embed(
            title="Confirm station upgrade",
            description=f"Upgrade station from level **{lvl}** to **{new_lvl}** for **{cost:,}** credits?",
            color=discord.Color.blue(),
        )
        embed.add_field(name="New staff capacity", value=f"max {self.cog._max_staff(new_lvl)}", inline=True)
        embed.add_field(name="New vehicle capacity", value=f"max {self.cog._max_vehicles(new_lvl)}", inline=True)
        embed.add_field(name="Required command level", value=str(new_lvl), inline=True)
        view = ConfirmUpgradeView(
            self.cog,
            self.user,
            new_lvl,
            cost,
            channel=interaction.channel or self.channel,
            guild=interaction.guild or self.guild,
            edit_message=True,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    @discord.ui.button(label="Career", style=discord.ButtonStyle.secondary)
    async def career(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        if not data["started"]:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action required", value="Create a station first.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=FscStartView(self.cog, self.user))
            return

        stype = data.get("station_type", "volunteer")
        if stype == "career":
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Career station", value="Your station is already a career station.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())
            return

        required_lvl = 2
        lvl = int(data.get("station_level", 1))
        if lvl < required_lvl:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(
                name="Career station",
                value=f"You must be at least station level {required_lvl} to convert to a career station.",
                inline=False,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())
            return

        glb = await self.cog.config.all()
        cost = int(glb.get("career_convert_cost", 250000))
        credits = await self.cog._get_credits(self.user)
        if credits < cost:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(
                name="Career station",
                value=f"Career conversion costs {cost:,} credits, but you only have {credits:,}.",
                inline=False,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())
            return

        embed = discord.Embed(
            title="Confirm career conversion",
            description=f"Convert your station to **career** for **{cost:,}** credits?",
            color=discord.Color.gold(),
        )
        embed.add_field(
            name="Effect",
            value="Turnout becomes effectively instant and more reliable.",
            inline=False,
        )
        view = ConfirmCareerView(
            self.cog,
            self.user,
            cost,
            channel=interaction.channel or self.channel,
            guild=interaction.guild or self.guild,
            edit_message=True,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    @discord.ui.button(label="Mission", style=discord.ButtonStyle.danger)
    async def mission(self, interaction: discord.Interaction, button: discord.ui.Button):
        channel = interaction.channel or self.channel
        data = await self.cog.config.user(self.user).all()
        if not data["started"]:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action required", value="Create a station first.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=FscStartView(self.cog, self.user))
            return

        active = data.get("active_mission", {}) or {}
        if active:
            embed = self.cog._build_mission_control_embed(active)
            view = await self.cog._build_mission_control_view(self.user, channel, self.guild, active)
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            return

        staff_total = int(data.get("staff_total", 0))
        if staff_total <= 0:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(
                name="Mission",
                value="You have no staff at your station. Recruit staff before taking incidents.",
                inline=False,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())
            return

        incident = self.cog._pick_random_incident(data)
        mission = self.cog._new_mission_state(
            incident,
            channel_id=channel.id,
            guild_id=self.guild.id if self.guild else None,
        )
        mission["missing_required_vehicles"] = self.cog._missing_required_vehicle_ids(
            incident,
            data.get("vehicles", []),
        )
        mission["missing_required_equipment"] = self.cog._missing_required_equipment_ids(
            incident,
            data.get("equipment", []),
        )
        mission["readiness_score"] = self.cog._readiness_score(incident, data)
        await self.cog.config.user(self.user).active_mission.set(mission)

        embed = discord.Embed(
            title=f"🚨 New incident: {incident['name']}",
            description=incident.get("dispatch_narrative", incident["hint"]),
            color=discord.Color.red(),
        )
        self.cog._apply_mission_image(embed, mission)
        self.cog._add_mission_requirement_fields(embed, mission)
        embed.add_field(name="Initial report", value=incident["hint"], inline=False)
        embed.set_footer(text="Choose how to alert your crew below.")
        await interaction.response.edit_message(
            content=None,
            embed=embed,
            view=AlertChoiceView(self.cog, channel, self.user),
        )

    @discord.ui.button(label="Commands", style=discord.ButtonStyle.secondary)
    async def commands(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="Fire Station Command options",
            description="Some options need extra input and are still command-based.",
            color=discord.Color.red(),
        )
        embed.add_field(name="Recruit", value="`[p]fsc recruit <amount>`", inline=False)
        embed.add_field(name="Upgrade", value="`[p]fsc upgrade`", inline=False)
        embed.add_field(name="Career station", value="`[p]fsc career`", inline=False)
        embed.add_field(name="Equipment", value="`[p]fsc equipment`", inline=False)
        await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())


class RecruitmentView(discord.ui.View):
    def __init__(
        self,
        cog: FireStationCommand,
        user: discord.abc.User,
        channel: discord.abc.Messageable,
        guild: discord.Guild | None = None,
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.user = user
        self.channel = channel
        self.guild = guild

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    async def _show_recruitment_status(
        self,
        interaction: discord.Interaction,
        title: str,
        message: str,
    ) -> None:
        embed = await self.cog._build_recruitment_embed(self.user)
        embed.add_field(name=title, value=message, inline=False)
        await interaction.response.edit_message(content=None, embed=embed, view=self)

    async def _confirm_hire(self, interaction: discord.Interaction, requested_amount: int | None) -> None:
        data = await self.cog.config.user(self.user).all()
        lvl = int(data.get("station_level", 1))
        staff_total = int(data.get("staff_total", 0))
        max_staff = self.cog._max_staff(lvl)
        open_slots = max(0, max_staff - staff_total)
        if open_slots <= 0:
            await self._show_recruitment_status(
                interaction,
                "Recruitment unavailable",
                "Your current station level is already at maximum staff capacity.",
            )
            return

        glb = await self.cog.config.all()
        cost_per = int(glb.get("staff_cost", 2000))
        credits = await self.cog._get_credits(self.user)
        affordable = credits // cost_per if cost_per > 0 else open_slots
        if affordable <= 0:
            await self._show_recruitment_status(
                interaction,
                "Recruitment unavailable",
                "You do not have enough credits to hire another recruit yet.",
            )
            return

        amount = min(open_slots, affordable) if requested_amount is None else min(requested_amount, open_slots)
        total_cost = amount * cost_per
        if credits < total_cost:
            await self._show_recruitment_status(
                interaction,
                "Not enough credits",
                f"Hiring {amount} staff costs {total_cost:,} credits, but you only have {credits:,}.",
            )
            return

        embed = discord.Embed(
            title="Confirm recruitment",
            description=f"Hire **{amount}** new staff for **{total_cost:,}** credits?",
            color=discord.Color.green(),
        )
        embed.add_field(name="After recruitment", value=f"{staff_total + amount} / {max_staff} staff", inline=False)
        view = ConfirmRecruitView(
            self.cog,
            self.user,
            amount,
            total_cost,
            channel=interaction.channel or self.channel,
            guild=interaction.guild or self.guild,
            edit_message=True,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    @discord.ui.button(label="Hire 1", style=discord.ButtonStyle.success)
    async def hire_one(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._confirm_hire(interaction, 1)

    @discord.ui.button(label="Hire 5", style=discord.ButtonStyle.success)
    async def hire_five(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._confirm_hire(interaction, 5)

    @discord.ui.button(label="Hire max", style=discord.ButtonStyle.primary)
    async def hire_max(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._confirm_hire(interaction, None)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog._build_dashboard_embed(self.user)
        channel = interaction.channel or self.channel
        guild = interaction.guild or self.guild
        view = FscDashboardView(self.cog, self.user, channel, guild)
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class AlertChoiceView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, channel: discord.abc.Messageable, user: discord.abc.User):
        super().__init__(timeout=120)
        self.cog = cog
        self.channel = channel
        self.user = user

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Normal turnout (volunteer)", style=discord.ButtonStyle.secondary)
    async def normal(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Disable buttons to prevent double-clicks
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog.handle_alert_choice(interaction, self.channel, self.user, "normal")
        self.stop()

    @discord.ui.button(label="Emergency turnout (volunteer)", style=discord.ButtonStyle.danger)
    async def emergency(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog.handle_alert_choice(interaction, self.channel, self.user, "emergency")
        self.stop()


class TurnoutDecisionView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, channel: discord.abc.Messageable, user: discord.abc.User):
        super().__init__(timeout=180)
        self.cog = cog
        self.channel = channel
        self.user = user

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    async def _disable_and_edit(self, interaction: discord.Interaction):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass

    @discord.ui.button(label="Re-alert", style=discord.ButtonStyle.secondary)
    async def realert(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._disable_and_edit(interaction)
        await self.cog.handle_realert(interaction, self.channel, self.user)
        self.stop()

    @discord.ui.button(label="Proceed to vehicle selection", style=discord.ButtonStyle.success)
    async def proceed(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._disable_and_edit(interaction)
        await self.cog.handle_proceed_to_vehicles(interaction, self.channel, self.user)
        self.stop()

    @discord.ui.button(label="Cancel incident", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._disable_and_edit(interaction)
        await self.cog.handle_cancel_incident(interaction, self.channel, self.user)
        self.stop()


class VehicleSelect(discord.ui.Select):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        vehicles: List[Dict[str, Any]],
    ):
        options: List[discord.SelectOption] = []
        for v in vehicles:
            label = f"{v['name']} (cap {v['crew_capacity']})"
            options.append(discord.SelectOption(label=label, value=str(v["id"])))
        if not options:
            options = [discord.SelectOption(label="No vehicles available", value="none")]

        super().__init__(
            placeholder="Select vehicles to dispatch",
            min_values=1,
            max_values=len(options),
            options=options,
        )
        self.cog = cog
        self.channel = channel
        self.user = user

    async def callback(self, interaction: discord.Interaction):
        if self.values == ["none"]:
            await interaction.response.send_message("You have no vehicles to dispatch.", ephemeral=True)
            return
        await self.cog.handle_vehicle_selection(interaction, self.channel, self.user, list(self.values))


class VehicleSelectView(discord.ui.View):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        vehicles: List[Dict[str, Any]],
    ):
        super().__init__(timeout=180)
        self.add_item(VehicleSelect(cog, channel, user, vehicles))


class MissionControlView(discord.ui.View):
    def __init__(
        self,
        cog: FireStationCommand,
        user: discord.abc.User,
        channel: discord.abc.Messageable,
        guild: discord.Guild | None,
        mission: Dict[str, Any],
        vehicles: List[Dict[str, Any]],
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.user = user
        self.channel = channel
        self.guild = guild
        self.mission = mission

        stage = self.cog._mission_stage(mission)
        if stage == self.cog.STAGE_ALERT_CHOICE:
            self._add_button("Normal turnout", discord.ButtonStyle.secondary, self._normal_turnout)
            self._add_button("Emergency turnout", discord.ButtonStyle.danger, self._emergency_turnout)
        elif stage == self.cog.STAGE_STAFF_TURNOUT and mission.get("next_action"):
            self._add_button("Refresh", discord.ButtonStyle.primary, self._refresh)
        elif stage == self.cog.STAGE_STAFF_TURNOUT:
            self._add_button("Re-alert", discord.ButtonStyle.secondary, self._realert)
            self._add_button("Proceed to vehicles", discord.ButtonStyle.success, self._proceed)
        elif stage == self.cog.STAGE_VEHICLE_SELECT:
            self.add_item(VehicleSelect(cog, channel, user, vehicles))
        elif stage in {self.cog.STAGE_TRAVEL, self.cog.STAGE_SCENE_WORK}:
            self._add_button("Refresh", discord.ButtonStyle.primary, self._refresh)

        self._add_button("Cancel incident", discord.ButtonStyle.danger, self._cancel)
        self._add_button("Back", discord.ButtonStyle.secondary, self._back)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    def _add_button(self, label: str, style: discord.ButtonStyle, callback):
        button = discord.ui.Button(label=label, style=style)
        button.callback = callback
        self.add_item(button)

    async def _disable_and_edit(self, interaction: discord.Interaction):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass

    async def _normal_turnout(self, interaction: discord.Interaction):
        await self._disable_and_edit(interaction)
        await self.cog.handle_alert_choice(interaction, self.channel, self.user, "normal")
        self.stop()

    async def _emergency_turnout(self, interaction: discord.Interaction):
        await self._disable_and_edit(interaction)
        await self.cog.handle_alert_choice(interaction, self.channel, self.user, "emergency")
        self.stop()

    async def _realert(self, interaction: discord.Interaction):
        await self._disable_and_edit(interaction)
        await self.cog.handle_realert(interaction, self.channel, self.user)
        self.stop()

    async def _proceed(self, interaction: discord.Interaction):
        await self._disable_and_edit(interaction)
        await self.cog.handle_proceed_to_vehicles(interaction, self.channel, self.user)
        self.stop()

    async def _cancel(self, interaction: discord.Interaction):
        await self._disable_and_edit(interaction)
        await self.cog.handle_cancel_incident(interaction, self.channel, self.user)
        self.stop()

    async def _refresh(self, interaction: discord.Interaction):
        data = await self.cog.config.user(self.user).all()
        mission = data.get("active_mission", {}) or {}
        if not mission:
            embed = await self.cog._build_dashboard_embed(self.user)
            view = FscDashboardView(
                self.cog,
                self.user,
                interaction.channel or self.channel,
                interaction.guild or self.guild,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            return

        channel = interaction.channel or self.channel
        guild = interaction.guild or self.guild
        embed = self.cog._build_mission_control_embed(mission)
        view = await self.cog._build_mission_control_view(self.user, channel, guild, mission)
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    async def _back(self, interaction: discord.Interaction):
        embed = await self.cog._build_dashboard_embed(self.user)
        channel = interaction.channel or self.channel
        guild = interaction.guild or self.guild
        view = FscDashboardView(self.cog, self.user, channel, guild)
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class VehicleShopSelect(discord.ui.Select):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        guild: discord.Guild | None = None,
        command_level: int = 1,
    ):
        self.cog = cog
        self.channel = channel
        self.user = user
        self.guild = guild

        options: List[discord.SelectOption] = []
        for vid, v in self.cog.VEHICLE_CATALOG.items():
            unlock_level = int(v.get("unlock_level", 1))
            locked = command_level < unlock_level
            label = f"{v['name']} ({v['price']:,} cr, cap {v['crew_capacity']})"
            description = None
            if locked:
                description = f"Requires command level {unlock_level}"
                label = f"Locked - {label}"
            options.append(
                discord.SelectOption(
                    label=label,
                    value=vid,
                    description=description,
                    default=False,
                    emoji=None,
                )
            )

        super().__init__(
            placeholder="Select a vehicle to buy",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        choice = self.values[0]
        vdef = self.cog.VEHICLE_CATALOG.get(choice)
        if not vdef:
            await interaction.response.send_message("Unknown vehicle type.", ephemeral=True)
            return
        data = await self.cog.config.user(self.user).all()
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self.cog._command_level_for_xp(xp)))
        if not self.cog._vehicle_is_unlocked(vdef, command_level):
            required_level = int(vdef.get("unlock_level", 1))
            embed = self.cog._build_vehicle_shop_embed(data)
            embed.add_field(
                name="Purchase locked",
                value=f"{vdef['name']} requires command level {required_level}.",
                inline=False,
            )
            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=VehicleShopView(
                    self.cog,
                    self.channel,
                    self.user,
                    interaction.guild or self.guild,
                    data=data,
                ),
            )
            return

        price = int(vdef["price"])
        embed = discord.Embed(
            title="Confirm vehicle purchase",
            description=f"Buy **{vdef['name']}** for **{price:,}** credits?",
            color=discord.Color.blurple(),
        )
        embed.add_field(name="Crew capacity", value=str(vdef["crew_capacity"]), inline=True)
        self.cog._apply_vehicle_image(embed, vdef)

        view = ConfirmVehiclePurchaseView(
            self.cog,
            self.channel,
            self.user,
            choice,
            guild=interaction.guild or self.guild,
            edit_message=True,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class VehicleShopView(discord.ui.View):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        guild: discord.Guild | None = None,
        data: Dict[str, Any] | None = None,
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.channel = channel
        self.user = user
        self.guild = guild
        data = data or {}
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", cog._command_level_for_xp(xp)))
        self.add_item(VehicleShopSelect(cog, channel, user, guild, command_level=command_level))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog._build_dashboard_embed(self.user)
        channel = interaction.channel or self.channel
        guild = interaction.guild or self.guild
        view = FscDashboardView(self.cog, self.user, channel, guild)
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class EquipmentShopSelect(discord.ui.Select):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        guild: discord.Guild | None = None,
        command_level: int = 1,
    ):
        self.cog = cog
        self.channel = channel
        self.user = user
        self.guild = guild

        options: List[discord.SelectOption] = []
        for equipment_id, equipment in self.cog.EQUIPMENT_CATALOG.items():
            unlock_level = int(equipment.get("unlock_level", 1))
            locked = command_level < unlock_level
            label = f"{equipment['name']} ({equipment['price']:,} cr)"
            description = None
            if locked:
                description = f"Requires command level {unlock_level}"
                label = f"Locked - {label}"
            options.append(
                discord.SelectOption(
                    label=label,
                    value=equipment_id,
                    description=description,
                    default=False,
                    emoji=None,
                )
            )

        if not options:
            options = [discord.SelectOption(label="No equipment available", value="none")]

        super().__init__(
            placeholder="Select equipment to buy",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        choice = self.values[0]
        if choice == "none":
            await interaction.response.send_message("No equipment available.", ephemeral=True)
            return

        equipment = self.cog.EQUIPMENT_CATALOG.get(choice)
        if not equipment:
            await interaction.response.send_message("Unknown equipment type.", ephemeral=True)
            return

        data = await self.cog.config.user(self.user).all()
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self.cog._command_level_for_xp(xp)))
        if not self.cog._equipment_is_unlocked(equipment, command_level):
            required_level = int(equipment.get("unlock_level", 1))
            embed = self.cog._build_equipment_shop_embed(data)
            embed.add_field(
                name="Purchase locked",
                value=f"{equipment['name']} requires command level {required_level}.",
                inline=False,
            )
            view = EquipmentShopView(
                self.cog,
                self.channel,
                self.user,
                interaction.guild or self.guild,
                data=data,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            return

        price = int(equipment["price"])
        embed = discord.Embed(
            title="Confirm equipment purchase",
            description=f"Buy **{equipment['name']}** for **{price:,}** credits?",
            color=discord.Color.blue(),
        )
        capabilities = equipment.get("capabilities", {})
        if isinstance(capabilities, dict) and capabilities:
            capability_text = ", ".join(f"{name}: {value}" for name, value in capabilities.items())
            embed.add_field(name="Capabilities", value=capability_text, inline=False)

        view = ConfirmEquipmentPurchaseView(
            self.cog,
            self.channel,
            self.user,
            choice,
            guild=interaction.guild or self.guild,
            edit_message=True,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class EquipmentShopView(discord.ui.View):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        guild: discord.Guild | None = None,
        data: Dict[str, Any] | None = None,
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.channel = channel
        self.user = user
        self.guild = guild
        data = data or {}
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", cog._command_level_for_xp(xp)))
        self.add_item(EquipmentShopSelect(cog, channel, user, guild, command_level=command_level))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog._build_dashboard_embed(self.user)
        channel = interaction.channel or self.channel
        guild = interaction.guild or self.guild
        view = FscDashboardView(self.cog, self.user, channel, guild)
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class ConfirmRecruitView(discord.ui.View):
    def __init__(
        self,
        cog: FireStationCommand,
        user: discord.abc.User,
        amount: int,
        cost: int,
        *,
        channel: discord.abc.Messageable | None = None,
        guild: discord.Guild | None = None,
        edit_message: bool = False,
    ):
        super().__init__(timeout=60)
        self.cog = cog
        self.user = user
        self.amount = amount
        self.cost = cost
        self.channel = channel
        self.guild = guild
        self.edit_message = edit_message

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog._confirm_recruit(
            interaction,
            self.user,
            self.amount,
            self.cost,
            edit_message=self.edit_message,
            channel=self.channel,
            guild=self.guild,
        )
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
        if self.edit_message:
            embed = await self.cog._build_recruitment_embed(self.user)
            embed.add_field(name="Recruitment cancelled", value="No staff were hired.", inline=False)
            view = RecruitmentView(
                self.cog,
                self.user,
                interaction.channel or self.channel,
                interaction.guild or self.guild,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            self.stop()
            return
        await interaction.response.send_message("Recruitment cancelled.", ephemeral=True)
        self.stop()


class ConfirmUpgradeView(discord.ui.View):
    def __init__(
        self,
        cog: FireStationCommand,
        user: discord.abc.User,
        new_level: int,
        cost: int,
        *,
        channel: discord.abc.Messageable | None = None,
        guild: discord.Guild | None = None,
        edit_message: bool = False,
    ):
        super().__init__(timeout=60)
        self.cog = cog
        self.user = user
        self.new_level = new_level
        self.cost = cost
        self.channel = channel
        self.guild = guild
        self.edit_message = edit_message

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog._confirm_upgrade(
            interaction,
            self.user,
            self.new_level,
            self.cost,
            edit_message=self.edit_message,
            channel=self.channel,
            guild=self.guild,
        )
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
        if self.edit_message:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Upgrade cancelled", value="Station level was not changed.", inline=False)
            view = FscDashboardView(
                self.cog,
                self.user,
                interaction.channel or self.channel,
                interaction.guild or self.guild,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            self.stop()
            return
        await interaction.response.send_message("Upgrade cancelled.", ephemeral=True)
        self.stop()


class ConfirmCareerView(discord.ui.View):
    def __init__(
        self,
        cog: FireStationCommand,
        user: discord.abc.User,
        cost: int,
        *,
        channel: discord.abc.Messageable | None = None,
        guild: discord.Guild | None = None,
        edit_message: bool = False,
    ):
        super().__init__(timeout=60)
        self.cog = cog
        self.user = user
        self.cost = cost
        self.channel = channel
        self.guild = guild
        self.edit_message = edit_message

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog._confirm_career(
            interaction,
            self.user,
            self.cost,
            edit_message=self.edit_message,
            channel=self.channel,
            guild=self.guild,
        )
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
        if self.edit_message:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Conversion cancelled", value="Station type was not changed.", inline=False)
            view = FscDashboardView(
                self.cog,
                self.user,
                interaction.channel or self.channel,
                interaction.guild or self.guild,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            self.stop()
            return
        await interaction.response.send_message("Conversion cancelled.", ephemeral=True)
        self.stop()


class ConfirmVehiclePurchaseView(discord.ui.View):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        vehicle_id: str,
        *,
        guild: discord.Guild | None = None,
        edit_message: bool = False,
    ):
        super().__init__(timeout=60)
        self.cog = cog
        self.channel = channel
        self.user = user
        self.vehicle_id = vehicle_id
        self.guild = guild
        self.edit_message = edit_message

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        if not self.edit_message:
            await interaction.message.edit(view=self)
        await self.cog._confirm_vehicle_purchase(
            interaction,
            self.channel,
            self.user,
            self.vehicle_id,
            edit_message=self.edit_message,
            guild=self.guild,
        )
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
        if self.edit_message:
            data = await self.cog.config.user(self.user).all()
            embed = self.cog._build_vehicle_shop_embed(data)
            embed.add_field(name="Purchase cancelled", value="No vehicle was purchased.", inline=False)
            view = VehicleShopView(
                self.cog,
                self.channel,
                self.user,
                interaction.guild or self.guild,
                data=data,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            self.stop()
            return
        await interaction.response.send_message("Purchase cancelled.", ephemeral=True)
        self.stop()


class ConfirmEquipmentPurchaseView(discord.ui.View):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        equipment_id: str,
        *,
        guild: discord.Guild | None = None,
        edit_message: bool = False,
    ):
        super().__init__(timeout=60)
        self.cog = cog
        self.channel = channel
        self.user = user
        self.equipment_id = equipment_id
        self.guild = guild
        self.edit_message = edit_message

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog._confirm_equipment_purchase(
            interaction,
            self.channel,
            self.user,
            self.equipment_id,
            edit_message=self.edit_message,
            guild=self.guild,
        )
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
        if self.edit_message:
            data = await self.cog.config.user(self.user).all()
            embed = self.cog._build_equipment_shop_embed(data)
            embed.add_field(name="Purchase cancelled", value="No equipment was purchased.", inline=False)
            view = EquipmentShopView(
                self.cog,
                self.channel,
                self.user,
                interaction.guild or self.guild,
                data=data,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            self.stop()
            return
        await interaction.response.send_message("Purchase cancelled.", ephemeral=True)
        self.stop()
