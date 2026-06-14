
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

    __version__ = "1.3.3"
    MISSION_SCHEMA_VERSION = 1
    MAX_COMMAND_LEVEL = 10
    STAGE_ALERT_CHOICE = "ALERT_CHOICE"
    STAGE_STAFF_TURNOUT = "STAFF_TURNOUT"
    STAGE_VEHICLE_SELECT = "VEHICLE_SELECT"
    STAGE_TRAVEL = "TRAVEL"
    STAGE_SCENE_BACKUP = "SCENE_BACKUP"
    STAGE_SCENE_WORK = "SCENE_WORK"
    ACTION_SHOW_TURNOUT_RESULT = "SHOW_TURNOUT_RESULT"
    ACTION_SHOW_TRAVEL_UPDATE = "SHOW_TRAVEL_UPDATE"
    ACTION_RESOLVE_BACKUP_WINDOW = "RESOLVE_BACKUP_WINDOW"
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
            "trainings": [],
            "expansions": [],
            "station_level": 1,
            "command_level": 1,
            "xp": 0,
            "reputation": 0,
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
        self.training_definitions = self._training_definitions()
        self.expansion_definitions = self._expansion_definitions()
        self.INCIDENTS = self._build_incidents()
        self.VEHICLE_CATALOG = self._build_vehicle_catalog()
        self.EQUIPMENT_CATALOG = self._build_equipment_catalog()
        self.TRAINING_CATALOG = self._build_training_catalog()
        self.EXPANSION_CATALOG = self._build_expansion_catalog()

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

    def _maintenance_multiplier(self) -> float:
        return max(0.0, self._balance_float("maintenance_cost_multiplier", 1.0))

    def _maintenance_out_of_service_threshold(self) -> int:
        return max(0, min(100, self._balance_int("maintenance_out_of_service_condition", 25)))

    def _maintenance_out_of_service_minutes(self) -> int:
        return max(1, self._balance_int("maintenance_out_of_service_minutes", 30))

    def _economy_scaled_cost(self, base_cost: int, credits: int) -> int:
        base = max(0, int(base_cost))
        if base <= 0:
            return 0
        threshold_multiplier = max(
            1.0,
            self._balance_float("economy_cost_scaling_threshold_multiplier", 10.0),
        )
        rate = max(0.0, self._balance_float("economy_cost_scaling_rate", 0.08))
        max_multiplier = max(1.0, self._balance_float("economy_cost_scaling_max_multiplier", 1.5))
        threshold = int(base * threshold_multiplier)
        if credits <= threshold or rate <= 0:
            return base
        scaled = base + int((credits - threshold) * rate)
        return min(int(base * max_multiplier), max(base, scaled))

    @staticmethod
    def _add_economy_pricing_note(embed: discord.Embed, base_cost: int, final_cost: int, credits: int) -> None:
        if final_cost <= base_cost:
            return
        embed.add_field(
            name="Economy pricing",
            value=(
                f"Base cost: {base_cost:,} credits. "
                f"Adjusted cost: {final_cost:,} credits based on your current balance of {credits:,}."
            ),
            inline=False,
        )

    def _command_level_from_data(self, data: Dict[str, Any]) -> int:
        xp = int(data.get("xp", 0))
        return int(data.get("command_level", self._command_level_for_xp(xp)))

    def _feature_available(self, data: Dict[str, Any], feature: str) -> bool:
        command_level = self._command_level_from_data(data)
        expansions = self._expansion_inventory_set(data.get("expansions", []))
        station_level = int(data.get("station_level", 1))

        if feature == "career_conversion":
            return station_level >= 2 and data.get("station_type", "volunteer") != "career"
        if feature == "expansions":
            return command_level >= 4 or bool(expansions)
        if feature == "maintenance":
            return command_level >= 4 or "workshop" in expansions
        if feature == "training":
            return command_level >= 5 or "training_facility" in expansions
        return True

    def _feature_locked_text(self, feature: str) -> str:
        if feature == "career_conversion":
            return "Career conversion unlocks at station level 2."
        if feature == "expansions":
            return "Station expansions unlock at command level 4."
        if feature == "maintenance":
            return "Maintenance bay unlocks at command level 4 or after building the workshop expansion."
        if feature == "training":
            return "Training desk unlocks at command level 5 or after building the training facility expansion."
        return "This feature is not available yet."

    def _reputation_delta_for_outcome(self, outcome_key: str) -> int:
        if outcome_key == "success":
            return self._balance_int("reputation_gain_success", 2)
        if outcome_key == "failure":
            return -self._balance_int("reputation_loss_fail", 3)
        if outcome_key == "skip":
            return -self._balance_int("reputation_loss_skip", 1)
        return 0

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

    def _training_definitions(self) -> Dict[str, Dict[str, Any]]:
        trainings = self.game_data.get("trainings", {}).get("trainings", [])
        if not isinstance(trainings, list):
            return {}

        definitions: Dict[str, Dict[str, Any]] = {}
        for training in trainings:
            if not isinstance(training, dict):
                continue
            training_id = training.get("id")
            if isinstance(training_id, str) and training_id:
                definitions[training_id] = training
        return definitions

    def _expansion_definitions(self) -> Dict[str, Dict[str, Any]]:
        expansions = self.game_data.get("expansions", {}).get("expansions", [])
        if not isinstance(expansions, list):
            return {}

        definitions: Dict[str, Dict[str, Any]] = {}
        for expansion in expansions:
            if not isinstance(expansion, dict):
                continue
            expansion_id = expansion.get("id")
            if isinstance(expansion_id, str) and expansion_id:
                definitions[expansion_id] = expansion
        return definitions

    @staticmethod
    def _training_inventory_set(trainings: Any) -> set[str]:
        if not isinstance(trainings, list):
            return set()
        trained: set[str] = set()
        for training in trainings:
            if isinstance(training, str) and training:
                trained.add(training)
            elif isinstance(training, dict):
                training_id = training.get("id") or training.get("catalog_id")
                if training_id:
                    trained.add(str(training_id))
        return trained

    @staticmethod
    def _expansion_inventory_set(expansions: Any) -> set[str]:
        if not isinstance(expansions, list):
            return set()
        owned: set[str] = set()
        for expansion in expansions:
            if isinstance(expansion, str) and expansion:
                owned.add(expansion)
            elif isinstance(expansion, dict):
                expansion_id = expansion.get("id") or expansion.get("catalog_id")
                if expansion_id:
                    owned.add(str(expansion_id))
        return owned

    def _expansion_effect_totals(self, expansions: Any) -> Dict[str, float]:
        totals: Dict[str, float] = {}
        for expansion_id in self._expansion_inventory_set(expansions):
            expansion = self.expansion_definitions.get(expansion_id)
            if not expansion:
                continue
            effects = expansion.get("effects", {})
            if not isinstance(effects, dict):
                continue
            for effect, raw_value in effects.items():
                try:
                    value = float(raw_value)
                except (TypeError, ValueError):
                    continue
                if isinstance(effect, str):
                    totals[effect] = totals.get(effect, 0.0) + value
        return totals

    @staticmethod
    def _required_expansion_ids(item: Dict[str, Any]) -> List[str]:
        required = item.get("required_expansions", [])
        if not isinstance(required, list):
            return []
        return [str(expansion_id) for expansion_id in required if expansion_id]

    def _missing_required_expansion_ids(self, item: Dict[str, Any], data: Dict[str, Any]) -> List[str]:
        owned = self._expansion_inventory_set(data.get("expansions", []))
        return [expansion_id for expansion_id in self._required_expansion_ids(item) if expansion_id not in owned]

    def _expansion_requirement_display_text(self, expansion_ids: Any) -> str | None:
        if not isinstance(expansion_ids, list) or not expansion_ids:
            return None
        names = []
        for expansion_id in expansion_ids:
            expansion_key = str(expansion_id)
            expansion = self.expansion_definitions.get(expansion_key)
            name = expansion.get("name") if expansion else None
            names.append(name if isinstance(name, str) else expansion_key)
        return ", ".join(names)

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

    @staticmethod
    def _vehicle_condition(vehicle: Dict[str, Any]) -> int:
        try:
            condition = int(vehicle.get("condition", 100))
        except (TypeError, ValueError):
            condition = 100
        return max(0, min(100, condition))

    def _vehicle_out_of_service_until(self, vehicle: Dict[str, Any]) -> datetime | None:
        return self._parse_timestamp(vehicle.get("out_of_service_until"))

    def _vehicle_is_out_of_service(self, vehicle: Dict[str, Any]) -> bool:
        until = self._vehicle_out_of_service_until(vehicle)
        return until is not None and until > self._utcnow()

    def _available_vehicles(self, vehicles: Any) -> List[Dict[str, Any]]:
        if not isinstance(vehicles, list):
            return []
        return [
            vehicle
            for vehicle in vehicles
            if isinstance(vehicle, dict) and not self._vehicle_is_out_of_service(vehicle)
        ]

    def _out_of_service_vehicle_text(self, vehicles: Any) -> str | None:
        if not isinstance(vehicles, list):
            return None
        rows: List[str] = []
        for vehicle in vehicles:
            if not isinstance(vehicle, dict) or not self._vehicle_is_out_of_service(vehicle):
                continue
            name = vehicle.get("name", "Vehicle")
            until = self._vehicle_out_of_service_until(vehicle)
            rows.append(f"{name}: unavailable until {until.isoformat() if until else 'maintenance clears'}")
        return "\n".join(rows[:10]) if rows else None

    def _vehicle_maintenance_cost(self, vehicle: Dict[str, Any]) -> int:
        condition = self._vehicle_condition(vehicle)
        if condition >= 100:
            return 0

        vehicle_id = str(vehicle.get("catalog_id", ""))
        configured = self.vehicle_definitions.get(vehicle_id, {})
        catalog = self.VEHICLE_CATALOG.get(vehicle_id, {})
        base_cost = configured.get("maintenance_cost", catalog.get("maintenance_cost", 500))
        try:
            base = int(base_cost)
        except (TypeError, ValueError):
            base = 500
        return int(math.ceil(base * self._maintenance_multiplier() * ((100 - condition) / 100)))

    def _fleet_maintenance_cost(self, vehicles: Any) -> int:
        if not isinstance(vehicles, list):
            return 0
        return sum(self._vehicle_maintenance_cost(vehicle) for vehicle in vehicles if isinstance(vehicle, dict))

    def _fleet_condition_text(self, vehicles: Any) -> str:
        if not isinstance(vehicles, list) or not vehicles:
            return "No vehicles."

        damaged: List[str] = []
        for vehicle in vehicles:
            if not isinstance(vehicle, dict):
                continue
            condition = self._vehicle_condition(vehicle)
            name = vehicle.get("name", "Vehicle")
            if condition < 100:
                damaged.append(f"{name}: {condition}% ({self._vehicle_maintenance_cost(vehicle):,} cr)")

        if not damaged:
            return "All vehicles are at 100% condition."
        return "\n".join(damaged[:10])

    @staticmethod
    def _vehicle_wear_for_outcome(outcome_key: str) -> int:
        if outcome_key == "success":
            return random.randint(3, 7)
        if outcome_key == "partial":
            return random.randint(6, 12)
        return random.randint(10, 18)

    def _apply_vehicle_wear(
        self,
        vehicles: Any,
        selected_ids: Any,
        outcome_key: str,
        data: Dict[str, Any] | None = None,
    ) -> List[Dict[str, Any]]:
        if not isinstance(vehicles, list):
            return []
        selected = {int(vehicle_id) for vehicle_id in selected_ids if str(vehicle_id).isdigit()}
        if not selected:
            return vehicles

        updated: List[Dict[str, Any]] = []
        for vehicle in vehicles:
            if not isinstance(vehicle, dict):
                continue
            current = dict(vehicle)
            try:
                vehicle_id = int(current.get("id"))
            except (TypeError, ValueError):
                vehicle_id = -1
            if vehicle_id in selected:
                condition = max(
                    0,
                    self._vehicle_condition(current) - self._vehicle_wear_for_outcome(outcome_key),
                )
                current["condition"] = condition
                if data is not None and self._feature_available(data, "maintenance") and (
                    condition <= self._maintenance_out_of_service_threshold()
                ):
                    current["out_of_service_until"] = self._timestamp_after_minutes(
                        self._maintenance_out_of_service_minutes()
                    )
            updated.append(current)
        return updated

    def _station_capabilities(self, vehicles: Any, equipment_inventory: Any = None) -> Dict[str, float]:
        if not isinstance(vehicles, list):
            vehicles = []

        equipment_counts = self._equipment_inventory_counts(equipment_inventory)
        totals: Dict[str, float] = {}
        for owned in vehicles:
            if not isinstance(owned, dict):
                continue
            if self._vehicle_is_out_of_service(owned):
                continue
            vehicle_id = owned.get("catalog_id")
            vehicle = self.vehicle_definitions.get(str(vehicle_id))
            if not vehicle:
                continue
            condition_factor = self._vehicle_condition(owned) / 100

            for capability, value in self._capabilities_from(vehicle).items():
                totals[capability] = totals.get(capability, 0.0) + (value * condition_factor)

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
                        totals[capability] = totals.get(capability, 0.0) + (value * condition_factor)

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
        missing_training = self._missing_required_training_ids(mission, data)
        training_score = 1.0 if not missing_training else max(0.0, 1.0 - (len(missing_training) * 0.25))
        level_required = self._unlock_level(mission)
        level_score = 1.0 if command_level >= level_required else max(0.0, command_level / level_required)

        score = (
            capability_score * 0.40
            + staff_score * 0.20
            + vehicle_score * 0.20
            + training_score * 0.10
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
            "scene_backup_chance": 0.25,
            "scene_backup_window_minutes_min": 0.5,
            "scene_backup_window_minutes_max": 1.0,
            "scene_backup_travel_minutes_min": 0.5,
            "scene_backup_travel_minutes_max": 1.5,
            "maintenance_out_of_service_condition": self._maintenance_out_of_service_threshold(),
            "maintenance_out_of_service_minutes": self._maintenance_out_of_service_minutes(),
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

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime | None:
        if not isinstance(value, str) or not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

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
            "required_expansions": incident.get("required_expansions", []),
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

    def _mission_due_action_ready(self, mission: Dict[str, Any]) -> bool:
        if not mission.get("next_action"):
            return False
        due_at = self._parse_timestamp(mission.get("next_action_at"))
        if due_at is None:
            return False
        return due_at <= self._utcnow()

    async def _run_due_mission_action(
        self,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
    ) -> bool:
        data = await self.config.user(user).all()
        mission = data.get("active_mission", {}) or {}
        if not self._mission_due_action_ready(mission):
            return False

        action = mission.get("next_action")
        if action == self.ACTION_SHOW_TURNOUT_RESULT:
            await self._show_turnout_result(channel, user)
            return True
        if action == self.ACTION_SHOW_TRAVEL_UPDATE:
            await self._send_travel_update(channel, user, sleep_after=False)
            return True
        if action == self.ACTION_RESOLVE_BACKUP_WINDOW:
            await self._resolve_backup_window(channel, user, sleep_after=False)
            return True
        if action == self.ACTION_RESOLVE_INCIDENT:
            await self._resolve_incident(channel, user)
            return True
        return False

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

    def _training_display_text(self, training_ids: Any) -> str | None:
        if not isinstance(training_ids, list) or not training_ids:
            return None

        names = []
        for training_id in training_ids:
            training_key = str(training_id)
            training = self.training_definitions.get(training_key)
            name = training.get("name") if training else None
            names.append(name if isinstance(name, str) else training_key)
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

    def _missing_required_training_ids(self, mission: Dict[str, Any], data: Dict[str, Any]) -> List[str]:
        trained = self._training_inventory_set(data.get("trainings", []))
        required_training: List[str] = []

        required_vehicles = mission.get("required_vehicles", [])
        if isinstance(required_vehicles, list):
            for vehicle_id in required_vehicles:
                vehicle = self.vehicle_definitions.get(str(vehicle_id))
                if not vehicle:
                    continue
                trainings = vehicle.get("required_training", [])
                if isinstance(trainings, list):
                    required_training.extend(str(training) for training in trainings)

        required_equipment = mission.get("required_equipment", [])
        if isinstance(required_equipment, list):
            for equipment_id in required_equipment:
                equipment = self.equipment_definitions.get(str(equipment_id))
                if not equipment:
                    continue
                trainings = equipment.get("required_training", [])
                if isinstance(trainings, list):
                    required_training.extend(str(training) for training in trainings)

        missing: List[str] = []
        for training_id in required_training:
            if training_id and training_id not in trained and training_id not in missing:
                missing.append(training_id)
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

    def _missing_required_vehicle_ids_from_selection(
        self,
        mission: Dict[str, Any],
        selected_vehicles: Any,
    ) -> List[str]:
        required = mission.get("required_vehicles", [])
        if not isinstance(required, list) or not required:
            return []
        if not isinstance(selected_vehicles, list):
            selected_vehicles = []

        selected_catalog_ids = {
            str(vehicle.get("catalog_id"))
            for vehicle in selected_vehicles
            if isinstance(vehicle, dict) and vehicle.get("catalog_id")
        }
        return [str(vehicle_id) for vehicle_id in required if str(vehicle_id) not in selected_catalog_ids]

    def _backup_candidate_vehicles(
        self,
        vehicles: Any,
        mission: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        if not isinstance(vehicles, list):
            return []
        selected_ids = mission.get("selected_vehicle_ids", [])
        backup_ids = mission.get("backup_vehicle_ids", [])
        if not isinstance(selected_ids, list):
            selected_ids = []
        if not isinstance(backup_ids, list):
            backup_ids = []
        dispatched_ids = {
            int(vehicle_id)
            for vehicle_id in selected_ids + backup_ids
            if isinstance(vehicle_id, int) or (isinstance(vehicle_id, str) and vehicle_id.isdigit())
        }
        return [
            vehicle
            for vehicle in vehicles
            if isinstance(vehicle, dict)
            and isinstance(vehicle.get("id"), int)
            and int(vehicle["id"]) not in dispatched_ids
        ]

    def _scene_backup_vehicle_requirements(
        self,
        mission: Dict[str, Any],
        selected_vehicles: Any,
    ) -> List[str]:
        missing = self._missing_required_vehicle_ids_from_selection(mission, selected_vehicles)
        if missing:
            return missing

        required = mission.get("required_vehicles", [])
        if isinstance(required, list) and required:
            return [str(required[0])]
        return []

    def _should_request_scene_backup(
        self,
        mission: Dict[str, Any],
        data: Dict[str, Any],
        selected_vehicles: Any,
        chance: float,
    ) -> bool:
        del data
        if mission.get("backup_window_opened") or mission.get("backup_vehicle_ids"):
            return False
        if self._missing_required_vehicle_ids_from_selection(mission, selected_vehicles):
            return True

        readiness = mission.get("readiness_score")
        if isinstance(readiness, int) and readiness < 80:
            return True

        return random.random() < max(0.0, min(1.0, chance))

    def _add_mission_requirement_fields(self, embed: discord.Embed, mission: Dict[str, Any]) -> None:
        embed.add_field(name="Required staff", value=str(mission.get("required_staff", "Unknown")), inline=True)
        vehicle_text = self._vehicle_requirement_display_text(mission.get("required_vehicles"))
        if vehicle_text:
            embed.add_field(name="Required vehicles", value=vehicle_text, inline=False)
        equipment_text = self._equipment_display_text(mission.get("required_equipment"))
        if equipment_text:
            embed.add_field(name="Required equipment", value=equipment_text, inline=False)
        expansion_text = self._expansion_requirement_display_text(mission.get("required_expansions"))
        if expansion_text:
            embed.add_field(name="Required expansions", value=expansion_text, inline=False)
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
        missing_training = mission.get("missing_required_training", [])
        missing_training_text = self._training_display_text(missing_training)
        if missing_training_text:
            embed.add_field(
                name="Training readiness",
                value=f"Missing training: {missing_training_text}",
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
                    "required_expansions": mission.get("required_expansions", []),
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
                "required_training": vehicle.get("required_training", []),
                "required_expansions": vehicle.get("required_expansions", []),
                "maintenance_cost": int(vehicle.get("maintenance_cost", 500)),
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
                "image": equipment.get("image"),
                "unlock_level": self._unlock_level(equipment),
                "capabilities": equipment.get("capabilities", {}),
                "required_training": equipment.get("required_training", []),
                "required_expansions": equipment.get("required_expansions", []),
            }
        return catalog

    def _build_training_catalog(self) -> Dict[str, Dict[str, Any]]:
        catalog: Dict[str, Dict[str, Any]] = {}
        for training_id, training in self.training_definitions.items():
            name = training.get("name")
            if not isinstance(name, str):
                continue
            catalog[training_id] = {
                "name": name,
                "price": int(training.get("cost", 0)),
                "unlock_level": self._unlock_level(training),
                "duration_hours": int(training.get("duration_hours", 0)),
            }
        return catalog

    def _build_expansion_catalog(self) -> Dict[str, Dict[str, Any]]:
        catalog: Dict[str, Dict[str, Any]] = {}
        for expansion_id, expansion in self.expansion_definitions.items():
            name = expansion.get("name")
            if not isinstance(name, str):
                continue
            catalog[expansion_id] = {
                "name": name,
                "description": expansion.get("description", ""),
                "price": int(expansion.get("base_cost", 0)),
                "unlock_level": self._unlock_level(expansion),
                "build_time_hours": int(expansion.get("build_time_hours", 0)),
                "effects": expansion.get("effects", {}),
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
            "condition": 100,
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
        await user_conf.trainings.set(["basic_firefighting"])
        await user_conf.expansions.set([])
        await user_conf.station_level.set(1)
        await user_conf.command_level.set(1)
        await user_conf.xp.set(0)
        await user_conf.reputation.set(0)
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
        embed.add_field(name="Staff", value="6 volunteers", inline=True)
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

    def _max_vehicles_for_data(self, data: Dict[str, Any]) -> int:
        level = int(data.get("station_level", 1))
        base = self._max_vehicles(level)
        effects = self._expansion_effect_totals(data.get("expansions", []))
        return base + int(effects.get("extra_vehicle_slots", 0))

    def _xp_progress_text(self, xp: int, command_level: int) -> str:
        next_xp = self._xp_for_next_command_level(command_level)
        if next_xp is None:
            return f"Level {command_level} - {xp:,} XP (max)"
        return f"Level {command_level} - {xp:,} / {next_xp:,} XP"

    def _vehicle_is_unlocked(
        self,
        vehicle: Dict[str, Any],
        command_level: int,
        data: Dict[str, Any] | None = None,
    ) -> bool:
        if command_level < int(vehicle.get("unlock_level", 1)):
            return False
        return data is None or not self._missing_required_expansion_ids(vehicle, data)

    def _equipment_is_unlocked(
        self,
        equipment: Dict[str, Any],
        command_level: int,
        data: Dict[str, Any] | None = None,
    ) -> bool:
        if command_level < int(equipment.get("unlock_level", 1)):
            return False
        return data is None or not self._missing_required_expansion_ids(equipment, data)

    def _training_is_unlocked(self, training: Dict[str, Any], command_level: int) -> bool:
        return command_level >= int(training.get("unlock_level", 1))

    def _expansion_is_unlocked(self, expansion: Dict[str, Any], command_level: int) -> bool:
        return command_level >= int(expansion.get("unlock_level", 1))

    def _pick_random_incident(self, data: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if not data:
            return random.choice(self.INCIDENTS)

        command_level = int(data.get("command_level", 1))
        challenge_limit = self._mission_challenge_limit(command_level)
        eligible: List[Dict[str, Any]] = []
        challenge: List[Dict[str, Any]] = []
        fallback: List[Dict[str, Any]] = []

        for incident in self.INCIDENTS:
            if self._missing_required_expansion_ids(incident, data):
                continue
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
        core_incidents = [
            incident for incident in self.INCIDENTS if not self._missing_required_expansion_ids(incident, data)
        ]
        if core_incidents:
            return random.choice(core_incidents)
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

    @staticmethod
    def _turnout_timeout_narrative() -> str:
        return (
            "Dispatch calls the station again, but the decision board stays silent. "
            "The original crew has not confirmed how to continue, so command opens the incident "
            "for another station member before the call is lost."
        )

    @staticmethod
    def _takeover_dispatch_narrative() -> str:
        return (
            "A new station officer acknowledges the open incident. Dispatch transfers the notes, "
            "repeats the address, and restarts the assignment as a fresh response."
        )

    @staticmethod
    def _abandoned_dispatch_narrative() -> str:
        return (
            "No one takes the radio. Dispatch repeats the call one last time, voice tight and urgent, "
            "then marks the assignment abandoned. The channel goes quiet with only the hope that "
            "someone closer can still make a difference."
        )

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
        available_vehicles = self._available_vehicles(vehicles)
        equipment_count = sum(self._equipment_inventory_counts(data.get("equipment", [])).values())
        training_count = len(self._training_inventory_set(data.get("trainings", [])))
        expansion_count = len(self._expansion_inventory_set(data.get("expansions", [])))
        credits = await self._get_credits(user)
        lvl = int(data.get("station_level", 1))
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        reputation = int(data.get("reputation", 0))
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
        embed.add_field(name="Reputation", value=str(reputation), inline=True)
        embed.add_field(name="Command XP", value=self._xp_progress_text(xp, command_level), inline=False)
        embed.add_field(
            name="Staff",
            value=f"{staff_total} total ({staff_trained} trained) / max {self._max_staff(lvl)}",
            inline=False,
        )
        embed.add_field(
            name="Vehicles",
            value=f"{len(vehicles)} / max {self._max_vehicles_for_data(data)} ({len(available_vehicles)} available)",
            inline=False,
        )
        embed.add_field(name="Equipment", value=f"{equipment_count} item(s)", inline=True)
        if self._feature_available(data, "training"):
            embed.add_field(name="Training", value=f"{training_count} certification(s)", inline=True)
        embed.add_field(name="Expansions", value=f"{expansion_count} built", inline=True)
        if self._feature_available(data, "maintenance"):
            embed.add_field(
                name="Maintenance",
                value=f"Repair estimate: {self._fleet_maintenance_cost(vehicles):,} credits",
                inline=False,
            )
        out_of_service_text = self._out_of_service_vehicle_text(vehicles)
        if out_of_service_text:
            embed.add_field(name="Out of service", value=out_of_service_text, inline=False)
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
        elif stage == self.STAGE_SCENE_BACKUP:
            guidance = (
                "On-scene command is requesting more resources. Dispatch backup before the deadline "
                "or the incident will continue to the result with current resources."
            )
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

        backup_ids = mission.get("backup_vehicle_ids", [])
        if isinstance(backup_ids, list) and backup_ids:
            embed.add_field(name="Backup vehicles", value=str(len(backup_ids)), inline=True)

        backup_required = mission.get("backup_required_vehicle_ids", [])
        backup_text = self._vehicle_requirement_display_text(backup_required)
        if backup_text:
            embed.add_field(name="Requested backup", value=backup_text, inline=False)

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
        if self._mission_is_stage(mission, self.STAGE_VEHICLE_SELECT) or self._mission_is_stage(
            mission, self.STAGE_SCENE_BACKUP
        ):
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
        reputation = int(data.get("reputation", 0))
        stype = data.get("station_type", "volunteer")
        staff_total = int(data.get("staff_total", 0))
        staff_trained = int(data.get("staff_trained", 0))
        vehicles = data.get("vehicles", [])

        max_staff = self._max_staff(lvl)
        max_veh = self._max_vehicles_for_data(data)

        embed = discord.Embed(
            title="Station overview",
            color=discord.Color.dark_red(),
        )
        embed.add_field(name="Level", value=str(lvl), inline=True)
        embed.add_field(name="Reputation", value=str(reputation), inline=True)
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

    def _build_maintenance_embed(self, data: Dict[str, Any]) -> discord.Embed:
        vehicles = data.get("vehicles", [])
        total_cost = self._fleet_maintenance_cost(vehicles)
        out_of_service = self._out_of_service_vehicle_text(vehicles)

        embed = discord.Embed(
            title="Maintenance bay",
            description="Vehicle condition affects mission readiness, capability output, and temporary availability.",
            color=discord.Color.dark_gray(),
        )
        embed.add_field(name="Fleet condition", value=self._fleet_condition_text(vehicles), inline=False)
        if out_of_service:
            embed.add_field(name="Out of service", value=out_of_service, inline=False)
        embed.add_field(name="Repair estimate", value=f"{total_cost:,} credits", inline=True)
        if total_cost <= 0:
            embed.add_field(name="Status", value="No repairs are needed.", inline=False)
        else:
            embed.add_field(
                name="Repair action",
                value="Use Repair fleet to restore all damaged vehicles to 100%.",
                inline=False,
            )
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

    def _mission_result_image_url(self, mission: Dict[str, Any], outcome_key: str) -> str | None:
        outcome_images = {
            "success": "Images/Outcomes/incident_success.png",
            "partial": "Images/Outcomes/incident_partial.png",
            "failure": "Images/Outcomes/incident_failure.png",
        }
        specific_key = f"{outcome_key}_image"
        specific_image = mission.get(specific_key)
        if isinstance(specific_image, str) and specific_image:
            return self._asset_image_url(specific_image)
        return self._asset_image_url(outcome_images.get(outcome_key))

    def _apply_mission_result_image(
        self,
        embed: discord.Embed,
        mission: Dict[str, Any],
        outcome_key: str,
    ) -> None:
        image_url = self._mission_result_image_url(mission, outcome_key)
        if image_url:
            embed.set_image(url=image_url)

    def _vehicle_image_url(self, vehicle: Dict[str, Any]) -> str | None:
        image = vehicle.get("image")
        return self._asset_image_url(image if isinstance(image, str) else None)

    def _apply_vehicle_image(self, embed: discord.Embed, vehicle: Dict[str, Any]) -> None:
        image_url = self._vehicle_image_url(vehicle)
        if image_url:
            embed.set_image(url=image_url)

    def _equipment_image_url(self, equipment: Dict[str, Any]) -> str | None:
        image = equipment.get("image")
        return self._asset_image_url(image if isinstance(image, str) else None)

    def _apply_equipment_image(self, embed: discord.Embed, equipment: Dict[str, Any]) -> None:
        image_url = self._equipment_image_url(equipment)
        if image_url:
            embed.set_image(url=image_url)

    @staticmethod
    def _compact_display_list(items: List[str], *, limit: int = 12) -> str:
        if len(items) <= limit:
            return ", ".join(items)
        shown = ", ".join(items[:limit])
        return f"{shown}, +{len(items) - limit} more"

    @staticmethod
    def _compact_bullet_list(items: List[str], *, limit: int = 8) -> str:
        if not items:
            return "None"
        shown = [f"- {item}" for item in items[:limit]]
        if len(items) > limit:
            shown.append(f"- +{len(items) - limit} more")
        return "\n".join(shown)

    @staticmethod
    def _grouped_unlock_list(items: List[Dict[str, Any]], *, limit: int = 8) -> str:
        if not items:
            return "None"
        grouped: Dict[int, List[str]] = {}
        for item in items:
            level = int(item.get("unlock_level", 1))
            grouped.setdefault(level, []).append(str(item.get("name", "Unknown")))

        lines: List[str] = []
        shown = 0
        total = sum(len(names) for names in grouped.values())
        for level in sorted(grouped):
            names = sorted(grouped[level])
            remaining = limit - shown
            if remaining <= 0:
                break
            selected = names[:remaining]
            lines.append(f"- Level {level}: {', '.join(selected)}")
            shown += len(selected)

        if total > shown:
            lines.append(f"- +{total - shown} more")
        return "\n".join(lines)

    @staticmethod
    def _grouped_requirement_list(items: List[tuple[str, str]], *, limit: int = 8) -> str:
        if not items:
            return "None"
        grouped: Dict[str, List[str]] = {}
        for name, requirement in items:
            grouped.setdefault(requirement, []).append(name)

        lines: List[str] = []
        shown = 0
        total = sum(len(names) for names in grouped.values())
        for requirement in sorted(grouped):
            names = sorted(grouped[requirement])
            remaining = limit - shown
            if remaining <= 0:
                break
            selected = names[:remaining]
            lines.append(f"- {requirement}: {', '.join(selected)}")
            shown += len(selected)

        if total > shown:
            lines.append(f"- +{total - shown} more")
        return "\n".join(lines)

    def _build_vehicle_shop_embed(self, data: Dict[str, Any]) -> discord.Embed:
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        vehicles = data.get("vehicles", [])
        max_veh = self._max_vehicles_for_data(data)
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
            vehicle
            for vehicle in self.VEHICLE_CATALOG.values()
            if command_level < int(vehicle.get("unlock_level", 1))
        ]
        expansion_locked = [
            (vehicle["name"], self._expansion_requirement_display_text(missing) or "Expansion required")
            for vehicle in self.VEHICLE_CATALOG.values()
            if command_level >= int(vehicle.get("unlock_level", 1))
            for missing in [self._missing_required_expansion_ids(vehicle, data)]
            if missing
        ]
        if locked:
            embed.add_field(name="Locked vehicles", value=self._grouped_unlock_list(locked), inline=False)
        if expansion_locked:
            embed.add_field(
                name="Expansion locked vehicles",
                value=self._grouped_requirement_list(expansion_locked),
                inline=False,
            )
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
            equipment
            for equipment in self.EQUIPMENT_CATALOG.values()
            if command_level < int(equipment.get("unlock_level", 1))
        ]
        expansion_locked = [
            (equipment["name"], self._expansion_requirement_display_text(missing) or "Expansion required")
            for equipment in self.EQUIPMENT_CATALOG.values()
            if command_level >= int(equipment.get("unlock_level", 1))
            for missing in [self._missing_required_expansion_ids(equipment, data)]
            if missing
        ]

        embed = discord.Embed(
            title="Equipment shop",
            description="Buy equipment to improve mission readiness.",
            color=discord.Color.blue(),
        )
        embed.add_field(name="Owned equipment", value=self._compact_bullet_list(owned, limit=6), inline=False)
        embed.add_field(name="Command XP", value=self._xp_progress_text(int(data.get("xp", 0)), command_level), inline=False)
        if locked:
            embed.add_field(name="Locked equipment", value=self._grouped_unlock_list(locked), inline=False)
        if expansion_locked:
            embed.add_field(
                name="Expansion locked equipment",
                value=self._grouped_requirement_list(expansion_locked),
                inline=False,
            )
        return embed

    def _build_training_embed(self, data: Dict[str, Any]) -> discord.Embed:
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        trained = self._training_inventory_set(data.get("trainings", []))
        owned = [
            self.TRAINING_CATALOG.get(training_id, {}).get("name", training_id)
            for training_id in sorted(trained)
        ]
        available = []
        locked = []
        for training_id, training in self.TRAINING_CATALOG.items():
            if training_id in trained:
                continue
            entry = f"{training['name']} ({training['price']:,} cr)"
            if self._training_is_unlocked(training, command_level):
                available.append(entry)
            else:
                locked.append(f"{training['name']} (level {training.get('unlock_level', 1)})")

        embed = discord.Embed(
            title="Training desk",
            description="Complete permanent station certifications to unlock safer and more capable responses.",
            color=discord.Color.gold(),
        )
        embed.add_field(
            name="Training scope",
            value="Station-wide certification. It applies to current and future staff and is not consumed per member.",
            inline=False,
        )
        embed.add_field(name="Completed training", value=", ".join(owned) if owned else "None", inline=False)
        embed.add_field(name="Command XP", value=self._xp_progress_text(xp, command_level), inline=False)
        if available:
            embed.add_field(name="Available training", value=", ".join(available), inline=False)
        if locked:
            embed.add_field(name="Locked training", value=", ".join(locked), inline=False)
        return embed

    def _build_expansion_embed(self, data: Dict[str, Any]) -> discord.Embed:
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        owned_ids = self._expansion_inventory_set(data.get("expansions", []))
        owned = [
            self.EXPANSION_CATALOG.get(expansion_id, {}).get("name", expansion_id)
            for expansion_id in sorted(owned_ids)
        ]
        available = []
        locked = []
        for expansion_id, expansion in self.EXPANSION_CATALOG.items():
            if expansion_id in owned_ids:
                continue
            entry = f"{expansion['name']} ({expansion['price']:,} cr)"
            if self._expansion_is_unlocked(expansion, command_level):
                available.append(entry)
            else:
                locked.append(f"{expansion['name']} (level {expansion.get('unlock_level', 1)})")

        embed = discord.Embed(
            title="Station expansions",
            description="Build permanent station upgrades.",
            color=discord.Color.blue(),
        )
        embed.add_field(name="Built expansions", value=", ".join(owned) if owned else "None", inline=False)
        embed.add_field(name="Command XP", value=self._xp_progress_text(xp, command_level), inline=False)
        if available:
            embed.add_field(name="Available expansions", value=", ".join(available), inline=False)
        if locked:
            embed.add_field(name="Locked expansions", value=", ".join(locked), inline=False)
        embed.add_field(
            name="Vehicle capacity",
            value=f"{len(data.get('vehicles', []))} / {self._max_vehicles_for_data(data)}",
            inline=True,
        )
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
        vehicles = data.get("vehicles", [])
        max_veh = self._max_vehicles_for_data(data)
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
        message = await send(embed=embed, view=view, **kwargs)
        if message is not None:
            view.message = message

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
        mission["missing_required_training"] = self._missing_required_training_ids(incident, data)
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
        message = await send(embed=embed, view=view, **kwargs)
        if message is not None:
            view.message = message

    async def _send_dashboard(self, ctx: commands.Context) -> None:
        data = await self.config.user(ctx.author).all()
        embed = await self._build_dashboard_embed(ctx.author)
        if data["started"]:
            view = FscDashboardView(self, ctx.author, ctx.channel, ctx.guild, data=data)
        else:
            view = FscStartView(self, ctx.author)
        message = await ctx.send(embed=embed, view=view)
        view.message = message

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
        reputation = int(data.get("reputation", 0))
        stype = data.get("station_type", "volunteer")
        staff_total = int(data.get("staff_total", 0))
        staff_trained = int(data.get("staff_trained", 0))
        active = data.get("active_mission", {}) or {}

        max_staff = self._max_staff(lvl)
        max_veh = self._max_vehicles_for_data(data)

        embed = discord.Embed(
            title="Fire Station Status",
            color=discord.Color.red(),
        )
        embed.add_field(name="Credits", value=f"{credits:,}", inline=True)
        embed.add_field(name="Station level", value=str(lvl), inline=True)
        embed.add_field(name="Type", value=stype.capitalize(), inline=True)
        embed.add_field(name="Reputation", value=str(reputation), inline=True)
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
        embed.add_field(
            name="Maintenance",
            value=f"Repair estimate: {self._fleet_maintenance_cost(vehicles):,} credits",
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
        reputation = int(data.get("reputation", 0))
        stype = data.get("station_type", "volunteer")
        staff_total = int(data.get("staff_total", 0))
        staff_trained = int(data.get("staff_trained", 0))
        vehicles = data.get("vehicles", [])
        expansion_count = len(self._expansion_inventory_set(data.get("expansions", [])))
        equipment_count = sum(self._equipment_inventory_counts(data.get("equipment", [])).values())
        training_count = len(self._training_inventory_set(data.get("trainings", [])))

        max_staff = self._max_staff(lvl)
        max_veh = self._max_vehicles_for_data(data)

        embed = discord.Embed(
            title="Station overview",
            color=discord.Color.dark_red(),
        )
        embed.add_field(name="Level", value=str(lvl), inline=True)
        embed.add_field(name="Type", value=stype.capitalize(), inline=True)
        embed.add_field(name="Reputation", value=str(reputation), inline=True)
        embed.add_field(name="Vehicle capacity", value=f"{len(vehicles)} / {max_veh}", inline=True)
        embed.add_field(name="Expansions", value=f"{expansion_count} built", inline=True)
        embed.add_field(name="Equipment", value=f"{equipment_count} item(s)", inline=True)
        embed.add_field(name="Training", value=f"{training_count} certification(s)", inline=True)
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
        message = await ctx.send(embed=embed, view=view)
        view.message = message

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

        new_lvl = lvl + 1
        if command_level < new_lvl:
            await ctx.send(
                f"Station upgrade to level {new_lvl} is not available yet. "
                f"Reach command level {new_lvl} first. Current progress: {self._xp_progress_text(xp, command_level)}."
            )
            return

        base = int(glb.get("upgrade_base_cost", 50000))
        cost = base * lvl

        credits = await self._get_credits(ctx.author)
        base_cost = cost
        cost = self._economy_scaled_cost(base_cost, credits)
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
        self._add_economy_pricing_note(embed, base_cost, cost, credits)

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
                    name="Upgrade not available yet",
                    value=(
                        f"Station level {new_level} unlocks at command level {new_level}. "
                        f"Current progress: {self._xp_progress_text(xp, command_level)}."
                    ),
                    inline=False,
                )
                view = FscDashboardView(self, user, channel or interaction.channel, guild or interaction.guild)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                f"Upgrade not available yet: command level {new_level} required.", ephemeral=True
            )
            return

        credits = await self._get_credits(user)
        glb = await self.config.all()
        base_upgrade_cost = int(glb.get("upgrade_base_cost", 50000)) * current_lvl
        cost = self._economy_scaled_cost(base_upgrade_cost, credits)
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
        self._add_economy_pricing_note(embed, base_upgrade_cost, cost, credits)
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

        base_cost = int(glb.get("career_convert_cost", 250000))
        credits = await self._get_credits(ctx.author)
        cost = self._economy_scaled_cost(base_cost, credits)
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
        self._add_economy_pricing_note(embed, base_cost, cost, credits)

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
        glb = await self.config.all()
        base_cost = int(glb.get("career_convert_cost", 250000))
        cost = self._economy_scaled_cost(base_cost, credits)
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
        self._add_economy_pricing_note(embed, base_cost, cost, credits)
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
        vehicles = data.get("vehicles", [])
        max_veh = self._max_vehicles_for_data(data)
        if len(vehicles) >= max_veh:
            await ctx.send("You are at maximum vehicle capacity. Upgrade your station to buy more vehicles.")
            return

        view = VehicleShopView(self, ctx.channel, ctx.author, ctx.guild, data=data)
        embed = self._build_vehicle_shop_embed(data)
        message = await ctx.send(embed=embed, view=view)
        view.message = message

    @fsc_group.command(name="equipment")
    async def fsc_equipment(self, ctx: commands.Context):
        """Open the equipment shop."""
        if not await self._ensure_started(ctx):
            return

        data = await self.config.user(ctx.author).all()
        embed = self._build_equipment_shop_embed(data)
        view = EquipmentShopView(self, ctx.channel, ctx.author, ctx.guild, data=data)
        message = await ctx.send(embed=embed, view=view)
        view.message = message

    @fsc_group.command(name="training")
    async def fsc_training(self, ctx: commands.Context):
        """Open the training desk."""
        if not await self._ensure_started(ctx):
            return

        data = await self.config.user(ctx.author).all()
        if not self._feature_available(data, "training"):
            await ctx.send(self._feature_locked_text("training"))
            return
        embed = self._build_training_embed(data)
        view = TrainingView(self, ctx.channel, ctx.author, ctx.guild, data=data)
        message = await ctx.send(embed=embed, view=view)
        view.message = message

    @fsc_group.command(name="expansions")
    async def fsc_expansions(self, ctx: commands.Context):
        """Open the station expansion desk."""
        if not await self._ensure_started(ctx):
            return

        data = await self.config.user(ctx.author).all()
        embed = self._build_expansion_embed(data)
        view = ExpansionView(self, ctx.channel, ctx.author, ctx.guild, data=data)
        message = await ctx.send(embed=embed, view=view)
        view.message = message

    @fsc_group.command(name="maintenance")
    async def fsc_maintenance(self, ctx: commands.Context):
        """Open the maintenance bay."""
        if not await self._ensure_started(ctx):
            return

        data = await self.config.user(ctx.author).all()
        if not self._feature_available(data, "maintenance"):
            await ctx.send(self._feature_locked_text("maintenance"))
            return
        embed = self._build_maintenance_embed(data)
        view = MaintenanceView(self, ctx.channel, ctx.author, ctx.guild)
        message = await ctx.send(embed=embed, view=view)
        view.message = message

    @fsc_group.command(name="mission")
    async def fsc_mission(self, ctx: commands.Context):
        """Start a new incident if none is active."""
        if not await self._ensure_started(ctx):
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        active = data.get("active_mission", {}) or {}
        if active:
            if await self._run_due_mission_action(ctx.channel, ctx.author):
                return
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
        mission["missing_required_training"] = self._missing_required_training_ids(incident, data)
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
        message = await ctx.send(embed=embed, view=view)
        view.message = message

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
            message = await channel.send(embed=embed, view=view)
            view.message = message
        except Exception:
            try:
                message = await user.send(embed=embed, view=view)
                view.message = message
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

        vehicles = self._available_vehicles(await self._get_user_vehicles(user))
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
        data = await user_conf.all()
        reputation = int(data.get("reputation", 0))
        reputation_delta = self._reputation_delta_for_outcome("skip")
        await user_conf.reputation.set(reputation + reputation_delta)
        await user_conf.active_mission.set({})
        await interaction.response.send_message(
            f"Incident cancelled. Reputation change: {reputation_delta:+}.",
            ephemeral=False,
        )

    def _incident_from_active_mission(self, mission: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": mission.get("id", "takeover_incident"),
            "name": mission.get("title", "Transferred Incident"),
            "required_staff": int(mission.get("required_staff", 4)),
            "base_credits": int(mission.get("base_credits", 1000)),
            "hint": mission.get("hint", "Transferred dispatch from another station member."),
            "detail": mission.get("detail", "Dispatch transfers the active incident notes."),
            "dispatch_narrative": mission.get("dispatch_narrative", ""),
            "success_narrative": mission.get("success_narrative", ""),
            "partial_narrative": mission.get("partial_narrative", ""),
            "failure_narrative": mission.get("failure_narrative", ""),
            "image": mission.get("image"),
            "required_vehicles": mission.get("required_vehicles", []),
            "required_equipment": mission.get("required_equipment", []),
            "required_expansions": mission.get("required_expansions", []),
            "base_xp": int(mission.get("base_xp", self._balance_int("xp_per_mission_base", 50))),
            "tier": int(mission.get("tier", 1)),
            "recommended_level": int(mission.get("recommended_level", 1)),
            "capabilities": mission.get("capabilities", {}),
        }

    async def handle_takeover_incident(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        guild: discord.Guild | None,
        original_user: discord.abc.User,
        new_user: discord.abc.User,
    ) -> None:
        if new_user.id == original_user.id:
            await interaction.response.send_message(
                "The original station already missed this decision window. Another member must take over.",
                ephemeral=True,
            )
            return

        original_conf = self.config.user(original_user)
        original_data = await original_conf.all()
        original_mission = original_data.get("active_mission", {}) or {}
        if not self._mission_is_stage(original_mission, self.STAGE_STAFF_TURNOUT):
            await interaction.response.send_message("This incident is no longer available for takeover.", ephemeral=True)
            return

        new_conf = self.config.user(new_user)
        new_data = await new_conf.all()
        if not new_data.get("started", False):
            await interaction.response.send_message("Create a station before taking over incidents.", ephemeral=True)
            return
        if new_data.get("active_mission"):
            await interaction.response.send_message("You already have an active incident.", ephemeral=True)
            return
        if int(new_data.get("staff_total", 0)) <= 0:
            await interaction.response.send_message("You need station staff before taking over incidents.", ephemeral=True)
            return

        incident = self._incident_from_active_mission(original_mission)
        mission = self._new_mission_state(
            incident,
            channel_id=getattr(channel, "id", 0),
            guild_id=guild.id if guild else None,
        )
        mission["dispatch_narrative"] = self._takeover_dispatch_narrative()
        mission["missing_required_vehicles"] = self._missing_required_vehicle_ids(incident, new_data.get("vehicles", []))
        mission["missing_required_equipment"] = self._missing_required_equipment_ids(
            incident,
            new_data.get("equipment", []),
        )
        mission["missing_required_training"] = self._missing_required_training_ids(incident, new_data)
        mission["readiness_score"] = self._readiness_score(incident, new_data)

        await original_conf.active_mission.set({})
        await new_conf.active_mission.set(mission)

        embed = discord.Embed(
            title=f"Transferred incident: {incident['name']}",
            description=mission["dispatch_narrative"],
            color=discord.Color.red(),
        )
        self._apply_mission_image(embed, mission)
        self._add_mission_requirement_fields(embed, mission)
        embed.add_field(name="Initial report", value=incident["hint"], inline=False)
        embed.set_footer(text="Choose how to alert your crew below.")

        view = AlertChoiceView(self, channel, new_user)
        await interaction.response.edit_message(content=None, embed=embed, view=view)

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
        available_ids = {
            str(vehicle.get("id"))
            for vehicle in self._available_vehicles(data.get("vehicles", []))
            if isinstance(vehicle, dict)
        }
        if any(vehicle_id not in available_ids for vehicle_id in values):
            await interaction.response.send_message(
                "One or more selected vehicles are out of service or no longer available.",
                ephemeral=True,
            )
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

    async def _send_travel_update(
        self,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        *,
        sleep_after: bool = True,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if not self._mission_is_stage(mission, self.STAGE_TRAVEL):
            return
        glb = await self.config.all()
        vehicles = await self._get_user_vehicles(user)
        selected_ids = mission.get("selected_vehicle_ids", [])
        if not isinstance(selected_ids, list):
            selected_ids = []
        selected = [
            vehicle
            for vehicle in vehicles
            if isinstance(vehicle, dict) and vehicle.get("id") in selected_ids
        ]
        backup_chance = float(glb.get("scene_backup_chance", 0.25))
        if self._should_request_scene_backup(mission, data, selected, backup_chance):
            min_minutes = float(glb.get("scene_backup_window_minutes_min", 0.5))
            max_minutes = float(glb.get("scene_backup_window_minutes_max", 1.0))
            minutes = random.uniform(min_minutes, max_minutes)
            mission["backup_window_opened"] = True
            mission["backup_required_vehicle_ids"] = self._scene_backup_vehicle_requirements(mission, selected)
            mission["backup_vehicle_ids"] = []
            mission["mutual_aid_requests"] = []
            self._set_mission_stage(mission, self.STAGE_SCENE_BACKUP)
            self._set_mission_due(mission, self.ACTION_RESOLVE_BACKUP_WINDOW, minutes)
            await user_conf.active_mission.set(mission)

            title = mission.get("title", "Incident")
            detail = mission.get("detail", "Units report additional information en route.")
            backup_text = self._vehicle_requirement_display_text(mission.get("backup_required_vehicle_ids"))
            backup_candidates = self._backup_candidate_vehicles(self._available_vehicles(vehicles), mission)

            embed = discord.Embed(
                title=f"On-scene update - {title}",
                description=(
                    f"{detail}\n\n"
                    "The officer on scene asks dispatch for additional resources before committing "
                    "to the final incident plan."
                ),
                color=discord.Color.orange(),
            )
            self._apply_mission_image(embed, mission)
            if backup_text:
                embed.add_field(name="Requested backup", value=backup_text, inline=False)
            embed.add_field(
                name="Decision window",
                value=f"Backup can be assigned {self._make_relative_text(minutes)}.",
                inline=True,
            )
            embed.add_field(
                name="Local backup available",
                value=str(len(backup_candidates)),
                inline=True,
            )
            embed.set_footer(
                text="Future mutual aid will let other members receive and run this backup as their own dispatch."
            )
            view = SceneBackupView(self, channel, user, backup_candidates)
            try:
                await channel.send(embed=embed, view=view)
            except Exception:
                try:
                    await user.send(embed=embed, view=view)
                except Exception:
                    pass

            if sleep_after:
                await asyncio.sleep(int(minutes * 60))
                await self._resolve_backup_window(channel, user)
            return

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

        if sleep_after:
            await asyncio.sleep(int(minutes * 60))
            await self._resolve_incident(channel, user)

    async def handle_backup_vehicle_selection(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        values: List[str],
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if not self._mission_is_stage(mission, self.STAGE_SCENE_BACKUP):
            await interaction.response.send_message("This incident is not waiting for scene backup.", ephemeral=True)
            return

        if not values:
            await interaction.response.send_message("No backup vehicles selected.", ephemeral=True)
            return

        available = self._backup_candidate_vehicles(
            self._available_vehicles(await self._get_user_vehicles(user)),
            mission,
        )
        available_ids = {str(vehicle["id"]) for vehicle in available}
        selected_ids = [int(vehicle_id) for vehicle_id in values if vehicle_id in available_ids]
        if not selected_ids:
            await interaction.response.send_message("Those vehicles are no longer available for backup.", ephemeral=True)
            return

        glb = await self.config.all()
        min_minutes = float(glb.get("scene_backup_travel_minutes_min", 0.5))
        max_minutes = float(glb.get("scene_backup_travel_minutes_max", 1.5))
        minutes = random.uniform(min_minutes, max_minutes)
        mission["backup_vehicle_ids"] = selected_ids
        mission["backup_status"] = "en_route"
        self._set_mission_stage(mission, self.STAGE_SCENE_WORK)
        self._set_mission_due(mission, self.ACTION_RESOLVE_INCIDENT, minutes)
        await user_conf.active_mission.set(mission)

        embed = discord.Embed(
            title="Backup assigned",
            description=(
                "Additional units are responding to the scene. Dispatch keeps the channel open while "
                "the first crew works defensively and waits for the extra apparatus."
            ),
            color=discord.Color.blue(),
        )
        embed.add_field(name="Backup vehicles", value=str(len(selected_ids)), inline=True)
        embed.add_field(name="Backup ETA", value=self._make_relative_text(minutes), inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=False)

        await asyncio.sleep(int(minutes * 60))
        await self._resolve_incident(channel, user)

    async def handle_continue_without_scene_backup(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
    ):
        await interaction.response.send_message(
            "Command continues with the resources already on scene.",
            ephemeral=False,
        )
        await self._resolve_backup_window(channel, user)

    async def _resolve_backup_window(
        self,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        *,
        sleep_after: bool = True,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if not self._mission_is_stage(mission, self.STAGE_SCENE_BACKUP):
            return

        glb = await self.config.all()
        min_minutes = float(glb.get("scene_work_minutes_min", 0.5))
        max_minutes = float(glb.get("scene_work_minutes_max", 1.5))
        minutes = random.uniform(min_minutes, max_minutes)
        mission["backup_status"] = "missed"
        self._set_mission_stage(mission, self.STAGE_SCENE_WORK)
        self._set_mission_due(mission, self.ACTION_RESOLVE_INCIDENT, minutes)
        await user_conf.active_mission.set(mission)

        embed = discord.Embed(
            title="Backup window closed",
            description=(
                "No additional units were assigned before the window closed. The incident commander "
                "continues with the original dispatch and accepts the operational risk."
            ),
            color=discord.Color.orange(),
        )
        embed.add_field(name="Incident result", value=f"Expected {self._make_relative_text(minutes)}.", inline=False)
        try:
            await channel.send(embed=embed)
        except Exception:
            try:
                await user.send(embed=embed)
            except Exception:
                pass

        if sleep_after:
            await asyncio.sleep(int(minutes * 60))
            await self._resolve_incident(channel, user)

    async def _resolve_incident(self, channel: discord.abc.Messageable, user: discord.abc.User):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if self._mission_stage(mission) not in {
            self.STAGE_TRAVEL,
            self.STAGE_VEHICLE_SELECT,
            self.STAGE_SCENE_BACKUP,
            self.STAGE_SCENE_WORK,
        }:
            return
        self._clear_mission_due(mission)

        required = int(mission.get("required_staff", 0))
        arrived = int(mission.get("turnout_total_arrived", 0))
        vehicles = await self._get_user_vehicles(user)
        selected_ids = mission.get("selected_vehicle_ids", [])
        backup_ids = mission.get("backup_vehicle_ids", [])
        if not isinstance(selected_ids, list):
            selected_ids = []
        if not isinstance(backup_ids, list):
            backup_ids = []
        dispatched_ids = selected_ids + backup_ids
        selected = [v for v in vehicles if v["id"] in dispatched_ids]

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
        reputation_delta = self._reputation_delta_for_outcome(outcome_key)
        new_reputation = int(data.get("reputation", 0)) + reputation_delta
        await user_conf.reputation.set(new_reputation)
        updated_vehicles = self._apply_vehicle_wear(vehicles, dispatched_ids, outcome_key, data=data)
        await user_conf.vehicles.set(updated_vehicles)
        await self._give(user, reward)
        total_credits = await self._get_credits(user)
        await user_conf.active_mission.set({})
        repair_estimate = self._fleet_maintenance_cost(updated_vehicles)
        out_of_service = self._out_of_service_vehicle_text(updated_vehicles)

        embed = discord.Embed(
            title=f"Incident result – {mission.get('title', 'Unknown')}",
            color=discord.Color.green() if success_score >= 1.0 else discord.Color.orange(),
        )
        self._apply_mission_result_image(embed, mission, outcome_key)
        embed.add_field(name="Required staff", value=str(required), inline=True)
        embed.add_field(name="Arrived staff", value=str(arrived), inline=True)
        embed.add_field(name="Vehicles dispatched", value=f"{len(selected)} (cap {total_capacity})", inline=True)
        if backup_ids:
            embed.add_field(name="Backup vehicles", value=str(len(backup_ids)), inline=True)
        elif mission.get("backup_status") == "missed":
            embed.add_field(name="Backup", value="Requested but not assigned in time.", inline=True)
        embed.add_field(name="Outcome", value=outcome, inline=False)
        embed.add_field(name="Narrative", value=narrative, inline=False)
        embed.add_field(name="Reward", value=f"{reward:,} credits", inline=True)
        embed.add_field(name="XP gained", value=f"{xp_result['earned']:,} XP", inline=True)
        embed.add_field(name="Reputation", value=f"{new_reputation} ({reputation_delta:+})", inline=True)
        embed.add_field(name="Total credits", value=f"{total_credits:,}", inline=True)
        embed.add_field(name="Repair estimate", value=f"{repair_estimate:,} credits", inline=True)
        if out_of_service:
            embed.add_field(name="Out of service", value=out_of_service, inline=False)
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

    async def _repair_fleet(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        guild: discord.Guild | None = None,
        *,
        edit_message: bool = False,
    ) -> None:
        user_conf = self.config.user(user)
        data = await user_conf.all()
        vehicles = data.get("vehicles", [])
        cost = self._fleet_maintenance_cost(vehicles)
        if cost <= 0:
            embed = self._build_maintenance_embed(data)
            embed.add_field(name="Repair skipped", value="No vehicle repairs are needed.", inline=False)
            view = MaintenanceView(self, channel, user, guild or interaction.guild)
            if edit_message:
                await interaction.response.edit_message(content=None, embed=embed, view=view)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=False)
            return

        credits = await self._get_credits(user)
        if credits < cost or not await self._spend(user, cost):
            embed = self._build_maintenance_embed(data)
            embed.add_field(
                name="Repair failed",
                value=f"Fleet repair costs {cost:,} credits, but you only have {credits:,}.",
                inline=False,
            )
            view = MaintenanceView(self, channel, user, guild or interaction.guild)
            if edit_message:
                await interaction.response.edit_message(content=None, embed=embed, view=view)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=False)
            return

        repaired = []
        vehicle_list = vehicles if isinstance(vehicles, list) else []
        for vehicle in vehicle_list:
            if not isinstance(vehicle, dict):
                continue
            current = dict(vehicle)
            current["condition"] = 100
            current.pop("out_of_service_until", None)
            repaired.append(current)

        await user_conf.vehicles.set(repaired)
        fresh_data = await user_conf.all()
        embed = self._build_maintenance_embed(fresh_data)
        embed.add_field(name="Fleet repaired", value=f"Spent {cost:,} credits.", inline=False)
        view = MaintenanceView(self, channel, user, guild or interaction.guild)
        if edit_message:
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            return
        await interaction.response.send_message(embed=embed, ephemeral=False)

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
        if not self._vehicle_is_unlocked(vdef, command_level, data):
            required_level = int(vdef.get("unlock_level", 1))
            missing_expansions = self._missing_required_expansion_ids(vdef, data)
            if missing_expansions:
                locked_text = (
                    f"{vdef['name']} requires expansion: "
                    f"{self._expansion_requirement_display_text(missing_expansions)}."
                )
            else:
                locked_text = f"{vdef['name']} requires command level {required_level}."
            if edit_message:
                embed = self._build_vehicle_shop_embed(data)
                embed.add_field(
                    name="Purchase locked",
                    value=locked_text,
                    inline=False,
                )
                view = VehicleShopView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(f"Purchase locked: {locked_text}", ephemeral=True)
            return

        trained = self._training_inventory_set(data.get("trainings", []))
        required_training = vdef.get("required_training", [])
        if isinstance(required_training, list):
            missing_training = [str(training) for training in required_training if str(training) not in trained]
        else:
            missing_training = []
        if missing_training:
            training_text = self._training_display_text(missing_training) or ", ".join(missing_training)
            if edit_message:
                embed = self._build_vehicle_shop_embed(data)
                embed.add_field(
                    name="Purchase locked",
                    value=f"{vdef['name']} requires training: {training_text}.",
                    inline=False,
                )
                view = VehicleShopView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                f"Purchase locked: training required: {training_text}.", ephemeral=True
            )
            return

        base_price = int(vdef["price"])

        # Check capacity again
        vehicles = data.get("vehicles", [])
        max_veh = self._max_vehicles_for_data(data)
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
        price = self._economy_scaled_cost(base_price, credits)
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
            "condition": 100,
        }
        vehicles.append(new_vehicle)

        await user_conf.vehicles.set(vehicles)
        await user_conf.next_vehicle_id.set(next_id + 1)

        embed = discord.Embed(
            title="Vehicle purchased",
            description=f"Purchased **{vdef['name']}** for **{price:,}** credits.",
            color=discord.Color.green(),
        )
        self._add_economy_pricing_note(embed, base_price, price, credits)
        embed.add_field(name="Crew capacity", value=str(vdef["crew_capacity"]), inline=True)
        embed.add_field(name="Condition", value="100%", inline=True)
        self._apply_vehicle_image(embed, vdef)
        if edit_message:
            embed.add_field(name="Vehicle capacity", value=f"{len(vehicles)} / {max_veh}", inline=True)
            fresh_data = await user_conf.all()
            if len(vehicles) >= max_veh:
                view = FscDashboardView(self, user, channel, guild or interaction.guild, data=fresh_data)
            else:
                view = VehicleShopView(self, channel, user, guild or interaction.guild, data=fresh_data)
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            return
        await interaction.response.send_message(embed=embed, ephemeral=False)

    async def _confirm_expansion_purchase(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        expansion_id: str,
        *,
        edit_message: bool = False,
        guild: discord.Guild | None = None,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()

        if expansion_id not in self.EXPANSION_CATALOG:
            if edit_message:
                embed = self._build_expansion_embed(data)
                embed.add_field(name="Expansion failed", value="Unknown expansion type.", inline=False)
                view = ExpansionView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message("Unknown expansion type.", ephemeral=True)
            return

        expansion = self.EXPANSION_CATALOG[expansion_id]
        owned = self._expansion_inventory_set(data.get("expansions", []))
        if expansion_id in owned:
            if edit_message:
                embed = self._build_expansion_embed(data)
                embed.add_field(name="Expansion already built", value="This expansion is already active.", inline=False)
                view = ExpansionView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message("This expansion is already active.", ephemeral=True)
            return

        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        if not self._expansion_is_unlocked(expansion, command_level):
            required_level = int(expansion.get("unlock_level", 1))
            if edit_message:
                embed = self._build_expansion_embed(data)
                embed.add_field(
                    name="Expansion not available yet",
                    value=f"{expansion['name']} unlocks at command level {required_level}.",
                    inline=False,
                )
                view = ExpansionView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                f"Expansion not available yet: command level {required_level} required.", ephemeral=True
            )
            return

        credits = await self._get_credits(user)
        base_price = int(expansion.get("price", 0))
        price = self._economy_scaled_cost(base_price, credits)
        if credits < price or not await self._spend(user, price):
            if edit_message:
                embed = self._build_expansion_embed(data)
                embed.add_field(
                    name="Expansion failed",
                    value="You do not have enough credits to build this expansion.",
                    inline=False,
                )
                view = ExpansionView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                "You do not have enough credits to build this expansion.",
                ephemeral=True,
            )
            return

        updated_expansions = sorted(owned | {expansion_id})
        await user_conf.expansions.set(updated_expansions)

        updated_data = dict(data)
        updated_data["expansions"] = updated_expansions
        embed = discord.Embed(
            title="Expansion built",
            description=f"Built **{expansion['name']}** for **{price:,}** credits.",
            color=discord.Color.green(),
        )
        effects = expansion.get("effects", {})
        if isinstance(effects, dict) and effects:
            effect_text = ", ".join(f"{name}: {value}" for name, value in effects.items())
            embed.add_field(name="Effects", value=effect_text, inline=False)
        self._add_economy_pricing_note(embed, base_price, price, credits)
        embed.add_field(
            name="Vehicle capacity",
            value=f"{len(updated_data.get('vehicles', []))} / {self._max_vehicles_for_data(updated_data)}",
            inline=True,
        )
        if edit_message:
            view = ExpansionView(self, channel, user, guild or interaction.guild, data=updated_data)
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            return
        await interaction.response.send_message(embed=embed, ephemeral=False)

    async def _confirm_training_purchase(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        training_id: str,
        *,
        edit_message: bool = False,
        guild: discord.Guild | None = None,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()

        if training_id not in self.TRAINING_CATALOG:
            if edit_message:
                embed = self._build_training_embed(data)
                embed.add_field(name="Training failed", value="Unknown training type.", inline=False)
                view = TrainingView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message("Unknown training type.", ephemeral=True)
            return

        training = self.TRAINING_CATALOG[training_id]
        trained = self._training_inventory_set(data.get("trainings", []))
        if training_id in trained:
            if edit_message:
                embed = self._build_training_embed(data)
                embed.add_field(name="Training complete", value="This training is already completed.", inline=False)
                view = TrainingView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message("This training is already completed.", ephemeral=True)
            return

        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self._command_level_for_xp(xp)))
        if not self._training_is_unlocked(training, command_level):
            required_level = int(training.get("unlock_level", 1))
            if edit_message:
                embed = self._build_training_embed(data)
                embed.add_field(
                    name="Training locked",
                    value=f"{training['name']} requires command level {required_level}.",
                    inline=False,
                )
                view = TrainingView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                f"Training locked: command level {required_level} required.", ephemeral=True
            )
            return

        credits = await self._get_credits(user)
        base_price = int(training.get("price", 0))
        price = self._economy_scaled_cost(base_price, credits)
        if credits < price or not await self._spend(user, price):
            if edit_message:
                embed = self._build_training_embed(data)
                embed.add_field(
                    name="Training failed",
                    value="You do not have enough credits to complete this training.",
                    inline=False,
                )
                view = TrainingView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                "You do not have enough credits to complete this training.",
                ephemeral=True,
            )
            return

        updated_trainings = sorted(trained | {training_id})
        await user_conf.trainings.set(updated_trainings)

        updated_data = dict(data)
        updated_data["trainings"] = updated_trainings
        embed = discord.Embed(
            title="Training completed",
            description=f"Completed station certification **{training['name']}** for **{price:,}** credits.",
            color=discord.Color.green(),
        )
        embed.add_field(
            name="Training scope",
            value="Permanent station certification for current and future staff.",
            inline=False,
        )
        duration = int(training.get("duration_hours", 0))
        if duration > 0:
            embed.add_field(name="Training duration", value=f"{duration} hour(s)", inline=True)
        self._add_economy_pricing_note(embed, base_price, price, credits)
        if edit_message:
            view = TrainingView(self, channel, user, guild or interaction.guild, data=updated_data)
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
        if not self._equipment_is_unlocked(equipment, command_level, data):
            required_level = int(equipment.get("unlock_level", 1))
            missing_expansions = self._missing_required_expansion_ids(equipment, data)
            if missing_expansions:
                locked_text = (
                    f"{equipment['name']} requires expansion: "
                    f"{self._expansion_requirement_display_text(missing_expansions)}."
                )
            else:
                locked_text = f"{equipment['name']} requires command level {required_level}."
            if edit_message:
                embed = self._build_equipment_shop_embed(data)
                embed.add_field(
                    name="Purchase locked",
                    value=locked_text,
                    inline=False,
                )
                view = EquipmentShopView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(f"Purchase locked: {locked_text}", ephemeral=True)
            return

        trained = self._training_inventory_set(data.get("trainings", []))
        required_training = equipment.get("required_training", [])
        if isinstance(required_training, list):
            missing_training = [str(training) for training in required_training if str(training) not in trained]
        else:
            missing_training = []
        if missing_training:
            training_text = self._training_display_text(missing_training) or ", ".join(missing_training)
            if edit_message:
                embed = self._build_equipment_shop_embed(data)
                embed.add_field(
                    name="Purchase locked",
                    value=f"{equipment['name']} requires training: {training_text}.",
                    inline=False,
                )
                view = EquipmentShopView(self, channel, user, guild or interaction.guild, data=data)
                await interaction.response.edit_message(content=None, embed=embed, view=view)
                return
            await interaction.response.send_message(
                f"Purchase locked: training required: {training_text}.", ephemeral=True
            )
            return

        credits = await self._get_credits(user)
        base_price = int(equipment.get("price", 0))
        price = self._economy_scaled_cost(base_price, credits)
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
        self._add_economy_pricing_note(embed, base_price, price, credits)
        self._apply_equipment_image(embed, equipment)
        if edit_message:
            view = EquipmentShopView(self, channel, user, guild or interaction.guild, data=updated_data)
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            return
        await interaction.response.send_message(embed=embed, ephemeral=False)


class FscTimedView(discord.ui.View):
    timeout_notice = "This menu timed out. Open `[p]fsc` again to continue."

    def __init__(self, *, timeout: float | None = 180):
        super().__init__(timeout=timeout)
        self.message = None

    def _disable_children(self) -> None:
        for child in self.children:
            child.disabled = True
            if isinstance(child, discord.ui.Button):
                child.style = discord.ButtonStyle.secondary

    async def _edit_timeout_message(self, *, content: str | None = None, view: discord.ui.View | None = None) -> None:
        if self.message is None:
            return
        self._disable_children()
        try:
            await self.message.edit(content=content or self.timeout_notice, view=view or self)
        except Exception:
            pass

    async def on_timeout(self) -> None:
        await self._edit_timeout_message()

    def stop(self) -> None:
        try:
            super().stop()
        except AttributeError:
            pass


class FscStartView(FscTimedView):
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
        data = await self.cog.config.user(self.user).all()
        view = FscDashboardView(self.cog, self.user, interaction.channel, interaction.guild, data=data)
        if created:
            await interaction.response.edit_message(embed=embed, view=view)
        else:
            await interaction.response.send_message("You already started.", ephemeral=True)
        self.stop()


class FscDashboardView(FscTimedView):
    FEATURE_BUTTONS = {
        "career_conversion": {"career"},
        "expansions": {"expansions"},
        "maintenance": {"maintenance"},
        "training": {"training"},
    }
    ACTION_FEATURES = {
        "career": "career_conversion",
        "expansions": "expansions",
        "maintenance": "maintenance",
        "training": "training",
    }
    ACTION_LABELS = {
        "equipment": "Buy equipment",
        "shop": "Buy vehicle",
        "expansions": "Build expansions",
        "career": "Career station",
        "commands": "Command help",
        "recruit": "Hire staff",
        "maintenance": "Maintenance bay",
        "mission": "Start mission",
        "refresh": "Refresh dashboard",
        "station": "Overview",
        "training": "Train staff",
        "upgrade": "Upgrade station",
    }
    DASHBOARD_CATEGORIES = {
        "Incidents": ("commands", "mission"),
        "Staff": ("recruit", "training"),
        "Station": ("expansions", "career", "station", "refresh", "upgrade"),
        "Vehicle": ("equipment", "maintenance", "shop"),
    }
    DASHBOARD_CATEGORY_LABELS = set(DASHBOARD_CATEGORIES)
    DASHBOARD_ACTION_CALLBACKS = set(ACTION_LABELS)
    DASHBOARD_ACTION_LABELS = set(ACTION_LABELS.values()) | {
        "Build expansions",
        "Career station",
        "Command help",
        "Equipment shop",
        "Hire staff",
        "Maintenance bay",
        "Refresh",
        "Start mission",
        "Station overview",
        "Training desk",
        "Upgrade station",
        "Vehicle shop",
    }

    def __init__(
        self,
        cog: FireStationCommand,
        user: discord.abc.User,
        channel: discord.abc.Messageable,
        guild: discord.Guild | None,
        data: Dict[str, Any] | None = None,
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.user = user
        self.channel = channel
        self.guild = guild
        self.data = data or {}
        if data:
            self._remove_unavailable_buttons(data)
        self._configure_category_buttons(data or {})

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    def _button_callback_name(self, child: discord.ui.Item) -> str:
        callback = getattr(child, "callback", None)
        return getattr(callback, "__name__", "")

    def _remove_child_item(self, child: discord.ui.Item) -> None:
        try:
            self.remove_item(child)
        except AttributeError:
            if child in self.children:
                self.children.remove(child)

    def _remove_buttons_by_callback(self, callback_names: set[str]) -> None:
        for child in list(self.children):
            if self._button_callback_name(child) in callback_names:
                self._remove_child_item(child)

    def _remove_buttons_by_label(self, labels: set[str]) -> None:
        for child in list(self.children):
            label = getattr(child, "label", None)
            if isinstance(label, str) and label in labels:
                self._remove_child_item(child)

    def _remove_unavailable_buttons(self, data: Dict[str, Any]) -> None:
        for feature, callbacks in self.FEATURE_BUTTONS.items():
            if not self.cog._feature_available(data, feature):
                self._remove_buttons_by_callback(callbacks)

    def _action_available(self, action: str, data: Dict[str, Any]) -> bool:
        feature = self.ACTION_FEATURES.get(action)
        return feature is None or self.cog._feature_available(data, feature)

    def _category_actions(self, category: str, data: Dict[str, Any]) -> List[str]:
        actions = [
            action
            for action in self.DASHBOARD_CATEGORIES.get(category, ())
            if self._action_available(action, data)
        ]
        return sorted(actions, key=lambda action: self.ACTION_LABELS[action].lower())

    def _available_categories(self, data: Dict[str, Any]) -> List[str]:
        return [
            category
            for category in sorted(self.DASHBOARD_CATEGORIES)
            if self._category_actions(category, data)
        ]

    def _configure_category_buttons(self, data: Dict[str, Any]) -> None:
        self._remove_buttons_by_callback(self.DASHBOARD_ACTION_CALLBACKS)
        self._remove_buttons_by_label(self.DASHBOARD_ACTION_LABELS | self.DASHBOARD_CATEGORY_LABELS)
        for category in self._available_categories(data):
            self.add_item(DashboardCategoryButton(category))

    def _dashboard_view(self, data: Dict[str, Any] | None = None) -> "FscDashboardView":
        return FscDashboardView(self.cog, self.user, self.channel, self.guild, data=data or self.data)

    async def open_category(self, interaction: discord.Interaction, category: str) -> None:
        data = await self.cog.config.user(self.user).all()
        embed = await self.cog._build_dashboard_embed(self.user)
        actions = self._category_actions(category, data)
        labels = [self.ACTION_LABELS[action] for action in actions]
        embed.add_field(
            name=f"{category} menu",
            value=", ".join(labels) if labels else "No available actions in this category yet.",
            inline=False,
        )
        view = FscDashboardCategoryView(
            self.cog,
            self.user,
            interaction.channel or self.channel,
            interaction.guild or self.guild,
            category,
            data=data,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.primary)
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        embed = await self.cog._build_dashboard_embed(self.user)
        await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view(data))

    @discord.ui.button(label="Station overview", style=discord.ButtonStyle.secondary)
    async def station(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        if not data["started"]:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action required", value="Create a station first.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=FscStartView(self.cog, self.user))
            return
        embed = self.cog._build_station_overview_embed(data)
        await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())

    @discord.ui.button(label="Hire staff", style=discord.ButtonStyle.success)
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

    @discord.ui.button(label="Vehicle shop", style=discord.ButtonStyle.secondary)
    async def shop(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        if not data["started"]:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action required", value="Create a station first.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=FscStartView(self.cog, self.user))
            return
        vehicles = data.get("vehicles", [])
        max_veh = self.cog._max_vehicles_for_data(data)
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

    @discord.ui.button(label="Equipment shop", style=discord.ButtonStyle.secondary)
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

    @discord.ui.button(label="Training desk", style=discord.ButtonStyle.secondary)
    async def training(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        if not data["started"]:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action required", value="Create a station first.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=FscStartView(self.cog, self.user))
            return
        if not self.cog._feature_available(data, "training"):
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Training locked", value=self.cog._feature_locked_text("training"), inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view(data))
            return

        embed = self.cog._build_training_embed(data)
        view = TrainingView(
            self.cog,
            interaction.channel or self.channel,
            self.user,
            interaction.guild or self.guild,
            data=data,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    @discord.ui.button(label="Build expansions", style=discord.ButtonStyle.secondary)
    async def expansions(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        if not data["started"]:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action required", value="Create a station first.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=FscStartView(self.cog, self.user))
            return
        if not self.cog._feature_available(data, "expansions"):
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Expansions locked", value=self.cog._feature_locked_text("expansions"), inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view(data))
            return

        embed = self.cog._build_expansion_embed(data)
        view = ExpansionView(
            self.cog,
            interaction.channel or self.channel,
            self.user,
            interaction.guild or self.guild,
            data=data,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    @discord.ui.button(label="Maintenance bay", style=discord.ButtonStyle.secondary)
    async def maintenance(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        if not data["started"]:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action required", value="Create a station first.", inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=FscStartView(self.cog, self.user))
            return
        if not self.cog._feature_available(data, "maintenance"):
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Maintenance locked", value=self.cog._feature_locked_text("maintenance"), inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view(data))
            return

        embed = self.cog._build_maintenance_embed(data)
        view = MaintenanceView(
            self.cog,
            interaction.channel or self.channel,
            self.user,
            interaction.guild or self.guild,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    @discord.ui.button(label="Upgrade station", style=discord.ButtonStyle.primary)
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

        new_lvl = lvl + 1
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self.cog._command_level_for_xp(xp)))
        if command_level < new_lvl:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(
                name="Upgrade not available yet",
                value=(
                    f"Station level {new_lvl} unlocks at command level {new_lvl}. "
                    f"Current progress: {self.cog._xp_progress_text(xp, command_level)}."
                ),
                inline=False,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())
            return

        base = int(glb.get("upgrade_base_cost", 50000))
        base_cost = base * lvl
        credits = await self.cog._get_credits(self.user)
        cost = self.cog._economy_scaled_cost(base_cost, credits)
        if credits < cost:
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(
                name="Station upgrade",
                value=f"Level {lvl} to {lvl + 1} costs {cost:,} credits, but you only have {credits:,}.",
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
        self.cog._add_economy_pricing_note(embed, base_cost, cost, credits)
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

    @discord.ui.button(label="Career station", style=discord.ButtonStyle.secondary)
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
        if not self.cog._feature_available(data, "career_conversion"):
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Career station locked", value=self.cog._feature_locked_text("career_conversion"), inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view(data))
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
        base_cost = int(glb.get("career_convert_cost", 250000))
        credits = await self.cog._get_credits(self.user)
        cost = self.cog._economy_scaled_cost(base_cost, credits)
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
        self.cog._add_economy_pricing_note(embed, base_cost, cost, credits)
        view = ConfirmCareerView(
            self.cog,
            self.user,
            cost,
            channel=interaction.channel or self.channel,
            guild=interaction.guild or self.guild,
            edit_message=True,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    @discord.ui.button(label="Start mission", style=discord.ButtonStyle.danger)
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
            if await self.cog._run_due_mission_action(channel, self.user):
                refreshed = await self.cog.config.user(self.user).all()
                active = refreshed.get("active_mission", {}) or {}
                if not active:
                    embed = await self.cog._build_dashboard_embed(self.user)
                    view = FscDashboardView(self.cog, self.user, channel, interaction.guild or self.guild)
                    await interaction.response.edit_message(content=None, embed=embed, view=view)
                    return
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
        mission["missing_required_training"] = self.cog._missing_required_training_ids(incident, data)
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

    @discord.ui.button(label="Command help", style=discord.ButtonStyle.secondary)
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
        embed.add_field(name="Training", value="`[p]fsc training`", inline=False)
        embed.add_field(name="Expansions", value="`[p]fsc expansions`", inline=False)
        await interaction.response.edit_message(content=None, embed=embed, view=self._dashboard_view())


class DashboardCategoryButton(discord.ui.Button):
    def __init__(self, category: str):
        super().__init__(label=category, style=discord.ButtonStyle.primary)
        self.category = category

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, FscDashboardView):
            await interaction.response.send_message("This dashboard is no longer available.", ephemeral=True)
            return
        await view.open_category(interaction, self.category)


class DashboardActionButton(discord.ui.Button):
    def __init__(self, action: str, label: str):
        super().__init__(label=label, style=discord.ButtonStyle.secondary)
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        view = self.view
        if not isinstance(view, FscDashboardCategoryView):
            await interaction.response.send_message("This menu is no longer available.", ephemeral=True)
            return
        await view.run_action(interaction, self.action)


class FscDashboardCategoryView(FscTimedView):
    def __init__(
        self,
        cog: FireStationCommand,
        user: discord.abc.User,
        channel: discord.abc.Messageable,
        guild: discord.Guild | None,
        category: str,
        data: Dict[str, Any] | None = None,
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.user = user
        self.channel = channel
        self.guild = guild
        self.category = category
        self.data = data or {}
        dashboard = FscDashboardView(cog, user, channel, guild, data=self.data)
        for action in dashboard._category_actions(category, self.data):
            self.add_item(DashboardActionButton(action, dashboard.ACTION_LABELS[action]))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    async def run_action(self, interaction: discord.Interaction, action: str) -> None:
        data = await self.cog.config.user(self.user).all()
        dashboard = FscDashboardView(
            self.cog,
            self.user,
            interaction.channel or self.channel,
            interaction.guild or self.guild,
            data=data,
        )
        if not dashboard._action_available(action, data):
            feature = dashboard.ACTION_FEATURES.get(action, action)
            embed = await self.cog._build_dashboard_embed(self.user)
            embed.add_field(name="Action locked", value=self.cog._feature_locked_text(feature), inline=False)
            await interaction.response.edit_message(content=None, embed=embed, view=dashboard)
            return
        handler = getattr(dashboard, action, None)
        if handler is None:
            await interaction.response.send_message("Unknown dashboard action.", ephemeral=True)
            return
        await handler(interaction, None)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=4)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        embed = await self.cog._build_dashboard_embed(self.user)
        view = FscDashboardView(
            self.cog,
            self.user,
            interaction.channel or self.channel,
            interaction.guild or self.guild,
            data=data,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class RecruitmentView(FscTimedView):
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


class AlertChoiceView(FscTimedView):
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


class TurnoutDecisionView(FscTimedView):
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
            if isinstance(child, discord.ui.Button):
                child.style = discord.ButtonStyle.secondary
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass

    async def on_timeout(self) -> None:
        self._disable_children()
        if self.message is not None:
            try:
                await self.message.edit(
                    content="Turnout decision timed out. The original station can no longer control this call.",
                    view=self,
                )
            except Exception:
                pass

        embed = discord.Embed(
            title="Dispatch needs a new officer",
            description=self.cog._turnout_timeout_narrative(),
            color=discord.Color.orange(),
        )
        embed.add_field(
            name="Takeover window",
            value="Another server member can take over this incident and run it as a fresh dispatch.",
            inline=False,
        )
        view = TurnoutTakeoverView(self.cog, self.channel, self.user)
        try:
            message = await self.channel.send(embed=embed, view=view)
            view.message = message
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


class TurnoutTakeoverView(FscTimedView):
    def __init__(self, cog: FireStationCommand, channel: discord.abc.Messageable, original_user: discord.abc.User):
        super().__init__(timeout=120)
        self.cog = cog
        self.channel = channel
        self.original_user = original_user

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id != self.original_user.id

    async def on_timeout(self) -> None:
        self._disable_children()
        user_conf = self.cog.config.user(self.original_user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if self.cog._mission_is_stage(mission, self.cog.STAGE_STAFF_TURNOUT):
            await user_conf.active_mission.set({})

        embed = discord.Embed(
            title="Dispatch abandoned",
            description=self.cog._abandoned_dispatch_narrative(),
            color=discord.Color.red(),
        )
        if self.message is not None:
            try:
                await self.message.edit(content=None, embed=embed, view=self)
                return
            except Exception:
                pass
        try:
            await self.channel.send(embed=embed)
        except Exception:
            pass

    @discord.ui.button(label="Take over dispatch", style=discord.ButtonStyle.danger)
    async def take_over(self, interaction: discord.Interaction, button: discord.ui.Button):
        self._disable_children()
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
        await self.cog.handle_takeover_incident(
            interaction,
            interaction.channel or self.channel,
            interaction.guild,
            self.original_user,
            interaction.user,
        )
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


class VehicleSelectView(FscTimedView):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        vehicles: List[Dict[str, Any]],
    ):
        super().__init__(timeout=180)
        self.add_item(VehicleSelect(cog, channel, user, vehicles))


class BackupVehicleSelect(discord.ui.Select):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        vehicles: List[Dict[str, Any]],
    ):
        options: List[discord.SelectOption] = []
        for vehicle in vehicles:
            label = f"{vehicle['name']} (cap {vehicle['crew_capacity']})"
            options.append(discord.SelectOption(label=label, value=str(vehicle["id"])))
        if not options:
            options = [discord.SelectOption(label="No local backup vehicles available", value="none")]

        super().__init__(
            placeholder="Select backup vehicles to dispatch",
            min_values=1,
            max_values=len(options),
            options=options,
        )
        self.cog = cog
        self.channel = channel
        self.user = user

    async def callback(self, interaction: discord.Interaction):
        if self.values == ["none"]:
            await interaction.response.send_message(
                "No local backup vehicles are available. Mutual aid from other members is planned next.",
                ephemeral=True,
            )
            return
        await self.cog.handle_backup_vehicle_selection(interaction, self.channel, self.user, list(self.values))


class SceneBackupView(FscTimedView):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        vehicles: List[Dict[str, Any]],
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.channel = channel
        self.user = user
        self.add_item(BackupVehicleSelect(cog, channel, user, vehicles))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Continue without backup", style=discord.ButtonStyle.secondary)
    async def continue_without_backup(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
        await self.cog.handle_continue_without_scene_backup(interaction, self.channel, self.user)
        self.stop()


class MissionControlView(FscTimedView):
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
        elif stage == self.cog.STAGE_SCENE_BACKUP:
            self.add_item(BackupVehicleSelect(cog, channel, user, cog._backup_candidate_vehicles(vehicles, mission)))
            self._add_button("Continue without backup", discord.ButtonStyle.secondary, self._continue_without_backup)
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

    async def _continue_without_backup(self, interaction: discord.Interaction):
        await self._disable_and_edit(interaction)
        await self.cog.handle_continue_without_scene_backup(interaction, self.channel, self.user)
        self.stop()

    async def _cancel(self, interaction: discord.Interaction):
        await self._disable_and_edit(interaction)
        await self.cog.handle_cancel_incident(interaction, self.channel, self.user)
        self.stop()

    async def _refresh(self, interaction: discord.Interaction):
        if await self.cog._run_due_mission_action(interaction.channel or self.channel, self.user):
            refreshed = await self.cog.config.user(self.user).all()
            mission = refreshed.get("active_mission", {}) or {}
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
            return

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
    PAGE_SIZE = 25

    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        guild: discord.Guild | None = None,
        data: Dict[str, Any] | None = None,
        command_level: int = 1,
        page: int = 0,
    ):
        self.cog = cog
        self.channel = channel
        self.user = user
        self.guild = guild
        self.data = data or {}
        self.page = max(0, page)

        options: List[discord.SelectOption] = []
        items = list(self.cog.VEHICLE_CATALOG.items())
        start = self.page * self.PAGE_SIZE
        for vid, v in items[start:start + self.PAGE_SIZE]:
            unlock_level = int(v.get("unlock_level", 1))
            missing_expansions = self.cog._missing_required_expansion_ids(v, self.data)
            locked = command_level < unlock_level or bool(missing_expansions)
            label = f"{v['name']} ({v['price']:,} cr, cap {v['crew_capacity']})"
            description = None
            if locked:
                if missing_expansions:
                    description = f"Requires {self.cog._expansion_requirement_display_text(missing_expansions)}"
                else:
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
            placeholder=f"Select a vehicle to buy (page {self.page + 1})",
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
        if not self.cog._vehicle_is_unlocked(vdef, command_level, data):
            required_level = int(vdef.get("unlock_level", 1))
            missing_expansions = self.cog._missing_required_expansion_ids(vdef, data)
            if missing_expansions:
                locked_text = (
                    f"{vdef['name']} requires expansion: "
                    f"{self.cog._expansion_requirement_display_text(missing_expansions)}."
                )
            else:
                locked_text = f"{vdef['name']} requires command level {required_level}."
            embed = self.cog._build_vehicle_shop_embed(data)
            embed.add_field(
                name="Purchase locked",
                value=locked_text,
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

        base_price = int(vdef["price"])
        credits = await self.cog._get_credits(self.user)
        price = self.cog._economy_scaled_cost(base_price, credits)
        embed = discord.Embed(
            title="Confirm vehicle purchase",
            description=f"Buy **{vdef['name']}** for **{price:,}** credits?",
            color=discord.Color.blurple(),
        )
        embed.add_field(name="Crew capacity", value=str(vdef["crew_capacity"]), inline=True)
        self.cog._add_economy_pricing_note(embed, base_price, price, credits)
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


class VehicleShopView(FscTimedView):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        guild: discord.Guild | None = None,
        data: Dict[str, Any] | None = None,
        page: int = 0,
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.channel = channel
        self.user = user
        self.guild = guild
        data = data or {}
        self.data = data
        self.page = max(0, page)
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", cog._command_level_for_xp(xp)))
        self.add_item(
            VehicleShopSelect(
                cog,
                channel,
                user,
                guild,
                data=self.data,
                command_level=command_level,
                page=self.page,
            )
        )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog._build_dashboard_embed(self.user)
        channel = interaction.channel or self.channel
        guild = interaction.guild or self.guild
        view = FscDashboardView(self.cog, self.user, channel, guild)
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class MaintenanceView(FscTimedView):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        guild: discord.Guild | None = None,
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.channel = channel
        self.user = user
        self.guild = guild

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Repair fleet", style=discord.ButtonStyle.success)
    async def repair(self, interaction: discord.Interaction, button: discord.ui.Button):
        channel = interaction.channel or self.channel
        guild = interaction.guild or self.guild
        await self.cog._repair_fleet(
            interaction,
            channel,
            self.user,
            guild,
            edit_message=True,
        )

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog._build_dashboard_embed(self.user)
        channel = interaction.channel or self.channel
        guild = interaction.guild or self.guild
        data = await self.cog.config.user(self.user).all()
        view = FscDashboardView(self.cog, self.user, channel, guild, data=data)
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class EquipmentShopSelect(discord.ui.Select):
    PAGE_SIZE = 25

    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        guild: discord.Guild | None = None,
        data: Dict[str, Any] | None = None,
        command_level: int = 1,
        page: int = 0,
    ):
        self.cog = cog
        self.channel = channel
        self.user = user
        self.guild = guild
        self.data = data or {}
        self.page = max(0, page)

        options: List[discord.SelectOption] = []
        items = list(self.cog.EQUIPMENT_CATALOG.items())
        start = self.page * self.PAGE_SIZE
        for equipment_id, equipment in items[start:start + self.PAGE_SIZE]:
            unlock_level = int(equipment.get("unlock_level", 1))
            missing_expansions = self.cog._missing_required_expansion_ids(equipment, self.data)
            locked = command_level < unlock_level or bool(missing_expansions)
            label = f"{equipment['name']} ({equipment['price']:,} cr)"
            description = None
            if locked:
                if missing_expansions:
                    description = f"Requires {self.cog._expansion_requirement_display_text(missing_expansions)}"
                else:
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
            placeholder=f"Select equipment to buy (page {self.page + 1})",
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
        if not self.cog._equipment_is_unlocked(equipment, command_level, data):
            required_level = int(equipment.get("unlock_level", 1))
            missing_expansions = self.cog._missing_required_expansion_ids(equipment, data)
            if missing_expansions:
                locked_text = (
                    f"{equipment['name']} requires expansion: "
                    f"{self.cog._expansion_requirement_display_text(missing_expansions)}."
                )
            else:
                locked_text = f"{equipment['name']} requires command level {required_level}."
            embed = self.cog._build_equipment_shop_embed(data)
            embed.add_field(
                name="Purchase locked",
                value=locked_text,
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

        base_price = int(equipment["price"])
        credits = await self.cog._get_credits(self.user)
        price = self.cog._economy_scaled_cost(base_price, credits)
        embed = discord.Embed(
            title="Confirm equipment purchase",
            description=f"Buy **{equipment['name']}** for **{price:,}** credits?",
            color=discord.Color.blue(),
        )
        self.cog._add_economy_pricing_note(embed, base_price, price, credits)
        capabilities = equipment.get("capabilities", {})
        if isinstance(capabilities, dict) and capabilities:
            capability_text = ", ".join(f"{name}: {value}" for name, value in capabilities.items())
            embed.add_field(name="Capabilities", value=capability_text, inline=False)
        self.cog._apply_equipment_image(embed, equipment)

        view = ConfirmEquipmentPurchaseView(
            self.cog,
            self.channel,
            self.user,
            choice,
            guild=interaction.guild or self.guild,
            edit_message=True,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class EquipmentShopView(FscTimedView):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        guild: discord.Guild | None = None,
        data: Dict[str, Any] | None = None,
        page: int = 0,
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.channel = channel
        self.user = user
        self.guild = guild
        data = data or {}
        self.data = data
        self.page = max(0, page)
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", cog._command_level_for_xp(xp)))
        self.add_item(
            EquipmentShopSelect(
                cog,
                channel,
                user,
                guild,
                data=self.data,
                command_level=command_level,
                page=self.page,
            )
        )

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog._build_dashboard_embed(self.user)
        channel = interaction.channel or self.channel
        guild = interaction.guild or self.guild
        view = FscDashboardView(self.cog, self.user, channel, guild)
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.secondary)
    async def previous_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        page = max(0, self.page - 1)
        embed = self.cog._build_equipment_shop_embed(data)
        view = EquipmentShopView(self.cog, interaction.channel or self.channel, self.user, interaction.guild or self.guild, data=data, page=page)
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        data = await self.cog.config.user(self.user).all()
        max_page = max(0, math.ceil(len(self.cog.EQUIPMENT_CATALOG) / EquipmentShopSelect.PAGE_SIZE) - 1)
        page = min(max_page, self.page + 1)
        embed = self.cog._build_equipment_shop_embed(data)
        view = EquipmentShopView(self.cog, interaction.channel or self.channel, self.user, interaction.guild or self.guild, data=data, page=page)
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class TrainingSelect(discord.ui.Select):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        guild: discord.Guild | None = None,
        data: Dict[str, Any] | None = None,
    ):
        self.cog = cog
        self.channel = channel
        self.user = user
        self.guild = guild
        data = data or {}
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", cog._command_level_for_xp(xp)))
        trained = cog._training_inventory_set(data.get("trainings", []))

        options: List[discord.SelectOption] = []
        for training_id, training in self.cog.TRAINING_CATALOG.items():
            if training_id in trained:
                continue
            unlock_level = int(training.get("unlock_level", 1))
            locked = command_level < unlock_level
            label = f"{training['name']} ({training['price']:,} cr)"
            description = f"{training.get('duration_hours', 0)} hour(s)"
            if locked:
                description = f"Requires command level {unlock_level}"
                label = f"Locked - {label}"
            options.append(
                discord.SelectOption(
                    label=label,
                    value=training_id,
                    description=description,
                    default=False,
                    emoji=None,
                )
            )

        if not options:
            options = [discord.SelectOption(label="No training available", value="none")]

        super().__init__(
            placeholder="Select training to complete",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        choice = self.values[0]
        if choice == "none":
            await interaction.response.send_message("No training available.", ephemeral=True)
            return

        training = self.cog.TRAINING_CATALOG.get(choice)
        if not training:
            await interaction.response.send_message("Unknown training type.", ephemeral=True)
            return

        data = await self.cog.config.user(self.user).all()
        trained = self.cog._training_inventory_set(data.get("trainings", []))
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self.cog._command_level_for_xp(xp)))
        if choice in trained:
            embed = self.cog._build_training_embed(data)
            embed.add_field(name="Training complete", value="This training is already completed.", inline=False)
            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=TrainingView(self.cog, self.channel, self.user, interaction.guild or self.guild, data=data),
            )
            return
        if not self.cog._training_is_unlocked(training, command_level):
            required_level = int(training.get("unlock_level", 1))
            embed = self.cog._build_training_embed(data)
            embed.add_field(
                name="Training locked",
                value=f"{training['name']} requires command level {required_level}.",
                inline=False,
            )
            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=TrainingView(self.cog, self.channel, self.user, interaction.guild or self.guild, data=data),
            )
            return

        base_price = int(training["price"])
        credits = await self.cog._get_credits(self.user)
        price = self.cog._economy_scaled_cost(base_price, credits)
        embed = discord.Embed(
            title="Confirm training",
            description=f"Complete station certification **{training['name']}** for **{price:,}** credits?",
            color=discord.Color.gold(),
        )
        embed.add_field(
            name="Training scope",
            value="Permanent station certification for current and future staff.",
            inline=False,
        )
        self.cog._add_economy_pricing_note(embed, base_price, price, credits)
        duration = int(training.get("duration_hours", 0))
        if duration > 0:
            embed.add_field(name="Training duration", value=f"{duration} hour(s)", inline=True)

        view = ConfirmTrainingView(
            self.cog,
            self.channel,
            self.user,
            choice,
            guild=interaction.guild or self.guild,
            edit_message=True,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class TrainingView(FscTimedView):
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
        self.add_item(TrainingSelect(cog, channel, user, guild, data=data))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog._build_dashboard_embed(self.user)
        channel = interaction.channel or self.channel
        guild = interaction.guild or self.guild
        view = FscDashboardView(self.cog, self.user, channel, guild)
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class ExpansionSelect(discord.ui.Select):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        guild: discord.Guild | None = None,
        data: Dict[str, Any] | None = None,
    ):
        self.cog = cog
        self.channel = channel
        self.user = user
        self.guild = guild
        data = data or {}
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", cog._command_level_for_xp(xp)))
        owned = cog._expansion_inventory_set(data.get("expansions", []))

        options: List[discord.SelectOption] = []
        for expansion_id, expansion in self.cog.EXPANSION_CATALOG.items():
            if expansion_id in owned:
                continue
            unlock_level = int(expansion.get("unlock_level", 1))
            locked = command_level < unlock_level
            label = f"{expansion['name']} ({expansion['price']:,} cr)"
            description = str(expansion.get("description", ""))[:100] or f"{expansion.get('build_time_hours', 0)} hour(s)"
            if locked:
                description = f"Requires command level {unlock_level}"
                label = f"Locked - {label}"
            options.append(
                discord.SelectOption(
                    label=label,
                    value=expansion_id,
                    description=description,
                    default=False,
                    emoji=None,
                )
            )

        if not options:
            options = [discord.SelectOption(label="No expansions available", value="none")]

        super().__init__(
            placeholder="Select expansion to build",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        choice = self.values[0]
        if choice == "none":
            await interaction.response.send_message("No expansions available.", ephemeral=True)
            return

        expansion = self.cog.EXPANSION_CATALOG.get(choice)
        if not expansion:
            await interaction.response.send_message("Unknown expansion type.", ephemeral=True)
            return

        data = await self.cog.config.user(self.user).all()
        owned = self.cog._expansion_inventory_set(data.get("expansions", []))
        xp = int(data.get("xp", 0))
        command_level = int(data.get("command_level", self.cog._command_level_for_xp(xp)))
        if choice in owned:
            embed = self.cog._build_expansion_embed(data)
            embed.add_field(name="Expansion already built", value="This expansion is already active.", inline=False)
            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=ExpansionView(self.cog, self.channel, self.user, interaction.guild or self.guild, data=data),
            )
            return
        if not self.cog._expansion_is_unlocked(expansion, command_level):
            required_level = int(expansion.get("unlock_level", 1))
            embed = self.cog._build_expansion_embed(data)
            embed.add_field(
                name="Expansion not available yet",
                value=f"{expansion['name']} unlocks at command level {required_level}.",
                inline=False,
            )
            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=ExpansionView(self.cog, self.channel, self.user, interaction.guild or self.guild, data=data),
            )
            return

        base_price = int(expansion["price"])
        credits = await self.cog._get_credits(self.user)
        price = self.cog._economy_scaled_cost(base_price, credits)
        embed = discord.Embed(
            title="Confirm expansion",
            description=f"Build **{expansion['name']}** for **{price:,}** credits?",
            color=discord.Color.blue(),
        )
        description = expansion.get("description")
        if isinstance(description, str) and description:
            embed.add_field(name="Description", value=description, inline=False)
        effects = expansion.get("effects", {})
        if isinstance(effects, dict) and effects:
            effect_text = ", ".join(f"{name}: {value}" for name, value in effects.items())
            embed.add_field(name="Effects", value=effect_text, inline=False)
        self.cog._add_economy_pricing_note(embed, base_price, price, credits)

        view = ConfirmExpansionView(
            self.cog,
            self.channel,
            self.user,
            choice,
            guild=interaction.guild or self.guild,
            edit_message=True,
        )
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class ExpansionView(FscTimedView):
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
        self.add_item(ExpansionSelect(cog, channel, user, guild, data=data))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = await self.cog._build_dashboard_embed(self.user)
        channel = interaction.channel or self.channel
        guild = interaction.guild or self.guild
        view = FscDashboardView(self.cog, self.user, channel, guild)
        await interaction.response.edit_message(content=None, embed=embed, view=view)


class ConfirmRecruitView(FscTimedView):
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


class ConfirmUpgradeView(FscTimedView):
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


class ConfirmCareerView(FscTimedView):
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


class ConfirmVehiclePurchaseView(FscTimedView):
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
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
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


class ConfirmEquipmentPurchaseView(FscTimedView):
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


class ConfirmTrainingView(FscTimedView):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        training_id: str,
        *,
        guild: discord.Guild | None = None,
        edit_message: bool = False,
    ):
        super().__init__(timeout=60)
        self.cog = cog
        self.channel = channel
        self.user = user
        self.training_id = training_id
        self.guild = guild
        self.edit_message = edit_message

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog._confirm_training_purchase(
            interaction,
            self.channel,
            self.user,
            self.training_id,
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
            embed = self.cog._build_training_embed(data)
            embed.add_field(name="Training cancelled", value="No training was completed.", inline=False)
            view = TrainingView(
                self.cog,
                self.channel,
                self.user,
                interaction.guild or self.guild,
                data=data,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            self.stop()
            return
        await interaction.response.send_message("Training cancelled.", ephemeral=True)
        self.stop()


class ConfirmExpansionView(FscTimedView):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        expansion_id: str,
        *,
        guild: discord.Guild | None = None,
        edit_message: bool = False,
    ):
        super().__init__(timeout=60)
        self.cog = cog
        self.channel = channel
        self.user = user
        self.expansion_id = expansion_id
        self.guild = guild
        self.edit_message = edit_message

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog._confirm_expansion_purchase(
            interaction,
            self.channel,
            self.user,
            self.expansion_id,
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
            embed = self.cog._build_expansion_embed(data)
            embed.add_field(name="Expansion cancelled", value="No expansion was built.", inline=False)
            view = ExpansionView(
                self.cog,
                self.channel,
                self.user,
                interaction.guild or self.guild,
                data=data,
            )
            await interaction.response.edit_message(content=None, embed=embed, view=view)
            self.stop()
            return
        await interaction.response.send_message("Expansion cancelled.", ephemeral=True)
        self.stop()
