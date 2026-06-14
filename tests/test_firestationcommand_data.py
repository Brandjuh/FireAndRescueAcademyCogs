import asyncio
from datetime import datetime, timezone
from pathlib import Path

import discord
import yaml

from FireStationCommand.fire_station_command import (
    AlertChoiceView,
    ConfirmCareerView,
    ConfirmUpgradeView,
    ConfirmVehiclePurchaseView,
    EquipmentShopSelect,
    EquipmentShopView,
    FireStationCommand,
    FscDashboardView,
    MaintenanceView,
    RecruitmentView,
    TurnoutTakeoverView,
    VehicleShopSelect,
    VehicleShopView,
)


_FSC_ROOT = Path(__file__).resolve().parents[1] / "FireStationCommand"


def _cog_with_game_data(game_data):
    cog = object.__new__(FireStationCommand)
    cog.game_data = game_data
    cog.vehicle_definitions = FireStationCommand._build_vehicle_definitions(cog)
    cog.equipment_definitions = FireStationCommand._equipment_definitions(cog)
    cog.training_definitions = FireStationCommand._training_definitions(cog)
    cog.expansion_definitions = FireStationCommand._expansion_definitions(cog)
    cog.VEHICLE_CATALOG = FireStationCommand._build_vehicle_catalog(cog)
    cog.EQUIPMENT_CATALOG = FireStationCommand._build_equipment_catalog(cog)
    cog.TRAINING_CATALOG = FireStationCommand._build_training_catalog(cog)
    cog.EXPANSION_CATALOG = FireStationCommand._build_expansion_catalog(cog)
    cog._utcnow = lambda: datetime(2026, 6, 12, 12, 0, tzinfo=timezone.utc)
    return cog


def _load_yaml_catalog(name):
    path = _FSC_ROOT / "data" / "config" / f"{name}.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))[name]


def test_config_files_parse_as_yaml_with_expected_roots():
    config_dir = _FSC_ROOT / "data" / "config"
    config_files = sorted(config_dir.glob("*.yaml"))

    assert config_files
    for path in config_files:
        parsed = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(parsed, dict)
        assert path.stem in parsed


class _ValueSetter:
    def __init__(self, data, key):
        self.data = data
        self.key = key

    async def set(self, value):
        self.data[self.key] = value


class _UserConfig:
    def __init__(self, data):
        self.data = data

    async def all(self):
        return dict(self.data)

    def __getattr__(self, name):
        return _ValueSetter(self.data, name)


class _Config:
    def __init__(self, user_data, global_data):
        self.user_data = user_data
        self.global_data = global_data

    def user(self, user):
        del user
        return _UserConfig(self.user_data)

    async def all(self):
        return dict(self.global_data)


class _MultiUserConfig:
    def __init__(self, users, global_data):
        self.users = users
        self.global_data = global_data

    def user(self, user):
        return _UserConfig(self.users[user.id])

    async def all(self):
        return dict(self.global_data)


class _Message:
    def __init__(self):
        self.edited = None

    async def edit(self, **kwargs):
        self.edited = kwargs


class _Channel:
    id = 987

    def __init__(self):
        self.sent = []

    async def send(self, **kwargs):
        message = _Message()
        self.sent.append({**kwargs, "message": message})
        return message


class _InteractionResponse:
    def __init__(self):
        self.edited = None
        self.sent = None

    async def edit_message(self, **kwargs):
        self.edited = kwargs

    async def send_message(self, *args, **kwargs):
        self.sent = {"args": args, **kwargs}


class _Interaction:
    def __init__(self, user, channel=None, guild=None):
        self.user = user
        self.channel = channel or object()
        self.guild = guild or object()
        self.message = _Message()
        self.response = _InteractionResponse()


def test_build_vehicle_catalog_uses_yaml_vehicle_data():
    cog = _cog_with_game_data(
        {
            "vehicles": {
                "vehicles": [
                    {
                        "id": "engine_basic",
                        "name": "Standard Fire Engine",
                        "required_staff": 4,
                        "base_cost": 50000,
                        "image": "Images/Vehicles/engine_basic.png",
                    }
                ]
            }
        }
    )

    catalog = FireStationCommand._build_vehicle_catalog(cog)

    assert catalog == {
        "engine_basic": {
            "name": "Standard Fire Engine",
            "crew_capacity": 4,
            "price": 50000,
            "image": "Images/Vehicles/engine_basic.png",
            "unlock_level": 1,
            "capabilities": {},
            "required_training": [],
            "required_expansions": [],
            "maintenance_cost": 500,
        }
    }


def test_imported_mission_catalog_has_balanced_xp_and_complete_narratives():
    missions = _load_yaml_catalog("missions")

    assert len(missions) >= 1200
    xp_values = [int(mission["base_xp"]) for mission in missions]
    assert min(xp_values) >= 35
    assert max(xp_values) >= 700
    assert len(set(xp_values)) >= 40

    high_credit_low_level = [
        mission
        for mission in missions
        if int(mission["base_credits"]) >= 10000 and int(mission["unlock_level"]) < 6
    ]
    assert high_credit_low_level == []

    narrative_fields = [
        "description",
        "dispatch_narrative",
        "scene_narrative",
        "success_narrative",
        "partial_narrative",
        "failure_narrative",
    ]
    for mission in missions:
        for field in narrative_fields:
            assert mission[field].strip()
        for field in ["description", "dispatch_narrative", "scene_narrative"]:
            assert mission["name"].lower() in mission[field].lower()


def test_imported_catalog_references_existing_vehicles_and_equipment():
    missions = _load_yaml_catalog("missions")
    vehicle_ids = {vehicle["id"] for vehicle in _load_yaml_catalog("vehicles")}
    equipment_ids = {equipment["id"] for equipment in _load_yaml_catalog("equipment")}

    missing_vehicle_refs = [
        (mission["id"], vehicle_id)
        for mission in missions
        for vehicle_id in mission.get("required_vehicles", [])
        if vehicle_id not in vehicle_ids
    ]
    missing_equipment_refs = [
        (mission["id"], equipment_id)
        for mission in missions
        for equipment_id in mission.get("required_equipment", [])
        if equipment_id not in equipment_ids
    ]

    assert missing_vehicle_refs == []
    assert missing_equipment_refs == []


def test_imported_equipment_catalog_has_images_and_progression_depth():
    equipment = _load_yaml_catalog("equipment")

    assert len(equipment) >= 30
    assert {item["id"] for item in equipment} >= {
        "traffic_control_kit",
        "ems_bag",
        "law_enforcement_kit",
        "hazmat_kit",
        "decon_kit",
        "water_rescue_gear",
        "command_tablet",
        "tactical_gear",
        "aviation_rescue_kit",
    }
    for item in equipment:
        assert 1 <= int(item.get("unlock_level", 0)) <= FireStationCommand.MAX_COMMAND_LEVEL
        assert item.get("capabilities")
        assert (_FSC_ROOT / item["image"]).exists()


def test_imported_missions_have_equipment_depth_without_losing_quantities():
    missions = _load_yaml_catalog("missions")
    equipment_ids = {equipment["id"] for equipment in _load_yaml_catalog("equipment")}
    missing_equipment = [mission["id"] for mission in missions if not mission.get("required_equipment")]
    personnel_count = sum(
        1
        for mission in missions
        for equipment_id in mission.get("required_equipment", [])
        if equipment_id == "personnel"
    )
    equipment_requirement_count = sum(
        1
        for mission in missions
        for equipment_id in mission.get("required_equipment", [])
        if equipment_id in equipment_ids
    )

    assert missing_equipment == []
    assert personnel_count >= 650
    assert equipment_requirement_count >= 4000


def test_imported_vehicle_equipment_slots_reference_catalog():
    vehicles = _load_yaml_catalog("vehicles")
    equipment_ids = {equipment["id"] for equipment in _load_yaml_catalog("equipment")}

    assert all(vehicle.get("equipment_slots") for vehicle in vehicles)
    assert [
        (vehicle["id"], slot)
        for vehicle in vehicles
        for slot in vehicle.get("equipment_slots", [])
        if slot not in equipment_ids
    ] == []


def test_imported_expansions_unlock_specialized_services():
    expansions = _load_yaml_catalog("expansions")
    expansion_ids = {expansion["id"] for expansion in expansions}

    assert expansion_ids >= {
        "command_room",
        "ems_bay",
        "police_liaison",
        "hazmat_unit",
        "water_rescue_bay",
        "wildland_cache",
        "foam_storage",
        "aviation_pad",
        "rescue_bay",
    }
    assert len(expansions) >= 12


def test_imported_ambulance_and_police_content_requires_extensions():
    vehicles = _load_yaml_catalog("vehicles")
    missions = _load_yaml_catalog("missions")

    assert all(
        "ems_bay" in vehicle.get("required_expansions", [])
        for vehicle in vehicles
        if vehicle.get("category") == "ems"
    )
    assert all(
        "police_liaison" in vehicle.get("required_expansions", [])
        for vehicle in vehicles
        if vehicle.get("category") == "police"
    )
    assert all(
        "ems_bay" in mission.get("required_expansions", [])
        for mission in missions
        if "Ambulance Missions" in mission.get("mission_type", "")
    )
    assert all(
        "police_liaison" in mission.get("required_expansions", [])
        for mission in missions
        if "Police Missions" in mission.get("mission_type", "")
    )


def test_imported_expansion_gating_keeps_fire_core_primary():
    missions = _load_yaml_catalog("missions")
    core_missions = [mission for mission in missions if not mission.get("required_expansions")]

    assert len(core_missions) >= 30
    assert any("Fire Fighting Missions" in mission.get("mission_type", "") for mission in core_missions)


def test_imported_missions_include_expansion_gates_from_required_loadout():
    missions = _load_yaml_catalog("missions")
    vehicles = _load_yaml_catalog("vehicles")
    equipment = _load_yaml_catalog("equipment")
    vehicle_expansions = {
        vehicle["id"]: set(vehicle.get("required_expansions", []))
        for vehicle in vehicles
    }
    equipment_expansions = {
        item["id"]: set(item.get("required_expansions", []))
        for item in equipment
    }

    missing = []
    for mission in missions:
        required_expansions = set(mission.get("required_expansions", []))
        loadout_expansions = set()
        for vehicle_id in mission.get("required_vehicles", []):
            loadout_expansions.update(vehicle_expansions.get(vehicle_id, set()))
        for equipment_id in mission.get("required_equipment", []):
            loadout_expansions.update(equipment_expansions.get(equipment_id, set()))
        missing.extend(
            (mission["id"], expansion_id)
            for expansion_id in sorted(loadout_expansions - required_expansions)
        )

    assert missing == []


def test_imported_early_extensions_are_affordable_from_core_missions():
    missions = _load_yaml_catalog("missions")
    expansions = _load_yaml_catalog("expansions")
    early_core_rewards = [
        int(mission.get("base_credits", 0))
        for mission in missions
        if int(mission.get("unlock_level", 1)) <= 2 and not mission.get("required_expansions")
    ]
    average_reward = sum(early_core_rewards) / len(early_core_rewards)
    early_expansions = [
        expansion
        for expansion in expansions
        if int(expansion.get("unlock_level", 1)) == 2
    ]

    assert early_core_rewards
    assert all(expansion["base_cost"] / average_reward <= 65 for expansion in early_expansions)


def test_build_incidents_derives_staff_from_required_vehicles():
    cog = _cog_with_game_data(
        {
            "missions": {
                "missions": [
                    {
                        "id": "small_bin_fire",
                        "name": "Small Bin Fire",
                        "base_credits": 200,
                        "base_xp": 40,
                        "min_tier": 1,
                        "recommended_level": 1,
                        "image": "Images/Missions/small_bin_fire.png",
                        "required_vehicles": ["engine_basic"],
                        "required_equipment": ["hose"],
                        "description": "A small bin fire in a residential area.",
                        "dispatch_narrative": "Smoke is showing behind several homes.",
                        "scene_narrative": "The crew finds fire spreading along a fence.",
                        "success_narrative": "The fire is knocked down quickly.",
                        "partial_narrative": "The fire is controlled with minor extension.",
                        "failure_narrative": "The fire spreads before crews gain control.",
                        "capabilities": {"fire_suppression": 35},
                    }
                ]
            },
            "vehicles": {
                "vehicles": [
                    {
                        "id": "engine_basic",
                        "name": "Standard Fire Engine",
                        "required_staff": 4,
                        "base_cost": 50000,
                    }
                ]
            },
        }
    )

    incidents = FireStationCommand._build_incidents(cog)

    assert incidents == [
        {
            "id": "small_bin_fire",
            "name": "Small Bin Fire",
            "required_staff": 4,
            "base_credits": 200,
            "base_xp": 40,
            "tier": 1,
            "recommended_level": 1,
            "unlock_level": 1,
            "capabilities": {"fire_suppression": 35},
            "image": "Images/Missions/small_bin_fire.png",
            "hint": "A small bin fire in a residential area.",
            "detail": "The crew finds fire spreading along a fence.",
            "dispatch_narrative": "Smoke is showing behind several homes.",
            "success_narrative": "The fire is knocked down quickly.",
            "partial_narrative": "The fire is controlled with minor extension.",
            "failure_narrative": "The fire spreads before crews gain control.",
            "required_vehicles": ["engine_basic"],
            "required_equipment": ["hose"],
            "required_expansions": [],
        }
    ]


def test_balance_helpers_read_values_with_fallbacks():
    cog = _cog_with_game_data(
        {
            "balance": {
                "balance": {
                    "career_upgrade_cost": 250000,
                    "career_turnout_seconds": 30,
                    "credits_reward_multiplier": 1.25,
                }
            }
        }
    )

    assert FireStationCommand._balance_int(cog, "career_upgrade_cost", 1) == 250000
    assert FireStationCommand._balance_seconds_as_minutes(cog, "career_turnout_seconds", 0) == 0.5
    assert FireStationCommand._reward_multiplier(cog) == 1.25
    assert FireStationCommand._balance_int(cog, "missing", 7) == 7


def test_command_level_uses_progression_thresholds():
    cog = _cog_with_game_data(
        {
            "progression": {
                "progression": {
                    "level_xp": {
                        1: 0,
                        2: 100,
                        3: 250,
                    }
                }
            }
        }
    )

    assert FireStationCommand._command_level_for_xp(cog, 0) == 1
    assert FireStationCommand._command_level_for_xp(cog, 100) == 2
    assert FireStationCommand._command_level_for_xp(cog, 249) == 2
    assert FireStationCommand._command_level_for_xp(cog, 250) == 3
    assert FireStationCommand._xp_for_next_command_level(cog, 2) == 250


def test_readiness_score_combines_capabilities_staff_vehicles_and_level():
    cog = _cog_with_game_data(
        {
            "vehicles": {
                "vehicles": [
                    {
                        "id": "engine_basic",
                        "name": "Standard Fire Engine",
                        "required_staff": 4,
                        "capabilities": {"fire_suppression": 45, "water_supply": 30},
                        "equipment_slots": ["hose", "basic_tools"],
                    }
                ]
            },
            "equipment": {
                "equipment": [
                    {"id": "hose", "capabilities": {"fire_suppression": 15}},
                    {"id": "basic_tools", "capabilities": {"scene_safety": 10}},
                ]
            },
        }
    )
    mission = {
        "required_staff": 4,
        "required_vehicles": ["engine_basic"],
        "recommended_level": 1,
        "capabilities": {"fire_suppression": 35, "water_supply": 20},
    }
    data = {
        "command_level": 1,
        "staff_total": 4,
        "vehicles": [{"id": 1, "catalog_id": "engine_basic"}],
    }

    assert FireStationCommand._readiness_score(cog, mission, data) == 100


def test_readiness_score_requires_owned_equipment_for_slot_capabilities():
    cog = _cog_with_game_data(
        {
            "vehicles": {
                "vehicles": [
                    {
                        "id": "engine_basic",
                        "name": "Standard Fire Engine",
                        "required_staff": 4,
                        "capabilities": {"fire_suppression": 20},
                        "equipment_slots": ["hose"],
                    }
                ]
            },
            "equipment": {
                "equipment": [
                    {"id": "hose", "capabilities": {"fire_suppression": 30}},
                ]
            },
        }
    )
    mission = {
        "required_staff": 4,
        "required_vehicles": ["engine_basic"],
        "recommended_level": 1,
        "capabilities": {"fire_suppression": 50},
    }
    without_equipment = {
        "command_level": 1,
        "staff_total": 4,
        "vehicles": [{"id": 1, "catalog_id": "engine_basic"}],
        "equipment": [],
    }
    with_equipment = {
        **without_equipment,
        "equipment": [{"catalog_id": "hose", "quantity": 1}],
    }

    assert FireStationCommand._readiness_score(cog, mission, without_equipment) < 100
    assert FireStationCommand._readiness_score(cog, mission, with_equipment) == 100


def test_random_incident_selection_prefers_current_level_ready_missions(monkeypatch):
    cog = _cog_with_game_data({})
    cog.INCIDENTS = [
        {
            "id": "small_bin_fire",
            "name": "Small Bin Fire",
            "required_staff": 4,
            "required_vehicles": [],
            "recommended_level": 1,
            "capabilities": {},
        },
        {
            "id": "high_rise_fire",
            "name": "High Rise Fire",
            "required_staff": 20,
            "required_vehicles": ["ladder"],
            "recommended_level": 10,
            "capabilities": {"aerial_access": 100},
        },
    ]
    monkeypatch.setattr("FireStationCommand.fire_station_command.random.random", lambda: 0.9)
    monkeypatch.setattr("FireStationCommand.fire_station_command.random.choice", lambda options: options[0])

    incident = FireStationCommand._pick_random_incident(
        cog,
        {
            "command_level": 1,
            "staff_total": 6,
            "vehicles": [],
        },
    )

    assert incident["id"] == "small_bin_fire"


def test_default_global_config_keeps_manual_gameplay_timers_short():
    cog = _cog_with_game_data(
        {
            "balance": {
                "balance": {
                    "career_turnout_seconds": 30,
                    "career_upgrade_cost": 250000,
                }
            }
        }
    )

    config = FireStationCommand._build_default_global_config(cog)

    assert config["volunteer_normal_minutes"] == 2.0
    assert config["volunteer_emergency_minutes"] == 0.5
    assert config["career_turnout_minutes"] == 0.5
    assert config["realert_minutes_min"] == 0.25
    assert config["realert_minutes_max"] == 0.75
    assert config["travel_minutes_min"] == 1.0
    assert config["travel_minutes_max"] == 2.0
    assert config["scene_work_minutes_min"] == 0.5
    assert config["scene_work_minutes_max"] == 1.5
    assert config["scene_backup_chance"] == 0.25
    assert config["scene_backup_window_minutes_min"] == 0.5
    assert config["scene_backup_window_minutes_max"] == 1.0
    assert config["scene_backup_travel_minutes_min"] == 0.5
    assert config["scene_backup_travel_minutes_max"] == 1.5
    assert config["maintenance_out_of_service_condition"] == 25
    assert config["maintenance_out_of_service_minutes"] == 30
    assert config["max_station_level"] == 10


def test_relative_text_rounds_short_positive_waits_up_to_one_minute():
    cog = _cog_with_game_data({})

    assert FireStationCommand._make_relative_text(cog, 0.0) == "now"
    assert FireStationCommand._make_relative_text(cog, 0.5) == "in 1 minute"
    assert FireStationCommand._make_relative_text(cog, 1.1) == "in 2 minutes"


def test_mission_image_helpers_build_raw_urls_and_apply_embed_image():
    cog = _cog_with_game_data({})
    mission = {"image": "Images/Missions/small_bin_fire.png"}
    embed = discord.Embed()

    assert FireStationCommand._mission_image_url(cog, mission) == (
        "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
        "refs/heads/main/FireStationCommand/Images/Missions/small_bin_fire.png"
    )
    FireStationCommand._apply_mission_image(cog, embed, mission)
    assert embed.image == {
        "url": (
            "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
            "refs/heads/main/FireStationCommand/Images/Missions/small_bin_fire.png"
        )
    }


def test_mission_result_image_helpers_use_outcome_images_and_overrides():
    cog = _cog_with_game_data({})
    mission = {"success_image": "Images/Missions/small_bin_fire_success.png"}
    embed = discord.Embed()

    assert FireStationCommand._mission_result_image_url(cog, mission, "success") == (
        "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
        "refs/heads/main/FireStationCommand/Images/Missions/small_bin_fire_success.png"
    )
    assert FireStationCommand._mission_result_image_url(cog, {}, "partial") == (
        "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
        "refs/heads/main/FireStationCommand/Images/Outcomes/incident_partial.png"
    )

    FireStationCommand._apply_mission_result_image(cog, embed, {}, "failure")
    assert embed.image == {
        "url": (
            "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
            "refs/heads/main/FireStationCommand/Images/Outcomes/incident_failure.png"
        )
    }


def test_station_image_helper_builds_level_urls_and_clamps_range():
    assert FireStationCommand._station_image_url(1) == (
        "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
        "refs/heads/main/FireStationCommand/Images/Stations/station_level_01.png"
    )
    assert FireStationCommand._station_image_url(10) == (
        "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
        "refs/heads/main/FireStationCommand/Images/Stations/station_level_10.png"
    )
    assert FireStationCommand._station_image_url(99) == (
        "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
        "refs/heads/main/FireStationCommand/Images/Stations/station_level_10.png"
    )


def test_vehicle_image_helpers_build_raw_urls_and_apply_embed_image():
    cog = _cog_with_game_data({})
    vehicle = {"image": "Images/Vehicles/engine_basic.png"}
    embed = discord.Embed()

    assert FireStationCommand._vehicle_image_url(cog, vehicle) == (
        "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
        "refs/heads/main/FireStationCommand/Images/Vehicles/engine_basic.png"
    )
    FireStationCommand._apply_vehicle_image(cog, embed, vehicle)
    assert embed.image == {
        "url": (
            "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
            "refs/heads/main/FireStationCommand/Images/Vehicles/engine_basic.png"
        )
    }


def test_equipment_image_helpers_build_raw_urls_and_apply_embed_image():
    cog = _cog_with_game_data({})
    equipment = {"image": "Images/Equipment/hose.png"}
    embed = discord.Embed()

    assert FireStationCommand._equipment_image_url(cog, equipment) == (
        "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
        "refs/heads/main/FireStationCommand/Images/Equipment/hose.png"
    )
    FireStationCommand._apply_equipment_image(cog, embed, equipment)
    assert embed.image == {
        "url": (
            "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/"
            "refs/heads/main/FireStationCommand/Images/Equipment/hose.png"
        )
    }


def test_equipment_display_text_uses_configured_names_and_fallback_ids():
    cog = _cog_with_game_data(
        {
            "equipment": {
                "equipment": [
                    {"id": "hose", "name": "Fire Hose Set"},
                    {"id": "basic_tools", "name": "Basic Hand Tools"},
                ]
            }
        }
    )

    assert FireStationCommand._equipment_display_text(cog, ["hose", "missing_tool", "basic_tools"]) == (
        "Fire Hose Set, missing_tool, Basic Hand Tools"
    )
    assert FireStationCommand._equipment_display_text(cog, []) is None
    assert FireStationCommand._equipment_display_text(cog, "hose") is None


def test_vehicle_requirement_display_text_uses_catalog_names_and_fallback_ids():
    cog = _cog_with_game_data(
        {
            "vehicles": {
                "vehicles": [
                    {"id": "engine_basic", "name": "Standard Fire Engine"},
                    {"id": "rescue_basic", "name": "Basic Rescue Truck"},
                ]
            }
        }
    )

    assert FireStationCommand._vehicle_requirement_display_text(
        cog,
        ["engine_basic", "missing_vehicle", "rescue_basic"],
    ) == "Standard Fire Engine, missing_vehicle, Basic Rescue Truck"
    assert FireStationCommand._vehicle_requirement_display_text(cog, []) is None
    assert FireStationCommand._vehicle_requirement_display_text(cog, "engine_basic") is None


def test_missing_required_vehicle_ids_compares_required_types_to_owned_catalog_ids():
    cog = _cog_with_game_data({})
    mission = {"required_vehicles": ["engine_basic", "rescue_basic"]}
    owned_vehicles = [
        {"id": 1, "catalog_id": "engine_basic", "name": "Standard Fire Engine"},
        {"id": 2, "catalog_id": "engine_basic", "name": "Second Fire Engine"},
    ]

    assert FireStationCommand._missing_required_vehicle_ids(cog, mission, owned_vehicles) == ["rescue_basic"]
    assert FireStationCommand._missing_required_vehicle_ids(cog, {"required_vehicles": []}, owned_vehicles) == []
    assert FireStationCommand._missing_required_vehicle_ids(cog, mission, "bad") == ["engine_basic", "rescue_basic"]


def test_scene_backup_helpers_use_dispatched_vehicle_selection():
    cog = _cog_with_game_data({})
    mission = {
        "required_vehicles": ["engine_basic", "rescue_basic"],
        "selected_vehicle_ids": [1],
        "backup_vehicle_ids": [3],
    }
    vehicles = [
        {"id": 1, "catalog_id": "engine_basic", "name": "Engine", "crew_capacity": 4},
        {"id": 2, "catalog_id": "rescue_basic", "name": "Rescue", "crew_capacity": 4},
        {"id": 3, "catalog_id": "ladder", "name": "Ladder", "crew_capacity": 2},
    ]
    selected = [vehicles[0]]

    assert FireStationCommand._missing_required_vehicle_ids_from_selection(cog, mission, selected) == [
        "rescue_basic"
    ]
    assert FireStationCommand._scene_backup_vehicle_requirements(cog, mission, selected) == [
        "rescue_basic"
    ]
    assert FireStationCommand._backup_candidate_vehicles(cog, vehicles, mission) == [vehicles[1]]


def test_handle_takeover_incident_transfers_turnout_to_new_user():
    original_user = type("User", (), {"id": 1001})()
    new_user = type("User", (), {"id": 2002})()
    guild = type("Guild", (), {"id": 3003})()
    channel = _Channel()
    original_mission = {
        "id": "shed_fire",
        "title": "Shed Fire",
        "stage": FireStationCommand.STAGE_STAFF_TURNOUT,
        "required_staff": 4,
        "base_credits": 750,
        "hint": "Caller reports a shed fire behind the house.",
        "detail": "Smoke is visible from the rear garden.",
        "dispatch_narrative": "Initial dispatch narrative.",
        "success_narrative": "Crews cool the shed and save the fence.",
        "partial_narrative": "The shed is lost, but exposures are protected.",
        "failure_narrative": "The fire reaches the house.",
        "required_vehicles": ["engine_basic"],
        "required_equipment": ["hose"],
        "base_xp": 45,
        "tier": 1,
        "recommended_level": 1,
        "capabilities": {"fire_suppression": 20},
    }
    users = {
        original_user.id: {"active_mission": original_mission},
        new_user.id: {
            "started": True,
            "staff_total": 6,
            "active_mission": {},
            "vehicles": [{"id": 1, "catalog_id": "engine_basic", "crew_capacity": 6}],
            "equipment": [{"catalog_id": "hose", "quantity": 1}],
            "trainings": [],
            "command_level": 1,
        },
    }
    cog = _cog_with_game_data(
        {
            "vehicles": {
                "vehicles": [
                    {
                        "id": "engine_basic",
                        "name": "Standard Fire Engine",
                        "required_staff": 4,
                        "capabilities": {"fire_suppression": 30},
                    }
                ]
            },
            "equipment": {"equipment": [{"id": "hose", "name": "Fire Hose Set"}]},
        }
    )
    cog.config = _MultiUserConfig(users, {"xp_per_mission_base": 50})
    interaction = _Interaction(new_user, channel=channel, guild=guild)

    asyncio.run(cog.handle_takeover_incident(interaction, channel, guild, original_user, new_user))

    assert users[original_user.id]["active_mission"] == {}
    new_mission = users[new_user.id]["active_mission"]
    assert new_mission["stage"] == FireStationCommand.STAGE_ALERT_CHOICE
    assert new_mission["title"] == "Shed Fire"
    assert new_mission["dispatch_narrative"] == cog._takeover_dispatch_narrative()
    assert new_mission["missing_required_vehicles"] == []
    assert new_mission["missing_required_equipment"] == []
    assert interaction.response.edited["embed"].kwargs["title"] == "Transferred incident: Shed Fire"
    assert isinstance(interaction.response.edited["view"], AlertChoiceView)


def test_turnout_takeover_timeout_clears_original_mission_and_disables_buttons():
    original_user = type("User", (), {"id": 1001})()
    channel = _Channel()
    users = {
        original_user.id: {
            "active_mission": {
                "id": "shed_fire",
                "title": "Shed Fire",
                "stage": FireStationCommand.STAGE_STAFF_TURNOUT,
            }
        }
    }
    cog = _cog_with_game_data({})
    cog.config = _MultiUserConfig(users, {})
    view = TurnoutTakeoverView(cog, channel, original_user)
    view.message = _Message()

    asyncio.run(view.on_timeout())

    assert users[original_user.id]["active_mission"] == {}
    assert view.message.edited["embed"].kwargs["title"] == "Dispatch abandoned"
    assert all(child.disabled for child in view.children)


def test_missing_required_equipment_ids_compares_required_types_to_owned_inventory():
    cog = _cog_with_game_data({})
    mission = {"required_equipment": ["hose", "rescue_tools"]}
    owned_equipment = [{"catalog_id": "hose", "quantity": 1}]

    assert FireStationCommand._missing_required_equipment_ids(cog, mission, owned_equipment) == ["rescue_tools"]
    assert FireStationCommand._missing_required_equipment_ids(cog, {"required_equipment": []}, owned_equipment) == []
    assert FireStationCommand._missing_required_equipment_ids(cog, mission, "bad") == ["hose", "rescue_tools"]


def test_missing_required_training_ids_uses_vehicle_and_equipment_requirements():
    cog = _cog_with_game_data(
        {
            "vehicles": {
                "vehicles": [
                    {"id": "rescue_basic", "name": "Basic Rescue Truck", "required_training": ["technical_rescue"]},
                ]
            },
            "equipment": {
                "equipment": [
                    {"id": "rescue_tools", "name": "Hydraulic Rescue Tools", "required_training": ["technical_rescue"]},
                    {"id": "ba_basic", "name": "Basic Breathing Apparatus", "required_training": ["breathing_apparatus"]},
                ]
            },
            "trainings": {
                "trainings": [
                    {"id": "technical_rescue", "name": "Technical Rescue"},
                    {"id": "breathing_apparatus", "name": "Breathing Apparatus"},
                ]
            },
        }
    )
    mission = {
        "required_vehicles": ["rescue_basic"],
        "required_equipment": ["rescue_tools", "ba_basic"],
    }

    assert FireStationCommand._missing_required_training_ids(cog, mission, {"trainings": []}) == [
        "technical_rescue",
        "breathing_apparatus",
    ]
    assert FireStationCommand._missing_required_training_ids(
        cog,
        mission,
        {"trainings": ["technical_rescue"]},
    ) == ["breathing_apparatus"]


def test_recruitment_embed_shows_hireable_staff():
    user = object()
    cog = _cog_with_game_data({})
    cog.config = _Config(
        {"station_level": 2, "staff_total": 6, "credits": 4500},
        {"staff_cost": 2000},
    )

    embed = asyncio.run(FireStationCommand._build_recruitment_embed(cog, user))

    assert embed.kwargs["title"] == "Recruitment desk"
    assert {"name": "Open positions", "value": "2", "inline": True} in embed.fields
    assert {"name": "Available actions", "value": "You can currently hire up to **2** staff.", "inline": False} in embed.fields


def test_recruitment_hire_max_caps_to_slots_and_credits():
    user = type("User", (), {"id": 123})()
    cog = _cog_with_game_data({})
    cog.config = _Config(
        {"station_level": 3, "staff_total": 6, "credits": 5000},
        {"staff_cost": 2000},
    )
    view = RecruitmentView(cog, user, object(), object())
    interaction = _Interaction(user)

    asyncio.run(view._confirm_hire(interaction, None))

    edited = interaction.response.edited
    assert edited["embed"].kwargs["title"] == "Confirm recruitment"
    assert edited["embed"].kwargs["description"] == "Hire **2** new staff for **4,000** credits?"
    assert edited["view"].amount == 2
    assert edited["view"].cost == 4000
    assert edited["view"].edit_message is True


def test_dashboard_upgrade_button_opens_confirm_view():
    user = type("User", (), {"id": 123})()
    cog = _cog_with_game_data({})
    cog.config = _Config(
        {
            "started": True,
            "station_level": 1,
            "command_level": 2,
            "xp": 100,
            "station_type": "volunteer",
            "staff_total": 6,
            "staff_trained": 0,
            "vehicles": [],
            "active_mission": {},
            "credits": 100000,
        },
        {"max_station_level": 5, "upgrade_base_cost": 50000},
    )
    view = FscDashboardView(cog, user, object(), object())
    interaction = _Interaction(user)

    asyncio.run(view.upgrade(interaction, None))

    edited = interaction.response.edited
    assert edited["embed"].kwargs["title"] == "Confirm station upgrade"
    assert isinstance(edited["view"], ConfirmUpgradeView)
    assert edited["view"].new_level == 2
    assert edited["view"].cost == 50000
    assert edited["view"].edit_message is True


def test_feature_availability_locks_early_facilities():
    cog = _cog_with_game_data({})
    data = {
        "started": True,
        "station_level": 1,
        "command_level": 1,
        "xp": 0,
        "station_type": "volunteer",
        "staff_total": 6,
        "staff_trained": 0,
        "vehicles": [],
        "expansions": [],
        "active_mission": {},
    }

    assert FireStationCommand._feature_available(cog, data, "training") is False
    assert FireStationCommand._feature_available(cog, data, "maintenance") is False
    assert FireStationCommand._feature_available(cog, data, "expansions") is False
    assert FireStationCommand._feature_available(cog, data, "career_conversion") is False


def test_feature_availability_shows_unlocked_facilities():
    cog = _cog_with_game_data({})
    data = {
        "started": True,
        "station_level": 5,
        "command_level": 5,
        "xp": 975,
        "station_type": "volunteer",
        "staff_total": 6,
        "staff_trained": 0,
        "vehicles": [],
        "expansions": [],
        "active_mission": {},
    }

    assert FireStationCommand._feature_available(cog, data, "training") is True
    assert FireStationCommand._feature_available(cog, data, "maintenance") is True
    assert FireStationCommand._feature_available(cog, data, "expansions") is True
    assert FireStationCommand._feature_available(cog, data, "career_conversion") is True


def test_dashboard_categories_and_actions_are_alphabetized():
    user = type("User", (), {"id": 123})()
    cog = _cog_with_game_data({})
    data = {
        "started": True,
        "station_level": 5,
        "command_level": 5,
        "xp": 975,
        "station_type": "volunteer",
        "staff_total": 6,
        "staff_trained": 0,
        "vehicles": [],
        "expansions": [],
        "active_mission": {},
    }
    view = FscDashboardView(cog, user, object(), object(), data=data)

    assert view._available_categories(data) == ["Incidents", "Staff", "Station", "Vehicle"]
    assert [view.ACTION_LABELS[action] for action in view._category_actions("Vehicle", data)] == [
        "Buy equipment",
        "Buy vehicle",
        "Maintenance bay",
    ]
    assert [view.ACTION_LABELS[action] for action in view._category_actions("Staff", data)] == [
        "Hire staff",
        "Train staff",
    ]
    assert [view.ACTION_LABELS[action] for action in view._category_actions("Station", data)] == [
        "Build expansions",
        "Career station",
        "Overview",
        "Upgrade station",
    ]


def test_dashboard_upgrade_button_explains_locked_level():
    user = type("User", (), {"id": 123})()
    cog = _cog_with_game_data({})
    cog.config = _Config(
        {
            "started": True,
            "station_level": 2,
            "command_level": 1,
            "xp": 0,
            "station_type": "volunteer",
            "staff_total": 6,
            "staff_trained": 0,
            "vehicles": [],
            "active_mission": {},
            "credits": 100000,
        },
        {"max_station_level": 5, "upgrade_base_cost": 50000},
    )
    view = FscDashboardView(cog, user, object(), object())
    interaction = _Interaction(user)

    asyncio.run(view.upgrade(interaction, None))

    edited = interaction.response.edited
    fields = {field["name"]: field["value"] for field in edited["embed"].fields}
    assert fields["Upgrade not available yet"].startswith(
        "Station level 3 unlocks at command level 3."
    )
    assert isinstance(edited["view"], FscDashboardView)


def test_dashboard_career_button_opens_confirm_view():
    user = type("User", (), {"id": 123})()
    cog = _cog_with_game_data({})
    cog.config = _Config(
        {
            "started": True,
            "station_level": 2,
            "station_type": "volunteer",
            "staff_total": 6,
            "staff_trained": 0,
            "vehicles": [],
            "active_mission": {},
            "credits": 300000,
        },
        {"career_convert_cost": 250000},
    )
    view = FscDashboardView(cog, user, object(), object())
    interaction = _Interaction(user)

    asyncio.run(view.career(interaction, None))

    edited = interaction.response.edited
    assert edited["embed"].kwargs["title"] == "Confirm career conversion"
    assert isinstance(edited["view"], ConfirmCareerView)
    assert edited["view"].cost == 250000
    assert edited["view"].edit_message is True


def test_vehicle_shop_select_edits_message_to_confirm_purchase():
    user = type("User", (), {"id": 123})()
    cog = _cog_with_game_data({})
    cog.VEHICLE_CATALOG = {
        "engine_basic": {
            "name": "Standard Fire Engine",
            "crew_capacity": 4,
            "price": 50000,
            "image": "Images/Vehicles/engine_basic.png",
        }
    }
    cog.config = _Config({"xp": 0, "command_level": 1}, {})
    select = VehicleShopSelect(cog, object(), user, object())
    select.values = ["engine_basic"]
    interaction = _Interaction(user)

    asyncio.run(select.callback(interaction))

    edited = interaction.response.edited
    assert edited["embed"].kwargs["title"] == "Confirm vehicle purchase"
    assert isinstance(edited["view"], ConfirmVehiclePurchaseView)
    assert edited["view"].vehicle_id == "engine_basic"
    assert edited["view"].edit_message is True


def test_vehicle_purchase_confirm_edits_message_and_stores_vehicle():
    user = type("User", (), {"id": 123})()
    user_data = {
        "started": True,
        "station_level": 2,
        "station_type": "volunteer",
        "staff_total": 6,
        "staff_trained": 0,
        "vehicles": [],
        "next_vehicle_id": 1,
        "active_mission": {},
        "credits": 100000,
    }
    cog = _cog_with_game_data({})
    cog.config = _Config(user_data, {})
    cog.VEHICLE_CATALOG = {
        "engine_basic": {
            "name": "Standard Fire Engine",
            "crew_capacity": 4,
            "price": 50000,
            "image": "Images/Vehicles/engine_basic.png",
        }
    }
    interaction = _Interaction(user)

    asyncio.run(
        cog._confirm_vehicle_purchase(
            interaction,
            object(),
            user,
            "engine_basic",
            edit_message=True,
            guild=object(),
        )
    )

    edited = interaction.response.edited
    assert edited["embed"].kwargs["title"] == "Vehicle purchased"
    assert isinstance(edited["view"], VehicleShopView)
    assert user_data["vehicles"] == [
            {
                "id": 1,
                "catalog_id": "engine_basic",
                "name": "Standard Fire Engine",
                "crew_capacity": 4,
                "image": "Images/Vehicles/engine_basic.png",
                "condition": 100,
            }
        ]
    assert user_data["next_vehicle_id"] == 2
    assert user_data["credits"] == 50000


def test_vehicle_purchase_confirm_button_disables_message_before_purchase():
    user = type("User", (), {"id": 123})()
    user_data = {
        "started": True,
        "station_level": 2,
        "command_level": 1,
        "xp": 0,
        "station_type": "volunteer",
        "staff_total": 6,
        "staff_trained": 0,
        "vehicles": [],
        "next_vehicle_id": 1,
        "active_mission": {},
        "credits": 100000,
    }
    cog = _cog_with_game_data({})
    cog.config = _Config(user_data, {})
    cog.VEHICLE_CATALOG = {
        "engine_basic": {
            "name": "Standard Fire Engine",
            "crew_capacity": 4,
            "price": 50000,
            "image": "Images/Vehicles/engine_basic.png",
            "unlock_level": 1,
        }
    }
    view = ConfirmVehiclePurchaseView(
        cog,
        object(),
        user,
        "engine_basic",
        guild=object(),
        edit_message=True,
    )
    interaction = _Interaction(user)

    asyncio.run(view.confirm(interaction, None))

    assert interaction.message.edited["view"] is view
    assert all(child.disabled for child in view.children)
    assert interaction.response.edited["embed"].kwargs["title"] == "Vehicle purchased"
    assert user_data["vehicles"][0]["catalog_id"] == "engine_basic"


def test_vehicle_shop_select_paginates_large_catalog():
    cog = _cog_with_game_data({})
    cog.VEHICLE_CATALOG = {
        f"vehicle_{idx}": {
            "name": f"Vehicle {idx}",
            "crew_capacity": 2,
            "price": 1000,
            "unlock_level": 1,
        }
        for idx in range(30)
    }

    first_page = VehicleShopSelect(cog, object(), object(), command_level=1, page=0)
    second_page = VehicleShopSelect(cog, object(), object(), command_level=1, page=1)

    assert len(first_page.options) == 25
    assert len(second_page.options) == 5
    assert second_page.placeholder == "Select a vehicle to buy (page 2)"


def test_equipment_shop_select_paginates_large_catalog():
    cog = _cog_with_game_data({})
    cog.EQUIPMENT_CATALOG = {
        f"equipment_{idx}": {
            "name": f"Equipment {idx}",
            "price": 1000,
            "unlock_level": 1,
        }
        for idx in range(30)
    }

    first_page = EquipmentShopSelect(cog, object(), object(), command_level=1, page=0)
    second_page = EquipmentShopSelect(cog, object(), object(), command_level=1, page=1)

    assert len(first_page.options) == 25
    assert len(second_page.options) == 5
    assert second_page.placeholder == "Select equipment to buy (page 2)"


def test_equipment_shop_embed_shows_owned_and_locked_equipment():
    cog = _cog_with_game_data(
        {
            "equipment": {
                "equipment": [
                    {"id": "hose", "name": "Fire Hose Set", "base_cost": 1500, "unlock_level": 1},
                    {
                        "id": "rescue_tools",
                        "name": "Hydraulic Rescue Tools",
                        "base_cost": 5000,
                        "unlock_level": 3,
                    },
                ]
            }
        }
    )

    embed = FireStationCommand._build_equipment_shop_embed(
        cog,
        {
            "xp": 0,
            "command_level": 1,
            "equipment": [{"catalog_id": "hose", "quantity": 2}],
        },
    )

    fields = {field["name"]: field["value"] for field in embed.fields}
    assert fields["Owned equipment"] == "- Fire Hose Set x2"
    assert fields["Locked equipment"] == "- Level 3: Hydraulic Rescue Tools"


def test_equipment_shop_embed_groups_locked_lists_for_readability():
    cog = _cog_with_game_data(
        {
            "equipment": {
                "equipment": [
                    {
                        "id": f"locked_{idx}",
                        "name": f"Locked Equipment {idx}",
                        "base_cost": 1000,
                        "unlock_level": 2 + (idx % 2),
                    }
                    for idx in range(12)
                ]
            }
        }
    )

    embed = FireStationCommand._build_equipment_shop_embed(
        cog,
        {
            "xp": 0,
            "command_level": 1,
            "equipment": [],
        },
    )

    fields = {field["name"]: field["value"] for field in embed.fields}
    assert fields["Locked equipment"].startswith("- Level 2:")
    assert "\n- Level 3:" in fields["Locked equipment"]
    assert "\n- +4 more" in fields["Locked equipment"]


def test_equipment_purchase_confirm_edits_message_and_stores_inventory():
    user = type("User", (), {"id": 123})()
    user_data = {
        "started": True,
        "station_level": 1,
        "command_level": 1,
        "xp": 0,
        "equipment": [{"catalog_id": "hose", "quantity": 1}],
        "credits": 10000,
    }
    cog = _cog_with_game_data(
        {
            "equipment": {
                "equipment": [
                    {"id": "hose", "name": "Fire Hose Set", "base_cost": 1500, "unlock_level": 1},
                ]
            }
        }
    )
    cog.config = _Config(user_data, {})
    interaction = _Interaction(user)

    asyncio.run(
        cog._confirm_equipment_purchase(
            interaction,
            object(),
            user,
            "hose",
            edit_message=True,
            guild=object(),
        )
    )

    edited = interaction.response.edited
    assert edited["embed"].kwargs["title"] == "Equipment purchased"
    assert isinstance(edited["view"], EquipmentShopView)
    assert user_data["equipment"] == [{"catalog_id": "hose", "quantity": 2}]
    assert user_data["credits"] == 8500


def test_training_embed_shows_completed_available_and_locked_training():
    cog = _cog_with_game_data(
        {
            "trainings": {
                "trainings": [
                    {"id": "basic_firefighting", "name": "Basic Firefighting", "cost": 1000, "unlock_level": 1},
                    {"id": "technical_rescue", "name": "Technical Rescue", "cost": 3000, "unlock_level": 2},
                    {"id": "breathing_apparatus", "name": "Breathing Apparatus", "cost": 2000, "unlock_level": 3},
                ]
            }
        }
    )

    embed = FireStationCommand._build_training_embed(
        cog,
        {
            "xp": 100,
            "command_level": 2,
            "trainings": ["basic_firefighting"],
        },
    )

    fields = {field["name"]: field["value"] for field in embed.fields}
    assert (
        fields["Training scope"]
        == "Station-wide certification. It applies to current and future staff and is not consumed per member."
    )
    assert fields["Completed training"] == "Basic Firefighting"
    assert fields["Available training"] == "Technical Rescue (3,000 cr)"
    assert fields["Locked training"] == "Breathing Apparatus (level 3)"


def test_training_purchase_confirm_edits_message_and_stores_training():
    user = type("User", (), {"id": 123})()
    user_data = {
        "started": True,
        "station_level": 2,
        "command_level": 2,
        "xp": 100,
        "trainings": ["basic_firefighting"],
        "credits": 10000,
    }
    cog = _cog_with_game_data(
        {
            "trainings": {
                "trainings": [
                    {"id": "technical_rescue", "name": "Technical Rescue", "cost": 3000, "unlock_level": 2},
                ]
            }
        }
    )
    cog.config = _Config(user_data, {})
    interaction = _Interaction(user)

    asyncio.run(
        cog._confirm_training_purchase(
            interaction,
            object(),
            user,
            "technical_rescue",
            edit_message=True,
            guild=object(),
        )
    )

    edited = interaction.response.edited
    assert edited["embed"].kwargs["title"] == "Training completed"
    fields = {field["name"]: field["value"] for field in edited["embed"].fields}
    assert fields["Training scope"] == "Permanent station certification for current and future staff."
    assert user_data["trainings"] == ["basic_firefighting", "technical_rescue"]
    assert user_data["credits"] == 7000


def test_expansion_effects_add_vehicle_capacity():
    cog = _cog_with_game_data(
        {
            "expansions": {
                "expansions": [
                    {
                        "id": "extra_bay",
                        "name": "Extra Vehicle Bay",
                        "base_cost": 30000,
                        "effects": {"extra_vehicle_slots": 1},
                    },
                ]
            }
        }
    )

    assert FireStationCommand._max_vehicles_for_data(
        cog,
        {"station_level": 2, "expansions": []},
    ) == 2
    assert FireStationCommand._max_vehicles_for_data(
        cog,
        {"station_level": 2, "expansions": ["extra_bay"]},
    ) == 3


def test_expansion_embed_shows_available_locked_and_capacity():
    cog = _cog_with_game_data(
        {
            "expansions": {
                "expansions": [
                    {
                        "id": "extra_bay",
                        "name": "Extra Vehicle Bay",
                        "description": "Adds one additional vehicle slot.",
                        "base_cost": 30000,
                        "unlock_level": 2,
                        "effects": {"extra_vehicle_slots": 1},
                    },
                    {
                        "id": "workshop",
                        "name": "Workshop",
                        "description": "Enables in-house vehicle repairs.",
                        "base_cost": 100000,
                        "unlock_level": 4,
                    },
                ]
            }
        }
    )

    embed = FireStationCommand._build_expansion_embed(
        cog,
        {
            "xp": 100,
            "command_level": 2,
            "station_level": 2,
            "vehicles": [{"id": 1}],
            "expansions": [],
        },
    )

    fields = {field["name"]: field["value"] for field in embed.fields}
    assert fields["Built expansions"] == "None"
    assert fields["Available expansions"] == "Extra Vehicle Bay (30,000 cr)"
    assert fields["Locked expansions"] == "Workshop (level 4)"
    assert fields["Vehicle capacity"] == "1 / 2"


def test_expansion_purchase_confirm_edits_message_and_stores_expansion():
    user = type("User", (), {"id": 123})()
    user_data = {
        "started": True,
        "station_level": 2,
        "command_level": 2,
        "xp": 100,
        "vehicles": [{"id": 1}],
        "expansions": [],
        "credits": 100000,
    }
    cog = _cog_with_game_data(
        {
            "expansions": {
                "expansions": [
                    {
                        "id": "extra_bay",
                        "name": "Extra Vehicle Bay",
                        "base_cost": 30000,
                        "unlock_level": 2,
                        "effects": {"extra_vehicle_slots": 1},
                    },
                ]
            }
        }
    )
    cog.config = _Config(user_data, {})
    interaction = _Interaction(user)

    asyncio.run(
        cog._confirm_expansion_purchase(
            interaction,
            object(),
            user,
            "extra_bay",
            edit_message=True,
            guild=object(),
        )
    )

    edited = interaction.response.edited
    assert edited["embed"].kwargs["title"] == "Expansion built"
    assert user_data["expansions"] == ["extra_bay"]
    assert user_data["credits"] == 70000


def test_expansion_requirements_lock_vehicle_and_equipment_until_built():
    cog = _cog_with_game_data(
        {
            "vehicles": {
                "vehicles": [
                    {
                        "id": "ambulance",
                        "name": "Ambulance",
                        "required_staff": 2,
                        "base_cost": 30000,
                        "unlock_level": 1,
                        "required_expansions": ["ems_bay"],
                    }
                ]
            },
            "equipment": {
                "equipment": [
                    {
                        "id": "ems_bag",
                        "name": "EMS Response Bag",
                        "base_cost": 2200,
                        "unlock_level": 1,
                        "required_expansions": ["ems_bay"],
                    }
                ]
            },
            "expansions": {
                "expansions": [
                    {
                        "id": "ems_bay",
                        "name": "Ambulance Bay",
                        "base_cost": 75000,
                        "unlock_level": 2,
                    }
                ]
            },
        }
    )
    locked_data = {"command_level": 2, "expansions": []}
    unlocked_data = {"command_level": 2, "expansions": ["ems_bay"]}

    assert not FireStationCommand._vehicle_is_unlocked(
        cog,
        cog.VEHICLE_CATALOG["ambulance"],
        2,
        locked_data,
    )
    assert FireStationCommand._vehicle_is_unlocked(
        cog,
        cog.VEHICLE_CATALOG["ambulance"],
        2,
        unlocked_data,
    )
    assert not FireStationCommand._equipment_is_unlocked(
        cog,
        cog.EQUIPMENT_CATALOG["ems_bag"],
        2,
        locked_data,
    )
    assert FireStationCommand._equipment_is_unlocked(
        cog,
        cog.EQUIPMENT_CATALOG["ems_bag"],
        2,
        unlocked_data,
    )


def test_mission_picker_skips_expansion_locked_incidents_when_core_is_available():
    cog = _cog_with_game_data({})
    cog.INCIDENTS = [
        {
            "id": "ambulance_call",
            "name": "Ambulance Call",
            "required_staff": 1,
            "base_credits": 100,
            "hint": "Patient needs help.",
            "detail": "Patient needs help.",
            "required_expansions": ["ems_bay"],
        },
        {
            "id": "bin_fire",
            "name": "Bin Fire",
            "required_staff": 1,
            "base_credits": 100,
            "hint": "Small fire.",
            "detail": "Small fire.",
        },
    ]

    picked = [
        FireStationCommand._pick_random_incident(
            cog,
            {"command_level": 10, "staff_total": 10, "vehicles": [], "equipment": [], "expansions": []},
        )["id"]
        for _ in range(20)
    ]

    assert set(picked) == {"bin_fire"}


def test_vehicle_condition_scales_station_capabilities():
    cog = _cog_with_game_data(
        {
            "vehicles": {
                "vehicles": [
                    {
                        "id": "engine_basic",
                        "name": "Standard Fire Engine",
                        "required_staff": 4,
                        "base_cost": 50000,
                        "capabilities": {"fire_suppression": 40},
                        "equipment_slots": ["hose"],
                    }
                ]
            },
            "equipment": {
                "equipment": [
                    {
                        "id": "hose",
                        "name": "Hose",
                        "base_cost": 500,
                        "capabilities": {"fire_suppression": 20},
                    }
                ]
            },
        }
    )

    capabilities = FireStationCommand._station_capabilities(
        cog,
        [{"id": 1, "catalog_id": "engine_basic", "condition": 50}],
        [{"catalog_id": "hose", "quantity": 1}],
    )

    assert capabilities["fire_suppression"] == 30


def test_out_of_service_vehicles_do_not_contribute_capabilities():
    cog = _cog_with_game_data(
        {
            "vehicles": {
                "vehicles": [
                    {
                        "id": "engine_basic",
                        "name": "Standard Fire Engine",
                        "required_staff": 4,
                        "base_cost": 50000,
                        "capabilities": {"fire_suppression": 40},
                    }
                ]
            },
        }
    )
    vehicle = {
        "id": 1,
        "catalog_id": "engine_basic",
        "condition": 100,
        "out_of_service_until": "2026-06-12T12:30:00Z",
    }

    assert FireStationCommand._available_vehicles(cog, [vehicle]) == []
    assert FireStationCommand._station_capabilities(cog, [vehicle]) == {}


def test_vehicle_wear_can_put_maintenance_unlocked_units_out_of_service(monkeypatch):
    cog = _cog_with_game_data(
        {
            "balance": {
                "balance": {
                    "maintenance_out_of_service_condition": 25,
                    "maintenance_out_of_service_minutes": 30,
                }
            }
        }
    )
    monkeypatch.setattr("FireStationCommand.fire_station_command.random.randint", lambda _low, _high: 10)

    vehicles = FireStationCommand._apply_vehicle_wear(
        cog,
        [{"id": 1, "catalog_id": "engine_basic", "condition": 30}],
        [1],
        "failure",
        data={"command_level": 4, "expansions": []},
    )

    assert vehicles[0]["condition"] == 20
    assert vehicles[0]["out_of_service_until"] == "2026-06-12T12:30:00Z"


def test_vehicle_wear_does_not_lock_early_players_without_maintenance(monkeypatch):
    cog = _cog_with_game_data({})
    monkeypatch.setattr("FireStationCommand.fire_station_command.random.randint", lambda _low, _high: 10)

    vehicles = FireStationCommand._apply_vehicle_wear(
        cog,
        [{"id": 1, "catalog_id": "engine_basic", "condition": 30}],
        [1],
        "failure",
        data={"command_level": 1, "expansions": []},
    )

    assert vehicles[0]["condition"] == 20
    assert "out_of_service_until" not in vehicles[0]


def test_maintenance_embed_lists_damaged_vehicles_and_cost():
    cog = _cog_with_game_data(
        {
            "balance": {"balance": {"maintenance_cost_multiplier": 1.0}},
            "vehicles": {
                "vehicles": [
                    {
                        "id": "engine_basic",
                        "name": "Standard Fire Engine",
                        "required_staff": 4,
                        "base_cost": 50000,
                        "maintenance_cost": 500,
                    }
                ]
            },
        }
    )

    embed = FireStationCommand._build_maintenance_embed(
        cog,
        {
            "vehicles": [
                {
                    "id": 1,
                    "catalog_id": "engine_basic",
                    "name": "Starter Fire Engine",
                    "condition": 80,
                }
            ],
        },
    )

    fields = {field["name"]: field["value"] for field in embed.fields}
    assert fields["Fleet condition"] == "Starter Fire Engine: 80% (100 cr)"
    assert fields["Repair estimate"] == "100 credits"


def test_maintenance_view_only_has_repair_and_back_buttons():
    assert hasattr(MaintenanceView, "repair")
    assert hasattr(MaintenanceView, "back")
    assert not hasattr(MaintenanceView, "previous_page")
    assert not hasattr(MaintenanceView, "next_page")


def test_repair_fleet_spends_credits_and_restores_condition():
    user = type("User", (), {"id": 123})()
    user_data = {
        "started": True,
        "credits": 1000,
        "vehicles": [
            {
                "id": 1,
                "catalog_id": "engine_basic",
                "name": "Starter Fire Engine",
                "condition": 80,
                "out_of_service_until": "2026-06-12T12:30:00Z",
            }
        ],
    }
    cog = _cog_with_game_data(
        {
            "balance": {"balance": {"maintenance_cost_multiplier": 1.0}},
            "vehicles": {
                "vehicles": [
                    {
                        "id": "engine_basic",
                        "name": "Standard Fire Engine",
                        "required_staff": 4,
                        "base_cost": 50000,
                        "maintenance_cost": 500,
                    }
                ]
            },
        }
    )
    cog.config = _Config(user_data, {})
    interaction = _Interaction(user)

    asyncio.run(
        cog._repair_fleet(
            interaction,
            object(),
            user,
            object(),
            edit_message=True,
        )
    )

    assert user_data["credits"] == 900
    assert user_data["vehicles"][0]["condition"] == 100
    assert "out_of_service_until" not in user_data["vehicles"][0]
    assert isinstance(interaction.response.edited["view"], MaintenanceView)
    assert interaction.response.edited["embed"].kwargs["title"] == "Maintenance bay"


def test_reputation_delta_uses_balance_config():
    cog = _cog_with_game_data(
        {
            "balance": {
                "balance": {
                    "reputation_gain_success": 4,
                    "reputation_loss_fail": 6,
                    "reputation_loss_skip": 2,
                }
            }
        }
    )

    assert FireStationCommand._reputation_delta_for_outcome(cog, "success") == 4
    assert FireStationCommand._reputation_delta_for_outcome(cog, "partial") == 0
    assert FireStationCommand._reputation_delta_for_outcome(cog, "failure") == -6
    assert FireStationCommand._reputation_delta_for_outcome(cog, "skip") == -2


def test_economy_scaled_cost_uses_current_balance_with_cap():
    cog = _cog_with_game_data(
        {
            "balance": {
                "balance": {
                    "economy_cost_scaling_threshold_multiplier": 10.0,
                    "economy_cost_scaling_rate": 0.08,
                    "economy_cost_scaling_max_multiplier": 1.5,
                }
            }
        }
    )

    assert FireStationCommand._economy_scaled_cost(cog, 1000, 9000) == 1000
    assert FireStationCommand._economy_scaled_cost(cog, 1000, 12000) == 1160
    assert FireStationCommand._economy_scaled_cost(cog, 1000, 50000) == 1500


def test_invalid_yaml_shapes_fall_back_to_static_catalog_and_incidents():
    cog = _cog_with_game_data({"missions": {"missions": "bad"}, "vehicles": {"vehicles": "bad"}})

    assert FireStationCommand._build_incidents(cog) == FireStationCommand._fallback_incidents()
    assert FireStationCommand._build_vehicle_catalog(cog) == FireStationCommand._fallback_vehicle_catalog()


def test_new_mission_state_includes_schema_and_initial_stage():
    cog = _cog_with_game_data({})
    incident = {
        "id": "small_bin_fire",
        "name": "Small Bin Fire",
        "required_staff": 4,
        "base_credits": 200,
        "image": "Images/Missions/small_bin_fire.png",
        "hint": "Quick response limits damage.",
        "detail": "A small bin fire in a residential area.",
        "dispatch_narrative": "Smoke is showing behind several homes.",
        "success_narrative": "The fire is knocked down quickly.",
        "partial_narrative": "The fire is controlled with minor extension.",
        "failure_narrative": "The fire spreads before crews gain control.",
        "required_vehicles": ["engine_basic"],
        "required_equipment": ["hose", "basic_tools"],
        "required_expansions": [],
        "base_xp": 40,
        "tier": 1,
        "recommended_level": 1,
        "capabilities": {"fire_suppression": 35},
    }

    mission = FireStationCommand._new_mission_state(
        cog,
        incident,
        channel_id=123,
        guild_id=456,
    )

    assert mission == {
        "schema_version": FireStationCommand.MISSION_SCHEMA_VERSION,
        "id": "small_bin_fire",
        "title": "Small Bin Fire",
        "required_staff": 4,
        "base_credits": 200,
        "image": "Images/Missions/small_bin_fire.png",
        "hint": "Quick response limits damage.",
        "detail": "A small bin fire in a residential area.",
        "dispatch_narrative": "Smoke is showing behind several homes.",
        "success_narrative": "The fire is knocked down quickly.",
        "partial_narrative": "The fire is controlled with minor extension.",
        "failure_narrative": "The fire spreads before crews gain control.",
        "required_vehicles": ["engine_basic"],
        "required_equipment": ["hose", "basic_tools"],
        "required_expansions": [],
        "base_xp": 40,
        "tier": 1,
        "recommended_level": 1,
        "capabilities": {"fire_suppression": 35},
        "stage": FireStationCommand.STAGE_ALERT_CHOICE,
        "alert_mode": None,
        "channel_id": 123,
        "guild_id": 456,
        "created_at": "2026-06-12T12:00:00Z",
        "updated_at": "2026-06-12T12:00:00Z",
        "next_action": None,
        "next_action_at": None,
    }


def test_set_mission_stage_preserves_schema_version():
    cog = _cog_with_game_data({})
    mission = {"stage": FireStationCommand.STAGE_ALERT_CHOICE}

    FireStationCommand._set_mission_stage(cog, mission, FireStationCommand.STAGE_TRAVEL)

    assert mission["schema_version"] == FireStationCommand.MISSION_SCHEMA_VERSION
    assert mission["stage"] == FireStationCommand.STAGE_TRAVEL
    assert mission["updated_at"] == "2026-06-12T12:00:00Z"
    assert FireStationCommand._mission_is_stage(cog, mission, FireStationCommand.STAGE_TRAVEL)


def test_set_and_clear_mission_due_tracks_next_action():
    cog = _cog_with_game_data({})
    mission = {"stage": FireStationCommand.STAGE_STAFF_TURNOUT}

    FireStationCommand._set_mission_due(
        cog,
        mission,
        FireStationCommand.ACTION_SHOW_TURNOUT_RESULT,
        minutes=2.5,
    )

    assert mission["next_action"] == FireStationCommand.ACTION_SHOW_TURNOUT_RESULT
    assert mission["next_action_at"] == "2026-06-12T12:02:30Z"
    assert mission["updated_at"] == "2026-06-12T12:00:00Z"

    FireStationCommand._clear_mission_due(cog, mission)

    assert mission["next_action"] is None
    assert mission["next_action_at"] is None
    assert mission["updated_at"] == "2026-06-12T12:00:00Z"


def test_mission_due_action_ready_handles_due_and_future_timestamps():
    cog = _cog_with_game_data({})

    assert FireStationCommand._mission_due_action_ready(
        cog,
        {
            "next_action": FireStationCommand.ACTION_SHOW_TURNOUT_RESULT,
            "next_action_at": "2026-06-12T11:59:00Z",
        },
    )
    assert not FireStationCommand._mission_due_action_ready(
        cog,
        {
            "next_action": FireStationCommand.ACTION_SHOW_TURNOUT_RESULT,
            "next_action_at": "2026-06-12T12:01:00Z",
        },
    )
    assert not FireStationCommand._mission_due_action_ready(
        cog,
        {
            "next_action": FireStationCommand.ACTION_SHOW_TURNOUT_RESULT,
            "next_action_at": "not-a-date",
        },
    )


def test_run_due_mission_action_dispatches_turnout_result():
    user = type("User", (), {"id": 123})()
    user_data = {
        "active_mission": {
            "next_action": FireStationCommand.ACTION_SHOW_TURNOUT_RESULT,
            "next_action_at": "2026-06-12T11:59:00Z",
        }
    }
    cog = _cog_with_game_data({})
    cog.config = _Config(user_data, {})
    calls = []

    async def fake_turnout(channel, actor):
        calls.append((channel, actor))

    cog._show_turnout_result = fake_turnout
    channel = object()

    assert asyncio.run(cog._run_due_mission_action(channel, user)) is True
    assert calls == [(channel, user)]


def test_run_due_mission_action_dispatches_backup_window_without_sleep():
    user = type("User", (), {"id": 123})()
    user_data = {
        "active_mission": {
            "next_action": FireStationCommand.ACTION_RESOLVE_BACKUP_WINDOW,
            "next_action_at": "2026-06-12T11:59:00Z",
        }
    }
    cog = _cog_with_game_data({})
    cog.config = _Config(user_data, {})
    calls = []

    async def fake_backup_window(channel, actor, *, sleep_after=True):
        calls.append((channel, actor, sleep_after))

    cog._resolve_backup_window = fake_backup_window
    channel = object()

    assert asyncio.run(cog._run_due_mission_action(channel, user)) is True
    assert calls == [(channel, user, False)]


def test_build_mission_control_embed_guides_alert_choice():
    cog = _cog_with_game_data({})
    mission = {
        "title": "Small Bin Fire",
        "required_staff": 4,
        "dispatch_narrative": "Smoke is showing behind several homes.",
        "stage": FireStationCommand.STAGE_ALERT_CHOICE,
    }

    embed = FireStationCommand._build_mission_control_embed(cog, mission)

    assert embed.kwargs["title"] == "Mission control - Small Bin Fire"
    assert embed.kwargs["description"] == "Smoke is showing behind several homes."
    assert embed.fields[0]["name"] == "Stage"
    assert embed.fields[0]["value"] == FireStationCommand.STAGE_ALERT_CHOICE
    assert embed.fields[2]["name"] == "Guidance"
    assert embed.fields[2]["value"] == "Choose how to alert your crew."


def test_build_mission_control_embed_shows_turnout_and_next_update():
    cog = _cog_with_game_data(
        {
            "vehicles": {
                "vehicles": [
                    {"id": "engine_basic", "name": "Standard Fire Engine"},
                    {"id": "rescue_basic", "name": "Basic Rescue Truck"},
                ]
            },
            "equipment": {
                "equipment": [
                    {"id": "hose", "name": "Fire Hose Set"},
                    {"id": "basic_tools", "name": "Basic Hand Tools"},
                ]
            }
        }
    )
    mission = {
        "title": "Traffic Collision",
        "required_staff": 6,
        "required_vehicles": ["engine_basic"],
        "required_equipment": ["hose", "basic_tools"],
        "missing_required_vehicles": ["rescue_basic"],
        "stage": FireStationCommand.STAGE_STAFF_TURNOUT,
        "turnout_total_arrived": 3,
        "turnout_available": 6,
        "next_action": FireStationCommand.ACTION_SHOW_TURNOUT_RESULT,
        "next_action_at": "2026-06-12T12:01:00Z",
    }

    embed = FireStationCommand._build_mission_control_embed(cog, mission)

    fields = {field["name"]: field["value"] for field in embed.fields}
    assert fields["Required vehicles"] == "Standard Fire Engine"
    assert fields["Required equipment"] == "Fire Hose Set, Basic Hand Tools"
    assert fields["Station readiness"] == "Missing vehicle types: Basic Rescue Truck"
    assert fields["Guidance"] == "Crew turnout is in progress. Refresh this panel after the expected turnout time."
    assert fields["Turnout"] == "3 / 6 arrived"
    assert fields["Next update"] == "2026-06-12T12:01:00Z"


def test_build_mission_control_embed_guides_scene_work():
    cog = _cog_with_game_data({})
    mission = {
        "title": "House Fire",
        "required_staff": 8,
        "stage": FireStationCommand.STAGE_SCENE_WORK,
        "next_action": FireStationCommand.ACTION_RESOLVE_INCIDENT,
        "next_action_at": "2026-06-12T12:04:00Z",
    }

    embed = FireStationCommand._build_mission_control_embed(cog, mission)

    fields = {field["name"]: field["value"] for field in embed.fields}
    assert fields["Stage"] == FireStationCommand.STAGE_SCENE_WORK
    assert fields["Guidance"] == "Crews are working on scene. Refresh this panel while waiting for the incident result."
    assert fields["Next update"] == "2026-06-12T12:04:00Z"


def test_build_mission_control_embed_guides_scene_backup():
    cog = _cog_with_game_data(
        {
            "vehicles": {
                "vehicles": [
                    {"id": "rescue_basic", "name": "Basic Rescue Truck"},
                ]
            }
        }
    )
    mission = {
        "title": "Warehouse Fire",
        "required_staff": 8,
        "stage": FireStationCommand.STAGE_SCENE_BACKUP,
        "selected_vehicle_ids": [1],
        "backup_required_vehicle_ids": ["rescue_basic"],
        "next_action": FireStationCommand.ACTION_RESOLVE_BACKUP_WINDOW,
        "next_action_at": "2026-06-12T12:04:00Z",
    }

    embed = FireStationCommand._build_mission_control_embed(cog, mission)

    fields = {field["name"]: field["value"] for field in embed.fields}
    assert fields["Stage"] == FireStationCommand.STAGE_SCENE_BACKUP
    assert fields["Guidance"].startswith("On-scene command is requesting more resources.")
    assert fields["Requested backup"] == "Basic Rescue Truck"
    assert fields["Next update"] == "2026-06-12T12:04:00Z"


def test_alert_narrative_includes_expected_turnout(monkeypatch):
    cog = _cog_with_game_data({})
    monkeypatch.setattr(
        "FireStationCommand.fire_station_command.random.choice",
        lambda options: options[0],
    )

    narrative = FireStationCommand._alert_narrative(cog, "emergency", "volunteer", 5.0)

    assert "pagers" in narrative
    assert "Turnout expected in 5 minutes." in narrative


def test_turnout_result_narrative_reflects_staffing_level():
    cog = _cog_with_game_data({})

    assert "confident first move" in FireStationCommand._turnout_result_narrative(cog, 4, 4, 6)
    assert "turnout is thin" in FireStationCommand._turnout_result_narrative(cog, 2, 4, 6)
    assert "No one makes it in" in FireStationCommand._turnout_result_narrative(cog, 0, 4, 6)


def test_realert_and_travel_narratives_include_eta(monkeypatch):
    cog = _cog_with_game_data({})
    monkeypatch.setattr(
        "FireStationCommand.fire_station_command.random.choice",
        lambda options: options[0],
    )

    assert "Additional turnout expected in 2 minutes." in FireStationCommand._realert_narrative(
        cog, 2.0
    )
    assert "ETA in 3 minutes." in FireStationCommand._travel_narrative(cog, 3.0)
