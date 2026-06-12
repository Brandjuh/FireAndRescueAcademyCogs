from datetime import datetime, timezone

import discord

from FireStationCommand.fire_station_command import FireStationCommand


def _cog_with_game_data(game_data):
    cog = object.__new__(FireStationCommand)
    cog.game_data = game_data
    cog.vehicle_definitions = FireStationCommand._build_vehicle_definitions(cog)
    cog._utcnow = lambda: datetime(2026, 6, 12, 12, 0, tzinfo=timezone.utc)
    return cog


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
        }
    }


def test_build_incidents_derives_staff_from_required_vehicles():
    cog = _cog_with_game_data(
        {
            "missions": {
                "missions": [
                    {
                        "id": "small_bin_fire",
                        "name": "Small Bin Fire",
                        "base_credits": 200,
                        "image": "Images/Missions/small_bin_fire.png",
                        "required_vehicles": ["engine_basic"],
                        "required_equipment": ["hose"],
                        "description": "A small bin fire in a residential area.",
                        "dispatch_narrative": "Smoke is showing behind several homes.",
                        "scene_narrative": "The crew finds fire spreading along a fence.",
                        "success_narrative": "The fire is knocked down quickly.",
                        "partial_narrative": "The fire is controlled with minor extension.",
                        "failure_narrative": "The fire spreads before crews gain control.",
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
            "image": "Images/Missions/small_bin_fire.png",
            "hint": "A small bin fire in a residential area.",
            "detail": "The crew finds fire spreading along a fence.",
            "dispatch_narrative": "Smoke is showing behind several homes.",
            "success_narrative": "The fire is knocked down quickly.",
            "partial_narrative": "The fire is controlled with minor extension.",
            "failure_narrative": "The fire spreads before crews gain control.",
            "required_vehicles": ["engine_basic"],
            "required_equipment": ["hose"],
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
    cog = _cog_with_game_data({})
    mission = {
        "title": "Traffic Collision",
        "required_staff": 6,
        "stage": FireStationCommand.STAGE_STAFF_TURNOUT,
        "turnout_total_arrived": 3,
        "turnout_available": 6,
        "next_action": FireStationCommand.ACTION_SHOW_TURNOUT_RESULT,
        "next_action_at": "2026-06-12T12:01:00Z",
    }

    embed = FireStationCommand._build_mission_control_embed(cog, mission)

    fields = {field["name"]: field["value"] for field in embed.fields}
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
