from datetime import datetime, timezone

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
                        "required_vehicles": ["engine_basic"],
                        "required_equipment": ["hose"],
                        "description": "A small bin fire in a residential area.",
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
            "hint": "A small bin fire in a residential area.",
            "detail": "A small bin fire in a residential area.",
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
        "hint": "Quick response limits damage.",
        "detail": "A small bin fire in a residential area.",
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
        "hint": "Quick response limits damage.",
        "detail": "A small bin fire in a residential area.",
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
