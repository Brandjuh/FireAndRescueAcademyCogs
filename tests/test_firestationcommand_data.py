from FireStationCommand.fire_station_command import FireStationCommand


def _cog_with_game_data(game_data):
    cog = object.__new__(FireStationCommand)
    cog.game_data = game_data
    cog.vehicle_definitions = FireStationCommand._build_vehicle_definitions(cog)
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
