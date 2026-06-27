import unittest
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from eventmanager.event_manager import (
    BROWSER_CLICK_START_SCRIPT,
    BROWSER_CAPTURE_SCRIPT,
    BROWSER_PREPARE_START_SCRIPT,
    build_browser_start_config,
    build_payload,
    EVENT_DEFAULT_OVERRIDES,
    DEFAULT_EVENT_ROUTE_TIME,
    DEFAULT_TIMEZONE,
    EVENT_ROUTE_LOCATIONS,
    EVENT_RADIO_FIELD,
    fields_for_selection,
    field_options_for_kind,
    find_type_option,
    MISSION_TYPE_FIELD,
    normalize_kind,
    normalize_optional_profile_arg,
    normalize_random_location_region,
    migrate_default_event_schedule_time,
    next_free_start_from_text,
    next_schedule_attempt_time,
    parse_event_form,
    parse_last_free_mission_time,
    parse_location_or_random_region,
    parse_location_value,
    parse_profile_names,
    profile_fields_for_start,
    profile_name_from_label,
    profile_location_summary,
    profile_start_summary,
    profile_type_summary,
    profile_with_selected_type,
    random_location_for_region,
    RANDOM_TYPE_KEY,
    TYPE_SEARCH_KEY,
    route_profile_for_location,
    route_locations_for_kind,
    route_profile_names,
    safe_debug_mapping,
    safe_debug_payload,
    schedule_run_key,
    select_scheduled_profile,
    summarize_browser_snapshot,
    summarize_payload_for_debug,
    summarize_response_for_debug,
    summarize_form,
    valid_time,
    EventManager,
    LATITUDE_FIELD,
    LONGITUDE_FIELD,
    ADDRESS_FIELD,
    build_browser_event_start_script,
    _ajax_get_headers,
    _ajax_submit_headers,
    _form_position_params,
    _replace_payload_value,
    _validate_free_submit,
)


FORM_HTML = """
<html>
  <body>
    <form action="/missionAlliance" method="post">
      <input type="hidden" name="authenticity_token" value="abc123" />
      <input type="text" name="mission_alliance[caption]" value="" required />
      <input type="text" name="mission_alliance[latitude]" value="40.1" />
      <input type="text" name="mission_alliance[longitude]" value="-73.9" />
      <select name="mission_alliance[mission_type_id]">
        <option value="1">Storm</option>
        <option value="2" selected>Factory Fire</option>
      </select>
      <textarea name="mission_alliance[description]">Default notes</textarea>
      <input type="submit" name="commit" value="Start" />
    </form>
  </body>
</html>
"""

RADIO_HTML = """
<html>
  <body>
    <form action="/missionAllianceCreate" method="post">
      <input type="hidden" name="authenticity_token" value="abc123" />
      <input type="radio" name="mission_position[mission_type_id]" value="41" />
      <input type="radio" name="mission_position[mission_type_id]" value="61" checked />
      <input type="radio" name="mission_position[mission_type_id]" value="62" />
      <input type="checkbox" name="event_radio_group" value="fire" checked />
      <input type="checkbox" name="event_radio_group" value="police" />
      <input type="submit" name="commit" value="Start 1 mission (Free)" />
    </form>
  </body>
</html>
"""

MISSIONCHIEF_LARGE_HTML = """
<form action="/missionAllianceCreate" method="post">
  <input name="utf8" type="hidden" value="&#x2713;" />
  <input name="authenticity_token" type="hidden" value="abc123" />
  <label><input checked="checked" name="mission_position[mission_type_id]" type="radio" value="41" />Major fire</label>
  <label><input name="mission_position[mission_type_id]" type="radio" value="61" />Unannounced demonstration</label>
  <label><input name="mission_position[mission_type_id]" type="radio" value="-1" />Own mission</label>
  <div id="custom_mission_creator">
    <input name="mission_position[mission_custom][caption]" type="text" />
    <input name="mission_position[mission_custom][mission_custom_values][need_lf]" value="0" />
  </div>
  <input name="mission_position[latitude]" type="hidden" />
  <input name="mission_position[longitude]" type="hidden" />
  <input name="mission_position[size]" type="hidden" value="1" />
  <input type="submit" name="commit" value="Start 1 mission (Free)" />
</form>
"""

MISSIONCHIEF_EVENT_HTML = """
<form action="/missionAllianceEventCreate" method="post">
  <input name="utf8" type="hidden" value="&#x2713;" />
  <input name="authenticity_token" type="hidden" value="abc123" />
  <input id="event_identifier" name="event_identifier" type="hidden" value="" />
  <label class="radio">
    <input type="radio" name="event_radio_group" id="event_0" data-event-id="0" />
    Storm
  </label>
  <label class="radio">
    <input type="radio" name="event_radio_group" id="event_1" data-event-id="1" />
    Civil Unrest
  </label>
  <input id="hidden_mission_type_id" name="mission_position[mission_type_id]" type="hidden" value="" />
  <input name="mission_position[latitude]" type="hidden" />
  <input name="mission_position[longitude]" type="hidden" />
  <input name="mission_position[duration]" type="hidden" value="3" />
  <input type="checkbox" name="event_precondition_0_fire_investigation_count" value="" checked />
  <input type="submit" name="commit" value="Start Event ( Free )" />
</form>
"""

DISABLED_FREE_SUBMIT_HTML = """
<form action="/missionAllianceCreate" method="post">
  <input name="authenticity_token" type="hidden" value="abc123" />
  <input type="submit" name="commit" value="Start 1 mission (Free)" disabled />
  <input type="submit" name="commit" value="Start 1 mission (10 Coins)" />
</form>
"""

BUTTON_SUBMIT_HTML = """
<form action="/missionAllianceEventCreate" method="post">
  <input name="authenticity_token" type="hidden" value="abc123" />
  <button type="submit" name="commit">Start Event ( Free )</button>
</form>
"""


class FakeResponse:
    def __init__(self, text: str, status: int = 200):
        self._text = text
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self._text


class FakeSession:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.requests = []

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return self.response


class FakeEventManagerConfig:
    def __init__(self, schedules, profiles, last_runs=None, retry_after=None, last_started_at=None):
        self._schedules = schedules
        self._profiles = profiles
        self._last_runs = last_runs or {}
        self._retry_after = retry_after or {}
        self._last_started_at = last_started_at or {}

    async def schedules(self):
        return self._schedules

    async def profiles(self):
        return self._profiles

    async def last_runs(self):
        return self._last_runs

    async def schedule_retry_after(self):
        return self._retry_after

    async def last_started_at(self):
        return self._last_started_at


class EventManagerFormTests(unittest.TestCase):
    def test_parse_event_form_extracts_fields_options_and_submit(self):
        form = parse_event_form(FORM_HTML, "https://www.missionchief.com/missionAllianceNew")

        self.assertEqual(form.action, "https://www.missionchief.com/missionAlliance")
        self.assertEqual(form.method, "post")
        self.assertEqual(form.submit_name, "commit")
        self.assertEqual(form.submit_value, "Start")
        field_names = [field.name for field in form.fields]
        self.assertIn("authenticity_token", field_names)
        self.assertIn("mission_alliance[caption]", field_names)
        mission_type = next(field for field in form.fields if field.name == "mission_alliance[mission_type_id]")
        self.assertEqual(mission_type.value, "2")
        self.assertEqual(mission_type.options[1].label, "Factory Fire")

    def test_build_payload_uses_form_defaults_and_profile_overrides(self):
        form = parse_event_form(FORM_HTML, "https://www.missionchief.com/missionAllianceNew")

        payload = build_payload(
            form,
            {
                "mission_alliance[caption]": "FARA Daily Mission",
                "mission_alliance[latitude]": "41.0",
            },
        )
        payload_dict = dict(payload)

        self.assertEqual(payload_dict["authenticity_token"], "abc123")
        self.assertEqual(payload_dict["mission_alliance[caption]"], "FARA Daily Mission")
        self.assertEqual(payload_dict["mission_alliance[latitude]"], "41.0")
        self.assertEqual(payload_dict["mission_alliance[longitude]"], "-73.9")
        self.assertEqual(payload_dict["mission_alliance[mission_type_id]"], "2")
        self.assertEqual(payload_dict["commit"], "Start")

    def test_radio_inputs_are_grouped_and_only_selected_value_is_posted(self):
        form = parse_event_form(RADIO_HTML, "https://www.missionchief.com/missionAllianceNew")

        mission_type = next(field for field in form.fields if field.name == "mission_position[mission_type_id]")
        self.assertEqual(mission_type.field_type, "radio")
        self.assertEqual(mission_type.value, "61")
        self.assertEqual([option.value for option in mission_type.options], ["41", "61", "62"])

        payload = build_payload(form, {"mission_position[mission_type_id]": "62"})
        self.assertEqual(
            [item for item in payload if item[0] == "mission_position[mission_type_id]"],
            [("mission_position[mission_type_id]", "62")],
        )

    def test_checked_checkbox_values_are_preserved(self):
        form = parse_event_form(RADIO_HTML, "https://www.missionchief.com/missionAllianceNew")

        payload = build_payload(form, {})

        self.assertEqual(
            [item for item in payload if item[0] == "event_radio_group"],
            [("event_radio_group", "fire")],
        )

    def test_missionchief_large_form_ignores_custom_missions(self):
        form = parse_event_form(MISSIONCHIEF_LARGE_HTML, "https://www.missionchief.com/missionAllianceNew")

        field_names = {field.name for field in form.fields}
        self.assertNotIn("mission_position[mission_custom][caption]", field_names)
        mission_type = next(field for field in form.fields if field.name == "mission_position[mission_type_id]")
        self.assertEqual(mission_type.value, "41")
        self.assertEqual(
            [(option.value, option.label) for option in mission_type.options],
            [("41", "Major fire"), ("61", "Unannounced demonstration")],
        )

    def test_missionchief_event_data_event_id_sets_hidden_mission_type(self):
        form = parse_event_form(MISSIONCHIEF_EVENT_HTML, "https://www.missionchief.com/missionAllianceEventNew")

        event_group = next(field for field in form.fields if field.name == "event_radio_group")
        self.assertEqual(
            [(option.value, option.label) for option in event_group.options],
            [("0", "Storm"), ("1", "Civil Unrest")],
        )

        payload = build_payload(form, {"event_radio_group": "1"})

        self.assertIn(("event_radio_group", "1"), payload)
        self.assertIn(("mission_position[mission_type_id]", "1"), payload)

    def test_event_payload_applies_standard_area_shape_and_call_volume(self):
        form = parse_event_form(MISSIONCHIEF_EVENT_HTML, "https://www.missionchief.com/missionAllianceEventNew")

        payload = dict(build_payload(form, {"event_radio_group": "1"}))

        self.assertEqual(payload["mission_position[size]"], "2")
        self.assertEqual(payload["mission_position[shape]"], "circle")
        self.assertEqual(payload["mission_position[amount]"], "0")

    def test_event_payload_preserves_extension_precondition_defaults(self):
        form = parse_event_form(MISSIONCHIEF_EVENT_HTML, "https://www.missionchief.com/missionAllianceEventNew")

        payload = build_payload(form, {"event_radio_group": "1"})

        self.assertIn(("event_precondition_0_fire_investigation_count", ""), payload)

    def test_free_submit_validation_rejects_coin_button(self):
        form = parse_event_form(MISSIONCHIEF_LARGE_HTML, "https://www.missionchief.com/missionAllianceNew")
        form.submit_value = "Start 1 mission (10 Coins)"

        payload = build_payload(form, {})

        self.assertIn("non-free", _validate_free_submit(form, payload))

    def test_free_submit_validation_rejects_nonzero_coins(self):
        form = parse_event_form(MISSIONCHIEF_LARGE_HTML, "https://www.missionchief.com/missionAllianceNew")

        payload = build_payload(form, {"mission_position[coins]": "10"})

        self.assertIn("spend coins", _validate_free_submit(form, payload))

    def test_disabled_free_submit_is_not_used(self):
        form = parse_event_form(DISABLED_FREE_SUBMIT_HTML, "https://www.missionchief.com/missionAllianceNew")

        payload = build_payload(form, {})

        self.assertEqual(form.submit_value, "Start 1 mission (10 Coins)")
        self.assertIn("non-free", _validate_free_submit(form, payload))

    def test_button_submit_is_supported(self):
        form = parse_event_form(BUTTON_SUBMIT_HTML, "https://www.missionchief.com/missionAllianceEventNew")

        payload = build_payload(form, {})

        self.assertEqual(form.submit_name, "commit")
        self.assertEqual(form.submit_value, "Start Event ( Free )")
        self.assertIn(("commit", "Start Event ( Free )"), payload)

    def test_parse_location_value_accepts_latitude_longitude_only(self):
        self.assertEqual(parse_location_value("40.7128, -74.0060"), ("40.7128", "-74.006"))
        with self.assertRaises(ValueError):
            parse_location_value("New York")

    def test_parse_location_or_random_region_accepts_regions(self):
        self.assertEqual(parse_location_or_random_region("nyc"), (None, None, "nyc"))
        self.assertEqual(parse_location_or_random_region("40.1, -73.9"), ("40.1", "-73.9", None))
        self.assertEqual(normalize_random_location_region("Bermuda Islands"), "bermuda")

    def test_random_location_for_region_returns_fixed_supported_coordinates(self):
        latitude, longitude, address, region = random_location_for_region("nyc_or_bermuda")

        self.assertEqual(region, "nyc")
        self.assertEqual(latitude, "40.729500")
        self.assertEqual(longitude, "-73.997200")
        self.assertEqual(address, "70 Washington Square South, 10012 New York, Manhattan")

    def test_profile_fields_for_start_resolves_random_location_and_sets_address(self):
        fields = profile_fields_for_start(
            {
                "random_location": "nyc",
                "fields": {
                    "mission_position[address]": "old address",
                    "mission_position[mission_type_id]": "41",
                },
            },
        )

        self.assertIn("mission_position[address]", fields)
        self.assertEqual(fields["mission_position[mission_type_id]"], "41")
        self.assertIn("mission_position[latitude]", fields)
        self.assertIn("mission_position[longitude]", fields)

    def test_profile_name_from_label_is_stable(self):
        self.assertEqual(profile_name_from_label("Major fire", prefix="large_"), "large_major_fire")
        self.assertEqual(profile_name_from_label("Storm Surge"), "storm_surge")

    def test_field_options_for_kind_reads_large_and_event_options(self):
        large_form = parse_event_form(MISSIONCHIEF_LARGE_HTML, "https://www.missionchief.com/missionAllianceNew")
        event_form = parse_event_form(MISSIONCHIEF_EVENT_HTML, "https://www.missionchief.com/missionAllianceEventNew")

        self.assertEqual([option.label for option in field_options_for_kind(large_form, "large")], ["Major fire", "Unannounced demonstration"])
        self.assertEqual([option.label for option in field_options_for_kind(event_form, "event")], ["Storm", "Civil Unrest"])

    def test_fields_for_selection_applies_event_defaults_and_random_region(self):
        profile = fields_for_selection("event", "1", random_region="nyc_or_bermuda")

        self.assertEqual(profile["random_location"], "nyc_or_bermuda")
        self.assertEqual(profile["fields"]["event_radio_group"], "1")
        self.assertEqual(profile["fields"]["mission_position[mission_type_id]"], "1")
        self.assertEqual(profile["fields"]["mission_position[size]"], "2")
        self.assertEqual(profile["fields"]["mission_position[shape]"], "circle")
        self.assertEqual(profile["fields"]["mission_position[amount]"], "0")

    def test_build_browser_start_config_resolves_event_profile(self):
        profile = fields_for_selection("event", "1", random_region="nyc_or_bermuda")

        config = build_browser_start_config("event", profile, label="weekly", allow_coins=False)

        self.assertEqual(config["kind"], "event")
        self.assertEqual(config["label"], "weekly")
        self.assertFalse(config["allowCoins"])
        self.assertEqual(config["eventValue"], "1")
        self.assertEqual(config["missionType"], "1")
        self.assertEqual(config["latitude"], "40.729500")
        self.assertEqual(config["longitude"], "-73.997200")
        self.assertEqual(config["size"], "2")
        self.assertEqual(config["shape"], "circle")
        self.assertEqual(config["amount"], "0")

    def test_route_profiles_use_fixed_locations_and_random_live_types(self):
        names = route_profile_names("event")
        large_names = route_profile_names("large")

        self.assertEqual(len(names), 10)
        self.assertEqual(len(large_names), 11)
        self.assertEqual(names[0], "route_new_york_city")
        self.assertEqual(names[-1], "route_beersheba_israel")
        self.assertIn("route_yakima_wildfire_wa", large_names)
        self.assertNotIn("route_yakima_wildfire_wa", names)

        large_profile = route_profile_for_location("large", EVENT_ROUTE_LOCATIONS[0])
        event_profile = route_profile_for_location("event", EVENT_ROUTE_LOCATIONS[0])

        self.assertTrue(large_profile[RANDOM_TYPE_KEY])
        self.assertTrue(event_profile[RANDOM_TYPE_KEY])
        self.assertEqual(large_profile["fields"]["mission_position[latitude]"], "40.712800")
        self.assertEqual(event_profile["fields"]["mission_position[size]"], EVENT_DEFAULT_OVERRIDES["mission_position[size]"])
        self.assertEqual(event_profile["fields"]["mission_position[shape]"], "circle")
        self.assertEqual(event_profile["fields"]["mission_position[amount]"], "0")

    def test_profile_start_summary_shows_next_route_location_and_random_type(self):
        profile = route_profile_for_location("event", EVENT_ROUTE_LOCATIONS[1])

        summary = profile_start_summary("event", profile)

        self.assertIn("Location: Portland, OR, USA", summary)
        self.assertIn("Type: Surprise Alliance event type", summary)

    def test_profile_type_summary_shows_resolved_event_type_label(self):
        profile = route_profile_for_location("event", EVENT_ROUTE_LOCATIONS[0])
        form = parse_event_form(MISSIONCHIEF_EVENT_HTML, "https://www.missionchief.com/missionAllianceEventNew")
        option = next(option for option in field_options_for_kind(form, "event") if option.label == "Civil Unrest")

        resolved = profile_with_selected_type("event", profile, option)

        self.assertEqual(profile_type_summary("event", resolved), "Civil Unrest")
        self.assertEqual(profile_location_summary(resolved), "New York City, NY, USA")

    def test_profile_with_selected_type_resolves_random_event_type(self):
        profile = route_profile_for_location("event", EVENT_ROUTE_LOCATIONS[0])
        form = parse_event_form(MISSIONCHIEF_EVENT_HTML, "https://www.missionchief.com/missionAllianceEventNew")
        option = next(option for option in field_options_for_kind(form, "event") if option.label == "Civil Unrest")

        resolved = profile_with_selected_type("event", profile, option)

        self.assertNotIn(RANDOM_TYPE_KEY, resolved)
        self.assertEqual(resolved["fields"][EVENT_RADIO_FIELD], "1")
        self.assertEqual(resolved["fields"][MISSION_TYPE_FIELD], "1")
        self.assertEqual(resolved["selected_type_label"], "Civil Unrest")

    def test_yakima_wildfire_profile_is_large_only_with_type_search(self):
        yakima = next(location for location in route_locations_for_kind("large") if location["label"] == "Yakima Wildfire, WA")

        profile = route_profile_for_location("large", yakima)

        self.assertEqual(profile[TYPE_SEARCH_KEY], "Wildfire")
        self.assertNotIn(RANDOM_TYPE_KEY, profile)
        self.assertIn("Yakima", profile["fields"][ADDRESS_FIELD])
        self.assertEqual(profile_type_summary("large", profile), "Wildfire")

    def test_find_type_option_matches_wildfire_without_hardcoded_id(self):
        options = [
            type("Option", (), {"value": "41", "label": "Major fire"})(),
            type("Option", (), {"value": "999", "label": "Wildfire"})(),
            type("Option", (), {"value": "61", "label": "Unannounced demonstration"})(),
        ]

        selected = find_type_option(options, "wild fire")

        self.assertIsNotNone(selected)
        self.assertEqual(selected.value, "999")
        self.assertEqual(selected.label, "Wildfire")

    def test_build_browser_start_config_resolves_large_profile(self):
        profile = fields_for_selection("large", "41", random_region="nyc")

        config = build_browser_start_config("large", profile, label="daily", allow_coins=True)

        self.assertEqual(config["kind"], "large")
        self.assertTrue(config["allowCoins"])
        self.assertEqual(config["missionType"], "41")
        self.assertEqual(config["latitude"], "40.729500")
        self.assertEqual(config["longitude"], "-73.997200")
        self.assertEqual(config["amount"], "1")

    def test_build_browser_start_config_requires_coordinates(self):
        with self.assertRaises(ValueError):
            build_browser_start_config(
                "event",
                {"fields": {"event_radio_group": "1", "mission_position[mission_type_id]": "1"}},
                label="broken",
            )

    def test_fields_for_selection_accepts_manual_large_coordinates_and_address(self):
        profile = fields_for_selection("large", "41", latitude="40.1", longitude="-73.9", address="Manual NYC")

        self.assertNotIn("random_location", profile)
        self.assertEqual(profile["fields"]["mission_position[mission_type_id]"], "41")
        self.assertEqual(profile["fields"]["mission_position[latitude]"], "40.1")
        self.assertEqual(profile["fields"]["mission_position[longitude]"], "-73.9")
        self.assertEqual(profile["fields"]["mission_position[address]"], "Manual NYC")

    def test_large_payload_matches_browser_marker_defaults(self):
        form = parse_event_form(MISSIONCHIEF_LARGE_HTML, "https://www.missionchief.com/missionAllianceNew")
        profile = fields_for_selection("large", "41", random_region="nyc")

        payload = dict(build_payload(form, profile_fields_for_start(profile)))

        self.assertEqual(payload["mission_position[mission_type_id]"], "41")
        self.assertEqual(payload["mission_position[latitude]"], "40.729500")
        self.assertEqual(payload["mission_position[longitude]"], "-73.997200")
        self.assertEqual(payload["mission_position[address]"], "70 Washington Square South, 10012 New York, Manhattan")
        self.assertEqual(payload["mission_position[poi_type]"], "0")
        self.assertEqual(payload["mission_position[size]"], "1")
        self.assertEqual(payload["mission_position[amount]"], "1")
        self.assertEqual(payload["mission_position[coins]"], "0")
        self.assertEqual(payload["mission_position[shape]"], "circle")

    def test_safe_payload_summary_excludes_authenticity_token(self):
        summary = summarize_payload_for_debug(
            [
                ("authenticity_token", "secret"),
                ("mission_position[latitude]", "40.729500"),
                ("mission_position[address]", "70 Washington Square South"),
                ("commit", "Start 1 mission (Free)"),
            ]
        )

        self.assertNotIn("secret", summary)
        self.assertNotIn("authenticity_token", summary)
        self.assertIn("mission_position[latitude]=40.729500", summary)
        self.assertIn("commit=Start 1 mission (Free)", summary)

    def test_response_summary_strips_html_and_redacts_tokens(self):
        summary = summarize_response_for_debug(
            "<html><script>secret()</script><body>Server error authenticity_token=abc123 failed</body></html>"
        )

        self.assertIn("Server error", summary)
        self.assertIn("authenticity_token=REDACTED", summary)
        self.assertNotIn("abc123", summary)
        self.assertNotIn("<body>", summary)

    def test_summarize_form_includes_option_preview(self):
        form = parse_event_form(FORM_HTML, "https://www.missionchief.com/missionAllianceNew")

        summary = summarize_form(form)

        self.assertIn("Action: https://www.missionchief.com/missionAlliance", summary)
        self.assertIn("mission_alliance[mission_type_id]", summary)
        self.assertIn("1:Storm", summary)
        self.assertIn("authenticity_token (input:hidden) = REDACTED", summary)
        self.assertNotIn("abc123", summary)

    def test_normalize_kind_accepts_aliases(self):
        self.assertEqual(normalize_kind("mission"), "large")
        self.assertEqual(normalize_kind("weekly"), "event")

    def test_valid_time_rejects_invalid_values(self):
        self.assertEqual(valid_time("23:55"), (23, 55))
        with self.assertRaises(ValueError):
            valid_time("25:00")

    def test_parse_profile_names_accepts_commas_and_spaces(self):
        self.assertEqual(
            parse_profile_names("daily, storm backup"),
            ["daily", "storm", "backup"],
        )

    def test_normalize_optional_profile_arg_ignores_placeholders(self):
        self.assertIsNone(normalize_optional_profile_arg(None))
        self.assertIsNone(normalize_optional_profile_arg(""))
        self.assertIsNone(normalize_optional_profile_arg("[profile]"))
        self.assertIsNone(normalize_optional_profile_arg("<profile>"))
        self.assertIsNone(normalize_optional_profile_arg("profile"))
        self.assertEqual(normalize_optional_profile_arg(" Storm_Surge "), "storm_surge")

    def test_select_scheduled_profile_rotates_profiles(self):
        profile, next_index = select_scheduled_profile(
            {"profiles": ["alpha", "bravo"], "rotation_index": 1}
        )

        self.assertEqual(profile, "bravo")
        self.assertEqual(next_index, 0)

    def test_last_free_time_parses_missionchief_cooldown_text(self):
        parsed = parse_last_free_mission_time("Last free mission: Sat, 20 Jun 2026 14:09:10 -0400")

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.astimezone(timezone.utc).isoformat(), "2026-06-20T18:09:10+00:00")

    def test_next_free_start_includes_grace_period(self):
        next_free = next_free_start_from_text("event", "Last free mission: Sat, 20 Jun 2026 14:09:10 -0400")

        self.assertIsNotNone(next_free)
        self.assertEqual(next_free.isoformat(), "2026-06-27T14:10:25-04:00")

    def test_next_weekly_schedule_attempt_retries_same_week_after_cooldown(self):
        now = datetime(2026, 6, 27, 15, 0, 30, tzinfo=ZoneInfo("America/New_York"))
        schedule = {
            "enabled": True,
            "profiles": ["weekly"],
            "rotation_index": 0,
            "time": DEFAULT_EVENT_ROUTE_TIME,
            "timezone": "America/New_York",
            "weekday": "saturday",
        }
        retry_after = {"event": "2026-06-27T19:01:15+00:00"}

        next_attempt = next_schedule_attempt_time("event", schedule, {}, retry_after, now)

        self.assertEqual(next_attempt.isoformat(), "2026-06-27T15:01:15-04:00")

    def test_due_weekly_schedule_attempt_is_now_before_run_key_is_written(self):
        now = datetime(2026, 6, 27, 15, 0, 30, tzinfo=ZoneInfo("America/New_York"))
        schedule = {
            "enabled": True,
            "profiles": ["weekly"],
            "rotation_index": 0,
            "time": DEFAULT_EVENT_ROUTE_TIME,
            "timezone": "America/New_York",
            "weekday": "saturday",
        }

        next_attempt = next_schedule_attempt_time("event", schedule, {}, {}, now)

        self.assertEqual(next_attempt, now)
        self.assertEqual(schedule_run_key("event", now), "2026-W26")

    def test_weekly_schedule_uses_actual_last_start_as_next_anchor(self):
        now = datetime(2026, 6, 27, 15, 0, 30, tzinfo=ZoneInfo("America/New_York"))
        schedule = {
            "enabled": True,
            "profiles": ["weekly"],
            "rotation_index": 0,
            "time": DEFAULT_EVENT_ROUTE_TIME,
            "timezone": "America/New_York",
            "weekday": "saturday",
        }
        last_started_at = {"event": "2026-06-20T19:08:30+00:00"}

        next_attempt = next_schedule_attempt_time("event", schedule, {}, {}, now, last_started_at)

        self.assertEqual(next_attempt.isoformat(), "2026-06-27T15:08:30-04:00")

    def test_weekly_schedule_is_due_when_actual_last_start_interval_has_passed(self):
        now = datetime(2026, 6, 27, 15, 9, 0, tzinfo=ZoneInfo("America/New_York"))
        schedule = {
            "enabled": True,
            "profiles": ["weekly"],
            "rotation_index": 0,
            "time": DEFAULT_EVENT_ROUTE_TIME,
            "timezone": "America/New_York",
            "weekday": "saturday",
        }
        last_started_at = {"event": "2026-06-20T19:08:30+00:00"}

        next_attempt = next_schedule_attempt_time("event", schedule, {}, {}, now, last_started_at)

        self.assertEqual(next_attempt, now)

    def test_daily_schedule_uses_actual_last_start_as_next_anchor(self):
        now = datetime(2026, 6, 27, 14, 0, 0, tzinfo=ZoneInfo("America/New_York"))
        schedule = {
            "enabled": True,
            "profiles": ["daily"],
            "rotation_index": 0,
            "time": "07:00",
            "timezone": "America/New_York",
            "weekday": None,
        }
        last_started_at = {"large": "2026-06-26T19:08:30+00:00"}

        next_attempt = next_schedule_attempt_time("large", schedule, {}, {}, now, last_started_at)

        self.assertEqual(next_attempt.isoformat(), "2026-06-27T15:08:30-04:00")

    def test_migrates_legacy_weekly_event_schedule_to_1500_new_york(self):
        schedules = {
            "event": {
                "enabled": True,
                "profiles": ["weekly"],
                "rotation_index": 0,
                "time": "07:00",
                "timezone": DEFAULT_TIMEZONE,
                "weekday": "saturday",
            }
        }

        changed = migrate_default_event_schedule_time(schedules)

        self.assertTrue(changed)
        self.assertEqual(schedules["event"]["time"], DEFAULT_EVENT_ROUTE_TIME)
        self.assertEqual(schedules["event"]["timezone"], DEFAULT_TIMEZONE)


class EventManagerAddressTests(unittest.IsolatedAsyncioTestCase):
    async def test_next_scheduled_profile_summary_uses_profile_after_current(self):
        profile_names = route_profile_names("event")
        event_locations = route_locations_for_kind("event")
        fake = type("FakeEventManager", (), {})()
        fake.config = FakeEventManagerConfig(
            schedules={
                "event": {
                    "enabled": True,
                    "profiles": profile_names,
                    "rotation_index": 0,
                }
            },
            profiles={
                "event": {
                    route_profile_names("event")[index]: route_profile_for_location("event", location)
                    for index, location in enumerate(event_locations)
                }
            },
        )

        summary = await EventManager._next_scheduled_profile_summary(fake, "event", "route_new_york_city")

        self.assertIn("Location: Portland, OR, USA", summary)
        self.assertIn("Type: Surprise Alliance event type", summary)

    async def test_notification_context_exposes_next_route_profile(self):
        profile_names = route_profile_names("large")
        large_locations = route_locations_for_kind("large")
        fake = type("FakeEventManager", (), {})()
        fake.config = FakeEventManagerConfig(
            schedules={
                "large": {
                    "enabled": True,
                    "profiles": profile_names,
                    "rotation_index": 0,
                }
            },
            profiles={
                "large": {
                    route_profile_names("large")[index]: route_profile_for_location("large", location)
                    for index, location in enumerate(large_locations)
                }
            },
        )
        fake._notification_contexts = {}

        async def next_summary(kind, profile_name):
            return await EventManager._next_scheduled_profile_summary(fake, kind, profile_name)

        fake._next_scheduled_profile_summary = next_summary

        await EventManager._remember_notification_context(
            fake,
            "large",
            "route_new_york_city",
            route_profile_for_location("large", EVENT_ROUTE_LOCATIONS[0]),
        )
        summary = await EventManager.get_next_notification_summary(fake, "large")

        self.assertIn("Location: Portland, OR, USA", summary)
        self.assertIn("Type: Surprise Large scale alliance mission type", summary)

    async def test_next_notification_details_include_location_type_and_schedule_time(self):
        profile_names = route_profile_names("event")
        event_locations = route_locations_for_kind("event")
        fake = type("FakeEventManager", (), {})()
        fake.config = FakeEventManagerConfig(
            schedules={
                "event": {
                    "enabled": True,
                    "profiles": profile_names,
                    "rotation_index": 1,
                    "time": DEFAULT_EVENT_ROUTE_TIME,
                    "timezone": DEFAULT_TIMEZONE,
                    "weekday": "saturday",
                }
            },
            profiles={
                "event": {
                    route_profile_names("event")[index]: route_profile_for_location("event", location)
                    for index, location in enumerate(event_locations)
                }
            },
            last_started_at={"event": "2099-06-20T19:08:30+00:00"},
        )
        fake._notification_contexts = {}

        details = await EventManager.get_next_notification_details(fake, "event")

        self.assertEqual(details["location"], "Portland, OR, USA")
        self.assertEqual(details["type"], "Surprise Alliance event type")
        self.assertEqual(details["scheduled_at"].isoformat(), "2099-06-27T15:08:30-04:00")

    async def test_reverse_address_replaces_payload_address(self):
        session = FakeSession(FakeResponse("MissionChief Address"))
        payload = [
            (LATITUDE_FIELD, "40.729500"),
            (LONGITUDE_FIELD, "-73.997200"),
            (ADDRESS_FIELD, "Fallback Address"),
        ]

        updated = await EventManager._resolve_reverse_address(None, session, "large", payload)

        self.assertIn((ADDRESS_FIELD, "MissionChief Address"), updated)
        self.assertNotIn((ADDRESS_FIELD, "Fallback Address"), updated)
        self.assertEqual(session.requests[0][1]["params"], {"latitude": "40.729500", "longitude": "-73.997200"})

    async def test_reverse_address_keeps_payload_when_response_is_empty(self):
        session = FakeSession(FakeResponse(""))
        payload = [
            (LATITUDE_FIELD, "40.729500"),
            (LONGITUDE_FIELD, "-73.997200"),
            (ADDRESS_FIELD, "Fallback Address"),
        ]

        updated = await EventManager._resolve_reverse_address(None, session, "large", payload)

        self.assertIn((ADDRESS_FIELD, "Fallback Address"), updated)

    def test_replace_payload_value_deduplicates_existing_values(self):
        payload = [(ADDRESS_FIELD, "old"), (ADDRESS_FIELD, "older")]

        self.assertEqual(_replace_payload_value(payload, ADDRESS_FIELD, "new"), [(ADDRESS_FIELD, "new")])

    def test_form_position_params_uses_latitude_and_longitude(self):
        self.assertEqual(
            _form_position_params({LATITUDE_FIELD: "40.729500", LONGITUDE_FIELD: "-73.997200"}),
            {"tlat": "40.729500", "tlng": "-73.997200"},
        )
        self.assertEqual(_form_position_params({LATITUDE_FIELD: "40.729500"}), {})

    def test_ajax_submit_headers_include_xhr_and_csrf_token(self):
        headers = _ajax_submit_headers("large", [("authenticity_token", "csrf-secret")])

        self.assertEqual(headers["X-Requested-With"], "XMLHttpRequest")
        self.assertEqual(headers["X-CSRF-Token"], "csrf-secret")
        self.assertEqual(headers["Origin"], "https://www.missionchief.com")
        self.assertIn("text/javascript", headers["Accept"])

    def test_ajax_get_headers_include_xhr(self):
        headers = _ajax_get_headers("large")

        self.assertEqual(headers["X-Requested-With"], "XMLHttpRequest")
        self.assertEqual(headers["Referer"], "https://www.missionchief.com")
        self.assertIn("text/javascript", headers["Accept"])

    def test_safe_debug_helpers_redact_tokens(self):
        mapping = safe_debug_mapping({"X-CSRF-Token": "secret", "Origin": "https://www.missionchief.com"})
        payload = safe_debug_payload([("authenticity_token", "secret"), ("mission_position[address]", "NYC")])

        self.assertIn("X-CSRF-Token: REDACTED", mapping)
        self.assertIn("Origin: https://www.missionchief.com", mapping)
        self.assertIn("authenticity_token=REDACTED", payload)
        self.assertIn("mission_position[address]=NYC", payload)
        self.assertNotIn("secret", mapping.replace("REDACTED", ""))
        self.assertNotIn("secret", payload.replace("REDACTED", ""))

    def test_browser_capture_script_does_not_submit(self):
        self.assertIn("No submit was sent", BROWSER_CAPTURE_SCRIPT)
        self.assertNotIn(".submit(", BROWSER_CAPTURE_SCRIPT)
        self.assertNotIn("fetch(", BROWSER_CAPTURE_SCRIPT)
        self.assertNotIn("XMLHttpRequest", BROWSER_CAPTURE_SCRIPT)

    def test_browser_event_start_script_uses_dom_and_free_button_only(self):
        script = build_browser_event_start_script(
            {
                "authenticity_token": "secret",
                "event_radio_group": "2",
                "mission_position[mission_type_id]": "2",
                "mission_position[latitude]": "40.729500",
                "mission_position[longitude]": "-73.997200",
                "mission_position[address]": "70 Washington Square South, 10012 New York, Manhattan",
                "mission_position[size]": "2",
                "mission_position[shape]": "circle",
                "mission_position[amount]": "0",
                "mission_position[coins]": "0",
            },
            label="storm surge",
        )

        self.assertIn("/missionAllianceEventNew", script)
        self.assertIn("missionAllianceEventCreate", script)
        self.assertIn('"allowCoins": false', script)
        self.assertIn("startButton.click()", script)
        self.assertIn("Refusing to click coin action", script)
        self.assertIn('"event_radio_group": "2"', script)
        self.assertIn('"mission_position[shape]": "circle"', script)
        self.assertIn("70 Washington Square South", script)
        self.assertNotIn("secret", script)
        self.assertNotIn("authenticity_token", script)
        self.assertNotIn("event_identifier", script)
        self.assertNotIn("fetch(", script)
        self.assertNotIn("XMLHttpRequest", script)

    def test_browser_event_start_script_can_explicitly_allow_coins(self):
        script = build_browser_event_start_script(
            {
                "event_radio_group": "2",
                "mission_position[mission_type_id]": "2",
                "mission_position[coins]": "20",
            },
            label="storm surge",
            allow_coins=True,
        )

        self.assertIn('"allowCoins": true', script)
        self.assertIn('"mission_position[coins]": "20"', script)
        self.assertIn("SPEND COINS", script)
        self.assertIn("startButton.click()", script)
        self.assertNotIn("fetch(", script)
        self.assertNotIn("XMLHttpRequest", script)

    def test_playwright_prepare_script_moves_missionchief_marker(self):
        self.assertIn("mission_position_new_marker", BROWSER_PREPARE_START_SCRIPT)
        self.assertIn("mission_position_new_dragend", BROWSER_PREPARE_START_SCRIPT)
        self.assertIn("updateAddress", BROWSER_PREPARE_START_SCRIPT)
        self.assertIn("No enabled", BROWSER_PREPARE_START_SCRIPT)
        self.assertNotIn("fetch(", BROWSER_PREPARE_START_SCRIPT)
        self.assertNotIn("XMLHttpRequest", BROWSER_PREPARE_START_SCRIPT)

    def test_playwright_prepare_script_treats_missing_coins_field_as_free(self):
        self.assertIn('const coinValue = fieldValue("mission_position[coins]") || "0";', BROWSER_PREPARE_START_SCRIPT)
        self.assertIn('coinValue !== "0"', BROWSER_PREPARE_START_SCRIPT)
        self.assertNotIn('fieldValue("mission_position[coins]") !== "0"', BROWSER_PREPARE_START_SCRIPT)

    def test_playwright_click_script_only_clicks_selected_submit_button(self):
        self.assertIn("submitIndex", BROWSER_CLICK_START_SCRIPT)
        self.assertIn("button.click()", BROWSER_CLICK_START_SCRIPT)
        self.assertNotIn("fetch(", BROWSER_CLICK_START_SCRIPT)
        self.assertNotIn("XMLHttpRequest", BROWSER_CLICK_START_SCRIPT)

    def test_summarize_browser_snapshot_includes_buttons_without_tokens(self):
        summary = summarize_browser_snapshot(
            {
                "url": "https://www.missionchief.com/",
                "missionType": "41",
                "latitude": "40.1",
                "longitude": "-73.9",
                "address": "NYC",
                "coins": "0",
                "submitButtons": [{"text": "Start Event ( Free )", "disabled": True}],
            }
        )

        self.assertIn("missionType: 41", summary)
        self.assertIn("Start Event", summary)
        self.assertNotIn("authenticity_token", summary)


if __name__ == "__main__":
    unittest.main()
