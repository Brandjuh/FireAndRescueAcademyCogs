import unittest

from eventmanager.event_manager import (
    build_payload,
    fields_for_selection,
    field_options_for_kind,
    normalize_kind,
    normalize_random_location_region,
    parse_event_form,
    parse_location_or_random_region,
    parse_location_value,
    parse_profile_names,
    profile_fields_for_start,
    profile_name_from_label,
    random_location_for_region,
    safe_debug_mapping,
    safe_debug_payload,
    select_scheduled_profile,
    summarize_payload_for_debug,
    summarize_response_for_debug,
    summarize_form,
    valid_time,
    EventManager,
    LATITUDE_FIELD,
    LONGITUDE_FIELD,
    ADDRESS_FIELD,
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

    def test_fields_for_selection_accepts_manual_large_coordinates_and_address(self):
        profile = fields_for_selection("large", "41", latitude="40.1", longitude="-73.9", address="Manual NYC")

        self.assertNotIn("random_location", profile)
        self.assertEqual(profile["fields"]["mission_position[mission_type_id]"], "41")
        self.assertEqual(profile["fields"]["mission_position[latitude]"], "40.1")
        self.assertEqual(profile["fields"]["mission_position[longitude]"], "-73.9")
        self.assertEqual(profile["fields"]["mission_position[address]"], "Manual NYC")

    def test_large_payload_adds_missionchief_position_defaults(self):
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
        self.assertEqual(payload["mission_position[shape]"], "")

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

    def test_select_scheduled_profile_rotates_profiles(self):
        profile, next_index = select_scheduled_profile(
            {"profiles": ["alpha", "bravo"], "rotation_index": 1}
        )

        self.assertEqual(profile, "bravo")
        self.assertEqual(next_index, 0)


class EventManagerAddressTests(unittest.IsolatedAsyncioTestCase):
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

    def test_safe_debug_helpers_redact_tokens(self):
        mapping = safe_debug_mapping({"X-CSRF-Token": "secret", "Origin": "https://www.missionchief.com"})
        payload = safe_debug_payload([("authenticity_token", "secret"), ("mission_position[address]", "NYC")])

        self.assertIn("X-CSRF-Token: REDACTED", mapping)
        self.assertIn("Origin: https://www.missionchief.com", mapping)
        self.assertIn("authenticity_token=REDACTED", payload)
        self.assertIn("mission_position[address]=NYC", payload)
        self.assertNotIn("secret", mapping.replace("REDACTED", ""))
        self.assertNotIn("secret", payload.replace("REDACTED", ""))


if __name__ == "__main__":
    unittest.main()
