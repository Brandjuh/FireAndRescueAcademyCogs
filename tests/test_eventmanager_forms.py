import unittest

from eventmanager.event_manager import (
    build_payload,
    normalize_kind,
    parse_event_form,
    parse_location_value,
    parse_profile_names,
    select_scheduled_profile,
    summarize_form,
    valid_time,
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
  <input type="submit" name="commit" value="Start Event ( Free )" />
</form>
"""


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

    def test_free_submit_validation_rejects_coin_button(self):
        form = parse_event_form(MISSIONCHIEF_LARGE_HTML, "https://www.missionchief.com/missionAllianceNew")
        form.submit_value = "Start 1 mission (10 Coins)"

        payload = build_payload(form, {})

        self.assertIn("non-free", _validate_free_submit(form, payload))

    def test_free_submit_validation_rejects_nonzero_coins(self):
        form = parse_event_form(MISSIONCHIEF_LARGE_HTML, "https://www.missionchief.com/missionAllianceNew")

        payload = build_payload(form, {"mission_position[coins]": "10"})

        self.assertIn("spend coins", _validate_free_submit(form, payload))

    def test_parse_location_value_accepts_latitude_longitude_only(self):
        self.assertEqual(parse_location_value("40.7128, -74.0060"), ("40.7128", "-74.006"))
        with self.assertRaises(ValueError):
            parse_location_value("New York")

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


if __name__ == "__main__":
    unittest.main()
