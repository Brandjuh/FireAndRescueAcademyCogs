import unittest

from eventmanager.event_manager import (
    build_payload,
    normalize_kind,
    parse_event_form,
    parse_profile_names,
    select_scheduled_profile,
    summarize_form,
    valid_time,
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

        self.assertEqual(payload["authenticity_token"], "abc123")
        self.assertEqual(payload["mission_alliance[caption]"], "FARA Daily Mission")
        self.assertEqual(payload["mission_alliance[latitude]"], "41.0")
        self.assertEqual(payload["mission_alliance[longitude]"], "-73.9")
        self.assertEqual(payload["mission_alliance[mission_type_id]"], "2")
        self.assertEqual(payload["commit"], "Start")

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
