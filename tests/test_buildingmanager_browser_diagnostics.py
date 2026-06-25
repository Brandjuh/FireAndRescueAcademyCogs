import unittest

from buildingmanager.buildingmanager import (
    BUILDING_CREATE_SCRIPT,
    _normalize_missionchief_url,
    build_alliance_building_config,
    build_browser_diagnostics_report,
)


class BuildingManagerBrowserDiagnosticsTests(unittest.TestCase):
    def test_normalize_missionchief_url_defaults_to_new_building_page(self):
        self.assertEqual(_normalize_missionchief_url(None), "https://www.missionchief.com/buildings/new")

    def test_normalize_missionchief_url_accepts_relative_path(self):
        self.assertEqual(
            _normalize_missionchief_url("/buildings/new"),
            "https://www.missionchief.com/buildings/new",
        )

    def test_normalize_missionchief_url_rejects_other_hosts(self):
        with self.assertRaises(ValueError):
            _normalize_missionchief_url("https://example.com/buildings/new")

    def test_build_alliance_building_config_allows_hospital(self):
        config = build_alliance_building_config(
            building_type="Hospital",
            building_name="Example Hospital",
            coordinates="40.123456789, -73.987654321",
            address="Example Street, New York",
        )

        self.assertEqual(config["buildingTypeId"], "2")
        self.assertEqual(config["name"], "Example Hospital")
        self.assertEqual(config["latitude"], "40.1234568")
        self.assertEqual(config["longitude"], "-73.9876543")

    def test_build_alliance_building_config_allows_prison(self):
        config = build_alliance_building_config(
            building_type="Prison",
            building_name="Example Prison",
            coordinates="40.1, -73.9",
            address=None,
        )

        self.assertEqual(config["buildingTypeId"], "10")

    def test_build_alliance_building_config_rejects_other_types(self):
        with self.assertRaises(ValueError):
            build_alliance_building_config(
                building_type="Fire Station",
                building_name="Example",
                coordinates="40.1, -73.9",
                address=None,
            )

    def test_build_alliance_building_config_rejects_invalid_coordinates(self):
        with self.assertRaises(ValueError):
            build_alliance_building_config(
                building_type="Hospital",
                building_name="Example",
                coordinates="not coordinates",
                address=None,
            )

    def test_create_script_requires_alliance_context_and_refuses_coins(self):
        self.assertIn("build_as_alliance", BUILDING_CREATE_SCRIPT)
        self.assertIn("build_with_coins", BUILDING_CREATE_SCRIPT)
        self.assertIn("build as alliance building", BUILDING_CREATE_SCRIPT)
        self.assertIn('text.includes("credits")', BUILDING_CREATE_SCRIPT)
        self.assertIn('!text.includes("coins")', BUILDING_CREATE_SCRIPT)

    def test_browser_diagnostics_report_formats_controls_and_redacted_fields(self):
        report = build_browser_diagnostics_report(
            {
                "url": "https://www.missionchief.com",
                "title": "MissionChief",
                "headings": ["Build alliance building"],
                "candidates": [
                    {
                        "index": 3,
                        "tag": "a",
                        "type": "",
                        "text": "Build alliance hospital",
                        "href": "https://www.missionchief.com/buildings/new",
                    }
                ],
                "forms": [
                    {
                        "index": 0,
                        "action": "https://www.missionchief.com/buildings",
                        "method": "post",
                        "id": "new_building",
                        "className": "",
                        "text": "Create building",
                        "fields": [
                            {"name": "authenticity_token", "type": "hidden", "value": "REDACTED"},
                            {"name": "building[latitude]", "type": "hidden", "value": "40.1"},
                        ],
                    }
                ],
            }
        )

        self.assertIn("NO FORM WAS SUBMITTED", report)
        self.assertIn("Build alliance hospital", report)
        self.assertIn("authenticity_token", report)
        self.assertIn("REDACTED", report)
        self.assertNotIn("secret-token", report)


if __name__ == "__main__":
    unittest.main()
