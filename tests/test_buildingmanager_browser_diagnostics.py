import unittest

from buildingmanager.buildingmanager import (
    _normalize_missionchief_url,
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
