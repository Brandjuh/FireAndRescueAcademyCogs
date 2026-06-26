import tempfile
import unittest

from buildingmanager.buildingmanager import (
    BUILDING_AUTOMATION_MAX_EXTENSION_STARTS_PER_RUN,
    BUILDING_AUTOMATION_PREPARE_SCRIPT,
    BUILDING_CREATE_SCRIPT,
    BUILDING_FETCH_API_SCRIPT,
    BuildingAutomationResult,
    BuildingDatabase,
    _normalize_missionchief_url,
    build_alliance_building_config,
    build_browser_diagnostics_report,
    extract_missionchief_building_id,
    find_created_alliance_building_id,
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
        self.assertIn("build_another", BUILDING_CREATE_SCRIPT)
        self.assertIn("buildAnother.checked = false", BUILDING_CREATE_SCRIPT)
        self.assertIn("build as alliance building", BUILDING_CREATE_SCRIPT)
        self.assertIn('text.includes("credits")', BUILDING_CREATE_SCRIPT)
        self.assertIn('!text.includes("coins")', BUILDING_CREATE_SCRIPT)

    def test_fetch_api_script_uses_same_browser_session(self):
        self.assertIn("/api/buildings", BUILDING_FETCH_API_SCRIPT)
        self.assertIn('credentials: "same-origin"', BUILDING_FETCH_API_SCRIPT)

    def test_extract_building_id_from_urls_and_snapshots(self):
        self.assertEqual(
            extract_missionchief_building_id("https://www.missionchief.com/buildings/123456"),
            123456,
        )
        self.assertEqual(
            extract_missionchief_building_id({"finalUrl": "https://www.missionchief.com/buildings/987"}),
            987,
        )
        self.assertIsNone(extract_missionchief_building_id("https://www.missionchief.com/buildings"))

    def test_find_created_building_id_matches_type_and_coordinates(self):
        config = build_alliance_building_config(
            building_type="Hospital",
            building_name="Example Hospital",
            coordinates="40.1234567, -73.9876543",
            address="Example Street",
        )
        buildings = [
            {
                "id": 100,
                "caption": "Wrong Type",
                "building_type": 10,
                "latitude": 40.1234567,
                "longitude": -73.9876543,
            },
            {
                "id": 101,
                "caption": "Wrong Coordinates",
                "building_type": 2,
                "latitude": 41.1234567,
                "longitude": -73.9876543,
            },
            {
                "id": 102,
                "caption": "Example Hospital",
                "building_type": 2,
                "latitude": 40.12345671,
                "longitude": -73.98765431,
            },
        ]

        self.assertEqual(find_created_alliance_building_id(buildings, config), 102)

    def test_find_created_building_id_prefers_name_then_highest_id(self):
        config = build_alliance_building_config(
            building_type="Prison",
            building_name="Target Prison",
            coordinates="40.1, -73.9",
            address=None,
        )
        buildings = [
            {"id": 200, "caption": "Other Prison", "building_type": 10, "latitude": "40.1", "longitude": "-73.9"},
            {"id": 201, "caption": "Target Prison", "building_type": 10, "latitude": "40.1", "longitude": "-73.9"},
        ]

        self.assertEqual(find_created_alliance_building_id(buildings, config), 201)

    def test_automation_script_refuses_coins_and_excludes_large_buildings(self):
        self.assertIn("coin|coins", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertIn("large hospital", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertIn("large prison", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertIn("maxExtensionStarts", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertEqual(BUILDING_AUTOMATION_MAX_EXTENSION_STARTS_PER_RUN, 3)

    def test_automation_queue_tracks_waiting_and_completion(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db = BuildingDatabase(f"{temp_dir}/building_manager.db")
            job_id = db.add_or_update_automation_job(
                request_id=42,
                guild_id=100,
                building_id=555,
                building_type="Hospital",
                building_name="Example Hospital",
                next_run_at=1,
            )

            due = db.get_due_automation_jobs(now_ts=2)
            self.assertEqual([job.job_id for job in due], [job_id])

            db.update_automation_job(
                job_id,
                BuildingAutomationResult(
                    ok=True,
                    completed=False,
                    wait=True,
                    reason="Waiting for next extension slot.",
                    actions=["Set tax to 20%", "Extension A"],
                    tax_complete=True,
                    extensions_started=1,
                ),
            )
            waiting = db.get_automation_job(job_id)
            self.assertEqual(waiting.status, "waiting")
            self.assertTrue(waiting.tax_complete)
            self.assertEqual(waiting.extensions_started, 1)

            db.update_automation_job(
                job_id,
                BuildingAutomationResult(
                    ok=True,
                    completed=True,
                    wait=False,
                    reason="Done.",
                    actions=[],
                    tax_complete=True,
                    level_complete=True,
                    extensions_complete=True,
                ),
            )
            completed = db.get_automation_job_by_request_or_building(555)
            self.assertEqual(completed.status, "completed")
            self.assertTrue(completed.level_complete)
            self.assertTrue(completed.extensions_complete)

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
