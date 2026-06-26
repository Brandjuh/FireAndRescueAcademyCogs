import tempfile
import unittest

from buildingmanager.buildingmanager import (
    ALLIANCE_BUILDING_TARGET_HOSPITAL_LEVEL,
    BUILDING_AUTOMATION_MAX_EXTENSION_STARTS_PER_RUN,
    BUILDING_AUTOMATION_DIRECT_SCRIPT,
    BUILDING_AUTOMATION_PREPARE_SCRIPT,
    BUILDING_CREATE_SCRIPT,
    BUILDING_FETCH_API_SCRIPT,
    BUILDING_FETCH_ALLIANCE_LIST_SCRIPT,
    BuildingAutomationResult,
    BuildingDatabase,
    BuildingRequest,
    LocationParser,
    MISSIONCHIEF_BUILDING_NAME_LIMIT,
    _clean_building_name,
    _missionchief_building_name,
    _normalize_missionchief_url,
    _truncate_discord_text,
    alliance_funds_allow_auto_build,
    build_alliance_building_config,
    building_request_from_row,
    build_browser_diagnostics_report,
    extract_missionchief_building_id,
    find_created_alliance_building_id,
    find_created_alliance_building_id_from_list,
    find_new_created_alliance_building_id_from_list,
    parse_alliance_funds_from_html,
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

    def test_missionchief_building_name_respects_game_limit(self):
        name = _missionchief_building_name("A very long hospital name that exceeds the game limit")

        self.assertEqual(len(name), MISSIONCHIEF_BUILDING_NAME_LIMIT)
        self.assertFalse(name.endswith("..."))

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

    def test_build_alliance_building_config_limits_name_to_missionchief_max(self):
        config = build_alliance_building_config(
            building_type="Hospital",
            building_name="A very long hospital name that exceeds the MissionChief limit",
            coordinates="42.6973, 9.4509",
            address=None,
        )

        self.assertEqual(len(config["name"]), MISSIONCHIEF_BUILDING_NAME_LIMIT)
        self.assertFalse(config["name"].endswith("..."))

    def test_build_alliance_building_config_decodes_url_encoded_names(self):
        config = build_alliance_building_config(
            building_type="Hospital",
            building_name="h%25C3%25B4pital de Bastia",
            coordinates="42.6973, 9.4509",
            address=None,
        )

        self.assertEqual(config["name"], "hôpital de Bastia")

    def test_location_parser_extracts_decoded_google_place_name(self):
        url = "https://www.google.nl/maps/place/h%25C3%25B4pital+de+Bastia/@42.6973,9.4509,17z"

        self.assertEqual(LocationParser.extract_place_name(url), "hôpital de Bastia")

    def test_clean_building_name_decodes_repeated_encoding(self):
        self.assertEqual(_clean_building_name("h%25C3%25B4pital de Bastia"), "hôpital de Bastia")
        self.assertEqual(
            _clean_building_name("OSPEDALE %22San Giuseppe%22 di Isili"),
            'OSPEDALE "San Giuseppe" di Isili',
        )

    def test_truncate_discord_text_respects_limit(self):
        value = _truncate_discord_text("A" * 50, 20)

        self.assertEqual(len(value), 20)
        self.assertTrue(value.endswith("..."))

    def test_building_request_cleans_encoded_names(self):
        request = BuildingRequest(
            user_id=1,
            username="Requester",
            building_type="Hospital",
            building_name="OSPEDALE %22San Giuseppe%22 di Isili",
            location_input="Location",
        )

        self.assertEqual(request.building_name, 'OSPEDALE "San Giuseppe" di Isili')

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

    def test_parse_alliance_funds_from_html_requires_alliance_funds_marker(self):
        html = """
        <div>Alliance Funds</div>
        <div>16,935,312 Credits</div>
        <div>Your contribution to the alliance</div>
        """

        self.assertEqual(parse_alliance_funds_from_html(html), 16935312)
        self.assertIsNone(parse_alliance_funds_from_html("<div>1,000 Credits</div>"))

    def test_parse_alliance_funds_from_html_allows_dom_order_variation(self):
        html = """
        <div>16,935,312 Credits</div>
        <h2>Alliance Funds</h2>
        <div>Your contribution to the alliance</div>
        """

        self.assertEqual(parse_alliance_funds_from_html(html), 16935312)

    def test_alliance_funds_allow_auto_build_requires_live_funds_over_threshold(self):
        self.assertTrue(alliance_funds_allow_auto_build(2_000_000, "live MissionChief"))
        self.assertTrue(alliance_funds_allow_auto_build(2_000_000, "live MissionChief browser"))
        self.assertFalse(alliance_funds_allow_auto_build(1_999_999, "live MissionChief"))
        self.assertFalse(alliance_funds_allow_auto_build(10_000_000, "income_v2.db"))
        self.assertFalse(alliance_funds_allow_auto_build(None, "live MissionChief"))

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

    def test_fetch_alliance_list_script_uses_same_browser_session(self):
        self.assertIn("/verband/gebauede", BUILDING_FETCH_ALLIANCE_LIST_SCRIPT)
        self.assertIn("[building_id]", BUILDING_FETCH_ALLIANCE_LIST_SCRIPT)
        self.assertIn('/buildings/', BUILDING_FETCH_ALLIANCE_LIST_SCRIPT)
        self.assertIn("targetName", BUILDING_FETCH_ALLIANCE_LIST_SCRIPT)
        self.assertIn("decodeURIComponent", BUILDING_FETCH_ALLIANCE_LIST_SCRIPT)
        self.assertIn("?page=", BUILDING_FETCH_ALLIANCE_LIST_SCRIPT)
        self.assertIn('credentials: "same-origin"', BUILDING_FETCH_ALLIANCE_LIST_SCRIPT)

    def test_extract_building_id_from_urls_and_snapshots(self):
        self.assertEqual(
            extract_missionchief_building_id("https://www.missionchief.com/buildings/123456"),
            123456,
        )
        self.assertEqual(
            extract_missionchief_building_id({"finalUrl": "https://www.missionchief.com/buildings/987"}),
            987,
        )
        self.assertEqual(
            extract_missionchief_building_id({"Location": "/buildings/654321"}),
            654321,
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

    def test_find_created_building_id_from_alliance_list_matches_name_and_type(self):
        config = build_alliance_building_config(
            building_type="Hospital",
            building_name="Maxima MC Eindhoven",
            coordinates="51.4, 5.4",
            address=None,
        )
        candidates = [
            {
                "id": 300,
                "text": "Other Hospital",
                "rowText": "Other Hospital",
                "searchAttribute": "Other Hospital",
                "imageSources": ["/images/building_hospital.png"],
            },
            {
                "id": 301,
                "text": "Maxima MC Eindhoven",
                "rowText": "[AA] Maxima MC Eindhoven",
                "searchAttribute": "Maxima MC Eindhoven",
                "imageSources": ["/images/building_hospital.png"],
            },
        ]

        self.assertEqual(find_created_alliance_building_id_from_list(candidates, config), 301)

    def test_find_created_building_id_from_alliance_list_decodes_encoded_candidate_names(self):
        config = build_alliance_building_config(
            building_type="Hospital",
            building_name='OSPEDALE "San Giuseppe" di Isili',
            coordinates="39.7426879, 9.1101257",
            address=None,
        )
        candidates = [
            {
                "id": 304,
                "text": "OSPEDALE %22San Giuseppe%22 di Isili",
                "rowText": "Hospital OSPEDALE %22San Giuseppe%22 di Isili",
                "searchAttribute": "OSPEDALE %22San Giuseppe%22 di Isili",
                "imageSources": ["/images/building_hospital.png"],
            }
        ]

        self.assertEqual(find_created_alliance_building_id_from_list(candidates, config), 304)

    def test_find_created_building_id_from_alliance_list_rejects_missing_name(self):
        config = build_alliance_building_config(
            building_type="Hospital",
            building_name="Maxima MC Eindhoven",
            coordinates="51.4, 5.4",
            address=None,
        )
        candidates = [
            {
                "id": 302,
                "text": "Generic Hospital",
                "rowText": "Generic Hospital",
                "searchAttribute": "Generic Hospital",
                "imageSources": ["/images/building_hospital.png"],
            }
        ]

        self.assertIsNone(find_created_alliance_building_id_from_list(candidates, config))

    def test_find_created_building_id_from_alliance_list_rejects_obvious_wrong_type(self):
        config = build_alliance_building_config(
            building_type="Hospital",
            building_name="Shared Campus",
            coordinates="51.4, 5.4",
            address=None,
        )
        candidates = [
            {
                "id": 303,
                "text": "Shared Campus",
                "rowText": "Shared Campus prison",
                "searchAttribute": "Shared Campus",
                "imageSources": ["/images/building_prison.png"],
            }
        ]

        self.assertIsNone(find_created_alliance_building_id_from_list(candidates, config))

    def test_find_created_building_id_from_alliance_list_prefers_highest_id_for_duplicate_name(self):
        config = build_alliance_building_config(
            building_type="Prison",
            building_name="County Prison",
            coordinates="40.1, -73.9",
            address=None,
        )
        candidates = [
            {
                "id": 310,
                "text": "County Prison",
                "rowText": "County Prison",
                "searchAttribute": "County Prison",
                "imageSources": ["/images/building_prison.png"],
            },
            {
                "id": 311,
                "text": "County Prison",
                "rowText": "County Prison",
                "searchAttribute": "County Prison",
                "imageSources": ["/images/building_prison.png"],
            },
        ]

        self.assertEqual(find_created_alliance_building_id_from_list(candidates, config), 311)

    def test_find_new_created_building_id_from_alliance_list_uses_snapshot_diff(self):
        config = build_alliance_building_config(
            building_type="Hospital",
            building_name="hôpital de Bastia",
            coordinates="42.7, 9.4",
            address=None,
        )
        before = [
            {
                "id": 400,
                "rowText": "Existing Hospital",
                "imageSources": ["/images/building_hospital.png"],
            }
        ]
        after = [
            *before,
            {
                "id": 401,
                "rowText": "h%C3%B4pital%20de%20Bastia",
                "imageSources": ["/images/building_hospital.png"],
            },
        ]

        self.assertEqual(find_new_created_alliance_building_id_from_list(before, after, config), 401)

    def test_find_new_created_building_id_from_alliance_list_prefers_requested_type(self):
        config = build_alliance_building_config(
            building_type="Prison",
            building_name="County Facility",
            coordinates="40.1, -73.9",
            address=None,
        )
        before = []
        after = [
            {
                "id": 500,
                "rowText": "County Facility Medical",
                "imageSources": ["/images/building_hospital.png"],
            },
            {
                "id": 501,
                "rowText": "County Facility",
                "imageSources": ["/images/building_prison.png"],
            },
        ]

        self.assertEqual(find_new_created_alliance_building_id_from_list(before, after, config), 501)

    def test_automation_script_refuses_coins_and_excludes_large_buildings(self):
        self.assertIn("coin|coins", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertIn("large hospital", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertIn("large prison", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertIn("maxExtensionStarts", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertEqual(BUILDING_AUTOMATION_MAX_EXTENSION_STARTS_PER_RUN, 3)
        self.assertEqual(ALLIANCE_BUILDING_TARGET_HOSPITAL_LEVEL, 20)

    def test_direct_automation_script_uses_missionchief_building_endpoints(self):
        self.assertIn("/alliance_costs/${targetTaxId}", BUILDING_AUTOMATION_DIRECT_SCRIPT)
        self.assertIn("/expand_do/credits?level=${levelTarget}", BUILDING_AUTOMATION_DIRECT_SCRIPT)
        self.assertIn("/extension/credits/", BUILDING_AUTOMATION_DIRECT_SCRIPT)
        self.assertIn("offer.extId !== 9", BUILDING_AUTOMATION_DIRECT_SCRIPT)
        self.assertIn("offer.extId !== 30", BUILDING_AUTOMATION_DIRECT_SCRIPT)
        self.assertIn("offer.price !== 200000", BUILDING_AUTOMATION_DIRECT_SCRIPT)
        self.assertIn("maxExtensionStarts", BUILDING_AUTOMATION_DIRECT_SCRIPT)

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

    def test_database_can_read_request_by_id_for_manual_automation_queue(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db = BuildingDatabase(f"{temp_dir}/building_manager.db")
            request_id = db.add_request(
                guild_id=100,
                user_id=200,
                username="Requester",
                building_type="Hospital",
                building_name="Example Hospital",
                location_input="https://maps.example/request",
                coordinates="40.1, -73.9",
                address="Example Street",
                notes=None,
            )

            request = db.get_request_by_id(request_id)

            self.assertIsNotNone(request)
            self.assertEqual(request["request_id"], request_id)
            self.assertEqual(request["building_type"], "Hospital")
            self.assertEqual(request["building_name"], "Example Hospital")

    def test_database_can_list_requests_waiting_for_funds(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db = BuildingDatabase(f"{temp_dir}/building_manager.db")
            request_id = db.add_request(
                guild_id=100,
                user_id=200,
                username="Requester",
                building_type="Prison",
                building_name="Example Prison",
                location_input="Location",
                coordinates="40.1, -73.9",
                address="Example Street",
                notes="Note",
            )
            db.update_request_status(request_id, "awaiting_funds")

            rows = db.get_requests_by_status("awaiting_funds")
            req = building_request_from_row(rows[0])

            self.assertEqual([row["request_id"] for row in rows], [request_id])
            self.assertEqual(req.request_id, request_id)
            self.assertEqual(req.building_type, "Prison")

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
