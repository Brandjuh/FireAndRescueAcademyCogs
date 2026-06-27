import asyncio
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from buildingmanager.buildingmanager import (
    ALLIANCE_BUILDING_TARGET_HOSPITAL_LEVEL,
    BOARD_REPLY_MARKER,
    BUILDING_AUTOMATION_MAX_ACTIONS_PER_RUN,
    BUILDING_AUTOMATION_MAX_EXTENSION_STARTS_PER_RUN,
    BUILDING_AUTOMATION_DIRECT_SCRIPT,
    BUILDING_AUTOMATION_PREPARE_SCRIPT,
    BUILDING_CREATE_SCRIPT,
    BUILDING_FETCH_API_SCRIPT,
    BUILDING_FETCH_ALLIANCE_LIST_SCRIPT,
    BUILDING_FETCH_ALLIANCE_LOGS_SCRIPT,
    AUTO_CANDIDATE_DUPLICATE_RADIUS_METERS,
    BuildingAutomationResult,
    BoardBuildingPost,
    BoardPage,
    BuildingCreateResult,
    BuildingDatabase,
    BuildingManager,
    BuildingRequest,
    LocationDetails,
    LocationParser,
    MISSIONCHIEF_BUILDING_NAME_LIMIT,
    _clean_building_name,
    _missionchief_building_name,
    _normalize_missionchief_url,
    _truncate_discord_text,
    alliance_funds_allow_auto_build,
    building_create_result_needs_recovery,
    build_alliance_building_config,
    building_request_from_row,
    build_building_board_guide_content,
    build_overpass_candidate_query,
    build_browser_diagnostics_report,
    extract_missionchief_building_id,
    extract_building_board_request,
    find_created_alliance_building_id,
    find_created_alliance_building_id_from_list,
    find_created_alliance_building_id_from_logs,
    find_new_created_alliance_building_id_from_list,
    parse_building_board_page,
    parse_alliance_funds_from_html,
    parse_overpass_auto_build_candidates,
    format_overpass_http_error,
    send_ephemeral_followup,
)


BUILDING_BOARD_HTML = """
<script>
  user_id = 88649;
</script>
<ul class="pagination pagination">
  <li><a href="/alliance_threads/6165?page=1">1</a></li>
  <li class="active"><span>2</span></li>
</ul>
<div class="panel panel-default" id="post-on-page-1">
  <div class="panel-body">
    <div class="row">
      <div class="col-md-1">
        <strong><a href="/profile/123456">BoardUser</a></strong>
        <br>
        <span title="June 26, 2026 08:15">June 26, 2026 08:15</span>
      </div>
      <div class="col-md-11">
        <p>Hospital: https://www.google.com/maps/place/Example+Hospital/@40.1,-73.9</p>
      </div>
    </div>
  </div>
  <div class="panel-footer">
    <a href="/alliance_posts/200001/edit">Edit</a>
  </div>
</div>
<form action="/alliance_posts?alliance_thread_id=6165" id="new_alliance_post" method="post">
  <input name="authenticity_token" type="hidden" value="token-building-board" />
  <textarea name="alliance_post[content]"></textarea>
</form>
"""


class _Response:
    def __init__(self, html, status=200, url="https://www.missionchief.com/test"):
        self._html = html
        self.status = status
        self.url = url

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self._html


class _Session:
    def __init__(self, html):
        self.html = html
        self.posts = []
        self.get_urls = []

    def get(self, url, **kwargs):
        self.get_urls.append(url)
        html = self.html.get(url, "") if isinstance(self.html, dict) else self.html
        return _Response(html, url=url)

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return _Response("<html>ok</html>", url=url)


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

    def test_ephemeral_followup_edits_deferred_original_response(self):
        interaction = types.SimpleNamespace(
            response=types.SimpleNamespace(is_done=lambda: True),
            edit_original_response=AsyncMock(),
            followup=types.SimpleNamespace(send=AsyncMock()),
        )

        asyncio.run(send_ephemeral_followup(interaction, "Done"))

        interaction.edit_original_response.assert_awaited_once_with(content="Done", embed=None, view=None)
        interaction.followup.send.assert_not_awaited()

    def test_summary_submit_uses_background_processing_without_thinking_defer(self):
        source = Path("buildingmanager/buildingmanager.py").read_text(encoding="utf-8")
        start = source.index('custom_id="bm:submit"')
        end = source.index('custom_id="bm:cancel"', start)
        submit_source = source[start:end]

        self.assertNotIn("thinking=True", submit_source)
        self.assertIn("processing it in the background", submit_source)
        self.assertIn("_schedule_background_submission", submit_source)

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

    def test_current_alliance_funds_reports_live_failure_reason(self):
        manager = BuildingManager.__new__(BuildingManager)

        async def fail_aiohttp():
            raise RuntimeError("session expired")

        async def fail_browser():
            raise RuntimeError("browser unavailable")

        manager._fetch_live_alliance_funds = fail_aiohttp
        manager._fetch_live_alliance_funds_browser = fail_browser
        manager._get_alliance_funds_from_contract = AsyncMock(return_value=None)
        manager._read_alliance_funds_from_income_db = lambda: None

        funds, source = asyncio.run(manager._get_current_alliance_funds())

        self.assertIsNone(funds)
        self.assertIn("live MissionChief: RuntimeError: session expired", source)
        self.assertIn("live MissionChief browser: RuntimeError: browser unavailable", source)

    def test_queue_waiting_for_funds_does_not_record_auto_approval(self):
        class FakeDb:
            def __init__(self):
                self.status = None
                self.actions = []

            def update_request_status(self, request_id, status):
                self.status = (request_id, status)

            def add_action(self, **kwargs):
                self.actions.append(kwargs)

        manager = BuildingManager.__new__(BuildingManager)
        manager.db = FakeDb()
        manager._send_building_request_game_update = AsyncMock(return_value={"ok": True})

        req = BuildingRequest(
            user_id=0,
            username="BoardUser",
            building_type="Hospital",
            building_name="Example Hospital",
            location_input="https://maps.app.goo.gl/example",
            request_id=49,
        )
        guild = types.SimpleNamespace(id=1, get_member=lambda _user_id: None)

        message = asyncio.run(
            manager._queue_request_waiting_for_funds(
                guild=guild,
                req=req,
                requester_id=0,
                admin_user=None,
                funds=None,
                source="unavailable; live MissionChief: session expired",
                minimum=2_000_000,
                log_channel=None,
                record_approval=False,
            )
        )

        self.assertEqual(manager.db.status, (49, "awaiting_funds"))
        self.assertEqual([action["action_type"] for action in manager.db.actions], ["awaiting_funds"])
        self.assertIn("No building was created yet", message)
        manager._send_building_request_game_update.assert_awaited_once()
        self.assertEqual(manager._send_building_request_game_update.await_args.kwargs["subject"], "Building request queued")

    def test_discord_request_contribution_uses_membersync_and_membersscraper(self):
        member_sync = types.SimpleNamespace(
            get_link_for_discord=AsyncMock(return_value={"discord_id": 123, "mc_user_id": "456"})
        )
        members_scraper = types.SimpleNamespace(
            get_member_snapshot=AsyncMock(return_value={"member_id": 456, "contribution_rate": 7.5})
        )
        manager = BuildingManager.__new__(BuildingManager)
        manager.bot = types.SimpleNamespace(
            get_cog=lambda name: {
                "MemberSync": member_sync,
                "MembersScraper": members_scraper,
            }.get(name)
        )
        user = types.SimpleNamespace(id=123)

        rate, source = asyncio.run(manager._get_discord_request_contribution_rate(user))

        self.assertEqual(rate, 7.5)
        self.assertIn("MC ID 456", source)
        member_sync.get_link_for_discord.assert_awaited_once_with(123)
        members_scraper.get_member_snapshot.assert_awaited_once_with("456")

    def test_discord_panel_request_unknown_tax_goes_to_admin_review(self):
        class FakeGuildConfig:
            async def all(self):
                return {"admin_channel_id": 10, "log_channel_id": 20}

        class FakeConfig:
            def guild(self, _guild):
                return FakeGuildConfig()

        guild = types.SimpleNamespace(id=1)
        requester = types.SimpleNamespace(id=123)
        request = BuildingRequest(
            user_id=123,
            username="DiscordUser",
            building_type="Hospital",
            building_name="Example Hospital",
            location_input="https://maps.app.goo.gl/example",
        )
        manager = BuildingManager.__new__(BuildingManager)
        manager.config = FakeConfig()
        manager._resolve_channel = AsyncMock(side_effect=[object(), object()])
        manager._get_discord_request_contribution_rate = AsyncMock(return_value=(None, "no snapshot"))
        manager._submit_building_request_to_admins = AsyncMock(return_value=900)

        message = asyncio.run(manager._process_discord_building_panel_request(guild, request, requester))

        self.assertIn("submitted to admins", message)
        self.assertIn("contribution rate unknown", request.notes)
        manager._submit_building_request_to_admins.assert_awaited_once_with(
            guild,
            request,
            source="Discord request - contribution unknown",
        )

    def test_discord_request_low_tax_is_rejected_without_admin_review(self):
        class FakeDb:
            def __init__(self):
                self.status = None
                self.actions = []

            def add_request(self, **kwargs):
                self.added = kwargs
                return 800

            def update_request_status(self, request_id, status):
                self.status = (request_id, status)

            def add_action(self, **kwargs):
                self.actions.append(kwargs)

        log_channel = types.SimpleNamespace(send=AsyncMock())
        guild = types.SimpleNamespace(id=1, get_member=lambda _member_id: None)
        requester = types.SimpleNamespace(id=123, mention="@User")
        manager = BuildingManager.__new__(BuildingManager)
        manager.db = FakeDb()
        request = BuildingRequest(
            user_id=123,
            username="DiscordUser",
            building_type="Hospital",
            building_name="Example Hospital",
            location_input="https://maps.app.goo.gl/example",
        )

        message = asyncio.run(
            manager._reject_discord_request_for_low_tax(
                guild,
                request,
                requester,
                contribution_rate=4.9,
                log_channel=log_channel,
            )
        )

        self.assertEqual(manager.db.status, (800, "denied"))
        self.assertEqual(manager.db.actions[0]["action_type"], "auto_denied_low_tax")
        self.assertIn("4.9%", message)
        log_channel.send.assert_awaited_once()

    def test_discord_request_auto_accept_builds_with_known_tax_and_live_funds(self):
        class FakeDb:
            def __init__(self):
                self.status = None
                self.actions = []

            def add_request(self, **kwargs):
                self.added = kwargs
                return 801

            def update_request_status(self, request_id, status):
                self.status = (request_id, status)

            def add_action(self, **kwargs):
                self.actions.append(kwargs)

        log_channel = types.SimpleNamespace(send=AsyncMock())
        guild = types.SimpleNamespace(id=1, get_member=lambda _member_id: None)
        requester = types.SimpleNamespace(id=123, mention="@User")
        manager = BuildingManager.__new__(BuildingManager)
        manager.db = FakeDb()
        manager._get_min_alliance_funds = AsyncMock(return_value=2_000_000)
        manager._get_current_alliance_funds = AsyncMock(return_value=(34_000_000, "live MissionChief"))
        manager._create_and_queue_approved_building = AsyncMock(
            return_value=(BuildingCreateResult(True, "created", details={"buildingId": 999}), "Queued automation.")
        )
        request = BuildingRequest(
            user_id=123,
            username="DiscordUser",
            building_type="Prison",
            building_name="Example Prison",
            location_input="https://maps.app.goo.gl/example",
        )

        message = asyncio.run(
            manager._auto_accept_discord_building_request(
                guild,
                request,
                requester,
                contribution_rate=5.0,
                log_channel=log_channel,
            )
        )

        self.assertEqual(manager.db.status, (801, "created"))
        self.assertEqual([action["action_type"] for action in manager.db.actions], ["auto_approved", "created"])
        self.assertIn("automatically created", message)
        log_channel.send.assert_awaited_once()

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

    def test_fetch_alliance_logs_script_reads_building_links(self):
        self.assertIn("/alliance_logfiles", BUILDING_FETCH_ALLIANCE_LOGS_SCRIPT)
        self.assertIn('/buildings/', BUILDING_FETCH_ALLIANCE_LOGS_SCRIPT)
        self.assertIn("affectedName", BUILDING_FETCH_ALLIANCE_LOGS_SCRIPT)
        self.assertIn('credentials: "same-origin"', BUILDING_FETCH_ALLIANCE_LOGS_SCRIPT)

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
        self.assertEqual(
            extract_missionchief_building_id(
                {
                    "allianceLogLookup": {
                        "ok": True,
                        "matchedBuildingId": 777,
                    }
                }
            ),
            777,
        )
        self.assertIsNone(extract_missionchief_building_id("https://www.missionchief.com/buildings"))

    def test_building_create_result_reads_nested_log_lookup_id(self):
        result = BuildingCreateResult(
            True,
            "Created.",
            details={
                "buildingId": None,
                "allianceLogLookup": {
                    "ok": True,
                    "matchedBuildingId": 888,
                },
            },
        )

        self.assertEqual(result.building_id, 888)

    def test_building_create_result_needs_recovery_only_for_timeouts(self):
        self.assertTrue(
            building_create_result_needs_recovery(
                BuildingCreateResult(False, "MissionChief browser building flow timed out: Timeout 30000ms")
            )
        )
        self.assertFalse(
            building_create_result_needs_recovery(
                BuildingCreateResult(False, "No enabled alliance build button was found.")
            )
        )
        self.assertFalse(
            building_create_result_needs_recovery(
                BuildingCreateResult(True, "Alliance building created through browser automation.")
            )
        )

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

    def test_find_created_building_id_from_alliance_logs_matches_requested_name(self):
        config = build_alliance_building_config(
            building_type="Hospital",
            building_name="University Hospital Quironsalud Madrid",
            coordinates="40.4027317, -3.7834356",
            address=None,
        )
        candidates = [
            {
                "id": 700,
                "affectedName": "Other Hospital",
                "rowText": "Building constructed Other Hospital",
                "href": "/buildings/700",
            },
            {
                "id": 701,
                "affectedName": "University Hospital Quironsalud Madrid",
                "rowText": "Building constructed University Hospital Quironsalud Madrid",
                "href": "/buildings/701",
            },
        ]

        self.assertEqual(find_created_alliance_building_id_from_logs(candidates, config), 701)

    def test_find_created_building_id_from_alliance_logs_rejects_unrelated_first_log(self):
        config = build_alliance_building_config(
            building_type="Prison",
            building_name="County Justice Center",
            coordinates="40.1, -73.9",
            address=None,
        )
        candidates = [
            {
                "id": 710,
                "affectedName": "Other Prison",
                "rowText": "Building constructed Other Prison",
                "href": "/buildings/710",
            }
        ]

        self.assertIsNone(find_created_alliance_building_id_from_logs(candidates, config))

    def test_automation_script_refuses_coins_and_excludes_large_buildings(self):
        self.assertIn("coin|coins", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertIn("large hospital", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertIn("large prison", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertIn("maxExtensionStarts", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertIn("config.maxExtensionStarts || 30", BUILDING_AUTOMATION_PREPARE_SCRIPT)
        self.assertIn("config.maxExtensionStarts || 30", BUILDING_AUTOMATION_DIRECT_SCRIPT)
        self.assertEqual(
            BUILDING_AUTOMATION_MAX_EXTENSION_STARTS_PER_RUN,
            BUILDING_AUTOMATION_MAX_ACTIONS_PER_RUN,
        )
        self.assertEqual(ALLIANCE_BUILDING_TARGET_HOSPITAL_LEVEL, 20)

    def test_admin_approval_button_is_labeled_auto_build(self):
        source = Path("buildingmanager/buildingmanager.py").read_text(encoding="utf-8")

        self.assertIn('label="Auto build"', source)
        self.assertIn('custom_id="bm:approve"', source)

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

    def test_overpass_parser_accepts_hospital_and_prison_candidates(self):
        data = {
            "elements": [
                {
                    "type": "node",
                    "id": 101,
                    "lat": 40.1,
                    "lon": -73.9,
                    "tags": {
                        "amenity": "hospital",
                        "name": "Example General Hospital",
                        "addr:city": "New York",
                        "addr:country": "US",
                    },
                },
                {
                    "type": "way",
                    "id": 202,
                    "center": {"lat": 41.1, "lon": -74.9},
                    "tags": {
                        "amenity": "prison",
                        "name": "Example Correctional Facility",
                        "addr:state": "NY",
                    },
                },
                {
                    "type": "node",
                    "id": 303,
                    "lat": 42.1,
                    "lon": -75.9,
                    "tags": {
                        "amenity": "hospital",
                        "name": "Example Doctor Clinic",
                    },
                },
            ]
        }

        candidates, stats = parse_overpass_auto_build_candidates(data)

        self.assertEqual(stats["source_elements"], 3)
        self.assertEqual(stats["accepted"], 2)
        self.assertEqual(stats["rejected"], 1)
        self.assertEqual([candidate["building_type"] for candidate in candidates], ["Hospital", "Prison"])
        self.assertEqual(candidates[0]["source_id"], "node/101")
        self.assertEqual(candidates[1]["source_id"], "way/202")

    def test_overpass_query_uses_bbox_and_supported_tags(self):
        query = build_overpass_candidate_query(40.0, -74.0, 41.0, -73.0)

        self.assertIn('nwr["amenity"="hospital"](40.0000000,-74.0000000,41.0000000,-73.0000000);', query)
        self.assertIn('nwr["healthcare"="hospital"]', query)
        self.assertIn('nwr["amenity"="prison"]', query)
        self.assertIn("out center tags;", query)

    def test_overpass_query_can_limit_to_one_candidate_type(self):
        hospital_query = build_overpass_candidate_query(40.0, -74.0, 41.0, -73.0, "hospital")
        prison_query = build_overpass_candidate_query(40.0, -74.0, 41.0, -73.0, "prison")

        self.assertIn('nwr["amenity"="hospital"]', hospital_query)
        self.assertNotIn('nwr["amenity"="prison"]', hospital_query)
        self.assertNotIn('nwr["amenity"="hospital"]', prison_query)
        self.assertIn('nwr["amenity"="prison"]', prison_query)

    def test_overpass_504_error_is_short_and_actionable(self):
        body = """<?xml version="1.0"?><html><head><title>OSM3S Response</title></head>
        <body><p>The data included in this document is from www.openstreetmap.org.</p></body></html>"""

        message = format_overpass_http_error(504, body, building_type="both")

        self.assertIn("HTTP 504", message)
        self.assertIn("smaller bounding box", message)
        self.assertNotIn("<?xml", message)
        self.assertLess(len(message), 350)

    def test_candidate_database_tracks_available_used_and_duplicate_status(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db = BuildingDatabase(f"{temp_dir}/building_manager.db")
            stats = db.upsert_auto_candidates(
                [
                    {
                        "source": "openstreetmap",
                        "source_id": "node/101",
                        "building_type": "Hospital",
                        "name": "Example Hospital",
                        "lat": 40.1,
                        "lon": -73.9,
                        "raw_tags_json": "{}",
                    },
                    {
                        "source": "openstreetmap",
                        "source_id": "way/202",
                        "building_type": "Prison",
                        "name": "Example Prison",
                        "lat": 41.1,
                        "lon": -74.9,
                        "raw_tags_json": "{}",
                    },
                ]
            )

            self.assertEqual(stats, {"inserted": 2, "updated": 0, "skipped": 0})
            hospital = db.get_random_auto_candidates("Hospital", limit=1)[0]
            db.mark_auto_candidate(hospital.candidate_id, "used", missionchief_building_id=999)
            counts = db.get_auto_candidate_stats()

            self.assertEqual(counts["Hospital:used"], 1)
            self.assertEqual(counts["Prison:available"], 1)
            self.assertEqual(db.get_auto_candidate(hospital.candidate_id).missionchief_building_id, 999)

    def test_candidate_selection_skips_nearby_existing_building(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = BuildingManager.__new__(BuildingManager)
            manager.db = BuildingDatabase(f"{temp_dir}/building_manager.db")
            manager.db.upsert_auto_candidates(
                [
                    {
                        "source": "openstreetmap",
                        "source_id": "node/101",
                        "building_type": "Hospital",
                        "name": "Duplicate Hospital",
                        "lat": 40.1000000,
                        "lon": -73.9000000,
                        "raw_tags_json": "{}",
                    },
                    {
                        "source": "openstreetmap",
                        "source_id": "node/102",
                        "building_type": "Hospital",
                        "name": "Available Hospital",
                        "lat": 41.0000000,
                        "lon": -74.0000000,
                        "raw_tags_json": "{}",
                    },
                ]
            )
            candidates = sorted(
                manager.db.get_random_auto_candidates("Hospital", limit=10),
                key=lambda candidate: candidate.name,
                reverse=True,
            )
            manager.db.get_random_auto_candidates = lambda *_args, **_kwargs: candidates
            existing = [
                {
                    "id": 555,
                    "building_type": 2,
                    "latitude": 40.10001,
                    "longitude": -73.90001,
                }
            ]

            plan = manager._select_auto_candidate(
                "Hospital",
                existing_buildings=existing,
                duplicate_source="test",
                duplicate_radius_m=AUTO_CANDIDATE_DUPLICATE_RADIUS_METERS,
            )

            self.assertIsNotNone(plan.candidate)
            self.assertEqual(plan.candidate.name, "Available Hospital")
            duplicate = manager.db.get_auto_candidate_stats()
            self.assertEqual(duplicate["Hospital:duplicate"], 1)

    def test_auto_candidate_build_marks_candidate_used_on_success(self):
        class FakeGuildConfig:
            async def auto_candidate_min_funds(self):
                return 5_000_000

            async def all(self):
                return {
                    "log_channel_id": None,
                    "admin_channel_id": None,
                }

        class FakeConfig:
            def guild(self, _guild):
                return FakeGuildConfig()

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = BuildingManager.__new__(BuildingManager)
            manager.db = BuildingDatabase(f"{temp_dir}/building_manager.db")
            manager.config = FakeConfig()
            manager.bot = types.SimpleNamespace()
            manager._get_current_alliance_funds = AsyncMock(return_value=(6_000_000, "live MissionChief"))
            manager._candidate_duplicate_context = AsyncMock(return_value=([], "test duplicate check", 250))
            manager._create_and_queue_approved_building = AsyncMock(
                return_value=(
                    BuildingCreateResult(
                        True,
                        "Created.",
                        details={"buildingId": 12345},
                    ),
                    "Queued post-creation automation.",
                )
            )
            manager._resolve_channel = AsyncMock(return_value=None)
            manager.db.upsert_auto_candidates(
                [
                    {
                        "source": "openstreetmap",
                        "source_id": "node/101",
                        "building_type": "Hospital",
                        "name": "Example Hospital",
                        "lat": 40.1,
                        "lon": -73.9,
                        "raw_tags_json": "{}",
                    }
                ]
            )

            result = asyncio.run(
                manager._run_auto_candidate_build(
                    types.SimpleNamespace(id=1),
                    "Hospital",
                    run_date="2026-06-27",
                    scheduled=False,
                )
            )

            self.assertIn("created Example Hospital", result)
            run = manager.db.get_auto_run(1, "2026-06-27", "Hospital")
            self.assertEqual(run["result"], "created")
            self.assertEqual(run["missionchief_building_id"], 12345)
            counts = manager.db.get_auto_candidate_stats()
            self.assertEqual(counts["Hospital:used"], 1)

    def test_auto_candidate_build_blocks_below_autonomous_threshold(self):
        class FakeGuildConfig:
            async def auto_candidate_min_funds(self):
                return 5_000_000

        class FakeConfig:
            def guild(self, _guild):
                return FakeGuildConfig()

        with tempfile.TemporaryDirectory() as temp_dir:
            manager = BuildingManager.__new__(BuildingManager)
            manager.db = BuildingDatabase(f"{temp_dir}/building_manager.db")
            manager.config = FakeConfig()
            manager._get_current_alliance_funds = AsyncMock(return_value=(4_999_999, "live MissionChief"))
            manager._candidate_duplicate_context = AsyncMock()
            manager._create_and_queue_approved_building = AsyncMock()

            result = asyncio.run(
                manager._run_auto_candidate_build(
                    types.SimpleNamespace(id=1),
                    "Hospital",
                    run_date="2026-06-27",
                    scheduled=False,
                )
            )

            self.assertIn("blocked by funds safety rule", result)
            self.assertIn("5,000,000 credits", result)
            manager._candidate_duplicate_context.assert_not_awaited()
            manager._create_and_queue_approved_building.assert_not_awaited()

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

    def test_parse_building_board_page_extracts_post_and_reply_form(self):
        page = parse_building_board_page(BUILDING_BOARD_HTML)

        self.assertEqual(page.last_page, 2)
        self.assertEqual(page.current_user_id, "88649")
        self.assertEqual(page.reply_action, "/alliance_posts?alliance_thread_id=6165")
        self.assertEqual(page.reply_token, "token-building-board")
        self.assertEqual(len(page.posts), 1)
        self.assertEqual(page.posts[0].post_id, 200001)
        self.assertEqual(page.posts[0].author_id, "123456")
        self.assertEqual(page.posts[0].author_name, "BoardUser")
        self.assertIn("Example+Hospital", page.posts[0].content)

    def test_extract_building_board_request_reads_type_and_google_maps_link(self):
        spec, error = extract_building_board_request(
            "Prison: https://maps.app.goo.gl/example"
        )

        self.assertIsNone(error)
        self.assertIsNotNone(spec)
        self.assertEqual(spec.building_type, "Prison")
        self.assertEqual(spec.location_input, "https://maps.app.goo.gl/example")

    def test_extract_building_board_request_accepts_missing_colon(self):
        spec, error = extract_building_board_request(
            "Prison https://maps.app.goo.gl/mPcFakXaRgMU99fm6"
        )

        self.assertIsNone(error)
        self.assertIsNotNone(spec)
        self.assertEqual(spec.building_type, "Prison")
        self.assertEqual(spec.location_input, "https://maps.app.goo.gl/mPcFakXaRgMU99fm6")

    def test_extract_building_board_request_accepts_link_icon_between_type_and_url(self):
        spec, error = extract_building_board_request(
            "Prison 🔗 https://maps.app.goo.gl/mPcFakXaRgMU99fm6"
        )

        self.assertIsNone(error)
        self.assertIsNotNone(spec)
        self.assertEqual(spec.building_type, "Prison")
        self.assertEqual(spec.location_input, "https://maps.app.goo.gl/mPcFakXaRgMU99fm6")

    def test_extract_building_board_request_accepts_small_type_typos(self):
        prison_spec, prison_error = extract_building_board_request(
            "prisn https://maps.app.goo.gl/example"
        )
        hospital_spec, hospital_error = extract_building_board_request(
            "hosptial https://www.google.com/maps/place/Example"
        )

        self.assertIsNone(prison_error)
        self.assertIsNone(hospital_error)
        self.assertEqual(prison_spec.building_type, "Prison")
        self.assertEqual(hospital_spec.building_type, "Hospital")

    def test_extract_building_board_request_ignores_type_words_inside_url(self):
        spec, error = extract_building_board_request(
            "Prison https://www.google.com/maps/place/Example+Hospital/@40.1,-73.9"
        )

        self.assertIsNone(error)
        self.assertEqual(spec.building_type, "Prison")

    def test_extract_building_board_request_accepts_link_only(self):
        spec, error = extract_building_board_request("https://www.google.com/maps/place/Example")

        self.assertIsNone(error)
        self.assertIsNotNone(spec)
        self.assertIsNone(spec.building_type)
        self.assertEqual(spec.location_input, "https://www.google.com/maps/place/Example")

    def test_building_board_guide_content_explains_format_and_cleanup(self):
        content = build_building_board_guide_content(6165)

        self.assertIn("[BM-GUIDE:overview]", content)
        self.assertIn("<Google Maps link>", content)
        self.assertIn("The bot detects the type", content)
        self.assertIn("Clinics, doctor offices, museums", content)
        self.assertIn("removed after 12 hours", content)
        self.assertNotIn("This post is maintained automatically by the Fire & Rescue Academy bot", content)

    def test_building_board_reply_with_id_returns_new_bot_reply_post_id(self):
        manager = BuildingManager.__new__(BuildingManager)
        reply_page_html = BUILDING_BOARD_HTML.replace(
            "</form>",
            """
<div class="panel panel-default" id="post-on-page-2">
  <div class="panel-body">
    <div class="row">
      <div class="col-md-1">
        <strong><a href="/profile/88649">FireAndRescueAcademy</a></strong>
        <br>
        <span title="June 26, 2026 08:16">June 26, 2026 08:16</span>
      </div>
      <div class="col-md-11">
        <p>[BM-REPLY]<br>Building request received for BoardUser.</p>
      </div>
    </div>
  </div>
  <div class="panel-footer">
    <a href="/alliance_posts/200002/edit">Edit</a>
  </div>
</div>
</form>
""",
        )
        session = _Session(
            {
                "https://www.missionchief.com/alliance_threads/6165": BUILDING_BOARD_HTML,
                "https://www.missionchief.com/alliance_threads/6165?page=2": reply_page_html,
            }
        )
        page = parse_building_board_page(BUILDING_BOARD_HTML)

        status, post_id = asyncio.run(
            manager._post_building_board_reply_with_id(
                session,
                6165,
                page,
                f"{BOARD_REPLY_MARKER}\nBuilding request received for BoardUser.",
            )
        )

        self.assertEqual(status, 200)
        self.assertEqual(post_id, 200002)
        self.assertEqual(session.posts[0][0], "https://www.missionchief.com/alliance_posts?alliance_thread_id=6165")
        self.assertEqual(session.posts[0][1]["data"]["authenticity_token"], "token-building-board")

    def test_delete_building_board_post_submits_delete_method_with_reply_token(self):
        manager = BuildingManager.__new__(BuildingManager)
        session = _Session(
            {
                "https://www.missionchief.com/alliance_threads/6165": BUILDING_BOARD_HTML,
                "https://www.missionchief.com/alliance_threads/6165?page=2": BUILDING_BOARD_HTML,
            }
        )

        deleted, reason = asyncio.run(manager._delete_board_post(session, 6165, 200001))

        self.assertTrue(deleted)
        self.assertEqual(reason, "deleted")
        self.assertEqual(session.posts[-1][0], "https://www.missionchief.com/alliance_posts/200001")
        self.assertEqual(session.posts[-1][1]["data"]["_method"], "delete")
        self.assertEqual(session.posts[-1][1]["data"]["authenticity_token"], "token-building-board")

    def test_board_processing_error_log_falls_back_to_admin_channel(self):
        manager = BuildingManager.__new__(BuildingManager)
        admin_channel = types.SimpleNamespace(send=AsyncMock())
        guild = types.SimpleNamespace(id=1)
        post = parse_building_board_page(BUILDING_BOARD_HTML).posts[0]
        manager._resolve_channel = AsyncMock(
            side_effect=lambda _guild, channel_id: admin_channel if channel_id == 99 else None
        )

        notified = asyncio.run(
            manager._send_board_processing_error_log(
                guild,
                {"log_channel_id": None, "admin_channel_id": 99},
                post,
                RuntimeError("boom"),
            )
        )

        self.assertTrue(notified)
        admin_channel.send.assert_awaited_once()

    def test_board_processing_error_log_prefers_admin_channel(self):
        manager = BuildingManager.__new__(BuildingManager)
        admin_channel = types.SimpleNamespace(send=AsyncMock())
        log_channel = types.SimpleNamespace(send=AsyncMock())
        guild = types.SimpleNamespace(id=1)
        post = parse_building_board_page(BUILDING_BOARD_HTML).posts[0]
        manager._resolve_channel = AsyncMock(
            side_effect=lambda _guild, channel_id: {99: admin_channel, 12: log_channel}.get(channel_id)
        )

        notified = asyncio.run(
            manager._send_board_processing_error_log(
                guild,
                {"log_channel_id": 12, "admin_channel_id": 99},
                post,
                RuntimeError("boom"),
            )
        )

        self.assertTrue(notified)
        admin_channel.send.assert_awaited_once()
        log_channel.send.assert_not_awaited()

    def test_board_poll_targets_skip_guilds_without_admin_channel(self):
        manager = BuildingManager.__new__(BuildingManager)
        unconfigured_guild = types.SimpleNamespace(id=1)
        configured_guild = types.SimpleNamespace(id=2)

        targets = manager._select_board_poll_targets(
            [
                (
                    unconfigured_guild,
                    {
                        "board_poll_enabled": True,
                        "board_thread_id": 6165,
                        "admin_channel_id": None,
                        "log_channel_id": 20,
                    },
                ),
                (
                    configured_guild,
                    {
                        "board_poll_enabled": True,
                        "board_thread_id": 6165,
                        "admin_channel_id": 10,
                        "log_channel_id": None,
                    },
                ),
            ]
        )

        self.assertEqual(targets[6165][0], configured_guild)

    def test_board_post_with_maps_link_is_forwarded_to_discord_admin_approval(self):
        class FakeGuildConfig:
            async def all(self):
                return {
                    "board_auto_accept_enabled": False,
                    "admin_channel_id": None,
                    "log_channel_id": None,
                }

        class FakeConfig:
            def guild(self, _guild):
                return FakeGuildConfig()

        manager = BuildingManager.__new__(BuildingManager)
        manager.config = FakeConfig()
        manager._submit_building_request_to_admins = AsyncMock(return_value=321)
        manager._post_building_board_reply_with_id = AsyncMock(return_value=(200, 200002))
        manager._schedule_board_post_deletion = AsyncMock()
        manager.bot = types.SimpleNamespace(fetch_channel=AsyncMock(return_value=None))
        guild = types.SimpleNamespace(id=1)
        page = BoardPage(posts=[], reply_action="/alliance_posts?alliance_thread_id=6165", reply_token="token")
        post = BoardBuildingPost(
            post_id=200003,
            author_id="88649",
            author_name="DutchFireFighter",
            created_at="June 26, 2026 11:58",
            content="https://maps.app.goo.gl/mPcFakXaRgMU99fm6",
        )
        original_resolve_location = LocationParser.resolve_location

        async def fake_resolve_location(cls, location_input, **_kwargs):
            self.assertEqual(location_input, "https://maps.app.goo.gl/mPcFakXaRgMU99fm6")
            return LocationDetails(
                original_input=location_input,
                resolved_input=location_input,
                place_name="Example Prison",
                coordinates="40.1, -73.9",
                address="Example Address",
                country="United States",
                region="New York",
                maps_url=location_input,
                provider="test",
            )

        try:
            LocationParser.resolve_location = classmethod(fake_resolve_location)
            asyncio.run(manager._handle_building_board_post(guild, object(), 6165, page, post))
        finally:
            LocationParser.resolve_location = original_resolve_location

        manager._submit_building_request_to_admins.assert_awaited_once()
        submitted_args = manager._submit_building_request_to_admins.await_args.args
        submitted_kwargs = manager._submit_building_request_to_admins.await_args.kwargs
        request = submitted_args[1]
        self.assertEqual(submitted_args[0], guild)
        self.assertEqual(submitted_kwargs["source"], "MissionChief board")
        self.assertEqual(submitted_kwargs["board_post"], post)
        self.assertEqual(request.building_type, "Prison")
        self.assertEqual(request.building_name, "Example Prison")
        self.assertEqual(request.location_input, "https://maps.app.goo.gl/mPcFakXaRgMU99fm6")
        self.assertEqual(request.coordinates, "40.1, -73.9")
        manager._post_building_board_reply_with_id.assert_awaited_once()
        reply_content = manager._post_building_board_reply_with_id.await_args.args[3]
        self.assertIn("Request ID: 321", reply_content)

    def test_invalid_board_facility_replies_without_discord_log(self):
        manager = BuildingManager.__new__(BuildingManager)
        manager._resolve_building_location = AsyncMock(
            return_value=LocationDetails(
                original_input="https://maps.app.goo.gl/example",
                resolved_input="https://maps.app.goo.gl/example",
                place_name="Example Clinic",
                coordinates="40.1, -73.9",
                address="Example Clinic, New York",
                detected_facility_type="clinic",
            )
        )
        manager._post_building_board_reply_with_id = AsyncMock(return_value=(200, 200004))
        manager._schedule_board_post_deletion = AsyncMock()
        manager._send_board_request_error_log = AsyncMock()
        page = BoardPage(posts=[], reply_action="/alliance_posts?alliance_thread_id=6165", reply_token="token")
        post = BoardBuildingPost(200003, "123", "BoardUser", "now", "https://maps.app.goo.gl/example")

        asyncio.run(manager._handle_building_board_post(types.SimpleNamespace(id=1), object(), 6165, page, post))

        manager._post_building_board_reply_with_id.assert_awaited_once()
        manager._send_board_request_error_log.assert_not_awaited()
        reply_content = manager._post_building_board_reply_with_id.await_args.args[3]
        self.assertIn("Building request could not be processed", reply_content)
        self.assertIn("clinic", reply_content)

    def test_board_post_with_unknown_tax_is_forwarded_to_admin_review(self):
        class FakeGuildConfig:
            async def all(self):
                return {
                    "board_auto_accept_enabled": True,
                    "admin_channel_id": None,
                    "log_channel_id": None,
                }

        class FakeConfig:
            def guild(self, _guild):
                return FakeGuildConfig()

        manager = BuildingManager.__new__(BuildingManager)
        manager.config = FakeConfig()
        manager.bot = types.SimpleNamespace(fetch_channel=AsyncMock(return_value=None))
        manager._resolve_building_location = AsyncMock(
            return_value=LocationDetails(
                original_input="https://maps.app.goo.gl/example",
                resolved_input="https://maps.app.goo.gl/example",
                place_name="Example Hospital",
                coordinates="40.1, -73.9",
                detected_facility_type="hospital",
            )
        )
        manager._get_board_request_contribution_rate = AsyncMock(return_value=(None, "no snapshot"))
        manager._submit_building_request_to_admins = AsyncMock(return_value=654)
        manager._post_building_board_reply_with_id = AsyncMock(return_value=(200, 200004))
        manager._schedule_board_post_deletion = AsyncMock()
        page = BoardPage(posts=[], reply_action="/alliance_posts?alliance_thread_id=6165", reply_token="token")
        post = BoardBuildingPost(200003, "123", "BoardUser", "now", "https://maps.app.goo.gl/example")

        asyncio.run(manager._handle_building_board_post(types.SimpleNamespace(id=1), object(), 6165, page, post))

        manager._submit_building_request_to_admins.assert_awaited_once()
        request = manager._submit_building_request_to_admins.await_args.args[1]
        self.assertIn("Auto-accept skipped", request.notes)
        reply_content = manager._post_building_board_reply_with_id.await_args.args[3]
        self.assertIn("contribution rate is unknown", reply_content)

    def test_board_post_with_low_tax_is_rejected_without_admin_approval(self):
        class FakeGuildConfig:
            async def all(self):
                return {
                    "board_auto_accept_enabled": True,
                    "admin_channel_id": None,
                    "log_channel_id": None,
                }

        class FakeConfig:
            def guild(self, _guild):
                return FakeGuildConfig()

        class FakeDb:
            def __init__(self):
                self.status = None
                self.actions = []

            def add_request(self, **_kwargs):
                return 777

            def update_request_status(self, request_id, status):
                self.status = (request_id, status)

            def add_action(self, **kwargs):
                self.actions.append(kwargs)

        manager = BuildingManager.__new__(BuildingManager)
        manager.config = FakeConfig()
        manager.db = FakeDb()
        manager.bot = types.SimpleNamespace(get_cog=lambda _name: None, fetch_channel=AsyncMock(return_value=None))
        manager._resolve_building_location = AsyncMock(
            return_value=LocationDetails(
                original_input="https://maps.app.goo.gl/example",
                resolved_input="https://maps.app.goo.gl/example",
                place_name="Example Prison",
                coordinates="40.1, -73.9",
                detected_facility_type="prison",
            )
        )
        manager._get_board_request_contribution_rate = AsyncMock(return_value=(4.9, "test"))
        manager._submit_building_request_to_admins = AsyncMock()
        manager._post_building_board_reply_with_id = AsyncMock(return_value=(200, 200004))
        manager._schedule_board_post_deletion = AsyncMock()
        page = BoardPage(posts=[], reply_action="/alliance_posts?alliance_thread_id=6165", reply_token="token")
        post = BoardBuildingPost(200003, "123", "BoardUser", "now", "https://maps.app.goo.gl/example")

        asyncio.run(manager._handle_building_board_post(types.SimpleNamespace(id=1), object(), 6165, page, post))

        manager._submit_building_request_to_admins.assert_not_awaited()
        self.assertEqual(manager.db.status, (777, "denied"))
        self.assertEqual(manager.db.actions[0]["action_type"], "auto_denied_low_tax")
        reply_content = manager._post_building_board_reply_with_id.await_args.args[3]
        self.assertIn("below the required", reply_content)

    def test_board_post_with_valid_tax_uses_auto_accept(self):
        class FakeGuildConfig:
            async def all(self):
                return {
                    "board_auto_accept_enabled": True,
                    "admin_channel_id": None,
                    "log_channel_id": None,
                }

        class FakeConfig:
            def guild(self, _guild):
                return FakeGuildConfig()

        manager = BuildingManager.__new__(BuildingManager)
        manager.config = FakeConfig()
        manager.bot = types.SimpleNamespace(fetch_channel=AsyncMock(return_value=None))
        manager._resolve_building_location = AsyncMock(
            return_value=LocationDetails(
                original_input="https://maps.app.goo.gl/example",
                resolved_input="https://maps.app.goo.gl/example",
                place_name="Example Hospital",
                coordinates="40.1, -73.9",
                detected_facility_type="hospital",
            )
        )
        manager._get_board_request_contribution_rate = AsyncMock(return_value=(5.0, "test"))
        manager._auto_accept_board_building_request = AsyncMock(return_value="Auto-built.")
        manager._submit_building_request_to_admins = AsyncMock()
        manager._post_building_board_reply_with_id = AsyncMock(return_value=(200, 200004))
        manager._schedule_board_post_deletion = AsyncMock()
        page = BoardPage(posts=[], reply_action="/alliance_posts?alliance_thread_id=6165", reply_token="token")
        post = BoardBuildingPost(200003, "123", "BoardUser", "now", "https://maps.app.goo.gl/example")

        asyncio.run(manager._handle_building_board_post(types.SimpleNamespace(id=1), object(), 6165, page, post))

        manager._submit_building_request_to_admins.assert_not_awaited()
        manager._auto_accept_board_building_request.assert_awaited_once()
        reply_content = manager._post_building_board_reply_with_id.await_args.args[3]
        self.assertIn("Auto-built.", reply_content)

    def test_building_request_game_update_sends_message_for_board_requester(self):
        message_manager = types.SimpleNamespace(
            _send_message_and_link=AsyncMock(
                return_value={
                    "ok": True,
                    "reason": "Message Sent.",
                    "resolved_username": "BoardUser",
                    "conversation_id": "12345",
                    "thread": None,
                }
            )
        )
        manager = BuildingManager.__new__(BuildingManager)
        manager.bot = types.SimpleNamespace(
            get_cog=lambda name: message_manager if name == "MessageManager" else None
        )
        request = BuildingRequest(
            user_id=0,
            username="BoardUser",
            building_type="Prison",
            building_name="Example Prison",
            location_input="https://maps.app.goo.gl/example",
            coordinates="40.1, -73.9",
            address="Example Address",
            request_id=123,
        )

        body = manager._build_game_approval_message(request, status="Your building has been created in MissionChief.")
        result = asyncio.run(
            manager._send_building_request_game_update(
                request,
                subject="Building request approved",
                body=body,
            )
        )

        self.assertTrue(result["ok"])
        message_manager._send_message_and_link.assert_awaited_once_with(
            "BoardUser",
            "Building request approved",
            body,
        )
        self.assertIn("Example Prison", body)
        self.assertIn("Your building has been created in MissionChief.", body)
        self.assertNotIn("Coordinates:", body)
        self.assertNotIn("Address:", body)
        self.assertNotIn("Request ID:", body)

    def test_building_request_game_update_skips_discord_requester(self):
        message_manager = types.SimpleNamespace(_send_message_and_link=AsyncMock())
        manager = BuildingManager.__new__(BuildingManager)
        manager.bot = types.SimpleNamespace(
            get_cog=lambda name: message_manager if name == "MessageManager" else None
        )
        request = BuildingRequest(
            user_id=555,
            username="DiscordUser",
            building_type="Hospital",
            building_name="Example Hospital",
            location_input="https://maps.app.goo.gl/example",
        )

        result = asyncio.run(
            manager._send_building_request_game_update(
                request,
                subject="Building request approved",
                body="Approved",
            )
        )

        self.assertIsNone(result)
        message_manager._send_message_and_link.assert_not_awaited()

    def test_board_request_submission_posts_admin_approval_with_location_fields(self):
        class FakeGuildConfig:
            async def all(self):
                return {"admin_channel_id": 10, "log_channel_id": 20}

        class FakeConfig:
            def guild(self, _guild):
                return FakeGuildConfig()

        class FakeDb:
            def __init__(self):
                self.added = None

            def add_request(self, **kwargs):
                self.added = kwargs
                return 123

        admin_channel = types.SimpleNamespace(send=AsyncMock())
        log_channel = types.SimpleNamespace(send=AsyncMock())
        guild = types.SimpleNamespace(
            id=1,
            get_member=lambda _member_id: None,
            get_channel=lambda channel_id: {10: admin_channel, 20: log_channel}.get(channel_id),
        )
        manager = BuildingManager.__new__(BuildingManager)
        manager.config = FakeConfig()
        manager.db = FakeDb()
        manager.bot = types.SimpleNamespace(fetch_channel=AsyncMock(return_value=None))
        request = BuildingRequest(
            user_id=0,
            username="BoardUser",
            building_type="Prison",
            building_name="Example Prison",
            location_input="https://maps.app.goo.gl/mPcFakXaRgMU99fm6",
            coordinates="40.1, -73.9",
            address="Example Address",
            country="United States",
            region="New York",
            maps_url="https://maps.app.goo.gl/mPcFakXaRgMU99fm6",
            facility_warning=None,
            notes="MissionChief board post #200001",
        )
        post = parse_building_board_page(BUILDING_BOARD_HTML).posts[0]

        request_id = asyncio.run(
            manager._submit_building_request_to_admins(
                guild,
                request,
                source="MissionChief board",
                board_post=post,
            )
        )

        self.assertEqual(request_id, 123)
        self.assertEqual(manager.db.added["building_type"], "Prison")
        admin_channel.send.assert_awaited_once()
        admin_embed = admin_channel.send.await_args.kwargs["embed"]
        admin_fields = {field["name"]: field["value"] for field in admin_embed.fields}
        self.assertEqual(admin_fields["Coordinates"], "40.1, -73.9")
        self.assertEqual(admin_fields["Country / Region"], "New York, United States")
        self.assertEqual(admin_fields["Address"], "Example Address")
        self.assertIn("maps.app.goo.gl", admin_fields["Maps URL"])
        log_channel.send.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
