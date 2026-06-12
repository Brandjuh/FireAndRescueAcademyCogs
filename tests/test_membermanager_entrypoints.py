import asyncio
import types
import unittest
from unittest.mock import AsyncMock

from MemberManager.membermanager import DEFAULTS, MemberManager
from MemberManager.models import MemberData
from MemberManager.views import MemberOverviewView, RefreshButton


class MemberManagerEntrypointTests(unittest.TestCase):
    def test_default_panel_channel_is_configured(self):
        self.assertEqual(DEFAULTS["panel_channel_id"], 1426226521231589507)
        self.assertIsNone(DEFAULTS["panel_message_id"])

    def test_missionchief_search_resolves_exact_id(self):
        cog = MemberManager.__new__(MemberManager)
        cog.alliance_scraper = None
        cog._get_mc_data = AsyncMock(
            return_value={
                "user_id": "12345",
                "name": "DutchFireFighter",
            }
        )

        results = asyncio.run(cog._search_missionchief_members("12345"))

        self.assertEqual(results[0]["mc_user_id"], "12345")
        self.assertEqual(results[0]["name"], "DutchFireFighter")

    def test_missionchief_search_resolves_name_from_alliance_scraper(self):
        cog = MemberManager.__new__(MemberManager)
        cog.alliance_scraper = types.SimpleNamespace(
            get_members=AsyncMock(
                return_value=[
                    {"user_id": "111", "name": "Alpha"},
                    {"user_id": "222", "name": "DutchFireFighter"},
                ]
            )
        )
        cog._get_mc_data = AsyncMock(return_value=None)

        results = asyncio.run(cog._search_missionchief_members("Dutch"))

        self.assertEqual(results[0]["mc_user_id"], "222")
        self.assertEqual(results[0]["name"], "DutchFireFighter")

    def test_member_candidate_search_returns_ranked_discord_and_mc_matches(self):
        class FakeMember:
            bot = False

            def __init__(self, member_id, username, display_name):
                self.id = member_id
                self.username = username
                self.display_name = display_name

            def __str__(self):
                return self.username

        cog = MemberManager.__new__(MemberManager)
        cog.membersync = types.SimpleNamespace(get_link_for_mc=AsyncMock(return_value=None))
        cog.alliance_scraper = types.SimpleNamespace(
            get_members=AsyncMock(
                return_value=[
                    {"user_id": "456", "name": "DutchFireFighter"},
                ]
            )
        )
        guild = types.SimpleNamespace(
            members=[
                FakeMember(123, "DutchFireFighter#0001", "Dutch"),
                FakeMember(999, "Other#0001", "Other"),
            ]
        )

        results = asyncio.run(cog._search_member_candidates(guild, "Dutch"))

        self.assertGreaterEqual(len(results), 2)
        self.assertEqual(results[0]["discord_id"], 123)
        self.assertTrue(any(result.get("mc_user_id") == "456" for result in results))

    def test_panel_search_shows_choices_for_ambiguous_fuzzy_matches(self):
        cog = MemberManager.__new__(MemberManager)
        cog._interaction_is_moderator = AsyncMock(return_value=True)
        cog._search_member_candidates = AsyncMock(
            return_value=[
                {"score": 0.9, "discord_id": 123, "name": "Franny192", "source": "discord"},
                {"score": 0.88, "mc_user_id": "456", "name": "Franny", "source": "missionchief"},
            ]
        )
        cog._resolve_target = AsyncMock()
        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(),
            user=types.SimpleNamespace(id=999),
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        asyncio.run(cog._open_member_profile_from_interaction(interaction, "Franny"))

        cog._resolve_target.assert_not_awaited()
        interaction.response.send_message.assert_awaited_once()
        kwargs = interaction.response.send_message.await_args.kwargs
        self.assertTrue(kwargs["ephemeral"])
        self.assertEqual(kwargs["embed"].kwargs["title"], "Member Search: Franny")

    def test_send_panel_message_stores_message_id(self):
        stored = {}
        cog = MemberManager.__new__(MemberManager)
        cog.config = types.SimpleNamespace(
            panel_message_id=types.SimpleNamespace(
                set=AsyncMock(side_effect=lambda value: stored.update({"id": value}))
            )
        )
        channel = types.SimpleNamespace(
            send=AsyncMock(return_value=types.SimpleNamespace(id=98765))
        )

        message = asyncio.run(cog._send_panel_message(channel))

        self.assertEqual(message.id, 98765)
        self.assertEqual(stored["id"], 98765)
        channel.send.assert_awaited_once()

    def test_context_menu_registration_syncs_panel_guild(self):
        calls = {}

        class Tree:
            def remove_command(self, name, *, type=None, guild=None):
                calls["removed"] = (name, type, guild.id)

            def add_command(self, command, *, guild=None):
                calls["added"] = (command.name, guild.id)

            async def sync(self, *, guild=None):
                calls["synced"] = guild.id

        guild = types.SimpleNamespace(id=111)
        channel = types.SimpleNamespace(guild=guild)
        cog = MemberManager.__new__(MemberManager)
        cog._member_context_menu = types.SimpleNamespace(name="Member Management")
        cog._context_menu_guild = None
        cog.config = types.SimpleNamespace(
            panel_channel_id=AsyncMock(return_value=222),
        )
        cog.bot = types.SimpleNamespace(
            tree=Tree(),
            get_channel=lambda channel_id: channel if channel_id == 222 else None,
            guilds=[],
        )

        asyncio.run(cog._register_context_menu())

        self.assertEqual(calls["removed"][0], "Member Management")
        self.assertEqual(calls["added"], ("Member Management", 111))
        self.assertEqual(calls["synced"], 111)
        self.assertEqual(cog._context_menu_guild.id, 111)

    def test_send_member_profile_refreshes_integrations_before_building_view(self):
        payload = {
            "membersync": object(),
            "alliance_scraper": object(),
            "logs_scraper": object(),
            "sanction_manager": object(),
        }
        cog = MemberManager.__new__(MemberManager)
        cog.bot = types.SimpleNamespace()
        cog.db = object()
        cog.config = object()
        cog._connect_integrations = AsyncMock()
        cog._get_integrations_payload = lambda: payload
        send = AsyncMock()

        asyncio.run(
            cog._send_member_profile(
                send,
                types.SimpleNamespace(id=1),
                999,
                MemberData(discord_id=123, discord_username="DiscordUser"),
            )
        )

        cog._connect_integrations.assert_awaited_once()
        view = send.await_args.kwargs["view"]
        self.assertEqual(view.integrations, payload)

    def test_refresh_button_refreshes_integrations_for_existing_view(self):
        payload = {
            "membersync": object(),
            "alliance_scraper": object(),
            "logs_scraper": object(),
            "sanction_manager": object(),
        }
        updated_data = MemberData(discord_id=123, discord_username="Updated")
        cog = types.SimpleNamespace(
            db=object(),
            _connect_integrations=AsyncMock(),
            _get_integrations_payload=lambda: payload,
            _build_member_data=AsyncMock(return_value=updated_data),
        )
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.bot = types.SimpleNamespace(get_cog=lambda name: cog if name == "MemberManager" else None)
        view.member_data = MemberData(discord_id=123, mc_user_id="456")
        view.integrations = {}
        view._update_view = AsyncMock()
        button = RefreshButton(view, row=4)
        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            response=types.SimpleNamespace(defer=AsyncMock()),
        )

        asyncio.run(button.callback(interaction))

        cog._connect_integrations.assert_awaited_once()
        self.assertEqual(view.integrations, payload)
        self.assertIs(view.db, cog.db)
        self.assertIs(view.member_data, updated_data)
        view._update_view.assert_awaited_once_with(interaction)

    def test_connect_integrations_accepts_sanctionsmanager_cog_name(self):
        sanction_manager = object()
        calls = []

        def get_cog(name):
            calls.append(name)
            if name == "SanctionsManager":
                return sanction_manager
            return None

        cog = MemberManager.__new__(MemberManager)
        cog.bot = types.SimpleNamespace(get_cog=get_cog)

        asyncio.run(cog._connect_integrations())

        self.assertIs(cog.sanction_manager, sanction_manager)
        self.assertIn("SanctionManager", calls)
        self.assertIn("SanctionsManager", calls)


if __name__ == "__main__":
    unittest.main()
