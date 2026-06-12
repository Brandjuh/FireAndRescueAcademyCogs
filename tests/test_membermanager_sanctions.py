import asyncio
import importlib
import importlib.util
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from MemberManager.models import MemberData
from MemberManager.views import (
    CreateSanctionModal,
    EditSanctionModal,
    MemberOverviewView,
    RemoveSanctionModal,
)


def load_sanctions_database_class():
    module_path = Path(__file__).resolve().parents[1] / "sanctionmanager" / "sanction_manager.py"
    spec = importlib.util.spec_from_file_location("sanction_manager_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.SanctionsDatabase


def load_sanctions_manager_class():
    module_path = Path(__file__).resolve().parents[1] / "sanctionmanager" / "sanction_manager.py"
    spec = importlib.util.spec_from_file_location("sanction_manager_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.SanctionsManager


def load_sanction_manager_module():
    module_path = Path(__file__).resolve().parents[1] / "sanctionmanager" / "sanction_manager.py"
    spec = importlib.util.spec_from_file_location("sanction_manager_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MemberManagerSanctionsTests(unittest.TestCase):
    def test_sanctionmanager_package_exports_loadable_cog(self):
        module = importlib.import_module("sanctionmanager")

        self.assertTrue(hasattr(module, "SanctionsManager"))

    def test_sanctionmanager_default_panel_channel_is_configured(self):
        module = load_sanction_manager_module()

        self.assertEqual(module.DEFAULT_PANEL_CHANNEL_ID, 1426226521231589507)

    def test_sanction_manager_permission_helper_accepts_administrator(self):
        SanctionsManager = load_sanctions_manager_class()
        manager = SanctionsManager.__new__(SanctionsManager)
        guild = types.SimpleNamespace(id=1)
        user = types.SimpleNamespace(
            guild_permissions=types.SimpleNamespace(administrator=True),
            roles=[],
        )

        allowed = asyncio.run(manager.can_manage_sanctions(guild, user))

        self.assertTrue(allowed)

    def test_sanction_manager_permission_helper_accepts_configured_role(self):
        SanctionsManager = load_sanctions_manager_class()
        role = object()
        manager = SanctionsManager.__new__(SanctionsManager)
        manager.config = types.SimpleNamespace(
            guild=lambda guild: types.SimpleNamespace(admin_role_id=AsyncMock(return_value=99))
        )
        guild = types.SimpleNamespace(id=1, get_role=lambda role_id: role if role_id == 99 else None)
        user = types.SimpleNamespace(
            guild_permissions=types.SimpleNamespace(administrator=False),
            roles=[role],
        )

        allowed = asyncio.run(manager.can_manage_sanctions(guild, user))

        self.assertTrue(allowed)

    def test_sanction_manager_permission_helper_rejects_unprivileged_member(self):
        SanctionsManager = load_sanctions_manager_class()
        manager = SanctionsManager.__new__(SanctionsManager)
        manager.config = types.SimpleNamespace(
            guild=lambda guild: types.SimpleNamespace(admin_role_id=AsyncMock(return_value=99))
        )
        guild = types.SimpleNamespace(id=1, get_role=lambda role_id: object())
        user = types.SimpleNamespace(
            guild_permissions=types.SimpleNamespace(administrator=False),
            roles=[],
        )

        allowed = asyncio.run(manager.can_manage_sanctions(guild, user))

        self.assertFalse(allowed)

    def test_sanctionmanager_send_panel_message_stores_message_id(self):
        module = load_sanction_manager_module()
        stored = {}

        class GuildConfig:
            panel_message_id = types.SimpleNamespace(
                set=AsyncMock(side_effect=lambda value: stored.update({"id": value}))
            )

        cog = module.SanctionsManager.__new__(module.SanctionsManager)
        cog.config = types.SimpleNamespace(guild=lambda guild: GuildConfig())
        guild = types.SimpleNamespace(id=1)
        channel = types.SimpleNamespace(
            send=AsyncMock(return_value=types.SimpleNamespace(id=98765))
        )

        message = asyncio.run(cog._send_panel_message(guild, channel))

        self.assertEqual(message.id, 98765)
        self.assertEqual(stored["id"], 98765)
        channel.send.assert_awaited_once()

    def test_sanctionmanager_deduplicates_existing_panel_messages(self):
        module = load_sanction_manager_module()
        stored = {}

        class GuildConfig:
            panel_message_id = types.SimpleNamespace(
                set=AsyncMock(side_effect=lambda value: stored.update({"id": value}))
            )

        def make_panel_message(message_id):
            return types.SimpleNamespace(
                id=message_id,
                author=types.SimpleNamespace(id=555),
                embeds=[types.SimpleNamespace(title="Sanction Management")],
                components=[],
                delete=AsyncMock(),
            )

        keep = make_panel_message(300)
        duplicate = make_panel_message(200)
        unrelated = types.SimpleNamespace(
            id=100,
            author=types.SimpleNamespace(id=555),
            embeds=[types.SimpleNamespace(title="Other")],
            components=[],
            delete=AsyncMock(),
        )

        class Channel:
            async def history(self, limit=50):
                for message in [keep, duplicate, unrelated]:
                    yield message

        cog = module.SanctionsManager.__new__(module.SanctionsManager)
        cog.bot = types.SimpleNamespace(user=types.SimpleNamespace(id=555))
        cog.config = types.SimpleNamespace(guild=lambda guild: GuildConfig())
        guild = types.SimpleNamespace(id=1)

        result = asyncio.run(cog._deduplicate_panel_messages(guild, Channel()))

        self.assertIs(result, keep)
        self.assertEqual(stored["id"], 300)
        duplicate.delete.assert_awaited_once()
        unrelated.delete.assert_not_awaited()

    def test_sanctionmanager_context_menu_registration_syncs_panel_guild(self):
        module = load_sanction_manager_module()
        calls = {}

        class Tree:
            def remove_command(self, name, *, type=None, guild=None):
                calls["removed"] = (name, type, guild.id)

            def add_command(self, command, *, guild=None):
                calls["added"] = (command.name, guild.id)

            async def sync(self, *, guild=None):
                calls["synced"] = guild.id

        cog = module.SanctionsManager.__new__(module.SanctionsManager)
        cog._sanction_context_menu = types.SimpleNamespace(name="Sanction Member")
        cog.bot = types.SimpleNamespace(
            tree=Tree(),
            guilds=[types.SimpleNamespace(id=111)],
        )

        asyncio.run(cog._register_context_menu())

        self.assertEqual(calls["removed"][0], "Sanction Member")
        self.assertEqual(calls["added"], ("Sanction Member", 111))
        self.assertEqual(calls["synced"], 111)
        self.assertEqual(cog._context_menu_guild.id, 111)

    def test_sanctionmanager_context_menu_opens_sanction_flow_for_discord_member(self):
        module = load_sanction_manager_module()

        member = types.SimpleNamespace(
            id=123,
            display_name="Server Nick",
            mention="<@123>",
            __str__=lambda self: "DiscordUser#0001",
        )
        target = {
            "score": 1.0,
            "discord_id": 123,
            "discord_username": "DiscordUser#0001",
            "discord_display_name": "Server Nick",
            "discord_member": member,
            "mc_user_id": "456",
            "mc_username": "MCUser",
            "name": "MCUser",
        }

        cog = module.SanctionsManager.__new__(module.SanctionsManager)
        cog._is_admin = AsyncMock(return_value=True)
        cog.search_sanction_targets = AsyncMock(return_value=[target])
        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            user=types.SimpleNamespace(id=999, __str__=lambda self: "Admin"),
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        asyncio.run(cog._sanction_context_menu_callback(interaction, member))

        cog.search_sanction_targets.assert_awaited_once_with(
            interaction.guild,
            "123",
            threshold=1.0,
            limit=1,
        )
        interaction.response.send_message.assert_awaited_once()
        kwargs = interaction.response.send_message.await_args.kwargs
        self.assertTrue(kwargs["ephemeral"])
        self.assertEqual(kwargs["content"], "Select the type of sanction:")
        self.assertEqual(kwargs["view"].target_discord_id, 123)
        self.assertEqual(kwargs["view"].target_mc_id, "456")

    def test_sanction_database_matches_discord_and_mc_ids_together(self):
        SanctionsDatabase = load_sanctions_database_class()

        with tempfile.TemporaryDirectory() as temp_dir:
            database = SanctionsDatabase(str(Path(temp_dir) / "sanctions.db"))
            database.add_sanction(
                guild_id=1,
                discord_user_id=123,
                mc_user_id=None,
                mc_username="Discord Only",
                admin_user_id=999,
                admin_username="Admin",
                sanction_type="Warning - Official 1st",
                reason_category="Conduct",
                reason_detail="Discord-linked sanction",
                additional_notes=None,
            )
            database.add_sanction(
                guild_id=1,
                discord_user_id=None,
                mc_user_id="456",
                mc_username="MCUser",
                admin_user_id=999,
                admin_username="Admin",
                sanction_type="Warning - Official 2nd",
                reason_category="Conduct",
                reason_detail="MC-linked sanction",
                additional_notes=None,
            )

            sanctions = database.get_user_sanctions(
                guild_id=1,
                discord_user_id=123,
                mc_user_id="456",
            )
            warnings = database.get_active_warnings(
                guild_id=1,
                discord_user_id=123,
                mc_user_id="456",
            )

        self.assertEqual(len(sanctions), 2)
        self.assertEqual(len(warnings), 2)
        self.assertEqual(
            {sanction["reason_detail"] for sanction in sanctions},
            {"Discord-linked sanction", "MC-linked sanction"},
        )

    def test_sanction_summary_separates_active_expired_removed_and_history(self):
        SanctionsDatabase = load_sanctions_database_class()
        now = 1_800_000_000
        sanctions = [
            {
                "sanction_id": 1,
                "sanction_type": "Warning - Official 1st",
                "created_at": now - (31 * 86400),
                "status": "active",
            },
            {
                "sanction_id": 2,
                "sanction_type": "Kick",
                "created_at": now - 100,
                "status": "active",
            },
            {
                "sanction_id": 3,
                "sanction_type": "Warning - Verbal warning",
                "created_at": now - 200,
                "status": "removed",
            },
        ]

        summary = SanctionsDatabase.summarize_sanctions(sanctions, now=now)

        self.assertEqual(summary["active_count"], 1)
        self.assertEqual(summary["expired_count"], 1)
        self.assertEqual(summary["removed_count"], 1)
        self.assertEqual(summary["historical_count"], 3)
        self.assertEqual(
            {sanction["sanction_id"]: sanction["effective_status"] for sanction in summary["sanctions"]},
            {1: "expired", 2: "active", 3: "removed"},
        )

    def test_sanction_stats_contract_defines_status_and_staff_activity_counts(self):
        SanctionsDatabase = load_sanctions_database_class()
        SanctionsManager = load_sanctions_manager_class()

        with tempfile.TemporaryDirectory() as temp_dir:
            database = SanctionsDatabase(str(Path(temp_dir) / "sanctions.db"))
            active_id = database.add_sanction(
                guild_id=1,
                discord_user_id=123,
                mc_user_id="456",
                mc_username="ActiveUser",
                admin_user_id=9001,
                admin_username="AdminOne",
                sanction_type="Warning - Official 1st",
                reason_category="Conduct",
                reason_detail="Active warning",
                additional_notes=None,
            )
            expired_id = database.add_sanction(
                guild_id=1,
                discord_user_id=124,
                mc_user_id="457",
                mc_username="ExpiredUser",
                admin_user_id=9001,
                admin_username="AdminOne",
                sanction_type="Warning - Official 1st",
                reason_category="Conduct",
                reason_detail="Expired warning",
                additional_notes=None,
                expires_at=1,
            )
            removed_id = database.add_sanction(
                guild_id=1,
                discord_user_id=125,
                mc_user_id="458",
                mc_username="RemovedUser",
                admin_user_id=9002,
                admin_username="AdminTwo",
                sanction_type="Kick",
                reason_category="Activity",
                reason_detail="Removed kick",
                additional_notes=None,
            )
            database.update_sanction_status(
                removed_id,
                "removed",
                admin_user_id=9002,
                notes="Resolved",
            )

            manager = SanctionsManager.__new__(SanctionsManager)
            manager.db = database

            stats = manager.get_sanction_stats(1)

        self.assertNotEqual(active_id, expired_id)
        self.assertEqual(stats["issued_total"], 3)
        self.assertEqual(stats["historical_count"], 3)
        self.assertEqual(stats["active_count"], 1)
        self.assertEqual(stats["expired_count"], 1)
        self.assertEqual(stats["removed_count"], 1)
        self.assertEqual(stats["type_counts"]["Warning - Official 1st"], 2)
        self.assertEqual(stats["type_counts"]["Kick"], 1)
        self.assertEqual(stats["reason_counts"]["Conduct"], 2)
        self.assertEqual(stats["history_action_counts"]["created"], 3)
        self.assertEqual(stats["history_action_counts"]["status_changed_to_removed"], 1)
        self.assertEqual(stats["staff_activity_total"], 4)

    def test_sanctions_embed_uses_view_guild_without_message_object(self):
        class FakeSanctionDB:
            def __init__(self):
                self.call = None

            def get_user_sanctions(self, **kwargs):
                self.call = kwargs
                return [
                    {
                        "sanction_id": 1,
                        "sanction_type": "Warning - Official 1st",
                        "reason_category": "Conduct",
                        "reason_detail": "Needs attention",
                        "admin_username": "Admin",
                        "mc_username": "SanctionMCUser",
                        "created_at": 1_800_000_000,
                        "status": "active",
                    }
                ]

        sanction_db = FakeSanctionDB()
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(
            discord_id=123,
            mc_user_id="456",
            discord_username="DiscordUser",
            mc_username="MCUser",
        )
        view.integrations = {"sanction_manager": types.SimpleNamespace(db=sanction_db)}
        view.guild = types.SimpleNamespace(id=1)
        view.message = None
        view.infraction_page = 0
        view.infractions_per_page = 5

        embed = asyncio.run(view.get_infractions_embed())

        self.assertIn("Active Sanction", embed.kwargs["title"])
        self.assertIn("SanctionMCUser", embed.kwargs["title"])
        self.assertNotIn("DiscordUser", embed.kwargs["title"])
        self.assertEqual(
            sanction_db.call,
            {
                "guild_id": 1,
                "discord_user_id": 123,
                "mc_user_id": "456",
            },
        )

    def test_sanctions_embed_prefers_public_sanction_contract(self):
        class FakeSanctionManager:
            def __init__(self):
                self.call = None
                self.db = types.SimpleNamespace(
                    get_user_sanctions=lambda **kwargs: (_ for _ in ()).throw(AssertionError("db fallback used"))
                )

            def get_member_sanctions(self, **kwargs):
                self.call = kwargs
                return [
                    {
                        "sanction_id": 1,
                        "sanction_type": "Warning - Official 1st",
                        "reason_category": "Conduct",
                        "reason_detail": "Needs attention",
                        "admin_username": "Admin",
                        "mc_username": "SanctionMCUser",
                        "created_at": 1_800_000_000,
                        "status": "active",
                    }
                ]

        sanction_manager = FakeSanctionManager()
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(
            discord_id=123,
            mc_user_id="456",
            discord_username="DiscordUser",
            mc_username="MCUser",
        )
        view.integrations = {"sanction_manager": sanction_manager}
        view.guild = types.SimpleNamespace(id=1)
        view.infraction_page = 0
        view.infractions_per_page = 5

        embed = asyncio.run(view.get_infractions_embed())

        self.assertIn("Active Sanction", embed.kwargs["title"])
        self.assertIn("SanctionMCUser", embed.kwargs["title"])
        self.assertNotIn("DiscordUser", embed.kwargs["title"])
        self.assertEqual(
            sanction_manager.call,
            {
                "guild_id": 1,
                "discord_user_id": 123,
                "mc_user_id": "456",
            },
        )

    def test_sanctions_embed_prefers_summary_contract_for_status_counts(self):
        class FakeSanctionManager:
            def __init__(self):
                self.call = None
                self.db = types.SimpleNamespace(
                    get_user_sanctions=lambda **kwargs: (_ for _ in ()).throw(AssertionError("db fallback used"))
                )

            def get_member_sanction_summary(self, **kwargs):
                self.call = kwargs
                return {
                    "sanctions": [
                        {
                            "sanction_id": 1,
                            "sanction_type": "Warning - Official 1st",
                            "reason_category": "Conduct",
                            "reason_detail": "Old warning",
                            "admin_username": "Admin",
                            "created_at": 1_700_000_000,
                            "status": "active",
                            "effective_status": "expired",
                            "_display_expired": True,
                        },
                        {
                            "sanction_id": 2,
                            "sanction_type": "Kick",
                            "reason_category": "Conduct",
                            "reason_detail": "Removed kick",
                            "admin_username": "Admin",
                            "created_at": 1_700_000_100,
                            "status": "removed",
                            "effective_status": "removed",
                            "_display_expired": False,
                        },
                    ],
                    "active_count": 0,
                    "expired_count": 1,
                    "removed_count": 1,
                    "historical_count": 2,
                }

        sanction_manager = FakeSanctionManager()
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(
            discord_id=123,
            mc_user_id="456",
            discord_username="DiscordUser",
            mc_username="MCUser",
        )
        view.integrations = {"sanction_manager": sanction_manager}
        view.guild = types.SimpleNamespace(id=1)
        view.infraction_page = 0
        view.infractions_per_page = 5

        embed = asyncio.run(view.get_infractions_embed())

        self.assertEqual(embed.description, "✅ No active sanctions")
        self.assertEqual(
            sanction_manager.call,
            {
                "guild_id": 1,
                "discord_user_id": 123,
                "mc_user_id": "456",
            },
        )
        self.assertIn("Expired warnings: 1", embed.fields[0]["value"])
        self.assertIn("Removed sanctions: 1", embed.fields[0]["value"])

    def test_sanctions_list_counts_expired_and_removed_statuses_separately(self):
        class FakeSanctionManager:
            def get_member_sanctions(self, **kwargs):
                del kwargs
                return [
                    {
                        "sanction_id": 1,
                        "sanction_type": "Warning - Official 1st",
                        "reason_detail": "Active warning",
                        "created_at": 1_800_000_000,
                        "effective_status": "active",
                    },
                    {
                        "sanction_id": 2,
                        "sanction_type": "Warning - Official 2nd",
                        "reason_detail": "Second active warning",
                        "created_at": 1_800_000_100,
                        "effective_status": "active",
                    },
                    {
                        "sanction_id": 3,
                        "sanction_type": "Warning - Official 1st",
                        "reason_detail": "Expired warning",
                        "created_at": 1_700_000_000,
                        "effective_status": "expired",
                    },
                    {
                        "sanction_id": 4,
                        "sanction_type": "Kick",
                        "reason_detail": "Removed sanction",
                        "created_at": 1_700_000_100,
                        "effective_status": "removed",
                    },
                ]

        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(discord_id=123, mc_user_id="456", discord_username="DiscordUser")
        view.integrations = {"sanction_manager": FakeSanctionManager()}
        view.guild = types.SimpleNamespace(id=1)
        view.infraction_page = 0
        view.infractions_per_page = 10

        embed = asyncio.run(view.get_infractions_embed())

        self.assertIn("Sanctions List", embed.kwargs["title"])
        self.assertIn("Active: 2", embed.footer["text"])
        self.assertIn("Expired: 1", embed.footer["text"])
        self.assertIn("Removed: 1", embed.footer["text"])

    def test_sanction_manager_public_contract_delegates_to_database(self):
        SanctionsManager = load_sanctions_manager_class()
        calls = {}

        class FakeDB:
            def get_sanction(self, sanction_id):
                calls["get_one"] = sanction_id
                return {"sanction_id": sanction_id}

            def get_user_sanctions(self, **kwargs):
                calls["get"] = kwargs
                return [{"sanction_id": 1}]

            def add_sanction(self, **kwargs):
                calls["add"] = kwargs
                return 42

            def edit_sanction(self, sanction_id, admin_user_id, **updates):
                calls["edit"] = (sanction_id, admin_user_id, updates)

            def update_sanction_status(self, sanction_id, status, admin_user_id, notes):
                calls["remove"] = (sanction_id, status, admin_user_id, notes)

        manager = SanctionsManager.__new__(SanctionsManager)
        manager.db = FakeDB()

        sanction = manager.get_sanction_by_id(42)
        sanctions = manager.get_member_sanctions(
            guild_id=1,
            discord_user_id=123,
            mc_user_id="456",
        )
        sanction_id = manager.create_sanction_for_member(
            guild_id=1,
            discord_user_id=123,
            mc_user_id="456",
            mc_username="MCUser",
            admin_user_id=999,
            admin_username="Admin",
            sanction_type="Warning",
            reason_category="Conduct",
            reason_detail="Reason",
            additional_notes="Notes",
        )
        manager.edit_member_sanction(42, admin_user_id=999, reason_detail="Updated")
        manager.remove_member_sanction(42, admin_user_id=999, notes="Resolved")

        self.assertEqual(sanction, {"sanction_id": 42})
        self.assertEqual(calls["get_one"], 42)
        self.assertEqual(sanctions[0]["sanction_id"], 1)
        self.assertEqual(sanctions[0]["effective_status"], "active")
        self.assertEqual(sanction_id, 42)
        self.assertEqual(calls["get"]["mc_user_id"], "456")
        self.assertEqual(calls["add"]["sanction_type"], "Warning")
        self.assertEqual(calls["edit"], (42, 999, {"reason_detail": "Updated"}))
        self.assertEqual(calls["remove"], (42, "removed", 999, "Resolved"))

    def test_sanction_manager_records_membermanager_audit_when_available(self):
        SanctionsManager = load_sanctions_manager_class()
        add_event = AsyncMock()
        manager = SanctionsManager.__new__(SanctionsManager)
        manager.bot = types.SimpleNamespace(
            get_cog=lambda name: types.SimpleNamespace(db=types.SimpleNamespace(add_event=add_event))
            if name == "MemberManager"
            else None
        )

        asyncio.run(
            manager._record_membermanager_sanction_event(
                guild_id=1,
                sanction={
                    "sanction_id": 42,
                    "discord_user_id": 123,
                    "mc_user_id": "456",
                    "sanction_type": "Warning",
                },
                event_type="sanction_added",
                actor_id=999,
                event_data={"source": "SanctionManager"},
            )
        )

        add_event.assert_awaited_once_with(
            guild_id=1,
            discord_id=123,
            mc_user_id="456",
            event_type="sanction_added",
            event_data={
                "sanction_id": 42,
                "sanction_type": "Warning",
                "source": "SanctionManager",
            },
            triggered_by="sanctionmanager",
            actor_id=999,
        )

    def test_sanction_target_search_finds_missionchief_member_by_exact_id(self):
        SanctionsManager = load_sanctions_manager_class()

        class FakeMembersScraper:
            async def get_members(self):
                return [
                    {"mc_user_id": "456", "name": "DutchFireFighter"},
                    {"mc_user_id": "789", "name": "Other Member"},
                ]

        manager = SanctionsManager.__new__(SanctionsManager)
        manager.bot = types.SimpleNamespace(
            get_cog=lambda name: FakeMembersScraper() if name == "MembersScraper" else None
        )
        guild = types.SimpleNamespace(members=[], get_member=lambda member_id: None)

        results = asyncio.run(manager.search_sanction_targets(guild, "456"))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["score"], 1.0)
        self.assertEqual(results[0]["mc_user_id"], "456")
        self.assertEqual(results[0]["mc_username"], "DutchFireFighter")
        self.assertEqual(results[0]["source"], "missionchief")

    def test_sanction_target_search_finds_missionchief_member_by_fuzzy_name(self):
        SanctionsManager = load_sanctions_manager_class()

        class FakeMembersScraper:
            async def get_members(self):
                return [
                    {"mc_user_id": "456", "name": "DutchFireFighter"},
                    {"mc_user_id": "789", "name": "Franny192"},
                ]

        manager = SanctionsManager.__new__(SanctionsManager)
        manager.bot = types.SimpleNamespace(
            get_cog=lambda name: FakeMembersScraper() if name == "MembersScraper" else None
        )
        guild = types.SimpleNamespace(members=[], get_member=lambda member_id: None)

        results = asyncio.run(manager.search_sanction_targets(guild, "DutchFire", threshold=0.5))

        self.assertEqual(results[0]["mc_user_id"], "456")
        self.assertEqual(results[0]["mc_username"], "DutchFireFighter")

    def test_sanction_target_search_enriches_missionchief_member_with_discord_link(self):
        SanctionsManager = load_sanctions_manager_class()

        discord_member = types.SimpleNamespace(
            id=123,
            name="DiscordUser",
            nick=None,
            display_name="Server Nick",
            bot=False,
            __str__=lambda self: "DiscordUser#0001",
        )

        class FakeMembersScraper:
            async def get_members(self):
                return [{"mc_user_id": "456", "name": "MCUser"}]

        class FakeMemberSync:
            async def get_link_for_mc(self, mc_user_id):
                return {"discord_id": 123, "mc_user_id": mc_user_id}

            async def get_link_for_discord(self, discord_id):
                return {"discord_id": discord_id, "mc_user_id": "456"}

        def get_cog(name):
            if name == "MembersScraper":
                return FakeMembersScraper()
            if name == "MemberSync":
                return FakeMemberSync()
            return None

        manager = SanctionsManager.__new__(SanctionsManager)
        manager.bot = types.SimpleNamespace(get_cog=get_cog)
        guild = types.SimpleNamespace(
            members=[discord_member],
            get_member=lambda member_id: discord_member if member_id == 123 else None,
        )

        results = asyncio.run(manager.search_sanction_targets(guild, "MCUser"))

        self.assertEqual(results[0]["mc_user_id"], "456")
        self.assertEqual(results[0]["discord_id"], 123)
        self.assertIs(results[0]["discord_member"], discord_member)

    def test_sanction_search_modal_uses_alliance_lookup_contract(self):
        module = load_sanction_manager_module()

        class FakeCog:
            async def search_sanction_targets(self, guild, query, limit=5):
                self.call = (guild, query, limit)
                return [
                    {
                        "score": 1.0,
                        "discord_id": None,
                        "discord_member": None,
                        "mc_user_id": "456",
                        "mc_username": "MCOnlyUser",
                        "name": "MCOnlyUser",
                    }
                ]

        cog = FakeCog()
        modal = module.DiscordMemberModal(cog)
        modal.member_input.value = "MCOnlyUser"

        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            user=types.SimpleNamespace(id=999, __str__=lambda self: "Admin"),
            response=types.SimpleNamespace(defer=AsyncMock()),
            followup=types.SimpleNamespace(send=AsyncMock()),
        )

        asyncio.run(modal.on_submit(interaction))

        self.assertEqual(cog.call, (interaction.guild, "MCOnlyUser", 5))
        interaction.followup.send.assert_awaited_once()
        kwargs = interaction.followup.send.await_args.kwargs
        self.assertEqual(kwargs["content"], "Select the type of sanction:")
        self.assertEqual(kwargs["view"].target_mc_id, "456")
        self.assertEqual(kwargs["view"].target_mc_username, "MCOnlyUser")
        self.assertIsNone(kwargs["view"].target_discord_id)

    def test_sanction_manager_audit_hook_is_noop_without_membermanager(self):
        SanctionsManager = load_sanctions_manager_class()
        manager = SanctionsManager.__new__(SanctionsManager)
        manager.bot = types.SimpleNamespace(get_cog=lambda name: None)

        asyncio.run(
            manager._record_membermanager_sanction_event(
                guild_id=1,
                sanction={"sanction_id": 42},
                event_type="sanction_removed",
                actor_id=999,
            )
        )

    def test_create_sanction_modal_uses_public_contract(self):
        calls = {}

        class FakeSanctionManager:
            db = types.SimpleNamespace(
                add_sanction=lambda **kwargs: (_ for _ in ()).throw(AssertionError("db fallback used"))
            )

            def create_sanction_for_member(self, **kwargs):
                calls["create"] = kwargs
                return 42

        parent = types.SimpleNamespace(
            integrations={"sanction_manager": FakeSanctionManager()},
            member_data=MemberData(
                discord_id=123,
                mc_user_id="456",
                discord_username="DiscordUser",
                mc_username="MCUser",
            ),
            db=types.SimpleNamespace(add_event=AsyncMock()),
            _update_view=AsyncMock(),
        )
        modal = CreateSanctionModal(parent)
        modal.sanction_type.value = "Warning"
        modal.reason_category.value = "Conduct"
        modal.reason_detail.value = "Reason"
        modal.admin_notes.value = "Internal"
        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            user=types.SimpleNamespace(id=999, __str__=lambda self: "Admin"),
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        asyncio.run(modal.on_submit(interaction))

        self.assertEqual(calls["create"]["discord_user_id"], 123)
        self.assertEqual(calls["create"]["mc_user_id"], "456")
        self.assertEqual(calls["create"]["sanction_type"], "Warning")
        parent.db.add_event.assert_awaited_once()
        parent._update_view.assert_awaited_once_with(interaction)

    def test_edit_and_remove_sanction_modals_use_public_contracts(self):
        calls = {}

        class FakeSanctionManager:
            db = types.SimpleNamespace(
                get_sanction=lambda sanction_id: (_ for _ in ()).throw(AssertionError("db fallback used")),
                edit_sanction=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("db fallback used")),
                update_sanction_status=lambda *args, **kwargs: (_ for _ in ()).throw(
                    AssertionError("db fallback used")
                ),
            )

            def get_sanction_by_id(self, sanction_id):
                calls.setdefault("get", []).append(sanction_id)
                return {"sanction_id": sanction_id, "guild_id": 1}

            def edit_member_sanction(self, sanction_id, *, admin_user_id, **updates):
                calls["edit"] = (sanction_id, admin_user_id, updates)

            def remove_member_sanction(self, sanction_id, *, admin_user_id, notes=None):
                calls["remove"] = (sanction_id, admin_user_id, notes)

        parent = types.SimpleNamespace(
            integrations={"sanction_manager": FakeSanctionManager()},
            member_data=MemberData(discord_id=123, mc_user_id="456"),
            db=types.SimpleNamespace(add_event=AsyncMock()),
            _update_view=AsyncMock(),
        )
        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            user=types.SimpleNamespace(id=999, __str__=lambda self: "Admin"),
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        edit_modal = EditSanctionModal(parent)
        edit_modal.sanction_id.value = "42"
        edit_modal.new_reason.value = "Updated reason"
        edit_modal.new_notes.value = ""
        asyncio.run(edit_modal.on_submit(interaction))

        remove_modal = RemoveSanctionModal(parent)
        remove_modal.sanction_id.value = "42"
        remove_modal.reason.value = "Resolved"
        remove_modal.confirm.value = "REMOVE"
        asyncio.run(remove_modal.on_submit(interaction))

        self.assertEqual(calls["get"], [42, 42])
        self.assertEqual(calls["edit"], (42, 999, {"reason_detail": "Updated reason"}))
        self.assertEqual(calls["remove"][0], 42)
        self.assertEqual(calls["remove"][1], 999)
        self.assertIn("Resolved", calls["remove"][2])

    def test_sanctions_embed_has_quiet_fallback_without_backend(self):
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(discord_id=123, mc_user_id="456", discord_username="DiscordUser")
        view.integrations = {"sanction_manager": None}
        view.guild = types.SimpleNamespace(id=1)

        embed = asyncio.run(view.get_infractions_embed())

        self.assertIn("Sanction data is currently unavailable", embed.description)
        self.assertNotIn("SanctionManager not available", embed.description)


if __name__ == "__main__":
    unittest.main()
