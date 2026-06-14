import asyncio
import importlib
import importlib.util
import sqlite3
import tempfile
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

from MemberManager.models import MemberData
from MemberManager.views import (
    CreateSanctionModal,
    EditSanctionModal,
    MemberOverviewView,
    RemoveSanctionModal,
    SearchSanctionQueryModal,
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

    def test_sanctionmanager_default_review_channel_is_configured(self):
        module = load_sanction_manager_module()

        self.assertEqual(module.DEFAULT_REVIEW_CHANNEL_ID, 1421625293130567690)

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
        self.assertEqual(kwargs["content"], "Sanction wizard: choose the matching reason first.")
        self.assertEqual(kwargs["view"].target_discord_id, 123)
        self.assertEqual(kwargs["view"].target_mc_id, "456")

    def test_sanctionmanager_panel_start_opens_member_search_modal_directly(self):
        module = load_sanction_manager_module()
        cog = module.SanctionsManager.__new__(module.SanctionsManager)
        cog._is_admin = AsyncMock(return_value=True)
        view = module.StartView(cog)
        interaction = types.SimpleNamespace(
            response=types.SimpleNamespace(send_modal=AsyncMock(), send_message=AsyncMock()),
        )

        asyncio.run(view.start(interaction, types.SimpleNamespace()))

        interaction.response.send_modal.assert_awaited_once()
        self.assertIsInstance(interaction.response.send_modal.await_args.args[0], module.DiscordMemberModal)
        interaction.response.send_message.assert_not_awaited()

    def test_reason_first_wizard_finds_reason_matches_and_sends_selection(self):
        module = load_sanction_manager_module()
        manager = module.SanctionsManager.__new__(module.SanctionsManager)
        manager.db = types.SimpleNamespace(get_custom_rules=lambda guild_id: [])
        manager._is_admin = AsyncMock(return_value=True)
        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            user=types.SimpleNamespace(id=999, __str__=lambda self: "Admin"),
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        asyncio.run(
            manager.start_sanction_wizard_for_target(
                interaction,
                {
                    "discord_id": 123,
                    "mc_user_id": "456",
                    "mc_username": "MCUser",
                    "name": "MCUser",
                },
            )
        )

        interaction.response.send_message.assert_awaited_once()
        kwargs = interaction.response.send_message.await_args.kwargs
        self.assertEqual(kwargs["content"], "Sanction wizard: choose the matching reason first.")
        self.assertTrue(kwargs["ephemeral"])
        self.assertGreater(len(kwargs["view"].reason_matches), 0)
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
            {
                "sanction_id": 4,
                "sanction_type": "Kick",
                "created_at": now - 300,
                "status": "unverified",
            },
        ]

        summary = SanctionsDatabase.summarize_sanctions(sanctions, now=now)

        self.assertEqual(summary["active_count"], 1)
        self.assertEqual(summary["unverified_count"], 1)
        self.assertEqual(summary["expired_count"], 1)
        self.assertEqual(summary["removed_count"], 1)
        self.assertEqual(summary["historical_count"], 4)
        self.assertEqual(
            {sanction["sanction_id"]: sanction["effective_status"] for sanction in summary["sanctions"]},
            {1: "expired", 2: "active", 3: "removed", 4: "unverified"},
        )

    def test_sanction_summary_tracks_warning_patterns_for_member_decisions(self):
        SanctionsDatabase = load_sanctions_database_class()
        now = 1_800_000_000
        sanctions = [
            {
                "sanction_id": 1,
                "sanction_type": "Warning - Official 1st",
                "reason_detail": "Low contribution",
                "created_at": now - 100,
                "status": "active",
            },
            {
                "sanction_id": 2,
                "sanction_type": "Warning - Official 2nd",
                "reason_detail": "Low contribution",
                "created_at": now - 200,
                "status": "active",
            },
            {
                "sanction_id": 3,
                "sanction_type": "Warning - Official 1st",
                "reason_detail": "Low contribution",
                "created_at": now - (31 * 86400),
                "status": "active",
            },
            {
                "sanction_id": 4,
                "sanction_type": "Warning - Verbal warning",
                "reason_detail": "Discord conduct",
                "created_at": now - 300,
                "status": "active",
            },
            {
                "sanction_id": 5,
                "sanction_type": "Warning - Verbal warning",
                "reason_detail": "Low contribution",
                "created_at": now - 400,
                "status": "removed",
            },
        ]

        insights = SanctionsDatabase.summarize_sanctions(sanctions, now=now)["warning_insights"]

        self.assertEqual(insights["total_warnings"], 4)
        self.assertEqual(insights["active_warnings"], 3)
        self.assertEqual(insights["official_warnings"], 3)
        self.assertEqual(insights["verbal_warnings"], 1)
        self.assertEqual(insights["repeated_reasons"], {"Low contribution": 3})
        self.assertIn("High active warning count", insights["signals"])
        self.assertIn("Repeated warning history", insights["signals"])
        self.assertIn("Repeated warning reason", insights["signals"])

    def test_sanction_summary_warns_when_same_reason_reaches_third_warning(self):
        module = load_sanction_manager_module()

        class FakeCog:
            def get_member_reason_warning_count(self, **kwargs):
                self.call = kwargs
                return 2

        cog = FakeCog()
        view = module.SummarySanctionView(
            cog,
            admin_user_id=999,
            admin_username="Admin",
            target_discord_id=None,
            target_mc_id="456",
            target_mc_username="CrashTestDummy",
            target_discord_user=None,
            sanction_type="Warning - Official 3rd and last warning",
            reason_category="Contribution",
            reason_detail="Low contribution",
        )

        embed = view._create_embed(guild_id=1)
        fields = {field["name"]: field["value"] for field in embed.fields}

        alert = fields["⚠️⚠️ REPEATED WARNING ALERT ⚠️⚠️"]
        self.assertIn("warning #3", alert)
        self.assertIn("exact same reason", alert)
        self.assertIn("Low contribution", alert)
        self.assertIn("escalation", alert)
        self.assertEqual(cog.call["guild_id"], 1)
        self.assertEqual(cog.call["mc_user_id"], "456")
        self.assertEqual(cog.call["reason_detail"], "Low contribution")

    def test_sanction_summary_prefers_admin_missionchief_name(self):
        module = load_sanction_manager_module()

        class FakeCog:
            def get_member_reason_warning_count(self, **kwargs):
                return 0

        view = module.SummarySanctionView(
            FakeCog(),
            admin_user_id=999,
            admin_username="brandjuh",
            target_discord_id=None,
            target_mc_id="456",
            target_mc_username="CrashTestDummy",
            target_discord_user=None,
            sanction_type="Warning - Official 1st warning",
            reason_category="Contribution",
            reason_detail="Low contribution",
        )
        view.admin_display_name = "DutchFireFighter"

        embed = view._create_embed(guild_id=1)
        fields = {field["name"]: field["value"] for field in embed.fields}

        self.assertEqual(fields["Admin"], "DutchFireFighter")

    def test_sanction_summary_history_button_sends_private_history(self):
        module = load_sanction_manager_module()
        history_embed = module.discord.Embed(title="Sanction History - CrashTestDummy")

        class FakeCog:
            def build_member_sanction_history_embed(self, **kwargs):
                self.call = kwargs
                return history_embed

        cog = FakeCog()
        view = module.SummarySanctionView(
            cog,
            admin_user_id=999,
            admin_username="Admin",
            target_discord_id=None,
            target_mc_id="456",
            target_mc_username="CrashTestDummy",
            target_discord_user=None,
            sanction_type="Warning - Verbal warning",
            reason_category="Contribution",
            reason_detail="Low contribution",
        )
        guild = types.SimpleNamespace(id=1)
        interaction = types.SimpleNamespace(
            guild=guild,
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        asyncio.run(view.view_history(interaction, None))

        interaction.response.send_message.assert_awaited_once_with(
            embed=history_embed,
            ephemeral=True,
        )
        self.assertEqual(cog.call["guild"], guild)
        self.assertEqual(cog.call["mc_user_id"], "456")
        self.assertEqual(cog.call["mc_username"], "CrashTestDummy")

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
            unverified_id = database.add_sanction(
                guild_id=1,
                discord_user_id=126,
                mc_user_id="459",
                mc_username="PendingUser",
                admin_user_id=9003,
                admin_username="AdminThree",
                sanction_type="Ban",
                reason_category="Game Log",
                reason_detail="Pending game-log confirmation",
                additional_notes=None,
                status="unverified",
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
            stored_unverified = database.get_sanction(unverified_id)

        self.assertNotEqual(active_id, expired_id)
        self.assertEqual(stats["issued_total"], 4)
        self.assertEqual(stats["historical_count"], 4)
        self.assertEqual(stats["active_count"], 1)
        self.assertEqual(stats["unverified_count"], 1)
        self.assertEqual(stats["expired_count"], 1)
        self.assertEqual(stats["removed_count"], 1)
        self.assertEqual(stats["type_counts"]["Warning - Official 1st"], 2)
        self.assertEqual(stats["type_counts"]["Kick"], 1)
        self.assertEqual(stats["type_counts"]["Ban"], 1)
        self.assertEqual(stats["reason_counts"]["Conduct"], 2)
        self.assertEqual(stats["history_action_counts"]["created"], 4)
        self.assertEqual(stats["history_action_counts"]["status_changed_to_removed"], 1)
        self.assertEqual(stats["staff_activity_total"], 5)
        self.assertEqual(stored_unverified["status"], "unverified")

    def test_sanction_stats_contract_supports_period_counts(self):
        SanctionsDatabase = load_sanctions_database_class()
        SanctionsManager = load_sanctions_manager_class()

        with tempfile.TemporaryDirectory() as temp_dir:
            database = SanctionsDatabase(str(Path(temp_dir) / "sanctions.db"))
            first_id = database.add_sanction(
                guild_id=1,
                discord_user_id=123,
                mc_user_id="456",
                mc_username="PeriodUser",
                admin_user_id=9001,
                admin_username="AdminOne",
                sanction_type="Warning - Official 1st",
                reason_category="Conduct",
                reason_detail="Period warning",
                additional_notes=None,
            )
            second_id = database.add_sanction(
                guild_id=1,
                discord_user_id=124,
                mc_user_id="457",
                mc_username="OutsideUser",
                admin_user_id=9001,
                admin_username="AdminOne",
                sanction_type="Kick",
                reason_category="Conduct",
                reason_detail="Outside kick",
                additional_notes=None,
            )
            database.update_sanction_status(
                first_id,
                "removed",
                admin_user_id=9001,
                notes="Resolved in period",
            )

            connection = sqlite3.connect(database.db_path)
            connection.execute(
                "UPDATE sanctions SET created_at = ? WHERE sanction_id = ?",
                (1000, first_id),
            )
            connection.execute(
                "UPDATE sanctions SET created_at = ? WHERE sanction_id = ?",
                (5000, second_id),
            )
            connection.execute(
                "UPDATE sanction_history SET action_at = ? WHERE sanction_id = ?",
                (1200, first_id),
            )
            connection.execute(
                "UPDATE sanction_history SET action_at = ? WHERE sanction_id = ?",
                (5000, second_id),
            )
            connection.commit()
            connection.close()

            manager = SanctionsManager.__new__(SanctionsManager)
            manager.db = database

            stats = manager.get_sanction_stats(
                1,
                period_start_ts=900,
                period_end_ts=2000,
            )

        self.assertEqual(stats["issued_total"], 2)
        self.assertEqual(stats["issued_period"], 1)
        self.assertEqual(stats["by_type_period"]["warnings"], 1)
        self.assertEqual(stats["by_type_period"]["kicks"], 0)
        self.assertEqual(stats["staff_activity_period"], 2)

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
                        {
                            "sanction_id": 3,
                            "sanction_type": "Ban",
                            "reason_category": "Game Log",
                            "reason_detail": "Pending game-log confirmation",
                            "admin_username": "Admin",
                            "created_at": 1_700_000_200,
                            "status": "unverified",
                            "effective_status": "unverified",
                            "_display_expired": False,
                        },
                    ],
                    "active_count": 0,
                    "unverified_count": 1,
                    "expired_count": 1,
                    "removed_count": 1,
                    "historical_count": 3,
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
        self.assertIn("Pending review: 1", embed.fields[0]["value"])

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
                    {
                        "sanction_id": 5,
                        "sanction_type": "Ban",
                        "reason_detail": "Pending review",
                        "created_at": 1_700_000_200,
                        "effective_status": "unverified",
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
        self.assertIn("Pending: 1", embed.footer["text"])
        self.assertIn("Expired: 1", embed.footer["text"])
        self.assertIn("Removed: 1", embed.footer["text"])

    def test_sanctions_embed_shows_warning_pattern_context(self):
        class FakeSanctionManager:
            def get_member_sanction_summary(self, **kwargs):
                del kwargs
                sanctions = [
                    {
                        "sanction_id": 1,
                        "sanction_type": "Warning - Official 1st",
                        "reason_detail": "Low contribution",
                        "created_at": 1_800_000_000,
                        "effective_status": "active",
                    },
                    {
                        "sanction_id": 2,
                        "sanction_type": "Warning - Official 2nd",
                        "reason_detail": "Low contribution",
                        "created_at": 1_800_000_100,
                        "effective_status": "active",
                    },
                    {
                        "sanction_id": 3,
                        "sanction_type": "Warning - Verbal warning",
                        "reason_detail": "Discord conduct",
                        "created_at": 1_800_000_200,
                        "effective_status": "active",
                    },
                ]
                return {
                    "sanctions": sanctions,
                    "active_count": 3,
                    "unverified_count": 0,
                    "expired_count": 0,
                    "removed_count": 0,
                    "historical_count": 3,
                    "warning_insights": {
                        "total_warnings": 3,
                        "active_warnings": 3,
                        "official_warnings": 2,
                        "verbal_warnings": 1,
                        "repeated_reasons": {"Low contribution": 2},
                        "signals": ["High active warning count", "Repeated warning reason"],
                    },
                }

        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(discord_id=123, mc_user_id="456", mc_username="MCUser")
        view.integrations = {"sanction_manager": FakeSanctionManager()}
        view.guild = types.SimpleNamespace(id=1)
        view.infraction_page = 0
        view.infractions_per_page = 10

        embed = asyncio.run(view.get_infractions_embed())
        warning_field = next(field for field in embed.fields if field["name"] == "Warning Pattern")

        self.assertIn("Active warnings: 3", warning_field["value"])
        self.assertIn("Historical warnings: 3", warning_field["value"])
        self.assertIn("Low contribution (2x)", warning_field["value"])
        self.assertIn("High active warning count", warning_field["value"])

    def test_sanction_search_modal_finds_member_sanctions_by_reason_text(self):
        class FakeSanctionManager:
            def get_member_sanction_summary(self, **kwargs):
                del kwargs
                return {
                    "sanctions": [
                        {
                            "sanction_id": 1,
                            "sanction_type": "Kick",
                            "reason_detail": "Other reason",
                            "created_at": 1_800_000_000,
                            "effective_status": "active",
                        },
                        {
                            "sanction_id": 2,
                            "sanction_type": "Warning - Official 1st",
                            "reason_category": "Contribution",
                            "reason_detail": "Low contribution",
                            "admin_username": "Admin",
                            "mc_username": "MCUser",
                            "created_at": 1_800_000_100,
                            "effective_status": "active",
                        },
                    ],
                    "warning_insights": {
                        "total_warnings": 1,
                        "active_warnings": 1,
                        "official_warnings": 1,
                        "verbal_warnings": 0,
                        "repeated_reasons": {},
                        "signals": [],
                    },
                }

        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(discord_id=123, mc_user_id="456", mc_username="MCUser")
        view.integrations = {"sanction_manager": FakeSanctionManager()}
        view.guild = types.SimpleNamespace(id=1)
        modal = SearchSanctionQueryModal(view)
        modal.sanction_query.value = "contribution"
        interaction = types.SimpleNamespace(
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        asyncio.run(modal.on_submit(interaction))

        interaction.response.send_message.assert_awaited_once()
        embed = interaction.response.send_message.await_args.kwargs["embed"]
        self.assertEqual(embed.kwargs["title"], "Sanction Details - #2")
        self.assertIn("Low contribution", embed.fields[1]["value"])

    def test_sanction_manager_public_contract_delegates_to_database(self):
        SanctionsManager = load_sanctions_manager_class()
        calls = {}

        class FakeDB:
            def get_sanction(self, sanction_id):
                calls["get_one"] = sanction_id
                return {"sanction_id": sanction_id}

            def get_user_sanctions(self, **kwargs):
                calls["get"] = kwargs
                return [
                    {
                        "sanction_id": 1,
                        "sanction_type": "Warning",
                        "reason_detail": "Reason",
                        "status": "active",
                    }
                ]

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
        warning_insights = manager.get_member_warning_insights(
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
            status="unverified",
        )
        manager.edit_member_sanction(42, admin_user_id=999, reason_detail="Updated")
        manager.remove_member_sanction(42, admin_user_id=999, notes="Resolved")

        self.assertEqual(sanction, {"sanction_id": 42})
        self.assertEqual(calls["get_one"], 42)
        self.assertEqual(sanctions[0]["sanction_id"], 1)
        self.assertEqual(sanctions[0]["effective_status"], "active")
        self.assertEqual(warning_insights["total_warnings"], 1)
        self.assertEqual(warning_insights["active_warnings"], 1)
        self.assertEqual(sanction_id, 42)
        self.assertEqual(calls["get"]["mc_user_id"], "456")
        self.assertEqual(calls["add"]["sanction_type"], "Warning")
        self.assertEqual(calls["add"]["status"], "unverified")
        self.assertEqual(calls["edit"], (42, 999, {"reason_detail": "Updated"}))
        self.assertEqual(calls["remove"], (42, "removed", 999, "Resolved"))

    def test_sanction_manager_create_contract_records_membermanager_audit(self):
        SanctionsManager = load_sanctions_manager_class()
        add_event = AsyncMock()
        created_tasks = []

        class FakeDB:
            def add_sanction(self, **kwargs):
                self.add = kwargs
                return 84

        class FakeLoop:
            def create_task(self, coro):
                created_tasks.append(coro)

        manager = SanctionsManager.__new__(SanctionsManager)
        manager.db = FakeDB()
        manager.bot = types.SimpleNamespace(
            get_cog=lambda name: types.SimpleNamespace(db=types.SimpleNamespace(add_event=add_event))
            if name == "MemberManager"
            else None
        )

        original_get_running_loop = asyncio.get_running_loop
        asyncio.get_running_loop = lambda: FakeLoop()
        try:
            sanction_id = manager.create_sanction_for_member(
                guild_id=1,
                discord_user_id=None,
                mc_user_id="456",
                mc_username="CrashTestDummy",
                admin_user_id=999,
                admin_username="Admin",
                sanction_type="Warning - Official 1st",
                reason_category="Contribution",
                reason_detail="Low contribution",
            )
        finally:
            asyncio.get_running_loop = original_get_running_loop

        self.assertEqual(sanction_id, 84)
        self.assertEqual(len(created_tasks), 1)
        asyncio.run(created_tasks[0])
        add_event.assert_awaited_once()
        kwargs = add_event.await_args.kwargs
        self.assertEqual(kwargs["mc_user_id"], "456")
        self.assertEqual(kwargs["event_type"], "sanction_added")
        self.assertEqual(kwargs["event_data"]["target_name"], "CrashTestDummy")
        self.assertEqual(kwargs["event_data"]["reason_detail"], "Low contribution")

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
                    "mc_username": "CrashTestDummy",
                    "sanction_type": "Warning",
                    "reason_category": "Contribution",
                    "reason_detail": "Low contribution",
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
                "reason_category": "Contribution",
                "reason_detail": "Low contribution",
                "target_name": "CrashTestDummy",
                "mc_username": "CrashTestDummy",
                "source": "SanctionManager",
            },
            triggered_by="sanctionmanager",
            actor_id=999,
        )

    def test_game_log_review_scan_creates_unverified_sanctions_once(self):
        module = load_sanction_manager_module()
        SanctionsDatabase = module.SanctionsDatabase
        SanctionsManager = module.SanctionsManager

        rows = [
            {
                "id": 10,
                "action_key": "expansion_finished",
                "ts": "June 13, 2026 17:35",
                "affected_name": "Hospital",
            },
            {
                "id": 11,
                "action_key": "kicked_from_alliance",
                "ts": "June 13, 2026 19:45",
                "executed_name": "DutchFireFighter",
                "executed_mc_id": "1",
                "affected_name": "Ezekiel27366",
                "affected_mc_id": "1251176",
                "description": "Kicked from the alliance",
            },
            {
                "id": 12,
                "action_key": "chat_ban_set",
                "ts": "June 13, 2026 19:41",
                "executed_name": "DutchFireFighter",
                "executed_mc_id": "1",
                "affected_name": "velvethunder",
                "affected_mc_id": "555",
                "description": "Chat ban set (13 Jun 19:46)",
            },
        ]

        class FakeLogsScraper:
            async def get_logs_after(self, last_id, limit=100):
                return [row for row in rows if row["id"] > last_id][:limit]

            async def get_recent_logs(self, limit=100):
                return rows[-limit:]

        class FakeValue:
            def __init__(self, value):
                self.value = value

            async def __call__(self):
                return self.value

            async def set(self, value):
                self.value = value

        class FakeGuildConfig:
            def __init__(self):
                self.game_log_review_last_id = FakeValue(0)
                self.game_log_review_channel_id = FakeValue(1421625293130567690)
                self.game_log_review_enabled = FakeValue(True)

        class FakeConfig:
            def __init__(self):
                self.guild_config = FakeGuildConfig()

            def guild(self, guild):
                return self.guild_config

        class FakeChannel:
            def __init__(self):
                self.messages = []

            async def send(self, **kwargs):
                message = types.SimpleNamespace(id=len(self.messages) + 100, **kwargs)
                self.messages.append(message)
                return message

        channel = FakeChannel()
        guild = types.SimpleNamespace(
            id=1,
            get_channel=lambda channel_id: channel if channel_id == 1421625293130567690 else None,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            database = SanctionsDatabase(str(Path(temp_dir) / "sanctions.db"))
            manager = SanctionsManager.__new__(SanctionsManager)
            manager.db = database
            manager.config = FakeConfig()
            manager.bot = types.SimpleNamespace(
                get_cog=lambda name: FakeLogsScraper() if name == "LogsScraper" else None,
                get_channel=lambda channel_id: channel if channel_id == 1421625293130567690 else None,
            )

            first = asyncio.run(manager.scan_recent_game_log_reviews(guild))
            second = asyncio.run(manager.scan_recent_game_log_reviews(guild))
            stats = manager.get_sanction_stats(1)
            kick = database.get_user_sanctions(guild_id=1, mc_user_id="1251176")[0]
            ban = database.get_user_sanctions(guild_id=1, mc_user_id="555")[0]
            kick_review = database.get_game_log_review(11)
            ban_review = database.get_game_log_review(12)

        self.assertEqual(first["scanned"], 3)
        self.assertEqual(first["created"], 2)
        self.assertEqual(first["skipped"], 1)
        self.assertEqual(first["last_id"], 12)
        self.assertEqual(second["scanned"], 3)
        self.assertEqual(second["created"], 0)
        self.assertEqual(second["skipped"], 3)
        self.assertEqual(stats["unverified_count"], 2)
        self.assertEqual(stats["active_count"], 0)
        self.assertEqual(kick["sanction_type"], "Kick")
        self.assertEqual(kick["status"], "unverified")
        self.assertEqual(ban["sanction_type"], "Mute")
        self.assertEqual(ban["status"], "unverified")
        self.assertIsNotNone(kick_review)
        self.assertIsNotNone(ban_review)
        self.assertEqual(len(channel.messages), 2)

    def test_game_log_review_scan_skips_existing_recent_manual_sanction(self):
        module = load_sanction_manager_module()
        SanctionsDatabase = module.SanctionsDatabase
        SanctionsManager = module.SanctionsManager

        row = {
            "id": 21,
            "action_key": "kicked_from_alliance",
            "ts": "June 13, 2026 19:45",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "executed_name": "DutchFireFighter",
            "executed_mc_id": "1",
            "affected_name": "CrashTestDummy",
            "affected_mc_id": "456",
            "description": "Kicked from the alliance",
        }

        class FakeChannel:
            def __init__(self):
                self.messages = []

            async def send(self, **kwargs):
                message = types.SimpleNamespace(id=len(self.messages) + 100, **kwargs)
                self.messages.append(message)
                return message

        class FakeValue:
            def __init__(self, value):
                self.value = value

            async def __call__(self):
                return self.value

            async def set(self, value):
                self.value = value

        class FakeGuildConfig:
            def __init__(self):
                self.game_log_review_channel_id = FakeValue(1421625293130567690)
                self.game_log_review_last_id = FakeValue(0)

        class FakeConfig:
            def __init__(self):
                self.guild_config = FakeGuildConfig()

            def guild(self, guild):
                return self.guild_config

        channel = FakeChannel()
        guild = types.SimpleNamespace(
            id=1,
            get_channel=lambda channel_id: channel if channel_id == 1421625293130567690 else None,
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            database = SanctionsDatabase(str(Path(temp_dir) / "sanctions.db"))
            manual_id = database.add_sanction(
                guild_id=1,
                discord_user_id=None,
                mc_user_id="456",
                mc_username="CrashTestDummy",
                admin_user_id=999,
                admin_username="Admin",
                sanction_type="Kick",
                reason_category="Manual",
                reason_detail="Kicked from the alliance",
                additional_notes=None,
            )
            manager = SanctionsManager.__new__(SanctionsManager)
            manager.db = database
            manager.config = FakeConfig()
            manager.bot = types.SimpleNamespace(
                get_cog=lambda name: None,
                get_channel=lambda channel_id: channel if channel_id == 1421625293130567690 else None,
            )

            result = asyncio.run(manager._process_game_log_review_rows(guild, [row]))
            sanctions = database.get_user_sanctions(guild_id=1, mc_user_id="456")
            review = database.get_game_log_review(21)

        self.assertEqual(result["created"], 0)
        self.assertEqual(result["skipped"], 1)
        self.assertEqual(len(sanctions), 1)
        self.assertEqual(sanctions[0]["sanction_id"], manual_id)
        self.assertEqual(review["sanction_id"], manual_id)
        self.assertEqual(review["review_status"], "already_recorded")
        self.assertEqual(len(channel.messages), 0)

    def test_game_log_review_embed_is_compact_and_shows_discord_nickname(self):
        module = load_sanction_manager_module()
        SanctionsManager = module.SanctionsManager
        manager = SanctionsManager.__new__(SanctionsManager)
        discord_member = types.SimpleNamespace(
            display_name="ServerNick",
            mention="<@123>",
        )
        guild = types.SimpleNamespace(get_member=lambda user_id: discord_member if user_id == 123 else None)

        embed = manager._build_game_log_review_embed(
            guild=guild,
            row={
                "id": 18514,
                "ts": "June 13, 2026 19:41",
                "executed_name": "DutchFireFighter",
                "executed_mc_id": "88649",
                "affected_name": "velvethunder",
                "affected_mc_id": "375558",
                "description": "Chat ban set",
            },
            sanction_id=83,
            sanction_type="Mute",
            discord_user_id=123,
        )

        field_values = {field["name"]: field["value"] for field in embed.fields}
        self.assertEqual(embed.kwargs["title"], "Sanction Review Required")
        self.assertNotIn("description", embed.kwargs)
        self.assertEqual(field_values["Member"], "velvethunder (`375558`)")
        self.assertEqual(field_values["Discord Server Nickname"], "ServerNick (<@123>)")

    def test_game_log_review_view_defines_edit_actions(self):
        module = load_sanction_manager_module()

        self.assertTrue(hasattr(module.GameLogReviewActionView, "approve"))
        self.assertTrue(hasattr(module.GameLogReviewActionView, "edit_type"))
        self.assertTrue(hasattr(module.GameLogReviewActionView, "edit_reason"))
        self.assertTrue(hasattr(module.GameLogReviewActionView, "edit_notes"))
        self.assertTrue(hasattr(module.GameLogReviewActionView, "dismiss"))

    def test_game_log_review_edit_type_opens_fixed_select(self):
        module = load_sanction_manager_module()
        sanction = {
            "sanction_id": 88,
            "guild_id": 1,
            "sanction_type": "Kick",
        }

        class FakeDB:
            def get_sanction(self, sanction_id):
                return sanction if sanction_id == 88 else None

        class FakeCog:
            db = FakeDB()

            async def _is_admin(self, interaction):
                return True

        embed = types.SimpleNamespace(
            footer=types.SimpleNamespace(text="Log ID: 18554 | Sanction ID: 88")
        )
        interaction = types.SimpleNamespace(
            message=types.SimpleNamespace(embeds=[embed]),
            user=types.SimpleNamespace(id=999),
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )
        view = module.GameLogReviewActionView(FakeCog())

        asyncio.run(view.edit_type(interaction, None))

        interaction.response.send_message.assert_awaited_once()
        kwargs = interaction.response.send_message.await_args.kwargs
        self.assertIsInstance(kwargs["view"], module.EditSanctionTypeView)
        self.assertTrue(kwargs["ephemeral"])

    def test_game_log_review_edit_reason_opens_search_modal(self):
        module = load_sanction_manager_module()
        sanction = {
            "sanction_id": 88,
            "guild_id": 1,
            "sanction_type": "Kick",
            "reason_detail": "Kicked from the alliance",
        }

        class FakeDB:
            def get_sanction(self, sanction_id):
                return sanction if sanction_id == 88 else None

        class FakeCog:
            db = FakeDB()

            async def _is_admin(self, interaction):
                return True

        embed = types.SimpleNamespace(
            footer=types.SimpleNamespace(text="Log ID: 18554 | Sanction ID: 88")
        )
        interaction = types.SimpleNamespace(
            message=types.SimpleNamespace(embeds=[embed]),
            user=types.SimpleNamespace(id=999),
            response=types.SimpleNamespace(send_modal=AsyncMock()),
        )
        view = module.GameLogReviewActionView(FakeCog())

        asyncio.run(view.edit_reason(interaction, None))

        interaction.response.send_modal.assert_awaited_once()
        modal = interaction.response.send_modal.await_args.args[0]
        self.assertIsInstance(modal, module.EditSanctionReasonSearchModal)
        self.assertEqual(modal.query.default, "Kicked from the alliance")

    def test_sanction_reason_search_modal_updates_exact_match(self):
        module = load_sanction_manager_module()
        apply_edit = AsyncMock()
        sanction = {
            "sanction_id": 88,
            "guild_id": 1,
            "sanction_type": "Kick",
        }
        editor = types.SimpleNamespace(id=999)
        cog = types.SimpleNamespace(
            find_sanction_reason_matches=lambda guild_id, query, limit=10: [
                {
                    "score": 1.0,
                    "category": "Activity",
                    "detail": "4.1. 5% donation to alliance",
                    "label": "4.1. 5% donation to alliance",
                }
            ],
            apply_sanction_reason_edit=apply_edit,
        )
        modal = module.EditSanctionReasonSearchModal(cog, sanction, editor)
        modal.query.value = "donation"
        interaction = types.SimpleNamespace(
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        asyncio.run(modal.on_submit(interaction))

        apply_edit.assert_awaited_once_with(
            sanction=sanction,
            editor=editor,
            reason_category="Activity",
            reason_detail="4.1. 5% donation to alliance",
        )
        interaction.response.send_message.assert_awaited_once()

    def test_sanction_reason_search_uses_aliases(self):
        SanctionsManager = load_sanctions_manager_class()
        manager = SanctionsManager.__new__(SanctionsManager)
        manager.db = types.SimpleNamespace(get_custom_rules=lambda guild_id: [])

        results = manager.find_sanction_reason_matches(1, "tax", limit=3)

        self.assertGreaterEqual(len(results), 1)
        self.assertEqual(results[0]["detail"], "4.1. 5% donation to alliance - Minimum 5% donation required.")

    def test_mute_is_available_as_manual_sanction_type(self):
        module = load_sanction_manager_module()

        self.assertIn("Mute", module.SANCTION_TYPES)
        self.assertNotIn("Chat Ban", module.SANCTION_TYPES)

    def test_sanction_reason_search_results_view_uses_safe_view_reference(self):
        module = load_sanction_manager_module()
        sanction = {
            "sanction_id": 88,
            "guild_id": 1,
            "sanction_type": "Kick",
        }
        cog = types.SimpleNamespace()
        editor = types.SimpleNamespace(id=999)
        matches = [
            {
                "score": 0.9,
                "category": "Activity",
                "detail": "4.1. 5% donation to alliance",
                "label": "4.1. 5% donation to alliance",
            }
        ]

        view = module.EditSanctionReasonSearchResultsView(cog, sanction, editor, matches)

        self.assertEqual(len(view.children), 1)
        self.assertIs(view.children[0].search_view, view)

    def test_legacy_sanction_type_select_uses_safe_view_reference(self):
        module = load_sanction_manager_module()
        sanction = {
            "sanction_id": 88,
            "guild_id": 1,
            "sanction_type": "Kick",
        }
        cog = types.SimpleNamespace()
        editor = types.SimpleNamespace(id=999)

        view = module.EditSanctionTypeView(cog, sanction, editor)

        self.assertEqual(len(view.children), 1)
        self.assertIs(view.children[0].edit_view, view)

    def test_game_log_review_scan_bootstraps_without_historical_import(self):
        module = load_sanction_manager_module()
        SanctionsDatabase = module.SanctionsDatabase
        SanctionsManager = module.SanctionsManager
        rows = [
            {
                "id": 42,
                "action_key": "kicked_from_alliance",
                "ts": "June 13, 2026 19:45",
                "executed_name": "DutchFireFighter",
                "affected_name": "OldMember",
                "affected_mc_id": "1251176",
            }
        ]

        class FakeLogsScraper:
            async def get_logs_after(self, last_id, limit=100):
                return [row for row in rows if row["id"] > last_id][:limit]

            async def get_recent_logs(self, limit=100):
                return rows[-limit:]

        class FakeValue:
            def __init__(self, value):
                self.value = value

            async def __call__(self):
                return self.value

            async def set(self, value):
                self.value = value

        class FakeGuildConfig:
            def __init__(self):
                self.game_log_review_last_id = FakeValue(0)
                self.game_log_review_channel_id = FakeValue(1421625293130567690)

        class FakeConfig:
            def __init__(self):
                self.guild_config = FakeGuildConfig()

            def guild(self, guild):
                return self.guild_config

        guild = types.SimpleNamespace(id=1, get_channel=lambda channel_id: None)

        with tempfile.TemporaryDirectory() as temp_dir:
            database = SanctionsDatabase(str(Path(temp_dir) / "sanctions.db"))
            manager = SanctionsManager.__new__(SanctionsManager)
            manager.db = database
            manager.config = FakeConfig()
            manager.bot = types.SimpleNamespace(
                get_cog=lambda name: FakeLogsScraper() if name == "LogsScraper" else None,
                get_channel=lambda channel_id: None,
            )

            result = asyncio.run(manager.scan_game_log_reviews(guild))
            stats = manager.get_sanction_stats(1)
            last_id = asyncio.run(manager.config.guild_config.game_log_review_last_id())

        self.assertTrue(result["bootstrapped"])
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["last_id"], 42)
        self.assertEqual(last_id, 42)
        self.assertEqual(stats["historical_count"], 0)

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

    def test_sanction_target_search_finds_alliance_only_member_by_name(self):
        SanctionsManager = load_sanctions_manager_class()

        class FakeMembersScraper:
            async def get_members(self):
                return [
                    {"id": "456", "mc_username": "CrashTestDummy"},
                    {"id": "789", "mc_username": "Other Member"},
                ]

        manager = SanctionsManager.__new__(SanctionsManager)
        manager.bot = types.SimpleNamespace(
            get_cog=lambda name: FakeMembersScraper() if name == "MembersScraper" else None
        )
        guild = types.SimpleNamespace(members=[], get_member=lambda member_id: None)

        results = asyncio.run(manager.search_sanction_targets(guild, "CrashTestDummy"))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["score"], 1.0)
        self.assertEqual(results[0]["mc_user_id"], "456")
        self.assertEqual(results[0]["mc_username"], "CrashTestDummy")
        self.assertIsNone(results[0]["discord_id"])

    def test_sanction_target_search_merges_memberscraper_and_alliancescraper(self):
        SanctionsManager = load_sanctions_manager_class()

        class FakeMembersScraper:
            async def get_members(self):
                return [{"mc_user_id": "123", "name": "MembersOnly"}]

        class FakeAllianceScraper:
            async def get_members(self):
                return [{"user_id": "456", "name": "AllianceOnly"}]

        def get_cog(name):
            if name == "MembersScraper":
                return FakeMembersScraper()
            if name == "AllianceScraper":
                return FakeAllianceScraper()
            return None

        manager = SanctionsManager.__new__(SanctionsManager)
        manager.bot = types.SimpleNamespace(get_cog=get_cog)
        guild = types.SimpleNamespace(members=[], get_member=lambda member_id: None)

        results = asyncio.run(manager.search_sanction_targets(guild, "AllianceOnly"))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["mc_user_id"], "456")
        self.assertEqual(results[0]["mc_username"], "AllianceOnly")

    def test_sanction_manager_resolves_discord_user_to_mc_display_name(self):
        SanctionsManager = load_sanctions_manager_class()

        class FakeMemberSync:
            async def get_link_for_discord(self, discord_id):
                return {"discord_id": discord_id, "mc_user_id": "88649"}

        class FakeMembersScraper:
            async def get_member_snapshot(self, mc_user_id):
                return {"user_id": mc_user_id, "name": "DutchFireFighter"}

        def get_cog(name):
            if name == "MemberSync":
                return FakeMemberSync()
            if name == "MembersScraper":
                return FakeMembersScraper()
            return None

        manager = SanctionsManager.__new__(SanctionsManager)
        manager.bot = types.SimpleNamespace(get_cog=get_cog)

        display = asyncio.run(
            manager.get_mc_display_name_for_discord(999, fallback="brandjuh")
        )

        self.assertEqual(display, "DutchFireFighter")

    def test_sanction_manager_builds_member_history_embed(self):
        SanctionsManager = load_sanctions_manager_class()

        class FakeDB:
            def get_user_sanctions(self, guild_id, discord_user_id=None, mc_user_id=None):
                self.call = (guild_id, discord_user_id, mc_user_id)
                return [
                    {
                        "sanction_id": 12,
                        "status": "active",
                        "sanction_type": "Warning - Verbal warning",
                        "reason_detail": "Low contribution",
                        "admin_username": "DutchFireFighter",
                        "created_at": 1_800_000_000,
                    }
                ]

        manager = SanctionsManager.__new__(SanctionsManager)
        manager.db = FakeDB()
        guild = types.SimpleNamespace(id=1)

        embed = manager.build_member_sanction_history_embed(
            guild=guild,
            mc_user_id="456",
            mc_username="CrashTestDummy",
        )

        self.assertEqual(embed.kwargs["title"], "Sanction History - CrashTestDummy")
        self.assertEqual(manager.db.call, (1, None, "456"))
        self.assertEqual(len(embed.fields), 1)
        self.assertIn("Warning - Verbal warning", embed.fields[0]["value"])
        self.assertIn("DutchFireFighter", embed.fields[0]["value"])

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

            async def send_sanction_reason_selection(self, interaction, target, *, use_followup=False):
                self.reason_call = (interaction, target, use_followup)
                await interaction.followup.send(
                    content="Sanction wizard: choose the matching reason first.",
                    view=types.SimpleNamespace(
                        target_mc_id=target.get("mc_user_id"),
                        target_mc_username=target.get("mc_username"),
                        target_discord_id=target.get("discord_id"),
                    ),
                    ephemeral=True,
                )

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
        self.assertEqual(kwargs["content"], "Sanction wizard: choose the matching reason first.")
        self.assertEqual(kwargs["view"].target_mc_id, "456")
        self.assertEqual(kwargs["view"].target_mc_username, "MCOnlyUser")
        self.assertIsNone(kwargs["view"].target_discord_id)
        self.assertIs(cog.reason_call[0], interaction)
        self.assertEqual(cog.reason_call[1]["mc_user_id"], "456")
        self.assertTrue(cog.reason_call[2])

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
