import asyncio
import importlib.util
import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from MemberManager.membermanager import MemberManager
from MemberManager.views import MemberOverviewView


def load_membersync_class():
    from discord import app_commands
    from redbot.core import checks, commands

    if not hasattr(app_commands, "describe"):
        app_commands.describe = lambda **kwargs: (lambda func: func)
    if not hasattr(checks, "admin_or_permissions"):
        checks.admin_or_permissions = lambda **kwargs: (lambda func: func)
    if not hasattr(commands, "cooldown"):
        commands.cooldown = lambda *args, **kwargs: (lambda func: func)
    if not hasattr(commands, "BucketType"):
        commands.BucketType = types.SimpleNamespace(user="user")

    module_path = Path(__file__).resolve().parents[1] / "membersync" / "membersync.py"
    spec = importlib.util.spec_from_file_location("membersync_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.MemberSync


class MemberManagerMemberSyncTests(unittest.TestCase):
    def _member(self, roles=None):
        return types.SimpleNamespace(
            id=123,
            roles=roles or [],
            joined_at=None,
            guild_permissions=types.SimpleNamespace(administrator=False),
            __str__=lambda self: "DiscordUser",
        )

    def test_build_member_data_uses_pending_membersync_status(self):
        member = self._member()
        guild = types.SimpleNamespace(
            get_member=lambda user_id: member if user_id == 123 else None,
            get_role=lambda role_id: object(),
        )
        membersync = types.SimpleNamespace(
            config=types.SimpleNamespace(verified_role_id=AsyncMock(return_value=999)),
            get_verification_status=AsyncMock(
                return_value={
                    "discord_id": "123",
                    "mc_user_id": "456",
                    "status": "pending",
                    "updated_at": "2026-06-12T12:00:00+00:00",
                    "reviewer_id": None,
                }
            ),
        )
        cog = MemberManager.__new__(MemberManager)
        cog.membersync = membersync
        cog.members_scraper = None
        cog.db = None
        cog.sanction_manager = None

        data = asyncio.run(cog._build_member_data(guild, discord_id=123))

        self.assertEqual(data.link_status, "pending")
        self.assertEqual(data.mc_user_id, "456")
        self.assertFalse(data.is_verified)
        self.assertEqual(data.link_updated, "2026-06-12T12:00:00+00:00")

    def test_build_member_data_marks_missing_verified_role_conflict(self):
        verified_role = object()
        member = self._member(roles=[])
        guild = types.SimpleNamespace(
            get_member=lambda user_id: member if user_id == 123 else None,
            get_role=lambda role_id: verified_role,
        )
        membersync = types.SimpleNamespace(
            config=types.SimpleNamespace(verified_role_id=AsyncMock(return_value=999)),
            get_verification_status=AsyncMock(
                return_value={
                    "discord_id": "123",
                    "mc_user_id": "456",
                    "status": "approved",
                    "updated_at": "2026-06-12T12:00:00+00:00",
                    "reviewer_id": "789",
                }
            ),
        )
        cog = MemberManager.__new__(MemberManager)
        cog.membersync = membersync
        cog.members_scraper = None
        cog.db = None
        cog.sanction_manager = None

        data = asyncio.run(cog._build_member_data(guild, discord_id=123))

        self.assertEqual(data.link_status, "approved")
        self.assertTrue(data.is_verified)
        self.assertFalse(data.verified_role_present)
        self.assertEqual(data.member_sync_conflict, "Approved link but missing verified role")
        self.assertEqual(data.link_reviewer_id, 789)

    def test_overview_embed_includes_membersync_field(self):
        data = types.SimpleNamespace(
            get_display_name=lambda: "DiscordUser",
            has_discord=lambda: True,
            has_mc=lambda: True,
            discord_username="DiscordUser",
            discord_id=123,
            discord_joined=None,
            is_verified=True,
            link_status="approved",
            mc_user_id="456",
            verified_role_present=False,
            link_updated="2026-06-12T12:00:00+00:00",
            link_reviewer_id=789,
            member_sync_conflict="Approved link but missing verified role",
            mc_username="MCUser",
            mc_role="Member",
            contribution_rate=None,
            contribution_data_status="missing",
            contribution_trend=None,
            contribution_history=[],
            contribution_snapshot_at=None,
            contribution_snapshot_source=None,
            contribution_grace_status=None,
            contribution_join_source=None,
            infractions_count=0,
            notes_count=0,
            severity_score=0,
            on_watchlist=False,
            is_linked=lambda: True,
        )
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = data
        view.overview_mode = "advanced"

        embed = asyncio.run(view.get_overview_embed())

        field_names = [field["name"] for field in embed.fields]
        self.assertIn("MemberSync", field_names)

    def test_membersync_records_membermanager_audit_when_available(self):
        MemberSync = load_membersync_class()
        add_event = AsyncMock()
        membersync = MemberSync.__new__(MemberSync)
        membersync.bot = types.SimpleNamespace(
            get_cog=lambda name: types.SimpleNamespace(db=types.SimpleNamespace(add_event=add_event))
            if name == "MemberManager"
            else None
        )

        asyncio.run(
            membersync._record_membermanager_link_event(
                guild_id=1,
                discord_id=123,
                mc_user_id="456",
                event_type="link_approved",
                actor_id=999,
                event_data={"status": "approved", "manual": True},
            )
        )

        add_event.assert_awaited_once_with(
            guild_id=1,
            discord_id=123,
            mc_user_id="456",
            event_type="link_approved",
            event_data={
                "mc_user_id": "456",
                "source": "MemberSync",
                "status": "approved",
                "manual": True,
            },
            triggered_by="membersync",
            actor_id=999,
        )

    def test_membersync_audit_hook_is_noop_without_membermanager(self):
        MemberSync = load_membersync_class()
        membersync = MemberSync.__new__(MemberSync)
        membersync.bot = types.SimpleNamespace(get_cog=lambda name: None)

        asyncio.run(
            membersync._record_membermanager_link_event(
                guild_id=1,
                discord_id=123,
                mc_user_id="456",
                event_type="link_denied",
                actor_id=999,
                event_data={"status": "denied", "reason": "Mismatch"},
            )
        )

    def test_membersync_prune_does_not_remove_role_without_exit_record(self):
        MemberSync = load_membersync_class()
        role = object()

        class FakeMember:
            id = 123
            name = "DiscordUser"

            def __init__(self):
                self.roles = [role]
                self.removed = []

            async def remove_roles(self, *args, **kwargs):
                self.removed.append((args, kwargs))

        class FakeConfig:
            async def guild_id(self):
                return 1

            async def verified_role_id(self):
                return 999

            async def log_channel_id(self):
                return None

        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "membersync.db"
            connection = sqlite3.connect(db_path)
            try:
                connection.execute(
                    "CREATE TABLE links (discord_id INTEGER, mc_user_id TEXT, status TEXT)"
                )
                connection.execute(
                    """
                    CREATE TABLE member_left_alliance (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mc_user_id TEXT NOT NULL,
                        username TEXT,
                        discord_id INTEGER,
                        exit_detected_at TEXT NOT NULL,
                        role_removed INTEGER DEFAULT 0
                    )
                    """
                )
                connection.execute(
                    "INSERT INTO links (discord_id, mc_user_id, status) VALUES (123, '456', 'approved')"
                )
                connection.commit()
            finally:
                connection.close()

            member = FakeMember()
            guild = types.SimpleNamespace(
                id=1,
                get_role=lambda role_id: role if role_id == 999 else None,
                get_member=lambda user_id: member if user_id == 123 else None,
                get_channel=lambda channel_id: None,
            )
            membersync = MemberSync.__new__(MemberSync)
            membersync.links_db = db_path
            membersync.config = FakeConfig()
            membersync.bot = types.SimpleNamespace(get_guild=lambda guild_id: guild, guilds=[guild])
            membersync._debug_log = AsyncMock()

            asyncio.run(membersync._prune_once_impl())

            self.assertEqual(member.removed, [])

    def test_membersync_prune_removes_role_for_confirmed_exit_record(self):
        MemberSync = load_membersync_class()
        role = object()

        class FakeMember:
            id = 123
            name = "DiscordUser"

            def __init__(self):
                self.roles = [role]
                self.removed = []

            async def remove_roles(self, *args, **kwargs):
                self.removed.append((args, kwargs))
                self.roles.remove(args[0])

        class FakeConfig:
            async def log_channel_id(self):
                return None

        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "membersync.db"
            connection = sqlite3.connect(db_path)
            try:
                connection.execute(
                    "CREATE TABLE links (discord_id INTEGER, mc_user_id TEXT, status TEXT)"
                )
                connection.execute(
                    """
                    CREATE TABLE member_left_alliance (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        mc_user_id TEXT NOT NULL,
                        username TEXT,
                        discord_id INTEGER,
                        exit_detected_at TEXT NOT NULL,
                        role_removed INTEGER DEFAULT 0
                    )
                    """
                )
                connection.execute(
                    "INSERT INTO links (discord_id, mc_user_id, status) VALUES (123, '456', 'approved')"
                )
                connection.execute(
                    """
                    INSERT INTO member_left_alliance
                    (mc_user_id, username, discord_id, exit_detected_at, role_removed)
                    VALUES ('456', 'MCUser', 123, '2026-06-12T12:00:00', 0)
                    """
                )
                connection.commit()
            finally:
                connection.close()

            member = FakeMember()
            guild = types.SimpleNamespace(
                get_member=lambda user_id: member if user_id == 123 else None,
                get_channel=lambda channel_id: None,
            )
            membersync = MemberSync.__new__(MemberSync)
            membersync.links_db = db_path
            membersync.config = FakeConfig()
            membersync._debug_log = AsyncMock()

            asyncio.run(
                membersync._prune_pending_exit_records(
                    guild,
                    role,
                    {"456": {"discord_id": 123, "mc_user_id": "456"}},
                )
            )

            self.assertEqual(len(member.removed), 1)
            self.assertEqual(member.removed[0][0][0], role)
            self.assertEqual(
                member.removed[0][1]["reason"],
                "MemberSync auto-prune: confirmed alliance exit",
            )
            connection = sqlite3.connect(db_path)
            try:
                role_removed = connection.execute(
                    "SELECT role_removed FROM member_left_alliance WHERE mc_user_id='456'"
                ).fetchone()[0]
            finally:
                connection.close()
            self.assertEqual(role_removed, 1)


if __name__ == "__main__":
    unittest.main()
