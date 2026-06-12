import asyncio
import types
import unittest
from unittest.mock import AsyncMock

from MemberManager.membermanager import MemberManager
from MemberManager.views import MemberOverviewView


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
            infractions_count=0,
            notes_count=0,
            severity_score=0,
            on_watchlist=False,
            is_linked=lambda: True,
        )
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = data

        embed = asyncio.run(view.get_overview_embed())

        field_names = [field["name"] for field in embed.fields]
        self.assertIn("MemberSync", field_names)


if __name__ == "__main__":
    unittest.main()
