import asyncio
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from MemberManager.membermanager import MemberManager
from MemberManager.models import MemberData
from MemberManager.views import MemberOverviewView


class MemberManagerContributionTests(unittest.TestCase):
    def test_build_member_data_populates_contribution_status(self):
        member = types.SimpleNamespace(
            id=123,
            roles=[],
            joined_at=None,
            __str__=lambda self: "DiscordUser",
        )
        guild = types.SimpleNamespace(
            id=1,
            get_member=lambda user_id: member if user_id == 123 else None,
            get_role=lambda role_id: object(),
        )
        membersync = types.SimpleNamespace(
            config=types.SimpleNamespace(verified_role_id=AsyncMock(return_value=None)),
            get_verification_status=AsyncMock(
                return_value={
                    "discord_id": "123",
                    "mc_user_id": "456",
                    "status": "approved",
                }
            ),
        )
        cog = MemberManager.__new__(MemberManager)
        cog.membersync = membersync
        cog.members_scraper = types.SimpleNamespace(db_path="members.db")
        cog.logs_scraper = None
        cog.db = None
        cog.sanction_manager = None
        cog._get_mc_data = AsyncMock(
            return_value={
                "user_id": "456",
                "name": "MCUser",
                "role": "Member",
                "contribution_rate": 6.5,
                "snapshot_at": "2026-06-12T12:00:00",
                "snapshot_source": "live",
            }
        )
        cog._get_historical_rates_for_member = AsyncMock(return_value=[6.5, 5.0, 4.5])
        cog._get_join_date_for_member = AsyncMock(
            return_value=(datetime(2026, 6, 1, tzinfo=timezone.utc), "members_first_seen")
        )

        data = asyncio.run(cog._build_member_data(guild, discord_id=123))

        self.assertEqual(data.contribution_data_status, "available")
        self.assertEqual(data.contribution_rate, 6.5)
        self.assertEqual(data.contribution_snapshot_at, "2026-06-12T12:00:00")
        self.assertEqual(data.contribution_snapshot_source, "live")
        self.assertEqual(data.contribution_history, [6.5, 5.0, 4.5])
        self.assertEqual(data.contribution_trend, "up")
        self.assertEqual(data.contribution_join_source, "members_first_seen")

    def test_contribution_status_unavailable_without_members_scraper(self):
        cog = MemberManager.__new__(MemberManager)
        cog.members_scraper = None
        cog.logs_scraper = None
        data = MemberData(mc_user_id="456")

        asyncio.run(cog._populate_contribution_data(data))

        self.assertEqual(data.contribution_data_status, "unavailable")

    def test_overview_embed_includes_contribution_field(self):
        data = MemberData(
            discord_id=123,
            mc_user_id="456",
            discord_username="DiscordUser",
            mc_username="MCUser",
            mc_role="Member",
            contribution_rate=6.5,
            contribution_trend="up",
            contribution_snapshot_at="2026-06-12T12:00:00",
            contribution_snapshot_source="live",
            contribution_history=[6.5, 5.0, 4.5],
            contribution_data_status="available",
            contribution_join_source="members_first_seen",
            contribution_grace_status="11 days in alliance",
            link_status="approved",
        )
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = data
        view.overview_mode = "advanced"

        embed = asyncio.run(view.get_overview_embed())

        field_names = [field["name"] for field in embed.fields]
        self.assertIn("Contribution", field_names)


if __name__ == "__main__":
    unittest.main()
