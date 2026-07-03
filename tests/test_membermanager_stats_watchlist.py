import asyncio
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from MemberManager.database import MemberDatabase
from MemberManager.membermanager import MemberManager


class MemberManagerStatsWatchlistTests(unittest.TestCase):
    def test_database_stats_and_identity_queries_are_guild_scoped(self):
        async def run_test():
            with tempfile.TemporaryDirectory() as temp_dir:
                database = MemberDatabase(str(Path(temp_dir) / "membermanager.db"))
                await database.initialize()
                try:
                    await database.add_note(
                        guild_id=1,
                        discord_id=123,
                        mc_user_id="456",
                        note_text="Guild 1 note",
                        author_id=999,
                        author_name="Admin",
                    )
                    await database.add_note(
                        guild_id=2,
                        discord_id=123,
                        mc_user_id="456",
                        note_text="Guild 2 note",
                        author_id=999,
                        author_name="Admin",
                    )
                    await database.add_event(
                        guild_id=1,
                        discord_id=123,
                        mc_user_id="456",
                        event_type="note_created",
                        event_data={"ref_code": "N2026-000001"},
                        triggered_by="MemberManager",
                    )
                    await database.add_event(
                        guild_id=1,
                        discord_id=123,
                        mc_user_id="456",
                        event_type="sanction_added",
                        event_data={"sanction_id": 10},
                        triggered_by="SanctionManager",
                    )
                    await database.add_event(
                        guild_id=1,
                        discord_id=123,
                        mc_user_id="456",
                        event_type="event_location_requested",
                        event_data={"location": "New York"},
                        triggered_by="EventManager",
                    )
                    await database.add_event(
                        guild_id=2,
                        discord_id=123,
                        mc_user_id="456",
                        event_type="note_created",
                        event_data={"ref_code": "N2026-000002"},
                        triggered_by="MemberManager",
                    )
                    first_watchlist_id = await database.add_to_watchlist(
                        guild_id=1,
                        discord_id=123,
                        mc_user_id="456",
                        reason="Needs review",
                        added_by=999,
                        watch_type="general",
                    )
                    await database.resolve_watchlist(
                        watchlist_id=first_watchlist_id,
                        resolved_by=999,
                        notes="Done",
                    )
                    await database.add_to_watchlist(
                        guild_id=1,
                        discord_id=123,
                        mc_user_id="456",
                        reason="Active review",
                        added_by=999,
                        watch_type="general",
                    )

                    notes = await database.get_notes(
                        guild_id=1,
                        discord_id=123,
                        mc_user_id="456",
                    )
                    events = await database.get_events(
                        guild_id=1,
                        discord_id=123,
                        mc_user_id="456",
                        limit=10,
                    )
                    watchlist = await database.get_member_watchlist(
                        guild_id=1,
                        discord_id=123,
                        mc_user_id="456",
                    )
                    stats = await database.get_stats(guild_id=1)
                finally:
                    await database.close()

            return notes, events, watchlist, stats

        notes, events, watchlist, stats = asyncio.run(run_test())

        self.assertEqual([note["note_text"] for note in notes], ["Guild 1 note"])
        self.assertEqual(len(events), 3)
        self.assertEqual(len(watchlist), 1)
        self.assertEqual(watchlist[0]["reason"], "Active review")
        self.assertEqual(stats["notes"]["created"], 1)
        self.assertEqual(stats["events"]["total"], 3)
        self.assertEqual(stats["events"]["sanctions"], 1)
        self.assertEqual(stats["events"]["event_requests"], 1)
        self.assertEqual(stats["watchlist"]["active"], 1)
        self.assertEqual(stats["watchlist"]["resolved"], 1)

    def test_build_member_data_populates_active_watchlist_status(self):
        class FakeDB:
            async def get_notes(self, **kwargs):
                del kwargs
                return []

            async def get_member_watchlist(self, **kwargs):
                del kwargs
                return [{"reason": "Needs staff review"}]

        async def run_test():
            cog = MemberManager.__new__(MemberManager)
            cog.membersync = None
            cog.alliance_scraper = None
            cog.members_scraper = None
            cog.logs_scraper = None
            cog.sanction_manager = None
            cog.db = FakeDB()
            cog._populate_contribution_data = AsyncMock()
            guild = types.SimpleNamespace(id=1, get_member=lambda user_id: None)
            return await cog._build_member_data(guild=guild, mc_user_id="456")

        data = asyncio.run(run_test())

        self.assertTrue(data.on_watchlist)
        self.assertEqual(data.watchlist_reason, "Needs staff review")


if __name__ == "__main__":
    unittest.main()
