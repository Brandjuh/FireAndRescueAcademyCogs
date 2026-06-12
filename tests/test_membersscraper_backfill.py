import asyncio
import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from membersscraper.members_scraper import MembersScraper


class MembersScraperBackfillTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        directory = Path(self.temporary_directory.name)
        self.scraper = MembersScraper.__new__(MembersScraper)
        self.scraper.db_path = str(directory / "members.db")
        self.scraper.membersync_db = str(directory / "membersync.db")
        self.scraper._debug_log = AsyncMock()
        self.scraper._get_session = AsyncMock(return_value=object())
        self.scraper._init_database()

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_passes_timestamp_and_context_to_page_scraper(self):
        member = {
            "member_id": 42,
            "username": "Fixture Member",
            "rank": "Member",
            "earned_credits": 12345,
            "contribution_rate": 5.0,
            "online_status": "offline",
        }
        self.scraper._scrape_members_page = AsyncMock(side_effect=[[member], []])
        ctx = types.SimpleNamespace(send=AsyncMock())

        asyncio.run(self.scraper.backfill_members(ctx, days=1))

        first_call = self.scraper._scrape_members_page.await_args_list[0]
        self.assertIsInstance(first_call.args[2], str)
        self.assertIs(first_call.args[3], ctx)

        connection = sqlite3.connect(self.scraper.db_path)
        try:
            stored_count = connection.execute("SELECT COUNT(*) FROM members").fetchone()[0]
            snapshot_sources = {
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT snapshot_source FROM members"
                ).fetchall()
            }
        finally:
            connection.close()
        self.assertEqual(stored_count, 1)
        self.assertEqual(snapshot_sources, {"backfill"})

    def test_live_scrape_marks_snapshot_source_live(self):
        member = {
            "member_id": 43,
            "username": "Live Member",
            "rank": "Member",
            "earned_credits": 67890,
            "contribution_rate": 6.0,
            "online_status": "online",
            "timestamp": "2026-06-12T14:00:00",
        }
        self.scraper._scrape_members_page = AsyncMock(side_effect=[[member], [], [], []])
        self.scraper._detect_exits = AsyncMock(return_value=[])

        self.assertTrue(asyncio.run(self.scraper._scrape_all_members()))

        connection = sqlite3.connect(self.scraper.db_path)
        try:
            snapshot_sources = {
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT snapshot_source FROM members"
                ).fetchall()
            }
        finally:
            connection.close()
        self.assertEqual(snapshot_sources, {"live"})


if __name__ == "__main__":
    unittest.main()
