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
        finally:
            connection.close()
        self.assertEqual(stored_count, 1)


if __name__ == "__main__":
    unittest.main()
