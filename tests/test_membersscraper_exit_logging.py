import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from membersscraper.members_scraper import MembersScraper


class MembersScraperExitLoggingTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        directory = Path(self.temporary_directory.name)
        self.scraper = MembersScraper.__new__(MembersScraper)
        self.scraper.db_path = str(directory / "members.db")
        self.scraper.membersync_db = str(directory / "membersync.db")
        self.scraper._debug_log = AsyncMock()
        self.scraper._init_database()

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_records_discord_link_and_suppresses_recent_duplicate(self):
        connection = sqlite3.connect(self.scraper.membersync_db)
        try:
            connection.execute(
                """
                CREATE TABLE links (
                    discord_id INTEGER,
                    mc_user_id TEXT,
                    status TEXT
                )
                """
            )
            connection.execute(
                "INSERT INTO links (discord_id, mc_user_id, status) VALUES (?, ?, ?)",
                (987654321, "42", "approved"),
            )
            connection.commit()
        finally:
            connection.close()

        exit_data = {
            "member_id": 42,
            "username": "Departed Member",
            "rank": "Member",
            "earned_credits": 12345,
            "contribution_rate": 5.0,
            "last_seen_at": "2026-06-11T12:00:00",
        }

        first_logged = asyncio.run(self.scraper._log_exits_to_database([exit_data]))
        second_logged = asyncio.run(self.scraper._log_exits_to_database([exit_data]))

        connection = sqlite3.connect(self.scraper.membersync_db)
        try:
            rows = connection.execute(
                """
                SELECT mc_user_id, username, discord_id, reason, role_removed, notified
                FROM member_left_alliance
                """
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual(first_logged, 1)
        self.assertEqual(second_logged, 0)
        self.assertEqual(
            rows,
            [
                (
                    "42",
                    "Departed Member",
                    987654321,
                    "auto-detected by scraper",
                    0,
                    0,
                )
            ],
        )


if __name__ == "__main__":
    unittest.main()
