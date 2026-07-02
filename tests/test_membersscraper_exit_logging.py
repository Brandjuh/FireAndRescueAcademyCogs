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

    def test_exit_notification_embed_does_not_include_rank_field(self):
        class FakeConfig:
            async def exit_log_channel_id(self):
                return 123

        class FakeChannel:
            name = "exit-log"

            def __init__(self):
                self.embeds = []

            async def send(self, *, embed):
                self.embeds.append(embed)

        channel = FakeChannel()
        self.scraper.config = FakeConfig()
        self.scraper.bot = type(
            "FakeBot",
            (),
            {"get_channel": staticmethod(lambda channel_id: channel if channel_id == 123 else None)},
        )()

        exit_data = {
            "member_id": 42,
            "username": "Departed Member",
            "rank": "Edit Admin rights Kick Set as Admin",
            "earned_credits": 12345,
            "contribution_rate": 5.0,
        }

        asyncio.run(self.scraper._send_exit_notifications([exit_data]))

        self.assertEqual(len(channel.embeds), 1)
        field_names = [field["name"] for field in channel.embeds[0].fields]
        self.assertEqual(field_names, ["Name", "MC ID", "Last Credits", "Contribution Rate"])
        self.assertNotIn("Rank", field_names)

    def test_exit_detection_skips_extremely_incomplete_scrape(self):
        previous_timestamp = "2026-07-03T00:00:00"
        connection = sqlite3.connect(self.scraper.db_path)
        try:
            for member_id in range(1, 121):
                connection.execute(
                    """
                    INSERT INTO members
                    (member_id, username, rank, earned_credits, contribution_rate, timestamp, snapshot_source)
                    VALUES (?, ?, 'Member', 1000, 5.0, ?, 'live')
                    """,
                    (member_id, f"Member {member_id}", previous_timestamp),
                )
            connection.commit()
        finally:
            connection.close()

        current_members = [
            {
                "member_id": member_id,
                "username": f"Member {member_id}",
                "rank": "Member",
                "earned_credits": 1000,
                "contribution_rate": 5.0,
                "timestamp": "2026-07-03T01:00:00",
            }
            for member_id in range(1, 41)
        ]

        exits = asyncio.run(self.scraper._detect_exits(current_members))

        self.assertEqual(exits, [])
        self.scraper._debug_log.assert_any_await(
            "Exit detection skipped: current scrape is too small (40/120, 33%). This usually means the member scrape was incomplete.",
            None,
        )


if __name__ == "__main__":
    unittest.main()
