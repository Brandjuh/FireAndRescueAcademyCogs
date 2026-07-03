import asyncio
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from membersscraper.members_scraper import MembersScraper


class _FakeResponse:
    def __init__(self, html, *, status=200, headers=None):
        self.html = html
        self.status = status
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        del exc_type, exc_value, traceback

    async def text(self):
        return self.html


class _FakeSession:
    def __init__(self, html, *, status=200, headers=None):
        self.html = html
        self.status = status
        self.headers = headers or {}

    def get(self, url):
        del url
        return _FakeResponse(self.html, status=self.status, headers=self.headers)


class MembersScraperCharacterizationTests(unittest.TestCase):
    def setUp(self):
        self.scraper = MembersScraper.__new__(MembersScraper)
        self.scraper.members_url = "https://www.missionchief.com/verband/mitglieder/1621"
        self.scraper._debug_log = AsyncMock()
        self.scraper._check_logged_in = AsyncMock(return_value=True)

    def scrape(self, html):
        with patch("membersscraper.members_scraper.asyncio.sleep", new=AsyncMock()):
            return asyncio.run(
                self.scraper._scrape_members_page(
                    _FakeSession(html),
                    page_num=1,
                    timestamp="2026-06-11T19:30:00",
                )
            )

    def test_parses_current_member_fields(self):
        members = self.scrape(
            """
            <table>
              <tr>
                <td><a href="/users/12345">Fixture Member</a></td>
                <td>Alliance Admin</td>
                <td>12,345 Credits</td>
                <td>7.5%</td>
                <td><span class="label-success">Online</span></td>
              </tr>
            </table>
            """
        )

        self.assertEqual(
            members,
            [
                {
                    "member_id": 12345,
                    "username": "Fixture Member",
                    "rank": "Alliance Admin",
                    "earned_credits": 12345,
                    "contribution_rate": 7.5,
                    "online_status": "online",
                    "timestamp": "2026-06-11T19:30:00",
                    "suspicious": False,
                }
            ],
        )

    def test_accepts_profile_links_and_marks_offline_members(self):
        members = self.scrape(
            """
            <tr>
              <td><a href="/profile/67890">Offline Member</a></td>
              <td>Member</td>
              <td>900 Credits</td>
            </tr>
            """
        )

        self.assertEqual(members[0]["member_id"], 67890)
        self.assertEqual(members[0]["online_status"], "offline")
        self.assertEqual(members[0]["contribution_rate"], 0.0)

    def test_marks_missing_credits_as_suspicious(self):
        members = self.scrape(
            """
            <tr>
              <td><a href="/users/111">Suspicious Member</a></td>
              <td>Member</td>
            </tr>
            """
        )

        self.assertTrue(members[0]["suspicious"])
        self.assertEqual(members[0]["reason"], "No credits found in expected format")
        self.assertIn("raw_html", members[0])

    def test_returns_no_members_when_login_check_fails(self):
        self.scraper._check_logged_in = AsyncMock(return_value=False)

        self.assertIsNone(self.scrape("<tr><td>Not available</td></tr>"))

    def test_returns_none_after_rate_limit_retries_are_exhausted(self):
        with patch("membersscraper.members_scraper.asyncio.sleep", new=AsyncMock()) as sleep_mock:
            members = asyncio.run(
                self.scraper._scrape_members_page(
                    _FakeSession(
                        "Too Many Requests",
                        status=429,
                        headers={"Retry-After": "0"},
                    ),
                    page_num=1,
                    timestamp="2026-06-11T19:30:00",
                )
            )

        self.assertIsNone(members)
        self.assertGreaterEqual(sleep_mock.await_count, 3)


class MembersScraperDatabaseCharacterizationTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        directory = Path(self.temporary_directory.name)
        self.scraper = MembersScraper.__new__(MembersScraper)
        self.scraper.db_path = str(directory / "members.db")
        self.scraper.membersync_db = str(directory / "membersync.db")
        self.scraper._debug_log = AsyncMock()

    def tearDown(self):
        self.temporary_directory.cleanup()

    def insert_member(self, member_id, username, timestamp):
        connection = sqlite3.connect(self.scraper.db_path)
        try:
            connection.execute(
                """
                INSERT INTO members
                (member_id, username, rank, earned_credits, contribution_rate,
                 online_status, timestamp, snapshot_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'live')
                """,
                (member_id, username, "Member", 1000, 5.0, "offline", timestamp),
            )
            connection.commit()
        finally:
            connection.close()

    def test_database_initialization_creates_expected_schema_and_latest_view(self):
        self.scraper._init_database()
        self.insert_member(1, "Older Member", "2026-06-10T12:00:00")
        self.insert_member(2, "Latest Member", "2026-06-11T12:00:00")

        connection = sqlite3.connect(self.scraper.db_path)
        try:
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(members)").fetchall()
            }
            current_members = connection.execute(
                "SELECT mc_user_id, name, scraped_at FROM members_current"
            ).fetchall()
        finally:
            connection.close()

        self.assertIn("contribution_rate", columns)
        self.assertIn("snapshot_source", columns)
        self.assertEqual(current_members, [(2, "Latest Member", "2026-06-11T12:00:00")])

    def test_exit_detection_uses_newest_stored_snapshot(self):
        self.scraper._init_database()
        self.insert_member(1, "Stale Member", "2026-06-10T12:00:00")
        self.insert_member(2, "Latest Member", "2026-06-11T12:00:00")

        exits = asyncio.run(
            self.scraper._detect_exits(
                [
                    {
                        "member_id": 2,
                        "suspicious": False,
                    }
                ]
            )
        )

        self.assertEqual(exits, [])

    def test_exit_detection_finds_member_missing_from_newest_stored_snapshot(self):
        self.scraper._init_database()
        self.insert_member(1, "Stale Member", "2026-06-10T12:00:00")
        self.insert_member(2, "Latest Member", "2026-06-11T12:00:00")

        exits = asyncio.run(self.scraper._detect_exits([]))

        self.assertEqual([member["member_id"] for member in exits], [2])

    def test_public_member_contract_returns_snapshot_history_and_first_seen(self):
        self.scraper._init_database()
        self.insert_member(1, "Older Member", "2026-06-10T12:00:00")
        self.insert_member(1, "Latest Member", "2026-06-11T12:00:00")

        snapshot = asyncio.run(self.scraper.get_member_snapshot("1"))
        history = asyncio.run(self.scraper.get_member_contribution_history("1"))
        first_seen = asyncio.run(self.scraper.get_member_first_seen("1"))

        self.assertEqual(snapshot["user_id"], 1)
        self.assertEqual(snapshot["name"], "Latest Member")
        self.assertEqual(snapshot["snapshot_at"], "2026-06-11T12:00:00")
        self.assertEqual(snapshot["snapshot_source"], "live")
        self.assertEqual(history, [5.0, 5.0])
        self.assertEqual(first_seen, "2026-06-10T12:00:00")

    def test_public_members_contract_returns_current_alliance_members(self):
        self.scraper._init_database()
        self.insert_member(1, "Older Member", "2026-06-10T12:00:00")
        self.insert_member(2, "Latest Member", "2026-06-11T12:00:00")

        members = asyncio.run(self.scraper.get_members())

        self.assertEqual(len(members), 1)
        self.assertEqual(members[0]["mc_user_id"], 2)
        self.assertEqual(members[0]["name"], "Latest Member")
        self.assertEqual(members[0]["scraped_at"], "2026-06-11T12:00:00")


if __name__ == "__main__":
    unittest.main()
