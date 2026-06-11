import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from membersscraper.members_scraper import MembersScraper


class _FakeResponse:
    status = 200

    def __init__(self, html):
        self.html = html

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        del exc_type, exc_value, traceback

    async def text(self):
        return self.html


class _FakeSession:
    def __init__(self, html):
        self.html = html

    def get(self, url):
        del url
        return _FakeResponse(self.html)


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

        self.assertEqual(self.scrape("<tr><td>Not available</td></tr>"), [])


if __name__ == "__main__":
    unittest.main()
