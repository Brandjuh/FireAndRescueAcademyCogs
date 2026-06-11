import asyncio
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from bs4 import BeautifulSoup


class _Decorator:
    def __call__(self, function):
        return function


class _CommandDecorator:
    def __call__(self, *args, **kwargs):
        del args, kwargs
        return _Decorator()


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
        self.requested_urls = []

    def get(self, url):
        self.requested_urls.append(url)
        return _FakeResponse(self.html)


def _install_redbot_test_stubs():
    """Provide only the import surface needed to load the cog for characterization tests."""
    discord = types.ModuleType("discord")
    redbot = types.ModuleType("redbot")
    redbot_core = types.ModuleType("redbot.core")
    commands = types.ModuleType("redbot.core.commands")

    commands.Cog = object
    commands.command = _CommandDecorator()
    commands.is_owner = _CommandDecorator()

    redbot_core.commands = commands
    redbot_core.Config = object
    redbot_core.data_manager = object
    redbot.core = redbot_core

    sys.modules.setdefault("discord", discord)
    sys.modules.setdefault("redbot", redbot)
    sys.modules.setdefault("redbot.core", redbot_core)
    sys.modules.setdefault("redbot.core.commands", commands)


_install_redbot_test_stubs()

from applicationscraper.applications_scraper import ApplicationsScraper  # noqa: E402


class ParseApplicationCharacterizationTests(unittest.TestCase):
    def setUp(self):
        self.scraper = ApplicationsScraper.__new__(ApplicationsScraper)
        self.scrape_timestamp = "2026-06-11T12:00:00"

    def parse(self, html):
        element = BeautifulSoup(html, "html.parser").find()
        return self.scraper._parse_application(element, self.scrape_timestamp)

    def test_parses_current_expected_application_fields(self):
        application = self.parse(
            """
            <div class="application">
              <a href="/profile/123">Test Applicant</a>
              <time datetime="2026-06-10T10:00:00">Yesterday</time>
              <p>Please accept me</p>
              <span class="badge">Approved</span>
              <span>12,345 Credits</span>
              <span>7 buildings</span>
            </div>
            """
        )

        self.assertEqual(
            application,
            {
                "applicant_name": "Test Applicant",
                "applicant_id": 123,
                "application_date": "2026-06-10T10:00:00",
                "status": "accepted",
                "message": "Please accept me",
                "credits": 12345,
                "buildings": 7,
                "scrape_timestamp": self.scrape_timestamp,
            },
        )

    def test_preserves_current_defaults_for_missing_fields(self):
        before = datetime.now(timezone.utc).replace(tzinfo=None)

        application = self.parse("<div class='application'></div>")

        after = datetime.now(timezone.utc).replace(tzinfo=None)
        parsed_date = datetime.fromisoformat(application["application_date"])
        self.assertEqual(application["applicant_name"], "")
        self.assertEqual(application["applicant_id"], 0)
        self.assertEqual(application["status"], "pending")
        self.assertEqual(application["message"], "")
        self.assertEqual(application["credits"], 0)
        self.assertEqual(application["buildings"], 0)
        self.assertLessEqual(before, parsed_date)
        self.assertLessEqual(parsed_date, after)

    def test_invalid_profile_id_is_currently_returned_as_zero(self):
        application = self.parse(
            """
            <div class="application">
              <a href="/profile/not-a-number">Test Applicant</a>
            </div>
            """
        )

        self.assertEqual(application["applicant_name"], "Test Applicant")
        self.assertEqual(application["applicant_id"], 0)

    def test_message_is_currently_limited_to_1000_characters(self):
        application = self.parse(f"<div class='application'><p>{'x' * 1200}</p></div>")

        self.assertEqual(len(application["message"]), 1000)

    def test_rejected_badge_maps_to_rejected_status(self):
        application = self.parse(
            """
            <div class="application">
              <span class="label">Declined</span>
            </div>
            """
        )

        self.assertEqual(application["status"], "rejected")


class LoginCheckCharacterizationTests(unittest.TestCase):
    def setUp(self):
        self.scraper = ApplicationsScraper.__new__(ApplicationsScraper)

    def test_logout_link_is_currently_treated_as_logged_in(self):
        result = asyncio.run(
            self.scraper._check_logged_in("<a href='/users/sign_out'>Sign out</a>")
        )

        self.assertTrue(result)

    def test_page_without_known_login_markers_is_currently_treated_as_logged_out(self):
        result = asyncio.run(self.scraper._check_logged_in("<html><body>Applications</body></html>"))

        self.assertFalse(result)


class ScrapeApplicationsCharacterizationTests(unittest.TestCase):
    def setUp(self):
        self.scraper = ApplicationsScraper.__new__(ApplicationsScraper)
        self.scraper.applications_url = "https://www.missionchief.com/verband/bewerbungen"

    def test_current_table_discovery_and_parsing_work_together(self):
        session = _FakeSession(
            """
            <html>
              <a href="/users/sign_out">Sign out</a>
              <table class="table">
                <tbody>
                  <tr>
                    <td><a href="/profile/123">Test Applicant</a></td>
                    <td><time datetime="2026-06-10T10:00:00">Yesterday</time></td>
                    <td>12,345 Credits and 7 buildings</td>
                  </tr>
                </tbody>
              </table>
            </html>
            """
        )

        with patch(
            "applicationscraper.applications_scraper.asyncio.sleep",
            new=AsyncMock(),
        ):
            applications = asyncio.run(self.scraper._scrape_applications(session))

        self.assertEqual(session.requested_urls, [self.scraper.applications_url])
        self.assertEqual(len(applications), 1)
        self.assertEqual(applications[0]["applicant_id"], 123)
        self.assertEqual(applications[0]["credits"], 12345)
        self.assertEqual(applications[0]["buildings"], 7)


if __name__ == "__main__":
    unittest.main()
