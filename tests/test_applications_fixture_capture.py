import asyncio
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from bs4 import BeautifulSoup

from applicationscraper.applications_scraper import ApplicationsScraper
from applicationscraper.fixture_capture import inspect_applications_page, sanitize_applications_fixture
from applicationscraper.fixture_capture_cog import ApplicationsFixtureCapture


class _FakeResponse:
    status = 200

    def __init__(self, html, url="https://www.missionchief.com/verband/bewerbungen"):
        self.html = html
        self.url = url

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        del exc_type, exc_value, traceback

    async def text(self):
        return self.html


class _FakeSession:
    def __init__(self, html):
        self.html = html

    def get(self, url, **kwargs):
        del url, kwargs
        return _FakeResponse(self.html)


class FixtureSanitizerTests(unittest.TestCase):
    def test_inspection_reports_empty_and_populated_pages(self):
        empty = inspect_applications_page("<div class='alert'>No applications</div>")
        populated = inspect_applications_page(
            """
            <table class="table"><tbody><tr><td><a href="/profile/123">A</a></td></tr></tbody></table>
            <div class="card"><a href="/profile/456">B</a></div>
            <li class="application"><a href="/profile/789">C</a></li>
            """
        )

        self.assertEqual(
            empty,
            {
                "table_rows": 0,
                "cards_or_panels": 0,
                "application_list_items": 0,
                "profile_links": 0,
            },
        )
        self.assertEqual(populated["table_rows"], 1)
        self.assertEqual(populated["cards_or_panels"], 1)
        self.assertEqual(populated["application_list_items"], 1)
        self.assertEqual(populated["profile_links"], 3)

    def test_removes_secrets_and_personal_data_while_preserving_parser_markers(self):
        sanitized = sanitize_applications_fixture(
            """
            <html>
              <script>window.secret = "token-value";</script>
              <form action="/accept">
                <input type="hidden" name="authenticity_token" value="secret-token">
                <a href="/profile/987654">Real Person</a>
                <time datetime="2026-06-11T10:00:00">Today</time>
                <p>Private application message</p>
                <span class="badge">Approved</span>
                <span>12,345 Credits and 7 buildings</span>
              </form>
            </html>
            """
        )

        self.assertNotIn("token-value", sanitized)
        self.assertNotIn("secret-token", sanitized)
        self.assertNotIn("987654", sanitized)
        self.assertNotIn("Real Person", sanitized)
        self.assertNotIn("Private application message", sanitized)
        self.assertIn("/profile/100001", sanitized)
        self.assertIn("Fixture Applicant", sanitized)
        self.assertIn("Approved", sanitized)
        self.assertIn("12,345 Credits and 7 buildings", sanitized)

        element = BeautifulSoup(sanitized, "html.parser").find("form")
        application = ApplicationsScraper._parse_application(
            ApplicationsScraper.__new__(ApplicationsScraper),
            element,
            "2026-01-01T00:00:00",
        )
        self.assertEqual(application["applicant_name"], "Fixture Applicant")
        self.assertEqual(application["applicant_id"], 100001)
        self.assertEqual(application["status"], "accepted")
        self.assertEqual(application["credits"], 12345)
        self.assertEqual(application["buildings"], 7)

    def test_capture_command_writes_only_local_raw_and_sanitized_files(self):
        applications_scraper = types.SimpleNamespace(
            applications_url="https://www.missionchief.com/verband/bewerbungen",
        )
        cookie_manager = types.SimpleNamespace(
            get_session=AsyncMock(
                return_value=_FakeSession(
                    """
                    <html>
                      <a href="/users/sign_out">Sign out</a>
                      <a href="/profile/987654">Real Person</a>
                    </html>
                    """
                )
            ),
            config=types.SimpleNamespace(
                login_failure_url_contains=AsyncMock(
                    return_value=["/users/sign_in", "/login"]
                )
            ),
        )
        bot = types.SimpleNamespace(
            get_cog=lambda name: {
                "ApplicationsScraper": applications_scraper,
                "CookieManager": cookie_manager,
            }.get(name)
        )
        capture_cog = ApplicationsFixtureCapture(bot)
        ctx = types.SimpleNamespace(send=AsyncMock())

        with tempfile.TemporaryDirectory() as temporary_directory:
            fake_data_manager = types.SimpleNamespace(
                cog_data_path=lambda **kwargs: Path(temporary_directory)
            )
            with patch(
                "applicationscraper.fixture_capture_cog.data_manager",
                fake_data_manager,
            ):
                asyncio.run(capture_cog.capture_applications_fixture(ctx))

            files = sorted(Path(temporary_directory).glob("applications-*.html"))
            self.assertEqual(len(files), 2)

            raw_file = next(path for path in files if ".raw-private." in path.name)
            sanitized_file = next(path for path in files if ".sanitized-review." in path.name)

            self.assertIn("Real Person", raw_file.read_text(encoding="utf-8"))
            sanitized = sanitized_file.read_text(encoding="utf-8")
            self.assertNotIn("Real Person", sanitized)
            self.assertNotIn("987654", sanitized)
            self.assertIn("Fixture Applicant", sanitized)
            ctx.send.assert_awaited_once()
            sent_message = ctx.send.await_args
            attachment = sent_message.kwargs["file"]
            self.assertEqual(Path(attachment.path), sanitized_file)
            self.assertEqual(attachment.filename, sanitized_file.name)
            self.assertNotEqual(Path(attachment.path), raw_file)
            self.assertIn("private raw file remains local", sent_message.args[0].lower())
            self.assertIn(
                "capture again while an application is pending",
                sent_message.args[0].lower(),
            )

    def test_capture_command_rejects_a_real_login_redirect(self):
        applications_scraper = types.SimpleNamespace(
            applications_url="https://www.missionchief.com/verband/bewerbungen"
        )
        response = _FakeResponse(
            "<html>Login</html>",
            url="https://www.missionchief.com/users/sign_in",
        )
        session = types.SimpleNamespace(get=lambda *args, **kwargs: response)
        cookie_manager = types.SimpleNamespace(
            get_session=AsyncMock(return_value=session),
            config=types.SimpleNamespace(
                login_failure_url_contains=AsyncMock(
                    return_value=["/users/sign_in", "/login"]
                )
            ),
        )
        bot = types.SimpleNamespace(
            get_cog=lambda name: {
                "ApplicationsScraper": applications_scraper,
                "CookieManager": cookie_manager,
            }.get(name)
        )
        capture_cog = ApplicationsFixtureCapture(bot)
        ctx = types.SimpleNamespace(send=AsyncMock())

        asyncio.run(capture_cog.capture_applications_fixture(ctx))

        message = ctx.send.await_args.args[0]
        self.assertIn("redirected to a login page", message)


if __name__ == "__main__":
    unittest.main()
