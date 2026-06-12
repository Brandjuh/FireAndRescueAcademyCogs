import asyncio
import types
import unittest
from unittest.mock import AsyncMock

from logscraper.logs_scraper import LogsScraper


class LogsScraperCommandTests(unittest.TestCase):
    def setUp(self):
        self.scraper = LogsScraper.__new__(LogsScraper)
        self.scraper._scrape_all_logs = AsyncMock()
        event_timezone = AsyncMock(return_value="America/New_York")
        event_timezone.set = AsyncMock()
        self.scraper.config = types.SimpleNamespace(event_timezone=event_timezone)
        self.ctx = types.SimpleNamespace(send=AsyncMock())

    def test_scrape_rejects_non_positive_page_count(self):
        asyncio.run(self.scraper.scrape_logs(self.ctx, max_pages=0))

        self.scraper._scrape_all_logs.assert_not_awaited()
        self.assertIn("between 1 and 100", self.ctx.send.await_args.args[0])

    def test_backfill_rejects_non_positive_page_count(self):
        asyncio.run(self.scraper.backfill_logs(self.ctx, max_pages=-1))

        self.scraper._scrape_all_logs.assert_not_awaited()
        self.assertIn("between 1 and 500", self.ctx.send.await_args.args[0])

    def test_timezone_rejects_invalid_zone(self):
        asyncio.run(self.scraper.event_timezone(self.ctx, "Not/A_Real_Zone"))

        self.scraper.config.event_timezone.set.assert_not_awaited()
        self.assertIn("Invalid timezone", self.ctx.send.await_args.args[0])

    def test_timezone_saves_valid_zone(self):
        asyncio.run(self.scraper.event_timezone(self.ctx, "America/New_York"))

        self.scraper.config.event_timezone.set.assert_awaited_once_with("America/New_York")


if __name__ == "__main__":
    unittest.main()
