import asyncio
import types
import unittest
from unittest.mock import AsyncMock

from logscraper.logs_scraper import LogsScraper


class LogsScraperCommandTests(unittest.TestCase):
    def setUp(self):
        self.scraper = LogsScraper.__new__(LogsScraper)
        self.scraper._scrape_all_logs = AsyncMock()
        self.ctx = types.SimpleNamespace(send=AsyncMock())

    def test_scrape_rejects_non_positive_page_count(self):
        asyncio.run(self.scraper.scrape_logs(self.ctx, max_pages=0))

        self.scraper._scrape_all_logs.assert_not_awaited()
        self.assertIn("between 1 and 100", self.ctx.send.await_args.args[0])

    def test_backfill_rejects_non_positive_page_count(self):
        asyncio.run(self.scraper.backfill_logs(self.ctx, max_pages=-1))

        self.scraper._scrape_all_logs.assert_not_awaited()
        self.assertIn("between 1 and 500", self.ctx.send.await_args.args[0])


if __name__ == "__main__":
    unittest.main()
