import asyncio
import types
import unittest
from unittest.mock import AsyncMock

from membersscraper.members_scraper import MembersScraper


class MembersScraperEmptyScrapeTests(unittest.TestCase):
    def test_empty_scrape_does_not_detect_or_log_exits(self):
        scraper = MembersScraper.__new__(MembersScraper)
        scraper._get_session = AsyncMock(return_value=object())
        scraper._scrape_members_page = AsyncMock(return_value=[])
        scraper._detect_exits = AsyncMock()
        scraper._log_exits_to_database = AsyncMock()
        scraper._send_exit_notifications = AsyncMock()
        scraper._debug_log = AsyncMock()
        ctx = types.SimpleNamespace(send=AsyncMock())

        result = asyncio.run(scraper._scrape_all_members(ctx))

        self.assertFalse(result)
        self.assertEqual(scraper._scrape_members_page.await_count, 3)
        scraper._detect_exits.assert_not_awaited()
        scraper._log_exits_to_database.assert_not_awaited()
        scraper._send_exit_notifications.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
