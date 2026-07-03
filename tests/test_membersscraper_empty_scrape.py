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

    def test_transient_page_failure_aborts_without_detecting_exits(self):
        scraper = MembersScraper.__new__(MembersScraper)
        scraper._get_session = AsyncMock(return_value=object())
        scraper._scrape_members_page = AsyncMock(return_value=None)
        scraper._detect_exits = AsyncMock()
        scraper._log_exits_to_database = AsyncMock()
        scraper._send_exit_notifications = AsyncMock()
        scraper._debug_log = AsyncMock()
        ctx = types.SimpleNamespace(send=AsyncMock())

        result = asyncio.run(scraper._scrape_all_members(ctx))

        self.assertFalse(result)
        self.assertEqual(scraper._scrape_members_page.await_count, 1)
        scraper._detect_exits.assert_not_awaited()
        scraper._log_exits_to_database.assert_not_awaited()
        scraper._send_exit_notifications.assert_not_awaited()

    def test_concurrent_scrape_is_rejected(self):
        scraper = MembersScraper.__new__(MembersScraper)
        scraper._debug_log = AsyncMock()
        scraper._scrape_lock = asyncio.Lock()
        ctx = types.SimpleNamespace(send=AsyncMock())

        async def run_with_existing_scrape():
            await scraper._scrape_lock.acquire()
            try:
                return await scraper._scrape_all_members(ctx)
            finally:
                scraper._scrape_lock.release()

        result = asyncio.run(run_with_existing_scrape())

        self.assertFalse(result)
        ctx.send.assert_awaited_once_with(
            "A members scrape is already running. Try again after it finishes."
        )


if __name__ == "__main__":
    unittest.main()
