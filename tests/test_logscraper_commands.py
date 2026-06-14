import asyncio
import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
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

    def test_get_recent_logs_returns_latest_rows_in_ascending_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            scraper = LogsScraper.__new__(LogsScraper)
            scraper.db_path = Path(temp_dir) / "logs.db"
            LogsScraper._init_database(scraper)

            conn = sqlite3.connect(scraper.db_path)
            cursor = conn.cursor()
            for index, action_key in enumerate(["created_course", "kicked_from_alliance", "chat_ban_set"], start=1):
                cursor.execute(
                    """
                    INSERT INTO logs
                    (hash, ts, action_key, action_text, executed_name, affected_name, affected_mc_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"hash-{index}",
                        f"June 13, 2026 19:4{index}",
                        action_key,
                        action_key.replace("_", " "),
                        "Admin",
                        f"Member{index}",
                        str(index),
                    ),
                )
            conn.commit()
            conn.close()

            rows = asyncio.run(scraper.get_recent_logs(limit=2))

        self.assertEqual([row["id"] for row in rows], [2, 3])
        self.assertEqual([row["action_key"] for row in rows], ["kicked_from_alliance", "chat_ban_set"])


if __name__ == "__main__":
    unittest.main()
