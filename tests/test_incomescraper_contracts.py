import asyncio
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from incomescraper.income_scraper import IncomeScraper


class IncomeScraperContractTests(unittest.TestCase):
    def test_next_pre_reset_snapshot_is_2355_new_york(self):
        eastern = ZoneInfo("America/New_York")

        target = IncomeScraper._next_pre_reset_snapshot(
            datetime(2026, 6, 12, 20, 0, tzinfo=eastern)
        )

        self.assertEqual(target, datetime(2026, 6, 12, 23, 55, tzinfo=eastern))

    def test_recent_expense_date_gets_new_york_year_and_utc_time(self):
        scraped_at = datetime(2026, 6, 12, 14, 0, tzinfo=ZoneInfo("UTC"))

        result = IncomeScraper._normalize_expense_timestamp("12 Jun 06:52", scraped_at)

        self.assertEqual(result, "2026-06-12T10:52:00+00:00")

    def test_old_expense_date_does_not_get_a_guessed_year(self):
        scraped_at = datetime(2026, 6, 12, 14, 0, tzinfo=ZoneInfo("UTC"))

        result = IncomeScraper._normalize_expense_timestamp("01 Feb 06:52", scraped_at)

        self.assertIsNone(result)

    def test_identical_visible_expenses_receive_occurrence_indexes(self):
        scraped_at = datetime(2026, 6, 12, 14, 0, tzinfo=ZoneInfo("UTC"))
        entries = [
            {
                "source_date": "12 Jun 06:52",
                "username": "Member",
                "amount": 25000,
                "description": "Extended guard",
            }
            for _ in range(4)
        ]

        IncomeScraper._assign_expense_occurrences(entries, scraped_at)

        self.assertEqual([entry["occurrence_index"] for entry in entries], [1, 2, 3, 4])
        self.assertEqual(len({entry["signature"] for entry in entries}), 1)

    def test_database_initialization_keeps_income_and_separates_expenses(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "income.db"
            scraper = IncomeScraper.__new__(IncomeScraper)
            scraper.db_path = db_path

            scraper._init_database()

            connection = sqlite3.connect(db_path)
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            expense_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(expenses)").fetchall()
            }
            connection.close()

        self.assertIn("income", tables)
        self.assertIn("expenses", tables)
        self.assertIn("source_date", expense_columns)
        self.assertIn("event_timestamp", expense_columns)
        self.assertIn("occurrence_index", expense_columns)

    def test_repeated_identical_expenses_are_preserved_without_rescrape_duplicates(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            scraper = IncomeScraper.__new__(IncomeScraper)
            scraper.db_path = Path(temp_dir) / "income.db"
            scraper._scrape_lock = asyncio.Lock()
            scraper._get_session = AsyncMock(return_value=object())
            scraper._scrape_income_tab = AsyncMock(return_value=[])
            scraper._debug_log = AsyncMock()
            expense = {
                "entry_type": "expense",
                "period": "paginated",
                "username": "Member",
                "amount": 25000,
                "description": "Extended guard",
                "source_date": datetime.now(ZoneInfo("America/New_York")).strftime(
                    "%d %b %H:%M"
                ),
            }
            scraper._scrape_expenses_pages = AsyncMock(
                return_value=[dict(expense) for _ in range(4)]
            )
            scraper._init_database()

            with patch("incomescraper.income_scraper.asyncio.sleep", new=AsyncMock()):
                self.assertTrue(
                    asyncio.run(
                        scraper._scrape_all_income(
                            include_expenses=True,
                            max_expense_pages=1,
                        )
                    )
                )
                self.assertTrue(
                    asyncio.run(
                        scraper._scrape_all_income(
                            include_expenses=True,
                            max_expense_pages=1,
                        )
                    )
                )

            connection = sqlite3.connect(scraper.db_path)
            count = connection.execute("SELECT COUNT(*) FROM expenses").fetchone()[0]
            connection.close()

        self.assertEqual(count, 4)


if __name__ == "__main__":
    unittest.main()
