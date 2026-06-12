import asyncio
import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from incomescraper.income_scraper import IncomeScraper
from logscraper.logs_scraper import LogsScraper


def field_values(embed):
    return {field["name"]: field["value"] for field in embed.fields}


class ScraperStorageStatsTests(unittest.TestCase):
    def test_logs_stats_reports_timestamp_and_occurrence_storage(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            scraper = LogsScraper.__new__(LogsScraper)
            scraper.db_path = Path(temp_dir) / "logs.db"
            scraper._init_database()

            connection = sqlite3.connect(scraper.db_path)
            connection.executemany(
                """
                INSERT INTO logs (
                    hash, ts, event_timestamp, signature, occurrence_index, action_key
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "one",
                        "June 12, 2026 06:52",
                        "2026-06-12T10:52:00+00:00",
                        "same",
                        1,
                        "created_course",
                    ),
                    (
                        "two",
                        "June 12, 2026 06:52",
                        "2026-06-12T10:52:00+00:00",
                        "same",
                        2,
                        "created_course",
                    ),
                    ("legacy", "old value", None, None, 1, "other"),
                ],
            )
            connection.commit()
            connection.close()

            ctx = types.SimpleNamespace(send=AsyncMock())
            asyncio.run(scraper.show_stats(ctx))

        embed = ctx.send.await_args.kwargs["embed"]
        values = field_values(embed)
        self.assertEqual(values["Event Timestamps"], "2 / 3")
        self.assertEqual(values["Repeated Actions Preserved"], "1")
        self.assertEqual(
            values["Event Range (UTC)"],
            "2026-06-12T10:52:00+00:00 to 2026-06-12T10:52:00+00:00",
        )

    def test_income_stats_reports_new_expense_storage_and_snapshots(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            scraper = IncomeScraper.__new__(IncomeScraper)
            scraper.db_path = Path(temp_dir) / "income.db"
            scraper._init_database()

            connection = sqlite3.connect(scraper.db_path)
            connection.executemany(
                """
                INSERT INTO income (
                    entry_type, period, username, amount, description, timestamp
                ) VALUES ('income', ?, ?, 100, NULL, ?)
                """,
                [
                    ("daily", "Member", "2026-06-12T03:55:00+00:00"),
                    ("monthly", "Member", "2026-06-12T03:55:00+00:00"),
                ],
            )
            connection.executemany(
                """
                INSERT INTO expenses (
                    signature, occurrence_index, username, amount, description,
                    source_date, event_timestamp, scraped_at
                ) VALUES (?, ?, 'Member', 25000, 'Extended guard', ?, ?, ?)
                """,
                [
                    (
                        "same",
                        1,
                        "12 Jun 06:52",
                        "2026-06-12T10:52:00+00:00",
                        "2026-06-12T11:00:00+00:00",
                    ),
                    (
                        "same",
                        2,
                        "12 Jun 06:52",
                        "2026-06-12T10:52:00+00:00",
                        "2026-06-12T11:00:00+00:00",
                    ),
                    (
                        "old",
                        1,
                        "01 Feb 06:52",
                        None,
                        "2026-06-12T11:00:00+00:00",
                    ),
                ],
            )
            connection.commit()
            connection.close()

            ctx = types.SimpleNamespace(send=AsyncMock())
            asyncio.run(scraper.show_stats(ctx))

        embed = ctx.send.await_args.kwargs["embed"]
        values = field_values(embed)
        self.assertEqual(values["Preserved Expense Entries"], "3")
        self.assertEqual(values["Timestamped Expenses"], "2 / 3")
        self.assertEqual(values["Repeated Expenses Preserved"], "1")
        self.assertIn("Daily: 2026-06-12T03:55:00+00:00", values["Latest Income Snapshots"])
        self.assertIn("Monthly: 2026-06-12T03:55:00+00:00", values["Latest Income Snapshots"])


if __name__ == "__main__":
    unittest.main()
