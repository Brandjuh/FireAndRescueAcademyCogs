import asyncio
import sqlite3
import tempfile
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

from logscraper.logs_scraper import LogsScraper


class LogsScraperEventTimestampTests(unittest.TestCase):
    def test_normalizes_full_missionchief_timestamp_to_utc(self):
        scraped_at = datetime(2026, 6, 13, 14, 0, tzinfo=ZoneInfo("UTC"))

        result = LogsScraper._normalize_event_timestamp(
            "June 12, 2026 06:52",
            scraped_at,
            "America/New_York",
        )

        self.assertEqual(result, "2026-06-12T10:52:00+00:00")

    def test_normalizes_recent_summer_timestamp_to_utc(self):
        scraped_at = datetime(2026, 6, 12, 14, 0, tzinfo=ZoneInfo("UTC"))

        result = LogsScraper._normalize_event_timestamp(
            "12 Jun 09:30",
            scraped_at,
            "America/New_York",
        )

        self.assertEqual(result, "2026-06-12T13:30:00+00:00")

    def test_normalizes_recent_winter_timestamp_to_utc(self):
        scraped_at = datetime(2026, 1, 12, 15, 0, tzinfo=ZoneInfo("UTC"))

        result = LogsScraper._normalize_event_timestamp(
            "12 Jan 09:30",
            scraped_at,
            "America/New_York",
        )

        self.assertEqual(result, "2026-01-12T14:30:00+00:00")

    def test_rolls_recent_new_year_timestamp_into_previous_year(self):
        scraped_at = datetime(2026, 1, 1, 6, 0, tzinfo=ZoneInfo("UTC"))

        result = LogsScraper._normalize_event_timestamp(
            "31 Dec 23:30",
            scraped_at,
            "America/New_York",
        )

        self.assertEqual(result, "2026-01-01T04:30:00+00:00")

    def test_rejects_ambiguous_old_yearless_timestamp(self):
        scraped_at = datetime(2026, 6, 12, 14, 0, tzinfo=ZoneInfo("UTC"))

        result = LogsScraper._normalize_event_timestamp(
            "01 May 09:30",
            scraped_at,
            "America/New_York",
        )

        self.assertIsNone(result)

    def test_rejects_naive_scrape_timestamp(self):
        with self.assertRaises(ValueError):
            LogsScraper._normalize_event_timestamp(
                "12 Jun 09:30",
                datetime(2026, 6, 12, 14, 0),
                "America/New_York",
            )

    def test_does_not_guess_an_unconfigured_timezone(self):
        scraped_at = datetime(2026, 6, 12, 14, 0, tzinfo=ZoneInfo("UTC"))

        result = LogsScraper._normalize_event_timestamp("12 Jun 09:30", scraped_at, None)

        self.assertIsNone(result)

    def test_database_migration_adds_event_timestamp_column_and_index(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "logs.db"
            connection = sqlite3.connect(db_path)
            connection.execute(
                """
                CREATE TABLE logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    hash TEXT UNIQUE,
                    ts TEXT,
                    action_key TEXT,
                    executed_mc_id TEXT
                )
                """
            )
            connection.commit()
            connection.close()

            scraper = LogsScraper.__new__(LogsScraper)
            scraper.db_path = db_path
            scraper._init_database()

            connection = sqlite3.connect(db_path)
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(logs)").fetchall()
            }
            indexes = {
                row[1] for row in connection.execute("PRAGMA index_list(logs)").fetchall()
            }
            connection.close()

        self.assertIn("event_timestamp", columns)
        self.assertIn("signature", columns)
        self.assertIn("occurrence_index", columns)
        self.assertIn("idx_logs_event_timestamp", indexes)

    def test_identical_visible_rows_receive_distinct_stable_hashes(self):
        signature = "same-visible-row"
        logs = [
            {"hash": signature},
            {"hash": signature},
            {"hash": signature},
            {"hash": signature},
        ]

        LogsScraper._assign_occurrence_hashes(logs)

        self.assertEqual([entry["occurrence_index"] for entry in logs], [1, 2, 3, 4])
        self.assertEqual(len({entry["hash"] for entry in logs}), 4)
        self.assertEqual(logs[0]["hash"], signature)

    def test_recent_duplicate_backfills_missing_event_timestamp(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "logs.db"
            scraper = LogsScraper.__new__(LogsScraper)
            scraper.db_path = db_path
            scraper._init_database()

            connection = sqlite3.connect(db_path)
            connection.execute(
                "INSERT INTO logs (hash, ts, action_key) VALUES (?, ?, ?)",
                ("existing-hash", "placeholder", "left_alliance"),
            )
            connection.commit()
            connection.close()

            raw_timestamp = datetime.now(ZoneInfo("America/New_York")).strftime("%d %b %H:%M")
            scraped_log = {
                "hash": "existing-hash",
                "ts": raw_timestamp,
                "action_key": "left_alliance",
                "action_text": "left the alliance",
                "executed_name": "Member",
                "executed_mc_id": "1",
                "executed_url": "",
                "affected_name": "",
                "affected_type": "",
                "affected_mc_id": "",
                "affected_url": "",
                "description": "left the alliance",
                "contribution_amount": 0,
            }
            event_timezone = AsyncMock(return_value="America/New_York")
            last_scrape = types.SimpleNamespace(set=AsyncMock())
            scraper.config = types.SimpleNamespace(
                event_timezone=event_timezone,
                last_scrape=last_scrape,
            )
            scraper._get_session = AsyncMock(return_value=object())
            scraper._scrape_logs_page = AsyncMock(return_value=[scraped_log])
            scraper._debug_log = AsyncMock()

            with patch("logscraper.logs_scraper.asyncio.sleep", new=AsyncMock()):
                result = asyncio.run(scraper._scrape_all_logs(None, max_pages=1))

            connection = sqlite3.connect(db_path)
            event_timestamp = connection.execute(
                "SELECT event_timestamp FROM logs WHERE hash = ?",
                ("existing-hash",),
            ).fetchone()[0]
            connection.close()

        self.assertTrue(result)
        self.assertIsNotNone(event_timestamp)

    def test_public_member_logs_contract_filters_identity_and_actions(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "logs.db"
            scraper = LogsScraper.__new__(LogsScraper)
            scraper.db_path = db_path
            scraper._init_database()

            connection = sqlite3.connect(db_path)
            connection.executemany(
                """
                INSERT INTO logs (
                    hash, ts, event_timestamp, action_key, action_text,
                    executed_name, executed_mc_id, affected_name, affected_mc_id,
                    description, occurrence_index, contribution_amount
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "chat-ban",
                        "2026-06-12T10:00:00+00:00",
                        "2026-06-12T10:00:00+00:00",
                        "chat_ban_set",
                        "Chat ban set",
                        "Admin",
                        "999",
                        "MCUser",
                        "456",
                        "Personal audit log",
                        1,
                        0,
                    ),
                    (
                        "building",
                        "2026-06-12T11:00:00+00:00",
                        "2026-06-12T11:00:00+00:00",
                        "building_constructed",
                        "Building constructed",
                        "MCUser",
                        "456",
                        "Station",
                        "",
                        "Building log",
                        1,
                        0,
                    ),
                ],
            )
            connection.commit()
            connection.close()

            result = asyncio.run(
                scraper.get_member_logs(
                    mc_user_id="456",
                    mc_username="MCUser",
                    action_keys={"chat_ban_set"},
                    include_total=True,
                )
            )

        self.assertEqual(result["total"], 1)
        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0]["action_key"], "chat_ban_set")


if __name__ == "__main__":
    unittest.main()
