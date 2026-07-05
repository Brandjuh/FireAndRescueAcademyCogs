import asyncio
import sqlite3
import sys
import tempfile
import unittest
import types
from datetime import datetime, timezone
from pathlib import Path

sys.modules.setdefault(
    "pytz",
    types.SimpleNamespace(
        UTC=timezone.utc,
        timezone=lambda name: timezone.utc,
    ),
)

from leaderboard.leaderboard import Leaderboard


class LeaderboardTreasuryPeriodTests(unittest.TestCase):
    def test_daily_contribution_uses_completed_new_york_day_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "income_v2.db"
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE income (
                    entry_type TEXT NOT NULL,
                    period TEXT NOT NULL,
                    username TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    description TEXT,
                    timestamp TEXT NOT NULL
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO income (
                    entry_type, period, username, amount, description, timestamp
                )
                VALUES ('income', 'daily', ?, ?, '', ?)
                """,
                [
                    ("WrongNewDay", 999999, "2026-07-04T04:05:00+00:00"),
                    ("ExpectedWinner", 500000, "2026-07-04T03:55:00+00:00"),
                    ("SecondPlace", 250000, "2026-07-04T03:55:00+00:00"),
                    ("ExpectedWinner", 100000, "2026-07-03T03:55:00+00:00"),
                ],
            )
            conn.commit()
            conn.close()

            leaderboard = Leaderboard.__new__(Leaderboard)
            leaderboard.income_db_path = db_path
            leaderboard.tz_amsterdam = timezone.utc
            leaderboard.tz_ny = timezone.utc

            def fixed_boundaries(period, current_time):
                del period, current_time
                return (
                    datetime(2026, 7, 3, 4, 0, tzinfo=timezone.utc),
                    datetime(2026, 7, 4, 3, 59, 59, tzinfo=timezone.utc),
                    datetime(2026, 7, 2, 4, 0, tzinfo=timezone.utc),
                    datetime(2026, 7, 3, 3, 59, 59, tzinfo=timezone.utc),
                )

            leaderboard._get_period_boundaries = fixed_boundaries

            result = asyncio.run(leaderboard._get_treasury_rankings("daily"))

        self.assertIsNotNone(result)
        self.assertEqual(result["current"][0]["username"], "ExpectedWinner")
        self.assertEqual(result["current"][0]["credits"], 500000)
        self.assertNotIn(
            "WrongNewDay",
            [entry["username"] for entry in result["current"]],
        )
        self.assertEqual(result["previous"][0]["username"], "ExpectedWinner")

    def test_daily_contribution_accepts_legacy_naive_amsterdam_timestamps(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "income_v2.db"
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE income (
                    entry_type TEXT NOT NULL,
                    period TEXT NOT NULL,
                    username TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    description TEXT,
                    timestamp TEXT NOT NULL
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO income (
                    entry_type, period, username, amount, description, timestamp
                )
                VALUES ('income', 'daily', ?, ?, '', ?)
                """,
                [
                    ("WrongNewDay", 999999, "2026-07-04T06:05:00"),
                    ("ExpectedWinner", 500000, "2026-07-04T05:55:00"),
                    ("SecondPlace", 250000, "2026-07-04T05:55:00"),
                    ("ExpectedWinner", 100000, "2026-07-03T05:55:00"),
                ],
            )
            conn.commit()
            conn.close()

            leaderboard = Leaderboard.__new__(Leaderboard)
            leaderboard.income_db_path = db_path
            leaderboard.tz_amsterdam = timezone.utc
            leaderboard.tz_ny = timezone.utc

            def fixed_boundaries(period, current_time):
                del period, current_time
                return (
                    datetime(2026, 7, 4, 5, 50, tzinfo=timezone.utc),
                    datetime(2026, 7, 4, 6, 0, tzinfo=timezone.utc),
                    datetime(2026, 7, 3, 5, 50, tzinfo=timezone.utc),
                    datetime(2026, 7, 3, 6, 0, tzinfo=timezone.utc),
                )

            leaderboard._get_period_boundaries = fixed_boundaries

            result = asyncio.run(leaderboard._get_treasury_rankings("daily"))

        self.assertIsNotNone(result)
        self.assertEqual(result["current"][0]["username"], "ExpectedWinner")
        self.assertNotIn(
            "WrongNewDay",
            [entry["username"] for entry in result["current"]],
        )


if __name__ == "__main__":
    unittest.main()
