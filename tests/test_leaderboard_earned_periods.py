import asyncio
import sqlite3
import sys
import tempfile
import unittest
import types
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path


class _UTC(tzinfo):
    def utcoffset(self, dt):
        del dt
        return timedelta(0)

    def dst(self, dt):
        del dt
        return timedelta(0)

    def tzname(self, dt):
        del dt
        return "UTC"

    def localize(self, value):
        return value.replace(tzinfo=self)


sys.modules.setdefault(
    "pytz",
    types.SimpleNamespace(
        UTC=_UTC(),
        timezone=lambda name: _UTC(),
    ),
)

from leaderboard.leaderboard import Leaderboard


def create_members_db(path: Path):
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE members (
            member_id INTEGER,
            username TEXT,
            rank TEXT,
            earned_credits INTEGER,
            contribution_rate REAL DEFAULT 0.0,
            online_status TEXT,
            timestamp TEXT,
            snapshot_source TEXT DEFAULT 'unknown',
            PRIMARY KEY (member_id, timestamp)
        )
        """
    )
    return connection


def insert_member_snapshot(
    connection,
    *,
    member_id: int,
    username: str,
    earned_credits: int,
    timestamp: str,
    snapshot_source: str = "live",
):
    connection.execute(
        """
        INSERT INTO members (
            member_id, username, rank, earned_credits, contribution_rate,
            online_status, timestamp, snapshot_source
        ) VALUES (?, ?, 'Member', ?, 5.0, 'offline', ?, ?)
        """,
        (member_id, username, earned_credits, timestamp, snapshot_source),
    )


def fixed_leaderboard(db_path: Path) -> Leaderboard:
    leaderboard = Leaderboard.__new__(Leaderboard)
    leaderboard.members_db_path = db_path
    leaderboard.tz_amsterdam = timezone.utc
    leaderboard.tz_ny = timezone.utc

    def fixed_boundaries(period, current_time):
        del period, current_time
        return (
            datetime(2026, 7, 5, 4, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 6, 3, 59, 59, tzinfo=timezone.utc),
            datetime(2026, 7, 4, 4, 0, tzinfo=timezone.utc),
            datetime(2026, 7, 5, 3, 59, 59, tzinfo=timezone.utc),
        )

    leaderboard._get_period_boundaries = fixed_boundaries
    return leaderboard


class LeaderboardEarnedPeriodTests(unittest.TestCase):
    def test_daily_earned_uses_before_window_baseline_with_single_in_window_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "members_v2.db"
            connection = create_members_db(db_path)
            try:
                insert_member_snapshot(
                    connection,
                    member_id=1,
                    username="ExpectedWinner",
                    earned_credits=1_000,
                    timestamp="2026-07-05T03:55:00",
                )
                insert_member_snapshot(
                    connection,
                    member_id=1,
                    username="ExpectedWinner",
                    earned_credits=1_700,
                    timestamp="2026-07-06T03:55:00",
                )
                insert_member_snapshot(
                    connection,
                    member_id=2,
                    username="SecondPlace",
                    earned_credits=5_000,
                    timestamp="2026-07-05T03:55:00",
                )
                insert_member_snapshot(
                    connection,
                    member_id=2,
                    username="SecondPlace",
                    earned_credits=5_300,
                    timestamp="2026-07-06T03:55:00",
                )
                connection.commit()
            finally:
                connection.close()

            result = asyncio.run(fixed_leaderboard(db_path)._get_earned_credits_rankings("daily"))

        self.assertIsNotNone(result)
        self.assertEqual(result["current"][0]["username"], "ExpectedWinner")
        self.assertEqual(result["current"][0]["credits"], 700)
        self.assertEqual(result["current"][1]["username"], "SecondPlace")
        self.assertEqual(result["current"][1]["credits"], 300)

    def test_daily_earned_prefers_live_snapshots_over_backfill_snapshots(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "members_v2.db"
            connection = create_members_db(db_path)
            try:
                insert_member_snapshot(
                    connection,
                    member_id=1,
                    username="LiveWinner",
                    earned_credits=1_000,
                    timestamp="2026-07-05T03:55:00",
                    snapshot_source="live",
                )
                insert_member_snapshot(
                    connection,
                    member_id=1,
                    username="LiveWinner",
                    earned_credits=1_500,
                    timestamp="2026-07-06T03:50:00",
                    snapshot_source="live",
                )
                insert_member_snapshot(
                    connection,
                    member_id=1,
                    username="LiveWinner",
                    earned_credits=9_999,
                    timestamp="2026-07-06T03:55:00",
                    snapshot_source="backfill",
                )
                connection.commit()
            finally:
                connection.close()

            result = asyncio.run(fixed_leaderboard(db_path)._get_earned_credits_rankings("daily"))

        self.assertIsNotNone(result)
        self.assertEqual(result["current"][0]["username"], "LiveWinner")
        self.assertEqual(result["current"][0]["credits"], 500)

    def test_daily_earned_uses_unknown_baseline_when_current_snapshot_is_live(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "members_v2.db"
            connection = create_members_db(db_path)
            try:
                insert_member_snapshot(
                    connection,
                    member_id=1,
                    username="MigratedBaseline",
                    earned_credits=2_000,
                    timestamp="2026-07-05T03:55:00",
                    snapshot_source="unknown",
                )
                insert_member_snapshot(
                    connection,
                    member_id=1,
                    username="MigratedBaseline",
                    earned_credits=2_600,
                    timestamp="2026-07-06T03:55:00",
                    snapshot_source="live",
                )
                connection.commit()
            finally:
                connection.close()

            result = asyncio.run(fixed_leaderboard(db_path)._get_earned_credits_rankings("daily"))

        self.assertIsNotNone(result)
        self.assertEqual(result["current"][0]["username"], "MigratedBaseline")
        self.assertEqual(result["current"][0]["credits"], 600)


if __name__ == "__main__":
    unittest.main()
