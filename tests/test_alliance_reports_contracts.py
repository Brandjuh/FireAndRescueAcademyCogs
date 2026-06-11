import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from alliance_reports.calculators.activity_score import ActivityScoreCalculator
from alliance_reports.data_aggregator import DataAggregator


class AllianceReportContractTests(unittest.TestCase):
    def test_activity_score_uses_daily_aggregator_keys(self):
        calculator = ActivityScoreCalculator(
            {
                "membership": 20,
                "training": 20,
                "buildings": 20,
                "treasury": 20,
                "operations": 20,
            }
        )
        data = {
            "membership": {
                "new_joins_24h": 2,
                "left_24h": 0,
                "kicked_24h": 0,
                "verifications_approved_24h": 1,
            },
            "training": {"started_24h": 2, "completed_24h": 2},
            "buildings": {
                "approved_24h": 2,
                "extensions_started_24h": 1,
                "extensions_completed_24h": 1,
            },
            "treasury": {
                "change_24h": 100,
                "change_24h_pct": 2,
                "contributors_24h": 2,
            },
            "operations": {
                "large_missions_started_24h": 1,
                "alliance_events_started_24h": 1,
            },
        }

        score = calculator.calculate_daily_score(data)

        self.assertGreater(score["components"]["membership"], 50)
        self.assertGreater(score["components"]["training"], 30)
        self.assertGreater(score["components"]["buildings"], 40)
        self.assertGreater(score["components"]["treasury"], 50)
        self.assertGreater(score["components"]["operations"], 40)

    def test_database_connection_uses_detected_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "members.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE marker (value INTEGER)")
            connection.close()

            config_manager = types.SimpleNamespace(
                _db_cache={"members_v2_db_path": db_path},
            )
            aggregator = DataAggregator(config_manager)

            detected_connection = aggregator._get_db_connection("members_v2")
            self.assertIsNotNone(detected_connection)
            detected_connection.close()

    def test_monthly_data_does_not_invent_metrics(self):
        aggregator = DataAggregator(types.SimpleNamespace(_db_cache={}))
        aggregator._get_membership_data_monthly = AsyncMock(return_value={})
        aggregator._get_training_data_monthly = AsyncMock(return_value={})
        aggregator._get_buildings_data_monthly = AsyncMock(return_value={})
        aggregator._get_operations_data_monthly = AsyncMock(return_value={})
        aggregator._get_treasury_data_monthly = AsyncMock(return_value={})
        aggregator._get_sanctions_data_monthly = AsyncMock(return_value={})
        aggregator._get_admin_activity_monthly = AsyncMock(return_value={})

        import asyncio
        from datetime import datetime

        data = asyncio.run(aggregator.get_monthly_data(datetime(2026, 5, 1)))

        self.assertIsNone(data["activity_score"])


if __name__ == "__main__":
    unittest.main()
