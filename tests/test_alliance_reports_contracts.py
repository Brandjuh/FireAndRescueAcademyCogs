import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from alliance_reports.calculators.activity_score import ActivityScoreCalculator
from alliance_reports.data_aggregator import DataAggregator
from alliance_reports.embed_formatter import EmbedFormatter
from alliance_reports.templates.daily_admin import DailyAdminReport
from alliance_reports.templates.monthly_admin import MonthlyAdminReport
from alliance_reports.templates.monthly_member import MonthlyMemberReport


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
        expected_start = datetime(2026, 5, 1)
        expected_end = datetime(2026, 6, 1)
        aggregator._get_membership_data_monthly.assert_awaited_once_with(expected_start, expected_end)

    def test_daily_member_output_contains_only_recorded_metrics(self):
        data = {
            "membership": {
                "total_members": 100,
                "new_joins_24h": 2,
                "left_24h": 1,
                "kicked_24h": 0,
                "verifications_approved_24h": 1,
            },
            "training": {"started_24h": 3, "completed_24h": 2},
            "buildings": {
                "approved_24h": 4,
                "extensions_started_24h": 2,
                "extensions_completed_24h": 1,
            },
            "operations": {
                "large_missions_started_24h": 1,
                "alliance_events_started_24h": 2,
            },
            "treasury": {
                "current_balance": 1000,
                "change_24h": 100,
                "change_24h_pct": 10,
                "contributors_24h": 3,
            },
        }

        embed = EmbedFormatter.create_daily_member_embed(data)
        output = "\n".join(field["value"] for field in embed.fields)

        self.assertIn("New members joined:** 2", output)
        self.assertIn("Courses started:** 3", output)
        self.assertNotIn("ACTIVITY SCORE", output.upper())
        self.assertNotIn("vs yesterday", output)

    def test_daily_admin_output_omits_unmeasured_claims(self):
        report = DailyAdminReport.__new__(DailyAdminReport)
        data = {
            "membership": {
                "total_members": 100,
                "new_joins_24h": 2,
                "left_24h": 1,
                "kicked_24h": 0,
                "verifications_approved_24h": 1,
                "verifications_pending": 2,
            },
            "training": {"started_24h": 3, "completed_24h": 2},
            "buildings": {
                "processed_24h": 3,
                "approved_24h": 2,
                "denied_24h": 1,
                "pending": 4,
                "extensions_started_24h": 1,
                "extensions_completed_24h": 1,
                "by_type_24h": {},
            },
            "treasury": {
                "current_balance": 1000,
                "change_24h": 100,
                "change_24h_pct": 10,
                "income_24h": 100,
                "expenses_24h": 0,
                "contributors_24h": 1,
                "largest_expense_24h": 0,
            },
            "sanctions": {"issued_24h": 1, "active_warnings": 2},
            "admin_activity": {
                "building_reviews_24h": 2,
                "sanctions_24h": 1,
                "most_active_admin": "Admin",
                "most_active_admin_count": 2,
            },
        }

        import discord

        embed = discord.Embed()
        report._add_membership(embed, data["membership"])
        report._add_training(embed, data["training"])
        report._add_buildings(embed, data["buildings"])
        report._add_sanctions(embed, data["sanctions"])
        report._add_admin_activity(embed, data["admin_activity"])
        output = "\n".join(field["value"] for field in embed.fields)

        for unsupported in (
            "Avg processing",
            "Inactive Watch",
            "Reminders sent",
            "In progress",
            "7-day trend",
            "1st warning",
            "Training approvals",
        ):
            self.assertNotIn(unsupported, output)

    def test_monthly_outputs_skip_failed_sources_and_fabricated_sections(self):
        from datetime import datetime

        data = {
            "membership": {"error": "unavailable"},
            "training": {"started_period": 3, "completed_period": 2},
            "buildings": {"error": "unavailable"},
            "operations": {"large_missions_period": 1, "alliance_events_period": 2},
            "sanctions": {"issued_period": 1},
            "admin_activity": {"error": "unavailable"},
        }
        now = datetime(2026, 6, 12)

        admin_embed = MonthlyAdminReport._create_overview(data, "May 2026", now, "Europe/Amsterdam")
        member_embed = MonthlyMemberReport._create_overview(data, "May 2026", now, "Europe/Amsterdam")
        admin_output = "\n".join(
            [field["name"] + "\n" + field["value"] for field in admin_embed.fields]
        )
        member_output = "\n".join(
            [field["name"] + "\n" + field["value"] for field in member_embed.fields]
        )

        for output in (admin_output, member_output):
            self.assertNotIn("Membership", output)
            self.assertNotIn("Buildings", output)
            self.assertNotIn("Treasury", output)
            self.assertNotIn("Activity Score", output)
            self.assertNotIn("Prediction", output)


if __name__ == "__main__":
    unittest.main()
