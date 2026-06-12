import sqlite3
import tempfile
import types
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

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
        aggregator._get_sanctions_data_monthly = AsyncMock(return_value={})
        aggregator._get_admin_activity_monthly = AsyncMock(return_value={})

        import asyncio

        data = asyncio.run(aggregator.get_monthly_data(datetime(2026, 5, 1)))

        self.assertNotIn("activity_score", data)
        self.assertNotIn("treasury", data)
        expected_start = datetime(2026, 5, 1, 4, tzinfo=ZoneInfo("UTC"))
        expected_end = datetime(2026, 6, 1, 4, tzinfo=ZoneInfo("UTC"))
        aggregator._get_membership_data_monthly.assert_awaited_once_with(expected_start, expected_end)

    def test_game_day_window_uses_eastern_dst_rules(self):
        winter_now = datetime(2026, 1, 12, 12, tzinfo=ZoneInfo("UTC"))
        summer_now = datetime(2026, 6, 12, 12, tzinfo=ZoneInfo("UTC"))

        winter_start, _ = DataAggregator._get_game_day_window(winter_now)
        summer_start, _ = DataAggregator._get_game_day_window(summer_now)

        self.assertEqual(winter_start.hour, 5)
        self.assertEqual(summer_start.hour, 4)

    def test_game_month_window_uses_eastern_dst_rules(self):
        winter_start, winter_end = DataAggregator._get_game_month_window(
            datetime(2026, 1, 15)
        )
        summer_start, summer_end = DataAggregator._get_game_month_window(
            datetime(2026, 6, 15)
        )

        self.assertEqual(winter_start, datetime(2026, 1, 1, 5, tzinfo=ZoneInfo("UTC")))
        self.assertEqual(winter_end, datetime(2026, 2, 1, 5, tzinfo=ZoneInfo("UTC")))
        self.assertEqual(summer_start, datetime(2026, 6, 1, 4, tzinfo=ZoneInfo("UTC")))
        self.assertEqual(summer_end, datetime(2026, 7, 1, 4, tzinfo=ZoneInfo("UTC")))

    def test_monthly_membership_uses_actual_snapshots_for_net_growth(self):
        import asyncio

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            members_path = temp_path / "members.db"
            logs_path = temp_path / "logs.db"
            sanctions_path = temp_path / "sanctions.db"

            members = sqlite3.connect(members_path)
            members.execute("CREATE TABLE members (member_id INTEGER, timestamp TEXT)")
            members.executemany(
                "INSERT INTO members VALUES (?, ?)",
                [
                    (1, "2026-04-30T23:00:00"),
                    (2, "2026-04-30T23:00:00"),
                    (1, "2026-05-31T23:00:00"),
                    (2, "2026-05-31T23:00:00"),
                    (3, "2026-05-31T23:00:00"),
                ],
            )
            members.commit()
            members.close()

            logs = sqlite3.connect(logs_path)
            logs.execute(
                "CREATE TABLE logs (action_key TEXT, scraped_at TEXT, event_timestamp TEXT)"
            )
            logs.executemany(
                "INSERT INTO logs VALUES (?, ?, ?)",
                [
                    ("added_to_alliance", "2026-06-12T12:00:00", "2026-05-10T12:00:00"),
                    ("added_to_alliance", "2026-06-12T12:00:00", "2026-05-11T12:00:00"),
                    ("left_alliance", "2026-06-12T12:00:00", "2026-05-12T12:00:00"),
                ],
            )
            logs.commit()
            logs.close()

            sanctions = sqlite3.connect(sanctions_path)
            sanctions.execute("CREATE TABLE sanctions (sanction_type TEXT, created_at INTEGER)")
            sanctions.commit()
            sanctions.close()

            config_manager = types.SimpleNamespace(
                _db_cache={
                    "members_v2_db_path": members_path,
                    "logs_v2_db_path": logs_path,
                    "sanctions_db_path": sanctions_path,
                }
            )
            aggregator = DataAggregator(config_manager)
            result = asyncio.run(
                aggregator._get_membership_data_monthly(
                    datetime(2026, 5, 1),
                    datetime(2026, 6, 1),
                )
            )

        self.assertEqual(result["starting_members"], 2)
        self.assertEqual(result["ending_members"], 3)
        self.assertEqual(result["net_growth"], 1)
        self.assertEqual(result["new_joins_period"], 2)
        self.assertEqual(result["left_period"], 1)

    def test_daily_training_uses_event_time_instead_of_scrape_time(self):
        import asyncio

        with tempfile.TemporaryDirectory() as temp_dir:
            logs_path = Path(temp_dir) / "logs.db"
            logs = sqlite3.connect(logs_path)
            logs.execute(
                "CREATE TABLE logs (action_key TEXT, scraped_at TEXT, event_timestamp TEXT)"
            )
            logs.executemany(
                "INSERT INTO logs VALUES (?, ?, ?)",
                [
                    ("created_course", "2026-06-12T12:00:00+00:00", "2026-06-12T05:00:00+00:00"),
                    ("course_completed", "2026-06-12T12:00:00+00:00", "2026-06-11T23:59:00+00:00"),
                    ("created_course", "2026-06-12T12:00:00+00:00", None),
                ],
            )
            logs.commit()
            logs.close()

            aggregator = DataAggregator(
                types.SimpleNamespace(_db_cache={"logs_v2_db_path": logs_path})
            )
            result = asyncio.run(
                aggregator._get_training_data_daily(
                    datetime(2026, 6, 12, 4, 0, tzinfo=ZoneInfo("UTC")),
                    datetime(2026, 6, 12, 13, 0, tzinfo=ZoneInfo("UTC")),
                )
            )

        self.assertEqual(result["started_24h"], 1)
        self.assertEqual(result["completed_24h"], 0)

    def test_monthly_operations_use_event_time_instead_of_scrape_time(self):
        import asyncio

        with tempfile.TemporaryDirectory() as temp_dir:
            logs_path = Path(temp_dir) / "logs.db"
            logs = sqlite3.connect(logs_path)
            logs.execute(
                "CREATE TABLE logs (action_key TEXT, scraped_at TEXT, event_timestamp TEXT)"
            )
            logs.executemany(
                "INSERT INTO logs VALUES (?, ?, ?)",
                [
                    ("large_mission_started", "2026-06-01T00:01:00", "2026-05-31T23:59:00"),
                    ("alliance_event_started", "2026-05-31T23:59:00", "2026-06-01T00:01:00"),
                    ("alliance_event_started", "2026-05-10T12:00:00", None),
                ],
            )
            logs.commit()
            logs.close()

            aggregator = DataAggregator(
                types.SimpleNamespace(_db_cache={"logs_v2_db_path": logs_path})
            )
            result = asyncio.run(
                aggregator._get_operations_data_monthly(
                    datetime(2026, 5, 1),
                    datetime(2026, 6, 1),
                )
            )

        self.assertEqual(result["large_missions_period"], 1)
        self.assertEqual(result["alliance_events_period"], 0)

    def test_monthly_training_reports_unavailable_without_event_coverage(self):
        import asyncio

        with tempfile.TemporaryDirectory() as temp_dir:
            logs_path = Path(temp_dir) / "logs.db"
            logs = sqlite3.connect(logs_path)
            logs.execute(
                "CREATE TABLE logs (action_key TEXT, scraped_at TEXT, event_timestamp TEXT)"
            )
            logs.executemany(
                "INSERT INTO logs VALUES (?, ?, ?)",
                [
                    ("created_course", "2026-05-10T12:00:00", None),
                    ("course_completed", "2026-05-11T12:00:00", None),
                ],
            )
            logs.commit()
            logs.close()

            aggregator = DataAggregator(
                types.SimpleNamespace(_db_cache={"logs_v2_db_path": logs_path})
            )
            result = asyncio.run(
                aggregator._get_training_data_monthly(
                    datetime(2026, 5, 1),
                    datetime(2026, 6, 1),
                )
            )

        self.assertIn("error", result)

    def test_monthly_membership_marks_log_activity_unavailable_without_coverage(self):
        import asyncio

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            members_path = temp_path / "members.db"
            logs_path = temp_path / "logs.db"
            sanctions_path = temp_path / "sanctions.db"

            members = sqlite3.connect(members_path)
            members.execute("CREATE TABLE members (member_id INTEGER, timestamp TEXT)")
            members.executemany(
                "INSERT INTO members VALUES (?, ?)",
                [
                    (1, "2026-04-30T23:00:00"),
                    (1, "2026-05-31T23:00:00"),
                    (2, "2026-05-31T23:00:00"),
                ],
            )
            members.commit()
            members.close()

            logs = sqlite3.connect(logs_path)
            logs.execute(
                "CREATE TABLE logs (action_key TEXT, scraped_at TEXT, event_timestamp TEXT)"
            )
            logs.execute("INSERT INTO logs VALUES ('added_to_alliance', '2026-05-10T12:00:00', NULL)")
            logs.commit()
            logs.close()

            sanctions = sqlite3.connect(sanctions_path)
            sanctions.execute("CREATE TABLE sanctions (sanction_type TEXT, created_at INTEGER)")
            sanctions.commit()
            sanctions.close()

            aggregator = DataAggregator(
                types.SimpleNamespace(
                    _db_cache={
                        "members_v2_db_path": members_path,
                        "logs_v2_db_path": logs_path,
                        "sanctions_db_path": sanctions_path,
                    }
                )
            )
            result = asyncio.run(
                aggregator._get_membership_data_monthly(
                    datetime(2026, 5, 1),
                    datetime(2026, 6, 1),
                )
            )

        self.assertFalse(result["log_activity_available"])

    def test_monthly_membership_does_not_publish_zero_without_start_snapshot(self):
        import asyncio

        with tempfile.TemporaryDirectory() as temp_dir:
            members_path = Path(temp_dir) / "members.db"
            members = sqlite3.connect(members_path)
            members.execute("CREATE TABLE members (member_id INTEGER, timestamp TEXT)")
            members.execute("INSERT INTO members VALUES (1, '2026-05-31T23:00:00')")
            members.commit()
            members.close()

            aggregator = DataAggregator(
                types.SimpleNamespace(_db_cache={"members_v2_db_path": members_path})
            )
            result = asyncio.run(
                aggregator._get_membership_data_monthly(
                    datetime(2026, 5, 1),
                    datetime(2026, 6, 1),
                )
            )

        self.assertIn("error", result)

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

        self.assertIn("Join logs recorded:** 2", output)
        self.assertNotIn("(+1)", output)
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

    def test_monthly_outputs_omit_log_lines_without_event_coverage(self):
        from datetime import datetime

        data = {
            "membership": {
                "starting_members": 10,
                "ending_members": 12,
                "new_joins_period": 0,
                "left_period": 0,
                "kicked_period": 1,
                "net_growth": 2,
                "log_activity_available": False,
            },
            "training": {"error": "Log event timestamps are unavailable"},
            "buildings": {
                "approved_period": 3,
                "denied_period": 1,
                "extensions_started_period": 0,
                "extensions_completed_period": 0,
                "extension_activity_available": False,
            },
            "operations": {"error": "Log event timestamps are unavailable"},
            "sanctions": {"issued_period": 1},
            "admin_activity": {"error": "unavailable"},
        }
        now = datetime(2026, 6, 12)

        admin_embed = MonthlyAdminReport._create_overview(data, "May 2026", now, "Europe/Amsterdam")
        member_embed = MonthlyMemberReport._create_overview(data, "May 2026", now, "Europe/Amsterdam")
        output = "\n".join(
            field["name"] + "\n" + field["value"]
            for embed in (admin_embed, member_embed)
            for field in embed.fields
        )

        self.assertIn("Starting members: 10", output)
        self.assertIn("Kicked: 1", output)
        self.assertIn("Requests approved: 3", output)
        self.assertNotIn("Join logs recorded", output)
        self.assertNotIn("Leave logs recorded", output)
        self.assertNotIn("Courses started", output)
        self.assertNotIn("Extensions started", output)
        self.assertNotIn("Large missions started", output)

    def test_monthly_member_uses_explicit_scheduled_report_month(self):
        import asyncio

        report = MonthlyMemberReport.__new__(MonthlyMemberReport)
        report.config_manager = types.SimpleNamespace(
            config=types.SimpleNamespace(timezone=AsyncMock(return_value="Europe/Amsterdam"))
        )
        report.aggregator = types.SimpleNamespace(get_monthly_data=AsyncMock(return_value={}))
        report_month = datetime(2026, 6, 30, 23, 55, tzinfo=ZoneInfo("America/New_York"))

        asyncio.run(report.generate(report_month=report_month))

        report.aggregator.get_monthly_data.assert_awaited_once_with(report_month)

    def test_monthly_admin_uses_explicit_scheduled_report_month(self):
        import asyncio

        report = MonthlyAdminReport.__new__(MonthlyAdminReport)
        report.config_manager = types.SimpleNamespace(
            config=types.SimpleNamespace(timezone=AsyncMock(return_value="Europe/Amsterdam"))
        )
        report.aggregator = types.SimpleNamespace(get_monthly_data=AsyncMock(return_value={}))
        report_month = datetime(2026, 6, 30, 23, 55, tzinfo=ZoneInfo("America/New_York"))

        asyncio.run(report.generate(report_month=report_month))

        report.aggregator.get_monthly_data.assert_awaited_once_with(report_month)


if __name__ == "__main__":
    unittest.main()
