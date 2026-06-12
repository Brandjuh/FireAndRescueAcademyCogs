import asyncio
import sqlite3
import tempfile
import types
import unittest
from pathlib import Path

import discord

from MemberManager.audit import (
    build_identity_filters,
    fetch_missionchief_events,
    merge_timeline_events,
    normalize_member_event,
    should_include_log_row,
)
from MemberManager.models import MemberData
from MemberManager.views import MemberOverviewView


class MemberManagerAuditTests(unittest.TestCase):
    def test_member_events_normalize_only_admin_audit_events(self):
        event = normalize_member_event(
            {
                "event_type": "note_created",
                "timestamp": 1_765_000_000,
                "triggered_by": "Admin",
                "actor_id": 123,
                "event_data": {"ref_code": "NOTE-1", "reason": "Needs follow-up"},
            }
        )

        self.assertIsNotNone(event)
        self.assertEqual(event.source, "MemberManager")
        self.assertEqual(event.title, "Note Created")
        self.assertEqual(event.reference, "NOTE-1")
        self.assertIn("Needs follow-up", event.details)

        self.assertIsNone(normalize_member_event({"event_type": "profile_viewed"}))

    def test_course_completed_logs_are_not_member_audit_entries(self):
        self.assertFalse(should_include_log_row({"action_key": "course_completed"}))
        self.assertFalse(should_include_log_row({"action_key": "course_created"}))
        self.assertFalse(should_include_log_row({"action_key": "building_constructed"}))
        self.assertFalse(should_include_log_row({"action_key": "extension_started"}))
        self.assertFalse(should_include_log_row({"action_key": "expansion_finished"}))
        self.assertFalse(should_include_log_row({"action_key": "large_mission_started"}))
        self.assertFalse(should_include_log_row({"action_key": "alliance_event_started"}))
        self.assertTrue(should_include_log_row({"action_key": "kicked_from_alliance"}))
        self.assertTrue(should_include_log_row({"action_key": "chat_ban_set"}))

    def test_identity_filter_uses_id_and_name_without_former_member_label(self):
        where_clause, params = build_identity_filters(
            mc_user_id="456",
            mc_username="MCUser",
        )

        self.assertIn("executed_mc_id", where_clause)
        self.assertIn("executed_name", where_clause)
        self.assertEqual(params, ["456", "456", "MCUser", "MCUser"])

        where_clause, params = build_identity_filters(
            mc_user_id="456",
            mc_username="Former member (456)",
        )

        self.assertIn("executed_mc_id", where_clause)
        self.assertNotIn("executed_name", where_clause)
        self.assertEqual(params, ["456", "456"])

    def test_merge_keeps_repeated_missionchief_events_and_sorts_newest_first(self):
        member_events = [
            {
                "event_type": "note_created",
                "timestamp": 100,
                "triggered_by": "Admin",
                "event_data": {"reason": "Older note"},
            }
        ]
        missionchief_events = [
            types.SimpleNamespace(
                source="MissionChief",
                event_type="course_created",
                timestamp=200,
                title="Created a course",
                actor_name="MCUser",
                actor_id=None,
                details="Course #1",
                reference="1",
                sort_key=200,
                matches=lambda query: query.lower() in "created a course course #1",
            ),
            types.SimpleNamespace(
                source="MissionChief",
                event_type="course_created",
                timestamp=200,
                title="Created a course",
                actor_name="MCUser",
                actor_id=None,
                details="Course #2",
                reference="2",
                sort_key=200,
                matches=lambda query: query.lower() in "created a course course #2",
            ),
        ]

        timeline = merge_timeline_events(member_events, missionchief_events)

        self.assertEqual(len(timeline), 3)
        self.assertEqual([event.reference for event in timeline[:2]], ["1", "2"])

    def test_fetch_missionchief_events_reads_existing_logscraper_database(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "logs_v3.db"
            connection = sqlite3.connect(db_path)
            connection.execute(
                """
                CREATE TABLE logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT,
                    event_timestamp TEXT,
                    action_key TEXT,
                    action_text TEXT,
                    executed_name TEXT,
                    executed_mc_id TEXT,
                    affected_name TEXT,
                    affected_mc_id TEXT,
                    description TEXT,
                    occurrence_index INTEGER
                )
                """
            )
            connection.executemany(
                """
                INSERT INTO logs (
                    ts, event_timestamp, action_key, action_text, executed_name,
                    executed_mc_id, affected_name, affected_mc_id, description,
                    occurrence_index
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "2026-06-12T10:00:00+00:00",
                        "2026-06-12T10:00:00+00:00",
                        "course_completed",
                        "Completed a course",
                        "MCUser",
                        "456",
                        "Academy #1",
                        "",
                        "Hotshot Crew Training",
                        1,
                    ),
                    (
                        "2026-06-12T10:01:00+00:00",
                        "2026-06-12T10:01:00+00:00",
                        "building_constructed",
                        "Building constructed",
                        "MCUser",
                        "456",
                        "Station #1",
                        "",
                        "Building log",
                        2,
                    ),
                    (
                        "2026-06-12T10:02:00+00:00",
                        "2026-06-12T10:02:00+00:00",
                        "large_mission_started",
                        "Large scale alliance mission started",
                        "MCUser",
                        "456",
                        "Mission",
                        "",
                        "Operation log",
                        3,
                    ),
                    (
                        "2026-06-12T10:00:00+00:00",
                        "2026-06-12T10:00:00+00:00",
                        "chat_ban_set",
                        "Chat ban set",
                        "MCUser",
                        "456",
                        "Academy #1",
                        "",
                        "Hotshot Crew Training",
                        4,
                    ),
                ],
            )
            connection.commit()
            connection.close()

            events = asyncio.run(
                fetch_missionchief_events(
                    db_path,
                    mc_user_id="456",
                    mc_username="MCUser",
                )
            )

        self.assertEqual(len(events), 1)
        self.assertTrue(all(event.source == "MissionChief" for event in events))
        self.assertEqual(events[0].title, "Chat ban set")

    def test_audit_embed_combines_membermanager_and_missionchief_events(self):
        class FakeDB:
            async def get_events(self, **kwargs):
                self.kwargs = kwargs
                return [
                    {
                        "event_type": "note_created",
                        "timestamp": 1_765_000_000,
                        "triggered_by": "Admin",
                        "actor_id": 123,
                        "event_data": {"reason": "Needs follow-up"},
                    }
                ]

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "logs_v3.db"
            connection = sqlite3.connect(db_path)
            connection.execute(
                """
                CREATE TABLE logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT,
                    event_timestamp TEXT,
                    action_key TEXT,
                    action_text TEXT,
                    executed_name TEXT,
                    executed_mc_id TEXT,
                    affected_name TEXT,
                    affected_mc_id TEXT,
                    description TEXT,
                    occurrence_index INTEGER
                )
                """
            )
            connection.execute(
                """
                INSERT INTO logs (
                    ts, event_timestamp, action_key, action_text, executed_name,
                    executed_mc_id, affected_name, affected_mc_id, description,
                    occurrence_index
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-06-12T10:00:00+00:00",
                    "2026-06-12T10:00:00+00:00",
                    "chat_ban_set",
                    "Chat ban set",
                    "MCUser",
                    "456",
                    "Academy #1",
                    "",
                    "Hotshot Crew Training",
                    1,
                ),
            )
            connection.execute(
                """
                INSERT INTO logs (
                    ts, event_timestamp, action_key, action_text, executed_name,
                    executed_mc_id, affected_name, affected_mc_id, description,
                    occurrence_index
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-06-12T11:00:00+00:00",
                    "2026-06-12T11:00:00+00:00",
                    "building_constructed",
                    "Building constructed",
                    "MCUser",
                    "456",
                    "Station #1",
                    "",
                    "Building log",
                    2,
                ),
            )
            connection.commit()
            connection.close()

            view = MemberOverviewView.__new__(MemberOverviewView)
            view.member_data = MemberData(
                discord_id=123,
                mc_user_id="456",
                discord_username="DiscordUser",
                mc_username="MCUser",
            )
            view.db = FakeDB()
            view.integrations = {"logs_scraper": types.SimpleNamespace(db_path=db_path)}
            view.audit_search_query = None
            view.audit_page = 0
            view.audit_per_page = 10
            view.guild = types.SimpleNamespace(
                get_member=lambda actor_id: types.SimpleNamespace(display_name="Admin Nick")
                if actor_id == 123
                else None
            )

            embed = discord.Embed(title="Audit", color=discord.Color.dark_gray())
            result = asyncio.run(view._build_audit_timeline_embed(embed))

        self.assertIn("Chat ban set", result.description)
        self.assertIn("Note Created", result.description)
        self.assertIn("Admin Nick", result.description)
        self.assertNotIn("Building constructed", result.description)


if __name__ == "__main__":
    unittest.main()
