import asyncio
import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from MemberManager.models import MemberData
from MemberManager.views import MemberOverviewView, ToggleOverviewModeButton


def create_logs_db(db_path: Path):
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
            contribution_amount INTEGER
        )
        """
    )
    connection.executemany(
        """
        INSERT INTO logs (
            ts, event_timestamp, action_key, action_text, executed_name,
            executed_mc_id, affected_name, affected_mc_id, description,
            contribution_amount
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "2026-06-12T10:00:00+00:00",
                "2026-06-12T10:00:00+00:00",
                "large_mission_started",
                "Large scale mission started",
                "MCUser",
                "456",
                "Mission",
                "",
                "Large operation",
                0,
            ),
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
                0,
            ),
        ],
    )
    connection.commit()
    connection.close()


def build_view(db_path: Path) -> MemberOverviewView:
    view = MemberOverviewView.__new__(MemberOverviewView)
    view.member_data = MemberData(
        discord_id=123,
        mc_user_id="456",
        discord_username="DiscordUser",
        mc_username="MCUser",
        link_status="approved",
        is_verified=True,
    )
    view.integrations = {"logs_scraper": types.SimpleNamespace(db_path=db_path)}
    view.events_page = 0
    view.events_per_page = 10
    view.buildings_page = 0
    view.buildings_per_page = 10
    return view


class MemberManagerViewFilterTests(unittest.TestCase):
    def test_events_tab_only_shows_operations(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "logs_v3.db"
            create_logs_db(db_path)
            view = build_view(db_path)

            embed = asyncio.run(view.get_events_embed())

        self.assertIn("Large scale mission started", embed.description)
        self.assertNotIn("Building constructed", embed.description)

    def test_buildings_tab_only_shows_building_activity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "logs_v3.db"
            create_logs_db(db_path)
            view = build_view(db_path)

            embed = asyncio.run(view.get_buildings_embed())

        self.assertIn("Building constructed", embed.description)
        self.assertNotIn("Large scale mission started", embed.description)

    def test_simple_overview_is_default_and_toggle_switches_to_advanced(self):
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(
            discord_id=123,
            mc_user_id="456",
            discord_username="DiscordUser",
            mc_username="MCUser",
            link_status="approved",
            is_verified=True,
            notes_count=2,
            infractions_count=1,
            severity_score=3,
        )
        view.overview_mode = "simple"
        view._update_view = AsyncMock()

        simple = asyncio.run(view.get_overview_embed())
        self.assertEqual(simple.footer["text"], "Simple overview • Use Advanced Overview for full details")

        button = ToggleOverviewModeButton(view, row=2)
        interaction = object()
        asyncio.run(button.callback(interaction))

        self.assertEqual(view.overview_mode, "advanced")
        view._update_view.assert_awaited_once_with(interaction)


if __name__ == "__main__":
    unittest.main()
