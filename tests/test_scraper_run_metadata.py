import sqlite3
import tempfile
import unittest
from pathlib import Path

from applicationscraper.applications_scraper import ApplicationsScraper
from buildingscraper.buildings_scraper import BuildingsScraper
from incomescraper.income_scraper import IncomeScraper
from logscraper.logs_scraper import LogsScraper
from membersscraper.members_scraper import MembersScraper


class ScraperRunMetadataTests(unittest.TestCase):
    def _table_names(self, db_path):
        connection = sqlite3.connect(db_path)
        try:
            return {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
        finally:
            connection.close()

    def test_core_scrapers_create_scrape_runs_table(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)

            members = MembersScraper.__new__(MembersScraper)
            members.db_path = str(directory / "members.db")
            members.membersync_db = str(directory / "membersync.db")
            members._init_database()

            buildings = BuildingsScraper.__new__(BuildingsScraper)
            buildings.db_path = directory / "buildings.db"
            buildings._init_database()

            applications = ApplicationsScraper.__new__(ApplicationsScraper)
            applications.db_path = str(directory / "applications.db")
            applications._init_database()

            logs = LogsScraper.__new__(LogsScraper)
            logs.db_path = directory / "logs.db"
            logs._init_database()

            income = IncomeScraper.__new__(IncomeScraper)
            income.db_path = directory / "income.db"
            income._init_database()

            for db_name in (
                "members.db",
                "buildings.db",
                "applications.db",
                "logs.db",
                "income.db",
            ):
                self.assertIn("scrape_runs", self._table_names(directory / db_name))

    def test_members_current_view_uses_latest_successful_live_scrape(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            scraper = MembersScraper.__new__(MembersScraper)
            scraper.db_path = str(directory / "members.db")
            scraper.membersync_db = str(directory / "membersync.db")
            scraper._init_database()

            connection = sqlite3.connect(scraper.db_path)
            try:
                connection.executemany(
                    """
                    INSERT INTO members (
                        member_id, username, rank, earned_credits, contribution_rate,
                        online_status, timestamp, snapshot_source
                    ) VALUES (?, ?, 'Member', 1000, 5.0, 'offline', ?, 'live')
                    """,
                    [
                        (1, "Good Snapshot", "2026-06-11T12:00:00"),
                        (2, "Failed Newer Snapshot", "2026-06-11T13:00:00"),
                    ],
                )
                connection.executemany(
                    """
                    INSERT INTO scrape_runs (
                        scraper, source, source_timestamp, started_at, finished_at, status
                    ) VALUES ('members', 'live', ?, ?, ?, ?)
                    """,
                    [
                        (
                            "2026-06-11T12:00:00",
                            "2026-06-11T12:00:01+00:00",
                            "2026-06-11T12:00:02+00:00",
                            "success",
                        ),
                        (
                            "2026-06-11T13:00:00",
                            "2026-06-11T13:00:01+00:00",
                            "2026-06-11T13:00:02+00:00",
                            "failed",
                        ),
                    ],
                )
                connection.commit()

                current_members = connection.execute(
                    "SELECT mc_user_id, name, scraped_at FROM members_current"
                ).fetchall()
            finally:
                connection.close()

        self.assertEqual(current_members, [(1, "Good Snapshot", "2026-06-11T12:00:00")])

    def test_members_current_view_falls_back_for_existing_databases_without_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            scraper = MembersScraper.__new__(MembersScraper)
            scraper.db_path = str(directory / "members.db")
            scraper.membersync_db = str(directory / "membersync.db")
            scraper._init_database()

            connection = sqlite3.connect(scraper.db_path)
            try:
                connection.executemany(
                    """
                    INSERT INTO members (
                        member_id, username, rank, earned_credits, contribution_rate,
                        online_status, timestamp, snapshot_source
                    ) VALUES (?, ?, 'Member', 1000, 5.0, 'offline', ?, 'live')
                    """,
                    [
                        (1, "Older", "2026-06-11T12:00:00"),
                        (2, "Newest", "2026-06-11T13:00:00"),
                    ],
                )
                connection.commit()
                current_members = connection.execute(
                    "SELECT mc_user_id, name, scraped_at FROM members_current"
                ).fetchall()
            finally:
                connection.close()

        self.assertEqual(current_members, [(2, "Newest", "2026-06-11T13:00:00")])


if __name__ == "__main__":
    unittest.main()
