import asyncio
import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from alliance_logs_pub.alliance_logs_pub import AllianceLogsPub


class AllianceLogsPubStatusTests(unittest.TestCase):
    def test_status_reports_pending_action_breakdown(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            scraper_db = Path(temp_dir) / "logs.db"
            connection = sqlite3.connect(scraper_db)
            connection.execute(
                """
                CREATE TABLE logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action_key TEXT
                )
                """
            )
            connection.executemany(
                "INSERT INTO logs (action_key) VALUES (?)",
                [
                    ("expansion_finished",),
                    ("expansion_finished",),
                    ("kicked_from_alliance",),
                ],
            )
            connection.commit()
            connection.close()

            publisher = AllianceLogsPub.__new__(AllianceLogsPub)
            publisher._get_last_id = AsyncMock(return_value=1)
            logs_scraper = types.SimpleNamespace(
                db_path=scraper_db,
                get_logs_after=AsyncMock(),
            )
            publisher.bot = types.SimpleNamespace(
                get_cog=lambda name: logs_scraper if name == "LogsScraper" else None
            )
            publisher.config = types.SimpleNamespace(
                all=AsyncMock(
                    return_value={
                        "max_posts_per_run": 50,
                        "interval_minutes": 5,
                        "main_channel_id": 123,
                    }
                )
            )
            ctx = types.SimpleNamespace(send=AsyncMock())

            asyncio.run(publisher.status(ctx))

        message = ctx.send.await_args.args[0]
        self.assertIn("Pending logs: 2", message)
        self.assertIn("Pending by action:", message)
        self.assertIn("- expansion_finished: 1", message)
        self.assertIn("- kicked_from_alliance: 1", message)


if __name__ == "__main__":
    unittest.main()
