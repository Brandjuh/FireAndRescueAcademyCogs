import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scrapertester.scraper_tester import SCRAPER_SPECS, ScraperTester


class FakeTask:
    def cancelled(self):
        return False

    def done(self):
        return False


class FakeScraperCog:
    def __init__(self, db_path):
        self.db_path = db_path
        self.scrape_task = FakeTask()


class FakeBot:
    def __init__(self, cogs):
        self._cogs = cogs

    def get_cog(self, name):
        return self._cogs.get(name)


def make_scrape_runs_db(path: Path, *, finished_at: str | None = None):
    finished_at = finished_at or datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE scrape_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT,
                started_at TEXT,
                finished_at TEXT,
                rows_parsed INTEGER,
                rows_inserted INTEGER,
                errors INTEGER,
                message TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO scrape_runs (
                status, started_at, finished_at, rows_parsed, rows_inserted, errors, message
            ) VALUES ('success', '2026-07-03T10:00:00+00:00', ?, 1, 1, 0, 'ok')
            """
            ,
            (finished_at,),
        )
        conn.commit()
    finally:
        conn.close()


def test_static_results_check_packaged_helpers_loaded_cogs_tasks_and_databases(tmp_path):
    cogs = {"CookieManager": object()}
    cogs_root = tmp_path / "cogs"
    cogs_root.mkdir()

    for spec in SCRAPER_SPECS:
        package_dir = cogs_root / spec.package_name
        package_dir.mkdir()
        (package_dir / "fara_db.py").write_text("# helper\n", encoding="utf-8")

        db_path = tmp_path / f"{spec.key}.db"
        make_scrape_runs_db(db_path)
        cogs[spec.cog_name] = FakeScraperCog(db_path)

    tester = ScraperTester(FakeBot(cogs))
    tester._cogs_root = cogs_root

    results = tester._collect_static_results()

    assert results
    assert all(result.ok for result in results)
    assert any(result.label == "Applications package helper" for result in results)
    assert any(result.label == "Logs database" and "latest=success" in result.detail for result in results)


def test_members_database_status_fails_when_latest_scrape_is_stale(tmp_path):
    cogs_root = tmp_path / "cogs"
    cogs_root.mkdir()
    package_dir = cogs_root / "membersscraper"
    package_dir.mkdir()
    (package_dir / "fara_db.py").write_text("# helper\n", encoding="utf-8")

    db_path = tmp_path / "members.db"
    stale_finished_at = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
    make_scrape_runs_db(db_path, finished_at=stale_finished_at)
    tester = ScraperTester(FakeBot({"MembersScraper": FakeScraperCog(db_path)}))
    tester._cogs_root = cogs_root

    results = tester._collect_static_results()
    members_database = next(result for result in results if result.label == "Members database")

    assert members_database.ok is False
    assert "stale_for=" in members_database.detail


def test_members_live_check_reads_only_page_one_without_full_scrape():
    class FakeMembersCog:
        full_scrape_called = False
        page_requested = None

        async def _get_session(self, ctx):
            assert ctx is None
            return object()

        async def _scrape_members_page(self, session, page, timestamp, ctx):
            assert session is not None
            assert timestamp
            assert ctx is None
            self.page_requested = page
            return [{"member_id": 1, "username": "Member"}]

        async def _scrape_all_members(self, ctx=None, custom_timestamp=None):
            self.full_scrape_called = True
            return True

    cog = FakeMembersCog()
    tester = ScraperTester(FakeBot({}))
    spec = next(item for item in SCRAPER_SPECS if item.key == "members")

    result = asyncio.run(tester._run_live_check(spec, cog, log_pages=1, expense_pages=1))

    assert result.ok is True
    assert cog.page_requested == 1
    assert cog.full_scrape_called is False
    assert "database not modified" in result.detail
