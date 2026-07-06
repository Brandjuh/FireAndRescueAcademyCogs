from __future__ import annotations

import asyncio
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from redbot.core import commands
from redbot.core.utils.chat_formatting import box, pagify


@dataclass(frozen=True)
class ScraperSpec:
    key: str
    label: str
    cog_name: str
    package_name: str
    helper_file: str = "fara_db.py"


@dataclass
class CheckResult:
    label: str
    ok: bool
    detail: str
    elapsed_seconds: Optional[float] = None


SCRAPER_SPECS: tuple[ScraperSpec, ...] = (
    ScraperSpec("applications", "Applications", "ApplicationsScraper", "applicationscraper"),
    ScraperSpec("members", "Members", "MembersScraper", "membersscraper"),
    ScraperSpec("buildings", "Buildings", "BuildingsScraper", "buildingscraper"),
    ScraperSpec("income", "Income", "IncomeScraper", "incomescraper"),
    ScraperSpec("logs", "Logs", "LogsScraper", "logscraper"),
)

STALE_AFTER_SECONDS: dict[str, int] = {
    "members": 2 * 60 * 60,
}


class ScraperTester(commands.Cog):
    """Owner-only health checks for the MissionChief scraper cogs."""

    def __init__(self, bot):
        self.bot = bot
        self._cogs_root = Path(__file__).resolve().parents[1]

    @commands.group(name="scrapers")
    @commands.is_owner()
    async def scrapers_group(self, ctx):
        """Scraper health check commands."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @scrapers_group.command(name="status")
    async def scrapers_status(self, ctx):
        """Show scraper package, loaded-cog, task, and database status."""
        results = self._collect_static_results()
        await self._send_report(ctx, "Scraper Status", results)

    @scrapers_group.command(name="smoke")
    async def scrapers_smoke(self, ctx, mode: str = "live", log_pages: int = 1, expense_pages: int = 1):
        """
        Run a small scraper smoke test.

        Usage:
        [p]scrapers smoke
        [p]scrapers smoke status
        [p]scrapers smoke live 1 1
        """
        mode = (mode or "live").lower()
        if mode not in {"live", "status", "static", "dry"}:
            await ctx.send("Usage: `[p]scrapers smoke [live|status] [log_pages=1] [expense_pages=1]`")
            return

        log_pages = max(1, min(int(log_pages), 5))
        expense_pages = max(1, min(int(expense_pages), 5))

        status_message = await ctx.send("Starting scraper smoke test...")
        results = self._collect_static_results()

        if mode == "live":
            live_results = await self._collect_live_results(
                log_pages=log_pages,
                expense_pages=expense_pages,
                progress_message=status_message,
            )
            results.extend(live_results)

        await self._send_report(
            ctx,
            "Scraper Smoke Test" if mode == "live" else "Scraper Static Smoke Test",
            results,
        )

        try:
            await status_message.edit(content="Scraper smoke test complete.")
        except Exception:
            pass

    def _collect_static_results(self) -> list[CheckResult]:
        results: list[CheckResult] = []

        cookie_manager = self.bot.get_cog("CookieManager")
        results.append(
            CheckResult(
                "CookieManager loaded",
                cookie_manager is not None,
                "available" if cookie_manager is not None else "CookieManager is not loaded",
            )
        )

        for spec in SCRAPER_SPECS:
            helper_path = self._cogs_root / spec.package_name / spec.helper_file
            results.append(
                CheckResult(
                    f"{spec.label} package helper",
                    helper_path.is_file(),
                    str(helper_path.name) if helper_path.is_file() else f"missing {helper_path}",
                )
            )

            cog = self.bot.get_cog(spec.cog_name)
            results.append(
                CheckResult(
                    f"{spec.label} cog loaded",
                    cog is not None,
                    spec.cog_name if cog is not None else f"{spec.cog_name} is not loaded",
                )
            )

            if cog is not None:
                results.append(self._task_result(spec, cog))
                results.append(self._database_result(spec, cog))

        return results

    async def _collect_live_results(
        self,
        *,
        log_pages: int,
        expense_pages: int,
        progress_message,
    ) -> list[CheckResult]:
        results: list[CheckResult] = []
        cookie_manager = self.bot.get_cog("CookieManager")
        results.append(await self._time_async("CookieManager live session", self._check_cookie_session(cookie_manager)))

        for index, spec in enumerate(SCRAPER_SPECS, start=1):
            cog = self.bot.get_cog(spec.cog_name)
            if cog is None:
                results.append(CheckResult(f"{spec.label} live check", False, f"{spec.cog_name} is not loaded"))
                continue

            try:
                await progress_message.edit(content=f"Running scraper smoke test: {spec.label} ({index}/{len(SCRAPER_SPECS)})")
            except Exception:
                pass

            results.append(
                await self._time_async(
                    f"{spec.label} live check",
                    self._run_live_check(spec, cog, log_pages=log_pages, expense_pages=expense_pages),
                )
            )
            await asyncio.sleep(2)

        return results

    async def _time_async(self, label: str, awaitable) -> CheckResult:
        started = time.monotonic()
        try:
            result = await awaitable
            result.label = label
            result.elapsed_seconds = time.monotonic() - started
            return result
        except Exception as exc:
            return CheckResult(label, False, f"{type(exc).__name__}: {exc}", time.monotonic() - started)

    async def _check_cookie_session(self, cookie_manager) -> CheckResult:
        if cookie_manager is None:
            return CheckResult("CookieManager live session", False, "CookieManager is not loaded")
        if not hasattr(cookie_manager, "get_session"):
            return CheckResult("CookieManager live session", False, "CookieManager has no get_session() API")

        session = await cookie_manager.get_session()
        if session is None:
            return CheckResult("CookieManager live session", False, "get_session() returned None")

        check_url = "https://www.missionchief.com/buildings"
        config = getattr(cookie_manager, "config", None)
        if config is not None and hasattr(config, "check_url"):
            try:
                check_url = await config.check_url()
            except Exception:
                pass

        async with session.get(check_url) as response:
            await response.text()
            ok = response.status == 200
            return CheckResult(
                "CookieManager live session",
                ok,
                f"GET {check_url} returned HTTP {response.status}",
            )

    async def _run_live_check(
        self,
        spec: ScraperSpec,
        cog,
        *,
        log_pages: int,
        expense_pages: int,
    ) -> CheckResult:
        if spec.key == "applications":
            session = await cog._get_session()
            if session is None:
                return CheckResult(spec.label, False, "session unavailable")
            rows = await asyncio.wait_for(cog._scrape_applications(session), timeout=90)
            return CheckResult(spec.label, True, f"applications page reachable; parsed {len(rows)} application rows")

        if spec.key == "members":
            session = await cog._get_session(None)
            if session is None:
                return CheckResult(spec.label, False, "session unavailable")
            timestamp = datetime.now(timezone.utc).isoformat()
            rows = await asyncio.wait_for(cog._scrape_members_page(session, 1, timestamp, None), timeout=120)
            if rows is None:
                return CheckResult(spec.label, False, "member page 1 returned no parse result")
            return CheckResult(spec.label, len(rows) > 0, f"member page 1 parsed {len(rows)} rows; database not modified")

        if spec.key == "buildings":
            ok = await asyncio.wait_for(cog._scrape_all_buildings(ctx=None), timeout=120)
            return CheckResult(spec.label, bool(ok), "single buildings page scrape completed" if ok else "buildings scrape failed")

        if spec.key == "income":
            ok = await asyncio.wait_for(
                cog._scrape_all_income(ctx=None, include_expenses=True, max_expense_pages=expense_pages),
                timeout=180,
            )
            return CheckResult(
                spec.label,
                bool(ok),
                f"income scrape completed with {expense_pages} expense page(s)" if ok else "income scrape failed",
            )

        if spec.key == "logs":
            ok = await asyncio.wait_for(cog._scrape_all_logs(ctx=None, max_pages=log_pages), timeout=180)
            return CheckResult(
                spec.label,
                bool(ok),
                f"logs scrape completed with {log_pages} page(s)" if ok else "logs scrape failed",
            )

        return CheckResult(spec.label, False, f"no live check implemented for {spec.key}")

    def _task_result(self, spec: ScraperSpec, cog) -> CheckResult:
        task = getattr(cog, "scrape_task", None) or getattr(cog, "scraping_task", None)
        if task is None:
            return CheckResult(f"{spec.label} background task", False, "task attribute not found")
        try:
            if task.cancelled():
                return CheckResult(f"{spec.label} background task", False, "cancelled")
            if task.done():
                exception = task.exception()
                detail = f"done with exception: {exception}" if exception else "done"
                return CheckResult(f"{spec.label} background task", False, detail)
            return CheckResult(f"{spec.label} background task", True, "running")
        except Exception as exc:
            return CheckResult(f"{spec.label} background task", False, f"could not inspect task: {exc}")

    def _database_result(self, spec: ScraperSpec, cog) -> CheckResult:
        db_path = getattr(cog, "db_path", None)
        if not db_path:
            return CheckResult(f"{spec.label} database", False, "db_path attribute not found")

        path = Path(str(db_path))
        if not path.exists():
            return CheckResult(f"{spec.label} database", False, f"{path.name} does not exist")

        try:
            conn = sqlite3.connect(path)
            try:
                row = conn.execute(
                    """
                    SELECT status, started_at, finished_at, rows_parsed, rows_inserted, errors, message
                    FROM scrape_runs
                    ORDER BY run_id DESC
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()
        except sqlite3.Error as exc:
            return CheckResult(f"{spec.label} database", False, f"{path.name}: {exc}")

        size_kb = path.stat().st_size / 1024
        if row is None:
            return CheckResult(f"{spec.label} database", True, f"{path.name}, {size_kb:.1f} KB, no scrape_runs yet")

        status, started_at, finished_at, rows_parsed, rows_inserted, errors, message = row
        ok = status != "failed"
        detail = (
            f"{path.name}, {size_kb:.1f} KB, latest={status}, "
            f"parsed={rows_parsed}, inserted={rows_inserted}, errors={errors}, "
            f"finished={finished_at or started_at}"
        )
        stale_after = STALE_AFTER_SECONDS.get(spec.key)
        if stale_after:
            age_seconds = self._age_seconds(finished_at or started_at)
            if age_seconds is None:
                ok = False
                detail = f"{detail}, age=unknown"
            elif age_seconds > stale_after:
                ok = False
                detail = f"{detail}, stale_for={self._format_age(age_seconds)}"
        if message:
            detail = f"{detail}, message={message}"
        return CheckResult(f"{spec.label} database", ok, detail)

    def _age_seconds(self, value: Optional[str]) -> Optional[float]:
        if not value:
            return None
        timestamp = value.strip()
        if timestamp.endswith("Z"):
            timestamp = f"{timestamp[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(timestamp)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        parsed = parsed.astimezone(timezone.utc)
        return (datetime.now(timezone.utc) - parsed).total_seconds()

    def _format_age(self, seconds: float) -> str:
        seconds = max(0, int(seconds))
        days, remainder = divmod(seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, _ = divmod(remainder, 60)
        if days:
            return f"{days}d {hours}h"
        if hours:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"

    async def _send_report(self, ctx, title: str, results: list[CheckResult]) -> None:
        ok_count = sum(1 for result in results if result.ok)
        failed = len(results) - ok_count
        lines = [
            title,
            f"Result: {ok_count}/{len(results)} checks passed, {failed} failed",
            "",
        ]

        for result in results:
            status = "OK" if result.ok else "FAIL"
            elapsed = f" ({result.elapsed_seconds:.1f}s)" if result.elapsed_seconds is not None else ""
            lines.append(f"[{status}] {result.label}{elapsed}: {result.detail}")

        text = "\n".join(lines)
        for page in pagify(text, page_length=1900):
            await ctx.send(box(page, lang="text"))
