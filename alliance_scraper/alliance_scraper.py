# alliance_scraper.py (v0.6.2) - DB migrations + robust ID backfill
from __future__ import annotations

import asyncio
import aiosqlite
import aiohttp
import re
import random
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from bs4 import BeautifulSoup

from redbot.core import commands, checks, Config
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.FARA.AllianceScraper")

DEFAULTS = {
    "base_url": "https://www.missionchief.com",
    "alliance_id": 1621,
    "pages_per_minute": 10,
    "members_refresh_minutes": 60,
    "backfill_auto": True,
    "backfill_concurrency": 5,
    "backfill_retry": 3,
    "backfill_jitter_ms": 250,
    "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "members_path_template": "/verband/mitglieder/{alliance_id}?page={page}",
}

ID_REGEXPS = [
    re.compile(r"/users/(\d+)", re.I),
    re.compile(r"/profile/(\d+)", re.I),
]

def now_utc() -> str:
    return datetime.utcnow().isoformat()

class AllianceScraper(commands.Cog):
    """Scrape alliance data with robust member ID extraction, backfill and safe DB migrations."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA11A9E55C0, force_registration=True)
        self.config.register_global(**DEFAULTS)
        self.data_path = cog_data_path(self)
        self.db_path = self.data_path / "alliance.db"
        self._bg_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        await self._init_db()
        await self._maybe_start_background()

    # ---------------- DB init & migrations ----------------
    async def _init_db(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
            CREATE TABLE IF NOT EXISTS members_current(
                user_id TEXT,
                name TEXT,
                role TEXT,
                earned_credits INTEGER,
                contribution_rate REAL,
                profile_href TEXT,
                scraped_at TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS members_history(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                name TEXT,
                role TEXT,
                earned_credits INTEGER,
                contribution_rate REAL,
                profile_href TEXT,
                scraped_at TEXT
            )
            """)
            await db.commit()
        await self._migrate_db()

    async def _migrate_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await self._ensure_columns(db, "members_current", [
                ("user_id", "TEXT", "''"),
                ("name", "TEXT", "''"),
                ("role", "TEXT", "''"),
                ("earned_credits", "INTEGER", "0"),
                ("contribution_rate", "REAL", "0.0"),
                ("profile_href", "TEXT", "''"),
                ("scraped_at", "TEXT", "''"),
            ])
            await self._ensure_columns(db, "members_history", [
                ("user_id", "TEXT", "''"),
                ("name", "TEXT", "''"),
                ("role", "TEXT", "''"),
                ("earned_credits", "INTEGER", "0"),
                ("contribution_rate", "REAL", "0.0"),
                ("profile_href", "TEXT", "''"),
                ("scraped_at", "TEXT", "''"),
            ])
            await db.commit()

    async def _ensure_columns(self, db: aiosqlite.Connection, table: str, cols: List[Tuple[str, str, str]]):
        cur = await db.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in await cur.fetchall()}  # row[1] = name
        for name, coltype, default in cols:
            if name not in existing:
                try:
                    await db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {coltype} DEFAULT {default}")
                except Exception as e:
                    log.warning("Failed to add column %s to %s: %s", name, table, e)

    # ---------------- Session helpers ----------------
    async def _get_auth_session(self) -> Tuple[aiohttp.ClientSession, bool]:
        headers = {"User-Agent": (await self.config.user_agent())}
        timeout = aiohttp.ClientTimeout(total=30)
        cm = self.bot.get_cog("CookieManager")
        if cm:
            for attr in ("get_session", "get_aiohttp_session", "session"):
                try:
                    maybe = getattr(cm, attr, None)
                    if maybe:
                        sess = await maybe() if asyncio.iscoroutinefunction(maybe) else maybe()
                        try:
                            sess.headers.update(headers)
                        except Exception:
                            pass
                        return sess, False
                except Exception:
                    continue
        sess = aiohttp.ClientSession(headers=headers, timeout=timeout)
        return sess, True

    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> Tuple[str, str]:
        async with session.get(url, allow_redirects=True) as resp:
            text = await resp.text()
            return text, str(resp.url)

    # ---------------- Parsing helpers ----------------
    def _extract_id_from_href(self, href: str) -> Optional[str]:
        if not href:
            return None
        for rx in ID_REGEXPS:
            m = rx.search(href)
            if m:
                return m.group(1)
        return None

    def _extract_member_rows(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        rows: List[Dict[str, Any]] = []
        for tr in soup.find_all("tr"):
            a = tr.find("a", href=True)
            if not a:
                continue
            name = a.get_text(strip=True)
            if not name:
                continue
            href = a["href"]
            user_id = self._extract_id_from_href(href) or ""
            tds = tr.find_all("td")
            role = ""
            credits = 0
            rate = 0.0
            for td in tds:
                txt = td.get_text(" ", strip=True)
                if not role and txt and not any(ch.isdigit() for ch in txt) and name not in txt:
                    role = txt
                if credits == 0:
                    m = re.search(r"(\d[\d.,]*)", txt)
                    if m:
                        try:
                            credits = int(re.sub(r"[^\d]", "", m.group(1)))
                        except Exception:
                            pass
                if "%" in txt and rate == 0.0:
                    m2 = re.search(r"(\d+(?:[.,]\d+)?)\s*%", txt)
                    if m2:
                        try:
                            rate = float(m2.group(1).replace(",", "."))
                        except Exception:
                            pass
            rows.append({
                "user_id": user_id,
                "name": name,
                "role": role,
                "earned_credits": credits,
                "contribution_rate": rate,
                "profile_href": href,
            })
        return rows

    async def _save_members(self, rows: List[Dict[str, Any]]):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM members_current")
            for r in rows:
                await db.execute("""
                INSERT INTO members_current(user_id, name, role, earned_credits, contribution_rate, profile_href, scraped_at)
                VALUES(?,?,?,?,?,?,?)
                """, (r.get("user_id") or "", r.get("name") or "", r.get("role") or "", int(r.get("earned_credits") or 0), float(r.get("contribution_rate") or 0.0), r.get("profile_href") or "", now_utc()))
                await db.execute("""
                INSERT INTO members_history(user_id, name, role, earned_credits, contribution_rate, profile_href, scraped_at)
                VALUES(?,?,?,?,?,?,?)
                """, (r.get("user_id") or "", r.get("name") or "", r.get("role") or "", int(r.get("earned_credits") or 0), float(r.get("contribution_rate") or 0.0), r.get("profile_href") or "", now_utc()))
            await db.commit()

    # ---------------- Public API ----------------
    async def get_members(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT user_id, name, role, earned_credits, contribution_rate FROM members_current")
            return [dict(r) for r in await cur.fetchall()]

    # ---------------- Backfill logic ----------------
    async def _resolve_member_id(self, session: aiohttp.ClientSession, base: str, href: str) -> Optional[str]:
        if not href:
            return None
        uid = self._extract_id_from_href(href)
        if uid:
            return uid
        url = href if href.startswith("http") else f"{base}{href}"
        try:
            async with session.get(url, allow_redirects=True) as resp:
                final_url = str(resp.url)
                for rx in ID_REGEXPS:
                    m = rx.search(final_url)
                    if m:
                        return m.group(1)
                text = await resp.text()
        except Exception:
            return None
        for rx in ID_REGEXPS:
            m = rx.search(text)
            if m:
                return m.group(1)
        return None

    async def _backfill_missing_ids(self, session: aiohttp.ClientSession, base: str, limit: Optional[int] = None) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            sql = "SELECT rowid, name, profile_href FROM members_current WHERE (user_id IS NULL OR user_id='')"
            if limit:
                sql += f" LIMIT {int(limit)}"
            cur = await db.execute(sql)
            rows = await cur.fetchall()
        if not rows:
            return 0
        sem = asyncio.Semaphore(int(await self.config.backfill_concurrency()))
        retry = int(await self.config.backfill_retry())
        jitter = int(await self.config.backfill_jitter_ms())

        updated = 0

        async def worker(row):
            nonlocal updated
            async with sem:
                href = row["profile_href"] or ""
                if not href:
                    return
                uid = None
                for attempt in range(1, retry + 1):
                    await asyncio.sleep(random.uniform(0, jitter / 1000.0))
                    try:
                        uid = await self._resolve_member_id(session, base, href)
                        if uid:
                            break
                    except Exception:
                        uid = None
                if uid:
                    async with aiosqlite.connect(self.db_path) as db2:
                        await db2.execute("UPDATE members_current SET user_id=? WHERE rowid=?", (uid, row["rowid"]))
                        await db2.commit()
                    updated += 1

        await asyncio.gather(*(worker(r) for r in rows))
        return updated

    # ---------------- Scrape members ----------------
    async def _scrape_members(self) -> int:
        base = await self.config.base_url()
        alliance_id = int(await self.config.alliance_id())
        tpl = await self.config.members_path_template()
        pages_per_minute = int(await self.config.pages_per_minute())
        delay = max(0.0, 60.0 / max(1, pages_per_minute))

        session, own = await self._get_auth_session()
        try:
            page1_url = f"{base}{tpl.format(alliance_id=alliance_id, page=1)}"
            html1, _ = await self._fetch(session, page1_url)
            soup1 = BeautifulSoup(html1, "html.parser")
            last_page = 1
            for a in soup1.find_all("a", href=True):
                if "page=" in a["href"]:
                    try:
                        p = int(re.search(r"page=(\d+)", a["href"]).group(1))
                        if p > last_page:
                            last_page = p
                    except Exception:
                        pass

            all_rows: List[Dict[str, Any]] = []
            for p in range(1, last_page + 1):
                url = f"{base}{tpl.format(alliance_id=alliance_id, page=p)}"
                try:
                    html, _ = await self._fetch(session, url)
                    all_rows.extend(self._extract_member_rows(html))
                except Exception as e:
                    log.warning("Failed to fetch members page %s: %s", p, e)
                await asyncio.sleep(delay)

            await self._save_members(all_rows)
            if await self.config.backfill_auto():
                updated = await self._backfill_missing_ids(session, base)
                log.info("Backfill updated %s member IDs", updated)
            return len(all_rows)
        finally:
            if own:
                try:
                    await session.close()
                except Exception:
                    pass

    # ---------------- Background ----------------
    async def _maybe_start_background(self):
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._bg_loop())

    async def _bg_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                mins = int(await self.config.members_refresh_minutes())
                await self._scrape_members()
            except Exception as e:
                log.warning("Background scrape error: %s", e)
            await asyncio.sleep(max(60, mins * 60))

    # ---------------- Commands ----------------
    @commands.group(name="scraper")
    @checks.is_owner()
    async def scraper_group(self, ctx: commands.Context):
        """AllianceScraper controls."""

    @scraper_group.command(name="status")
    async def status(self, ctx: commands.Context):
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT COUNT(*), SUM(user_id!='') FROM members_current")
            total, with_id = await cur.fetchone()
        cfg = await self.config.all()
        msg = (
            f"Members total: {total or 0}\n"
            f"Members with ID: {with_id or 0}\n"
            f"Backfill auto: {cfg['backfill_auto']} (concurrency={cfg['backfill_concurrency']})\n"
            f"Refresh minutes: {cfg['members_refresh_minutes']}\n"
            f"Pages/minute: {cfg['pages_per_minute']}\n"
        )
        await ctx.send(f"```\n{msg}```")

    @scraper_group.group(name="members")
    async def members_group(self, ctx: commands.Context):
        """Members scraping controls."""

    @members_group.command(name="full")
    async def members_full(self, ctx: commands.Context):
        await ctx.send("Scraping all roster pages, then backfilling missing IDs...")
        n = await self._scrape_members()
        await ctx.send(f"Done. Parsed {n} rows.")

    @members_group.group(name="backfill")
    async def backfill_group(self, ctx: commands.Context):
        """Manage missing-ID backfill."""

    @backfill_group.command(name="now")
    async def backfill_now(self, ctx: commands.Context, limit: Optional[int] = None):
        base = await self.config.base_url()
        session, own = await self._get_auth_session()
        try:
            updated = await self._backfill_missing_ids(session, base, limit=limit)
        finally:
            if own:
                try:
                    await session.close()
                except Exception:
                    pass
        await ctx.send(f"Backfill updated {updated} members.")
    
    @backfill_group.command(name="auto")
    async def backfill_auto(self, ctx: commands.Context, value: bool):
        await self.config.backfill_auto.set(bool(value))
        await ctx.send(f"Backfill auto set to {bool(value)}")

    @backfill_group.command(name="status")
    async def backfill_status(self, ctx: commands.Context):
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM members_current WHERE user_id IS NULL OR user_id=''")
            missing = (await cur.fetchone())[0]
        await ctx.send(f"Members missing ID: {missing}")

    @scraper_group.group(name="db")
    async def db_group(self, ctx: commands.Context):
        """Database utilities for AllianceScraper."""

    @db_group.command(name="migrate")
    async def db_migrate(self, ctx: commands.Context):
        await self._migrate_db()
        await ctx.send("Database migration completed.")

    @db_group.command(name="schema")
    async def db_schema(self, ctx: commands.Context):
        async with aiosqlite.connect(self.db_path) as db:
            out = []
            for table in ("members_current", "members_history"):
                cur = await db.execute(f"PRAGMA table_info({table})")
                cols = await cur.fetchall()
                out.append(f"{table}: " + ", ".join([c[1] for c in cols]))
        await ctx.send("```\n" + "\n".join(out) + "\n```")

async def setup(bot):
    cog = AllianceScraper(bot)
    await bot.add_cog(cog)
