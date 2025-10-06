# alliance_scraper.py (v0.7.0) - members + logs
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
    # members
    "pages_per_minute": 10,
    "members_refresh_minutes": 60,
    "backfill_auto": True,
    "backfill_concurrency": 5,
    "backfill_retry": 3,
    "backfill_jitter_ms": 250,
    # logs
    "logs_refresh_minutes": 5,
    "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "members_path_template": "/verband/mitglieder/{alliance_id}?page={page}",
}

ID_REGEXPS = [
    re.compile(r"/users/(\d+)", re.I),
    re.compile(r"/profile/(\d+)", re.I),
]

LOG_ID_RX = [
    re.compile(r"/users/(\d+)", re.I),
    re.compile(r"/profile/(\d+)", re.I),
    re.compile(r"/buildings/(\d+)", re.I),
]

INT64_MAX = 9223372036854775807
INT64_MIN = -9223372036854775808

def now_utc() -> str:
    return datetime.utcnow().isoformat()

def parse_int64_from_text(txt: str) -> int:
    if not txt:
        return 0
    m = re.search(r"(-?\d[\d.,]*)", txt)
    if not m:
        return 0
    raw = m.group(1)
    neg = raw.strip().startswith("-")
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return 0
    try:
        val = int(digits)
    except Exception:
        return INT64_MIN if neg else INT64_MAX
    if neg:
        val = -val
    if val > INT64_MAX:
        return INT64_MAX
    if val < INT64_MIN:
        return INT64_MIN
    return val

def parse_percent(txt: str) -> float:
    if not txt or "%" not in txt:
        return 0.0
    m = re.search(r"(-?\d+(?:[.,]\d+)?)\s*%", txt)
    if not m:
        return 0.0
    try:
        return float(m.group(1).replace(",", "."))
    except Exception:
        return 0.0

def _extract_id_from_href(href: str) -> Optional[str]:
    if not href:
        return None
    for rx in ID_REGEXPS:
        m = rx.search(href)
        if m:
            return m.group(1)
    return None

def _extract_any_id(url: str) -> Optional[str]:
    if not url:
        return None
    for rx in LOG_ID_RX:
        m = rx.search(url)
        if m:
            return m.group(1)
    return None

def _hash_key(ts: str, exec_name: str, action_text: str, affected_name: str, desc: str) -> str:
    import hashlib
    raw = f"{ts}|{exec_name}|{action_text}|{affected_name}|{desc}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

ACTION_MAP = [
    ("added_to_alliance", [r"added to the alliance", r"added to alliance"]),
    ("application_denied", [r"application denied"]),
    ("left_alliance", [r"left the alliance", r"has left the alliance"]),
    ("kicked_from_alliance", [r"kicked from the alliance", r"removed from the alliance"]),
    ("set_transport_admin", [r"set as transport request admin"]),
    ("removed_transport_admin", [r"removed as transport request admin"]),
    ("removed_admin", [r"removed as admin"]),
    ("set_admin", [r"set as admin"]),
    ("removed_education_admin", [r"removed as education admin"]),
    ("set_education_admin", [r"set as education admin"]),
    ("set_finance_admin", [r"set as finance admin"]),
    ("removed_finance_admin", [r"removed as finance admin"]),
    ("set_co_admin", [r"set as co admin", r"set as co-admin"]),
    ("removed_co_admin", [r"removed as co admin", r"removed as co-admin"]),
    ("set_mod_action_admin", [r"set as moderator action admin"]),
    ("removed_mod_action_admin", [r"removed as moderator action admin"]),
    ("chat_ban_removed", [r"chat ban removed"]),
    ("chat_ban_set", [r"chat ban set"]),
    ("allowed_to_apply", [r"allowed to apply for the alliance"]),
    ("not_allowed_to_apply", [r"not allowed to apply for the alliance"]),
    ("created_course", [r"created a course"]),
    ("course_completed", [r"course completed"]),
    ("building_destroyed", [r"building destroyed"]),
    ("building_constructed", [r"building constructed"]),
    ("extension_started", [r"extension started"]),
    ("expansion_finished", [r"expansion finished"]),
    ("large_mission_started", [r"large mission started"]),
    ("alliance_event_started", [r"alliance event started"]),
    ("set_as_staff", [r"set as staff"]),
    ("removed_as_staff", [r"removed as staff"]),
    ("removed_event_manager", [r"removed as event manager"]),
    ("removed_custom_large_mission", [r"removed custom large scale mission"]),
    ("promoted_event_manager", [r"promoted to event manager"]),
]

def _norm_action(text: str) -> Tuple[str, str]:
    t = (text or "").strip().lower()
    for key, patterns in ACTION_MAP:
        for pat in patterns:
            if re.search(pat, t, re.I):
                return key, text
    key = re.sub(r"[^a-z0-9]+", "_", t)[:60].strip("_") or "unknown"
    return key, text

class AllianceScraper(commands.Cog):
    """Scrape alliance data with robust member ID extraction; store alliance logs; provide APIs for other cogs."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA11A9E55C0, force_registration=True)
        self.config.register_global(**DEFAULTS)
        self.data_path = cog_data_path(self)
        self.db_path = self.data_path / "alliance.db"
        self._bg_members: Optional[asyncio.Task] = None
        self._bg_logs: Optional[asyncio.Task] = None

    async def cog_load(self):
        await self._init_db()
        await self._maybe_start_background()

    # ---------------- DB init & migrations ----------------
    async def _init_db(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            # members tables
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
            # logs table
            await db.execute("""
            CREATE TABLE IF NOT EXISTS logs(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE,
                ts TEXT,
                action_key TEXT,
                action_text TEXT,
                executed_name TEXT,
                executed_mc_id TEXT,
                executed_url TEXT,
                affected_name TEXT,
                affected_type TEXT,
                affected_mc_id TEXT,
                affected_url TEXT,
                description TEXT,
                scraped_at TEXT
            )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(ts)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_logs_hash ON logs(hash)")
            await db.commit()
        await self._migrate_db()

    async def _migrate_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            # ensure member columns exist
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
            # logs columns as above by creation
            await db.commit()

    async def _ensure_columns(self, db: aiosqlite.Connection, table: str, cols: List[Tuple[str, str, str]]):
        cur = await db.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in await cur.fetchall()}
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

    # ---------------- Members parsing/saving ----------------
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
            user_id = _extract_id_from_href(href) or ""
            tds = tr.find_all("td")
            role = ""
            credits = 0
            rate = 0.0
            for td in tds:
                txt = td.get_text(" ", strip=True)
                if not role and txt and not any(ch.isdigit() for ch in txt) and name not in txt:
                    role = txt
                if credits == 0:
                    val = parse_int64_from_text(txt)
                    if val != 0:
                        credits = val
                if "%" in txt and rate == 0.0:
                    rate = parse_percent(txt)
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
                credits = r.get("earned_credits") or 0
                if credits > INT64_MAX:
                    credits = INT64_MAX
                if credits < INT64_MIN:
                    credits = INT64_MIN
                await db.execute("""
                INSERT INTO members_current(user_id, name, role, earned_credits, contribution_rate, profile_href, scraped_at)
                VALUES(?,?,?,?,?,?,?)
                """, (r.get("user_id") or "", r.get("name") or "", r.get("role") or "", int(credits), float(r.get("contribution_rate") or 0.0), r.get("profile_href") or "", now_utc()))
                await db.execute("""
                INSERT INTO members_history(user_id, name, role, earned_credits, contribution_rate, profile_href, scraped_at)
                VALUES(?,?,?,?,?,?,?)
                """, (r.get("user_id") or "", r.get("name") or "", r.get("role") or "", int(credits), float(r.get("contribution_rate") or 0.0), r.get("profile_href") or "", now_utc()))
            await db.commit()

    async def get_members(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT user_id, name, role, earned_credits, contribution_rate FROM members_current")
            return [dict(r) for r in await cur.fetchall()]

    async def _resolve_member_id(self, session: aiohttp.ClientSession, base: str, href: str) -> Optional[str]:
        if not href:
            return None
        uid = _extract_id_from_href(href)
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

    # ---------------- Logs parsing/saving ----------------
    def _parse_logs_page(self, html: str, base: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        results: List[Dict[str, Any]] = []
        for tr in soup.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            dt_text = tds[0].get_text(" ", strip=True)
            exec_a = tds[1].find("a", href=True)
            executed_name = exec_a.get_text(strip=True) if exec_a else tds[1].get_text(" ", strip=True)
            executed_url = (exec_a["href"] if exec_a else "")
            if executed_url and not executed_url.startswith("http"):
                executed_url = f"{base}{executed_url}"
            executed_mc_id = _extract_any_id(executed_url) or ""
            desc_text = tds[2].get_text(" ", strip=True)
            action_key, action_text = _norm_action(desc_text)
            affected_name, affected_url, affected_type, affected_mc_id = "", "", "", ""
            if len(tds) >= 4:
                aff_a = tds[3].find("a", href=True)
                affected_name = aff_a.get_text(strip=True) if aff_a else tds[3].get_text(" ", strip=True)
                affected_url = (aff_a["href"] if aff_a else "")
                if affected_url and not affected_url.startswith("http"):
                    affected_url = f"{base}{affected_url}"
                if "/buildings/" in affected_url:
                    affected_type = "building"
                elif "/users/" in affected_url or "/profile/" in affected_url:
                    affected_type = "user"
                affected_mc_id = _extract_any_id(affected_url) or ""
            h = _hash_key(dt_text, executed_name, action_text, affected_name, desc_text)
            results.append({
                "hash": h,
                "ts": dt_text,
                "action_key": action_key,
                "action_text": action_text,
                "executed_name": executed_name,
                "executed_mc_id": executed_mc_id,
                "executed_url": executed_url,
                "affected_name": affected_name,
                "affected_type": affected_type,
                "affected_mc_id": affected_mc_id,
                "affected_url": affected_url,
                "description": desc_text,
            })
        return results

    async def _insert_logs(self, rows: List[Dict[str, Any]]) -> int:
        inserted = 0
        async with aiosqlite.connect(self.db_path) as db:
            for row in rows:
                try:
                    await db.execute("""
                    INSERT INTO logs(hash, ts, action_key, action_text, executed_name, executed_mc_id, executed_url,
                                     affected_name, affected_type, affected_mc_id, affected_url, description, scraped_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (row["hash"], row["ts"], row["action_key"], row["action_text"],
                          row["executed_name"], row["executed_mc_id"], row["executed_url"],
                          row["affected_name"], row["affected_type"], row["affected_mc_id"], row["affected_url"],
                          row["description"], now_utc()))
                    inserted += 1
                except aiosqlite.IntegrityError:
                    # duplicate
                    continue
            await db.commit()
        return inserted

    async def _scrape_logs_once(self, backfill_pages: Optional[int] = None) -> int:
        base = await self.config.base_url()
        session, own = await self._get_auth_session()
        try:
            page = 1
            total_seen = 0
            while True:
                url = f"{base}/alliance_logfiles?page={page}"
                html, _ = await self._fetch(session, url)
                rows = self._parse_logs_page(html, base)
                if not rows:
                    break
                ins = await self._insert_logs(rows)
                total_seen += len(rows)
                if backfill_pages is not None and page >= backfill_pages:
                    break
                # stop early: if no new inserts this page, next pages likely old too
                if ins == 0:
                    break
                page += 1
                await asyncio.sleep(0.8)
            return total_seen
        finally:
            if own:
                try:
                    await session.close()
                except Exception:
                    pass

    # ---------------- Public API for consumers ----------------
    async def get_logs_after(self, last_id: int, limit: int = 500) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""
                SELECT id, ts, action_key, action_text, executed_name, executed_mc_id, executed_url,
                       affected_name, affected_type, affected_mc_id, affected_url, description
                FROM logs
                WHERE id > ?
                ORDER BY id ASC
                LIMIT ?
            """, (int(last_id), int(limit)))
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    # ---------------- Background ----------------
    async def _maybe_start_background(self):
        if self._bg_members is None:
            self._bg_members = asyncio.create_task(self._members_loop())
        if self._bg_logs is None:
            self._bg_logs = asyncio.create_task(self._logs_loop())

    async def _members_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                mins = int(await self.config.members_refresh_minutes())
                await self._scrape_members()
            except Exception as e:
                log.warning("Background members scrape error: %s", e)
            await asyncio.sleep(max(60, mins * 60))

    async def _logs_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                mins = int(await self.config.logs_refresh_minutes())
                await self._scrape_logs_once(backfill_pages=1)
            except Exception as e:
                log.warning("Background logs scrape error: %s", e)
            await asyncio.sleep(max(60, mins * 60))

    # ---------------- Commands ----------------
    @commands.group(name="scraper")
    @checks.is_owner()
    async def scraper_group(self, ctx: commands.Context):
        """AllianceScraper controls."""

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

    @scraper_group.group(name="logs")
    async def logs_group(self, ctx: commands.Context):
        """Alliance logs scraping controls."""

    @logs_group.command(name="run")
    async def logs_run(self, ctx: commands.Context, pages: Optional[int] = None):
        await ctx.send("Scraping alliance logs...")
        n = await self._scrape_logs_once(backfill_pages=pages)
        await ctx.send(f"Done. Parsed about {n} rows (including known).")

    @logs_group.command(name="status")
    async def logs_status(self, ctx: commands.Context):
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT COUNT(*), MAX(id) FROM logs")
            total, maxid = await cur.fetchone()
        mins = int(await self.config.logs_refresh_minutes())
        await ctx.send(f"```\nLogs total: {total or 0}\nNewest id: {maxid or 0}\nRefresh minutes: {mins}\n```")

    @logs_group.command(name="setinterval")
    async def logs_setinterval(self, ctx: commands.Context, minutes: int):
        await self.config.logs_refresh_minutes.set(max(1, int(minutes)))
        await ctx.send(f"Logs refresh interval set to {minutes} minute(s)")

async def setup(bot):
    cog = AllianceScraper(bot)
    await bot.add_cog(cog)
