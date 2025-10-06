# alliance_scraper.py (v0.9.0) - added treasury scraping for /verband/kasse
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
import discord

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
    # treasury
    "treasury_refresh_minutes": 15,
    "treasury_initial_backfill": True,
    "treasury_expenses_per_minute": 20,
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

def _hash_expense(date: str, credits: str, name: str, desc: str) -> str:
    import hashlib
    raw = f"{date}|{credits}|{name}|{desc}"
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
    ("contributed_to_alliance", [r"contributed .* coins to the alliance"]),
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
        self._bg_treasury: Optional[asyncio.Task] = None

    async def cog_load(self):
        await self._init_db()
        await self._maybe_start_background()

    async def cog_unload(self):
        """Cancel background tasks on unload."""
        if self._bg_members:
            self._bg_members.cancel()
        if self._bg_logs:
            self._bg_logs.cancel()
        if self._bg_treasury:
            self._bg_treasury.cancel()

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
            # logs table with contribution_amount
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
                contribution_amount INTEGER DEFAULT 0,
                scraped_at TEXT
            )
            """)
            
            # treasury tables
            await db.execute("""
            CREATE TABLE IF NOT EXISTS treasury_income(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period TEXT,
                user_name TEXT,
                user_id TEXT,
                credits INTEGER,
                scraped_at TEXT
            )
            """)
            
            await db.execute("""
            CREATE TABLE IF NOT EXISTS treasury_balance(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_funds INTEGER,
                scraped_at TEXT
            )
            """)
            
            await db.execute("""
            CREATE TABLE IF NOT EXISTS treasury_expenses(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE,
                expense_date TEXT,
                credits INTEGER,
                name TEXT,
                description TEXT,
                scraped_at TEXT
            )
            """)
            
            # Indices for better performance
            await db.execute("CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(ts)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_logs_hash ON logs(hash)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_logs_action_key ON logs(action_key)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_logs_executed_mc_id ON logs(executed_mc_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_members_history_scraped ON members_history(scraped_at)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_members_history_user_id ON members_history(user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_treasury_income_period ON treasury_income(period)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_treasury_income_user_id ON treasury_income(user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_treasury_expenses_hash ON treasury_expenses(hash)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_treasury_expenses_date ON treasury_expenses(expense_date)")
            
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
            # ensure contribution_amount exists in logs
            await self._ensure_columns(db, "logs", [
                ("contribution_amount", "INTEGER", "0"),
            ])
            await db.commit()

    async def _ensure_columns(self, db: aiosqlite.Connection, table: str, cols: List[Tuple[str, str, str]]):
        cur = await db.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in await cur.fetchall()}
        for name, coltype, default in cols:
            if name not in existing:
                try:
                    await db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {coltype} DEFAULT {default}")
                    log.info("Added column %s to %s", name, table)
                except Exception as e:
                    log.warning("Failed to add column %s to %s: %s", name, table, e)

    # ---------------- Session helpers ----------------
    async def _get_auth_session(self) -> Tuple[aiohttp.ClientSession, bool]:
        """Get authenticated session. Returns (session, should_close_after_use)."""
        headers = {"User-Agent": (await self.config.user_agent())}
        timeout = aiohttp.ClientTimeout(total=30)
        cm = self.bot.get_cog("CookieManager")
        if cm:
            try:
                sess = await cm.get_session()
                try:
                    sess.headers.update(headers)
                except Exception:
                    pass
                return sess, False
            except Exception as e:
                log.warning("Failed to get CookieManager session: %s", e)
        
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
            failed_requests = 0
            
            for p in range(1, last_page + 1):
                url = f"{base}{tpl.format(alliance_id=alliance_id, page=p)}"
                try:
                    html, _ = await self._fetch(session, url)
                    all_rows.extend(self._extract_member_rows(html))
                    failed_requests = 0
                except Exception as e:
                    failed_requests += 1
                    log.warning("Failed to fetch members page %s (failure #%d): %s", p, failed_requests, e)
                    if failed_requests >= 3:
                        log.error("Too many consecutive failures, stopping scrape")
                        break
                
                actual_delay = delay * (1.5 ** failed_requests) if failed_requests > 0 else delay
                await asyncio.sleep(actual_delay)

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
            
            contribution_amount = 0
            if "contributed" in desc_text.lower() and "coins" in desc_text.lower():
                match = re.search(r'contributed\s+([\d,.]+)\s+coins', desc_text, re.I)
                if match:
                    contribution_amount = parse_int64_from_text(match.group(1))
            
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
                "contribution_amount": contribution_amount,
            })
        return results

    async def _insert_logs(self, rows: List[Dict[str, Any]]) -> int:
        inserted = 0
        async with aiosqlite.connect(self.db_path) as db:
            for row in rows:
                try:
                    await db.execute("""
                    INSERT INTO logs(hash, ts, action_key, action_text, executed_name, executed_mc_id, executed_url,
                                     affected_name, affected_type, affected_mc_id, affected_url, description, 
                                     contribution_amount, scraped_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (row["hash"], row["ts"], row["action_key"], row["action_text"],
                          row["executed_name"], row["executed_mc_id"], row["executed_url"],
                          row["affected_name"], row["affected_type"], row["affected_mc_id"], row["affected_url"],
                          row["description"], row["contribution_amount"], now_utc()))
                    inserted += 1
                except aiosqlite.IntegrityError:
                    continue
            await db.commit()
        return inserted

    async def _scrape_logs_once(self, backfill_pages: Optional[int] = None) -> int:
        base = await self.config.base_url()
        session, own = await self._get_auth_session()
        try:
            page = 1
            total_seen = 0
            failed_requests = 0
            
            while True:
                url = f"{base}/alliance_logfiles?page={page}"
                try:
                    html, _ = await self._fetch(session, url)
                    rows = self._parse_logs_page(html, base)
                    if not rows:
                        break
                    ins = await self._insert_logs(rows)
                    total_seen += len(rows)
                    failed_requests = 0
                    
                    if backfill_pages is not None and page >= backfill_pages:
                        break
                    if ins == 0:
                        break
                    page += 1
                except Exception as e:
                    failed_requests += 1
                    log.warning("Failed to fetch logs page %s (failure #%d): %s", page, failed_requests, e)
                    if failed_requests >= 3:
                        log.error("Too many consecutive failures, stopping logs scrape")
                        break
                
                actual_delay = 0.8 * (1.5 ** failed_requests) if failed_requests > 0 else 0.8
                await asyncio.sleep(actual_delay)
                
            return total_seen
        finally:
            if own:
                try:
                    await session.close()
                except Exception:
                    pass

    # ---------------- Treasury parsing/saving ----------------
    def _parse_treasury_page(self, html: str) -> Tuple[int, List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Parse /verband/kasse page. Returns (total_funds, income_rows, expense_rows)"""
        soup = BeautifulSoup(html, "html.parser")
        
        # Extract total funds
        total_funds = 0
        for strong in soup.find_all("strong"):
            text = strong.get_text(strip=True)
            if "coins" in text.lower() or "$" in text:
                total_funds = parse_int64_from_text(text)
                break
        
        # Parse income table (contributions)
        income_rows = []
        tables = soup.find_all("table")
        
        for table in tables:
            # Check if this is the income table by looking for headers
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if not any("name" in h or "credits" in h for h in headers):
                continue
            
            # Determine period from table context (look for "Daily" or "Monthly" nearby)
            period = "unknown"
            prev_sibling = table.find_previous_sibling()
            if prev_sibling:
                text = prev_sibling.get_text(strip=True).lower()
                if "daily" in text or "today" in text:
                    period = "daily"
                elif "monthly" in text or "month" in text:
                    period = "monthly"
            
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 2:
                    continue
                
                # Get name and link
                a = tds[0].find("a", href=True)
                if not a:
                    continue
                    
                name = a.get_text(strip=True)
                href = a["href"]
                user_id = _extract_id_from_href(href) or ""
                
                # Get credits (second column)
                credits = 0
                if len(tds) >= 2:
                    credits = parse_int64_from_text(tds[1].get_text(strip=True))
                
                # Skip if contribution rate < 10% (check if there's a percentage column)
                skip = False
                for td in tds:
                    text = td.get_text(strip=True)
                    if "%" in text:
                        rate = parse_percent(text)
                        if 0 < rate <= 10.0:
                            skip = True
                            break
                
                if not skip and credits > 0:
                    income_rows.append({
                        "period": period,
                        "user_name": name,
                        "user_id": user_id,
                        "credits": credits,
                    })
        
        return total_funds, income_rows, []
    
    def _parse_treasury_expenses_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse /verband/kasse expenses page"""
        soup = BeautifulSoup(html, "html.parser")
        expenses = []
        
        # Find expense table
        for table in soup.find_all("table"):
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) < 3:
                    continue
                
                # Column 0: Date
                date_text = tds[0].get_text(" ", strip=True)
                
                # Column 1: Credits (negative values)
                credits = parse_int64_from_text(tds[1].get_text(strip=True))
                
                # Column 2: Name
                name = tds[2].get_text(" ", strip=True)
                
                # Column 3: Description (if exists)
                description = ""
                if len(tds) >= 4:
                    description = tds[3].get_text(" ", strip=True)
                
                if date_text and credits != 0:
                    h = _hash_expense(date_text, str(credits), name, description)
                    expenses.append({
                        "hash": h,
                        "expense_date": date_text,
                        "credits": abs(credits),  # Store as positive
                        "name": name,
                        "description": description,
                    })
        
        return expenses

    async def _save_treasury_balance(self, total_funds: int):
        """Save current alliance balance"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
            INSERT INTO treasury_balance(total_funds, scraped_at)
            VALUES(?, ?)
            """, (total_funds, now_utc()))
            await db.commit()

    async def _save_treasury_income(self, rows: List[Dict[str, Any]]):
        """Save income contributions (replaces old data for same period)"""
        if not rows:
            return
        
        async with aiosqlite.connect(self.db_path) as db:
            # Get unique periods
            periods = set(r["period"] for r in rows)
            
            # Delete old entries for these periods
            for period in periods:
                await db.execute("DELETE FROM treasury_income WHERE period = ?", (period,))
            
            # Insert new data
            for r in rows:
                await db.execute("""
                INSERT INTO treasury_income(period, user_name, user_id, credits, scraped_at)
                VALUES(?, ?, ?, ?, ?)
                """, (r["period"], r["user_name"], r["user_id"], r["credits"], now_utc()))
            
            await db.commit()

    async def _insert_treasury_expenses(self, rows: List[Dict[str, Any]]) -> int:
        """Insert new expense entries (skip duplicates)"""
        inserted = 0
        async with aiosqlite.connect(self.db_path) as db:
            for row in rows:
                try:
                    await db.execute("""
                    INSERT INTO treasury_expenses(hash, expense_date, credits, name, description, scraped_at)
                    VALUES(?, ?, ?, ?, ?, ?)
                    """, (row["hash"], row["expense_date"], row["credits"], 
                          row["name"], row["description"], now_utc()))
                    inserted += 1
                except aiosqlite.IntegrityError:
                    continue
            await db.commit()
        return inserted

    async def _scrape_treasury_once(self, backfill_expenses: bool = False) -> Tuple[int, int, int]:
        """
        Scrape /verband/kasse page.
        Returns (balance, income_rows, expense_rows_inserted)
        """
        base = await self.config.base_url()
        session, own = await self._get_auth_session()
        
        try:
            # Scrape main treasury page
            url = f"{base}/verband/kasse"
            html, _ = await self._fetch(session, url)
            total_funds, income_rows, _ = self._parse_treasury_page(html)
            
            # Save balance and income
            await self._save_treasury_balance(total_funds)
            await self._save_treasury_income(income_rows)
            
            # Scrape expenses
            expenses_inserted = 0
            if backfill_expenses:
                # Initial backfill: scrape all expense pages
                expenses_per_minute = int(await self.config.treasury_expenses_per_minute())
                delay = max(0.0, 60.0 / max(1, expenses_per_minute))
                
                page = 1
                failed_requests = 0
                
                while True:
                    exp_url = f"{base}/verband/kasse?page={page}"
                    try:
                        html, _ = await self._fetch(session, exp_url)
                        expenses = self._parse_treasury_expenses_page(html)
                        
                        if not expenses:
                            break
                        
                        ins = await self._insert_treasury_expenses(expenses)
                        expenses_inserted += ins
                        failed_requests = 0
                        
                        # Stop if no new inserts (all duplicates)
                        if ins == 0:
                            log.info("No new expenses on page %d, stopping backfill", page)
                            break
                        
                        page += 1
                        
                    except Exception as e:
                        failed_requests += 1
                        log.warning("Failed to fetch treasury expenses page %s (failure #%d): %s", 
                                   page, failed_requests, e)
                        if failed_requests >= 3:
                            log.error("Too many consecutive failures, stopping treasury expenses scrape")
                            break
                    
                    actual_delay = delay * (1.5 ** failed_requests) if failed_requests > 0 else delay
                    await asyncio.sleep(actual_delay)
                    
            else:
                # Incremental: only scrape first page (most recent)
                exp_url = f"{base}/verband/kasse?page=1"
                try:
                    html, _ = await self._fetch(session, exp_url)
                    expenses = self._parse_treasury_expenses_page(html)
                    expenses_inserted = await self._insert_treasury_expenses(expenses)
                except Exception as e:
                    log.warning("Failed to fetch treasury expenses: %s", e)
            
            return total_funds, len(income_rows), expenses_inserted
            
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
                       affected_name, affected_type, affected_mc_id, affected_url, description, contribution_amount
                FROM logs
                WHERE id > ?
                ORDER BY id ASC
                LIMIT ?
            """, (int(last_id), int(limit)))
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_treasury_income(self, period: str = "daily") -> List[Dict[str, Any]]:
        """Get treasury income data for a specific period"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""
                SELECT user_name, user_id, credits
                FROM treasury_income
                WHERE period = ?
                ORDER BY credits DESC
            """, (period,))
            return [dict(r) for r in await cur.fetchall()]

    async def get_treasury_balance(self) -> Optional[int]:
        """Get most recent alliance balance"""
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("""
                SELECT total_funds 
                FROM treasury_balance 
                ORDER BY id DESC 
                LIMIT 1
            """)
            row = await cur.fetchone()
            return row[0] if row else None

    async def get_treasury_expenses(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent treasury expenses"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""
                SELECT expense_date, credits, name, description
                FROM treasury_expenses
                ORDER BY id DESC
                LIMIT ?
            """, (limit,))
            return [dict(r) for r in await cur.fetchall()]

    # ---------------- Background ----------------
    async def _maybe_start_background(self):
        if self._bg_members is None:
            self._bg_members = asyncio.create_task(self._members_loop())
        if self._bg_logs is None:
            self._bg_logs = asyncio.create_task(self._logs_loop())
        if self._bg_treasury is None:
            self._bg_treasury = asyncio.create_task(self._treasury_loop())

    async def _members_loop(self):
        await self.bot.wait_until_red_ready()
        failures = 0
        while True:
            try:
                mins = int(await self.config.members_refresh_minutes())
                await self._scrape_members()
                failures = 0
            except asyncio.CancelledError:
                log.info("Members loop cancelled")
                raise
            except Exception as e:
                failures += 1
                log.warning("Background members scrape error (failure #%d): %s", failures, e)
            
            wait_time = max(60, mins * 60) if failures == 0 else min(3600, 60 * (2 ** failures))
            await asyncio.sleep(wait_time)

    async def _logs_loop(self):
        await self.bot.wait_until_red_ready()
        failures = 0
        while True:
            try:
                mins = int(await self.config.logs_refresh_minutes())
                await self._scrape_logs_once(backfill_pages=1)
                failures = 0
            except asyncio.CancelledError:
                log.info("Logs loop cancelled")
                raise
            except Exception as e:
                failures += 1
                log.warning("Background logs scrape error (failure #%d): %s", failures, e)
            
            wait_time = max(60, mins * 60) if failures == 0 else min(3600, 60 * (2 ** failures))
            await asyncio.sleep(wait_time)

    async def _treasury_loop(self):
        await self.bot.wait_until_red_ready()
        
        # Initial backfill on first run
        if await self.config.treasury_initial_backfill():
            try:
                log.info("Starting initial treasury expenses backfill...")
                balance, income, expenses = await self._scrape_treasury_once(backfill_expenses=True)
                log.info("Initial backfill complete: balance=%d, income=%d, expenses=%d", 
                        balance, income, expenses)
                await self.config.treasury_initial_backfill.set(False)
            except Exception as e:
                log.error("Initial treasury backfill failed: %s", e)
        
        # Regular loop
        failures = 0
        while True:
            try:
                mins = int(await self.config.treasury_refresh_minutes())
                await self._scrape_treasury_once(backfill_expenses=False)
                failures = 0
            except asyncio.CancelledError:
                log.info("Treasury loop cancelled")
                raise
            except Exception as e:
                failures += 1
                log.warning("Background treasury scrape error (failure #%d): %s", failures, e)
            
            wait_time = max(60, mins * 60) if failures == 0 else min(3600, 60 * (2 ** failures))
            await asyncio.sleep(wait_time)

    # ---------------- Commands ----------------
    @commands.group(name="scraper")
    @checks.is_owner()
    async def scraper_group(self, ctx: commands.Context):
        """AllianceScraper controls."""
        pass

    @scraper_group.command(name="dbinfo")
    async def db_info(self, ctx: commands.Context):
        """Show database statistics and health check."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            cur = await db.execute("SELECT COUNT(*) FROM members_current")
            members_current = (await cur.fetchone())[0]
            
            cur = await db.execute("SELECT COUNT(*) FROM members_history")
            members_history = (await cur.fetchone())[0]
            
            cur = await db.execute("SELECT COUNT(*) FROM logs")
            logs_total = (await cur.fetchone())[0]
            
            cur = await db.execute("SELECT COUNT(*) FROM members_current WHERE user_id IS NULL OR user_id = ''")
            missing_ids = (await cur.fetchone())[0]
            
            cur = await db.execute("SELECT MAX(scraped_at) FROM members_current")
            last_member_scrape = (await cur.fetchone())[0] or "Never"
            
            cur = await db.execute("SELECT MAX(scraped_at) FROM logs")
            last_log_scrape = (await cur.fetchone())[0] or "Never"
            
            # Treasury stats
            cur = await db.execute("SELECT COUNT(*) FROM treasury_income")
            treasury_income_count = (await cur.fetchone())[0]
            
            cur = await db.execute("SELECT COUNT(*) FROM treasury_expenses")
            treasury_expenses_count = (await cur.fetchone())[0]
            
            cur = await db.execute("SELECT total_funds FROM treasury_balance ORDER BY id DESC LIMIT 1")
            treasury_balance_row = await cur.fetchone()
            treasury_balance = treasury_balance_row[0] if treasury_balance_row else 0
            
            cur = await db.execute("SELECT MAX(scraped_at) FROM treasury_balance")
            last_treasury_scrape = (await cur.fetchone())[0] or "Never"
            
            cur = await db.execute("""
                SELECT action_key, COUNT(*) as cnt 
                FROM logs 
                GROUP BY action_key 
                ORDER BY cnt DESC 
                LIMIT 10
            """)
            top_actions = await cur.fetchall()
            
            cur = await db.execute("""
                SELECT COUNT(*), SUM(contribution_amount) 
                FROM logs 
                WHERE contribution_amount > 0
            """)
            contrib_row = await cur.fetchone()
            contrib_count = contrib_row[0] or 0
            contrib_total = contrib_row[1] or 0
            
            cur = await db.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='index' AND sql IS NOT NULL
                ORDER BY name
            """)
            indices = [row[0] for row in await cur.fetchall()]
            
        embed = discord.Embed(title="Database Statistics", color=discord.Color.blue())
        
        embed.add_field(
            name="Members Tables",
            value=f"Current: {members_current:,}\n"
                  f"History: {members_history:,}\n"
                  f"Missing IDs: {missing_ids}",
            inline=True
        )
        
        embed.add_field(
            name="Logs Table",
            value=f"Total: {logs_total:,}\n"
                  f"Contributions: {contrib_count:,}\n"
                  f"Total Coins: {contrib_total:,}",
            inline=True
        )
        
        embed.add_field(
            name="Treasury Tables",
            value=f"Balance: {treasury_balance:,}\n"
                  f"Income Entries: {treasury_income_count:,}\n"
                  f"Expense Entries: {treasury_expenses_count:,}",
            inline=True
        )
        
        embed.add_field(
            name="Last Scrapes",
            value=f"Members: {last_member_scrape[:19] if len(last_member_scrape) > 19 else last_member_scrape}\n"
                  f"Logs: {last_log_scrape[:19] if len(last_log_scrape) > 19 else last_log_scrape}\n"
                  f"Treasury: {last_treasury_scrape[:19] if len(last_treasury_scrape) > 19 else last_treasury_scrape}",
            inline=False
        )
        
        if top_actions:
            actions_text = "\n".join([f"{row['action_key']}: {row['cnt']:,}" for row in top_actions[:5]])
            embed.add_field(
                name="Top 5 Action Types",
                value=actions_text,
                inline=False
            )
        
        embed.add_field(
            name="Indices",
            value=f"{len(indices)} indices created",
            inline=False
        )
        
        embed.set_footer(text=f"Database: {self.db_path.name}")
        
        await ctx.send(embed=embed)
    
    @scraper_group.command(name="dbdump")
    async def db_dump(self, ctx: commands.Context, table: str = "logs", limit: int = 5):
        """Dump sample data from a table"""
        valid_tables = ["logs", "members_current", "members_history", "treasury_income", 
                       "treasury_balance", "treasury_expenses"]
        if table not in valid_tables:
            await ctx.send(f"Invalid table. Choose: {', '.join(valid_tables)}")
            return
        
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            if table == "logs":
                cur = await db.execute(f"""
                    SELECT id, ts, action_key, executed_name, affected_name, 
                           contribution_amount, description 
                    FROM {table} 
                    ORDER BY id DESC 
                    LIMIT ?
                """, (limit,))
            elif table in ["members_current", "members_history"]:
                cur = await db.execute(f"""
                    SELECT user_id, name, role, earned_credits, 
                           contribution_rate, scraped_at 
                    FROM {table} 
                    ORDER BY scraped_at DESC 
                    LIMIT ?
                """, (limit,))
            elif table == "treasury_income":
                cur = await db.execute(f"""
                    SELECT period, user_name, user_id, credits, scraped_at
                    FROM {table}
                    ORDER BY scraped_at DESC, credits DESC
                    LIMIT ?
                """, (limit,))
            elif table == "treasury_balance":
                cur = await db.execute(f"""
                    SELECT total_funds, scraped_at
                    FROM {table}
                    ORDER BY id DESC
                    LIMIT ?
                """, (limit,))
            else:  # treasury_expenses
                cur = await db.execute(f"""
                    SELECT expense_date, credits, name, description, scraped_at
                    FROM {table}
                    ORDER BY id DESC
                    LIMIT ?
                """, (limit,))
            
            rows = await cur.fetchall()
        
        if not rows:
            await ctx.send(f"No data in {table}")
            return
        
        output = f"**{table} - Last {len(rows)} rows:**\n```\n"
        
        for row in rows:
            row_dict = dict(row)
            if table == "logs":
                output += f"ID: {row_dict['id']}\n"
                output += f"  Time: {row_dict['ts']}\n"
                output += f"  Action: {row_dict['action_key']}\n"
                output += f"  By: {row_dict['executed_name']}\n"
                output += f"  Affected: {row_dict['affected_name']}\n"
                if row_dict['contribution_amount'] > 0:
                    output += f"  Contribution: {row_dict['contribution_amount']:,}\n"
                output += f"  Description: {row_dict['description'][:60]}\n"
            elif table in ["members_current", "members_history"]:
                output += f"User: {row_dict['name']} (ID: {row_dict['user_id']})\n"
                output += f"  Role: {row_dict['role']}\n"
                output += f"  Credits: {row_dict['earned_credits']:,}\n"
                output += f"  Rate: {row_dict['contribution_rate']:.1f}%\n"
                output += f"  Scraped: {row_dict['scraped_at'][:19]}\n"
            elif table == "treasury_income":
                output += f"{row_dict['period']}: {row_dict['user_name']} (ID: {row_dict['user_id']})\n"
                output += f"  Credits: {row_dict['credits']:,}\n"
                output += f"  Scraped: {row_dict['scraped_at'][:19]}\n"
            elif table == "treasury_balance":
                output += f"Balance: {row_dict['total_funds']:,}\n"
                output += f"  Scraped: {row_dict['scraped_at'][:19]}\n"
            else:  # treasury_expenses
                output += f"Date: {row_dict['expense_date']}\n"
                output += f"  Credits: {row_dict['credits']:,}\n"
                output += f"  Name: {row_dict['name']}\n"
                output += f"  Description: {row_dict['description'][:60]}\n"
            output += "\n"
        
        output += "```"
        
        if len(output) > 1900:
            output = output[:1900] + "\n... (truncated)\n```"
        
        await ctx.send(output)

    @scraper_group.command(name="dbquery")
    async def db_query(self, ctx: commands.Context, *, query: str):
        """Execute a read-only SQL query (SELECT only)."""
        query = query.strip()
        
        if not query.upper().startswith("SELECT"):
            await ctx.send("Only SELECT queries are allowed for safety")
            return
        
        dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE"]
        if any(word in query.upper() for word in dangerous):
            await ctx.send("Query contains forbidden keywords")
            return
        
        try:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cur = await db.execute(query)
                rows = await cur.fetchall()
            
            if not rows:
                await ctx.send("Query returned no results")
                return
            
            output = f"**Query Results ({len(rows)} rows):**\n```\n"
            
            if rows:
                cols = list(rows[0].keys())
                output += " | ".join(cols) + "\n"
                output += "-" * 60 + "\n"
                
                for row in rows[:20]:
                    values = [str(row[col]) for col in cols]
                    output += " | ".join(values[:5]) + "\n"
            
            output += "```"
            
            if len(output) > 1900:
                output = output[:1900] + "\n... (truncated)\n```"
            
            await ctx.send(output)
            
        except Exception as e:
            await ctx.send(f"Query error: {e}")

    @scraper_group.group(name="members")
    async def members_group(self, ctx: commands.Context):
        """Members scraping controls."""
        pass

    @members_group.command(name="full")
    async def members_full(self, ctx: commands.Context):
        await ctx.send("Scraping all roster pages, then backfilling missing IDs...")
        n = await self._scrape_members()
        await ctx.send(f"Done. Parsed {n} rows.")

    @members_group.group(name="backfill")
    async def backfill_group(self, ctx: commands.Context):
        """Manage missing-ID backfill."""
        pass

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
        pass

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

    @scraper_group.group(name="treasury")
    async def treasury_group(self, ctx: commands.Context):
        """Treasury scraping controls."""
        pass

    @treasury_group.command(name="run")
    async def treasury_run(self, ctx: commands.Context, backfill: bool = False):
        """Manually scrape treasury. Use backfill=True to scrape all expense pages."""
        await ctx.send("Scraping alliance treasury...")
        balance, income, expenses = await self._scrape_treasury_once(backfill_expenses=backfill)
        await ctx.send(f"Done. Balance: {balance:,}, Income rows: {income}, Expenses inserted: {expenses}")

    @treasury_group.command(name="status")
    async def treasury_status(self, ctx: commands.Context):
        """Show treasury scraping status."""
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM treasury_income")
            income_count = (await cur.fetchone())[0]
            
            cur = await db.execute("SELECT COUNT(*) FROM treasury_expenses")
            expenses_count = (await cur.fetchone())[0]
            
            cur = await db.execute("SELECT total_funds FROM treasury_balance ORDER BY id DESC LIMIT 1")
            balance_row = await cur.fetchone()
            balance = balance_row[0] if balance_row else 0
        
        mins = int(await self.config.treasury_refresh_minutes())
        await ctx.send(f"```\nBalance: {balance:,}\nIncome entries: {income_count:,}\n"
                      f"Expense entries: {expenses_count:,}\nRefresh minutes: {mins}\n```")

    @treasury_group.command(name="setinterval")
    async def treasury_setinterval(self, ctx: commands.Context, minutes: int):
        """Set treasury scraping interval."""
        await self.config.treasury_refresh_minutes.set(max(1, int(minutes)))
        await ctx.send(f"Treasury refresh interval set to {minutes} minute(s)")

    @treasury_group.command(name="resetbackfill")
    async def treasury_resetbackfill(self, ctx: commands.Context):
        """Reset the initial backfill flag to re-run full expense scrape."""
        await self.config.treasury_initial_backfill.set(True)
        await ctx.send("Treasury initial backfill flag reset. Restart the cog to trigger full expense scrape.")

    @treasury_group.command(name="debug")
    async def treasury_debug(self, ctx: commands.Context):
        """Debug treasury HTML parsing."""
        base = await self.config.base_url()
        session, own = await self._get_auth_session()
        
        try:
            url = f"{base}/verband/kasse"
            html, _ = await self._fetch(session, url)
            
            # Save to file for inspection
            debug_file = self.data_path / "treasury_debug.html"
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(html)
            
            # Parse and show what we found
            soup = BeautifulSoup(html, "html.parser")
            
            # Find all tables
            tables = soup.find_all("table")
            await ctx.send(f"Found {len(tables)} tables in the page")
            
            # Look for balance indicators
            balance_candidates = []
            for strong in soup.find_all("strong"):
                text = strong.get_text(strip=True)
                if any(word in text.lower() for word in ["coin", "$", "credit", "balance", "fund"]):
                    balance_candidates.append(text)
            
            if balance_candidates:
                await ctx.send(f"Balance candidates: {', '.join(balance_candidates[:5])}")
            
            # Look for table headers
            for i, table in enumerate(tables[:3]):
                headers = [th.get_text(strip=True) for th in table.find_all("th")]
                if headers:
                    await ctx.send(f"Table {i} headers: {', '.join(headers)}")
                
                # Show first 2 rows
                rows = table.find_all("tr")[:3]
                for j, tr in enumerate(rows):
                    tds = [td.get_text(strip=True)[:30] for td in tr.find_all("td")]
                    if tds:
                        await ctx.send(f"Table {i} row {j}: {' | '.join(tds)}")
            
            await ctx.send(f"HTML saved to: {debug_file}")
            
        finally:
            if own:
                try:
                    await session.close()
                except Exception:
                    pass

async def setup(bot):
    cog = AllianceScraper(bot)
    await bot.add_cog(cog)
