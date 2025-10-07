# alliance_scraper.py (v0.9.3) - Final corrected version
from __future__ import annotations

import asyncio
import aiosqlite
import aiohttp
import re
import random
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

from bs4 import BeautifulSoup
import discord

from redbot.core import commands, checks, Config
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.FARA.AllianceScraper")

DEFAULTS = {
    "base_url": "https://www.missionchief.com",
    "alliance_id": 1621,
    "pages_per_minute": 10,
    "members_refresh_minutes": 15,
    "backfill_auto": True,
    "backfill_concurrency": 5,
    "backfill_retry": 3,
    "backfill_jitter_ms": 250,
    "logs_refresh_minutes": 5,
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
    """Return current UTC time as ISO format string."""
    return datetime.now(timezone.utc).isoformat()

def parse_int64_from_text(txt: str) -> int:
    """Parse integer from text with international number formatting."""
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
    except (ValueError, OverflowError):
        return INT64_MIN if neg else INT64_MAX
    if neg:
        val = -val
    return max(INT64_MIN, min(INT64_MAX, val))

def parse_percent(txt: str) -> float:
    """Parse percentage value from text."""
    if not txt or "%" not in txt:
        return 0.0
    m = re.search(r"(-?\d+(?:[.,]\d+)?)\s*%", txt)
    if not m:
        return 0.0
    try:
        return float(m.group(1).replace(",", "."))
    except (ValueError, TypeError):
        return 0.0

def _extract_id_from_href(href: str) -> Optional[str]:
    """Extract user ID from profile href."""
    if not href:
        return None
    for rx in ID_REGEXPS:
        m = rx.search(href)
        if m:
            return m.group(1)
    return None

def _extract_any_id(url: str) -> Optional[str]:
    """Extract any ID (user, building, etc.) from URL."""
    if not url:
        return None
    for rx in LOG_ID_RX:
        m = rx.search(url)
        if m:
            return m.group(1)
    return None

def _hash_key(ts: str, exec_name: str, action_text: str, affected_name: str, desc: str) -> str:
    """Generate hash key for log deduplication."""
    import hashlib
    raw = f"{ts}|{exec_name}|{action_text}|{affected_name}|{desc}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def _hash_expense(date: str, credits: str, name: str, desc: str) -> str:
    """Generate hash key for expense deduplication."""
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
    """Normalize action text to standardized key."""
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
        self._task_lock = asyncio.Lock()

    async def cog_load(self):
        """Initialize cog on load."""
        await self._init_db()
        await self._maybe_start_background()

    async def cog_unload(self):
        """Cleanup on cog unload."""
        tasks = [self._bg_members, self._bg_logs, self._bg_treasury]
        for task in tasks:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _init_db(self):
        """Initialize database tables and indexes."""
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
            CREATE TABLE IF NOT EXISTS members_current(
                user_id TEXT, name TEXT, role TEXT, earned_credits INTEGER,
                contribution_rate REAL, profile_href TEXT, scraped_at TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS members_history(
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, name TEXT, role TEXT,
                earned_credits INTEGER, contribution_rate REAL, profile_href TEXT, scraped_at TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS logs(
                id INTEGER PRIMARY KEY AUTOINCREMENT, hash TEXT UNIQUE, ts TEXT,
                action_key TEXT, action_text TEXT, executed_name TEXT, executed_mc_id TEXT,
                executed_url TEXT, affected_name TEXT, affected_type TEXT, affected_mc_id TEXT,
                affected_url TEXT, description TEXT, contribution_amount INTEGER DEFAULT 0, scraped_at TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS treasury_income(
                id INTEGER PRIMARY KEY AUTOINCREMENT, period TEXT, user_name TEXT,
                user_id TEXT, credits INTEGER, scraped_at TEXT,
                UNIQUE(period, user_id, scraped_at)
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS treasury_balance(
                id INTEGER PRIMARY KEY AUTOINCREMENT, total_funds INTEGER, scraped_at TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS treasury_expenses(
                id INTEGER PRIMARY KEY AUTOINCREMENT, hash TEXT UNIQUE, expense_date TEXT,
                credits INTEGER, name TEXT, description TEXT, scraped_at TEXT
            )
            """)
            
            # Create indices
            await db.execute("CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(ts)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_logs_hash ON logs(hash)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_logs_action_key ON logs(action_key)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_logs_executed_mc_id ON logs(executed_mc_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_members_history_scraped ON members_history(scraped_at)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_members_history_user_id ON members_history(user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_treasury_income_period ON treasury_income(period)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_treasury_income_user_id ON treasury_income(user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_treasury_income_scraped ON treasury_income(scraped_at)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_treasury_expenses_hash ON treasury_expenses(hash)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_treasury_expenses_date ON treasury_expenses(expense_date)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_treasury_balance_scraped ON treasury_balance(scraped_at)")
            
            await db.commit()
        await self._migrate_db()

    async def _migrate_db(self):
        """Perform database migrations for schema updates."""
        async with aiosqlite.connect(self.db_path) as db:
            await self._ensure_columns(db, "members_current", [
                ("user_id", "TEXT", "''"), ("name", "TEXT", "''"), ("role", "TEXT", "''"),
                ("earned_credits", "INTEGER", "0"), ("contribution_rate", "REAL", "0.0"),
                ("profile_href", "TEXT", "''"), ("scraped_at", "TEXT", "''"),
            ])
            await self._ensure_columns(db, "members_history", [
                ("user_id", "TEXT", "''"), ("name", "TEXT", "''"), ("role", "TEXT", "''"),
                ("earned_credits", "INTEGER", "0"), ("contribution_rate", "REAL", "0.0"),
                ("profile_href", "TEXT", "''"), ("scraped_at", "TEXT", "''"),
            ])
            await self._ensure_columns(db, "logs", [("contribution_amount", "INTEGER", "0")])
            await db.commit()

    async def _ensure_columns(self, db: aiosqlite.Connection, table: str, cols: List[Tuple[str, str, str]]):
        """Ensure table has required columns, add if missing."""
        cur = await db.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in await cur.fetchall()}
        for name, coltype, default in cols:
            if name not in existing:
                try:
                    await db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {coltype} DEFAULT {default}")
                    log.info("Added column %s to %s", name, table)
                except Exception as e:
                    log.warning("Failed to add column %s to %s: %s", name, table, e)

    async def _get_auth_session(self) -> Tuple[aiohttp.ClientSession, bool]:
        """Get authenticated session, either from CookieManager or new session."""
        headers = {"User-Agent": (await self.config.user_agent())}
        timeout = aiohttp.ClientTimeout(total=30)
        cm = self.bot.get_cog("CookieManager")
        if cm:
            try:
                sess = await cm.get_session()
                sess.headers.update(headers)
                return sess, False
            except (AttributeError, RuntimeError) as e:
                log.warning("Failed to get CookieManager session: %s", e)
        sess = aiohttp.ClientSession(headers=headers, timeout=timeout)
        return sess, True

    async def _fetch(self, session: aiohttp.ClientSession, url: str) -> Tuple[str, str]:
        """Fetch URL and return content and final URL."""
        async with session.get(url, allow_redirects=True) as resp:
            resp.raise_for_status()
            text = await resp.text()
            return text, str(resp.url)

    def _extract_member_rows(self, html: str) -> List[Dict[str, Any]]:
        """Extract member data from HTML table."""
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
                "user_id": user_id, "name": name, "role": role,
                "earned_credits": credits, "contribution_rate": rate, "profile_href": href,
            })
        return rows

    async def _save_members(self, rows: List[Dict[str, Any]]):
        """Save member data to database with transaction safety."""
        async with aiosqlite.connect(self.db_path) as db:
            try:
                await db.execute("BEGIN TRANSACTION")
                await db.execute("DELETE FROM members_current")
                
                # Use executemany for better performance
                timestamp = now_utc()
                current_data = []
                history_data = []
                
                for r in rows:
                    credits = r.get("earned_credits") or 0
                    credits = max(INT64_MIN, min(INT64_MAX, credits))
                    
                    record = (
                        r.get("user_id") or "", 
                        r.get("name") or "", 
                        r.get("role") or "", 
                        int(credits),
                        float(r.get("contribution_rate") or 0.0), 
                        r.get("profile_href") or "", 
                        timestamp
                    )
                    current_data.append(record)
                    history_data.append(record)
                
                if current_data:
                    await db.executemany("""
                    INSERT INTO members_current(user_id, name, role, earned_credits, contribution_rate, profile_href, scraped_at)
                    VALUES(?,?,?,?,?,?,?)
                    """, current_data)
                
                if history_data:
                    await db.executemany("""
                    INSERT INTO members_history(user_id, name, role, earned_credits, contribution_rate, profile_href, scraped_at)
                    VALUES(?,?,?,?,?,?,?)
                    """, history_data)
                
                await db.commit()
            except Exception:
                await db.rollback()
                raise

    async def get_members(self) -> List[Dict[str, Any]]:
        """Get current member list from database."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT user_id, name, role, earned_credits, contribution_rate FROM members_current")
            return [dict(r) for r in await cur.fetchall()]

    async def _resolve_member_id(self, session: aiohttp.ClientSession, base: str, href: str) -> Optional[str]:
        """Resolve member ID by following redirects if needed."""
        if not href:
            return None
        uid = _extract_id_from_href(href)
        if uid:
            return uid
        url = href if href.startswith("http") else f"{base}{href}"
        try:
            async with session.get(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                final_url = str(resp.url)
                for rx in ID_REGEXPS:
                    m = rx.search(final_url)
                    if m:
                        return m.group(1)
                text = await resp.text()
                for rx in ID_REGEXPS:
                    m = rx.search(text)
                    if m:
                        return m.group(1)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.debug("Failed to resolve member ID from %s: %s", href, e)
            return None
        return None

    async def _backfill_missing_ids(self, session: aiohttp.ClientSession, base: str, limit: Optional[int] = None) -> int:
        """Backfill missing member IDs by resolving profile URLs."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            sql = "SELECT rowid, name, profile_href FROM members_current WHERE (user_id IS NULL OR user_id='')"
            if limit:
                sql += " LIMIT ?"
                cur = await db.execute(sql, (int(limit),))
            else:
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
                    except Exception as e:
                        log.debug("Backfill attempt %d failed for %s: %s", attempt, href, e)
                        uid = None
                if uid:
                    async with aiosqlite.connect(self.db_path) as db2:
                        await db2.execute("UPDATE members_current SET user_id=? WHERE rowid=?", (uid, row["rowid"]))
                        await db2.commit()
                    updated += 1

        await asyncio.gather(*(worker(r) for r in rows), return_exceptions=True)
        return updated

    async def _scrape_members(self) -> int:
        """Scrape all member pages and store in database."""
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
                        match = re.search(r"page=(\d+)", a["href"])
                        if match:
                            p = int(match.group(1))
                            if p > last_page:
                                last_page = p
                    except (AttributeError, ValueError):
                        pass

            all_rows: List[Dict[str, Any]] = []
            failed_requests = 0
            
            for p in range(1, last_page + 1):
                url = f"{base}{tpl.format(alliance_id=alliance_id, page=p)}"
                try:
                    html, _ = await self._fetch(session, url)
                    all_rows.extend(self._extract_member_rows(html))
                    failed_requests = 0
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
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
                await session.close()

    def _parse_logs_page(self, html: str, base: str) -> List[Dict[str, Any]]:
        """Parse alliance log entries from HTML."""
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
                "hash": h, "ts": dt_text, "action_key": action_key, "action_text": action_text,
                "executed_name": executed_name, "executed_mc_id": executed_mc_id, "executed_url": executed_url,
                "affected_name": affected_name, "affected_type": affected_type, "affected_mc_id": affected_mc_id,
                "affected_url": affected_url, "description": desc_text, "contribution_amount": contribution_amount,
            })
        return results

    async def _insert_logs(self, rows: List[Dict[str, Any]]) -> int:
        """Insert log entries into database."""
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
        """Scrape alliance logs, optionally limiting to specific number of pages."""
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
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
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
                await session.close()

    def _parse_treasury_page(self, html: str) -> Tuple[int, List[Dict[str, Any]]]:
        """Parse treasury balance and income from HTML page."""
        soup = BeautifulSoup(html, "html.parser")
        
        # Find total funds/balance
        total_funds = 0
        for elem in soup.find_all(["div", "span", "strong", "p", "h1", "h2", "h3"]):
            text = elem.get_text(strip=True)
            if any(word in text.lower() for word in ["fund", "balance", "coin", "treasury"]):
                val = parse_int64_from_text(text)
                if val > 1000000:
                    total_funds = val
                    break
        
        if total_funds == 0:
            for elem in soup.find_all(text=True):
                text = elem.strip()
                if text and any(c.isdigit() for c in text):
                    val = parse_int64_from_text(text)
                    if val > total_funds:
                        total_funds = val
        
        # Parse income table
        income_rows = []
        tables = soup.find_all("table")
        
        if len(tables) >= 1:
            income_table = tables[0]
            headers = [th.get_text(strip=True).lower() for th in income_table.find_all("th")]
            
            if "name" in headers and "credits" in headers:
                period = "daily"
                prev = income_table.find_previous(["h1", "h2", "h3", "h4", "strong"])
                if prev:
                    heading = prev.get_text(strip=True).lower()
                    if "month" in heading:
                        period = "monthly"
                
                for tr in income_table.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) < 2:
                        continue
                    
                    a = tds[0].find("a", href=True)
                    if not a:
                        continue
                    
                    name = a.get_text(strip=True)
                    href = a["href"]
                    user_id = _extract_id_from_href(href) or ""
                    credits = parse_int64_from_text(tds[1].get_text(strip=True))
                    
                    if credits > 0:
                        income_rows.append({
                            "period": period,
                            "user_name": name,
                            "user_id": user_id,
                            "credits": credits,
                        })
        
        return total_funds, income_rows
    
    def _parse_treasury_expenses_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse treasury expenses from HTML page."""
        soup = BeautifulSoup(html, "html.parser")
        expenses = []
        
        tables = soup.find_all("table")
        
        # Find the expenses table (usually has date/description columns)
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            
            # Look for expense-related headers
            if any(word in " ".join(headers) for word in ["date", "expense", "description", "cost"]):
                for tr in table.find_all("tr"):
                    tds = tr.find_all("td")
                    if len(tds) < 3:
                        continue
                    
                    # Extract date (first column typically)
                    expense_date = tds[0].get_text(strip=True)
                    
                    # Extract credits (look for numeric value)
                    credits = 0
                    name = ""
                    description = ""
                    
                    for idx, td in enumerate(tds[1:], 1):
                        txt = td.get_text(strip=True)
                        
                        # Try to parse as credits
                        val = parse_int64_from_text(txt)
                        if val != 0 and credits == 0:
                            credits = val
                        elif not name and txt and not any(c.isdigit() for c in txt):
                            name = txt
                        elif txt and txt != name:
                            description = txt
                    
                    # If we couldn't identify columns clearly, use positional parsing
                    if not name and len(tds) >= 3:
                        expense_date = tds[0].get_text(strip=True)
                        credits = parse_int64_from_text(tds[1].get_text(strip=True))
                        name = tds[2].get_text(strip=True) if len(tds) > 2 else ""
                        description = tds[3].get_text(strip=True) if len(tds) > 3 else ""
                    
                    if expense_date and credits != 0:
                        h = _hash_expense(expense_date, str(credits), name, description)
                        expenses.append({
                            "hash": h,
                            "expense_date": expense_date,
                            "credits": credits,
                            "name": name,
                            "description": description,
                        })
        
        return expenses
    
    async def _insert_treasury_data(self, balance: int, income: List[Dict[str, Any]], 
                                   expenses: List[Dict[str, Any]]) -> Tuple[int, int, int]:
        """Insert treasury data into database."""
        inserted_income = 0
        inserted_expenses = 0
        timestamp = now_utc()
        
        async with aiosqlite.connect(self.db_path) as db:
            # Insert balance
            if balance > 0:
                await db.execute("""
                INSERT INTO treasury_balance(total_funds, scraped_at)
                VALUES(?,?)
                """, (balance, timestamp))
            
            # Insert income records (with unique constraint handling)
            for record in income:
                try:
                    await db.execute("""
                    INSERT INTO treasury_income(period, user_name, user_id, credits, scraped_at)
                    VALUES(?,?,?,?,?)
                    """, (record["period"], record["user_name"], record["user_id"], 
                          record["credits"], timestamp))
                    inserted_income += 1
                except aiosqlite.IntegrityError:
                    # Duplicate entry, skip
                    continue
            
            # Insert expense records
            for expense in expenses:
                try:
                    await db.execute("""
                    INSERT INTO treasury_expenses(hash, expense_date, credits, name, description, scraped_at)
                    VALUES(?,?,?,?,?,?)
                    """, (expense["hash"], expense["expense_date"], expense["credits"],
                          expense["name"], expense["description"], timestamp))
                    inserted_expenses += 1
                except aiosqlite.IntegrityError:
                    continue
            
            await db.commit()
        
        return 1 if balance > 0 else 0, inserted_income, inserted_expenses
    
    async def _scrape_treasury_once(self) -> Tuple[int, int, int]:
        """Scrape treasury data once and return counts of inserted records."""
        base = await self.config.base_url()
        session, own = await self._get_auth_session()
        
        try:
            # Fetch main treasury page
            treasury_url = f"{base}/alliance_finances"
            html, _ = await self._fetch(session, treasury_url)
            
            balance, income = self._parse_treasury_page(html)
            
            # Fetch expenses page if it exists
            expenses = []
            try:
                expenses_url = f"{base}/alliance_finances/expenses"
                expenses_html, _ = await self._fetch(session, expenses_url)
                expenses = self._parse_treasury_expenses_page(expenses_html)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                log.debug("Could not fetch expenses page: %s", e)
            
            balance_count, income_count, expense_count = await self._insert_treasury_data(
                balance, income, expenses
            )
            
            log.info("Treasury scrape complete: balance=%d, income=%d, expenses=%d", 
                    balance_count, income_count, expense_count)
            
            return balance_count, income_count, expense_count
            
        finally:
            if own:
                await session.close()
    
    async def _maybe_start_background(self):
        """Start background tasks if enabled in configuration."""
        async with self._task_lock:
            members_refresh = await self.config.members_refresh_minutes()
            logs_refresh = await self.config.logs_refresh_minutes()
            treasury_refresh = await self.config.treasury_refresh_minutes()
            
            if members_refresh > 0 and (not self._bg_members or self._bg_members.done()):
                self._bg_members = asyncio.create_task(self._background_scrape_members())
                log.info("Started background member scraping task")
            
            if logs_refresh > 0 and (not self._bg_logs or self._bg_logs.done()):
                self._bg_logs = asyncio.create_task(self._background_scrape_logs())
                log.info("Started background logs scraping task")
            
            if treasury_refresh > 0 and (not self._bg_treasury or self._bg_treasury.done()):
                self._bg_treasury = asyncio.create_task(self._background_scrape_treasury())
                log.info("Started background treasury scraping task")
    
    async def _background_scrape_members(self):
        """Background task to periodically scrape members."""
        await self.bot.wait_until_ready()
        
        # Initial scrape immediately
        try:
            count = await self._scrape_members()
            log.info("Initial background member scrape completed: %d members", count)
        except Exception as e:
            log.exception("Error in initial member scrape")
        
        while True:
            try:
                minutes = await self.config.members_refresh_minutes()
                if minutes <= 0:
                    break
                await asyncio.sleep(minutes * 60)
                count = await self._scrape_members()
                log.info("Background member scrape completed: %d members", count)
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("Error in background member scrape")
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    async def _background_scrape_logs(self):
        """Background task to periodically scrape logs."""
        await self.bot.wait_until_ready()
        
        # Initial scrape immediately
        try:
            count = await self._scrape_logs_once()
            log.info("Initial background logs scrape completed: %d entries", count)
        except Exception as e:
            log.exception("Error in initial logs scrape")
        
        while True:
            try:
                minutes = await self.config.logs_refresh_minutes()
                if minutes <= 0:
                    break
                await asyncio.sleep(minutes * 60)
                count = await self._scrape_logs_once()
                log.info("Background logs scrape completed: %d entries", count)
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("Error in background logs scrape")
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    async def _background_scrape_treasury(self):
        """Background task to periodically scrape treasury."""
        await self.bot.wait_until_ready()
        
        # Initial scrape immediately
        try:
            balance, income, expenses = await self._scrape_treasury_once()
            log.info("Initial background treasury scrape completed: balance=%d, income=%d, expenses=%d",
                    balance, income, expenses)
        except Exception as e:
            log.exception("Error in initial treasury scrape")
        
        while True:
            try:
                minutes = await self.config.treasury_refresh_minutes()
                if minutes <= 0:
                    break
                await asyncio.sleep(minutes * 60)
                balance, income, expenses = await self._scrape_treasury_once()
                log.info("Background treasury scrape completed: balance=%d, income=%d, expenses=%d",
                        balance, income, expenses)
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("Error in background treasury scrape")
                await asyncio.sleep(300)  # Wait 5 minutes on error
    
    # Public API methods for other cogs
    
    async def get_logs(self, action_key: Optional[str] = None, 
                      limit: int = 100) -> List[Dict[str, Any]]:
        """Get alliance logs, optionally filtered by action key."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            if action_key:
                sql = """
                SELECT ts, action_key, action_text, executed_name, executed_mc_id,
                       affected_name, affected_type, affected_mc_id, description, 
                       contribution_amount
                FROM logs 
                WHERE action_key = ?
                ORDER BY ts DESC 
                LIMIT ?
                """
                cur = await db.execute(sql, (action_key, limit))
            else:
                sql = """
                SELECT ts, action_key, action_text, executed_name, executed_mc_id,
                       affected_name, affected_type, affected_mc_id, description,
                       contribution_amount
                FROM logs 
                ORDER BY ts DESC 
                LIMIT ?
                """
                cur = await db.execute(sql, (limit,))
            
            return [dict(r) for r in await cur.fetchall()]
    
    async def get_treasury_balance(self) -> Optional[int]:
        """Get the most recent treasury balance."""
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("""
            SELECT total_funds 
            FROM treasury_balance 
            ORDER BY scraped_at DESC 
            LIMIT 1
            """)
            row = await cur.fetchone()
            return row[0] if row else None
    
    async def get_treasury_income(self, period: str = "daily") -> List[Dict[str, Any]]:
        """Get treasury income records for specified period (latest scrape only)."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            # Get the most recent scrape time for this period
            cur = await db.execute("""
            SELECT MAX(scraped_at) FROM treasury_income WHERE period = ?
            """, (period,))
            latest_scrape = await cur.fetchone()
            
            if not latest_scrape or not latest_scrape[0]:
                return []
            
            # Get all records from that scrape
            cur = await db.execute("""
            SELECT user_name, user_id, credits, scraped_at
            FROM treasury_income
            WHERE period = ? AND scraped_at = ?
            ORDER BY credits DESC
            """, (period, latest_scrape[0]))
            return [dict(r) for r in await cur.fetchall()]
    
    async def get_treasury_expenses(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent treasury expenses."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""
            SELECT expense_date, credits, name, description
            FROM treasury_expenses
            ORDER BY expense_date DESC
            LIMIT ?
            """, (limit,))
            return [dict(r) for r in await cur.fetchall()]
    
    async def get_member_history(self, user_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get historical snapshots for a specific member."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""
            SELECT name, role, earned_credits, contribution_rate, scraped_at
            FROM members_history
            WHERE user_id = ?
            ORDER BY scraped_at DESC
            LIMIT ?
            """, (user_id, limit))
            return [dict(r) for r in await cur.fetchall()]
    
    # Discord commands
    
    @commands.group()
    @checks.is_owner()
    async def alliance(self, ctx):
        """Alliance scraper commands."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help()
    
    @alliance.command(name="scrape_members")
    async def scrape_members_cmd(self, ctx):
        """Manually trigger a member scrape."""
        async with ctx.typing():
            try:
                count = await self._scrape_members()
                await ctx.send(f"\u2705 Scraped {count} members successfully.")
            except Exception as e:
                log.exception("Error scraping members")
                await ctx.send(f"\u274c Error scraping members: {type(e).__name__}")
    
    @alliance.command(name="scrape_logs")
    async def scrape_logs_cmd(self, ctx, pages: int = 5):
        """Manually trigger a logs scrape."""
        async with ctx.typing():
            try:
                count = await self._scrape_logs_once(backfill_pages=pages)
                await ctx.send(f"\u2705 Scraped {count} log entries from {pages} pages.")
            except Exception as e:
                log.exception("Error scraping logs")
                await ctx.send(f"\u274c Error scraping logs: {type(e).__name__}")
    
    @alliance.command(name="scrape_treasury")
    async def scrape_treasury_cmd(self, ctx):
        """Manually trigger a treasury scrape."""
        async with ctx.typing():
            try:
                balance, income, expenses = await self._scrape_treasury_once()
                await ctx.send(
                    f"\u2705 Treasury scrape complete:\n"
                    f"- Balance records: {balance}\n"
                    f"- Income records: {income}\n"
                    f"- Expense records: {expenses}"
                )
            except Exception as e:
                log.exception("Error scraping treasury")
                await ctx.send(f"\u274c Error scraping treasury: {type(e).__name__}")
    
    @alliance.command(name="stats")
    async def stats_cmd(self, ctx):
        """Show database statistics."""
        async with ctx.typing():
            try:
                async with aiosqlite.connect(self.db_path) as db:
                    # Get member count
                    cur = await db.execute("SELECT COUNT(*) FROM members_current")
                    member_count = (await cur.fetchone())[0]
                    
                    # Get log count
                    cur = await db.execute("SELECT COUNT(*) FROM logs")
                    log_count = (await cur.fetchone())[0]
                    
                    # Get treasury balance
                    cur = await db.execute("""
                    SELECT total_funds FROM treasury_balance 
                    ORDER BY scraped_at DESC LIMIT 1
                    """)
                    balance_row = await cur.fetchone()
                    balance = balance_row[0] if balance_row else 0
                    
                    # Get latest scrape times
                    cur = await db.execute("""
                    SELECT scraped_at FROM members_current 
                    ORDER BY scraped_at DESC LIMIT 1
                    """)
                    last_member_scrape = await cur.fetchone()
                    
                    cur = await db.execute("""
                    SELECT scraped_at FROM logs 
                    ORDER BY scraped_at DESC LIMIT 1
                    """)
                    last_log_scrape = await cur.fetchone()
                
                embed = discord.Embed(
                    title="Alliance Scraper Statistics",
                    color=discord.Color.blue()
                )
                embed.add_field(name="Members", value=str(member_count), inline=True)
                embed.add_field(name="Log Entries", value=str(log_count), inline=True)
                embed.add_field(name="Treasury Balance", value=f"{balance:,}", inline=True)
                
                if last_member_scrape:
                    embed.add_field(name="Last Member Scrape", value=last_member_scrape[0], inline=False)
                if last_log_scrape:
                    embed.add_field(name="Last Log Scrape", value=last_log_scrape[0], inline=False)
                
                await ctx.send(embed=embed)
            except Exception:
                log.exception("Error fetching stats")
                await ctx.send("\u274c Error fetching statistics")


async def setup(bot):
    """Load the AllianceScraper cog."""
    await bot.add_cog(AllianceScraper(bot))
