# alliance_scraper.py (v1.2.2)
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import aiosqlite
from bs4 import BeautifulSoup, Tag
from redbot.core import commands, checks, Config
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.FARA.AllianceScraper")

DEFAULTS = {
    # Source URLs
    "members_url": "https://www.missionchief.com/verband/mitglieder/1621",
    "logs_url": "https://www.missionchief.com/alliance_logfiles",
    "schoolings_url": "https://www.missionchief.com/schoolings",
    "kasse_url": "https://www.missionchief.com/verband/kasse",
    # Scheduling (minutes)
    "interval_members": 60,
    "interval_logs": 5,
    "interval_schoolings": 60,
    "interval_kasse": 5,
    # Rate limiting
    "per_request_delay_seconds": 3,
    "per_request_jitter_seconds": 1,
    # Pagination hints
    "page_param": "page",
    # Member ID extraction regexes (first capture group should be the numeric id)
    "member_id_href_patterns": [r"/users/(\\d+)", r"/profile/(\\d+)", r"user_id=(\\d+)", r"/user/(\\d+)"]
}

# Header synonyms for robust parsing (EN/DE)
HEADER_MAP = {
    "members": {
        "name": ["name", "mitglied", "member", "benutzer", "username"],
        "role": ["role", "rolle"],
        "earned_credits": ["earned credits", "verdiente credits", "verdiente", "credits"],
        "contrib_rate": ["contribution rate", "beitragsquote", "contribution", "alliance contribution rate"],
    },
    "logs": {
        "date": ["date", "datum"],
        "executed_by": ["executed by", "ausgeführt von", "von", "user"],
        "description": ["description", "beschreibung"],
        "affected": ["affected", "betroffen"],
    },
    "schoolings": {
        "course": ["course", "lehrgang", "training"],
        "time_left": ["time left", "restzeit", "dauer"],
        "user": ["user", "leiter", "host", "owner", "benutzer"],
    },
    "kasse": {
        "date": ["date", "datum"],
        "description": ["description", "beschreibung", "verwendungszweck"],
        "amount": ["amount", "betrag"],
        "balance": ["balance", "kontostand"],
        "type": ["type", "art"],
    }
}

def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

def _header_index_map(headers: List[str], wanted_map: Dict[str, List[str]]) -> Dict[str, int]:
    index = {}
    for i, h in enumerate(headers):
        nh = _norm(h)
        for key, variants in wanted_map.items():
            if key in index:
                continue
            for v in variants:
                nv = _norm(v)
                if nv == nh or nv in nh or nh in nv:
                    index[key] = i
                    break
    return index

class AllianceScraper(commands.Cog):
    """Scrapes MissionChief alliance data using CookieManager session and stores into SQLite.
       v1.2.2: robust schema migration, fix race in setup, add !scraper fixdb.
    """

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA11A9E55C, force_registration=True)
        self.config.register_global(**DEFAULTS)
        self.data_path = cog_data_path(self)
        self.db_path = self.data_path / "alliance.db"
        self._bg_task: Optional[asyncio.Task] = None
        self._locks = {
            "members": asyncio.Lock(),
            "logs": asyncio.Lock(),
            "schoolings": asyncio.Lock(),
            "kasse": asyncio.Lock(),
        }
        self._migrated = False

    # ----------------- DB init & migration -----------------
    async def _init_db(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
            CREATE TABLE IF NOT EXISTS members_current (
                member_id TEXT PRIMARY KEY,
                user_id TEXT,
                name TEXT,
                role TEXT,
                earned_credits INTEGER,
                contrib_rate REAL,
                last_seen_utc TEXT,
                source_page INTEGER,
                updated_at_utc TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS members_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                member_id TEXT,
                name TEXT,
                role TEXT,
                earned_credits INTEGER,
                contrib_rate REAL,
                snapshot_utc TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS alliance_logs (
                id TEXT PRIMARY KEY, -- hash key
                date_utc TEXT,
                executed_by TEXT,
                description TEXT,
                affected TEXT,
                raw_hash TEXT, 
                inserted_at_utc TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS schoolings_open (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                course TEXT,
                time_left TEXT,
                user TEXT,
                snapshot_utc TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS kasse_transactions (
                id TEXT PRIMARY KEY, -- hash key
                date_utc TEXT,
                description TEXT,
                amount TEXT,
                balance TEXT,
                type TEXT,
                raw_hash TEXT,
                inserted_at_utc TEXT
            )
            """)
            await db.commit()
        await self._migrate_schema()
        self._migrated = True

    async def _migrate_schema(self):
        async with aiosqlite.connect(self.db_path) as db:
            # members_current.user_id
            cols = [r[1] for r in await (await db.execute("PRAGMA table_info(members_current)")).fetchall()]
            if "user_id" not in cols:
                await db.execute("ALTER TABLE members_current ADD COLUMN user_id TEXT")
            # members_history.user_id
            cols = [r[1] for r in await (await db.execute("PRAGMA table_info(members_history)")).fetchall()]
            if "user_id" not in cols:
                await db.execute("ALTER TABLE members_history ADD COLUMN user_id TEXT")
            # indexes
            await db.execute("CREATE INDEX IF NOT EXISTS idx_members_user_id ON members_current(user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_logs_date ON alliance_logs(date_utc)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_kasse_date ON kasse_transactions(date_utc)")
            await db.commit()

    async def _ensure_migrated(self):
        if not self._migrated:
            await self._init_db()

    # ----------------- Public API for other cogs -----------------
    async def get_members(self) -> List[Dict[str, Any]]:
        await self._ensure_migrated()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM members_current ORDER BY earned_credits DESC")
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_logs_since(self, since_utc: Optional[str] = None, limit: int = 500) -> List[Dict[str, Any]]:
        await self._ensure_migrated()
        query = "SELECT * FROM alliance_logs"
        params: List[Any] = []
        if since_utc:
            query += " WHERE date_utc >= ?"
            params.append(since_utc)
        query += " ORDER BY date_utc DESC LIMIT ?"
        params.append(limit)
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(query, params)
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_schoolings(self) -> List[Dict[str, Any]]:
        await self._ensure_migrated()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM schoolings_open ORDER BY snapshot_utc DESC, course ASC")
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_kasse_transactions(self, limit: int = 500) -> List[Dict[str, Any]]:
        await self._ensure_migrated()
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM kasse_transactions ORDER BY date_utc DESC LIMIT ?", (limit,))
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    # ----------------- Internal helpers -----------------
    async def _get_session(self) -> aiohttp.ClientSession:
        cm = self.bot.get_cog("CookieManager")
        if not cm:
            raise RuntimeError("CookieManager not loaded")
        return await cm.get_session()

    async def _fetch_pages(self, base_url: str, cfg: Dict[str, Any], stop_after_seen: Optional[callable] = None) -> List[str]:
        """Fetch pages using ?page= pagination until no rows or stop_after_seen signals stop."""
        html_pages: List[str] = []
        page = 1
        while True:
            url = base_url
            if "?" in base_url:
                url = f"{base_url}&{cfg['page_param']}={page}"
            else:
                url = f"{base_url}?{cfg['page_param']}={page}"
            session = await self._get_session()
            try:
                r = await session.get(url, allow_redirects=True)
                if r.status != 200:
                    break
                html = await r.text()
            finally:
                await session.close()
            soup = BeautifulSoup(html, "lxml")
            rows = soup.find_all("tr")
            if not rows or len(rows) <= 1:
                break
            html_pages.append(html)
            if stop_after_seen and stop_after_seen(html):
                break
            await asyncio.sleep(max(1, int(cfg.get("per_request_delay_seconds", 3))))
            page += 1
            if page > 1000:
                break
        return html_pages

    def _parse_table(self, html: str) -> Tuple[List[str], List[List[str]], List[Tag]]:
        """Return headers, row text cells, and the TR elements for link parsing."""
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        if not table:
            return [], [], []
        headers: List[str] = []
        thead = table.find("thead")
        if thead:
            headers = [th.get_text(strip=True) for th in thead.find_all("th")]
        else:
            first = table.find("tr")
            if first:
                headers = [th.get_text(strip=True) for th in first.find_all(["th","td"])]
        rows_text: List[List[str]] = []
        rows_tr: List[Tag] = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            rows_text.append([td.get_text(" ", strip=True) for td in tds])
            rows_tr.append(tr)
        return headers, rows_text, rows_tr

    async def _extract_member_id(self, tr: Tag, patterns: List[str]) -> Optional[str]:
        # 1) data attributes
        for attr in ["data-user-id", "data-userid", "data-user", "data-member-id", "data-id"]:
            v = tr.get(attr) or tr.attrs.get(attr)
            if v and str(v).isdigit():
                return str(v)
        # 2) look for anchors with href patterns
        anchors = tr.find_all("a", href=True)
        for a in anchors:
            href = a["href"]
            for pat in patterns:
                m = re.search(pat, href)
                if m:
                    return m.group(1)
        # 3) inputs/spans with data-* inside cells
        for tag in tr.find_all(["span","div","input"]):
            for attr, val in tag.attrs.items():
                if attr.startswith("data-") and isinstance(val, (str,int)):
                    if str(val).isdigit():
                        return str(val)
        return None

    # Members
    async def _scrape_members(self) -> int:
        await self._ensure_migrated()
        cfg = await self.config.all()
        url = cfg["members_url"]
        patterns = cfg.get("member_id_href_patterns", DEFAULTS["member_id_href_patterns"])
        async with self._locks["members"]:
            pages = await self._fetch_pages(url, cfg)
            total = 0
            async with aiosqlite.connect(self.db_path) as db:
                for i, html in enumerate(pages, start=1):
                    headers, rows, trs = self._parse_table(html)
                    idx = _header_index_map(headers, HEADER_MAP["members"])
                    for r, tr in zip(rows, trs):
                        # parse basics
                        name = r[idx.get("name", -1)] if idx.get("name", -1) >= 0 and idx.get("name") < len(r) else None
                        role = r[idx.get("role", -1)] if idx.get("role", -1) >= 0 and idx.get("role") < len(r) else None
                        earned = r[idx.get("earned_credits", -1)] if idx.get("earned_credits", -1) >= 0 and idx.get("earned_credits") < len(r) else "0"
                        contrib = r[idx.get("contrib_rate", -1)] if idx.get("contrib_rate", -1) >= 0 and idx.get("contrib_rate") < len(r) else "0"
                        # extract user_id
                        user_id = await self._extract_member_id(tr, patterns)
                        # legacy member_id fallback: normalized name
                        member_id = user_id or (name or "").strip().lower()
                        try:
                            earned_i = int("".join(c for c in str(earned) if c.isdigit()))
                        except Exception:
                            earned_i = 0
                        try:
                            contrib_f = float(str(contrib).replace("%","").replace(",","."))
                        except Exception:
                            contrib_f = 0.0
                        now = datetime.utcnow().isoformat()
                        await db.execute("""
                        INSERT INTO members_current(member_id, user_id, name, role, earned_credits, contrib_rate, last_seen_utc, source_page, updated_at_utc)
                        VALUES(?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(member_id) DO UPDATE SET
                            user_id=excluded.user_id,
                            name=excluded.name,
                            role=excluded.role,
                            earned_credits=excluded.earned_credits,
                            contrib_rate=excluded.contrib_rate,
                            last_seen_utc=excluded.last_seen_utc,
                            source_page=excluded.source_page,
                            updated_at_utc=excluded.updated_at_utc
                        """, (member_id, user_id, name, role, earned_i, contrib_f, now, i, now))
                        await db.execute("""
                        INSERT INTO members_history(user_id, member_id, name, role, earned_credits, contrib_rate, snapshot_utc)
                        VALUES(?,?,?,?,?,?,?)
                        """, (user_id, member_id, name, role, earned_i, contrib_f, now))
                        total += 1
                await db.commit()
        if total:
            try:
                self.bot.dispatch("fara_members_updated", {"rows": total, "ts": datetime.utcnow().isoformat()})
            except Exception:
                pass
        return total

    # Logs
    def _log_row_hash(self, row: Dict[str, Any]) -> str:
        base = f"{row.get('date_utc','')}|{row.get('executed_by','')}|{row.get('description','')}|{row.get('affected','')}"
        import hashlib
        return hashlib.sha1(base.encode("utf-8")).hexdigest()

    async def _scrape_logs(self, incremental: bool = True) -> int:
        await self._ensure_migrated()
        cfg = await self.config.all()
        url = cfg["logs_url"]
        async with self._locks["logs"]:
            async def stop_after_seen(html: str) -> bool:
                if not incremental:
                    return False
                headers, rows, _ = self._parse_table(html)
                idx = _header_index_map(headers, HEADER_MAP["logs"])
                async with aiosqlite.connect(self.db_path) as db:
                    db.row_factory = aiosqlite.Row
                    cur = await db.execute("SELECT date_utc FROM alliance_logs ORDER BY date_utc DESC LIMIT 1")
                    latest = await cur.fetchone()
                    latest_dt = latest["date_utc"] if latest else None
                if latest_dt:
                    for r in rows:
                        dt = r[idx.get("date", -1)] if idx.get("date", -1) >= 0 and idx.get("date") < len(r) else None
                        if dt and dt >= latest_dt:
                            return False
                    return True
                return False

            pages = await self._fetch_pages(url, cfg, stop_after_seen=stop_after_seen if incremental else None)
            inserted = 0
            async with aiosqlite.connect(self.db_path) as db:
                for html in pages:
                    headers, rows, _ = self._parse_table(html)
                    idx = _header_index_map(headers, HEADER_MAP["logs"])
                    for r in rows:
                        date_s = r[idx.get("date", -1)] if idx.get("date", -1) >= 0 and idx.get("date") < len(r) else None
                        executed_by = r[idx.get("executed_by", -1)] if idx.get("executed_by", -1) >= 0 and idx.get("executed_by") < len(r) else None
                        description = r[idx.get("description", -1)] if idx.get("description", -1) >= 0 and idx.get("description") < len(r) else None
                        affected = r[idx.get("affected", -1)] if idx.get("affected", -1) >= 0 and idx.get("affected") < len(r) else None
                        rowd = {"date_utc": date_s, "executed_by": executed_by, "description": description, "affected": affected}
                        hid = self._log_row_hash(rowd)
                        now = datetime.utcnow().isoformat()
                        try:
                            await db.execute("""
                            INSERT INTO alliance_logs(id, date_utc, executed_by, description, affected, raw_hash, inserted_at_utc)
                            VALUES(?,?,?,?,?,?,?)
                            """, (hid, date_s, executed_by, description, affected, hid, now))
                            inserted += 1
                        except Exception:
                            pass
                await db.commit()
        if inserted:
            try:
                self.bot.dispatch("fara_logs_updated", {"rows": inserted, "ts": datetime.utcnow().isoformat()})
            except Exception:
                pass
        return inserted

    # Schoolings
    async def _scrape_schoolings(self) -> int:
        await self._ensure_migrated()
        cfg = await self.config.all()
        url = cfg["schoolings_url"]
        async with self._locks["schoolings"]:
            session = await self._get_session()
            try:
                r = await session.get(url, allow_redirects=True)
                html = await r.text()
            finally:
                await session.close()
            headers, rows, _ = self._parse_table(html)
            idx = _header_index_map(headers, HEADER_MAP["schoolings"])
            total = 0
            async with aiosqlite.connect(self.db_path) as db:
                for r in rows:
                    course = r[idx.get("course", -1)] if idx.get("course", -1) >= 0 and idx.get("course") < len(r) else None
                    time_left = r[idx.get("time_left", -1)] if idx.get("time_left", -1) >= 0 and idx.get("time_left") < len(r) else None
                    user = r[idx.get("user", -1)] if idx.get("user", -1) >= 0 and idx.get("user") < len(r) else None
                    now = datetime.utcnow().isoformat()
                    await db.execute("""
                    INSERT INTO schoolings_open(course, time_left, user, snapshot_utc) VALUES(?,?,?,?)
                    """, (course, time_left, user, now))
                    total += 1
                await db.commit()
        if total:
            try:
                self.bot.dispatch("fara_schoolings_updated", {"rows": total, "ts": datetime.utcnow().isoformat()})
            except Exception:
                pass
        return total

    # Kasse
    def _kasse_row_hash(self, row: Dict[str, Any]) -> str:
        s = f"{row.get('date_utc','')}|{row.get('description','')}|{row.get('amount','')}|{row.get('balance','')}|{row.get('type','')}"
        import hashlib
        return hashlib.sha1(s.encode("utf-8")).hexdigest()

    async def _scrape_kasse(self, incremental: bool = True) -> int:
        await self._ensure_migrated()
        cfg = await self.config.all()
        url = cfg["kasse_url"]

        async def stop_after_seen(html: str) -> bool:
            if not incremental:
                return False
            headers, rows, _ = self._parse_table(html)
            idx = _header_index_map(headers, HEADER_MAP["kasse"])
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cur = await db.execute("SELECT date_utc FROM kasse_transactions ORDER BY date_utc DESC LIMIT 1")
                latest = await cur.fetchone()
                latest_dt = latest["date_utc"] if latest else None
            if latest_dt:
                for r in rows:
                    dt = r[idx.get("date", -1)] if idx.get("date", -1) >= 0 and idx.get("date") < len(r) else None
                    if dt and dt >= latest_dt:
                        return False
                return True
            return False

        async with self._locks["kasse"]:
            pages = await self._fetch_pages(url, cfg, stop_after_seen=stop_after_seen if incremental else None)
            inserted = 0
            async with aiosqlite.connect(self.db_path) as db:
                for html in pages:
                    headers, rows, _ = self._parse_table(html)
                    idx = _header_index_map(headers, HEADER_MAP["kasse"])
                    for r in rows:
                        date_s = r[idx.get("date", -1)] if idx.get("date", -1) >= 0 and idx.get("date") < len(r) else None
                        description = r[idx.get("description", -1)] if idx.get("description", -1) >= 0 and idx.get("description") < len(r) else None
                        amount = r[idx.get("amount", -1)] if idx.get("amount", -1) >= 0 and idx.get("amount") < len(r) else None
                        balance = r[idx.get("balance", -1)] if idx.get("balance", -1) >= 0 and idx.get("balance") < len(r) else None
                        type_ = r[idx.get("type", -1)] if idx.get("type", -1) >= 0 and idx.get("type") < len(r) else None
                        rowd = {"date_utc": date_s, "description": description, "amount": amount, "balance": balance, "type": type_}
                        hid = self._kasse_row_hash(rowd)
                        now = datetime.utcnow().isoformat()
                        try:
                            await db.execute("""
                            INSERT INTO kasse_transactions(id, date_utc, description, amount, balance, type, raw_hash, inserted_at_utc)
                            VALUES(?,?,?,?,?,?,?,?)
                            """, (hid, date_s, description, amount, balance, type_, hid, now))
                            inserted += 1
                        except Exception:
                            pass
                await db.commit()
        if inserted:
            try:
                self.bot.dispatch("fara_kasse_updated", {"rows": inserted, "ts": datetime.utcnow().isoformat()})
            except Exception:
                pass
        return inserted

    # ----------------- Background scheduling -----------------
    async def _maybe_start_background(self):
        await self.bot.wait_until_red_ready()
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._background_worker())

    async def _background_worker(self):
        while True:
            try:
                cfg = await self.config.all()
                asyncio.create_task(self._scrape_members())
                asyncio.create_task(self._scrape_logs(incremental=True))
                asyncio.create_task(self._scrape_schoolings())
                asyncio.create_task(self._scrape_kasse(incremental=True))
            except Exception as e:
                log.exception("Background worker error: %s", e)
            mins = min(cfg["interval_members"], cfg["interval_logs"], cfg["interval_schoolings"], cfg["interval_kasse"])
            await asyncio.sleep(max(60, mins * 60))

    # ----------------- Commands -----------------
    @commands.group(name="scraper")
    @checks.is_owner()
    async def scraper(self, ctx: commands.Context):
        """AllianceScraper controls (owner only)."""

    @scraper.command(name="fixdb")
    async def fixdb(self, ctx: commands.Context):
        """Run DB migration now (safe to run multiple times)."""
        await self._migrate_schema()
        self._migrated = True
        await ctx.send("DB migration completed.")

    @scraper.command(name="status")
    async def status(self, ctx: commands.Context):
        """Show scraper status and DB info counts."""
        await self._ensure_migrated()
        async with aiosqlite.connect(self.db_path) as db:
            counts = {}
            for table in ["members_current", "members_history", "alliance_logs", "schoolings_open", "kasse_transactions"]:
                cur = await db.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = (await cur.fetchone())[0]
        cfg = await self.config.all()
        lines = [
            f"members_current: {counts['members_current']}",
            f"members_history: {counts['members_history']}",
            f"alliance_logs: {counts['alliance_logs']}",
            f"schoolings_open: {counts['schoolings_open']}",
            f"kasse_transactions: {counts['kasse_transactions']}",
            "",
            f"members_url: {cfg['members_url']}",
            f"logs_url: {cfg['logs_url']}",
            f"schoolings_url: {cfg['schoolings_url']}",
            f"kasse_url: {cfg['kasse_url']}",
        ]
        await ctx.send("```\n" + "\n".join(lines) + "\n```")

    @scraper.command(name="run")
    async def run(self, ctx: commands.Context, target: str, mode: str = "inc"):
        """
        Run a scrape now. Targets: members, logs, schoolings, kasse
        Mode: inc (incremental) or full
        """
        await self._ensure_migrated()
        target = target.lower().strip()
        inc = (mode != "full")
        if target == "members":
            n = await self._scrape_members()
        elif target == "logs":
            n = await self._scrape_logs(incremental=inc)
        elif target == "schoolings":
            n = await self._scrape_schoolings()
        elif target == "kasse":
            n = await self._scrape_kasse(incremental=inc)
        else:
            await ctx.send("Unknown target. Use: members, logs, schoolings, kasse")
            return
        await ctx.send(f"Scraped {target}: {n} rows.")

    @scraper.command(name="export")
    async def export(self, ctx: commands.Context, dataset: str, fmt: str = "csv"):
        """
        Export a dataset as CSV or JSON.
        datasets: members_current, members_history, alliance_logs, schoolings_open, kasse_transactions
        """
        await self._ensure_migrated()
        import discord  # lazy import for file sending
        dataset = dataset.strip().lower()
        fmt = fmt.strip().lower()
        valid = ["members_current", "members_history", "alliance_logs", "schoolings_open", "kasse_transactions"]
        if dataset not in valid:
            await ctx.send("Unknown dataset.")
            return
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(f"SELECT * FROM {dataset}")
            rows = await cur.fetchall()
            rows = [dict(r) for r in rows]
        if fmt == "json":
            data = json.dumps(rows, ensure_ascii=False, indent=2)
            fp = io.BytesIO(data.encode("utf-8"))
            fp.seek(0)
            await ctx.send(file=discord.File(fp, filename=f"{dataset}.json"))
        else:
            if not rows:
                await ctx.send("No data.")
                return
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
            fp = io.BytesIO(output.getvalue().encode("utf-8"))
            fp.seek(0)
            await ctx.send(file=discord.File(fp, filename=f"{dataset}.csv"))

    @scraper.group(name="config")
    async def config_group(self, ctx: commands.Context):
        """Configure scraper settings."""

    @config_group.command(name="set")
    async def config_set(self, ctx: commands.Context, key: str, *, value: str):
        """
        Set a config value.
        Keys: members_url, logs_url, schoolings_url, kasse_url,
              interval_members, interval_logs, interval_schoolings, interval_kasse,
              per_request_delay_seconds, per_request_jitter_seconds,
              member_id_href_patterns (comma-separated regexes with one capture group)
        """
        key = key.strip().lower()
        ints = ["interval_members", "interval_logs", "interval_schoolings", "interval_kasse",
                "per_request_delay_seconds", "per_request_jitter_seconds"]
        if key in ints:
            try:
                v = int(value)
            except Exception:
                await ctx.send("Value must be an integer.")
                return
            await getattr(self.config, key).set(v)
            await ctx.send(f"Set {key} = {v}")
            return
        str_keys = ["members_url", "logs_url", "schoolings_url", "kasse_url"]
        if key in str_keys:
            await getattr(self.config, key).set(value)
            await ctx.send(f"Set {key}.")
            return
        if key == "member_id_href_patterns":
            pats = [p.strip() for p in value.split(",") if p.strip()]
            await self.config.member_id_href_patterns.set(pats)
            await ctx.send(f"Set member_id_href_patterns = {pats}")
            return
        await ctx.send("Unknown key.")

async def setup(bot):
    # Ensure DB is fully initialized BEFORE commands are available
    cog = AllianceScraper(bot)
    await cog._init_db()
    await bot.add_cog(cog)
    await cog._maybe_start_background()
