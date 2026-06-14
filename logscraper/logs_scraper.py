import discord
from redbot.core import commands, Config, data_manager
import asyncio
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import hashlib
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, Optional


class LogsScrapePageError(RuntimeError):
    """Raised when a required MissionChief logs page cannot be parsed."""


class LogsScraper(commands.Cog):
    """Scrapes alliance logs from MissionChief with complete data extraction"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0x4C4F47534352415045522056330A)
        self.config.register_global(
            debug_mode=False,
            scrape_interval=3600,  # 1 hour
            last_scrape=None,
            event_timezone="America/New_York",
        )
        
        # Database path
        data_path = data_manager.cog_data_path(raw_name="scraper_databases")
        data_path.mkdir(parents=True, exist_ok=True)
        self.db_path = data_path / "logs_v3.db"
        
        self.logs_url = "https://www.missionchief.com/alliance_logfiles"
        self.scrape_task = None
        
        # Initialize database
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
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
                scraped_at TEXT,
                event_timestamp TEXT,
                signature TEXT,
                occurrence_index INTEGER NOT NULL DEFAULT 1,
                contribution_amount INTEGER DEFAULT 0
            )
        ''')

        cursor.execute("PRAGMA table_info(logs)")
        columns = {column[1] for column in cursor.fetchall()}
        if "event_timestamp" not in columns:
            cursor.execute("ALTER TABLE logs ADD COLUMN event_timestamp TEXT")
        if "signature" not in columns:
            cursor.execute("ALTER TABLE logs ADD COLUMN signature TEXT")
        if "occurrence_index" not in columns:
            cursor.execute(
                "ALTER TABLE logs ADD COLUMN occurrence_index INTEGER NOT NULL DEFAULT 1"
            )
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(ts)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_hash ON logs(hash)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_action_key ON logs(action_key)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_executed_mc_id ON logs(executed_mc_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_event_timestamp ON logs(event_timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_signature ON logs(signature)')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS training_courses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_id INTEGER,
                course_name TEXT,
                username TEXT,
                timestamp TEXT,
                FOREIGN KEY (log_id) REFERENCES logs(id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    async def cog_load(self):
        """Start background scraping task"""
        self.scrape_task = self.bot.loop.create_task(self._background_scrape())
        await self._debug_log("✅ LogsScraper loaded - background task started")
    
    def cog_unload(self):
        """Cancel background task on unload"""
        if self.scrape_task:
            self.scrape_task.cancel()

    @asynccontextmanager
    async def _bot_status(self, detail, *, priority=75):
        bot = getattr(self, "bot", None)
        botstatus = bot.get_cog("BotStatus") if bot else None
        if botstatus and hasattr(botstatus, "track_activity"):
            async with botstatus.track_activity("LogsScraper", detail, priority=priority):
                yield
        else:
            yield

    async def _report_bot_status(self, detail, *, priority=80, ttl_seconds=120):
        bot = getattr(self, "bot", None)
        botstatus = bot.get_cog("BotStatus") if bot else None
        if botstatus and hasattr(botstatus, "report_activity"):
            await botstatus.report_activity(
                "LogsScraper",
                detail,
                priority=priority,
                ttl_seconds=ttl_seconds,
            )

    @staticmethod
    def _build_member_identity_filter(
        *,
        mc_user_id: Optional[str],
        mc_username: Optional[str],
    ) -> tuple[str, list[str]]:
        """Build a WHERE clause for matching stored log rows to a member."""
        where_parts = []
        params = []

        if mc_user_id:
            where_parts.append("(executed_mc_id = ? OR affected_mc_id = ?)")
            params.extend([str(mc_user_id), str(mc_user_id)])

        clean_username = mc_username
        if clean_username and "Former member" in clean_username:
            clean_username = None

        if clean_username:
            where_parts.append("(executed_name = ? OR affected_name = ?)")
            params.extend([clean_username, clean_username])

        if not where_parts:
            return "", []

        return " OR ".join(where_parts), params

    def _query_member_logs_sync(
        self,
        *,
        mc_user_id: Optional[str],
        mc_username: Optional[str],
        action_keys: Optional[Iterable[str]] = None,
        limit: int = 250,
        offset: int = 0,
        include_total: bool = False,
    ) -> Dict[str, Any]:
        """Return stored LogsScraper rows for a MissionChief member."""
        where_clause, params = self._build_member_identity_filter(
            mc_user_id=mc_user_id,
            mc_username=mc_username,
        )
        if not where_clause:
            return {"rows": [], "total": 0 if include_total else None}

        action_key_list = sorted({str(key) for key in (action_keys or [])})
        query_params = list(params)
        action_filter = ""
        if action_key_list:
            placeholders = ", ".join("?" for _ in action_key_list)
            action_filter = f" AND action_key IN ({placeholders})"
            query_params.extend(action_key_list)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = [
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT id, ts, event_timestamp, action_key, action_text,
                           executed_name, executed_mc_id, affected_name, affected_mc_id,
                           description, occurrence_index, contribution_amount
                    FROM logs
                    WHERE ({where_clause}){action_filter}
                    ORDER BY COALESCE(event_timestamp, ts) DESC, id DESC
                    LIMIT ? OFFSET ?
                    """,
                    [*query_params, int(limit), int(offset)],
                ).fetchall()
            ]

            total = None
            if include_total:
                total = conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM logs
                    WHERE ({where_clause}){action_filter}
                    """,
                    query_params,
                ).fetchone()[0]

            return {"rows": rows, "total": total}
        finally:
            conn.close()

    async def get_member_logs(
        self,
        *,
        mc_user_id: Optional[str],
        mc_username: Optional[str],
        action_keys: Optional[Iterable[str]] = None,
        limit: int = 250,
        offset: int = 0,
        include_total: bool = False,
    ) -> Dict[str, Any]:
        """Public API: return stored MissionChief log rows for one member."""
        return await asyncio.to_thread(
            self._query_member_logs_sync,
            mc_user_id=mc_user_id,
            mc_username=mc_username,
            action_keys=action_keys,
            limit=limit,
            offset=offset,
            include_total=include_total,
        )

    @staticmethod
    def _next_scrape_time(now):
        """Return the next hourly :15 scrape time."""
        next_run = now.replace(minute=15, second=0, microsecond=0)
        if now >= next_run:
            next_run += timedelta(hours=1)
        return next_run

    @staticmethod
    def _normalize_event_timestamp(raw_timestamp, scraped_at, event_timezone):
        """Normalize a MissionChief alliance-log timestamp to UTC."""
        if scraped_at.tzinfo is None:
            raise ValueError("scraped_at must be timezone-aware")

        try:
            event_tz = ZoneInfo(event_timezone)
        except (TypeError, ValueError, KeyError):
            return None

        try:
            parsed = datetime.strptime(raw_timestamp, "%B %d, %Y %H:%M")
            return parsed.replace(tzinfo=event_tz).astimezone(ZoneInfo("UTC")).isoformat()
        except (TypeError, ValueError):
            pass

        try:
            parsed = datetime.strptime(raw_timestamp, "%d %b %H:%M")
        except (TypeError, ValueError):
            return None

        scraped_local = scraped_at.astimezone(event_tz)
        candidate = parsed.replace(year=scraped_local.year, tzinfo=event_tz)
        if candidate > scraped_local + timedelta(minutes=5):
            candidate = candidate.replace(year=candidate.year - 1)

        age = scraped_local - candidate
        if age < timedelta(minutes=-5) or age > timedelta(days=7):
            return None

        return candidate.astimezone(ZoneInfo("UTC")).isoformat()

    @staticmethod
    def _assign_occurrence_hashes(logs):
        """Assign stable hashes without collapsing identical visible rows."""
        occurrences = {}
        for log_entry in logs:
            signature = log_entry.get("signature") or log_entry["hash"]
            occurrence = occurrences.get(signature, 0) + 1
            occurrences[signature] = occurrence
            log_entry["signature"] = signature
            log_entry["occurrence_index"] = occurrence
            if occurrence == 1:
                log_entry["hash"] = signature
            else:
                log_entry["hash"] = hashlib.sha256(
                    f"{signature}:{occurrence}".encode()
                ).hexdigest()
        return logs
    
    async def _background_scrape(self):
        """Background task - scrapes every hour at :15"""
        await self.bot.wait_until_ready()
        
        while True:
            try:
                # Wait until next :15 mark
                now = datetime.now()
                next_run = self._next_scrape_time(now)
                
                wait_seconds = (next_run - now).total_seconds()
                await self._debug_log(f"⏰ Next auto-scrape: {next_run.strftime('%H:%M')}")
                await asyncio.sleep(wait_seconds)
                
                # Run scrape
                await self._debug_log("🔄 Auto-scraping logs...")
                await self._scrape_all_logs(None, max_pages=5)
                
            except asyncio.CancelledError:
                await self._debug_log("❌ Background scrape task cancelled")
                break
            except Exception as e:
                await self._debug_log(f"Background scrape error: {e}")
                await asyncio.sleep(300)  # Wait 5 min on error
    
    async def _debug_log(self, message, ctx=None):
        """Send debug message if debug mode is on"""
        if await self.config.debug_mode():
            if ctx:
                await ctx.send(f"🔍 {message}")
            else:
                print(f"[LogsScraper] {message}")
    
    async def _get_session(self, ctx=None):
        """Get authenticated session from CookieManager"""
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager:
            await self._debug_log("❌ CookieManager cog not loaded!", ctx)
            return None
        
        try:
            session = await cookie_manager.get_session()
            await self._debug_log("✅ Session obtained", ctx)
            return session
        except Exception as e:
            await self._debug_log(f"❌ Failed to get session: {e}", ctx)
            return None
    
    async def _scrape_logs_page(self, session, page_num, ctx=None):
        """Scrape a single page of logs with COMPLETE data extraction"""
        if page_num == 1 or page_num % 10 == 0:
            await self._report_bot_status(f"scraping alliance logs page {page_num}")
        url = f"{self.logs_url}?page={page_num}"
        await self._debug_log(f"🌐 Page {page_num}: {url}", ctx)
        
        try:
            async with session.get(url) as response:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Find the logs table
                table = soup.find('table', class_='table')
                if not table:
                    await self._debug_log(f"⚠️ No table found on page {page_num}", ctx)
                    if page_num == 1:
                        raise LogsScrapePageError(
                            "No alliance log table found on page 1; "
                            "the MissionChief session may be logged out or the page layout changed"
                        )
                    return []
                
                tbody = table.find('tbody')
                if not tbody:
                    await self._debug_log(f"⚠️ No tbody found on page {page_num}", ctx)
                    if page_num == 1:
                        raise LogsScrapePageError(
                            "No alliance log rows found on page 1; "
                            "the MissionChief session may be logged out or the page layout changed"
                        )
                    return []
                
                rows = tbody.find_all('tr')
                logs = []
                
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) < 3:
                        continue
                    
                    # Column 0: Timestamp
                    timestamp = cols[0].get_text(strip=True)
                    
                    # Column 1: Executed by (username + MC ID)
                    user_link = cols[1].find('a', href=True)
                    executed_name = user_link.get_text(strip=True) if user_link else ""
                    executed_url = ""
                    executed_mc_id = ""
                    
                    if user_link and user_link.get('href'):
                        href = user_link['href']
                        executed_url = f"https://www.missionchief.com{href}"
                        # Extract MC ID from /users/123456 OR /profile/123456
                        match = re.search(r'/(users|profile)/(\d+)', href)
                        if match:
                            executed_mc_id = match.group(2)
                    
                    # Column 2: Description + contribution amount
                    desc_col = cols[2]
                    description = desc_col.get_text(strip=True)
                    
                    # Extract action key from description (NO icon in HTML!)
                    description_text = desc_col.get_text(strip=True).lower()
                    action_key = "unknown"
                    
                    # Map description to action_key
                    if "added to the alliance" in description_text:
                        action_key = "added_to_alliance"
                    elif "application denied" in description_text:
                        action_key = "application_denied"
                    elif "left the alliance" in description_text:
                        action_key = "left_alliance"
                    elif "kicked from the alliance" in description_text:
                        action_key = "kicked_from_alliance"
                    elif "set as transport admin" in description_text or "transport admin set" in description_text:
                        action_key = "set_transport_admin"
                    elif "removed transport admin" in description_text:
                        action_key = "removed_transport_admin"
                    elif "removed admin" in description_text and "co-admin" not in description_text:
                        action_key = "removed_admin"
                    elif "set as admin" in description_text or "promoted to admin" in description_text:
                        action_key = "set_admin"
                    elif "removed education admin" in description_text:
                        action_key = "removed_education_admin"
                    elif "set as education admin" in description_text:
                        action_key = "set_education_admin"
                    elif "set as finance admin" in description_text:
                        action_key = "set_finance_admin"
                    elif "removed finance admin" in description_text:
                        action_key = "removed_finance_admin"
                    elif "set as co-admin" in description_text or "promoted to co-admin" in description_text:
                        action_key = "set_co_admin"
                    elif "removed co-admin" in description_text:
                        action_key = "removed_co_admin"
                    elif "set as mod action admin" in description_text:
                        action_key = "set_mod_action_admin"
                    elif "removed mod action admin" in description_text:
                        action_key = "removed_mod_action_admin"
                    elif "chat ban removed" in description_text:
                        action_key = "chat_ban_removed"
                    elif "chat ban set" in description_text:
                        action_key = "chat_ban_set"
                    elif "allowed to apply" in description_text:
                        action_key = "allowed_to_apply"
                    elif "not allowed to apply" in description_text:
                        action_key = "not_allowed_to_apply"
                    elif "created a course" in description_text or "created course" in description_text:
                        action_key = "created_course"
                    elif "course completed" in description_text or "completed a course" in description_text:
                        action_key = "course_completed"
                    elif "building destroyed" in description_text:
                        action_key = "building_destroyed"
                    elif "building constructed" in description_text:
                        action_key = "building_constructed"
                    elif "extension started" in description_text:
                        action_key = "extension_started"
                    elif "expansion finished" in description_text:
                        action_key = "expansion_finished"
                    elif "large scale mission started" in description_text or "large mission started" in description_text:
                        action_key = "large_mission_started"
                    elif "alliance event started" in description_text:
                        action_key = "alliance_event_started"
                    elif "set as staff" in description_text:
                        action_key = "set_as_staff"
                    elif "removed as staff" in description_text:
                        action_key = "removed_as_staff"
                    elif "removed event manager" in description_text:
                        action_key = "removed_event_manager"
                    elif "removed custom large scale mission" in description_text:
                        action_key = "removed_custom_large_scale_mission"
                    elif "promoted to event manager" in description_text:
                        action_key = "promoted_to_event_manager"
                    elif "contributed to the alliance" in description_text or "contribution" in description_text:
                        action_key = "contributed_to_alliance"
                    
                    # Extract contribution amount from <span class="label">
                    contribution_amount = 0
                    label = desc_col.find('span', class_='label')
                    if label:
                        label_text = label.get_text(strip=True)
                        # Extract number from "-500 Credits" or "+1000 Credits"
                        match = re.search(r'([-+]?\d+)', label_text)
                        if match:
                            contribution_amount = int(match.group(1))
                        # Remove label from description
                        description = desc_col.get_text(strip=True).replace(label_text, '').strip()
                    
                    # Column 3: Affected (building/user/etc)
                    affected_name = ""
                    affected_url = ""
                    affected_mc_id = ""
                    affected_type = ""
                    
                    if len(cols) > 3:
                        affected_link = cols[3].find('a', href=True)
                        if affected_link:
                            affected_name = affected_link.get_text(strip=True)
                            affected_url = f"https://www.missionchief.com{affected_link['href']}"
                            
                            # Determine type from URL
                            if '/buildings/' in affected_link['href']:
                                affected_type = "building"
                                match = re.search(r'/buildings/(\d+)', affected_link['href'])
                                if match:
                                    affected_mc_id = match.group(1)
                            elif '/users/' in affected_link['href'] or '/profile/' in affected_link['href']:
                                affected_type = "user"
                                match = re.search(r'/(users|profile)/(\d+)', affected_link['href'])
                                if match:
                                    affected_mc_id = match.group(2)
                            elif '/missions/' in affected_link['href']:
                                affected_type = "mission"
                                match = re.search(r'/missions/(\d+)', affected_link['href'])
                                if match:
                                    affected_mc_id = match.group(1)
                            elif '/vehicles/' in affected_link['href']:
                                affected_type = "vehicle"
                                match = re.search(r'/vehicles/(\d+)', affected_link['href'])
                                if match:
                                    affected_mc_id = match.group(1)
                    
                    # Generate unique hash for deduplication (like old scraper)
                    hash_string = f"{timestamp}{action_key}{executed_name}{affected_name}{description}"
                    log_hash = hashlib.sha256(hash_string.encode()).hexdigest()
                    
                    logs.append({
                        'hash': log_hash,
                        'signature': log_hash,
                        'ts': timestamp,
                        'action_key': action_key,
                        'action_text': description,
                        'executed_name': executed_name,
                        'executed_mc_id': executed_mc_id,
                        'executed_url': executed_url,
                        'affected_name': affected_name,
                        'affected_type': affected_type,
                        'affected_mc_id': affected_mc_id,
                        'affected_url': affected_url,
                        'description': description,
                        'contribution_amount': contribution_amount
                    })
                
                await self._debug_log(f"✅ Page {page_num}: {len(logs)} logs", ctx)
                return logs
                
        except Exception as e:
            if isinstance(e, LogsScrapePageError):
                raise
            await self._debug_log(f"❌ Page {page_num} error: {e}", ctx)
            return []
    
    async def _scrape_all_logs(self, ctx, max_pages=5):
        detail = f"scraping alliance logs ({max_pages} pages)"
        async with self._bot_status(detail):
            return await self._scrape_all_logs_impl(ctx, max_pages)

    async def _scrape_all_logs_impl(self, ctx, max_pages=5):
        """Scrape multiple pages of logs"""
        session = await self._get_session(ctx)
        if not session:
            return False
        
        await self._debug_log(f"🔄 Starting logs scrape (max {max_pages} pages)", ctx)
        
        all_logs = []
        try:
            for page in range(1, max_pages + 1):
                logs = await self._scrape_logs_page(session, page, ctx)
                all_logs.extend(logs)

                # Progress update every 10 pages
                if page % 10 == 0:
                    await self._debug_log(f"Progress: {page}/{max_pages} pages, {len(all_logs)} logs collected", ctx)

                await asyncio.sleep(1.5)  # Rate limiting
        except LogsScrapePageError as exc:
            await self._debug_log(f"Logs scrape failed: {exc}", ctx)
            if ctx:
                await ctx.send(f"Logs scrape failed: {exc}")
            return False

        self._assign_occurrence_hashes(all_logs)
        
        # Store in database
        scraped_at_dt = datetime.now(ZoneInfo("UTC"))
        scraped_at = scraped_at_dt.isoformat()
        event_timezone = await self.config.event_timezone()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        inserted = 0
        duplicates = 0
        training_inserted = 0
        
        for log in all_logs:
            event_timestamp = self._normalize_event_timestamp(
                log["ts"],
                scraped_at_dt,
                event_timezone,
            )
            try:
                cursor.execute('''
                    INSERT INTO logs (hash, ts, action_key, action_text, executed_name,
                                    executed_mc_id, executed_url, affected_name, affected_type,
                                    affected_mc_id, affected_url, description, scraped_at,
                                    event_timestamp, signature, occurrence_index,
                                    contribution_amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (log['hash'], log['ts'], log['action_key'], log['action_text'], log['executed_name'],
                      log['executed_mc_id'], log['executed_url'], log['affected_name'], log['affected_type'],
                      log['affected_mc_id'], log['affected_url'], log['description'], scraped_at,
                      event_timestamp, log['signature'], log['occurrence_index'],
                      log['contribution_amount']))
                inserted += 1
                
                # Count training courses (don't insert separately - data is in logs table)
                if log['action_key'] in ['created_course', 'course_completed']:
                    training_inserted += 1
                    
            except sqlite3.IntegrityError:
                if event_timestamp:
                    cursor.execute(
                        """
                        UPDATE logs SET event_timestamp = ?
                        WHERE hash = ? AND event_timestamp IS NULL
                        """,
                        (event_timestamp, log["hash"]),
                    )
                duplicates += 1
        
        conn.commit()
        conn.close()
        
        await self._debug_log(f"💾 Database: {inserted} new logs, {training_inserted} training courses, {duplicates} duplicates", ctx)
        
        if ctx:
            await ctx.send(f"✅ Scraped {len(all_logs)} logs\n"
                          f"📊 {inserted} new logs, {training_inserted} training courses, {duplicates} duplicates")
        
        await self.config.last_scrape.set(datetime.now().isoformat())
        
        return True
    
    async def get_logs_after(self, last_id: int, limit: int = 50):
        """Get logs after a specific ID - for alliance_logs_pub compatibility
        Returns data in exact format that AllianceLogsPub expects"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                id,
                hash,
                ts,
                action_key,
                action_text,
                executed_name,
                executed_mc_id,
                executed_url,
                affected_name,
                affected_type,
                affected_mc_id,
                affected_url,
                description,
                contribution_amount
            FROM logs
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
        ''', (last_id, limit))
        
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return rows

    async def get_recent_logs(self, limit: int = 100):
        """Get the most recent stored logs in ascending ID order."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT
                id,
                hash,
                ts,
                action_key,
                action_text,
                executed_name,
                executed_mc_id,
                executed_url,
                affected_name,
                affected_type,
                affected_mc_id,
                affected_url,
                description,
                contribution_amount
            FROM logs
            ORDER BY id DESC
            LIMIT ?
        ''', (limit,))

        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return list(reversed(rows))
    
    # ==================== COMMANDS ====================
    
    @commands.group(name="logs")
    @commands.admin_or_permissions(administrator=True)
    async def logs_group(self, ctx):
        """Alliance logs scraper commands"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @logs_group.command(name="scrape")
    async def scrape_logs(self, ctx, max_pages: int = 5):
        """Manually scrape logs (max 100 pages)"""
        if max_pages < 1 or max_pages > 100:
            await ctx.send("❌ Pages must be between 1 and 100")
            return
        
        await ctx.send(f"🔄 Starting logs scrape (max {max_pages} pages)...")
        success = await self._scrape_all_logs(ctx, max_pages)
        
        if success:
            await ctx.send("✅ Logs scrape completed")
        else:
            await ctx.send("❌ Scrape failed - check CookieManager")
    
    @logs_group.command(name="backfill")
    async def backfill_logs(self, ctx, max_pages: int = 100):
        """Backfill historical logs (use with caution - can take a while!)"""
        if max_pages < 1 or max_pages > 500:
            await ctx.send("❌ Pages must be between 1 and 500 for backfill")
            return
        
        await ctx.send(f"⚠️ Starting backfill of {max_pages} pages (~{max_pages * 1.5 / 60:.1f} minutes)...")
        success = await self._scrape_all_logs(ctx, max_pages)
        
        if success:
            await ctx.send("✅ Backfill completed!")
        else:
            await ctx.send("❌ Backfill failed")
    
    @logs_group.command(name="stats")
    async def show_stats(self, ctx):
        """Show logs statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM logs")
        total_logs = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM logs WHERE action_key IN ('created_course', 'course_completed')")
        total_courses = cursor.fetchone()[0]
        
        cursor.execute("SELECT MAX(id) FROM logs")
        max_id = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(ts), MAX(ts) FROM logs")
        date_range = cursor.fetchone()

        cursor.execute("SELECT COUNT(*) FROM logs WHERE event_timestamp IS NOT NULL")
        timestamped_logs = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM logs WHERE occurrence_index > 1")
        repeated_logs = cursor.fetchone()[0]

        cursor.execute("SELECT MAX(event_timestamp) FROM logs")
        latest_event_timestamp = cursor.fetchone()[0]

        cursor.execute("SELECT MAX(scraped_at) FROM logs")
        latest_scraped_at = cursor.fetchone()[0]
        
        conn.close()
        
        embed = discord.Embed(title="📊 Logs Statistics", color=discord.Color.blue())
        embed.add_field(name="Total Logs", value=f"{total_logs:,}", inline=True)
        embed.add_field(name="Training Courses", value=f"{total_courses:,}", inline=True)
        embed.add_field(name="Max Log ID", value=f"{max_id or 0:,}", inline=True)
        embed.add_field(
            name="Event Timestamps",
            value=f"{timestamped_logs:,} / {total_logs:,}",
            inline=True,
        )
        embed.add_field(
            name="Repeated Actions Preserved",
            value=f"{repeated_logs:,}",
            inline=True,
        )
        embed.add_field(
            name="Latest Event (UTC)",
            value=latest_event_timestamp or "None",
            inline=False,
        )
        embed.add_field(
            name="Latest Stored Scrape (UTC)",
            value=latest_scraped_at or "None",
            inline=False,
        )
        embed.add_field(
            name="Source Data Range",
            value=(
                f"{date_range[0]} to {date_range[1]}"
                if date_range[0] and date_range[1]
                else "None"
            ),
            inline=False,
        )
        embed.set_footer(text=f"Database: {self.db_path.name}")
        
        await ctx.send(embed=embed)
    
    @logs_group.command(name="debug")
    async def toggle_debug(self, ctx, mode: str = None):
        """Toggle debug mode (on/off)"""
        if mode:
            new_state = mode.lower() == "on"
            await self.config.debug_mode.set(new_state)
        else:
            current = await self.config.debug_mode()
            await self.config.debug_mode.set(not current)
            new_state = not current
        
        await ctx.send(f"🔍 Debug mode: {'ON' if new_state else 'OFF'}")
    
    @logs_group.command(name="timezone")
    async def event_timezone(self, ctx, timezone: str = None):
        """Show or set the timezone used by MissionChief alliance-log timestamps."""
        if timezone is None:
            current = await self.config.event_timezone()
            await ctx.send(f"Log event timezone: {current or 'Not configured'}")
            return

        try:
            ZoneInfo(timezone)
        except (ValueError, KeyError):
            await ctx.send(f"Invalid timezone: {timezone}")
            return

        await self.config.event_timezone.set(timezone)
        await ctx.send(f"Log event timezone set to {timezone}")

    @logs_group.command(name="taskstatus")
    async def task_status(self, ctx):
        """Check if background scraping task is running"""
        if self.scrape_task is None:
            await ctx.send("❌ Background task is NOT running!")
        elif self.scrape_task.done():
            await ctx.send("⚠️ Background task exists but is DONE (crashed or completed)")
            try:
                exc = self.scrape_task.exception()
                if exc:
                    await ctx.send(f"💥 Task exception: {exc}")
            except Exception:
                pass
        elif self.scrape_task.cancelled():
            await ctx.send("⚠️ Background task was CANCELLED")
        else:
            await ctx.send("✅ Background task is running")
            last_scrape = await self.config.last_scrape()
            if last_scrape:
                await ctx.send(f"📅 Last scrape: {last_scrape}")
    
    @logs_group.command(name="restarttask")
    async def restart_task(self, ctx):
        """Restart the background scraping task"""
        # Cancel old task if exists
        if self.scrape_task and not self.scrape_task.done():
            self.scrape_task.cancel()
            await ctx.send("🛑 Cancelled old task")
            await asyncio.sleep(1)
        
        # Start new task
        self.scrape_task = self.bot.loop.create_task(self._background_scrape())
        await ctx.send("✅ Background scraping task restarted!")
        await self._debug_log("✅ Background task manually restarted", ctx)

async def setup(bot):
    await bot.add_cog(LogsScraper(bot))
