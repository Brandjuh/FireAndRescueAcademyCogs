import discord
from redbot.core import commands, Config, data_manager
import asyncio
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import random
import re
import logging
from typing import Any, Dict, List, Optional

try:
    from .fara_db import (
        backup_database,
        connect_database,
        ensure_scrape_runs_table,
        finish_scrape_run,
        start_scrape_run,
    )
except ImportError:  # pragma: no cover - direct module loading in local tooling
    from fara_db import (
        backup_database,
        connect_database,
        ensure_scrape_runs_table,
        finish_scrape_run,
        start_scrape_run,
    )

log = logging.getLogger("red.FARA.MembersScraper")

# SQLite INTEGER limits
INT64_MAX = 9223372036854775807
INT64_MIN = -9223372036854775808
MIN_EXIT_DETECTION_BASELINE = 100
MIN_EXIT_DETECTION_RETENTION = 0.50
PAGE_REQUEST_BASE_DELAY_SECONDS = 1.5
PAGE_REQUEST_MAX_ATTEMPTS = 3
PAGE_REQUEST_BACKOFF_CAP_SECONDS = 60.0
FULL_MEMBER_SCRAPE_TIMEOUT_SECONDS = 30 * 60

class MembersScraper(commands.Cog):
    """Scrapes alliance members data from MissionChief"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621001, force_registration=True)
        
        # Default config including log channel for exit notifications
        default_global = {
            "exit_log_channel_id": None,
        }
        self.config.register_global(**default_global)
        
        # Setup database path in shared location
        base_path = data_manager.cog_data_path(raw_name="scraper_databases")
        base_path.mkdir(parents=True, exist_ok=True)
        self.db_path = str(base_path / "members_v2.db")
        
        # Also get access to MemberSync database for exit tracking
        membersync_path = data_manager.cog_data_path(raw_name="MemberSync")
        membersync_path.mkdir(parents=True, exist_ok=True)
        self.membersync_db = str(membersync_path / "membersync.db")
        
        self.base_url = "https://www.missionchief.com"
        self.members_url = f"{self.base_url}/verband/mitglieder/1621"
        self.scraping_task = None
        self.last_auto_scrape_started_at = None
        self.last_auto_scrape_finished_at = None
        self.last_auto_scrape_status = "never"
        self.last_auto_scrape_error = None
        self.current_scrape_run_id = None
        self.current_scrape_page = None
        self.last_scrape_page_finished = None
        self.last_scrape_page_finished_at = None
        self._scrape_lock = asyncio.Lock()
        self.debug_mode = False
        self.debug_channel = None
        self._init_database()
        
    def cog_load(self):
        """Start background task when cog loads"""
        self._ensure_background_task()
        log.info("MembersScraper loaded - WITH exit detection")
        
    def cog_unload(self):
        """Cancel background task when cog unloads"""
        if self.scraping_task:
            self.scraping_task.cancel()

    def _task_is_running(self, task=None) -> bool:
        task = self.scraping_task if task is None else task
        if task is None:
            return False
        try:
            return not task.cancelled() and not task.done()
        except Exception:
            return False

    def _ensure_background_task(self) -> bool:
        """Start the hourly background task if it is missing or stopped."""
        if self._task_is_running():
            return False
        self.scraping_task = self.bot.loop.create_task(self._background_scraper())
        return True

    def _format_elapsed_since(self, value) -> Optional[str]:
        if not value:
            return None
        try:
            started = datetime.fromisoformat(str(value))
        except ValueError:
            return None
        now = datetime.now(started.tzinfo) if started.tzinfo else datetime.utcnow()
        seconds = max(0, int((now - started).total_seconds()))
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            return f"{hours}h {minutes}m {seconds}s"
        if minutes:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    @asynccontextmanager
    async def _bot_status(self, detail, *, priority=80):
        bot = getattr(self, "bot", None)
        botstatus = bot.get_cog("BotStatus") if bot else None
        if botstatus and hasattr(botstatus, "track_activity"):
            async with botstatus.track_activity("MembersScraper", detail, priority=priority):
                yield
        else:
            yield

    async def _report_bot_status(self, detail, *, priority=85, ttl_seconds=120):
        bot = getattr(self, "bot", None)
        botstatus = bot.get_cog("BotStatus") if bot else None
        if botstatus and hasattr(botstatus, "report_activity"):
            await botstatus.report_activity(
                "MembersScraper",
                detail,
                priority=priority,
                ttl_seconds=ttl_seconds,
            )
    
    def _init_database(self):
        """Initialize SQLite database with schema"""
        import time
        
        # Retry logic for locked database
        max_retries = 5
        for attempt in range(max_retries):
            try:
                conn = connect_database(self.db_path, timeout=10.0)
                cursor = conn.cursor()
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS members (
                        member_id INTEGER,
                        username TEXT,
                        rank TEXT,
                        earned_credits INTEGER,
                        contribution_rate REAL DEFAULT 0.0,
                        online_status TEXT,
                        timestamp TEXT,
                        snapshot_source TEXT DEFAULT 'unknown',
                        PRIMARY KEY (member_id, timestamp)
                    )
                ''')
                
                # Auto-migration: add contribution_rate if not exists
                cursor.execute("PRAGMA table_info(members)")
                columns = [col[1] for col in cursor.fetchall()]
                
                if 'contribution_rate' not in columns:
                    backup_database(self.db_path, "add-members-contribution-rate", logger=log)
                    log.info("🔧 MIGRATION: Adding contribution_rate column")
                    cursor.execute('ALTER TABLE members ADD COLUMN contribution_rate REAL DEFAULT 0.0')
                    log.info("✅ Migration complete")
                
                if 'snapshot_source' not in columns:
                    backup_database(self.db_path, "add-members-snapshot-source", logger=log)
                    log.info("MIGRATION: Adding snapshot_source column")
                    cursor.execute("ALTER TABLE members ADD COLUMN snapshot_source TEXT DEFAULT 'unknown'")
                    log.info("Migration complete")

                cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON members(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_member_id ON members(member_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_contribution_rate ON members(contribution_rate)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_snapshot_source ON members(snapshot_source)')
                ensure_scrape_runs_table(cursor)
                
                # Suspicious members table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS suspicious_members (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        member_id INTEGER,
                        username TEXT,
                        rank TEXT,
                        parsed_credits INTEGER,
                        raw_html TEXT,
                        reason TEXT,
                        timestamp TEXT
                    )
                ''')
                
                # CRITICAL FIX: MemberSync compatibility VIEW
                # OLD BROKEN VIEW used DATE(timestamp) which gets ALL scrapes from today
                # NEW FIXED VIEW uses MAX(timestamp) to get ONLY the latest scrape
                cursor.execute('DROP VIEW IF EXISTS members_current')
                cursor.execute('''
                    CREATE VIEW members_current AS
                    SELECT 
                        member_id as user_id,
                        member_id as mc_user_id,
                        username as name,
                        rank as role,
                        earned_credits,
                        contribution_rate,
                        '' as profile_href,
                        timestamp as scraped_at
                    FROM members
                    WHERE timestamp = COALESCE(
                        (
                            SELECT source_timestamp
                            FROM scrape_runs
                            WHERE scraper = 'members'
                              AND source = 'live'
                              AND status = 'success'
                              AND source_timestamp IS NOT NULL
                            ORDER BY finished_at DESC, run_id DESC
                            LIMIT 1
                        ),
                        (SELECT MAX(timestamp) FROM members WHERE snapshot_source = 'live'),
                        (SELECT MAX(timestamp) FROM members)
                    )
                ''')
                log.info("✅ MemberSync VIEW created (using MAX(timestamp) for latest scrape only)")
                
                conn.commit()
                conn.close()
                break
            except sqlite3.OperationalError:
                if attempt < max_retries - 1:
                    print(f"[MembersScraper] Database locked, retrying... ({attempt + 1}/{max_retries})")
                    time.sleep(0.5)
                else:
                    print(f"[MembersScraper] Failed to initialize database after {max_retries} attempts")
                    raise
        
        # Ensure member_left_alliance table exists in MemberSync DB
        self._init_membersync_exit_table()
    
    def _init_membersync_exit_table(self):
        """Ensure the exit tracking table exists in MemberSync database"""
        try:
            conn = connect_database(self.membersync_db, timeout=10.0)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS member_left_alliance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mc_user_id TEXT NOT NULL,
                    username TEXT,
                    discord_id INTEGER,
                    rank_role TEXT,
                    earned_credits INTEGER,
                    contribution_rate REAL,
                    last_seen_at TEXT,
                    exit_detected_at TEXT NOT NULL,
                    reason TEXT DEFAULT 'auto-detected',
                    role_removed INTEGER DEFAULT 0,
                    notified INTEGER DEFAULT 0
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_exit_mc ON member_left_alliance(mc_user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_exit_discord ON member_left_alliance(discord_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_exit_detected ON member_left_alliance(exit_detected_at)')
            
            conn.commit()
            conn.close()
            log.info("✅ MemberSync exit table verified")
        except Exception as e:
            log.error(f"Failed to init MemberSync exit table: {e}")
    
    async def _debug_log(self, message, ctx=None, *, discord=True):
        """Log debug messages to console AND Discord"""
        print(f"[MembersScraper DEBUG] {message}")

        if not discord:
            return
        
        if self.debug_mode and (ctx or self.debug_channel):
            try:
                channel = ctx.channel if ctx else self.debug_channel
                if channel:
                    await channel.send(f"🐛 `{message}`")
            except Exception as e:
                print(f"[MembersScraper DEBUG] Failed to send to Discord: {e}")

    def _get_scrape_lock(self):
        if not hasattr(self, "_scrape_lock") or self._scrape_lock is None:
            self._scrape_lock = asyncio.Lock()
        return self._scrape_lock

    def _start_scrape_run(self, source: str, source_timestamp: str) -> Optional[int]:
        if not getattr(self, "db_path", None):
            return None
        try:
            conn = connect_database(self.db_path)
            try:
                return start_scrape_run(
                    conn,
                    "members",
                    source=source,
                    source_timestamp=source_timestamp,
                )
            finally:
                conn.close()
        except Exception:
            log.exception("Failed to start members scrape run")
            return None

    def _finish_scrape_run(
        self,
        run_id: Optional[int],
        status: str,
        *,
        pages_attempted: int = 0,
        pages_succeeded: int = 0,
        rows_parsed: int = 0,
        rows_inserted: int = 0,
        duplicates: int = 0,
        errors: int = 0,
        message: Optional[str] = None,
    ) -> None:
        if run_id is None or not getattr(self, "db_path", None):
            return
        try:
            conn = connect_database(self.db_path)
            try:
                finish_scrape_run(
                    conn,
                    run_id,
                    status,
                    pages_attempted=pages_attempted,
                    pages_succeeded=pages_succeeded,
                    rows_parsed=rows_parsed,
                    rows_inserted=rows_inserted,
                    duplicates=duplicates,
                    errors=errors,
                    message=message,
                )
            finally:
                conn.close()
        except Exception:
            log.exception("Failed to finish members scrape run")

    def _retry_after_seconds(self, response) -> Optional[float]:
        headers = getattr(response, "headers", {}) or {}
        retry_after = None
        if hasattr(headers, "get"):
            retry_after = headers.get("Retry-After") or headers.get("retry-after")

        if not retry_after:
            return None

        try:
            return max(0.0, min(float(retry_after), PAGE_REQUEST_BACKOFF_CAP_SECONDS))
        except (TypeError, ValueError):
            return None

    def _page_retry_delay(self, response, attempt: int) -> Optional[float]:
        status = getattr(response, "status", None)
        if status == 429:
            retry_after = self._retry_after_seconds(response)
            if retry_after is not None:
                return retry_after
            return min(15.0 * (attempt + 1), PAGE_REQUEST_BACKOFF_CAP_SECONDS)

        if status is not None and 500 <= int(status) < 600:
            return min(5.0 * (2 ** attempt), PAGE_REQUEST_BACKOFF_CAP_SECONDS)

        return None

    def _query_member_snapshot_sync(self, mc_user_id: str) -> Optional[Dict[str, Any]]:
        """Return the latest stored member snapshot for a MissionChief user."""
        try:
            conn = connect_database(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(members)")
                columns = {row["name"] for row in cursor.fetchall()}
                source_select = "snapshot_source" if "snapshot_source" in columns else "'unknown' AS snapshot_source"

                cursor.execute(f"""
                    SELECT member_id, username, rank, earned_credits, contribution_rate,
                           timestamp, {source_select}
                    FROM members
                    WHERE member_id = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                """, (str(mc_user_id),))
                row = cursor.fetchone()
                if not row:
                    return None

                return {
                    "user_id": row["member_id"],
                    "name": row["username"],
                    "role": row["rank"],
                    "earned_credits": row["earned_credits"],
                    "contribution_rate": row["contribution_rate"],
                    "snapshot_at": row["timestamp"],
                    "snapshot_source": row["snapshot_source"],
                }
            finally:
                conn.close()
        except Exception as e:
            log.error(f"Failed to query member snapshot for {mc_user_id}: {e}", exc_info=True)
            return None

    def _query_member_contribution_history_sync(self, mc_user_id: str, limit: int = 12) -> List[float]:
        """Return recent stored contribution rates for a MissionChief user."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    """
                    SELECT contribution_rate
                    FROM members
                    WHERE member_id = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    (str(mc_user_id), int(limit)),
                ).fetchall()
                return [row["contribution_rate"] for row in rows if row["contribution_rate"] is not None]
            finally:
                conn.close()
        except Exception as e:
            log.error(f"Failed to query contribution history for {mc_user_id}: {e}", exc_info=True)
            return []

    def _query_member_first_seen_sync(self, mc_user_id: str) -> Optional[str]:
        """Return the first timestamp where a MissionChief user was seen."""
        try:
            conn = sqlite3.connect(self.db_path)
            try:
                row = conn.execute(
                    """
                    SELECT MIN(timestamp) AS first_seen
                    FROM members
                    WHERE member_id = ?
                    """,
                    (str(mc_user_id),),
                ).fetchone()
                return row[0] if row and row[0] else None
            finally:
                conn.close()
        except Exception as e:
            log.error(f"Failed to query first seen for {mc_user_id}: {e}", exc_info=True)
            return None

    async def get_member_snapshot(self, mc_user_id: str) -> Optional[Dict[str, Any]]:
        """Public API: return the latest stored snapshot for a MissionChief user."""
        return await asyncio.to_thread(self._query_member_snapshot_sync, str(mc_user_id))

    async def get_member_contribution_history(self, mc_user_id: str, limit: int = 12) -> List[float]:
        """Public API: return recent contribution rates, newest first."""
        return await asyncio.to_thread(
            self._query_member_contribution_history_sync,
            str(mc_user_id),
            int(limit),
        )

    async def get_member_first_seen(self, mc_user_id: str) -> Optional[str]:
        """Public API: return when a member was first seen by MembersScraper."""
        return await asyncio.to_thread(self._query_member_first_seen_sync, str(mc_user_id))

    def _query_current_members_sync(self) -> List[Dict[str, Any]]:
        """Return the latest stored MissionChief alliance member snapshot."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    """
                    SELECT user_id, mc_user_id, name, role, earned_credits,
                           contribution_rate, profile_href, scraped_at
                    FROM members_current
                    ORDER BY LOWER(name)
                    """
                ).fetchall()
                return [dict(row) for row in rows]
            finally:
                conn.close()
        except Exception as e:
            log.error(f"Failed to query current members: {e}", exc_info=True)
            return []

    async def get_members(self) -> List[Dict[str, Any]]:
        """Public API: return current MissionChief alliance members."""
        return await asyncio.to_thread(self._query_current_members_sync)
    
    async def _get_cookie_manager(self):
        """Get CookieManager cog instance"""
        return self.bot.get_cog("CookieManager")
    
    async def _get_session(self, ctx=None):
        """Get authenticated session from CookieManager cog"""
        cookie_manager = await self._get_cookie_manager()
        if not cookie_manager:
            await self._debug_log("❌ CookieManager cog not loaded!", ctx)
            return None
        
        try:
            session = await cookie_manager.get_session()
            await self._debug_log("✅ Session obtained successfully", ctx)
            return session
        except Exception as e:
            await self._debug_log(f"❌ Failed to get session: {e}", ctx)
            return None
    
    async def _check_logged_in(self, html_content, ctx=None):
        """Check if still logged in by looking for logout button or user menu"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        logout_button = soup.find('a', href='/users/sign_out')
        user_menu = soup.find('li', class_='dropdown user-menu')
        profile_link = soup.find('a', href=lambda x: x and '/profile' in str(x))
        settings_link = soup.find('a', href='/settings')
        has_member_links = bool(soup.find('a', href=lambda x: x and '/users/' in str(x)))
        
        is_logged_in = (logout_button is not None or 
                        user_menu is not None or 
                        profile_link is not None or
                        settings_link is not None or
                        has_member_links)
        
        await self._debug_log(f"Login check: {'✅ Logged in' if is_logged_in else '❌ NOT logged in'}", ctx)
        
        return is_logged_in
    
    async def _scrape_members_page(self, session, page_num, timestamp, ctx=None):
        """Scrape a single page of members"""
        if page_num == 1 or page_num % 10 == 0:
            await self._report_bot_status(f"scraping alliance members page {page_num}")
        url = f"{self.members_url}?page={page_num}"
        await self._debug_log(f"🌐 Scraping page {page_num}: {url}", ctx)
        
        for attempt in range(PAGE_REQUEST_MAX_ATTEMPTS):
            try:
                await asyncio.sleep(PAGE_REQUEST_BASE_DELAY_SECONDS + random.uniform(0.0, 0.75))
                
                async with session.get(url) as response:
                    await self._debug_log(f"📡 Response status: {response.status}", ctx)
                    
                    if response.status != 200:
                        await self._debug_log(f"❌ Page {page_num} returned status {response.status}", ctx)
                        retry_delay = self._page_retry_delay(response, attempt)
                        if retry_delay is None or attempt == PAGE_REQUEST_MAX_ATTEMPTS - 1:
                            return None
                        await self._debug_log(
                            f"Retrying page {page_num} after {retry_delay:.1f}s due to HTTP {response.status}",
                            ctx,
                        )
                        await asyncio.sleep(retry_delay + random.uniform(0.0, 1.0))
                        continue
                    
                    html = await response.text()
                    await self._debug_log(f"📄 HTML length: {len(html)} chars", ctx)
                    
                    if not await self._check_logged_in(html, ctx):
                        await self._debug_log(f"❌ Session expired on page {page_num}", ctx)
                        return None
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    members_data = []
                    
                    await self._debug_log("🔍 Searching for all <tr> tags with links...", ctx)
                    
                    for tr in soup.find_all("tr"):
                        a = tr.find("a", href=True)
                        if not a:
                            continue
                        
                        name = a.get_text(strip=True)
                        if not name:
                            continue
                        
                        href = a["href"]
                        user_id = ""
                        for pattern in [r"/users/(\d+)", r"/profile/(\d+)"]:
                            match = re.search(pattern, href)
                            if match:
                                user_id = match.group(1)
                                break
                        
                        tds = tr.find_all("td")
                        
                        role = ""
                        credits = 0
                        credits_raw = ""
                        rate = 0.0
                        
                        for td in tds:
                            txt = td.get_text(" ", strip=True)
                            
                            if not role and txt and not any(ch.isdigit() for ch in txt) and name not in txt:
                                role = txt
                            
                            if credits == 0:
                                credits_match = re.search(r'([\d,]+)\s+Credits?\b', txt, re.I)
                                if credits_match:
                                    credits_raw = credits_match.group(0)
                                    cleaned = credits_match.group(1).replace(',', '')
                                    try:
                                        val = int(cleaned)
                                        if 0 <= val <= 50000000000:
                                            credits = val
                                        else:
                                            credits = -1
                                    except Exception:
                                        credits = -1
                            
                            if "%" in txt and rate == 0.0:
                                match = re.search(r'(\d+(?:\.\d+)?)\s*%', txt)
                                if match:
                                    try:
                                        rate = float(match.group(1))
                                    except Exception:
                                        pass
                        
                        online_status = "online" if tr.find('span', class_='label-success') else "offline"
                        
                        is_suspicious = False
                        reason = ""
                        
                        if credits == -1:
                            is_suspicious = True
                            reason = f"Credits out of range: {credits_raw}"
                        elif credits == 0 and not credits_raw:
                            is_suspicious = True
                            reason = "No credits found in expected format"
                        elif credits > 10000000000:
                            is_suspicious = True
                            reason = f"Unusually high credits: {credits:,}"
                        
                        if is_suspicious:
                            await self._debug_log(f"🚨 SUSPICIOUS: {name} - {reason}", ctx)
                            
                            members_data.append({
                                'member_id': int(user_id) if user_id else 0,
                                'username': name,
                                'rank': role,
                                'earned_credits': credits if credits > 0 else 0,
                                'contribution_rate': rate,
                                'online_status': online_status,
                                'timestamp': timestamp,
                                'suspicious': True,
                                'reason': reason,
                                'raw_html': str(tr)[:500]
                            })
                        else:
                            members_data.append({
                                'member_id': int(user_id) if user_id else 0,
                                'username': name,
                                'rank': role,
                                'earned_credits': credits,
                                'contribution_rate': rate,
                                'online_status': online_status,
                                'timestamp': timestamp,
                                'suspicious': False
                            })
                            
                            await self._debug_log(
                                f"👤 Found: {name} (ID: {user_id}, Credits: {credits:,}, Rate: {rate}%, Role: {role})",
                                ctx,
                                discord=False,
                            )
                    
                    await self._debug_log(f"✅ Parsed {len(members_data)} members from page {page_num}", ctx)
                    return members_data
                    
            except asyncio.TimeoutError:
                await self._debug_log(f"⏱️ Timeout on page {page_num}, attempt {attempt + 1}/3", ctx)
                if attempt == PAGE_REQUEST_MAX_ATTEMPTS - 1:
                    return None
                await asyncio.sleep(min(5.0 * (2 ** attempt), PAGE_REQUEST_BACKOFF_CAP_SECONDS))
            except Exception as e:
                await self._debug_log(f"❌ Error scraping page {page_num}: {e}", ctx)
                if attempt == PAGE_REQUEST_MAX_ATTEMPTS - 1:
                    return None
                await asyncio.sleep(min(5.0 * (2 ** attempt), PAGE_REQUEST_BACKOFF_CAP_SECONDS))
        
        return None
    
    async def _detect_exits(self, current_members, ctx=None):
        """
        Detect members who left since last scrape and log them
        """
        await self._debug_log("🔍 Starting exit detection...", ctx)
        
        # Get previous scrape members
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # The current scrape is not saved yet, so the latest stored snapshot is previous.
        cursor.execute('''
            SELECT DISTINCT timestamp 
            FROM members 
            ORDER BY timestamp DESC 
            LIMIT 1
        ''')
        timestamps = cursor.fetchall()
        
        if not timestamps:
            await self._debug_log("No historical data available for exit detection", ctx)
            conn.close()
            return []
        
        previous_timestamp = timestamps[0][0]
        await self._debug_log(f"📅 Comparing current scrape to: {previous_timestamp}", ctx)
        
        # Get all members from previous scrape
        cursor.execute('''
            SELECT member_id, username, rank, earned_credits, contribution_rate, timestamp
            FROM members 
            WHERE timestamp = ?
        ''', (previous_timestamp,))
        
        previous_members = {}
        for row in cursor.fetchall():
            previous_members[row[0]] = {
                'member_id': row[0],
                'username': row[1],
                'rank': row[2],
                'earned_credits': row[3],
                'contribution_rate': row[4],
                'last_seen_at': row[5]
            }
        
        conn.close()
        
        # Get current member IDs
        current_ids = {m['member_id'] for m in current_members if not m.get('suspicious', False)}

        previous_count = len(previous_members)
        current_count = len(current_ids)
        if previous_count >= MIN_EXIT_DETECTION_BASELINE:
            retention = current_count / max(previous_count, 1)
            if retention < MIN_EXIT_DETECTION_RETENTION:
                await self._debug_log(
                    "Exit detection skipped: current scrape is too small "
                    f"({current_count}/{previous_count}, {retention:.0%}). "
                    "This usually means the member scrape was incomplete.",
                    ctx,
                )
                return []
        
        # Find who left (in previous but not in current)
        exits = []
        for prev_id, prev_data in previous_members.items():
            if prev_id not in current_ids and prev_id != 0:
                exits.append(prev_data)
                await self._debug_log(f"👋 DETECTED EXIT: {prev_data['username']} (ID: {prev_id})", ctx)
        
        await self._debug_log(f"📊 Exit Detection Results: {len(exits)} member(s) left", ctx)
        
        return exits
    
    async def _log_exits_to_database(self, exits, ctx=None):
        """
        Log exits to member_left_alliance table in MemberSync database
        """
        if not exits:
            return
        
        await self._debug_log(f"💾 Logging {len(exits)} exits to database...", ctx)
        
        conn = sqlite3.connect(self.membersync_db)
        cursor = conn.cursor()
        
        now = datetime.utcnow().isoformat()
        logged_count = 0
        
        # Try to find Discord ID from MemberSync links table
        def get_discord_id(mc_id):
            try:
                cursor.execute("SELECT discord_id FROM links WHERE mc_user_id=? AND status='approved'", (str(mc_id),))
                result = cursor.fetchone()
                return int(result[0]) if result else None
            except Exception:
                return None
        
        for exit_data in exits:
            mc_id = exit_data['member_id']
            discord_id = get_discord_id(mc_id)
            
            try:
                # Check if exit already recorded
                cursor.execute('''
                    SELECT id FROM member_left_alliance 
                    WHERE mc_user_id=? 
                    ORDER BY exit_detected_at DESC 
                    LIMIT 1
                ''', (str(mc_id),))
                
                existing = cursor.fetchone()
                
                # Only log if not already recorded OR if last record is old (>24h)
                should_log = True
                if existing:
                    cursor.execute('SELECT exit_detected_at FROM member_left_alliance WHERE id=?', (existing[0],))
                    last_exit = cursor.fetchone()[0]
                    try:
                        last_exit_dt = datetime.fromisoformat(last_exit.replace('Z', '+00:00'))
                        now_dt = datetime.fromisoformat(now.replace('Z', '+00:00'))
                        hours_since = (now_dt - last_exit_dt).total_seconds() / 3600
                        should_log = hours_since > 24  # Only log if >24h since last exit
                    except Exception:
                        should_log = True
                
                if should_log:
                    cursor.execute('''
                        INSERT INTO member_left_alliance 
                        (mc_user_id, username, discord_id, rank_role, earned_credits, 
                         contribution_rate, last_seen_at, exit_detected_at, reason, role_removed, notified)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        str(mc_id),
                        exit_data['username'],
                        discord_id,
                        exit_data['rank'],
                        exit_data['earned_credits'],
                        exit_data['contribution_rate'],
                        exit_data['last_seen_at'],
                        now,
                        'auto-detected by scraper',
                        0,  # role_removed (will be done by MemberSync prune)
                        0   # notified
                    ))
                    logged_count += 1
                    await self._debug_log(f"✅ Logged exit: {exit_data['username']} (MC: {mc_id})", ctx)
            
            except Exception as e:
                await self._debug_log(f"❌ Failed to log exit for {exit_data['username']}: {e}", ctx)
                log.error(f"Failed to log exit: {e}")
        
        conn.commit()
        conn.close()
        
        await self._debug_log(f"✅ Successfully logged {logged_count} exits to database", ctx)
        
        return logged_count
    
    async def _send_exit_notifications(self, exits, ctx=None):
        """
        Send Discord notifications for exits
        """
        if not exits:
            return
        
        channel_id = await self.config.exit_log_channel_id()
        if not channel_id:
            await self._debug_log("⚠️ No exit log channel configured, skipping notifications", ctx)
            return
        
        channel = self.bot.get_channel(int(channel_id))
        if not isinstance(channel, discord.TextChannel):
            await self._debug_log(f"❌ Exit log channel {channel_id} not found or not a text channel", ctx)
            return
        
        await self._debug_log(f"📢 Sending {len(exits)} exit notifications to {channel.name}...", ctx)
        
        # Group notifications if many exits (>5)
        if len(exits) > 5:
            # Send summary embed
            embed = discord.Embed(
                title="👋 Multiple Members Left Alliance",
                description=f"**{len(exits)} members** have left the alliance since last check",
                color=discord.Color.orange(),
                timestamp=datetime.utcnow()
            )
            
            # List names (max 20)
            names_list = []
            for exit_data in exits[:20]:
                mc_id = exit_data['member_id']
                name = exit_data['username']
                names_list.append(f"• **{name}** (MC: [{mc_id}](https://www.missionchief.com/users/{mc_id}))")
            
            if len(exits) > 20:
                names_list.append(f"... and {len(exits) - 20} more")
            
            embed.add_field(name="Members", value="\n".join(names_list), inline=False)
            embed.set_footer(text=f"Total exits: {len(exits)} | Check [p]membersync exits for full list")
            
            try:
                await channel.send(embed=embed)
                await self._debug_log(f"✅ Sent summary notification for {len(exits)} exits", ctx)
            except Exception as e:
                await self._debug_log(f"❌ Failed to send exit notification: {e}", ctx)
        
        else:
            # Send individual embeds for each exit
            for exit_data in exits:
                mc_id = exit_data['member_id']
                name = exit_data['username']
                credits = exit_data['earned_credits']
                rate = exit_data['contribution_rate']
                
                embed = discord.Embed(
                    title="👋 Member Left Alliance",
                    color=discord.Color.red(),
                    timestamp=datetime.utcnow()
                )
                
                embed.add_field(name="Name", value=name, inline=True)
                embed.add_field(name="MC ID", value=f"[{mc_id}](https://www.missionchief.com/users/{mc_id})", inline=True)
                embed.add_field(name="Last Credits", value=f"{credits:,}", inline=True)
                embed.add_field(name="Contribution Rate", value=f"{rate}%", inline=True)
                
                embed.set_footer(text="Verified role will be removed automatically")
                
                try:
                    await channel.send(embed=embed)
                    await self._debug_log(f"✅ Sent notification for {name}", ctx)
                except Exception as e:
                    await self._debug_log(f"❌ Failed to send notification for {name}: {e}", ctx)
    
    async def _scrape_all_members(self, ctx=None, custom_timestamp=None):
        lock = self._get_scrape_lock()
        if lock.locked():
            await self._debug_log("Members scrape skipped: another scrape is already running", ctx)
            if ctx:
                await ctx.send("A members scrape is already running. Try again after it finishes.")
            return False

        detail = "backfilling member snapshots" if custom_timestamp else "scraping alliance members"
        async with lock:
            async with self._bot_status(detail):
                self.current_scrape_page = None
                self.last_scrape_page_finished = None
                self.last_scrape_page_finished_at = None
                try:
                    scrape = self._scrape_all_members_impl(ctx, custom_timestamp)
                    if custom_timestamp:
                        return await scrape
                    return await asyncio.wait_for(scrape, timeout=FULL_MEMBER_SCRAPE_TIMEOUT_SECONDS)
                except asyncio.TimeoutError:
                    self.last_auto_scrape_error = (
                        f"TimeoutError: members scrape exceeded "
                        f"{FULL_MEMBER_SCRAPE_TIMEOUT_SECONDS // 60} minutes"
                    )
                    self._finish_scrape_run(
                        self.current_scrape_run_id,
                        "failed",
                        pages_attempted=self.current_scrape_page or 0,
                        pages_succeeded=self.last_scrape_page_finished or 0,
                        errors=1,
                        message=f"scrape timed out after {FULL_MEMBER_SCRAPE_TIMEOUT_SECONDS // 60} minutes",
                    )
                    await self._debug_log(
                        f"Members scrape timed out after {FULL_MEMBER_SCRAPE_TIMEOUT_SECONDS // 60} minutes",
                        ctx,
                    )
                    if ctx:
                        await ctx.send(
                            f"Members scrape timed out after {FULL_MEMBER_SCRAPE_TIMEOUT_SECONDS // 60} minutes. "
                            "No new member snapshot was saved."
                        )
                    return False
                finally:
                    self.current_scrape_page = None
                    self.current_scrape_run_id = None

    async def _scrape_all_members_impl(self, ctx=None, custom_timestamp=None):
        """Scrape all pages of members"""
        scrape_timestamp = custom_timestamp if custom_timestamp else datetime.utcnow().isoformat()
        snapshot_source = "backfill" if custom_timestamp else "live"
        run_id = self._start_scrape_run(snapshot_source, scrape_timestamp)
        self.current_scrape_run_id = run_id

        session = await self._get_session(ctx)
        if not session:
            self._finish_scrape_run(run_id, "failed", message="session unavailable", errors=1)
            if ctx:
                await ctx.send("❌ Failed to get session. Is CookieManager loaded and logged in?")
            return False
        
        all_members = []
        page = 1
        max_pages = 100
        
        await self._debug_log(f"🚀 Starting member scrape (max {max_pages} pages)", ctx)
        await self._debug_log(f"📅 Scrape timestamp: {scrape_timestamp}", ctx)
        
        empty_page_count = 0
        
        while page <= max_pages:
            self.current_scrape_page = page
            members = await self._scrape_members_page(session, page, scrape_timestamp, ctx)
            
            if members is None:
                await self._debug_log(
                    f"Member scrape aborted on page {page}; transient MissionChief failure or expired session",
                    ctx,
                )
                if ctx:
                    await ctx.send("Member scrape aborted because MissionChief returned an error or the session expired. No member data was saved.")
                self._finish_scrape_run(
                    run_id,
                    "failed",
                    pages_attempted=page,
                    pages_succeeded=max(0, page - 1),
                    rows_parsed=len(all_members),
                    errors=1,
                    message=f"page {page} unavailable",
                )
                return False

            if not members:
                empty_page_count += 1
                await self._debug_log(f"⚠️ Page {page} returned 0 members (empty count: {empty_page_count})", ctx)
                
                if empty_page_count >= 3:
                    await self._debug_log(f"⛔ Stopped after {empty_page_count} consecutive empty pages", ctx)
                    break
            else:
                empty_page_count = 0
                
                all_members.extend(members)
                await self._debug_log(f"✅ Page {page}: {len(members)} members (total so far: {len(all_members)})", ctx)
            
            self.last_scrape_page_finished = page
            self.last_scrape_page_finished_at = datetime.utcnow().isoformat()
            page += 1
        
        await self._debug_log(f"📊 Total members scraped: {len(all_members)} across {page - 1} pages", ctx)
        
        # Detect exits before saving (only if not backfilling)
        exits = []
        if not custom_timestamp and all_members:  # Only detect exits on successful live scrapes
            exits = await self._detect_exits(all_members, ctx)
            
            if exits:
                # Log to database
                await self._log_exits_to_database(exits, ctx)
                
                # Send Discord notifications
                await self._send_exit_notifications(exits, ctx)
        
        # Save to database
        if all_members:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            inserted = 0
            duplicates = 0
            suspicious_count = 0
            
            for member in all_members:
                try:
                    if member.get('suspicious', False):
                        suspect_credits = member['earned_credits']
                        if suspect_credits == -1:
                            suspect_credits = 0
                        else:
                            suspect_credits = max(0, min(INT64_MAX, int(suspect_credits)))
                        
                        cursor.execute('''
                            INSERT INTO suspicious_members 
                            (member_id, username, rank, parsed_credits, raw_html, reason, timestamp)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            member['member_id'],
                            member['username'],
                            member['rank'],
                            suspect_credits,
                            member.get('raw_html', ''),
                            member.get('reason', ''),
                            member['timestamp']
                        ))
                        suspicious_count += 1
                    else:
                        credits = max(0, min(INT64_MAX, int(member['earned_credits'])))
                        contribution_rate = float(member.get('contribution_rate', 0.0))
                        
                        cursor.execute('''
                            INSERT OR REPLACE INTO members 
                            (member_id, username, rank, earned_credits, contribution_rate,
                             online_status, timestamp, snapshot_source)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            member['member_id'],
                            member['username'],
                            member['rank'],
                            credits,
                            contribution_rate,
                            member['online_status'],
                            member['timestamp'],
                            snapshot_source
                        ))
                        if cursor.rowcount > 0:
                            inserted += 1
                except sqlite3.IntegrityError:
                    duplicates += 1
                except Exception as e:
                    await self._debug_log(f"⚠️ DB Error for {member['username']}: {e}", ctx)
                    duplicates += 1
            
            conn.commit()
            conn.close()
            self._finish_scrape_run(
                run_id,
                "success",
                pages_attempted=page - 1,
                pages_succeeded=page - 1,
                rows_parsed=len(all_members),
                rows_inserted=inserted,
                duplicates=duplicates,
                errors=suspicious_count,
                message=f"{len(all_members)} members scraped",
            )
            
            await self._debug_log(f"💾 Database: {inserted} inserted, {duplicates} duplicates, {suspicious_count} suspicious", ctx)
            
            if ctx:
                msg = f"✅ Scraped {len(all_members)} members across {page - 1} pages\n"
                msg += f"💾 Database: {inserted} new records, {duplicates} duplicates"
                if suspicious_count > 0:
                    msg += f"\n🚨 **WARNING**: {suspicious_count} suspicious entries detected!"
                if exits:
                    msg += f"\n👋 **{len(exits)} member(s) left the alliance** (logged & notified)"
                await ctx.send(msg)
            return True
        else:
            self._finish_scrape_run(
                run_id,
                "failed",
                pages_attempted=page - 1,
                pages_succeeded=max(0, page - empty_page_count - 1),
                rows_parsed=0,
                message="no member rows found",
            )
            if ctx:
                await ctx.send("⚠️ No members data found")
            return False
    
    async def _background_scraper(self):
        """Background task that runs every hour"""
        await self.bot.wait_until_ready()
        
        while not self.bot.is_closed():
            try:
                self.last_auto_scrape_started_at = datetime.utcnow().isoformat()
                self.last_auto_scrape_status = "running"
                self.last_auto_scrape_error = None
                print(f"[MembersScraper] Starting automatic scrape at {self.last_auto_scrape_started_at}")
                success = await self._scrape_all_members()
                self.last_auto_scrape_finished_at = datetime.utcnow().isoformat()
                self.last_auto_scrape_status = "success" if success else "failed"
                print("[MembersScraper] Automatic scrape completed")
            except Exception as e:
                self.last_auto_scrape_finished_at = datetime.utcnow().isoformat()
                self.last_auto_scrape_status = "error"
                self.last_auto_scrape_error = f"{type(e).__name__}: {e}"
                print(f"[MembersScraper] Background task error: {e}")
                log.exception("Background scraper error")
            
            await asyncio.sleep(3600)  # Wait 1 hour
    
    @commands.group(name="members")
    @commands.is_owner()
    async def members_group(self, ctx):
        """Members scraper commands"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @members_group.command(name="scrape")
    async def scrape_members(self, ctx):
        """Manually trigger members scraping"""
        await ctx.send("🔄 Starting members scrape...")
        success = await self._scrape_all_members(ctx)
        if success:
            await ctx.send("✅ Members scrape completed successfully")
    
    @members_group.command(name="backfill")
    async def backfill_members(self, ctx, days: int = 30):
        """
        Back-fill historical data by scraping current data with past timestamps.
        This simulates daily snapshots for the past X days.
        
        Usage: [p]members backfill 30
        """
        if days < 1 or days > 365:
            await ctx.send("❌ Days must be between 1 and 365")
            return
        
        await ctx.send(f"🔄 Starting back-fill for {days} days of historical data...")
        await ctx.send("⚠️ Note: This uses current member data with past timestamps to create historical baseline")
        
        session = await self._get_session(ctx)
        if not session:
            await ctx.send("❌ Failed to get session. Is CookieManager loaded and logged in?")
            return
        
        await self._debug_log("Fetching current member data for back-fill", ctx)
        all_members = []
        page = 1
        max_pages = 100
        scrape_timestamp = datetime.utcnow().isoformat()
        
        while page <= max_pages:
            members = await self._scrape_members_page(session, page, scrape_timestamp, ctx)
            if not members:
                break
            all_members.extend(members)
            page += 1
        
        if not all_members:
            await ctx.send("❌ Failed to fetch member data")
            return
        
        await ctx.send(f"📊 Fetched {len(all_members)} current members, creating {days} historical snapshots...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        total_inserted = 0
        
        for day_offset in range(days, 0, -1):
            past_date = datetime.utcnow() - timedelta(days=day_offset)
            past_date = past_date.replace(hour=12, minute=0, second=0, microsecond=0)
            timestamp = past_date.isoformat()
            
            for member in all_members:
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO members 
                        (member_id, username, rank, earned_credits, contribution_rate,
                         online_status, timestamp, snapshot_source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'backfill')
                    ''', (
                        member['member_id'],
                        member['username'],
                        member['rank'],
                        member['earned_credits'],
                        member.get('contribution_rate', 0.0),
                        member['online_status'],
                        timestamp
                    ))
                    if cursor.rowcount > 0:
                        total_inserted += 1
                except sqlite3.IntegrityError:
                    pass
            
            if day_offset % 10 == 0:
                conn.commit()
                await ctx.send(f"⏳ Progress: {days - day_offset}/{days} days completed...")
        
        conn.commit()
        conn.close()
        
        await self._debug_log(f"Back-fill completed: {total_inserted} records inserted", ctx)
        await ctx.send(f"✅ Back-fill completed!\n"
                      f"📊 Inserted {total_inserted} historical records across {days} days\n"
                      f"💡 You now have baseline data for trend analysis")
    
    @members_group.command(name="debug")
    async def debug_members(self, ctx, enable: bool = True):
        """Enable or disable debug logging to Discord"""
        self.debug_mode = enable
        self.debug_channel = ctx.channel if enable else None
        await ctx.send(f"🐛 Debug mode: {'**ENABLED**' if enable else '**DISABLED**'}\n"
                      f"Debug messages will be sent to this channel.")
    
    @members_group.command(name="task")
    async def task_members(self, ctx):
        """Show MembersScraper background task status."""
        task = self.scraping_task
        if task is None:
            task_state = "missing"
        else:
            try:
                if task.cancelled():
                    task_state = "cancelled"
                elif task.done():
                    exception = task.exception()
                    task_state = f"done with exception: {exception}" if exception else "done"
                else:
                    task_state = "running"
            except Exception as exc:
                task_state = f"could not inspect task: {exc}"

        lines = [
            "MembersScraper background task",
            f"Task: {task_state}",
            f"Last auto start: {self.last_auto_scrape_started_at or 'never'}",
            f"Last auto finish: {self.last_auto_scrape_finished_at or 'never'}",
            f"Last auto status: {self.last_auto_scrape_status or 'unknown'}",
        ]
        if self.last_auto_scrape_status == "running":
            elapsed = self._format_elapsed_since(self.last_auto_scrape_started_at)
            if elapsed:
                lines.append(f"Current auto scrape elapsed: {elapsed}")
            if self.current_scrape_page:
                lines.append(f"Current page: {self.current_scrape_page}")
            if self.last_scrape_page_finished:
                lines.append(f"Last completed page: {self.last_scrape_page_finished}")
                if self.last_scrape_page_finished_at:
                    lines.append(f"Last completed page at: {self.last_scrape_page_finished_at}")
        if self.last_auto_scrape_error:
            lines.append(f"Last auto error: {self.last_auto_scrape_error}")
        lines.append(f"Database: {self.db_path}")
        await ctx.send("\n".join(lines))

    @members_group.command(name="restarttask")
    async def restart_members_task(self, ctx):
        """Restart the MembersScraper hourly background task."""
        old_task = self.scraping_task
        restart_notice = None
        if self._task_is_running(old_task):
            old_task.cancel()
            try:
                await asyncio.wait_for(old_task, timeout=10)
            except asyncio.CancelledError:
                pass
            except asyncio.TimeoutError:
                await ctx.send(
                    "MembersScraper background task did not stop within 10 seconds. "
                    "No replacement task was started; reload the cog to avoid duplicate loops."
                )
                return
            except Exception as exc:
                restart_notice = f"Previous MembersScraper task stopped with error: {type(exc).__name__}: {exc}"
        self.scraping_task = None
        self._ensure_background_task()
        message = "MembersScraper background task restarted."
        if restart_notice:
            message = f"{message}\n{restart_notice}"
        await ctx.send(message)

    @members_group.command(name="setexitchannel")
    async def set_exit_channel(self, ctx, channel: discord.TextChannel):
        """
        Set the channel where exit notifications will be sent.
        
        Usage: [p]members setexitchannel #channel-name
        """
        await self.config.exit_log_channel_id.set(int(channel.id))
        await ctx.send(f"✅ Exit notification channel set to {channel.mention}\n"
                      f"You will now receive notifications when members leave the alliance.")
    
    @members_group.command(name="stats")
    async def stats_members(self, ctx):
        """Show database statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM members")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT member_id) FROM members")
        unique = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM members")
        date_range = cursor.fetchone()
        
        # FIXED: Get count from LATEST scrape only
        cursor.execute("SELECT MAX(timestamp) FROM members")
        latest_timestamp = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM members WHERE timestamp = ?", (latest_timestamp,))
        latest_count = cursor.fetchone()[0]
        
        # Check VIEW
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='members_current'")
        view_exists = cursor.fetchone()[0] > 0
        
        # Check VIEW count
        view_count = 0
        view_timestamp = "Unknown"
        if view_exists:
            cursor.execute("SELECT COUNT(*) FROM members_current")
            view_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT MAX(scraped_at) FROM members_current")
            view_timestamp_row = cursor.fetchone()
            if view_timestamp_row and view_timestamp_row[0]:
                view_timestamp = view_timestamp_row[0][:19]
        
        conn.close()
        
        # Check exit log channel
        exit_channel_id = await self.config.exit_log_channel_id()
        exit_channel = self.bot.get_channel(int(exit_channel_id)) if exit_channel_id else None
        exit_status = f"✅ {exit_channel.mention}" if exit_channel else "❌ Not configured"
        
        # Check exit records
        try:
            exit_conn = sqlite3.connect(self.membersync_db)
            exit_cursor = exit_conn.cursor()
            exit_cursor.execute("SELECT COUNT(*) FROM member_left_alliance")
            exit_count = exit_cursor.fetchone()[0]
            exit_conn.close()
        except Exception:
            exit_count = 0
        
        embed = discord.Embed(title="📊 Members Database Statistics", color=discord.Color.blue())
        embed.add_field(name="Total Records", value=f"{total:,}", inline=True)
        embed.add_field(name="Unique Members", value=f"{unique:,}", inline=True)
        embed.add_field(name="Snapshots", value=f"{total // max(unique, 1):,}", inline=True)
        
        if date_range[0]:
            embed.add_field(name="First Record", value=date_range[0][:10], inline=True)
            embed.add_field(name="Last Record", value=date_range[1][:10], inline=True)
        
        if latest_timestamp:
            embed.add_field(name="Latest Scrape", value=f"{latest_count} members\n{latest_timestamp[:16]}", inline=False)
        
        # VIEW status with quality check
        if view_exists:
            if view_count == latest_count and view_timestamp[:19] == latest_timestamp[:19]:
                sync_status = f"✅ Correct ({view_count} members)"
            elif view_count > latest_count * 1.5:
                sync_status = f"⚠️ DUPLICATES ({view_count} members, should be {latest_count})"
            elif view_count < latest_count * 0.5:
                sync_status = f"⚠️ Missing data ({view_count} vs {latest_count})"
            else:
                sync_status = f"⚠️ Mismatch ({view_count} vs {latest_count})"
        else:
            sync_status = "❌ Missing"
        
        embed.add_field(name="MemberSync VIEW", value=sync_status, inline=True)
        
        embed.add_field(name="Exit Detection", value=exit_status, inline=True)
        embed.add_field(name="Exit Records", value=f"{exit_count:,}", inline=True)
        
        embed.set_footer(text=f"Database: {self.db_path}")
        await ctx.send(embed=embed)

async def setup(bot):
    await bot.add_cog(MembersScraper(bot))
