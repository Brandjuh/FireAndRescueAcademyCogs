import discord
from redbot.core import commands, Config, data_manager
import aiohttp
import asyncio
import sqlite3
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pathlib import Path
import re
import logging

log = logging.getLogger("red.FARA.MembersScraper")

# SQLite INTEGER limits
INT64_MAX = 9223372036854775807
INT64_MIN = -9223372036854775808

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
        self.debug_mode = False
        self.debug_channel = None
        self._init_database()
        
    def cog_load(self):
        """Start background task when cog loads"""
        self.scraping_task = self.bot.loop.create_task(self._background_scraper())
        log.info("MembersScraper loaded - WITH exit detection")
        
    def cog_unload(self):
        """Cancel background task when cog unloads"""
        if self.scraping_task:
            self.scraping_task.cancel()
    
    def _init_database(self):
        """Initialize SQLite database with schema"""
        import time
        
        # Retry logic for locked database
        max_retries = 5
        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self.db_path, timeout=10.0)
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
                        PRIMARY KEY (member_id, timestamp)
                    )
                ''')
                
                # Auto-migration: add contribution_rate if not exists
                cursor.execute("PRAGMA table_info(members)")
                columns = [col[1] for col in cursor.fetchall()]
                
                if 'contribution_rate' not in columns:
                    log.info("🔧 MIGRATION: Adding contribution_rate column")
                    cursor.execute('ALTER TABLE members ADD COLUMN contribution_rate REAL DEFAULT 0.0')
                    log.info("✅ Migration complete")
                
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON members(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_member_id ON members(member_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_contribution_rate ON members(contribution_rate)')
                
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
                    WHERE timestamp = (SELECT MAX(timestamp) FROM members)
                ''')
                log.info("✅ MemberSync VIEW created (using MAX(timestamp) for latest scrape only)")
                
                conn.commit()
                conn.close()
                break
            except sqlite3.OperationalError as e:
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
            conn = sqlite3.connect(self.membersync_db, timeout=10.0)
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
    
    async def _debug_log(self, message, ctx=None):
        """Log debug messages to console AND Discord"""
        print(f"[MembersScraper DEBUG] {message}")
        
        if self.debug_mode and (ctx or self.debug_channel):
            try:
                channel = ctx.channel if ctx else self.debug_channel
                if channel:
                    await channel.send(f"🐛 `{message}`")
            except Exception as e:
                print(f"[MembersScraper DEBUG] Failed to send to Discord: {e}")
    
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
    
    async def _scrape_members_page(self, session, page_num, ctx=None):
        """Scrape a single page of members"""
        url = f"{self.members_url}?page={page_num}"
        await self._debug_log(f"🌐 Scraping page {page_num}: {url}", ctx)
        
        for attempt in range(3):
            try:
                await asyncio.sleep(1.5)
                
                async with session.get(url) as response:
                    await self._debug_log(f"📡 Response status: {response.status}", ctx)
                    
                    if response.status != 200:
                        await self._debug_log(f"❌ Page {page_num} returned status {response.status}", ctx)
                        return []
                    
                    html = await response.text()
                    await self._debug_log(f"📄 HTML length: {len(html)} chars", ctx)
                    
                    if not await self._check_logged_in(html, ctx):
                        await self._debug_log(f"❌ Session expired on page {page_num}", ctx)
                        return []
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    members_data = []
                    timestamp = datetime.utcnow().isoformat()
                    
                    await self._debug_log(f"🔍 Searching for all <tr> tags with links...", ctx)
                    
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
                                    except:
                                        credits = -1
                            
                            if "%" in txt and rate == 0.0:
                                match = re.search(r'(\d+(?:\.\d+)?)\s*%', txt)
                                if match:
                                    try:
                                        rate = float(match.group(1))
                                    except:
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
                            
                            await self._debug_log(f"👤 Found: {name} (ID: {user_id}, Credits: {credits:,}, Rate: {rate}%, Role: {role})", ctx)
                    
                    await self._debug_log(f"✅ Parsed {len(members_data)} members from page {page_num}", ctx)
                    return members_data
                    
            except asyncio.TimeoutError:
                await self._debug_log(f"⏱️ Timeout on page {page_num}, attempt {attempt + 1}/3", ctx)
                if attempt == 2:
                    return []
            except Exception as e:
                await self._debug_log(f"❌ Error scraping page {page_num}: {e}", ctx)
                if attempt == 2:
                    return []
        
        return []
    
    async def _detect_exits(self, current_members, ctx=None):
        """
        Detect members who left since last scrape and log them
        """
        await self._debug_log("🔍 Starting exit detection...", ctx)
        
        # Get previous scrape members
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get the second-most recent timestamp (previous scrape)
        cursor.execute('''
            SELECT DISTINCT timestamp 
            FROM members 
            ORDER BY timestamp DESC 
            LIMIT 2
        ''')
        timestamps = cursor.fetchall()
        
        if len(timestamps) < 2:
            await self._debug_log("⚠️ Not enough historical data for exit detection (need at least 2 scrapes)", ctx)
            conn.close()
            return []
        
        previous_timestamp = timestamps[1][0]
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
            except:
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
                    except:
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
                rank = exit_data['rank'] or "No rank"
                credits = exit_data['earned_credits']
                rate = exit_data['contribution_rate']
                
                embed = discord.Embed(
                    title="👋 Member Left Alliance",
                    color=discord.Color.red(),
                    timestamp=datetime.utcnow()
                )
                
                embed.add_field(name="Name", value=name, inline=True)
                embed.add_field(name="MC ID", value=f"[{mc_id}](https://www.missionchief.com/users/{mc_id})", inline=True)
                embed.add_field(name="Rank", value=rank, inline=True)
                embed.add_field(name="Last Credits", value=f"{credits:,}", inline=True)
                embed.add_field(name="Contribution Rate", value=f"{rate}%", inline=True)
                
                embed.set_footer(text="Verified role will be removed automatically")
                
                try:
                    await channel.send(embed=embed)
                    await self._debug_log(f"✅ Sent notification for {name}", ctx)
                except Exception as e:
                    await self._debug_log(f"❌ Failed to send notification for {name}: {e}", ctx)
    
    async def _scrape_all_members(self, ctx=None, custom_timestamp=None):
        """Scrape all pages of members"""
        session = await self._get_session(ctx)
        if not session:
            if ctx:
                await ctx.send("❌ Failed to get session. Is CookieManager loaded and logged in?")
            return False
        
        all_members = []
        page = 1
        max_pages = 100
        
        await self._debug_log(f"🚀 Starting member scrape (max {max_pages} pages)", ctx)
        
        empty_page_count = 0
        
        while page <= max_pages:
            members = await self._scrape_members_page(session, page, ctx)
            
            if not members:
                empty_page_count += 1
                await self._debug_log(f"⚠️ Page {page} returned 0 members (empty count: {empty_page_count})", ctx)
                
                if empty_page_count >= 3:
                    await self._debug_log(f"⛔ Stopped after {empty_page_count} consecutive empty pages", ctx)
                    break
            else:
                empty_page_count = 0
                
                if custom_timestamp:
                    for member in members:
                        member['timestamp'] = custom_timestamp
                
                all_members.extend(members)
                await self._debug_log(f"✅ Page {page}: {len(members)} members (total so far: {len(all_members)})", ctx)
            
            page += 1
        
        await self._debug_log(f"📊 Total members scraped: {len(all_members)} across {page - 1} pages", ctx)
        
        # Detect exits before saving (only if not backfilling)
        exits = []
        if not custom_timestamp:  # Only detect exits on live scrapes
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
                            (member_id, username, rank, earned_credits, contribution_rate, online_status, timestamp)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            member['member_id'],
                            member['username'],
                            member['rank'],
                            credits,
                            contribution_rate,
                            member['online_status'],
                            member['timestamp']
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
            if ctx:
                await ctx.send("⚠️ No members data found")
            return False
    
    async def _background_scraper(self):
        """Background task that runs every hour"""
        await self.bot.wait_until_ready()
        
        while not self.bot.is_closed():
            try:
                print(f"[MembersScraper] Starting automatic scrape at {datetime.utcnow()}")
                await self._scrape_all_members()
                print(f"[MembersScraper] Automatic scrape completed")
            except Exception as e:
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
        await ctx.send(f"⚠️ Note: This uses current member data with past timestamps to create historical baseline")
        
        session = await self._get_session(ctx)
        if not session:
            await ctx.send("❌ Failed to get session. Is CookieManager loaded and logged in?")
            return
        
        await self._debug_log(f"Fetching current member data for back-fill", ctx)
        all_members = []
        page = 1
        max_pages = 100
        
        while page <= max_pages:
            members = await self._scrape_members_page(session, page, ctx)
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
                        (member_id, username, rank, earned_credits, contribution_rate, online_status, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
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
        except:
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
            if view_count == latest_count and view_timestamp == latest_timestamp:
                sync_status = f"✅ Correct ({view_count} members)"
            elif view_count > latest_count * 1.5:
                sync_status = f"⚠️ DUPLICATES ({view_count} members, should be {latest_count})"
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
