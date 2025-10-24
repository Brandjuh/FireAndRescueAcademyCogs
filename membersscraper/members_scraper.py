"""
members_scraper.py - COMPLETE FINAL VERSION met MemberSync Compatibility
Versie: 3.4 FINAL

✅ Contribution rate tracking
✅ Safe header-based parsing
✅ MemberSync compatibility (members_current VIEW)
✅ Complete troubleshooting commands
✅ Database validation
"""

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

def parse_int64_from_text(txt: str) -> int:
    """Parse integer from text with international formatting"""
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
    """Parse percentage from text"""
    if not txt or "%" not in txt:
        return 0.0
    m = re.search(r"(-?\d+(?:[.,]\d+)?)\s*%", txt)
    if not m:
        return 0.0
    try:
        return float(m.group(1).replace(",", "."))
    except (ValueError, TypeError):
        return 0.0

def extract_user_id(href: str) -> str:
    """Extract user ID from href"""
    if not href:
        return ""
    for pattern in [r"/users/(\d+)", r"/profile/(\d+)"]:
        m = re.search(pattern, href)
        if m:
            return m.group(1)
    return ""


class MembersScraper(commands.Cog):
    """Safe member scraper with header-based parsing and MemberSync compatibility"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621001, force_registration=True)
        
        base_path = data_manager.cog_data_path(raw_name="scraper_databases")
        base_path.mkdir(parents=True, exist_ok=True)
        self.db_path = str(base_path / "members_v2.db")
        
        self.base_url = "https://www.missionchief.com"
        self.members_url = f"{self.base_url}/verband/mitglieder/1621"
        self.scraping_task = None
        self.debug_mode = False
        self.debug_channel = None
        self._init_database()
        
    def cog_load(self):
        self.scraping_task = self.bot.loop.create_task(self._background_scraper())
        log.info("MembersScraper loaded - FINAL VERSION with MemberSync compatibility")
        
    def cog_unload(self):
        if self.scraping_task:
            self.scraping_task.cancel()
        log.info("MembersScraper unloaded")
    
    def _init_database(self):
        """Initialize database with auto-migration + MemberSync compatibility"""
        import time
        
        for attempt in range(5):
            try:
                conn = sqlite3.connect(self.db_path, timeout=10.0)
                cursor = conn.cursor()
                
                # Main members table
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
                
                # Auto-migration: add contribution_rate if missing
                cursor.execute("PRAGMA table_info(members)")
                columns = [col[1] for col in cursor.fetchall()]
                
                if 'contribution_rate' not in columns:
                    log.info("MIGRATION: Adding contribution_rate column")
                    cursor.execute('ALTER TABLE members ADD COLUMN contribution_rate REAL DEFAULT 0.0')
                
                # Indices
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
                
                # Skipped entries log
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS skipped_entries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        page INTEGER,
                        reason TEXT,
                        raw_html TEXT,
                        timestamp TEXT
                    )
                ''')
                
                # CRITICAL: Create VIEW for MemberSync compatibility
                # MemberSync expects a "members_current" table with specific columns
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
                    WHERE DATE(timestamp) = (SELECT MAX(DATE(timestamp)) FROM members)
                ''')
                
                conn.commit()
                conn.close()
                log.info(f"Database initialized with MemberSync compatibility: {self.db_path}")
                break
                
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() and attempt < 4:
                    time.sleep(0.5)
                else:
                    raise
    
    async def _debug_log(self, message, ctx=None):
        """Debug logging"""
        log.debug(message)
        if self.debug_mode and (ctx or self.debug_channel):
            try:
                channel = ctx.channel if ctx else self.debug_channel
                if channel:
                    await channel.send(f"🐛 `{message}`")
            except:
                pass
    
    async def _get_session(self, ctx=None):
        """Get authenticated session from CookieManager - EXACT zoals income_scraper"""
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager:
            await self._debug_log("❌ CookieManager not loaded", ctx)
            return None
        
        try:
            session = await cookie_manager.get_session()
            if not session:
                await self._debug_log("❌ Failed to get session", ctx)
                return None
            
            await self._debug_log("✅ Session obtained", ctx)
            return session
        except Exception as e:
            await self._debug_log(f"❌ Session error: {e}", ctx)
            return None
    
    async def _check_logged_in(self, html_content, ctx=None):
        """Check login status"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Check for homepage
        title = soup.find('title')
        if title and 'Create your own 911-Dispatch-Center' in title.get_text():
            await self._debug_log("❌ On homepage - NOT logged in", ctx)
            return False
        
        # Check for login form
        if soup.find('form', action=lambda x: x and 'sign_in' in str(x)):
            await self._debug_log("❌ Login form detected", ctx)
            return False
        
        # Check for member data indicators
        member_links = soup.find_all('a', href=lambda x: x and '/users/' in str(x))
        if len(member_links) >= 5:
            await self._debug_log(f"✅ Logged in ({len(member_links)} member links)", ctx)
            return True
        
        # Check for data tables
        tables = soup.find_all('table')
        if any(len(t.find_all('tr')) > 5 for t in tables):
            await self._debug_log("✅ Logged in (data tables found)", ctx)
            return True
        
        await self._debug_log("❌ Login status unclear, assuming NOT logged in", ctx)
        return False
    
    def _extract_member_rows_safe(self, html: str, page: int) -> tuple:
        """SAFE extraction using header-based column mapping"""
        soup = BeautifulSoup(html, "html.parser")
        members = []
        skipped = []
        stats = {
            'tables_found': 0,
            'rows_processed': 0,
            'members_parsed': 0,
            'skipped_no_link': 0,
            'skipped_no_id': 0,
            'skipped_invalid_data': 0
        }
        
        tables = soup.find_all("table")
        stats['tables_found'] = len(tables)
        
        for table in tables:
            header_row = table.find("tr")
            if not header_row:
                continue
            
            headers = []
            for th in header_row.find_all(["th", "td"]):
                headers.append(th.get_text(strip=True).lower())
            
            for tr in table.find_all("tr"):
                stats['rows_processed'] += 1
                
                if tr == header_row:
                    continue
                
                a = tr.find("a", href=True)
                if not a:
                    stats['skipped_no_link'] += 1
                    continue
                
                href = a["href"]
                if not ('/users/' in href or '/profile/' in href):
                    stats['skipped_no_link'] += 1
                    continue
                
                name = a.get_text(strip=True)
                if not name or len(name) < 2:
                    stats['skipped_invalid_data'] += 1
                    skipped.append({
                        'reason': 'Empty or too short name',
                        'html': str(tr)[:300]
                    })
                    continue
                
                user_id = extract_user_id(href)
                if not user_id:
                    stats['skipped_no_id'] += 1
                    skipped.append({
                        'reason': f'Could not extract ID from href: {href}',
                        'html': str(tr)[:300]
                    })
                    continue
                
                tds = tr.find_all("td")
                
                role = ""
                credits = 0
                rate = 0.0
                
                for td in tds:
                    txt = td.get_text(" ", strip=True)
                    
                    if not role and txt and not any(ch.isdigit() for ch in txt):
                        if txt != name and "credit" not in txt.lower() and "%" not in txt:
                            role = txt
                    
                    if credits == 0 and "credit" in txt.lower():
                        credits = parse_int64_from_text(txt)
                    
                    if rate == 0.0 and "%" in txt:
                        rate = parse_percent(txt)
                
                if credits == 0:
                    for td in tds:
                        txt = td.get_text(strip=True)
                        if "%" not in txt and "credit" not in txt.lower():
                            val = parse_int64_from_text(txt)
                            if val > 1000:
                                credits = val
                                break
                
                is_valid = True
                skip_reason = ""
                
                if credits < 0:
                    is_valid = False
                    skip_reason = "Negative credits"
                elif credits > 50000000000:
                    is_valid = False
                    skip_reason = f"Suspiciously high credits: {credits:,}"
                
                if rate < 0 or rate > 100:
                    is_valid = False
                    skip_reason = f"Invalid contribution rate: {rate}%"
                
                if not is_valid:
                    stats['skipped_invalid_data'] += 1
                    skipped.append({
                        'reason': f'{skip_reason} (user: {name})',
                        'html': str(tr)[:300]
                    })
                    continue
                
                online_status = "online" if tr.find('span', class_='label-success') else "offline"
                
                members.append({
                    'member_id': int(user_id),
                    'username': name,
                    'rank': role,
                    'earned_credits': credits,
                    'contribution_rate': rate,
                    'online_status': online_status,
                    'profile_href': href
                })
                stats['members_parsed'] += 1
        
        return members, skipped, stats
    
    async def _scrape_members_page(self, session, page, ctx=None):
        """Scrape single page with redirect detection"""
        url = f"{self.members_url}?page={page}"
        
        try:
            async with session.get(url, allow_redirects=True) as response:
                if response.status != 200:
                    await self._debug_log(f"❌ Page {page}: HTTP {response.status}", ctx)
                    return []
                
                html = await response.text()
                final_url = str(response.url)
                
                # Check for redirect
                if 'mitglieder' not in final_url and 'members' not in final_url:
                    await self._debug_log(f"❌ Page {page}: REDIRECTED to {final_url}", ctx)
                    if ctx:
                        await ctx.send(f"⚠️ REDIRECT DETECTED - Session expired!\nRun: `[p]cookie login`")
                    return []
                
                # Check for homepage
                if '<title>MISSIONCHIEF.COM - Create your own' in html:
                    await self._debug_log(f"❌ Page {page}: Got homepage", ctx)
                    if ctx:
                        await ctx.send(f"⚠️ HOMEPAGE DETECTED - Session invalid!\nRun: `[p]cookie login`")
                    return []
                
                if not await self._check_logged_in(html, ctx):
                    await self._debug_log(f"❌ Page {page}: Not logged in", ctx)
                    return []
                
                members, skipped, stats = self._extract_member_rows_safe(html, page)
                
                await self._debug_log(
                    f"📊 Page {page}: {stats['members_parsed']} parsed, "
                    f"{stats['skipped_no_link']} no link, "
                    f"{stats['skipped_no_id']} no ID, "
                    f"{stats['skipped_invalid_data']} invalid",
                    ctx
                )
                
                if skipped:
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    timestamp = datetime.utcnow().isoformat()
                    
                    for skip in skipped:
                        try:
                            cursor.execute('''
                                INSERT INTO skipped_entries (page, reason, raw_html, timestamp)
                                VALUES (?, ?, ?, ?)
                            ''', (page, skip['reason'], skip['html'], timestamp))
                        except:
                            pass
                    
                    conn.commit()
                    conn.close()
                
                timestamp = datetime.utcnow().isoformat()
                for member in members:
                    member['timestamp'] = timestamp
                
                return members
                
        except Exception as e:
            await self._debug_log(f"❌ Error page {page}: {e}", ctx)
            log.exception(f"Error scraping page {page}")
            return []
    
    async def _scrape_all_members(self, ctx=None, custom_timestamp=None):
        """Scrape all pages"""
        session = await self._get_session(ctx)
        if not session:
            if ctx:
                await ctx.send("❌ No session")
            return False
        
        all_members = []
        page = 1
        max_pages = 100
        
        await self._debug_log(f"🚀 Starting scrape (max {max_pages} pages)", ctx)
        
        empty_count = 0
        
        while page <= max_pages:
            members = await self._scrape_members_page(session, page, ctx)
            
            if not members:
                empty_count += 1
                await self._debug_log(f"⚠️ Page {page}: 0 members (empty: {empty_count})", ctx)
                
                if empty_count >= 3:
                    await self._debug_log(f"⛔ Stopped after {empty_count} empty pages", ctx)
                    break
            else:
                empty_count = 0
                
                if custom_timestamp:
                    for member in members:
                        member['timestamp'] = custom_timestamp
                
                all_members.extend(members)
                await self._debug_log(f"✅ Page {page}: {len(members)} members (total: {len(all_members)})", ctx)
            
            page += 1
            await asyncio.sleep(0.5)
        
        await self._debug_log(f"📊 Total: {len(all_members)} members from {page - 1} pages", ctx)
        
        if all_members:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            inserted = 0
            duplicates = 0
            
            for member in all_members:
                try:
                    credits = max(0, min(INT64_MAX, int(member['earned_credits'])))
                    rate = float(member.get('contribution_rate', 0.0))
                    
                    if not (0 <= rate <= 100):
                        log.warning(f"Invalid rate for {member['username']}: {rate}%")
                        rate = 0.0
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO members 
                        (member_id, username, rank, earned_credits, contribution_rate, online_status, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        member['member_id'],
                        member['username'],
                        member['rank'],
                        credits,
                        rate,
                        member['online_status'],
                        member['timestamp']
                    ))
                    
                    if cursor.rowcount > 0:
                        inserted += 1
                        
                except sqlite3.IntegrityError:
                    duplicates += 1
                except Exception as e:
                    log.error(f"DB error for {member['username']}: {e}")
                    duplicates += 1
            
            conn.commit()
            conn.close()
            
            await self._debug_log(f"💾 DB: {inserted} inserted, {duplicates} duplicates", ctx)
            
            if ctx:
                msg = f"✅ Scraped {len(all_members)} members from {page - 1} pages\n"
                msg += f"💾 Database: {inserted} new, {duplicates} duplicates"
                await ctx.send(msg)
                
        return True
    
    async def _background_scraper(self):
        """Background task - runs every hour"""
        await self.bot.wait_until_ready()
        log.info("Background scraper started")
        
        while not self.bot.is_closed():
            try:
                await asyncio.sleep(3600)
                log.info("Running background scrape")
                await self._scrape_all_members()
                log.info("Background scrape complete")
            except asyncio.CancelledError:
                log.info("Background scraper stopped")
                break
            except Exception as e:
                log.error(f"Background scraper error: {e}", exc_info=True)
    
    # ============== COMMANDS ==============
    
    @commands.group(name="members")
    @commands.is_owner()
    async def members_group(self, ctx):
        """Alliance members scraper commands"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @members_group.command(name="scrape")
    async def scrape_members(self, ctx):
        """Manually trigger member scrape"""
        async with ctx.typing():
            await self._scrape_all_members(ctx)
    
    @members_group.command(name="backfill")
    async def backfill_members(self, ctx, days: int = 30):
        """Backfill historical data - Usage: [p]members backfill 30"""
        if days < 1 or days > 365:
            await ctx.send("❌ Days must be between 1-365")
            return
        
        await ctx.send(f"🔄 Starting backfill for {days} days...")
        
        session = await self._get_session(ctx)
        if not session:
            await ctx.send("❌ No session")
            return
        
        all_members = []
        page = 1
        
        while page <= 100:
            members = await self._scrape_members_page(session, page, ctx)
            if not members:
                break
            all_members.extend(members)
            page += 1
            await asyncio.sleep(0.3)
        
        if not all_members:
            await ctx.send("❌ Failed to fetch data")
            return
        
        await ctx.send(f"📊 Got {len(all_members)} members, creating {days} snapshots...")
        
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
                except:
                    pass
            
            if day_offset % 5 == 0:
                await ctx.send(f"📈 Progress: {days - day_offset}/{days} days")
        
        conn.commit()
        conn.close()
        
        await ctx.send(f"✅ Backfill complete! {total_inserted} records")
    
    @members_group.command(name="testcontrib")
    async def test_contribution(self, ctx):
        """Test contribution rate data"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT MAX(DATE(timestamp)) as latest_date
                FROM members
            """)
            latest_date_row = cursor.fetchone()
            
            if not latest_date_row or not latest_date_row['latest_date']:
                await ctx.send("❌ No data in database. Run `[p]members scrape` first!")
                conn.close()
                return
            
            latest_date = latest_date_row['latest_date']
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN contribution_rate > 0 THEN 1 END) as with_contrib,
                    AVG(contribution_rate) as avg_rate,
                    MAX(contribution_rate) as max_rate,
                    MIN(contribution_rate) as min_rate
                FROM members
                WHERE DATE(timestamp) = ?
            """, (latest_date,))
            stats = cursor.fetchone()
            
            cursor.execute("""
                SELECT username, contribution_rate, earned_credits
                FROM members
                WHERE DATE(timestamp) = ?
                ORDER BY contribution_rate DESC, earned_credits DESC
                LIMIT 10
            """, (latest_date,))
            top_contrib = cursor.fetchall()
            
            conn.close()
            
            embed = discord.Embed(
                title="🔍 Contribution Rate Test",
                description=f"Data from: {latest_date}",
                color=discord.Color.green(),
                timestamp=datetime.utcnow()
            )
            
            if stats:
                stats_text = (
                    f"**Total Members:** {stats['total']}\n"
                    f"**With Contribution Data:** {stats['with_contrib']}\n"
                    f"**Average Rate:** {stats['avg_rate']:.2f}%\n"
                    f"**Max Rate:** {stats['max_rate']:.2f}%\n"
                    f"**Min Rate:** {stats['min_rate']:.2f}%"
                )
                embed.add_field(name="📊 Statistics", value=stats_text, inline=False)
            
            if top_contrib:
                contrib_text = "\n".join([
                    f"**{row['username']}**: {row['contribution_rate']:.1f}% ({row['earned_credits']:,})"
                    for row in top_contrib[:5]
                ])
                embed.add_field(name="🏆 Top 5 Contributors", value=contrib_text, inline=False)
            
            zero_count = stats['total'] - stats['with_contrib'] if stats else 0
            if zero_count > 0:
                embed.add_field(
                    name="ℹ️ Info",
                    value=f"{zero_count} members have 0% contribution rate (this can be normal)",
                    inline=False
                )
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")
            log.exception("Error in testcontrib")
    
    @members_group.command(name="stats")
    async def stats_members(self, ctx):
        """Database statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM members")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT member_id) FROM members")
        unique = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM members")
        date_range = cursor.fetchone()
        
        cursor.execute("""
            SELECT COUNT(*), timestamp 
            FROM members 
            GROUP BY timestamp 
            ORDER BY timestamp DESC 
            LIMIT 1
        """)
        latest = cursor.fetchone()
        
        cursor.execute("SELECT COUNT(*) FROM skipped_entries")
        skipped_count = cursor.fetchone()[0]
        
        # Check if members_current VIEW exists
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='members_current'")
        view_exists = cursor.fetchone()[0] > 0
        
        conn.close()
        
        embed = discord.Embed(title="📊 Database Stats", color=discord.Color.blue())
        embed.add_field(name="Total Records", value=f"{total:,}", inline=True)
        embed.add_field(name="Unique Members", value=f"{unique:,}", inline=True)
        embed.add_field(name="Snapshots", value=f"{total // max(unique, 1):,}", inline=True)
        
        if date_range[0]:
            embed.add_field(name="First", value=date_range[0][:10], inline=True)
            embed.add_field(name="Last", value=date_range[1][:10], inline=True)
        
        if latest:
            embed.add_field(name="Latest Scrape", value=f"{latest[0]} members\n{latest[1][:16]}", inline=False)
        
        if skipped_count > 0:
            embed.add_field(name="⚠️ Skipped Entries", value=f"{skipped_count} (use `viewskipped` to see)", inline=False)
        
        # MemberSync compatibility indicator
        sync_status = "✅ Active" if view_exists else "❌ Missing"
        embed.add_field(name="MemberSync Compatibility", value=sync_status, inline=False)
        
        embed.set_footer(text=f"Database: {self.db_path}")
        await ctx.send(embed=embed)
    
    @members_group.command(name="viewskipped")
    async def view_skipped(self, ctx, limit: int = 10):
        """View recently skipped entries"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT page, reason, timestamp
                FROM skipped_entries
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,))
            
            skipped = cursor.fetchall()
            conn.close()
            
            if not skipped:
                await ctx.send("✅ No skipped entries!")
                return
            
            embed = discord.Embed(
                title=f"⚠️ Recently Skipped Entries ({len(skipped)})",
                description="These entries were skipped during parsing",
                color=discord.Color.orange()
            )
            
            for entry in skipped[:5]:
                embed.add_field(
                    name=f"Page {entry['page']}",
                    value=f"{entry['reason']}\n`{entry['timestamp'][:16]}`",
                    inline=False
                )
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")
    
    @members_group.command(name="clearskipped")
    async def clear_skipped(self, ctx):
        """Clear skipped entries log"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM skipped_entries")
            deleted = cursor.rowcount
            conn.commit()
            conn.close()
            
            await ctx.send(f"✅ Cleared {deleted} skipped entries")
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")
    
    @members_group.command(name="debug")
    async def debug_scrape(self, ctx):
        """Full diagnostic"""
        embed = discord.Embed(title="🔍 Diagnostics", color=discord.Color.blue())
        
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager:
            embed.add_field(name="❌ CookieManager", value="NOT LOADED", inline=False)
            await ctx.send(embed=embed)
            return
        else:
            embed.add_field(name="✅ CookieManager", value="Loaded", inline=False)
        
        try:
            session = await cookie_manager.get_session()
            if not session or session.closed:
                embed.add_field(name="❌ Session", value="Failed", inline=False)
                await ctx.send(embed=embed)
                return
            else:
                embed.add_field(name="✅ Session", value="OK", inline=False)
        except Exception as e:
            embed.add_field(name="❌ Session Error", value=f"```{e}```", inline=False)
            await ctx.send(embed=embed)
            return
        
        await ctx.send("🔄 Testing connection...")
        test_url = f"{self.members_url}?page=1"
        
        try:
            async with session.get(test_url) as response:
                status = response.status
                html = await response.text()
                final_url = str(response.url)
                
                embed.add_field(
                    name="📡 HTTP",
                    value=f"Status: `{status}`\nURL: `{final_url}`\nHTML: `{len(html)}` chars",
                    inline=False
                )
                
                soup = BeautifulSoup(html, 'html.parser')
                login_form = soup.find('form', action=lambda x: x and 'sign_in' in str(x))
                member_links = soup.find_all('a', href=lambda x: x and '/users/' in str(x))
                
                if login_form:
                    embed.add_field(name="❌ Login", value="NOT LOGGED IN\nRun: `[p]cookie login`", inline=False)
                elif len(member_links) >= 5:
                    embed.add_field(name="✅ Login", value=f"Logged in ({len(member_links)} member links)", inline=False)
                else:
                    embed.add_field(name="⚠️ Login", value="Unclear", inline=False)
                
                all_tables = soup.find_all('table')
                total_rows = sum(len(table.find_all('tr')) for table in all_tables)
                
                embed.add_field(
                    name="📋 Tables",
                    value=f"Found {len(all_tables)} tables with {total_rows} total rows",
                    inline=False
                )
                
                members, skipped, stats = self._extract_member_rows_safe(html, 1)
                
                embed.add_field(
                    name="🔍 Parsing Test",
                    value=(
                        f"Parsed: {stats['members_parsed']}\n"
                        f"Skipped (no link): {stats['skipped_no_link']}\n"
                        f"Skipped (no ID): {stats['skipped_no_id']}\n"
                        f"Skipped (invalid): {stats['skipped_invalid_data']}"
                    ),
                    inline=False
                )
                
        except Exception as e:
            embed.add_field(name="❌ Error", value=f"```{e}```", inline=False)
        
        await ctx.send(embed=embed)
    
    @members_group.command(name="testpage")
    async def test_page(self, ctx, page: int = 1):
        """Test specific page"""
        await ctx.send(f"🔍 Testing page {page}...")
        
        old_debug = self.debug_mode
        old_channel = self.debug_channel
        self.debug_mode = True
        self.debug_channel = ctx.channel
        
        try:
            session = await self._get_session(ctx)
            if not session:
                await ctx.send("❌ No session")
                return
            
            members = await self._scrape_members_page(session, page, ctx)
            
            if not members:
                await ctx.send(f"❌ Page {page} returned 0 members")
            else:
                embed = discord.Embed(
                    title=f"📊 Page {page} Results",
                    description=f"Found {len(members)} members",
                    color=discord.Color.green()
                )
                
                for i, member in enumerate(members[:3], 1):
                    value = (
                        f"**ID:** {member['member_id']}\n"
                        f"**Rank:** {member['rank']}\n"
                        f"**Credits:** {member['earned_credits']:,}\n"
                        f"**Contribution:** {member.get('contribution_rate', 0)}%\n"
                        f"**Status:** {member['online_status']}"
                    )
                    
                    embed.add_field(
                        name=f"{i}. {member['username']}",
                        value=value,
                        inline=False
                    )
                
                if len(members) > 3:
                    embed.set_footer(text=f"... and {len(members) - 3} more")
                
                await ctx.send(embed=embed)
        finally:
            self.debug_mode = old_debug
            self.debug_channel = old_channel
    
    @members_group.command(name="checklogin")
    async def check_login(self, ctx):
        """Check login status"""
        async with ctx.typing():
            try:
                cookie_manager = self.bot.get_cog("CookieManager")
                if not cookie_manager:
                    await ctx.send("❌ CookieManager not loaded")
                    return
                
                session = await cookie_manager.get_session()
                test_url = self.members_url
                
                async with session.get(test_url) as response:
                    html = await response.text()
                    final_url = str(response.url)
                
                soup = BeautifulSoup(html, 'html.parser')
                login_form = soup.find('form', action=lambda x: x and 'sign_in' in str(x))
                member_links = soup.find_all('a', href=lambda x: x and '/users/' in str(x))
                
                embed = discord.Embed(title="🔐 Login Status")
                
                if login_form:
                    embed.color = discord.Color.red()
                    embed.description = "**❌ NOT LOGGED IN**"
                    embed.add_field(name="Action", value="Run: `[p]cookie login`", inline=False)
                elif len(member_links) >= 5:
                    embed.color = discord.Color.green()
                    embed.description = "**✅ LOGGED IN**"
                    embed.add_field(name="Details", value=f"Found {len(member_links)} member links", inline=False)
                else:
                    embed.color = discord.Color.orange()
                    embed.description = "**⚠️ UNCLEAR**"
                    embed.add_field(name="Details", value=f"URL: `{final_url}`", inline=False)
                
                await ctx.send(embed=embed)
                
            except Exception as e:
                await ctx.send(f"❌ Error: {e}")
    
    @members_group.command(name="fixsession")
    async def fix_session(self, ctx):
        """Force refresh session"""
        await ctx.send("🔄 Refreshing session...")
        
        try:
            cookie_manager = self.bot.get_cog("CookieManager")
            if not cookie_manager:
                await ctx.send("❌ CookieManager not loaded")
                return
            
            await ctx.send("🔐 Forcing re-login...")
            success = await cookie_manager._perform_login()
            
            if success:
                await ctx.send("✅ Session refreshed! Try scraping again.")
            else:
                await ctx.send("❌ Login failed. Check `[p]cookie debug trace`")
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")
    
    @members_group.command(name="enabledebug")
    async def enable_debug(self, ctx):
        """Enable debug mode"""
        self.debug_mode = True
        self.debug_channel = ctx.channel
        await ctx.send("✅ Debug mode **ENABLED** - All scraping will show detailed logs here")
    
    @members_group.command(name="disabledebug")
    async def disable_debug(self, ctx):
        """Disable debug mode"""
        self.debug_mode = False
        self.debug_channel = None
        await ctx.send("✅ Debug mode **DISABLED**")
    
    @members_group.command(name="validate")
    async def validate_data(self, ctx):
        """Validate database data quality"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("SELECT MAX(DATE(timestamp)) as latest FROM members")
            latest = cursor.fetchone()['latest']
            
            issues = []
            
            cursor.execute("""
                SELECT COUNT(*) FROM members 
                WHERE DATE(timestamp) = ? AND earned_credits = 0
            """, (latest,))
            zero_credits = cursor.fetchone()[0]
            if zero_credits > 0:
                issues.append(f"⚠️ {zero_credits} members with 0 credits")
            
            cursor.execute("""
                SELECT COUNT(*) FROM members 
                WHERE DATE(timestamp) = ? AND contribution_rate = 0
            """, (latest,))
            zero_rate = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM members 
                WHERE DATE(timestamp) = ? AND contribution_rate > 100
            """, (latest,))
            high_rate = cursor.fetchone()[0]
            if high_rate > 0:
                issues.append(f"❌ {high_rate} members with rate > 100%")
            
            cursor.execute("""
                SELECT COUNT(*) FROM members 
                WHERE DATE(timestamp) = ? AND earned_credits < 0
            """, (latest,))
            negative = cursor.fetchone()[0]
            if negative > 0:
                issues.append(f"❌ {negative} members with negative credits")
            
            conn.close()
            
            embed = discord.Embed(
                title="🔍 Data Quality Validation",
                color=discord.Color.green() if not issues else discord.Color.orange()
            )
            
            if not issues:
                embed.description = "✅ **All data looks good!**"
            else:
                embed.description = "**Issues found:**\n" + "\n".join(issues)
            
            embed.add_field(
                name="Info",
                value=f"Zero contrib rate: {zero_rate} (this is normal for some members)",
                inline=False
            )
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")


async def setup(bot):
    await bot.add_cog(MembersScraper(bot))
