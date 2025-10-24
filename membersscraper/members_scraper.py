"""
members_scraper.py - WERKENDE VERSIE met contribution_rate + MemberSync VIEW

Deze versie gebruikt de BEWEZEN WERKENDE code met ALLEEN MemberSync VIEW toegevoegd
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

class MembersScraper(commands.Cog):
    """Scrapes alliance members data from MissionChief"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621001, force_registration=True)
        
        # Setup database path in shared location
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
        """Start background task when cog loads"""
        self.scraping_task = self.bot.loop.create_task(self._background_scraper())
        log.info("MembersScraper loaded - WERKENDE VERSIE met MemberSync")
        
    def cog_unload(self):
        """Cancel background task when cog unloads"""
        if self.scraping_task:
            self.scraping_task.cancel()
    
    def _init_database(self):
        """Initialize SQLite database with schema - UPDATED WITH CONTRIBUTION_RATE + MEMBERSYNC VIEW"""
        import time
        
        max_retries = 5
        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self.db_path, timeout=10.0)
                cursor = conn.cursor()
                
                # Create main members table with contribution_rate
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
                
                # Check if contribution_rate column exists, if not add it
                cursor.execute("PRAGMA table_info(members)")
                columns = [col[1] for col in cursor.fetchall()]
                
                if 'contribution_rate' not in columns:
                    log.info("🔧 MIGRATION: Adding contribution_rate column to members table...")
                    cursor.execute('ALTER TABLE members ADD COLUMN contribution_rate REAL DEFAULT 0.0')
                    log.info("✅ Migration complete!")
                
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
                
                # ============== ENIGE NIEUWE TOEVOEGING: MEMBERSYNC VIEW ==============
                # Create VIEW for MemberSync compatibility
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
                log.info("✅ MemberSync VIEW created")
                # ====================================================================
                
                conn.commit()
                conn.close()
                break
                
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower():
                    time.sleep(1)
                    continue
                raise
    
    async def _get_session(self, ctx=None):
        """Get authenticated session from CookieManager - WERKENDE VERSIE"""
        cookie_manager = self.bot.get_cog('CookieManager')
        if not cookie_manager:
            if ctx:
                await self._debug_log("❌ CookieManager not loaded", ctx)
            return None
        
        try:
            session = await cookie_manager.get_session()
            return session
        except Exception as e:
            if ctx:
                await self._debug_log(f"❌ Failed to get session: {e}", ctx)
            return None
    
    async def _debug_log(self, message, ctx=None):
        """Send debug message to channel if debug mode enabled"""
        if self.debug_mode and self.debug_channel:
            try:
                await self.debug_channel.send(message)
            except:
                pass
        if ctx:
            print(f"[DEBUG] {message}")
    
    async def _scrape_members_page(self, session, page, ctx=None):
        """Scrape a single page of members - WERKENDE VERSIE"""
        url = f"{self.members_url}?page={page}"
        
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    await self._debug_log(f"❌ Page {page}: HTTP {response.status}", ctx)
                    return []
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                table = soup.find('table', class_='table')
                if not table:
                    await self._debug_log(f"⚠️ Page {page}: No table found", ctx)
                    return []
                
                members_data = []
                timestamp = datetime.utcnow().isoformat()
                
                for tr in table.find_all('tr')[1:]:  # Skip header
                    try:
                        # Find user link
                        user_link = tr.find('a', href=lambda x: x and '/users/' in x)
                        if not user_link:
                            continue
                        
                        name = user_link.get_text(strip=True)
                        href = user_link['href']
                        
                        # Extract member ID
                        match = re.search(r'/users/(\d+)', href)
                        if not match:
                            continue
                        user_id = match.group(1)
                        
                        tds = tr.find_all('td')
                        if len(tds) < 3:
                            continue
                        
                        # Initialize variables
                        role = ""
                        credits = 0
                        credits_raw = ""
                        rate = 0.0
                        
                        # Parse role, credits, and contribution rate from td elements
                        for td in tds:
                            txt = td.get_text(" ", strip=True)
                            
                            # Role: text without digits, not the name itself
                            if not role and txt and not any(ch.isdigit() for ch in txt) and name not in txt:
                                role = txt
                            
                            # Credits: ONLY accept "X,XXX Credits" format
                            if credits == 0:
                                credits_match = re.search(r'([\d,]+)\s+Credits?\b', txt, re.I)
                                if credits_match:
                                    credits_raw = credits_match.group(0)
                                    cleaned = credits_match.group(1).replace(',', '')
                                    try:
                                        val = int(cleaned)
                                        if 0 <= val <= 50000000000:  # Max 50 billion
                                            credits = val
                                        else:
                                            credits = -1  # Flag as suspicious
                                    except:
                                        credits = -1
                            
                            # Contribution rate: percentage
                            if "%" in txt and rate == 0.0:
                                match = re.search(r'(\d+(?:\.\d+)?)\s*%', txt)
                                if match:
                                    try:
                                        rate = float(match.group(1))
                                    except:
                                        pass
                        
                        # Determine online status
                        online_status = "online" if tr.find('span', class_='label-success') else "offline"
                        
                        # Check if suspicious
                        is_suspicious = False
                        reason = ""
                        
                        if credits == -1:
                            is_suspicious = True
                            reason = f"Credits out of range: {credits_raw}"
                        elif credits == 0 and not credits_raw:
                            is_suspicious = True
                            reason = "No credits found in expected format"
                        elif credits > 10000000000:  # 10 billion
                            is_suspicious = True
                            reason = f"Unusually high credits: {credits:,}"
                        
                        if is_suspicious:
                            await self._debug_log(f"🚨 SUSPICIOUS: {name} - {reason}", ctx)
                            
                            members_data.append({
                                'member_id': int(user_id),
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
                            # Normal entry
                            members_data.append({
                                'member_id': int(user_id),
                                'username': name,
                                'rank': role,
                                'earned_credits': credits,
                                'contribution_rate': rate,
                                'online_status': online_status,
                                'timestamp': timestamp,
                                'suspicious': False
                            })
                    
                    except Exception as e:
                        await self._debug_log(f"⚠️ Error parsing row on page {page}: {e}", ctx)
                        continue
                
                return members_data
                
        except Exception as e:
            await self._debug_log(f"❌ Error scraping page {page}: {e}", ctx)
            return []
    
    async def _scrape_all_members(self, ctx=None, custom_timestamp=None):
        """Scrape all members pages - WERKENDE VERSIE"""
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
                        # Normal insert
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
                await ctx.send(msg)
                
        return True
    
    async def _background_scraper(self):
        """Background task that runs every hour"""
        await self.bot.wait_until_ready()
        log.info("Background scraper started")
        
        while not self.bot.is_closed():
            try:
                await asyncio.sleep(3600)  # Wait 1 hour
                log.info("Running background scrape")
                await self._scrape_all_members()
                log.info("Background scrape complete")
            except asyncio.CancelledError:
                log.info("Background scraper stopped")
                break
            except Exception as e:
                log.error(f"Error in background scraper: {e}", exc_info=True)
    
    # ============== COMMANDS ==============
    
    @commands.group(name="members")
    @commands.is_owner()
    async def members_group(self, ctx):
        """Alliance members scraper commands"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @commands.command(name="scrape")
    async def scrape_members(self, ctx):
        """Manually trigger a member scrape"""
        async with ctx.typing():
            await self._scrape_all_members(ctx)
    
    @members_group.command(name="backfill")
    async def backfill_members(self, ctx, days: int = 30):
        """
        Back-fill historical data by creating snapshots for past days.
        This uses CURRENT member data with past timestamps.
        
        Usage: [p]members backfill 30
        """
        if days < 1 or days > 365:
            await ctx.send("❌ Days must be between 1 and 365")
            return
        
        await ctx.send(f"🔄 Starting back-fill for {days} days of historical data...")
        
        session = await self._get_session(ctx)
        if not session:
            await ctx.send("❌ Failed to get session")
            return
        
        # Get current member data once
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
        
        # Insert historical snapshots
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
                except Exception as e:
                    pass
            
            if day_offset % 5 == 0:  # Progress update every 5 days
                await ctx.send(f"📈 Progress: {days - day_offset}/{days} days completed...")
        
        conn.commit()
        conn.close()
        
        await ctx.send(f"✅ Back-fill complete! Inserted {total_inserted} historical records across {days} days")
    
    @members_group.command(name="testcontrib")
    async def test_contribution(self, ctx):
        """Test command to verify contribution rates are being scraped and stored"""
        async with ctx.typing():
            try:
                conn = sqlite3.connect(self.db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Get latest date
                cursor.execute("SELECT MAX(DATE(timestamp)) as latest_date FROM members")
                latest_date_row = cursor.fetchone()
                
                if not latest_date_row or not latest_date_row['latest_date']:
                    await ctx.send("❌ No data in database. Run `[p]members scrape` first!")
                    conn.close()
                    return
                
                latest_date = latest_date_row['latest_date']
                
                # Get statistics
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
                
                # Get top contributors
                cursor.execute("""
                    SELECT username, contribution_rate, earned_credits
                    FROM members
                    WHERE DATE(timestamp) = ?
                    ORDER BY contribution_rate DESC
                    LIMIT 10
                """, (latest_date,))
                top_contrib = cursor.fetchall()
                
                conn.close()
                
                # Build response
                embed = discord.Embed(
                    title="🔍 Contribution Rate Test Results",
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
                        f"**{row['username']}**: {row['contribution_rate']:.1f}% ({row['earned_credits']:,} credits)"
                        for row in top_contrib[:5]
                    ])
                    embed.add_field(
                        name="🏆 Top 5 Contributors",
                        value=contrib_text,
                        inline=False
                    )
                
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
        """Show database statistics"""
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
        
        # Check if members_current VIEW exists
        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='members_current'")
        view_exists = cursor.fetchone()[0] > 0
        
        conn.close()
        
        embed = discord.Embed(title="📊 Members Database Statistics", color=discord.Color.blue())
        embed.add_field(name="Total Records", value=f"{total:,}", inline=True)
        embed.add_field(name="Unique Members", value=f"{unique:,}", inline=True)
        embed.add_field(name="Snapshots", value=f"{total // max(unique, 1):,}", inline=True)
        
        if date_range[0]:
            embed.add_field(name="First Record", value=date_range[0][:10], inline=True)
            embed.add_field(name="Last Record", value=date_range[1][:10], inline=True)
        
        if latest:
            embed.add_field(
                name="Latest Scrape", 
                value=f"{latest[0]} members\n{latest[1][:16]}", 
                inline=False
            )
        
        # MemberSync compatibility indicator
        sync_status = "✅ Active" if view_exists else "❌ Missing"
        embed.add_field(name="MemberSync Compatibility", value=sync_status, inline=False)
        
        embed.set_footer(text=f"Database: {self.db_path}")
        await ctx.send(embed=embed)
    
    @members_group.command(name="enabledebug")
    async def enable_debug(self, ctx):
        """Enable debug mode"""
        self.debug_mode = True
        self.debug_channel = ctx.channel
        await ctx.send("✅ Debug mode **ENABLED**")
    
    @members_group.command(name="disabledebug")
    async def disable_debug(self, ctx):
        """Disable debug mode"""
        self.debug_mode = False
        self.debug_channel = None
        await ctx.send("✅ Debug mode **DISABLED**")
    
    @members_group.command(name="testpage")
    async def test_page(self, ctx, page: int = 1):
        """Test specific page"""
        await ctx.send(f"🔍 Testing page {page}...")
        
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

async def setup(bot):
    await bot.add_cog(MembersScraper(bot))
