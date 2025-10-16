import discord
from redbot.core import commands, Config, data_manager
import aiohttp
import asyncio
import sqlite3
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pathlib import Path

class MembersScraper(commands.Cog):
    """Scrapes alliance members data from MissionChief"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621001, force_registration=True)
        
        # Setup database path in shared location
        base_path = data_manager.cog_data_path(raw_name="scraper_databases")
        base_path.mkdir(parents=True, exist_ok=True)
        self.db_path = str(base_path / "members.db")
        
        self.base_url = "https://www.missionchief.com"
        self.members_url = f"{self.base_url}/verband/mitglieder/1621"
        self.scraping_task = None
        self.debug_mode = False
        self.debug_channel = None  # Will be set when debug command is used
        self._init_database()
        
    def cog_load(self):
        """Start background task when cog loads"""
        self.scraping_task = self.bot.loop.create_task(self._background_scraper())
        
    def cog_unload(self):
        """Cancel background task when cog unloads"""
        if self.scraping_task:
            self.scraping_task.cancel()
    
    def _init_database(self):
        """Initialize SQLite database with schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS members (
                member_id INTEGER,
                username TEXT,
                rank TEXT,
                earned_credits INTEGER,
                online_status TEXT,
                timestamp TEXT,
                PRIMARY KEY (member_id, timestamp)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON members(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_member_id ON members(member_id)')
        conn.commit()
        conn.close()
    
    async def _debug_log(self, message, ctx=None):
        """Log debug messages to console AND Discord"""
        print(f"[MembersScraper DEBUG] {message}")
        
        # Also send to Discord if debug mode is on and we have a channel
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
        is_logged_in = logout_button is not None or user_menu is not None
        
        await self._debug_log(f"Login check: {'✅ Logged in' if is_logged_in else '❌ NOT logged in'}", ctx)
        
        # Extra debug: show what we found
        if not is_logged_in:
            await self._debug_log(f"Logout button found: {logout_button is not None}", ctx)
            await self._debug_log(f"User menu found: {user_menu is not None}", ctx)
        
        return is_logged_in
    
    async def _scrape_members_page(self, session, page_num, ctx=None):
        """Scrape a single page of members"""
        url = f"{self.members_url}?page={page_num}"
        await self._debug_log(f"🌐 Scraping page {page_num}: {url}", ctx)
        
        for attempt in range(3):
            try:
                await asyncio.sleep(1.5)  # Rate limiting
                
                async with session.get(url) as response:
                    await self._debug_log(f"📡 Response status: {response.status}", ctx)
                    
                    if response.status != 200:
                        await self._debug_log(f"❌ Page {page_num} returned status {response.status}", ctx)
                        return []
                    
                    html = await response.text()
                    await self._debug_log(f"📄 HTML length: {len(html)} chars", ctx)
                    
                    # Check if still logged in
                    if not await self._check_logged_in(html, ctx):
                        await self._debug_log(f"❌ Session expired on page {page_num}", ctx)
                        return []
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    members_data = []
                    timestamp = datetime.utcnow().isoformat()
                    
                    # Find members table - try different selectors
                    table = soup.find('table', class_='table')
                    if not table:
                        await self._debug_log(f"⚠️ No table with class='table' found", ctx)
                        # Try alternative
                        table = soup.find('table')
                        if table:
                            await self._debug_log(f"✅ Found table without class", ctx)
                        else:
                            await self._debug_log(f"❌ No table found at all on page {page_num}", ctx)
                            # Debug: show what we DO have
                            tables = soup.find_all('table')
                            await self._debug_log(f"Total tables on page: {len(tables)}", ctx)
                            return []
                    
                    rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')
                    await self._debug_log(f"📊 Found {len(rows)} rows on page {page_num}", ctx)
                    
                    for idx, row in enumerate(rows):
                        cols = row.find_all('td')
                        await self._debug_log(f"Row {idx}: {len(cols)} columns", ctx)
                        
                        if len(cols) >= 3:  # Changed from 4 to 3 - more lenient
                            # Extract member data
                            member_link = cols[0].find('a')
                            if member_link:
                                member_id = member_link.get('href', '').split('/')[-1]
                                username = member_link.text.strip()
                                await self._debug_log(f"👤 Found member: {username} (ID: {member_id})", ctx)
                            else:
                                await self._debug_log(f"⚠️ No link in first column", ctx)
                                continue
                            
                            rank = cols[1].text.strip() if len(cols) > 1 else ""
                            earned_credits = cols[2].text.strip().replace(',', '').replace('$', '') if len(cols) > 2 else "0"
                            online_status = "online" if cols[0].find('span', class_='label-success') else "offline"
                            
                            try:
                                earned_credits = int(earned_credits)
                            except:
                                earned_credits = 0
                            
                            members_data.append({
                                'member_id': int(member_id) if member_id.isdigit() else 0,
                                'username': username,
                                'rank': rank,
                                'earned_credits': earned_credits,
                                'online_status': online_status,
                                'timestamp': timestamp
                            })
                    
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
    
    async def _scrape_all_members(self, ctx=None, custom_timestamp=None):
        """Scrape all pages of members"""
        session = await self._get_session(ctx)
        if not session:
            if ctx:
                await ctx.send("❌ Failed to get session. Is CookieManager loaded and logged in?")
            return False
        
        all_members = []
        page = 1
        max_pages = 50  # Safety limit
        
        await self._debug_log(f"🚀 Starting member scrape (max {max_pages} pages)", ctx)
        
        while page <= max_pages:
            members = await self._scrape_members_page(session, page, ctx)
            
            if not members:
                await self._debug_log(f"⛔ No members found on page {page}, stopping pagination", ctx)
                break
            
            # Override timestamp if provided (for back-filling)
            if custom_timestamp:
                for member in members:
                    member['timestamp'] = custom_timestamp
            
            all_members.extend(members)
            page += 1
        
        await self._debug_log(f"📊 Total members scraped: {len(all_members)} across {page - 1} pages", ctx)
        
        # Save to database
        if all_members:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            inserted = 0
            duplicates = 0
            
            for member in all_members:
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO members 
                        (member_id, username, rank, earned_credits, online_status, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        member['member_id'],
                        member['username'],
                        member['rank'],
                        member['earned_credits'],
                        member['online_status'],
                        member['timestamp']
                    ))
                    if cursor.rowcount > 0:
                        inserted += 1
                except sqlite3.IntegrityError:
                    duplicates += 1
            
            conn.commit()
            conn.close()
            
            await self._debug_log(f"💾 Database: {inserted} inserted, {duplicates} duplicates skipped", ctx)
            
            if ctx:
                await ctx.send(f"✅ Scraped {len(all_members)} members across {page - 1} pages\n"
                             f"💾 Database: {inserted} new records, {duplicates} duplicates")
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
        
        # Get current member data once
        await self._debug_log(f"Fetching current member data for back-fill", ctx)
        all_members = []
        page = 1
        max_pages = 50
        
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
                        (member_id, username, rank, earned_credits, online_status, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        member['member_id'],
                        member['username'],
                        member['rank'],
                        member['earned_credits'],
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
        
        cursor.execute("SELECT COUNT(*), timestamp FROM members GROUP BY timestamp ORDER BY timestamp DESC LIMIT 1")
        latest = cursor.fetchone()
        
        conn.close()
        
        embed = discord.Embed(title="📊 Members Database Statistics", color=discord.Color.blue())
        embed.add_field(name="Total Records", value=f"{total:,}", inline=True)
        embed.add_field(name="Unique Members", value=f"{unique:,}", inline=True)
        embed.add_field(name="Snapshots", value=f"{total // max(unique, 1):,}", inline=True)
        
        if date_range[0]:
            embed.add_field(name="First Record", value=date_range[0][:10], inline=True)
            embed.add_field(name="Last Record", value=date_range[1][:10], inline=True)
        
        if latest:
            embed.add_field(name="Latest Scrape", value=f"{latest[0]} members\n{latest[1][:16]}", inline=False)
        
        embed.set_footer(text=f"Database: {self.db_path}")
        await ctx.send(embed=embed)

async def setup(bot):
    await bot.add_cog(MembersScraper(bot))
