import discord
from redbot.core import commands, Config, data_manager
import aiohttp
import asyncio
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path

class LogsScraper(commands.Cog):
    """Scrapes alliance logs from MissionChief"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621002, force_registration=True)
        
        # Setup database path in shared location
        base_path = data_manager.cog_data_path(raw_name="scraper_databases")
        base_path.mkdir(parents=True, exist_ok=True)
        self.db_path = str(base_path / "logs.db")
        
        self.base_url = "https://www.missionchief.com"
        self.logs_url = f"{self.base_url}/alliance_logfiles"
        self.scraping_task = None
        self.debug_mode = False
        self._init_database()
        
    def cog_load(self):
        """Start background task when cog loads"""
        self.scraping_task = self.bot.loop.create_task(self._background_scraper())
        
    def cog_unload(self):
        """Cancel background task when cog unloads"""
        if self.scraping_task:
            self.scraping_task.cancel()
    
    def _init_database(self):
        """Initialize SQLite database with schema for logs"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_type TEXT,
                username TEXT,
                action TEXT,
                details TEXT,
                log_timestamp TEXT,
                scrape_timestamp TEXT,
                UNIQUE(log_type, username, action, log_timestamp)
            )
        ''')
        
        # Training courses can appear up to 4 times per timestamp
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS training_courses (
                course_id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT,
                course_name TEXT,
                log_timestamp TEXT,
                scrape_timestamp TEXT,
                occurrence INTEGER DEFAULT 1,
                UNIQUE(username, course_name, log_timestamp, occurrence)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_log_timestamp ON logs(log_timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_scrape_timestamp ON logs(scrape_timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_training_timestamp ON training_courses(log_timestamp)')
        
        conn.commit()
        conn.close()
    
    def _debug_log(self, message):
        """Log debug messages"""
        print(f"[LogsScraper DEBUG] {message}")
    
    async def _get_cookie_manager(self):
        """Get CookieManager cog instance"""
        return self.bot.get_cog("CookieManager")
    
    async def _get_session(self):
        """Get authenticated session from CookieManager cog"""
        cookie_manager = await self._get_cookie_manager()
        if not cookie_manager:
            self._debug_log("CookieManager cog not loaded!")
            return None
        
        try:
            session = await cookie_manager.get_session()
            self._debug_log("Session obtained successfully")
            return session
        except Exception as e:
            self._debug_log(f"Failed to get session: {e}")
            return None
    
    async def _check_logged_in(self, html_content):
        """Check if still logged in"""
        soup = BeautifulSoup(html_content, 'html.parser')
        logout_button = soup.find('a', href='/users/sign_out')
        user_menu = soup.find('li', class_='dropdown user-menu')
        is_logged_in = logout_button is not None or user_menu is not None
        self._debug_log(f"Login check: {'✓ Logged in' if is_logged_in else '✗ Not logged in'}")
        return is_logged_in
    
    def _parse_log_entry(self, row, scrape_timestamp):
        """Parse a single log entry row"""
        cols = row.find_all('td')
        if len(cols) < 2:
            return None
        
        # Timestamp column (this is the ACTUAL log timestamp from MissionChief)
        timestamp_col = cols[0].text.strip()
        
        # Action column
        action_col = cols[1]
        action_text = action_col.get_text(strip=True, separator=' ')
        
        # Detect log type
        log_type = "general"
        username = ""
        action = ""
        details = ""
        
        # Training course detection
        if "started" in action_text.lower() and "education" in action_text.lower():
            log_type = "training_course"
            parts = action_text.split("started education")
            if len(parts) == 2:
                username = parts[0].strip()
                details = parts[1].strip()
        elif "promoted" in action_text.lower():
            log_type = "promotion"
            username = action_text.split("promoted")[0].strip() if "promoted" in action_text else ""
            details = action_text
        elif "built" in action_text.lower() or "expanded" in action_text.lower():
            log_type = "building"
            username = action_text.split()[0] if action_text.split() else ""
            details = action_text
        else:
            log_type = "general"
            details = action_text
        
        return {
            'log_type': log_type,
            'username': username,
            'action': action_text[:200],
            'details': details[:500],
            'log_timestamp': timestamp_col,  # ORIGINAL timestamp from MissionChief
            'scrape_timestamp': scrape_timestamp  # When WE scraped it
        }
    
    async def _scrape_logs_page(self, session, page_num):
        """Scrape a single page of logs"""
        url = f"{self.logs_url}?page={page_num}"
        self._debug_log(f"Scraping logs page {page_num}: {url}")
        
        for attempt in range(3):
            try:
                await asyncio.sleep(1.5)
                
                async with session.get(url) as response:
                    if response.status != 200:
                        self._debug_log(f"Page {page_num} returned status {response.status}")
                        return []
                    
                    html = await response.text()
                    
                    if not await self._check_logged_in(html):
                        self._debug_log(f"Session expired on page {page_num}")
                        return []
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    logs_data = []
                    scrape_timestamp = datetime.utcnow().isoformat()
                    
                    # Find logs table
                    table = soup.find('table', class_='table')
                    if not table:
                        self._debug_log(f"No table found on page {page_num}")
                        return []
                    
                    rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')
                    self._debug_log(f"Found {len(rows)} log entries on page {page_num}")
                    
                    for row in rows:
                        log_entry = self._parse_log_entry(row, scrape_timestamp)
                        if log_entry:
                            logs_data.append(log_entry)
                    
                    self._debug_log(f"Parsed {len(logs_data)} valid log entries from page {page_num}")
                    return logs_data
                    
            except asyncio.TimeoutError:
                self._debug_log(f"Timeout on page {page_num}, attempt {attempt + 1}/3")
                if attempt == 2:
                    return []
            except Exception as e:
                self._debug_log(f"Error scraping page {page_num}: {e}")
                if attempt == 2:
                    return []
        
        return []
    
    async def _scrape_all_logs(self, ctx=None, max_pages=100):
        """Scrape all pages of logs"""
        session = await self._get_session()
        if not session:
            if ctx:
                await ctx.send("❌ Failed to get session. Is CookieManager loaded and logged in?")
            return False
        
        all_logs = []
        page = 1
        
        self._debug_log(f"Starting logs scrape (max {max_pages} pages)")
        
        while page <= max_pages:
            logs = await self._scrape_logs_page(session, page)
            
            if not logs:
                self._debug_log(f"No logs found on page {page}, stopping pagination")
                break
            
            all_logs.extend(logs)
            page += 1
            
            # Progress update for backfill
            if ctx and page % 10 == 0:
                await ctx.send(f"⏳ Progress: {page} pages scraped, {len(all_logs)} log entries collected...")
        
        self._debug_log(f"Total logs scraped: {len(all_logs)} across {page - 1} pages")
        
        # Save to database
        if all_logs:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            training_count = 0
            general_count = 0
            duplicates = 0
            
            for log in all_logs:
                if log['log_type'] == 'training_course':
                    # Check how many times this training already exists
                    cursor.execute('''
                        SELECT COUNT(*) FROM training_courses 
                        WHERE username = ? AND course_name = ? AND log_timestamp = ?
                    ''', (log['username'], log['details'], log['log_timestamp']))
                    
                    count = cursor.fetchone()[0]
                    
                    if count < 4:  # Allow up to 4 occurrences
                        try:
                            cursor.execute('''
                                INSERT INTO training_courses 
                                (username, course_name, log_timestamp, scrape_timestamp, occurrence)
                                VALUES (?, ?, ?, ?, ?)
                            ''', (
                                log['username'],
                                log['details'],
                                log['log_timestamp'],
                                log['scrape_timestamp'],
                                count + 1
                            ))
                            training_count += 1
                        except sqlite3.IntegrityError:
                            duplicates += 1
                else:
                    # Regular logs - insert or ignore
                    try:
                        cursor.execute('''
                            INSERT INTO logs 
                            (log_type, username, action, details, log_timestamp, scrape_timestamp)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            log['log_type'],
                            log['username'],
                            log['action'],
                            log['details'],
                            log['log_timestamp'],
                            log['scrape_timestamp']
                        ))
                        if cursor.rowcount > 0:
                            general_count += 1
                    except sqlite3.IntegrityError:
                        duplicates += 1
            
            conn.commit()
            conn.close()
            
            self._debug_log(f"Database: {general_count} general logs, {training_count} trainings, {duplicates} duplicates")
            
            if ctx:
                await ctx.send(f"✅ Scraped {len(all_logs)} log entries across {page - 1} pages\n"
                             f"💾 Database: {general_count} general logs, {training_count} training courses, {duplicates} duplicates")
            return True
        else:
            if ctx:
                await ctx.send("⚠️ No logs data found")
            return False
    
    async def _background_scraper(self):
        """Background task that runs every hour"""
        await self.bot.wait_until_ready()
        await asyncio.sleep(900)  # Stagger: 15 minutes offset
        
        while not self.bot.is_closed():
            try:
                self._debug_log(f"Starting automatic scrape at {datetime.utcnow()}")
                # Only scrape first 10 pages on automatic runs
                await self._scrape_all_logs(max_pages=10)
                self._debug_log(f"Automatic scrape completed")
            except Exception as e:
                self._debug_log(f"Background task error: {e}")
            
            await asyncio.sleep(3600)
    
    @commands.group(name="logs")
    @commands.is_owner()
    async def logs_group(self, ctx):
        """Logs scraper commands"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @logs_group.command(name="scrape")
    async def scrape_logs(self, ctx, max_pages: int = 10):
        """
        Manually trigger logs scraping
        
        Usage: [p]logs scrape [max_pages]
        Example: [p]logs scrape 50
        """
        await ctx.send(f"🔄 Starting logs scrape (max {max_pages} pages)...")
        success = await self._scrape_all_logs(ctx, max_pages=max_pages)
        if success:
            await ctx.send("✅ Logs scrape completed successfully")
    
    @logs_group.command(name="backfill")
    async def backfill_logs(self, ctx, max_pages: int = 200):
        """
        Back-fill ALL historical logs from MissionChief.
        This scrapes ALL available log pages to get complete history.
        
        Usage: [p]logs backfill [max_pages]
        Example: [p]logs backfill 500
        
        Note: This preserves the original timestamps from MissionChief logs!
        """
        if max_pages < 1 or max_pages > 1000:
            await ctx.send("❌ Max pages must be between 1 and 1000")
            return
        
        await ctx.send(f"🔄 Starting back-fill of ALL historical logs (up to {max_pages} pages)...")
        await ctx.send(f"💡 This will preserve original timestamps from MissionChief")
        await ctx.send(f"⚠️ This may take several minutes depending on alliance activity...")
        
        success = await self._scrape_all_logs(ctx, max_pages=max_pages)
        
        if success:
            await ctx.send(f"✅ Back-fill completed! All historical logs are now in the database.")
    
    @logs_group.command(name="debug")
    async def debug_logs(self, ctx, enable: bool = True):
        """Enable or disable debug logging"""
        self.debug_mode = enable
        await ctx.send(f"🐛 Debug mode: {'**ENABLED**' if enable else '**DISABLED**'}")
    
    @logs_group.command(name="stats")
    async def stats_logs(self, ctx):
        """Show database statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total general logs
        cursor.execute("SELECT COUNT(*) FROM logs")
        total_logs = cursor.fetchone()[0]
        
        # Total training courses
        cursor.execute("SELECT COUNT(*) FROM training_courses")
        total_training = cursor.fetchone()[0]
        
        # Log types breakdown
        cursor.execute("SELECT log_type, COUNT(*) FROM logs GROUP BY log_type")
        log_types = cursor.fetchall()
        
        # Date range
        cursor.execute("SELECT MIN(log_timestamp), MAX(log_timestamp) FROM logs")
        date_range = cursor.fetchone()
        
        # Recent activity (last 24h)
        cursor.execute("""
            SELECT COUNT(*) FROM logs 
            WHERE datetime(scrape_timestamp) > datetime('now', '-1 day')
        """)
        recent = cursor.fetchone()[0]
        
        conn.close()
        
        embed = discord.Embed(title="📊 Logs Database Statistics", color=discord.Color.green())
        embed.add_field(name="Total Logs", value=f"{total_logs:,}", inline=True)
        embed.add_field(name="Training Courses", value=f"{total_training:,}", inline=True)
        embed.add_field(name="Last 24h", value=f"{recent:,}", inline=True)
        
        if log_types:
            types_str = "\n".join([f"{t[0]}: {t[1]:,}" for t in log_types[:5]])
            embed.add_field(name="Log Types", value=types_str, inline=False)
        
        if date_range[0]:
            embed.add_field(name="Oldest Log", value=date_range[0][:16], inline=True)
            embed.add_field(name="Newest Log", value=date_range[1][:16], inline=True)
        
        embed.set_footer(text=f"Database: {self.db_path}")
        await ctx.send(embed=embed)

async def setup(bot):
    await bot.add_cog(LogsScraper(bot))
