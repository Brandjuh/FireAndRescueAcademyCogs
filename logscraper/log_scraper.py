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
    
    async def _get_cookie_manager(self):
        """Get CookieManager cog instance"""
        return self.bot.get_cog("CookieManager")
    
    async def _get_session(self):
        """Get authenticated session from CookieManager cog"""
        cookie_manager = await self._get_cookie_manager()
        if not cookie_manager:
            print("[LogsScraper] CookieManager cog not loaded!")
            return None
        
        try:
            session = await cookie_manager.get_session()
            return session
        except Exception as e:
            print(f"[LogsScraper] Failed to get session: {e}")
            return None
    
    async def _check_logged_in(self, html_content):
        """Check if still logged in"""
        soup = BeautifulSoup(html_content, 'html.parser')
        logout_button = soup.find('a', href='/users/sign_out')
        user_menu = soup.find('li', class_='dropdown user-menu')
        return logout_button is not None or user_menu is not None
    
    def _parse_log_entry(self, row, scrape_timestamp):
        """Parse a single log entry row"""
        cols = row.find_all('td')
        if len(cols) < 2:
            return None
        
        # Timestamp column
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
            # Parse: "Username started education Course Name"
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
            'action': action_text[:200],  # Limit length
            'details': details[:500],
            'log_timestamp': timestamp_col,
            'scrape_timestamp': scrape_timestamp
        }
    
    async def _scrape_logs_page(self, session, page_num):
        """Scrape a single page of logs"""
        url = f"{self.logs_url}?page={page_num}"
        
        for attempt in range(3):
            try:
                await asyncio.sleep(1.5)
                
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"[LogsScraper] Page {page_num} returned status {response.status}")
                        return []
                    
                    html = await response.text()
                    
                    if not await self._check_logged_in(html):
                        print(f"[LogsScraper] Session expired, will retry on next run")
                        return []
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    logs_data = []
                    scrape_timestamp = datetime.utcnow().isoformat()
                    
                    # Find logs table
                    table = soup.find('table', class_='table')
                    if not table:
                        print(f"[LogsScraper] No table found on page {page_num}")
                        return []
                    
                    rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')
                    
                    for row in rows:
                        log_entry = self._parse_log_entry(row, scrape_timestamp)
                        if log_entry:
                            logs_data.append(log_entry)
                    
                    return logs_data
                    
            except asyncio.TimeoutError:
                print(f"[LogsScraper] Timeout on page {page_num}, attempt {attempt + 1}")
                if attempt == 2:
                    return []
            except Exception as e:
                print(f"[LogsScraper] Error scraping page {page_num}: {e}")
                if attempt == 2:
                    return []
        
        return []
    
    async def _scrape_all_logs(self, ctx=None):
        """Scrape all pages of logs"""
        session = await self._get_session()
        if not session:
            if ctx:
                await ctx.send("❌ Failed to get session. Is CookieManager loaded and logged in?")
            return False
        
        all_logs = []
        page = 1
        max_pages = 100  # Logs can have many pages
        
        while page <= max_pages:
            logs = await self._scrape_logs_page(session, page)
            
            if not logs:
                break
            
            all_logs.extend(logs)
            page += 1
        
        # Save to database
        if all_logs:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            training_count = 0
            
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
                            pass  # Already exists
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
                    except sqlite3.IntegrityError:
                        pass  # Duplicate
            
            conn.commit()
            conn.close()
            
            if ctx:
                await ctx.send(f"✅ Scraped {len(all_logs)} log entries ({training_count} training courses) across {page - 1} pages")
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
                print(f"[LogsScraper] Starting automatic scrape at {datetime.utcnow()}")
                await self._scrape_all_logs()
                print(f"[LogsScraper] Automatic scrape completed")
            except Exception as e:
                print(f"[LogsScraper] Background task error: {e}")
            
            await asyncio.sleep(3600)
    
    @commands.command(name="scrape_logs")
    @commands.is_owner()
    async def scrape_logs(self, ctx):
        """Manually trigger logs scraping (Owner only)"""
        await ctx.send("🔄 Starting logs scrape...")
        success = await self._scrape_all_logs(ctx)
        if success:
            await ctx.send("✅ Logs scrape completed successfully")

async def setup(bot):
    await bot.add_cog(LogsScraper(bot))
