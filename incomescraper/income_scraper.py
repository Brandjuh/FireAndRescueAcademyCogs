import discord
from redbot.core import commands, Config, data_manager
import aiohttp
import asyncio
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
import re

class IncomeScraper(commands.Cog):
    """Scrapes alliance income/expenses from MissionChief"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621003, force_registration=True)
        
        # Setup database path in shared location
        base_path = data_manager.cog_data_path(raw_name="scraper_databases")
        base_path.mkdir(parents=True, exist_ok=True)
        self.db_path = str(base_path / "income.db")
        
        self.base_url = "https://www.missionchief.com"
        self.income_url = f"{self.base_url}/verband/kasse"
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
        """Initialize SQLite database with schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS income_expenses (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_type TEXT,
                period_date TEXT,
                entry_type TEXT,
                description TEXT,
                amount INTEGER,
                scrape_timestamp TEXT,
                UNIQUE(period_type, period_date, entry_type, description, scrape_timestamp)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_period ON income_expenses(period_type, period_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_scrape_time ON income_expenses(scrape_timestamp)')
        
        conn.commit()
        conn.close()
    
    async def _get_cookie_manager(self):
        """Get CookieManager cog instance"""
        return self.bot.get_cog("CookieManager")
    
    async def _get_session(self):
        """Get authenticated session from CookieManager cog"""
        cookie_manager = await self._get_cookie_manager()
        if not cookie_manager:
            print("[IncomeScraper] CookieManager cog not loaded!")
            return None
        
        try:
            session = await cookie_manager.get_session()
            return session
        except Exception as e:
            print(f"[IncomeScraper] Failed to get session: {e}")
            return None
    
    async def _check_logged_in(self, html_content):
        """Check if still logged in"""
        soup = BeautifulSoup(html_content, 'html.parser')
        logout_button = soup.find('a', href='/users/sign_out')
        user_menu = soup.find('li', class_='dropdown user-menu')
        return logout_button is not None or user_menu is not None
    
    def _parse_amount(self, amount_str):
        """Parse amount string to integer"""
        # Remove currency symbols, commas, and convert to int
        cleaned = re.sub(r'[^\d-]', '', amount_str)
        try:
            return int(cleaned)
        except:
            return 0
    
    async def _scrape_income_tab(self, session, tab_type='daily'):
        """Scrape income/expense data from a specific tab (daily or monthly)"""
        url = f"{self.income_url}?tab={tab_type}"
        
        for attempt in range(3):
            try:
                await asyncio.sleep(1.5)
                
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"[IncomeScraper] Tab {tab_type} returned status {response.status}")
                        return []
                    
                    html = await response.text()
                    
                    if not await self._check_logged_in(html):
                        print(f"[IncomeScraper] Session expired, will retry on next run")
                        return []
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    data = []
                    scrape_timestamp = datetime.utcnow().isoformat()
                    
                    # Find all tables or sections with income/expense data
                    tables = soup.find_all('table', class_='table')
                    
                    for table in tables:
                        # Try to determine if it's income or expense table
                        table_header = table.find_previous('h3') or table.find_previous('h4')
                        section_type = "unknown"
                        
                        if table_header:
                            header_text = table_header.text.lower()
                            if 'income' in header_text or 'revenue' in header_text or 'einnahmen' in header_text:
                                section_type = "income"
                            elif 'expense' in header_text or 'cost' in header_text or 'ausgaben' in header_text:
                                section_type = "expense"
                        
                        rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')
                        
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 2:
                                # First column: description/date
                                description = cols[0].get_text(strip=True)
                                
                                # Last column usually contains amount
                                amount_str = cols[-1].get_text(strip=True)
                                amount = self._parse_amount(amount_str)
                                
                                # Extract period date if available
                                period_date = description if re.match(r'\d{4}-\d{2}', description) else datetime.utcnow().strftime('%Y-%m')
                                
                                data.append({
                                    'period_type': tab_type,
                                    'period_date': period_date,
                                    'entry_type': section_type,
                                    'description': description[:500],
                                    'amount': amount,
                                    'scrape_timestamp': scrape_timestamp
                                })
                    
                    return data
                    
            except asyncio.TimeoutError:
                print(f"[IncomeScraper] Timeout on tab {tab_type}, attempt {attempt + 1}")
                if attempt == 2:
                    return []
            except Exception as e:
                print(f"[IncomeScraper] Error scraping tab {tab_type}: {e}")
                if attempt == 2:
                    return []
        
        return []
    
    async def _scrape_all_income(self, ctx=None):
        """Scrape both daily and monthly income/expense tabs"""
        session = await self._get_session()
        if not session:
            if ctx:
                await ctx.send("❌ Failed to get session. Is CookieManager loaded and logged in?")
            return False
        
        all_data = []
        
        # Scrape daily tab
        daily_data = await self._scrape_income_tab(session, 'daily')
        all_data.extend(daily_data)
        
        # Scrape monthly tab
        monthly_data = await self._scrape_income_tab(session, 'monthly')
        all_data.extend(monthly_data)
        
        # Save to database
        if all_data:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for entry in all_data:
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO income_expenses 
                        (period_type, period_date, entry_type, description, amount, scrape_timestamp)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        entry['period_type'],
                        entry['period_date'],
                        entry['entry_type'],
                        entry['description'],
                        entry['amount'],
                        entry['scrape_timestamp']
                    ))
                except sqlite3.IntegrityError:
                    pass  # Duplicate entry, skip
            
            conn.commit()
            conn.close()
            
            if ctx:
                await ctx.send(f"✅ Scraped {len(all_data)} income/expense entries (daily: {len(daily_data)}, monthly: {len(monthly_data)})")
            return True
        else:
            if ctx:
                await ctx.send("⚠️ No income/expense data found")
            return False
    
    async def _background_scraper(self):
        """Background task that runs every hour"""
        await self.bot.wait_until_ready()
        await asyncio.sleep(1800)  # Stagger: 30 minutes offset
        
        while not self.bot.is_closed():
            try:
                print(f"[IncomeScraper] Starting automatic scrape at {datetime.utcnow()}")
                await self._scrape_all_income()
                print(f"[IncomeScraper] Automatic scrape completed")
            except Exception as e:
                print(f"[IncomeScraper] Background task error: {e}")
            
            await asyncio.sleep(3600)
    
    @commands.command(name="scrape_income")
    @commands.is_owner()
    async def scrape_income(self, ctx):
        """Manually trigger income/expenses scraping (Owner only)"""
        await ctx.send("🔄 Starting income/expenses scrape...")
        success = await self._scrape_all_income(ctx)
        if success:
            await ctx.send("✅ Income/expenses scrape completed successfully")

async def setup(bot):
    await bot.add_cog(IncomeScraper(bot))
