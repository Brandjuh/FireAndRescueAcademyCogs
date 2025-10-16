import discord
from redbot.core import commands, Config, data_manager
import aiohttp
import asyncio
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
import re

class ApplicationsScraper(commands.Cog):
    """Scrapes alliance applications from MissionChief"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621005, force_registration=True)
        
        # Setup database path in shared location
        base_path = data_manager.cog_data_path(raw_name="scraper_databases")
        base_path.mkdir(parents=True, exist_ok=True)
        self.db_path = str(base_path / "applications.db")
        
        self.base_url = "https://www.missionchief.com"
        self.applications_url = f"{self.base_url}/verband/bewerbungen"
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
            CREATE TABLE IF NOT EXISTS applications (
                application_id INTEGER PRIMARY KEY AUTOINCREMENT,
                applicant_name TEXT,
                applicant_id INTEGER,
                application_date TEXT,
                status TEXT,
                message TEXT,
                credits INTEGER,
                buildings INTEGER,
                scrape_timestamp TEXT,
                UNIQUE(applicant_id, application_date, scrape_timestamp)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON applications(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_scrape_time ON applications(scrape_timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_applicant ON applications(applicant_id)')
        
        conn.commit()
        conn.close()
    
    async def _get_cookie_manager(self):
        """Get CookieManager cog instance"""
        return self.bot.get_cog("CookieManager")
    
    async def _get_session(self):
        """Get authenticated session from CookieManager cog"""
        cookie_manager = await self._get_cookie_manager()
        if not cookie_manager:
            print("[ApplicationsScraper] CookieManager cog not loaded!")
            return None
        
        try:
            session = await cookie_manager.get_session()
            return session
        except Exception as e:
            print(f"[ApplicationsScraper] Failed to get session: {e}")
            return None
    
    async def _check_logged_in(self, html_content):
        """Check if still logged in"""
        soup = BeautifulSoup(html_content, 'html.parser')
        logout_button = soup.find('a', href='/users/sign_out')
        user_menu = soup.find('li', class_='dropdown user-menu')
        return logout_button is not None or user_menu is not None
    
    def _parse_application(self, element, scrape_timestamp):
        """Parse a single application element"""
        # Applications might be in table rows or card/div elements
        
        applicant_name = ""
        applicant_id = 0
        application_date = ""
        status = "pending"
        message = ""
        credits = 0
        buildings = 0
        
        # Try to find applicant link
        applicant_link = element.find('a', href=lambda x: x and '/profile/' in x)
        if applicant_link:
            applicant_name = applicant_link.get_text(strip=True)
            # Extract ID from URL
            href = applicant_link.get('href', '')
            id_match = href.split('/profile/')[-1].split('/')[0]
            applicant_id = int(id_match) if id_match.isdigit() else 0
        
        # Find date
        date_elem = element.find('time') or element.find(class_=lambda x: x and 'date' in x.lower())
        if date_elem:
            application_date = date_elem.get('datetime') or date_elem.get_text(strip=True)
        
        # Find message
        message_elem = element.find('p') or element.find(class_=lambda x: x and 'message' in x.lower())
        if message_elem:
            message = message_elem.get_text(strip=True)[:1000]  # Limit length
        
        # Find status from buttons or badges
        accept_btn = element.find('button', class_=lambda x: x and 'accept' in x.lower())
        reject_btn = element.find('button', class_=lambda x: x and 'reject' in x.lower())
        status_badge = element.find('span', class_=lambda x: x and ('badge' in x.lower() or 'label' in x.lower()))
        
        if status_badge:
            status_text = status_badge.get_text(strip=True).lower()
            if 'accept' in status_text or 'approved' in status_text:
                status = "accepted"
            elif 'reject' in status_text or 'declined' in status_text:
                status = "rejected"
        elif accept_btn or reject_btn:
            status = "pending"
        
        # Try to extract credits and buildings from text
        text_content = element.get_text()
        
        credits_match = re.search(r'(\d+(?:,\d+)*)\s*(?:credits|coins|\$)', text_content, re.IGNORECASE)
        if credits_match:
            credits = int(credits_match.group(1).replace(',', ''))
        
        buildings_match = re.search(r'(\d+)\s*(?:buildings|stations)', text_content, re.IGNORECASE)
        if buildings_match:
            buildings = int(buildings_match.group(1))
        
        return {
            'applicant_name': applicant_name,
            'applicant_id': applicant_id,
            'application_date': application_date or datetime.utcnow().isoformat(),
            'status': status,
            'message': message,
            'credits': credits,
            'buildings': buildings,
            'scrape_timestamp': scrape_timestamp
        }
    
    async def _scrape_applications(self, session):
        """Scrape applications page"""
        url = self.applications_url
        
        for attempt in range(3):
            try:
                await asyncio.sleep(1.5)
                
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"[ApplicationsScraper] Page returned status {response.status}")
                        return []
                    
                    html = await response.text()
                    
                    if not await self._check_logged_in(html):
                        print(f"[ApplicationsScraper] Session expired, will retry on next run")
                        return []
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    applications_data = []
                    scrape_timestamp = datetime.utcnow().isoformat()
                    
                    # Method 1: Look for table with applications
                    table = soup.find('table', class_='table')
                    if table:
                        rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')
                        
                        for row in rows:
                            app_data = self._parse_application(row, scrape_timestamp)
                            if app_data['applicant_name']:  # Valid application
                                applications_data.append(app_data)
                    
                    # Method 2: Look for card/panel based layout
                    cards = soup.find_all('div', class_=lambda x: x and ('card' in x.lower() or 'panel' in x.lower()))
                    for card in cards:
                        app_data = self._parse_application(card, scrape_timestamp)
                        if app_data['applicant_name']:
                            applications_data.append(app_data)
                    
                    # Method 3: Look for list items
                    list_items = soup.find_all('li', class_=lambda x: x and 'application' in x.lower())
                    for item in list_items:
                        app_data = self._parse_application(item, scrape_timestamp)
                        if app_data['applicant_name']:
                            applications_data.append(app_data)
                    
                    return applications_data
                    
            except asyncio.TimeoutError:
                print(f"[ApplicationsScraper] Timeout, attempt {attempt + 1}")
                if attempt == 2:
                    return []
            except Exception as e:
                print(f"[ApplicationsScraper] Error scraping applications: {e}")
                if attempt == 2:
                    return []
        
        return []
    
    async def _scrape_all_applications(self, ctx=None):
        """Scrape all applications"""
        session = await self._get_session()
        if not session:
            if ctx:
                await ctx.send("❌ Failed to get session. Is CookieManager loaded and logged in?")
            return False
        
        applications = await self._scrape_applications(session)
        
        # Save to database
        if applications:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            new_count = 0
            for app in applications:
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO applications 
                        (applicant_name, applicant_id, application_date, status, message, 
                         credits, buildings, scrape_timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        app['applicant_name'],
                        app['applicant_id'],
                        app['application_date'],
                        app['status'],
                        app['message'],
                        app['credits'],
                        app['buildings'],
                        app['scrape_timestamp']
                    ))
                    if cursor.rowcount > 0:
                        new_count += 1
                except sqlite3.IntegrityError:
                    pass  # Duplicate
            
            conn.commit()
            conn.close()
            
            if ctx:
                pending = sum(1 for a in applications if a['status'] == 'pending')
                await ctx.send(f"✅ Scraped {len(applications)} applications ({pending} pending, {new_count} new)")
            return True
        else:
            if ctx:
                await ctx.send("ℹ️ No applications found (this is normal if there are no pending applications)")
            return True  # Not an error, just no applications
    
    async def _background_scraper(self):
        """Background task that runs every hour"""
        await self.bot.wait_until_ready()
        await asyncio.sleep(3300)  # Stagger: 55 minutes offset
        
        while not self.bot.is_closed():
            try:
                print(f"[ApplicationsScraper] Starting automatic scrape at {datetime.utcnow()}")
                await self._scrape_all_applications()
                print(f"[ApplicationsScraper] Automatic scrape completed")
            except Exception as e:
                print(f"[ApplicationsScraper] Background task error: {e}")
            
            await asyncio.sleep(3600)
    
    @commands.command(name="scrape_applications")
    @commands.is_owner()
    async def scrape_applications(self, ctx):
        """Manually trigger applications scraping (Owner only)"""
        await ctx.send("🔄 Starting applications scrape...")
        success = await self._scrape_all_applications(ctx)
        if success:
            await ctx.send("✅ Applications scrape completed successfully")

async def setup(bot):
    await bot.add_cog(ApplicationsScraper(bot))
