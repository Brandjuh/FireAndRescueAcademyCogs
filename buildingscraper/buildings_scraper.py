import discord
from redbot.core import commands, Config, data_manager
import aiohttp
import asyncio
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
import re

class BuildingsScraper(commands.Cog):
    """Scrapes alliance buildings from MissionChief"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621004, force_registration=True)
        
        # Setup database path in shared location
        base_path = data_manager.cog_data_path(raw_name="scraper_databases")
        base_path.mkdir(parents=True, exist_ok=True)
        self.db_path = str(base_path / "buildings.db")
        
        self.base_url = "https://www.missionchief.com"
        self.buildings_url = f"{self.base_url}/verband/gebauede"
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
            CREATE TABLE IF NOT EXISTS buildings (
                building_id INTEGER,
                owner_username TEXT,
                building_type TEXT,
                building_name TEXT,
                location TEXT,
                classrooms INTEGER,
                status TEXT,
                level INTEGER,
                scrape_timestamp TEXT,
                PRIMARY KEY (building_id, scrape_timestamp)
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON buildings(scrape_timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_building_id ON buildings(building_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_owner ON buildings(owner_username)')
        
        conn.commit()
        conn.close()
    
    async def _get_cookie_manager(self):
        """Get CookieManager cog instance"""
        return self.bot.get_cog("CookieManager")
    
    async def _get_session(self):
        """Get authenticated session from CookieManager cog"""
        cookie_manager = await self._get_cookie_manager()
        if not cookie_manager:
            print("[BuildingsScraper] CookieManager cog not loaded!")
            return None
        
        try:
            session = await cookie_manager.get_session()
            return session
        except Exception as e:
            print(f"[BuildingsScraper] Failed to get session: {e}")
            return None
    
    async def _check_logged_in(self, html_content):
        """Check if still logged in"""
        soup = BeautifulSoup(html_content, 'html.parser')
        logout_button = soup.find('a', href='/users/sign_out')
        user_menu = soup.find('li', class_='dropdown user-menu')
        return logout_button is not None or user_menu is not None
    
    def _extract_number(self, text):
        """Extract first number from text"""
        match = re.search(r'\d+', str(text))
        return int(match.group()) if match else 0
    
    async def _scrape_buildings(self, session):
        """Scrape buildings data - handles scrollable lists"""
        url = self.buildings_url
        
        for attempt in range(3):
            try:
                await asyncio.sleep(2)
                
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"[BuildingsScraper] Page returned status {response.status}")
                        return []
                    
                    html = await response.text()
                    
                    if not await self._check_logged_in(html):
                        print(f"[BuildingsScraper] Session expired, will retry on next run")
                        return []
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    buildings_data = []
                    scrape_timestamp = datetime.utcnow().isoformat()
                    
                    # Method 1: Try to find table with buildings
                    table = soup.find('table', class_='table')
                    if table:
                        rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')
                        
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) >= 3:
                                # Extract building ID from link or data attribute
                                building_link = row.find('a', href=re.compile(r'/buildings/\d+'))
                                building_id = 0
                                if building_link:
                                    match = re.search(r'/buildings/(\d+)', building_link.get('href', ''))
                                    if match:
                                        building_id = int(match.group(1))
                                
                                # Owner
                                owner = cols[0].get_text(strip=True) if len(cols) > 0 else ""
                                
                                # Building name/type
                                building_name = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                                
                                # Location
                                location = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                                
                                # Classrooms (if available)
                                classrooms = 0
                                if len(cols) > 3:
                                    classrooms = self._extract_number(cols[3].get_text(strip=True))
                                
                                # Status/Level
                                status = "active"
                                level = 1
                                if len(cols) > 4:
                                    status_text = cols[4].get_text(strip=True).lower()
                                    if "building" in status_text or "constructing" in status_text:
                                        status = "building"
                                    level = self._extract_number(cols[4].get_text(strip=True))
                                
                                # Determine building type from name
                                building_type = "unknown"
                                name_lower = building_name.lower()
                                if "fire" in name_lower or "station" in name_lower:
                                    building_type = "fire_station"
                                elif "police" in name_lower:
                                    building_type = "police_station"
                                elif "hospital" in name_lower or "rescue" in name_lower:
                                    building_type = "hospital"
                                elif "dispatch" in name_lower:
                                    building_type = "dispatch"
                                elif "school" in name_lower or "academy" in name_lower or "training" in name_lower:
                                    building_type = "training_center"
                                
                                buildings_data.append({
                                    'building_id': building_id,
                                    'owner_username': owner,
                                    'building_type': building_type,
                                    'building_name': building_name,
                                    'location': location,
                                    'classrooms': classrooms,
                                    'status': status,
                                    'level': level,
                                    'scrape_timestamp': scrape_timestamp
                                })
                    
                    # Method 2: Try to find scrollable list or div-based layout
                    building_divs = soup.find_all('div', class_=re.compile(r'building|gebaeude'))
                    for div in building_divs:
                        # Extract building data from div structure
                        building_id_elem = div.get('data-building-id') or div.get('id')
                        if building_id_elem:
                            building_id = self._extract_number(building_id_elem)
                            
                            name_elem = div.find(class_=re.compile(r'name|title'))
                            building_name = name_elem.get_text(strip=True) if name_elem else "Unknown"
                            
                            owner_elem = div.find(class_=re.compile(r'owner|user'))
                            owner = owner_elem.get_text(strip=True) if owner_elem else "Unknown"
                            
                            buildings_data.append({
                                'building_id': building_id,
                                'owner_username': owner,
                                'building_type': 'unknown',
                                'building_name': building_name,
                                'location': '',
                                'classrooms': 0,
                                'status': 'active',
                                'level': 1,
                                'scrape_timestamp': scrape_timestamp
                            })
                    
                    return buildings_data
                    
            except asyncio.TimeoutError:
                print(f"[BuildingsScraper] Timeout, attempt {attempt + 1}")
                if attempt == 2:
                    return []
            except Exception as e:
                print(f"[BuildingsScraper] Error scraping buildings: {e}")
                if attempt == 2:
                    return []
        
        return []
    
    async def _scrape_all_buildings(self, ctx=None):
        """Scrape all buildings"""
        session = await self._get_session()
        if not session:
            if ctx:
                await ctx.send("❌ Failed to get session. Is CookieManager loaded and logged in?")
            return False
        
        buildings = await self._scrape_buildings(session)
        
        # Save to database
        if buildings:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for building in buildings:
                cursor.execute('''
                    INSERT OR REPLACE INTO buildings 
                    (building_id, owner_username, building_type, building_name, location, 
                     classrooms, status, level, scrape_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    building['building_id'],
                    building['owner_username'],
                    building['building_type'],
                    building['building_name'],
                    building['location'],
                    building['classrooms'],
                    building['status'],
                    building['level'],
                    building['scrape_timestamp']
                ))
            
            conn.commit()
            conn.close()
            
            # Count classrooms
            total_classrooms = sum(b['classrooms'] for b in buildings)
            
            if ctx:
                await ctx.send(f"✅ Scraped {len(buildings)} buildings ({total_classrooms} total classrooms)")
            return True
        else:
            if ctx:
                await ctx.send("⚠️ No buildings data found")
            return False
    
    async def _background_scraper(self):
        """Background task that runs every hour"""
        await self.bot.wait_until_ready()
        await asyncio.sleep(2700)  # Stagger: 45 minutes offset
        
        while not self.bot.is_closed():
            try:
                print(f"[BuildingsScraper] Starting automatic scrape at {datetime.utcnow()}")
                await self._scrape_all_buildings()
                print(f"[BuildingsScraper] Automatic scrape completed")
            except Exception as e:
                print(f"[BuildingsScraper] Background task error: {e}")
            
            await asyncio.sleep(3600)
    
    @commands.command(name="scrape_buildings")
    @commands.is_owner()
    async def scrape_buildings(self, ctx):
        """Manually trigger buildings scraping (Owner only)"""
        await ctx.send("🔄 Starting buildings scrape...")
        success = await self._scrape_all_buildings(ctx)
        if success:
            await ctx.send("✅ Buildings scrape completed successfully")

async def setup(bot):
    await bot.add_cog(BuildingsScraper(bot))
