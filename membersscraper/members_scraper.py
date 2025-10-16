import discord
from redbot.core import commands, Config
import aiohttp
import asyncio
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
import sys
import os

# Import cookie manager
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from cookie_manager import CookieManager

class MembersScraper(commands.Cog):
    """Scrapes alliance members data from MissionChief"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621001, force_registration=True)
        self.db_path = "members.db"
        self.base_url = "https://www.missionchief.com"
        self.members_url = f"{self.base_url}/verband/mitglieder/1621"
        self.cookie_manager = CookieManager()
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
    
    async def _get_session(self):
        """Get authenticated session from cookie manager"""
        try:
            session = await self.cookie_manager.get_session()
            return session
        except Exception as e:
            print(f"[MembersScraper] Failed to get session: {e}")
            return None
    
    async def _check_logged_in(self, html_content):
        """Check if still logged in by looking for logout button or user menu"""
        soup = BeautifulSoup(html_content, 'html.parser')
        # Check for common logged-in indicators
        logout_button = soup.find('a', href='/users/sign_out')
        user_menu = soup.find('li', class_='dropdown user-menu')
        return logout_button is not None or user_menu is not None
    
    async def _scrape_members_page(self, session, page_num):
        """Scrape a single page of members"""
        url = f"{self.members_url}?page={page_num}"
        
        for attempt in range(3):
            try:
                await asyncio.sleep(1.5)  # Rate limiting
                
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"[MembersScraper] Page {page_num} returned status {response.status}")
                        return []
                    
                    html = await response.text()
                    
                    # Check if still logged in
                    if not await self._check_logged_in(html):
                        print(f"[MembersScraper] Session expired, re-authenticating...")
                        session = await self._get_session()
                        if not session:
                            return []
                        continue
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    members_data = []
                    timestamp = datetime.utcnow().isoformat()
                    
                    # Find members table
                    table = soup.find('table', class_='table')
                    if not table:
                        print(f"[MembersScraper] No table found on page {page_num}")
                        return []
                    
                    rows = table.find('tbody').find_all('tr')
                    
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 4:
                            # Extract member data
                            member_link = cols[0].find('a')
                            if member_link:
                                member_id = member_link.get('href', '').split('/')[-1]
                                username = member_link.text.strip()
                            else:
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
                    
                    return members_data
                    
            except asyncio.TimeoutError:
                print(f"[MembersScraper] Timeout on page {page_num}, attempt {attempt + 1}")
                if attempt == 2:
                    return []
            except Exception as e:
                print(f"[MembersScraper] Error scraping page {page_num}: {e}")
                if attempt == 2:
                    return []
        
        return []
    
    async def _scrape_all_members(self, ctx=None):
        """Scrape all pages of members"""
        session = await self._get_session()
        if not session:
            if ctx:
                await ctx.send("❌ Failed to authenticate with MissionChief")
            return False
        
        all_members = []
        page = 1
        max_pages = 50  # Safety limit
        
        while page <= max_pages:
            members = await self._scrape_members_page(session, page)
            
            if not members:
                # No more members found, end pagination
                break
            
            all_members.extend(members)
            page += 1
        
        # Save to database
        if all_members:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for member in all_members:
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
            
            conn.commit()
            conn.close()
            
            if ctx:
                await ctx.send(f"✅ Scraped {len(all_members)} members across {page - 1} pages")
            return True
        else:
            if ctx:
                await ctx.send("⚠️ No members data found")
            return False
    
    async def _background_scraper(self):
        """Background task that runs every hour"""
        await self.bot.wait_until_ready()
        # Stagger: start immediately (0 min offset)
        
        while not self.bot.is_closed():
            try:
                print(f"[MembersScraper] Starting automatic scrape at {datetime.utcnow()}")
                await self._scrape_all_members()
                print(f"[MembersScraper] Automatic scrape completed")
            except Exception as e:
                print(f"[MembersScraper] Background task error: {e}")
                # Optionally send error to Discord channel
                # You can add channel notification here if needed
            
            await asyncio.sleep(3600)  # Wait 1 hour
    
    @commands.command(name="scrape_members")
    @commands.is_owner()
    async def scrape_members(self, ctx):
        """Manually trigger members scraping (Owner only)"""
        await ctx.send("🔄 Starting members scrape...")
        success = await self._scrape_all_members(ctx)
        if success:
            await ctx.send("✅ Members scrape completed successfully")

def setup(bot):
    bot.add_cog(MembersScraper(bot))
