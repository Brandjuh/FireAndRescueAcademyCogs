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
        
        base_path = data_manager.cog_data_path(self.bot.get_cog("CookieManager"))
        db_dir = base_path.parent / "scraper_databases"
        db_dir.mkdir(exist_ok=True)
        self.db_path = db_dir / "buildings_v2.db"
        
        self.buildings_url = "https://www.missionchief.com/verband/gebauede"
        self.debug_mode = False
        
        self._init_database()
        self.scrape_task = self.bot.loop.create_task(self._background_scraper())
    
    def _init_database(self):
        """Initialize SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS buildings (
                building_id INTEGER,
                owner_name TEXT NOT NULL,
                building_type TEXT NOT NULL,
                classrooms INTEGER DEFAULT 0,
                timestamp TEXT NOT NULL,
                PRIMARY KEY (building_id, timestamp)
            )
        ''')
        conn.commit()
        conn.close()
    
    def cog_unload(self):
        if hasattr(self, 'scrape_task'):
            self.scrape_task.cancel()
    
    async def _background_scraper(self):
        """Background task - scrapes every hour at :45"""
        await self.bot.wait_until_ready()
        
        while True:
            try:
                now = datetime.now()
                next_run = now.replace(minute=45, second=0, microsecond=0)
                if now.minute >= 45:
                    next_run = next_run.replace(hour=now.hour + 1)
                wait_seconds = (next_run - now).total_seconds()
                
                await asyncio.sleep(wait_seconds)
                await self._scrape_all_buildings(ctx=None)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[BuildingsScraper] Background error: {e}")
                await asyncio.sleep(3600)
    
    async def _get_session(self, ctx=None):
        """Get authenticated session from CookieManager"""
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager:
            await self._debug_log("❌ CookieManager not loaded!", ctx)
            return None
        
        try:
            session = await cookie_manager.get_session()
            if not session:
                await self._debug_log("❌ Failed to get session", ctx)
                return None
            
            await self._debug_log("✅ Session obtained", ctx)
            return session
        except Exception as e:
            await self._debug_log(f"❌ Error: {str(e)}", ctx)
            return None
    
    async def _debug_log(self, message, ctx=None):
        """Log debug messages"""
        print(f"[BuildingsScraper] {message}")
        if self.debug_mode and ctx:
            try:
                await ctx.send(message)
            except:
                pass
    
    async def _scrape_all_buildings(self, ctx=None):
        """Scrape all buildings from the buildings page"""
        session = await self._get_session(ctx)
        if not session:
            if ctx:
                await ctx.send("❌ Failed to get session")
            return False
        
        await self._debug_log("🏢 Starting buildings scrape", ctx)
        
        try:
            async with session.get(self.buildings_url) as resp:
                if resp.status != 200:
                    await self._debug_log(f"❌ Bad status {resp.status}", ctx)
                    return False
                
                html = await resp.text()
                await self._debug_log(f"📄 HTML: {len(html)} chars", ctx)
                
                # Parse buildings
                soup = BeautifulSoup(html, 'html.parser')
                buildings = []
                
                # Find all building rows - look for links to /buildings/
                for link in soup.find_all('a', href=lambda x: x and '/buildings/' in str(x)):
                    # Extract building ID
                    match = re.search(r'/buildings/(\d+)', link['href'])
                    if not match:
                        continue
                    
                    building_id = int(match.group(1))
                    
                    # Get parent row for more info
                    row = link.find_parent('tr')
                    if not row:
                        continue
                    
                    # Extract data
                    building_name = link.get_text(strip=True)
                    
                    # Find owner (usually in another column)
                    cols = row.find_all('td')
                    owner_name = "Unknown"
                    classrooms = 0
                    
                    for col in cols:
                        # Look for owner link
                        owner_link = col.find('a', href=lambda x: x and '/users/' in str(x))
                        if owner_link:
                            owner_name = owner_link.get_text(strip=True)
                        
                        # Look for classroom count
                        text = col.get_text(strip=True)
                        classroom_match = re.search(r'(\d+)\s*classroom', text, re.IGNORECASE)
                        if classroom_match:
                            classrooms = int(classroom_match.group(1))
                    
                    buildings.append({
                        'building_id': building_id,
                        'owner_name': owner_name,
                        'building_type': building_name,
                        'classrooms': classrooms
                    })
                    
                    if self.debug_mode:
                        await self._debug_log(f"🏢 {owner_name}: {building_name} (ID: {building_id}, Classrooms: {classrooms})", ctx)
                
                if not buildings:
                    await self._debug_log("⚠️ No buildings found", ctx)
                    return False
                
                # Store in database
                timestamp = datetime.now().isoformat()
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                inserted = 0
                duplicates = 0
                
                for building in buildings:
                    try:
                        cursor.execute('''
                            INSERT INTO buildings (building_id, owner_name, building_type, classrooms, timestamp)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (building['building_id'], building['owner_name'], 
                              building['building_type'], building['classrooms'], timestamp))
                        inserted += 1
                    except sqlite3.IntegrityError:
                        duplicates += 1
                
                conn.commit()
                conn.close()
                
                await self._debug_log(f"💾 Database: {inserted} new, {duplicates} duplicates", ctx)
                
                if ctx:
                    await ctx.send(f"✅ Scraped {len(buildings)} buildings\n"
                                  f"💾 {inserted} new records, {duplicates} duplicates")
                
                return True
                
        except Exception as e:
            await self._debug_log(f"❌ Error: {str(e)}", ctx)
            if ctx:
                await ctx.send(f"❌ Scrape failed: {str(e)}")
            return False
    
    @commands.group(name="buildings")
    @commands.is_owner()
    async def buildings_group(self, ctx):
        """Buildings scraper commands"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @buildings_group.command(name="scrape")
    async def scrape_buildings(self, ctx):
        """Manually scrape buildings"""
        await ctx.send("🔄 Starting buildings scrape...")
        success = await self._scrape_all_buildings(ctx)
        
        if success:
            await ctx.send("✅ Buildings scrape completed")
        else:
            await ctx.send("❌ Buildings scrape failed")
    
    @buildings_group.command(name="debug")
    async def toggle_debug(self, ctx, mode: str = None):
        """Toggle debug mode (on/off)"""
        if mode is None:
            await ctx.send(f"Debug: {'ON' if self.debug_mode else 'OFF'}")
            return
        
        if mode.lower() in ['on', '1', 'true']:
            self.debug_mode = True
            await ctx.send("✅ Debug ON")
        else:
            self.debug_mode = False
            await ctx.send("✅ Debug OFF")
    
    @buildings_group.command(name="stats")
    async def show_stats(self, ctx):
        """Show buildings statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(DISTINCT building_id) FROM buildings")
        total_buildings = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT owner_name) FROM buildings")
        total_owners = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(classrooms) FROM buildings WHERE timestamp = (SELECT MAX(timestamp) FROM buildings)")
        total_classrooms = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM buildings")
        min_time, max_time = cursor.fetchone()
        
        conn.close()
        
        embed = discord.Embed(title="🏢 Buildings Statistics", color=discord.Color.blue())
        embed.add_field(name="Total Buildings", value=f"{total_buildings:,}", inline=True)
        embed.add_field(name="Total Owners", value=f"{total_owners:,}", inline=True)
        embed.add_field(name="Total Classrooms", value=f"{total_classrooms:,}", inline=True)
        
        if min_time and max_time:
            embed.add_field(name="Data Range", value=f"{min_time[:10]} to {max_time[:10]}", inline=False)
        
        embed.set_footer(text=f"Database: {self.db_path.name}")
        
        await ctx.send(embed=embed)
