import discord
from redbot.core import commands, Config, data_manager
import aiohttp
import asyncio
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
import re
import hashlib

class LogsScraper(commands.Cog):
    """Scrapes alliance logs from MissionChief with COMPLETE data"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621002, force_registration=True)
        
        base_path = data_manager.cog_data_path(self.bot.get_cog("CookieManager"))
        db_dir = base_path.parent / "scraper_databases"
        db_dir.mkdir(exist_ok=True)
        self.db_path = db_dir / "logs_v3.db"  # New database with full schema
        
        self.logs_url = "https://www.missionchief.com/alliance_logfiles"
        self.debug_mode = False
        
        self._init_database()
        self.scrape_task = self.bot.loop.create_task(self._background_scraper())
    
    def _init_database(self):
        """Initialize SQLite database with complete schema"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE,
                ts TEXT,
                action_key TEXT,
                action_text TEXT,
                executed_name TEXT,
                executed_mc_id TEXT,
                executed_url TEXT,
                affected_name TEXT,
                affected_type TEXT,
                affected_mc_id TEXT,
                affected_url TEXT,
                description TEXT,
                scraped_at TEXT,
                contribution_amount INTEGER DEFAULT 0
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(ts)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_hash ON logs(hash)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_logs_action_key ON logs(action_key)')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS training_courses (
                username TEXT NOT NULL,
                course_name TEXT NOT NULL,
                log_timestamp TEXT NOT NULL,
                occurrence INTEGER DEFAULT 1,
                PRIMARY KEY (username, course_name, log_timestamp, occurrence)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def cog_unload(self):
        if hasattr(self, 'scrape_task'):
            self.scrape_task.cancel()
    
    async def _background_scraper(self):
        """Background task - scrapes every hour at :15"""
        await self.bot.wait_until_ready()
        
        while True:
            try:
                now = datetime.now()
                next_run = now.replace(minute=15, second=0, microsecond=0)
                if now.minute >= 15:
                    next_run = next_run.replace(hour=now.hour + 1)
                wait_seconds = (next_run - now).total_seconds()
                
                await asyncio.sleep(wait_seconds)
                await self._scrape_all_logs(ctx=None, max_pages=10)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[LogsScraper] Background error: {e}")
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
        print(f"[LogsScraper] {message}")
        if self.debug_mode and ctx:
            try:
                await ctx.send(message)
            except:
                pass
    
    async def _scrape_logs_page(self, session, page_num, ctx=None):
        """Scrape a single page of logs with COMPLETE data extraction"""
        url = f"{self.logs_url}?page={page_num}"
        await self._debug_log(f"🌐 Page {page_num}: {url}", ctx)
        
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    await self._debug_log(f"❌ Bad status {resp.status}", ctx)
                    return []
                
                html = await resp.text()
                soup = BeautifulSoup(html, 'html.parser')
                logs = []
                
                # Find the logs table
                table = soup.find('table', class_='table')
                if not table:
                    return []
                
                tbody = table.find('tbody')
                if not tbody:
                    return []
                
                # Process each row
                for tr in tbody.find_all('tr'):
                    cols = tr.find_all('td')
                    if len(cols) < 4:
                        continue
                    
                    # Column 0: Date/Time
                    timestamp = cols[0].get_text(strip=True).replace('\xa0', ' ')
                    
                    # Column 1: Executed by (with user link)
                    executed_name = ""
                    executed_mc_id = ""
                    executed_url = ""
                    
                    user_link = cols[1].find('a', href=True)
                    if user_link:
                        executed_name = user_link.get_text(strip=True)
                        href = user_link['href']
                        user_id_match = re.search(r'/users/(\d+)', href)
                        if user_id_match:
                            executed_mc_id = user_id_match.group(1)
                            executed_url = f"https://www.missionchief.com{href}"
                    
                    # Determine action_key from icon
                    action_key = "unknown"
                    img = cols[1].find('img', src=True)
                    if img:
                        icon_match = re.search(r'/alliance_log/([^.]+)\.png', img['src'])
                        if icon_match:
                            action_key = icon_match.group(1)
                    
                    # Column 2: Description + contribution amount
                    desc_col = cols[2]
                    contribution_amount = 0
                    
                    # Extract contribution from <span class="label...">
                    credit_span = desc_col.find('span', class_='label')
                    if credit_span:
                        credit_text = credit_span.get_text(strip=True)
                        credit_span.decompose()  # Remove from tree
                        
                        # Parse credit amount (e.g., "-500 Credits" or "+1000 Credits")
                        credit_match = re.search(r'([+-]?\d+)', credit_text)
                        if credit_match:
                            contribution_amount = int(credit_match.group(1))
                    
                    description = desc_col.get_text(strip=True)
                    action_text = description
                    
                    # Column 3: Affected (building/mission/vehicle link)
                    affected_name = ""
                    affected_mc_id = ""
                    affected_url = ""
                    affected_type = ""
                    
                    affected_link = cols[3].find('a', href=True)
                    if affected_link:
                        affected_name = affected_link.get_text(strip=True)
                        href = affected_link['href']
                        affected_url = f"https://www.missionchief.com{href}"
                        
                        # Determine type from URL
                        if '/buildings/' in href:
                            affected_type = 'building'
                            id_match = re.search(r'/buildings/(\d+)', href)
                            if id_match:
                                affected_mc_id = id_match.group(1)
                        elif '/missions/' in href:
                            affected_type = 'mission'
                            id_match = re.search(r'/missions/(\d+)', href)
                            if id_match:
                                affected_mc_id = id_match.group(1)
                        elif '/vehicles/' in href:
                            affected_type = 'vehicle'
                            id_match = re.search(r'/vehicles/(\d+)', href)
                            if id_match:
                                affected_mc_id = id_match.group(1)
                        elif '/users/' in href:
                            affected_type = 'user'
                            id_match = re.search(r'/users/(\d+)', href)
                            if id_match:
                                affected_mc_id = id_match.group(1)
                    
                    # Generate unique hash for deduplication
                    hash_string = f"{timestamp}{action_key}{executed_name}{affected_name}{description}"
                    log_hash = hashlib.sha256(hash_string.encode()).hexdigest()
                    
                    logs.append({
                        'hash': log_hash,
                        'ts': timestamp,
                        'action_key': action_key,
                        'action_text': action_text,
                        'executed_name': executed_name,
                        'executed_mc_id': executed_mc_id,
                        'executed_url': executed_url,
                        'affected_name': affected_name,
                        'affected_type': affected_type,
                        'affected_mc_id': affected_mc_id,
                        'affected_url': affected_url,
                        'description': description,
                        'contribution_amount': contribution_amount
                    })
                
                if self.debug_mode and logs:
                    await self._debug_log(f"✅ Page {page_num}: {len(logs)} logs", ctx)
                
                return logs
                
        except Exception as e:
            await self._debug_log(f"❌ Error page {page_num}: {str(e)}", ctx)
            import traceback
            traceback.print_exc()
            return []
    
    async def _scrape_all_logs(self, ctx=None, max_pages=100):
        """Scrape all pages of logs"""
        session = await self._get_session(ctx)
        if not session:
            if ctx:
                await ctx.send("❌ Failed to get session")
            return False
        
        await self._debug_log(f"🚀 Starting logs scrape (max {max_pages} pages)", ctx)
        
        all_logs = []
        page = 1
        empty_count = 0
        
        while page <= max_pages:
            logs = await self._scrape_logs_page(session, page, ctx)
            
            if not logs:
                empty_count += 1
                if empty_count >= 3:
                    await self._debug_log(f"⛔ Stopped after 3 empty pages", ctx)
                    break
            else:
                all_logs.extend(logs)
                empty_count = 0
                
                # Progress update every 50 pages
                if page % 50 == 0 and ctx:
                    await ctx.send(f"⏳ Progress: {page}/{max_pages} pages, {len(all_logs)} logs collected")
            
            page += 1
            await asyncio.sleep(1.5)
        
        if not all_logs:
            await self._debug_log("⚠️ No logs found", ctx)
            return False
        
        # Store in database
        scraped_at = datetime.now().isoformat()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        inserted = 0
        duplicates = 0
        training_inserted = 0
        
        for log in all_logs:
            try:
                cursor.execute('''
                    INSERT INTO logs (hash, ts, action_key, action_text, executed_name, executed_mc_id, executed_url,
                                     affected_name, affected_type, affected_mc_id, affected_url, description, 
                                     scraped_at, contribution_amount)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (log['hash'], log['ts'], log['action_key'], log['action_text'], log['executed_name'],
                      log['executed_mc_id'], log['executed_url'], log['affected_name'], log['affected_type'],
                      log['affected_mc_id'], log['affected_url'], log['description'], scraped_at,
                      log['contribution_amount']))
                inserted += 1
                
                # If it's a training course, also store in training_courses table
                if log['action_key'] in ['created_course', 'created_a_course', 'course_completed']:
                    cursor.execute('''
                        SELECT COUNT(*) FROM training_courses 
                        WHERE username = ? AND course_name = ? AND log_timestamp = ?
                    ''', (log['executed_name'], log['action_text'], log['ts']))
                    count = cursor.fetchone()[0]
                    
                    if count < 4:
                        try:
                            cursor.execute('''
                                INSERT INTO training_courses (username, course_name, log_timestamp, occurrence)
                                VALUES (?, ?, ?, ?)
                            ''', (log['executed_name'], log['action_text'], log['ts'], count + 1))
                            training_inserted += 1
                        except sqlite3.IntegrityError:
                            pass
                
            except sqlite3.IntegrityError:
                duplicates += 1
        
        conn.commit()
        conn.close()
        
        await self._debug_log(f"💾 Database: {inserted} new logs, {training_inserted} training courses, {duplicates} duplicates", ctx)
        
        if ctx:
            await ctx.send(f"✅ Scraped {len(all_logs)} logs\n"
                          f"💾 {inserted} new logs, {training_inserted} training courses, {duplicates} duplicates")
        
        return True
    
    @commands.group(name="logs")
    @commands.is_owner()
    async def logs_group(self, ctx):
        """Logs scraper commands"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @logs_group.command(name="scrape")
    async def scrape_logs(self, ctx, max_pages: int = 10):
        """Manually scrape logs"""
        await ctx.send(f"🔄 Starting logs scrape (max {max_pages} pages)...")
        success = await self._scrape_all_logs(ctx, max_pages)
        
        if success:
            await ctx.send("✅ Logs scrape completed")
        else:
            await ctx.send("❌ Logs scrape failed")
    
    @logs_group.command(name="backfill")
    async def backfill_logs(self, ctx, max_pages: int = 200):
        """Back-fill ALL historical logs"""
        if max_pages < 1 or max_pages > 5000:
            await ctx.send("❌ Max pages must be between 1 and 5000")
            return
        
        await ctx.send(f"🔄 Starting back-fill (up to {max_pages} pages)...")
        await ctx.send("⏳ This preserves original timestamps")
        
        success = await self._scrape_all_logs(ctx, max_pages)
        
        if success:
            await ctx.send("✅ Back-fill completed!")
        else:
            await ctx.send("❌ Back-fill failed")
    
    @logs_group.command(name="debug")
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
    
    @logs_group.command(name="stats")
    async def show_stats(self, ctx):
        """Show logs statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM logs")
        total_logs = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM training_courses")
        total_training = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(ts), MAX(ts) FROM logs")
        min_time, max_time = cursor.fetchone()
        
        cursor.execute("SELECT MAX(id) FROM logs")
        max_id = cursor.fetchone()[0]
        
        conn.close()
        
        embed = discord.Embed(title="📜 Logs Statistics", color=discord.Color.green())
        embed.add_field(name="Total Logs", value=f"{total_logs:,}", inline=True)
        embed.add_field(name="Training Courses", value=f"{total_training:,}", inline=True)
        embed.add_field(name="Max Log ID", value=f"{max_id:,}" if max_id else "0", inline=True)
        
        if min_time and max_time:
            embed.add_field(name="Data Range", value=f"{min_time[:10]} to {max_time[:10]}", inline=False)
        
        embed.set_footer(text=f"Database: {self.db_path.name}")
        
        await ctx.send(embed=embed)
    
    async def get_logs_after(self, last_id: int, limit: int = 50):
        """Get logs after a specific ID - for alliance_logs_pub compatibility
        Returns data in exact format that AllianceLogsPub expects"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                id,
                hash,
                ts,
                action_key,
                action_text,
                executed_name,
                executed_mc_id,
                executed_url,
                affected_name,
                affected_type,
                affected_mc_id,
                affected_url,
                description,
                contribution_amount
            FROM logs
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
        ''', (last_id, limit))
        
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return rows
