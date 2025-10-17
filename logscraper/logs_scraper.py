import discord
from redbot.core import commands, Config, data_manager
import aiohttp
import asyncio
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
import re

class LogsScraper(commands.Cog):
    """Scrapes alliance logs from MissionChief"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621002, force_registration=True)
        
        base_path = data_manager.cog_data_path(self.bot.get_cog("CookieManager"))
        db_dir = base_path.parent / "scraper_databases"
        db_dir.mkdir(exist_ok=True)
        self.db_path = db_dir / "logs_v2.db"
        
        self.logs_url = "https://www.missionchief.com/alliance_logfiles"
        self.debug_mode = False
        
        self._init_database()
        self.scrape_task = self.bot.loop.create_task(self._background_scraper())
    
    def _init_database(self):
        """Initialize SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                log_id INTEGER,
                log_type TEXT NOT NULL,
                username TEXT NOT NULL,
                action TEXT,
                description TEXT,
                log_timestamp TEXT NOT NULL,
                PRIMARY KEY (log_id)
            )
        ''')
        
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
        """Scrape a single page of logs"""
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
                
                # Find all table rows with log data
                for tr in soup.find_all("tr"):
                    # Skip header rows
                    if tr.find("th"):
                        continue
                    
                    cols = tr.find_all("td")
                    if len(cols) < 3:
                        continue
                    
                    # Extract log data
                    timestamp_col = cols[0].get_text(strip=True)
                    username_col = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                    action_col = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                    
                    # Try to extract log ID from any links or data attributes
                    log_id = None
                    for link in tr.find_all('a', href=True):
                        id_match = re.search(r'/(\d+)', link['href'])
                        if id_match:
                            log_id = int(id_match.group(1))
                            break
                    
                    # If no ID found, generate one from timestamp + username
                    if not log_id:
                        log_id = hash(f"{timestamp_col}{username_col}{action_col}") % (10**10)
                    
                    # Extract username
                    username_link = cols[1].find('a') if len(cols) > 1 else None
                    username = username_link.get_text(strip=True) if username_link else username_col
                    
                    # Determine log type
                    log_type = "unknown"
                    action_lower = action_col.lower()
                    
                    if "added to" in action_lower or "joined" in action_lower:
                        log_type = "added_to_alliance"
                    elif "left" in action_lower:
                        log_type = "left_alliance"
                    elif "kicked" in action_lower:
                        log_type = "kicked_from_alliance"
                    elif "admin" in action_lower:
                        log_type = "set_as_admin"
                    elif "training" in action_lower or "course" in action_lower:
                        log_type = "created_course"
                    elif "completed" in action_lower:
                        log_type = "course_completed"
                    elif "contributed" in action_lower:
                        log_type = "contributed_to_alliance"
                    
                    logs.append({
                        'log_id': log_id,
                        'log_type': log_type,
                        'username': username,
                        'action': action_col,
                        'description': action_col,
                        'log_timestamp': timestamp_col
                    })
                
                if self.debug_mode and logs:
                    await self._debug_log(f"✅ Page {page_num}: {len(logs)} logs", ctx)
                
                return logs
                
        except Exception as e:
            await self._debug_log(f"❌ Error page {page_num}: {str(e)}", ctx)
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
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        inserted = 0
        duplicates = 0
        training_inserted = 0
        
        for log in all_logs:
            try:
                cursor.execute('''
                    INSERT INTO logs (log_id, log_type, username, action, details, log_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (log['log_id'], log['log_type'], log['username'], 
                      log['action'], log.get('details', ''), log['log_timestamp']))
                inserted += 1
                
                # If it's a training course, also store in training_courses table
                if log['log_type'] in ['created_course', 'course_completed']:
                    # Count existing occurrences
                    cursor.execute('''
                        SELECT COUNT(*) FROM training_courses 
                        WHERE username = ? AND course_name = ? AND log_timestamp = ?
                    ''', (log['username'], log['action'], log['log_timestamp']))
                    count = cursor.fetchone()[0]
                    
                    if count < 4:  # Allow up to 4 occurrences
                        try:
                            cursor.execute('''
                                INSERT INTO training_courses (username, course_name, log_timestamp, occurrence)
                                VALUES (?, ?, ?, ?)
                            ''', (log['username'], log['action'], log['log_timestamp'], count + 1))
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
        
        cursor.execute("SELECT MIN(log_timestamp), MAX(log_timestamp) FROM logs")
        min_time, max_time = cursor.fetchone()
        
        cursor.execute("SELECT MAX(log_id) FROM logs")
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
        """Get logs after a specific ID - for alliance_logs_pub compatibility"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Old AllianceScraper column mapping:
        # log_type = action type (e.g. "expansion_finished")
        # action = username who did it
        # username = description of what happened
        # details = username (duplicate)
        cursor.execute('''
            SELECT 
                rowid as id,
                log_type as action_key,
                COALESCE(NULLIF(action, ''), details, 'Unknown') as executed_name,
                username as description,
                log_timestamp as ts
            FROM logs
            WHERE rowid > ?
            ORDER BY rowid ASC
            LIMIT ?
        ''', (last_id, limit))
        
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return rows
