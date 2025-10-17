import discord
from redbot.core import commands, Config, data_manager
import aiohttp
import asyncio
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup
from pathlib import Path
import re

# SQLite INTEGER limits
INT64_MAX = 9223372036854775807
INT64_MIN = -9223372036854775808

class IncomeScraper(commands.Cog):
    """Scrapes alliance income/expenses from MissionChief"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621003, force_registration=True)
        
        # Setup database path in shared location
        base_path = data_manager.cog_data_path(self.bot.get_cog("CookieManager"))
        db_dir = base_path.parent / "scraper_databases"
        db_dir.mkdir(exist_ok=True)
        self.db_path = db_dir / "income_v2.db"
        
        self.income_url = "https://www.missionchief.com/verband/kasse"
        self.debug_mode = False
        
        self._init_database()
        self.scrape_task = self.bot.loop.create_task(self._background_scraper())
    
    def _init_database(self):
        """Initialize SQLite database with schema"""
        import time
        
        # Retry logic for locked database
        max_retries = 5
        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS income (
                        entry_type TEXT NOT NULL,
                        period TEXT NOT NULL,
                        username TEXT NOT NULL,
                        amount INTEGER NOT NULL,
                        description TEXT,
                        timestamp TEXT NOT NULL,
                        PRIMARY KEY (entry_type, period, username, timestamp)
                    )
                ''')
                conn.commit()
                conn.close()
                return
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() and attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
                raise
    
    def cog_unload(self):
        """Cancel background task on unload"""
        if hasattr(self, 'scrape_task'):
            self.scrape_task.cancel()
    
    async def _background_scraper(self):
        """Background task that scrapes income/expenses every hour at :30"""
        await self.bot.wait_until_ready()
        
        while True:
            try:
                # Wait until the next :30 minute mark
                now = datetime.now()
                next_run = now.replace(minute=30, second=0, microsecond=0)
                if now.minute >= 30:
                    next_run = next_run.replace(hour=now.hour + 1)
                wait_seconds = (next_run - now).total_seconds()
                
                await asyncio.sleep(wait_seconds)
                
                # Run scrape (without expenses pagination for background)
                await self._scrape_all_income(ctx=None, include_expenses=False)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[IncomeScraper] Background scrape error: {e}")
                await asyncio.sleep(3600)  # Wait 1 hour on error
    
    async def _get_session(self, ctx=None):
        """Get authenticated session from CookieManager"""
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager:
            await self._debug_log("❌ CookieManager cog not loaded!", ctx)
            return None
        
        session = await cookie_manager.get_session()
        if not session:
            await self._debug_log("❌ Failed to get session from CookieManager", ctx)
            return None
        
        await self._debug_log("✅ Session obtained successfully", ctx)
        return session
    
    async def _debug_log(self, message, ctx=None):
        """Log debug messages to Discord if debug mode is on"""
        print(f"[IncomeScraper DEBUG] {message}")
        if self.debug_mode and ctx:
            try:
                await ctx.send(message)
            except:
                pass
    
    async def _check_logged_in(self, html_content, ctx=None):
        """Check if still logged in"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Check for table rows with user links (indicates logged in)
        has_member_links = bool(soup.find('a', href=lambda x: x and '/users/' in str(x)))
        
        is_logged_in = has_member_links
        
        await self._debug_log(f"Login check: {'✅ Logged in' if is_logged_in else '❌ NOT logged in'}", ctx)
        
        return is_logged_in
    
    async def _scrape_income_tab(self, session, tab_type='daily', ctx=None):
        """Scrape income/expense data from a specific tab (daily or monthly)"""
        # Construct URL with tab parameter
        if tab_type == 'monthly':
            url = f"{self.income_url}?tab=monthly"
        else:
            url = self.income_url
        
        await self._debug_log(f"🌐 Scraping {tab_type} income: {url}", ctx)
        
        try:
            async with session.get(url) as resp:
                await self._debug_log(f"📡 Response status: {resp.status}", ctx)
                
                if resp.status != 200:
                    await self._debug_log(f"❌ Bad status {resp.status}", ctx)
                    return []
                
                html_content = await resp.text()
                await self._debug_log(f"📄 HTML length: {len(html_content)} chars", ctx)
                
                # Check login
                if not await self._check_logged_in(html_content, ctx):
                    await self._debug_log(f"❌ Session expired", ctx)
                    return []
                
                # Parse tables
                soup = BeautifulSoup(html_content, 'html.parser')
                tables = soup.find_all('table')
                
                await self._debug_log(f"📊 Found {len(tables)} tables on page", ctx)
                
                entries = []
                
                for table_idx, table in enumerate(tables):
                    # Try to determine table type by headers or content
                    headers = [th.get_text(strip=True) for th in table.find_all('th')]
                    
                    # Determine if this is income or expense table
                    # Income tables usually have "Username" or "Member" columns
                    # Expense tables might have "Amount" or "Cost" columns
                    is_income_table = any('member' in h.lower() or 'user' in h.lower() for h in headers)
                    is_expense_table = any('expense' in h.lower() or 'cost' in h.lower() for h in headers)
                    
                    # If can't determine, look at first data row
                    if not is_income_table and not is_expense_table:
                        first_row = table.find('tr')
                        if first_row:
                            first_data = first_row.get_text()
                            # Simple heuristic: if has username-like text, it's income
                            is_income_table = bool(re.search(r'[A-Za-z]+\d+', first_data))
                    
                    entry_type = 'income' if is_income_table else 'expense'
                    
                    await self._debug_log(f"Table {table_idx}: type={entry_type}", ctx)
                    
                    # Parse rows
                    for row in table.find_all('tr')[1:]:  # Skip header
                        cols = row.find_all('td')
                        if len(cols) < 2:
                            continue
                        
                        # Extract username and amount
                        username = ""
                        amount = 0
                        description = ""
                        
                        # Try to find username (usually first column with link)
                        for col in cols:
                            link = col.find('a', href=True)
                            if link and '/users/' in link['href']:
                                username = link.get_text(strip=True)
                                break
                        
                        # If no username found, use first column text
                        if not username and cols:
                            username = cols[0].get_text(strip=True)
                        
                        # Find amount (look for numbers with commas)
                        for col in cols:
                            text = col.get_text(strip=True)
                            # Look for currency amounts: 123,456 or 123456
                            match = re.search(r'([\d,]+)', text)
                            if match:
                                amount_str = match.group(1).replace(',', '')
                                try:
                                    amount = int(amount_str)
                                    if amount > 100:  # Sanity check
                                        # Clamp to INT64 range
                                        amount = max(INT64_MIN, min(INT64_MAX, amount))
                                        break
                                except ValueError:
                                    continue
                        
                        # Extract description (longest text column)
                        for col in cols:
                            text = col.get_text(strip=True)
                            if len(text) > len(description) and not re.match(r'^[\d,]+$', text):
                                description = text
                        
                        if username and amount > 0:
                            await self._debug_log(f"💰 {entry_type}: {username[:20]}... = {amount:,}", ctx)
                            
                            entries.append({
                                'entry_type': entry_type,
                                'period': tab_type,
                                'username': username,
                                'amount': amount,
                                'description': description
                            })
                
                await self._debug_log(f"✅ Parsed {len(entries)} entries from {tab_type} tab", ctx)
                return entries
                
        except Exception as e:
            await self._debug_log(f"❌ Error scraping {tab_type}: {str(e)}", ctx)
            return []
    
    async def _scrape_expenses_pages(self, session, ctx=None, max_pages=100):
        """Scrape expenses with pagination from the treasury page"""
        await self._debug_log(f"💸 Starting EXPENSES scrape with pagination (max {max_pages} pages)", ctx)
        
        all_entries = []
        page = 1
        empty_count = 0
        
        while page <= max_pages:
            # Expenses pagination: ?page= parameter changes only the expense table
            url = f"{self.income_url}?page={page}"
            await self._debug_log(f"🌐 Scraping expenses page {page}: {url}", ctx)
            
            try:
                async with session.get(url) as resp:
                    await self._debug_log(f"📡 Response status: {resp.status}", ctx)
                    
                    if resp.status != 200:
                        await self._debug_log(f"❌ Expenses page {page} returned status {resp.status}", ctx)
                        empty_count += 1
                        if empty_count >= 3:
                            await self._debug_log(f"⛔ Stopped expenses after 3 consecutive bad responses", ctx)
                            break
                        page += 1
                        continue
                    
                    html_content = await resp.text()
                    
                    # Check login
                    if not await self._check_logged_in(html_content, ctx):
                        await self._debug_log(f"❌ Session expired on expenses page {page}", ctx)
                        break
                    
                    # Parse expense table
                    soup = BeautifulSoup(html_content, 'html.parser')
                    tables = soup.find_all('table')
                    
                    # Look for the expense table (usually the one with pagination)
                    page_entries = []
                    for table in tables:
                        # Look for expense indicators
                        table_text = table.get_text().lower()
                        if 'expense' in table_text or 'cost' in table_text:
                            await self._debug_log(f"📋 Found expenses table on page {page}", ctx)
                            
                            for row in table.find_all('tr')[1:]:  # Skip header
                                cols = row.find_all('td')
                                if len(cols) < 2:
                                    continue
                                
                                username = ""
                                amount = 0
                                description = ""
                                
                                # Extract data similar to income scraping
                                for col in cols:
                                    link = col.find('a', href=True)
                                    if link and '/users/' in link['href']:
                                        username = link.get_text(strip=True)
                                        break
                                
                                if not username and cols:
                                    username = cols[0].get_text(strip=True)
                                
                                for col in cols:
                                    text = col.get_text(strip=True)
                                    match = re.search(r'([\d,]+)', text)
                                    if match:
                                        amount_str = match.group(1).replace(',', '')
                                        try:
                                            amount = int(amount_str)
                                            if amount > 100:
                                                amount = max(INT64_MIN, min(INT64_MAX, amount))
                                                break
                                        except ValueError:
                                            continue
                                
                                for col in cols:
                                    text = col.get_text(strip=True)
                                    if len(text) > len(description) and not re.match(r'^[\d,]+$', text):
                                        description = text
                                
                                if username and amount > 0:
                                    await self._debug_log(f"💸 Expense: {username[:20]}... = {amount:,}", ctx)
                                    
                                    page_entries.append({
                                        'entry_type': 'expense',
                                        'period': 'paginated',
                                        'username': username,
                                        'amount': amount,
                                        'description': description
                                    })
                    
                    if not page_entries:
                        await self._debug_log(f"⚠️ Expenses page {page} returned 0 entries (empty count: {empty_count + 1})", ctx)
                        empty_count += 1
                        if empty_count >= 3:
                            await self._debug_log(f"⛔ Stopped expenses after 3 consecutive empty pages", ctx)
                            break
                    else:
                        await self._debug_log(f"✅ Page {page}: {len(page_entries)} expenses (total so far: {len(all_entries) + len(page_entries)})", ctx)
                        all_entries.extend(page_entries)
                        empty_count = 0
                    
                    page += 1
                    await asyncio.sleep(1.5)  # Rate limiting
                    
            except Exception as e:
                await self._debug_log(f"❌ Error on expenses page {page}: {str(e)}", ctx)
                page += 1
                await asyncio.sleep(2)
        
        await self._debug_log(f"📊 Total expenses scraped: {len(all_entries)} across {page - 1} pages", ctx)
        return all_entries
    
    async def _scrape_all_income(self, ctx=None, include_expenses=True, max_expense_pages=100):
        """Scrape daily income, monthly income, and expenses from the treasury page"""
        session = await self._get_session(ctx)
        if not session:
            if ctx:
                await ctx.send("❌ Failed to get session")
            return False
        
        await self._debug_log("🚀 Starting income/expense scrape", ctx)
        
        all_data = []
        
        # 1. Scrape daily income/expense tab
        await self._debug_log("📅 Scraping DAILY income tab...", ctx)
        daily_data = await self._scrape_income_tab(session, 'daily', ctx)
        all_data.extend(daily_data)
        
        await asyncio.sleep(1.5)
        
        # 2. Scrape monthly income/expense tab
        await self._debug_log("📆 Scraping MONTHLY income tab...", ctx)
        monthly_data = await self._scrape_income_tab(session, 'monthly', ctx)
        all_data.extend(monthly_data)
        
        await asyncio.sleep(1.5)
        
        # 3. Scrape expenses with pagination
        if include_expenses:
            expenses_data = await self._scrape_expenses_pages(session, ctx, max_expense_pages)
            all_data.extend(expenses_data)
        
        # Store in database
        if not all_data:
            if ctx:
                await ctx.send("❌ No income/expense data found")
            return False
        
        timestamp = datetime.now().isoformat()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        inserted = 0
        duplicates = 0
        
        for entry in all_data:
            try:
                cursor.execute('''
                    INSERT INTO income (entry_type, period, username, amount, description, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (entry['entry_type'], entry['period'], entry['username'], 
                      entry['amount'], entry.get('description', ''), timestamp))
                inserted += 1
            except sqlite3.IntegrityError:
                duplicates += 1
        
        conn.commit()
        conn.close()
        
        await self._debug_log(f"💾 Database: {inserted} inserted, {duplicates} duplicates", ctx)
        
        if ctx:
            await ctx.send(f"✅ Scraped {len(all_data)} income/expense entries\n"
                          f"💾 Database: {inserted} new records, {duplicates} duplicates")
        
        return True
    
    @commands.group(name="income")
    @commands.is_owner()
    async def income_group(self, ctx):
        """Income/expense scraper commands"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @income_group.command(name="scrape")
    async def scrape_income(self, ctx, max_expense_pages: int = 5):
        """
        Manually scrape income/expenses
        
        Usage: !income scrape [max_expense_pages]
        Example: !income scrape 10
        """
        await ctx.send("🔄 Starting income/expenses scrape...")
        success = await self._scrape_all_income(ctx, include_expenses=True, max_expense_pages=max_expense_pages)
        
        if success:
            await ctx.send("✅ Income/expenses scrape completed successfully")
        else:
            await ctx.send("❌ Income/expenses scrape failed")
    
    @income_group.command(name="backfill")
    async def backfill_expenses(self, ctx, max_pages: int = 200):
        """
        Back-fill ALL historical expenses from MissionChief
        
        Usage: !income backfill [max_pages]
        Example: !income backfill 500
        """
        if max_pages < 1 or max_pages > 2000:
            await ctx.send("❌ Max pages must be between 1 and 2000")
            return
        
        await ctx.send(f"🔄 Starting back-fill of expenses (up to {max_pages} pages)...")
        await ctx.send("⏳ This may take several minutes...")
        
        # Just scrape expenses with high page count
        success = await self._scrape_all_income(ctx, include_expenses=True, max_expense_pages=max_pages)
        
        if success:
            await ctx.send("✅ Expense back-fill completed!")
        else:
            await ctx.send("❌ Back-fill failed")
    
    @income_group.command(name="debug")
    async def toggle_debug(self, ctx, mode: str = None):
        """Toggle debug mode (on/off)"""
        if mode is None:
            await ctx.send(f"Debug mode: {'ENABLED' if self.debug_mode else 'DISABLED'}")
            return
        
        if mode.lower() in ['on', 'enable', 'true', '1']:
            self.debug_mode = True
            await ctx.send("✅ Debug mode: ENABLED")
        elif mode.lower() in ['off', 'disable', 'false', '0']:
            self.debug_mode = False
            await ctx.send("✅ Debug mode: DISABLED")
        else:
            await ctx.send("❌ Invalid mode. Use 'on' or 'off'")
    
    @income_group.command(name="stats")
    async def show_stats(self, ctx):
        """Show income/expense statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get totals
        cursor.execute("SELECT COUNT(*) FROM income WHERE entry_type='income'")
        income_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM income WHERE entry_type='expense'")
        expense_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(amount) FROM income WHERE entry_type='income'")
        total_income = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT SUM(amount) FROM income WHERE entry_type='expense'")
        total_expense = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM income")
        min_time, max_time = cursor.fetchone()
        
        conn.close()
        
        embed = discord.Embed(title="💰 Income/Expense Statistics", color=discord.Color.gold())
        embed.add_field(name="Income Entries", value=f"{income_count:,}", inline=True)
        embed.add_field(name="Expense Entries", value=f"{expense_count:,}", inline=True)
        embed.add_field(name="Total Entries", value=f"{income_count + expense_count:,}", inline=True)
        embed.add_field(name="Total Income", value=f"{total_income:,} credits", inline=True)
        embed.add_field(name="Total Expenses", value=f"{total_expense:,} credits", inline=True)
        embed.add_field(name="Net Balance", value=f"{total_income - total_expense:,} credits", inline=True)
        
        if min_time and max_time:
            embed.add_field(name="Data Range", value=f"{min_time[:10]} to {max_time[:10]}", inline=False)
        
        embed.set_footer(text=f"Database: {self.db_path.name}")
        
        await ctx.send(embed=embed)
