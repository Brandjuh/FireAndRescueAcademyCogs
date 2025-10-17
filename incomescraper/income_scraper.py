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
        base_path = data_manager.cog_data_path(raw_name="scraper_databases")
        base_path.mkdir(parents=True, exist_ok=True)
        self.db_path = str(base_path / "income_v2.db")
        
        self.base_url = "https://www.missionchief.com"
        self.income_url = f"{self.base_url}/verband/kasse"
        self.scraping_task = None
        self.debug_mode = False
        self.debug_channel = None
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
        import time
        
        max_retries = 5
        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self.db_path, timeout=10.0)
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
                break
            except sqlite3.OperationalError as e:
                if attempt < max_retries - 1:
                    print(f"[IncomeScraper] Database locked, retrying... ({attempt + 1}/{max_retries})")
                    time.sleep(0.5)
                else:
                    print(f"[IncomeScraper] Failed to initialize database after {max_retries} attempts")
                    raise
    
    async def _debug_log(self, message, ctx=None):
        """Log debug messages to console AND Discord"""
        print(f"[IncomeScraper DEBUG] {message}")
        
        if self.debug_mode and (ctx or self.debug_channel):
            try:
                channel = ctx.channel if ctx else self.debug_channel
                if channel:
                    await channel.send(f"🐛 `{message}`")
            except Exception as e:
                print(f"[IncomeScraper DEBUG] Failed to send to Discord: {e}")
    
    async def _get_cookie_manager(self):
        """Get CookieManager cog instance"""
        return self.bot.get_cog("CookieManager")
    
    async def _get_session(self, ctx=None):
        """Get authenticated session from CookieManager cog"""
        cookie_manager = await self._get_cookie_manager()
        if not cookie_manager:
            await self._debug_log("❌ CookieManager cog not loaded!", ctx)
            return None
        
        try:
            session = await cookie_manager.get_session()
            await self._debug_log("✅ Session obtained successfully", ctx)
            return session
        except Exception as e:
            await self._debug_log(f"❌ Failed to get session: {e}", ctx)
            return None
    
    async def _check_logged_in(self, html_content, ctx=None):
        """Check if still logged in"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        logout_button = soup.find('a', href='/users/sign_out')
        user_menu = soup.find('li', class_='dropdown user-menu')
        
        # Check if we can see tables (income/expense data)
        has_tables = bool(soup.find_all('table'))
        
        is_logged_in = (logout_button is not None or 
                        user_menu is not None or
                        has_tables)
        
        await self._debug_log(f"Login check: {'✅ Logged in' if is_logged_in else '❌ NOT logged in'}", ctx)
        return is_logged_in
    
    def _parse_amount(self, amount_str):
        """Parse amount string to integer with validation"""
        if not amount_str:
            return 0
        
        # Remove all non-digit characters except minus sign
        cleaned = re.sub(r'[^\d-]', '', amount_str)
        
        if not cleaned or cleaned == '-':
            return 0
        
        try:
            val = int(cleaned)
            # Clamp to safe range
            return max(INT64_MIN, min(INT64_MAX, val))
        except (ValueError, OverflowError):
            return 0
    
    async def _scrape_income_tab(self, session, tab_type='daily', ctx=None):
        """Scrape income/expense data from a specific tab (daily or monthly)"""
        # Construct URL with tab parameter
        if tab_type == 'monthly':
            url = f"{self.income_url}?tab=monthly"
        else:
            url = self.income_url  # Default/daily
        
        await self._debug_log(f"🌐 Scraping {tab_type} income: {url}", ctx)
        
        for attempt in range(3):
            try:
                await asyncio.sleep(1.5)
                
                async with session.get(url) as response:
                    await self._debug_log(f"📡 Response status: {response.status}", ctx)
                    
                    if response.status != 200:
                        await self._debug_log(f"❌ Tab {tab_type} returned status {response.status}", ctx)
                        return []
                    
                    html = await response.text()
                    await self._debug_log(f"📄 HTML length: {len(html)} chars", ctx)
                    
                    if not await self._check_logged_in(html, ctx):
                        await self._debug_log(f"❌ Session expired on {tab_type} tab", ctx)
                        return []
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    data = []
                    scrape_timestamp = datetime.utcnow().isoformat()
                    
                    # Find all tables
                    tables = soup.find_all('table')
                    await self._debug_log(f"📊 Found {len(tables)} tables on page", ctx)
                    
                    for table_idx, table in enumerate(tables):
                        # Try to determine if it's income or expense table from nearby headers
                        table_header = table.find_previous(['h1', 'h2', 'h3', 'h4', 'strong'])
                        section_type = "income"  # Default to income for treasury page
                        
                        if table_header:
                            header_text = table_header.text.lower()
                            if any(word in header_text for word in ['expense', 'cost', 'ausgaben', 'spending']):
                                section_type = "expense"
                        
                        await self._debug_log(f"Table {table_idx}: type={section_type}", ctx)
                        
                        # Parse table rows
                        rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')
                        
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) < 2:
                                continue
                            
                            # First column: description/date/name
                            description = cols[0].get_text(strip=True)
                            
                            # Last column usually contains amount
                            amount_str = cols[-1].get_text(strip=True)
                            amount = self._parse_amount(amount_str)
                            
                            # Only store if we have meaningful data
                            if description and amount != 0:
                                # Try to extract period date from description or use current month
                                period_date = datetime.utcnow().strftime('%Y-%m')
                                date_match = re.search(r'(\d{4}-\d{2})', description)
                                if date_match:
                                    period_date = date_match.group(1)
                                
                                data.append({
                                    'period_type': tab_type,
                                    'period_date': period_date,
                                    'entry_type': section_type,
                                    'description': description[:500],
                                    'amount': amount,
                                    'scrape_timestamp': scrape_timestamp
                                })
                                
                                await self._debug_log(f"💰 {section_type}: {description[:30]}... = {amount:,}", ctx)
                    
                    await self._debug_log(f"✅ Parsed {len(data)} entries from {tab_type} tab", ctx)
                    return data
                    
            except asyncio.TimeoutError:
                await self._debug_log(f"⏱️ Timeout on tab {tab_type}, attempt {attempt + 1}/3", ctx)
                if attempt == 2:
                    return []
            except Exception as e:
                await self._debug_log(f"❌ Error scraping tab {tab_type}: {e}", ctx)
                if attempt == 2:
                    return []
        
        return []
    
    async def _scrape_expenses_page(self, session, page_num, ctx=None):
        """Scrape a single page of expenses"""
        url = f"{self.income_url}/expenses?page={page_num}"
        await self._debug_log(f"🌐 Scraping expenses page {page_num}: {url}", ctx)
        
        for attempt in range(3):
            try:
                await asyncio.sleep(1.5)
                
                async with session.get(url) as response:
                    await self._debug_log(f"📡 Response status: {response.status}", ctx)
                    
                    if response.status != 200:
                        await self._debug_log(f"❌ Expenses page {page_num} returned status {response.status}", ctx)
                        return []
                    
                    html = await response.text()
                    
                    if not await self._check_logged_in(html, ctx):
                        await self._debug_log(f"❌ Session expired on expenses page {page_num}", ctx)
                        return []
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    data = []
                    scrape_timestamp = datetime.utcnow().isoformat()
                    
                    # Find expense tables
                    tables = soup.find_all('table')
                    
                    for table in tables:
                        rows = table.find('tbody').find_all('tr') if table.find('tbody') else table.find_all('tr')
                        
                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) < 2:
                                continue
                            
                            # Typical expense format: Date | Amount | Description
                            # Adjust based on actual table structure
                            expense_date = cols[0].get_text(strip=True) if len(cols) > 0 else ""
                            amount_str = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                            description = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                            
                            # If structure is different, try alternative parsing
                            if not description and len(cols) >= 2:
                                description = cols[0].get_text(strip=True)
                                amount_str = cols[-1].get_text(strip=True)
                            
                            amount = self._parse_amount(amount_str)
                            
                            if (expense_date or description) and amount != 0:
                                # Extract period from date
                                period_date = datetime.utcnow().strftime('%Y-%m')
                                date_match = re.search(r'(\d{4}-\d{2})', expense_date)
                                if date_match:
                                    period_date = date_match.group(1)
                                
                                data.append({
                                    'period_type': 'expense_detail',
                                    'period_date': period_date,
                                    'entry_type': 'expense',
                                    'description': f"{expense_date} - {description}"[:500],
                                    'amount': amount,
                                    'scrape_timestamp': scrape_timestamp
                                })
                                
                                await self._debug_log(f"💸 Expense: {description[:30]}... = {amount:,}", ctx)
                    
                    await self._debug_log(f"✅ Parsed {len(data)} expenses from page {page_num}", ctx)
                    return data
                    
            except asyncio.TimeoutError:
                await self._debug_log(f"⏱️ Timeout on expenses page {page_num}, attempt {attempt + 1}/3", ctx)
                if attempt == 2:
                    return []
            except Exception as e:
                await self._debug_log(f"❌ Error scraping expenses page {page_num}: {e}", ctx)
                if attempt == 2:
                    return []
        
        return []
    
    async def _scrape_all_expenses(self, session, ctx=None, max_pages=100):
        """Scrape all pages of expenses with pagination"""
        await self._debug_log(f"🚀 Starting expenses scrape (max {max_pages} pages)", ctx)
        
        all_expenses = []
        page = 1
        empty_page_count = 0
        
        while page <= max_pages:
            expenses = await self._scrape_expenses_page(session, page, ctx)
            
            if not expenses:
                empty_page_count += 1
                await self._debug_log(f"⚠️ Expenses page {page} returned 0 entries (empty count: {empty_page_count})", ctx)
                
                if empty_page_count >= 3:
                    await self._debug_log(f"⛔ Stopped expenses after {empty_page_count} consecutive empty pages", ctx)
                    break
            else:
                empty_page_count = 0
                all_expenses.extend(expenses)
                await self._debug_log(f"✅ Expenses page {page}: {len(expenses)} entries (total: {len(all_expenses)})", ctx)
            
            page += 1
        
        await self._debug_log(f"📊 Total expenses scraped: {len(all_expenses)} across {page - 1} pages", ctx)
        return all_expenses
    
    async def _scrape_all_income(self, ctx=None, include_expenses=True, max_expense_pages=100):
        """Scrape daily income, monthly income, and all expense pages"""
        session = await self._get_session(ctx)
        if not session:
            if ctx:
                await ctx.send("❌ Failed to get session. Is CookieManager loaded and logged in?")
            return False
        
        await self._debug_log("🚀 Starting complete income/expense scrape", ctx)
        
        all_data = []
        
        # 1. Scrape daily income tab
        await self._debug_log("📅 Scraping DAILY income...", ctx)
        daily_data = await self._scrape_income_tab(session, 'daily', ctx)
        all_data.extend(daily_data)
        
        # 2. Scrape monthly income tab
        await self._debug_log("📆 Scraping MONTHLY income...", ctx)
        monthly_data = await self._scrape_income_tab(session, 'monthly', ctx)
        all_data.extend(monthly_data)
        
        # 3. Scrape all expense pages (with pagination)
        if include_expenses:
            await self._debug_log("💸 Scraping EXPENSES (paginated)...", ctx)
            expense_data = await self._scrape_all_expenses(session, ctx, max_expense_pages)
            all_data.extend(expense_data)
        
        # Save to database
        if all_data:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            cursor = conn.cursor()
            
            inserted = 0
            duplicates = 0
            
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
                    if cursor.rowcount > 0:
                        inserted += 1
                    else:
                        duplicates += 1
                except sqlite3.IntegrityError:
                    duplicates += 1
            
            conn.commit()
            conn.close()
            
            await self._debug_log(f"💾 Database: {inserted} inserted, {duplicates} duplicates", ctx)
            
            if ctx:
                daily_count = len(daily_data)
                monthly_count = len(monthly_data)
                expense_count = len(all_data) - daily_count - monthly_count
                
                msg = f"✅ Scraped {len(all_data)} total entries:\n"
                msg += f"  📅 Daily: {daily_count}\n"
                msg += f"  📆 Monthly: {monthly_count}\n"
                msg += f"  💸 Expenses: {expense_count}\n"
                msg += f"💾 Database: {inserted} new records, {duplicates} duplicates"
                await ctx.send(msg)
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
    
    @commands.group(name="income")
    @commands.is_owner()
    async def income_group(self, ctx):
        """Income/expenses scraper commands"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @income_group.command(name="scrape")
    async def scrape_income(self, ctx, max_expense_pages: int = 100):
        """
        Manually trigger income/expenses scraping
        
        Scrapes:
        - Daily income tab
        - Monthly income tab
        - All expense pages (with pagination)
        
        Usage: [p]income scrape [max_expense_pages]
        Example: [p]income scrape 200
        """
        await ctx.send(f"🔄 Starting complete income/expenses scrape (up to {max_expense_pages} expense pages)...")
        success = await self._scrape_all_income(ctx, include_expenses=True, max_expense_pages=max_expense_pages)
        if success:
            await ctx.send("✅ Income/expenses scrape completed successfully")
    
    @income_group.command(name="backfill")
    async def backfill_income(self, ctx, max_expense_pages: int = 500):
        """
        Back-fill all expense history from MissionChief
        
        Usage: [p]income backfill [max_expense_pages]
        Example: [p]income backfill 1000
        """
        if max_expense_pages < 1 or max_expense_pages > 2000:
            await ctx.send("❌ Max pages must be between 1 and 2000")
            return
        
        await ctx.send(f"🔄 Starting expense back-fill (up to {max_expense_pages} pages)...")
        await ctx.send(f"⚠️ This may take **{max_expense_pages * 2 // 60} to {max_expense_pages * 3 // 60} minutes**...")
        
        success = await self._scrape_all_income(ctx, include_expenses=True, max_expense_pages=max_expense_pages)
        
        if success:
            await ctx.send(f"✅ Back-fill completed!")
    
    @income_group.command(name="debug")
    async def debug_income(self, ctx, enable: bool = True):
        """Enable or disable debug logging to Discord"""
        self.debug_mode = enable
        self.debug_channel = ctx.channel if enable else None
        await ctx.send(f"🐛 Debug mode: {'**ENABLED**' if enable else '**DISABLED**'}\n"
                      f"Debug messages will be sent to this channel.")
    
    @income_group.command(name="stats")
    async def stats_income(self, ctx):
        """Show database statistics"""
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM income_expenses")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM income_expenses WHERE entry_type='income'")
        income_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM income_expenses WHERE entry_type='expense'")
        expense_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(amount) FROM income_expenses WHERE entry_type='income'")
        total_income = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT SUM(amount) FROM income_expenses WHERE entry_type='expense'")
        total_expense = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT MIN(scrape_timestamp), MAX(scrape_timestamp) FROM income_expenses")
        date_range = cursor.fetchone()
        
        conn.close()
        
        embed = discord.Embed(title="📊 Income/Expenses Database Statistics", color=discord.Color.gold())
        embed.add_field(name="Total Records", value=f"{total:,}", inline=True)
        embed.add_field(name="Income Entries", value=f"{income_count:,}", inline=True)
        embed.add_field(name="Expense Entries", value=f"{expense_count:,}", inline=True)
        embed.add_field(name="Total Income", value=f"${total_income:,}", inline=True)
        embed.add_field(name="Total Expenses", value=f"${total_expense:,}", inline=True)
        embed.add_field(name="Net", value=f"${total_income - total_expense:,}", inline=True)
        
        if date_range[0]:
            embed.add_field(name="First Record", value=date_range[0][:10], inline=True)
            embed.add_field(name="Last Record", value=date_range[1][:10], inline=True)
        
        embed.set_footer(text=f"Database: {self.db_path}")
        await ctx.send(embed=embed)

async def setup(bot):
    await bot.add_cog(IncomeScraper(bot))
