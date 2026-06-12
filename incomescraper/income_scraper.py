import discord
from redbot.core import commands, Config, data_manager
import asyncio
import sqlite3
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re
import hashlib
from zoneinfo import ZoneInfo

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
        self._scrape_lock = asyncio.Lock()
        
        self._init_database()
        self.scrape_task = self.bot.loop.create_task(self._background_scraper())
        self.pre_reset_task = self.bot.loop.create_task(self._pre_reset_snapshot_loop())
    
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
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS expenses (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        signature TEXT NOT NULL,
                        occurrence_index INTEGER NOT NULL,
                        username TEXT NOT NULL,
                        amount INTEGER NOT NULL,
                        description TEXT,
                        source_date TEXT NOT NULL,
                        event_timestamp TEXT,
                        scraped_at TEXT NOT NULL,
                        UNIQUE(signature, occurrence_index)
                    )
                ''')
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_expenses_event_timestamp "
                    "ON expenses(event_timestamp)"
                )
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
        if hasattr(self, 'pre_reset_task'):
            self.pre_reset_task.cancel()

    @staticmethod
    def _next_pre_reset_snapshot(now):
        """Return the next 23:55 America/New_York snapshot time."""
        target = now.replace(hour=23, minute=55, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return target

    async def _pre_reset_snapshot_loop(self):
        """Capture daily/monthly income immediately before the New York reset."""
        await self.bot.wait_until_ready()
        eastern = ZoneInfo("America/New_York")

        while not self.bot.is_closed():
            try:
                now = datetime.now(eastern)
                target = self._next_pre_reset_snapshot(now)
                await asyncio.sleep((target - now).total_seconds())
                await self._scrape_all_income(
                    ctx=None,
                    include_expenses=False,
                    max_expense_pages=0,
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[IncomeScraper] Pre-reset snapshot error: {e}")
                await asyncio.sleep(60)

    @staticmethod
    def _normalize_expense_timestamp(source_date, scraped_at):
        """Normalize only recent, unambiguous yearless expense dates."""
        eastern = ZoneInfo("America/New_York")
        scraped_local = scraped_at.astimezone(eastern)
        try:
            parsed = datetime.strptime(source_date, "%d %b %H:%M")
        except (TypeError, ValueError):
            return None

        candidate = parsed.replace(year=scraped_local.year, tzinfo=eastern)
        if candidate > scraped_local + timedelta(minutes=5):
            candidate = candidate.replace(year=candidate.year - 1)
        if not timedelta() <= scraped_local - candidate <= timedelta(days=7):
            return None
        return candidate.astimezone(ZoneInfo("UTC")).isoformat()

    @staticmethod
    def _assign_expense_occurrences(entries, scraped_at):
        """Assign stable occurrence indexes to identical visible expenses."""
        occurrences = {}
        for entry in entries:
            event_timestamp = IncomeScraper._normalize_expense_timestamp(
                entry["source_date"],
                scraped_at,
            )
            entry["event_timestamp"] = event_timestamp
            signature_text = "|".join(
                (
                    event_timestamp or entry["source_date"],
                    entry["username"],
                    str(entry["amount"]),
                    entry.get("description", ""),
                )
            )
            signature = hashlib.sha256(signature_text.encode()).hexdigest()
            occurrence = occurrences.get(signature, 0) + 1
            occurrences[signature] = occurrence
            entry["signature"] = signature
            entry["occurrence_index"] = occurrence
        return entries
    
    async def _background_scraper(self):
        """Background task that scrapes income/expenses every hour"""
        await self.bot.wait_until_ready()
        
        while not self.bot.is_closed():
            try:
                # Run scrape
                await self._scrape_all_income(ctx=None, include_expenses=False, max_expense_pages=5)
                
                # Wait 1 hour
                await asyncio.sleep(3600)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[IncomeScraper] Background scrape error: {e}")
                await asyncio.sleep(3600)
    
    async def _get_session(self, ctx=None):
        """Get authenticated session from CookieManager"""
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager:
            await self._debug_log("❌ CookieManager cog not loaded", ctx)
            if ctx:
                await ctx.send("❌ CookieManager cog not loaded! Load it with: `!load cookiemanager`")
            return None
        
        try:
            session = await cookie_manager.get_session()
            if not session:
                await self._debug_log("❌ Failed to get session from CookieManager", ctx)
                if ctx:
                    await ctx.send("❌ Failed to get session. Try: `!cookie status` and `!cookie login`")
                return None
            
            await self._debug_log("✅ Session obtained successfully", ctx)
            return session
        except Exception as e:
            await self._debug_log(f"❌ Error getting session: {str(e)}", ctx)
            if ctx:
                await ctx.send(f"❌ Error getting session: {str(e)}\nTry: `!cookie status`")
            return None
    
    async def _debug_log(self, message, ctx=None):
        """Log debug messages to Discord if debug mode is on"""
        print(f"[IncomeScraper DEBUG] {message}")
        if self.debug_mode and ctx:
            try:
                await ctx.send(message)
            except Exception:
                pass
    
    async def _check_logged_in(self, html_content, ctx=None):
        """Check if still logged in - simplified check"""
        # Very simple check: if we got HTML content with tables, we're probably logged in
        # The CookieManager handles the actual authentication
        
        if len(html_content) < 1000:
            await self._debug_log(f"⚠️ HTML too short ({len(html_content)} chars) - might be error page", ctx)
            return False
        
        # Just check if there's any data-like content (tables, divs with data)
        has_content = '<table' in html_content or 'class="table"' in html_content
        
        if self.debug_mode:
            await self._debug_log(f"Login check: {'✅ Logged in' if has_content else '❌ NOT logged in'}", ctx)
        
        return has_content
    
    async def _scrape_income_tab(self, session, tab_type='daily', ctx=None):
        """Scrape income/expense data from a specific tab (daily or monthly) - FIXED VERSION"""
        # Construct URL with type parameter (NOT tab!)
        # Daily: https://www.missionchief.com/verband/kasse
        # Monthly: https://www.missionchief.com/verband/kasse?type=monthly
        if tab_type == 'monthly':
            url = f"{self.income_url}?type=monthly"
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
                    await self._debug_log("❌ Session expired", ctx)
                    return []
                
                # Parse HTML
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Look for tab links to understand structure
                tab_links = soup.find_all('a', href=True)
                tab_info = [a['href'] for a in tab_links if 'tab=' in a.get('href', '')]
                if tab_info and ctx:
                    await self._debug_log(f"📑 Found tab links: {tab_info[:5]}", ctx)
                
                # Find the correct table based on tab_type
                # Look for heading/label that indicates daily vs monthly
                tables = soup.find_all('table')
                
                await self._debug_log(f"📊 Found {len(tables)} tables on page", ctx)
                
                entries = []
                
                for table_idx, table in enumerate(tables):
                    # Get headers to identify column positions
                    header_row = table.find('tr')
                    if not header_row:
                        continue
                    
                    headers = [th.get_text(strip=True).lower() for th in header_row.find_all('th')]
                    
                    if not headers:
                        # No headers found, skip this table
                        continue
                    
                    await self._debug_log(f"Table {table_idx} headers: {headers}", ctx)
                    
                    # CRITICAL: Skip expense tables on daily/monthly tabs
                    # The expenses table has pagination and is scraped separately
                    # Expense tables have 4 columns: ['credits', 'name', 'description', 'date']
                    # Income tables have 2 columns: ['name', 'credits']
                    if len(headers) >= 4 or ('description' in headers and 'date' in headers):
                        await self._debug_log(f"⏭️  Table {table_idx}: Skipping expenses table (scraped with pagination separately)", ctx)
                        continue
                    
                    # Also skip if this is clearly an expense table based on column order
                    # Expense tables start with 'credits' column, income tables start with 'name'
                    if headers and headers[0] in ['credits', 'credit', 'amount']:
                        await self._debug_log(f"⏭️ Table {table_idx}: Skipping expense table (credits column first)", ctx)
                        continue
                    
                    # Find column indices based on headers
                    name_col_idx = None
                    credits_col_idx = None
                    
                    for idx, header in enumerate(headers):
                        if 'name' in header or 'user' in header or 'member' in header or 'player' in header:
                            name_col_idx = idx
                        elif 'credit' in header or 'amount' in header or 'coin' in header or 'contribution' in header:
                            credits_col_idx = idx
                    
                    # If we couldn't identify columns by headers, try positional approach
                    # Typically: Column 0 = Name, Column 1 = Credits
                    if name_col_idx is None and credits_col_idx is None:
                        if len(headers) >= 2:
                            await self._debug_log("⚠️ No clear headers, using positional: col0=name, col1=credits", ctx)
                            name_col_idx = 0
                            credits_col_idx = 1
                        else:
                            await self._debug_log(f"❌ Table {table_idx}: not enough columns", ctx)
                            continue
                    
                    await self._debug_log(f"✅ Table {table_idx}: name_col={name_col_idx}, credits_col={credits_col_idx}", ctx)
                    
                    # Determine if this is income or expense table (though for contributions, it's all 'expense' type)
                    is_income_table = any('member' in h or 'user' in h or 'name' in h for h in headers)
                    entry_type = 'income' if is_income_table else 'expense'
                    
                    # Parse data rows
                    parsed_count = 0
                    for row in table.find_all('tr')[1:]:  # Skip header row
                        cols = row.find_all('td')
                        if len(cols) < 2:
                            continue
                        
                        # Extract username from the identified column
                        username = ""
                        if name_col_idx is not None and name_col_idx < len(cols):
                            name_cell = cols[name_col_idx]
                            # Try to find link first (more reliable)
                            link = name_cell.find('a', href=True)
                            if link and '/users/' in link.get('href', ''):
                                username = link.get_text(strip=True)
                            else:
                                username = name_cell.get_text(strip=True)
                        
                        # Extract credits from the identified column
                        amount = 0
                        if credits_col_idx is not None and credits_col_idx < len(cols):
                            credits_cell = cols[credits_col_idx]
                            text = credits_cell.get_text(strip=True)
                            
                            # Parse credits - look for numbers with optional commas
                            match = re.search(r'([\d,]+)', text)
                            if match:
                                amount_str = match.group(1).replace(',', '')
                                try:
                                    amount = int(amount_str)
                                    # Clamp to INT64 range
                                    amount = max(INT64_MIN, min(INT64_MAX, amount))
                                except ValueError:
                                    await self._debug_log(f"⚠️ Could not parse amount: {text}", ctx)
                                    continue
                        
                        # Validate and add entry
                        if username and amount > 0:
                            parsed_count += 1
                            if parsed_count <= 5:  # Only log first 5 for brevity
                                await self._debug_log(f"💰 {entry_type}: {username} = {amount:,}", ctx)
                            
                            entries.append({
                                'entry_type': entry_type,
                                'period': tab_type,
                                'username': username,
                                'amount': amount,
                                'description': ''
                            })
                
                await self._debug_log(f"✅ Parsed {len(entries)} entries from {tab_type} tab", ctx)
                return entries
                
        except Exception as e:
            await self._debug_log(f"❌ Error scraping {tab_type}: {str(e)}", ctx)
            import traceback
            await self._debug_log(f"Traceback: {traceback.format_exc()}", ctx)
            return []
    
    async def _scrape_expenses_pages(self, session, ctx=None, max_pages=100):
        """Scrape expenses with pagination - page param changes the expense table only"""
        await self._debug_log(f"💸 Starting EXPENSES scrape (max {max_pages} pages)", ctx)
        if ctx and max_pages > 50:
            est_minutes = (max_pages * 1.5) / 60
            await ctx.send(f"⏱️ Estimated time: ~{est_minutes:.0f} minutes")
        
        all_entries = []
        page = 1
        empty_count = 0
        
        while page <= max_pages:
            # Progress update every 100 pages
            if ctx and page % 100 == 0:
                pct = (page / max_pages) * 100
                await ctx.send(f"⏳ Progress: {page}/{max_pages} ({pct:.1f}%) - {len(all_entries)} expenses collected")
            
            url = f"{self.income_url}" if page == 1 else f"{self.income_url}?page={page}"
            await self._debug_log(f"🌐 Page {page}: {url}", ctx)
            
            try:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        empty_count += 1
                        if empty_count >= 3: 
                            break
                        page += 1
                        continue
                    
                    html = await resp.text()
                    if not await self._check_logged_in(html, ctx): 
                        break
                    
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find expense table by structure: 4 columns (Credits, Name, Description, Date)
                    page_entries = []
                    for table in soup.find_all('table'):
                        rows = table.find_all('tr')
                        if len(rows) < 2: 
                            continue
                        
                        # Check header row for expense table signature
                        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all('th')]
                        if headers[:4] != ["credits", "name", "description", "date"]:
                            continue
                        
                        # Parse data rows
                        for row in rows[1:]:
                            cols = row.find_all('td')
                            if len(cols) < 4:
                                continue
                            
                            # Column order: Credits, Name, Description, Date
                            credits_col = cols[0].get_text(strip=True)
                            name_col = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                            desc_col = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                            date_col = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                            
                            # Parse credits
                            credits_match = re.search(r'([\d,]+)', credits_col)
                            if not credits_match: 
                                continue
                            
                            amount = int(credits_match.group(1).replace(',', ''))
                            if amount < 100: 
                                continue  # Skip invalid
                            amount = min(amount, INT64_MAX)
                            
                            # Get username (might have link)
                            username = name_col
                            link = cols[1].find('a') if len(cols) > 1 else None
                            if link:
                                username = link.get_text(strip=True)
                            
                            page_entries.append({
                                'entry_type': 'expense',
                                'period': 'paginated',
                                'username': username,
                                'amount': amount,
                                'description': desc_col,
                                'source_date': date_col,
                            })
                    
                    if not page_entries:
                        empty_count += 1
                        if empty_count >= 3:
                            await self._debug_log("⛔ Stopped after 3 empty pages", ctx)
                            break
                    else:
                        await self._debug_log(f"✅ Page {page}: {len(page_entries)} expenses", ctx)
                        all_entries.extend(page_entries)
                        empty_count = 0
                    
                    page += 1
                    await asyncio.sleep(1.5)
                    
            except Exception as e:
                await self._debug_log(f"❌ Error page {page}: {str(e)}", ctx)
                page += 1
        
        await self._debug_log(f"📊 Total: {len(all_entries)} expenses", ctx)
        return all_entries
    
    async def _scrape_all_income(self, ctx=None, include_expenses=True, max_expense_pages=100):
        """Serialize income scrapes so scheduled tasks cannot overlap."""
        async with self._scrape_lock:
            return await self._scrape_all_income_unlocked(
                ctx,
                include_expenses,
                max_expense_pages,
            )

    async def _scrape_all_income_unlocked(self, ctx=None, include_expenses=True, max_expense_pages=100):
        """Scrape daily income, monthly income, and expenses from the treasury page"""
        session = await self._get_session(ctx)
        if not session:
            if ctx:
                await ctx.send("❌ Failed to get session")
            return False
        
        await self._debug_log("🚀 Starting income/expense scrape", ctx)
        
        income_data = []
        expenses_data = []
        
        # 1. Scrape daily income/expense tab
        await self._debug_log("📅 Scraping DAILY income tab...", ctx)
        daily_data = await self._scrape_income_tab(session, 'daily', ctx)
        income_data.extend(daily_data)
        
        await asyncio.sleep(1.5)
        
        # 2. Scrape monthly income/expense tab
        await self._debug_log("📆 Scraping MONTHLY income tab...", ctx)
        monthly_data = await self._scrape_income_tab(session, 'monthly', ctx)
        income_data.extend(monthly_data)
        
        await asyncio.sleep(1.5)
        
        # 3. Scrape expenses with pagination
        if include_expenses:
            expenses_data = await self._scrape_expenses_pages(session, ctx, max_expense_pages)
        
        # Store in database
        if not income_data and not expenses_data:
            if ctx:
                await ctx.send("❌ No income/expense data found")
            return False
        
        scraped_at_dt = datetime.now(ZoneInfo("UTC"))
        timestamp = scraped_at_dt.isoformat()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        inserted = 0
        duplicates = 0
        
        for entry in income_data:
            try:
                cursor.execute('''
                    INSERT INTO income (entry_type, period, username, amount, description, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (entry['entry_type'], entry['period'], entry['username'], 
                      entry['amount'], entry.get('description', ''), timestamp))
                inserted += 1
            except sqlite3.IntegrityError:
                duplicates += 1

        # Preserve the existing legacy table contract for current commands/readers.
        for entry in expenses_data:
            try:
                cursor.execute(
                    """
                    INSERT INTO income (
                        entry_type, period, username, amount, description, timestamp
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry["entry_type"],
                        entry["period"],
                        entry["username"],
                        entry["amount"],
                        entry.get("description", ""),
                        timestamp,
                    ),
                )
            except sqlite3.IntegrityError:
                pass

        self._assign_expense_occurrences(expenses_data, scraped_at_dt)
        for entry in expenses_data:
            try:
                cursor.execute(
                    """
                    INSERT INTO expenses (
                        signature, occurrence_index, username, amount, description,
                        source_date, event_timestamp, scraped_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry["signature"],
                        entry["occurrence_index"],
                        entry["username"],
                        entry["amount"],
                        entry.get("description", ""),
                        entry["source_date"],
                        entry["event_timestamp"],
                        timestamp,
                    ),
                )
                inserted += 1
            except sqlite3.IntegrityError:
                duplicates += 1
        
        conn.commit()
        conn.close()
        
        await self._debug_log(f"💾 Database: {inserted} inserted, {duplicates} duplicates", ctx)
        
        if ctx:
            await ctx.send(f"✅ Scraped {len(income_data) + len(expenses_data)} income/expense entries\n"
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
    
    @income_group.command(name="cleanexpenses")
    async def clean_expenses(self, ctx):
        """Delete all paginated expense records (use before backfill)"""
        await ctx.send("⚠️ This will DELETE all paginated expense records! Type 'YES' to confirm.")
        
        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel
        
        try:
            msg = await self.bot.wait_for('message', check=check, timeout=30.0)
            if msg.content.upper() != 'YES':
                return await ctx.send("❌ Cancelled")
        except asyncio.TimeoutError:
            return await ctx.send("❌ Timed out")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM income WHERE entry_type='expense' AND period='paginated'")
        legacy_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM expenses")
        expense_count = cursor.fetchone()[0]
        
        cursor.execute("DELETE FROM income WHERE entry_type='expense' AND period='paginated'")
        cursor.execute("DELETE FROM expenses")
        conn.commit()
        conn.close()
        
        await ctx.send(
            f"✅ Deleted {legacy_count + expense_count} paginated expense records. "
            "Ready for backfill!"
        )
    
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

        cursor.execute("SELECT COUNT(*) FROM expenses")
        preserved_expense_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM expenses WHERE event_timestamp IS NOT NULL")
        timestamped_expense_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM expenses WHERE occurrence_index > 1")
        repeated_expense_count = cursor.fetchone()[0]

        cursor.execute("SELECT MAX(scraped_at) FROM expenses")
        latest_expense_scrape = cursor.fetchone()[0]

        cursor.execute(
            "SELECT period, MAX(timestamp) FROM income "
            "WHERE entry_type='income' AND period IN ('daily', 'monthly') "
            "GROUP BY period"
        )
        latest_income_snapshots = dict(cursor.fetchall())
        
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

        embed.add_field(
            name="Preserved Expense Entries",
            value=f"{preserved_expense_count:,}",
            inline=True,
        )
        embed.add_field(
            name="Timestamped Expenses",
            value=f"{timestamped_expense_count:,} / {preserved_expense_count:,}",
            inline=True,
        )
        embed.add_field(
            name="Repeated Expenses Preserved",
            value=f"{repeated_expense_count:,}",
            inline=True,
        )
        embed.add_field(
            name="Latest Expense Scrape (UTC)",
            value=latest_expense_scrape or "None",
            inline=False,
        )
        embed.add_field(
            name="Latest Income Snapshots",
            value=(
                f"Daily: {latest_income_snapshots.get('daily', 'None')}\n"
                f"Monthly: {latest_income_snapshots.get('monthly', 'None')}"
            ),
            inline=False,
        )
        
        embed.set_footer(text=f"Database: {self.db_path.name}")
        
        await ctx.send(embed=embed)
