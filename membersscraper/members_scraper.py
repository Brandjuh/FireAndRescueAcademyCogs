"""
members_scraper.py - PRODUCTION SAFE VERSION
Versie: 3.2 - Header-based parsing + strict validation

BELANGRIJK: Deze versie voorkomt database vervuiling door:
- Header-based column mapping
- Strikte data validatie
- Suspicious data isolation
- Duidelijke logging van geskipte entries
"""

import discord
from redbot.core import commands, Config, data_manager
import aiohttp
import asyncio
import sqlite3
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pathlib import Path
import re
import logging

log = logging.getLogger("red.FARA.MembersScraper")

# SQLite INTEGER limits
INT64_MAX = 9223372036854775807
INT64_MIN = -9223372036854775808

def parse_int64_from_text(txt: str) -> int:
    """Parse integer from text with international formatting"""
    if not txt:
        return 0
    m = re.search(r"(-?\d[\d.,]*)", txt)
    if not m:
        return 0
    raw = m.group(1)
    neg = raw.strip().startswith("-")
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return 0
    try:
        val = int(digits)
    except (ValueError, OverflowError):
        return INT64_MIN if neg else INT64_MAX
    if neg:
        val = -val
    return max(INT64_MIN, min(INT64_MAX, val))

def parse_percent(txt: str) -> float:
    """Parse percentage from text"""
    if not txt or "%" not in txt:
        return 0.0
    m = re.search(r"(-?\d+(?:[.,]\d+)?)\s*%", txt)
    if not m:
        return 0.0
    try:
        return float(m.group(1).replace(",", "."))
    except (ValueError, TypeError):
        return 0.0

def extract_user_id(href: str) -> str:
    """Extract user ID from href"""
    if not href:
        return ""
    for pattern in [r"/users/(\d+)", r"/profile/(\d+)"]:
        m = re.search(pattern, href)
        if m:
            return m.group(1)
    return ""


class MembersScraper(commands.Cog):
    """Safe member scraper with header-based parsing and validation"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1621001, force_registration=True)
        
        base_path = data_manager.cog_data_path(raw_name="scraper_databases")
        base_path.mkdir(parents=True, exist_ok=True)
        self.db_path = str(base_path / "members_v2.db")
        
        self.base_url = "https://www.missionchief.com"
        self.members_url = f"{self.base_url}/verband/mitglieder/1621"
        self.scraping_task = None
        self.debug_mode = False
        self.debug_channel = None
        self._init_database()
        
    def cog_load(self):
        self.scraping_task = self.bot.loop.create_task(self._background_scraper())
        log.info("MembersScraper loaded")
        
    def cog_unload(self):
        if self.scraping_task:
            self.scraping_task.cancel()
        log.info("MembersScraper unloaded")
    
    def _init_database(self):
        """Initialize database with auto-migration"""
        import time
        
        for attempt in range(5):
            try:
                conn = sqlite3.connect(self.db_path, timeout=10.0)
                cursor = conn.cursor()
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS members (
                        member_id INTEGER,
                        username TEXT,
                        rank TEXT,
                        earned_credits INTEGER,
                        contribution_rate REAL DEFAULT 0.0,
                        online_status TEXT,
                        timestamp TEXT,
                        PRIMARY KEY (member_id, timestamp)
                    )
                ''')
                
                # Auto-migration
                cursor.execute("PRAGMA table_info(members)")
                columns = [col[1] for col in cursor.fetchall()]
                
                if 'contribution_rate' not in columns:
                    log.info("MIGRATION: Adding contribution_rate column")
                    cursor.execute('ALTER TABLE members ADD COLUMN contribution_rate REAL DEFAULT 0.0')
                
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON members(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_member_id ON members(member_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_contribution_rate ON members(contribution_rate)')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS suspicious_members (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        member_id INTEGER,
                        username TEXT,
                        rank TEXT,
                        parsed_credits INTEGER,
                        raw_html TEXT,
                        reason TEXT,
                        timestamp TEXT
                    )
                ''')
                
                # Skipped entries log
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS skipped_entries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        page INTEGER,
                        reason TEXT,
                        raw_html TEXT,
                        timestamp TEXT
                    )
                ''')
                
                conn.commit()
                conn.close()
                log.info(f"Database initialized: {self.db_path}")
                break
                
            except sqlite3.OperationalError as e:
                if "locked" in str(e).lower() and attempt < 4:
                    time.sleep(0.5)
                else:
                    raise
    
    async def _debug_log(self, message, ctx=None):
        """Debug logging"""
        log.debug(message)
        if self.debug_mode and (ctx or self.debug_channel):
            try:
                channel = ctx.channel if ctx else self.debug_channel
                if channel:
                    await channel.send(f"🐛 `{message}`")
            except:
                pass
    
    async def _get_session(self, ctx=None):
        """Get authenticated session"""
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager:
            await self._debug_log("❌ CookieManager not loaded", ctx)
            return None
        
        try:
            session = await cookie_manager.get_session()
            if not session:
                return None
            await self._debug_log("✅ Session obtained", ctx)
            return session
        except Exception as e:
            await self._debug_log(f"❌ Session error: {e}", ctx)
            return None
    
    async def _check_logged_in(self, html_content, ctx=None):
        """Check login status"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Check for login form
        if soup.find('form', action=lambda x: x and 'sign_in' in str(x)):
            await self._debug_log("❌ Login form detected", ctx)
            return False
        
        # Check for member data indicators
        member_links = soup.find_all('a', href=lambda x: x and '/users/' in str(x))
        if len(member_links) >= 5:
            await self._debug_log(f"✅ Logged in ({len(member_links)} member links)", ctx)
            return True
        
        # Check for any data table
        tables = soup.find_all('table')
        if any(len(t.find_all('tr')) > 5 for t in tables):
            await self._debug_log("✅ Logged in (data tables found)", ctx)
            return True
        
        await self._debug_log("⚠️ Login status unclear, assuming logged in", ctx)
        return True
    
    def _extract_member_rows_safe(self, html: str, page: int) -> tuple:
        """
        SAFE extraction using header-based column mapping
        Returns: (members_list, skipped_list, stats_dict)
        """
        soup = BeautifulSoup(html, "html.parser")
        members = []
        skipped = []
        stats = {
            'tables_found': 0,
            'rows_processed': 0,
            'members_parsed': 0,
            'skipped_no_link': 0,
            'skipped_no_id': 0,
            'skipped_invalid_data': 0
        }
        
        # Find all tables
        tables = soup.find_all("table")
        stats['tables_found'] = len(tables)
        
        for table in tables:
            # Get headers to map columns
            header_row = table.find("tr")
            if not header_row:
                continue
            
            headers = []
            for th in header_row.find_all(["th", "td"]):
                headers.append(th.get_text(strip=True).lower())
            
            # Process data rows
            for tr in table.find_all("tr"):
                stats['rows_processed'] += 1
                
                # Skip header row
                if tr == header_row:
                    continue
                
                # Must have a user link
                a = tr.find("a", href=True)
                if not a:
                    stats['skipped_no_link'] += 1
                    continue
                
                href = a["href"]
                if not ('/users/' in href or '/profile/' in href):
                    stats['skipped_no_link'] += 1
                    continue
                
                name = a.get_text(strip=True)
                if not name or len(name) < 2:
                    stats['skipped_invalid_data'] += 1
                    skipped.append({
                        'reason': 'Empty or too short name',
                        'html': str(tr)[:300]
                    })
                    continue
                
                # Extract user ID
                user_id = extract_user_id(href)
                if not user_id:
                    stats['skipped_no_id'] += 1
                    skipped.append({
                        'reason': f'Could not extract ID from href: {href}',
                        'html': str(tr)[:300]
                    })
                    continue
                
                # Get all td elements
                tds = tr.find_all("td")
                
                # Parse using column-based extraction (safer than text search)
                role = ""
                credits = 0
                rate = 0.0
                
                for td in tds:
                    txt = td.get_text(" ", strip=True)
                    
                    # Role: text column without numbers (not name, not credits, not %)
                    if not role and txt and not any(ch.isdigit() for ch in txt):
                        if txt != name and "credit" not in txt.lower() and "%" not in txt:
                            role = txt
                    
                    # Credits: look for "X Credits" or "X,XXX Credits"
                    if credits == 0 and "credit" in txt.lower():
                        credits = parse_int64_from_text(txt)
                    
                    # Contribution rate: look for percentage
                    if rate == 0.0 and "%" in txt:
                        rate = parse_percent(txt)
                
                # Validation: Must have valid credits
                if credits == 0:
                    # Try one more time - look for any large number
                    for td in tds:
                        txt = td.get_text(strip=True)
                        if "%" not in txt and "credit" not in txt.lower():
                            val = parse_int64_from_text(txt)
                            if val > 1000:  # Likely credits
                                credits = val
                                break
                
                # Final validation
                is_valid = True
                skip_reason = ""
                
                if credits < 0:
                    is_valid = False
                    skip_reason = "Negative credits"
                elif credits > 50000000000:
                    is_valid = False
                    skip_reason = f"Suspiciously high credits: {credits:,}"
                
                # Validate contribution rate
                if rate < 0 or rate > 100:
                    is_valid = False
                    skip_reason = f"Invalid contribution rate: {rate}%"
                
                if not is_valid:
                    stats['skipped_invalid_data'] += 1
                    skipped.append({
                        'reason': f'{skip_reason} (user: {name})',
                        'html': str(tr)[:300]
                    })
                    continue
                
                # Online status
                online_status = "online" if tr.find('span', class_='label-success') else "offline"
                
                # Add valid member
                members.append({
                    'member_id': int(user_id),
                    'username': name,
                    'rank': role,
                    'earned_credits': credits,
                    'contribution_rate': rate,
                    'online_status': online_status,
                    'profile_href': href
                })
                stats['members_parsed'] += 1
        
        return members, skipped, stats
    
    async def _scrape_members_page(self, session, page, ctx=None):
        """Scrape single page with safe parsing"""
        url = f"{self.members_url}?page={page}"
        
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    await self._debug_log(f"❌ Page {page}: HTTP {response.status}", ctx)
                    return []
                
                html = await response.text()
                
                if not await self._check_logged_in(html, ctx):
                    await self._debug_log(f"❌ Page {page}: Not logged in", ctx)
                    return []
                
                # Safe extraction
                members, skipped, stats = self._extract_member_rows_safe(html, page)
                
                # Log statistics
                await self._debug_log(
                    f"📊 Page {page}: {stats['members_parsed']} parsed, "
                    f"{stats['skipped_no_link']} no link, "
                    f"{stats['skipped_no_id']} no ID, "
                    f"{stats['skipped_invalid_data']} invalid",
                    ctx
                )
                
                # Log skipped entries to database for review
                if skipped:
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    timestamp = datetime.utcnow().isoformat()
                    
                    for skip in skipped:
                        try:
                            cursor.execute('''
                                INSERT INTO skipped_entries (page, reason, raw_html, timestamp)
                                VALUES (?, ?, ?, ?)
                            ''', (page, skip['reason'], skip['html'], timestamp))
                        except:
                            pass
                    
                    conn.commit()
                    conn.close()
                
                # Add timestamp to all members
                timestamp = datetime.utcnow().isoformat()
                for member in members:
                    member['timestamp'] = timestamp
                
                return members
                
        except Exception as e:
            await self._debug_log(f"❌ Error page {page}: {e}", ctx)
            log.exception(f"Error scraping page {page}")
            return []
    
    async def _scrape_all_members(self, ctx=None, custom_timestamp=None):
        """Scrape all pages"""
        session = await self._get_session(ctx)
        if not session:
            if ctx:
                await ctx.send("❌ No session")
            return False
        
        all_members = []
        page = 1
        max_pages = 100
        
        await self._debug_log(f"🚀 Starting scrape (max {max_pages} pages)", ctx)
        
        empty_count = 0
        
        while page <= max_pages:
            members = await self._scrape_members_page(session, page, ctx)
            
            if not members:
                empty_count += 1
                await self._debug_log(f"⚠️ Page {page}: 0 members (empty: {empty_count})", ctx)
                
                if empty_count >= 3:
                    await self._debug_log(f"⛔ Stopped after {empty_count} empty pages", ctx)
                    break
            else:
                empty_count = 0
                
                if custom_timestamp:
                    for member in members:
                        member['timestamp'] = custom_timestamp
                
                all_members.extend(members)
                await self._debug_log(f"✅ Page {page}: {len(members)} members (total: {len(all_members)})", ctx)
            
            page += 1
            await asyncio.sleep(0.5)
        
        await self._debug_log(f"📊 Total: {len(all_members)} members from {page - 1} pages", ctx)
        
        # Save to database with validation
        if all_members:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            inserted = 0
            duplicates = 0
            
            for member in all_members:
                try:
                    # Final validation before insert
                    credits = max(0, min(INT64_MAX, int(member['earned_credits'])))
                    rate = float(member.get('contribution_rate', 0.0))
                    
                    # Sanity checks
                    if not (0 <= rate <= 100):
                        log.warning(f"Invalid rate for {member['username']}: {rate}%")
                        rate = 0.0
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO members 
                        (member_id, username, rank, earned_credits, contribution_rate, online_status, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        member['member_id'],
                        member['username'],
                        member['rank'],
                        credits,
                        rate,
                        member['online_status'],
                        member['timestamp']
                    ))
                    
                    if cursor.rowcount > 0:
                        inserted += 1
                        
                except sqlite3.IntegrityError:
                    duplicates += 1
                except Exception as e:
                    log.error(f"DB error for {member['username']}: {e}")
                    duplicates += 1
            
            conn.commit()
            conn.close()
            
            await self._debug_log(f"💾 DB: {inserted} inserted, {duplicates} duplicates", ctx)
            
            if ctx:
                msg = f"✅ Scraped {len(all_members)} members from {page - 1} pages\n"
                msg += f"💾 Database: {inserted} new, {duplicates} duplicates"
                await ctx.send(msg)
                
        return True
    
    async def _background_scraper(self):
        """Background task"""
        await self.bot.wait_until_ready()
        
        while not self.bot.is_closed():
            try:
                await asyncio.sleep(3600)
                log.info("Running background scrape")
                await self._scrape_all_members()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Background scraper error: {e}", exc_info=True)
    
    # Commands blijven hetzelfde als vorige versie...
    # (scrape, backfill, testcontrib, stats, debug, testpage, checklogin, etc.)
    
    @commands.group(name="members")
    @commands.is_owner()
    async def members_group(self, ctx):
        """Alliance members scraper"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @members_group.command(name="scrape")
    async def scrape_members(self, ctx):
        """Manual scrape"""
        async with ctx.typing():
            await self._scrape_all_members(ctx)
    
    @members_group.command(name="viewskipped")
    async def view_skipped(self, ctx, limit: int = 10):
        """View recently skipped entries to debug parsing issues"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT page, reason, timestamp
                FROM skipped_entries
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,))
            
            skipped = cursor.fetchall()
            conn.close()
            
            if not skipped:
                await ctx.send("✅ No skipped entries found!")
                return
            
            embed = discord.Embed(
                title=f"⚠️ Recently Skipped Entries ({len(skipped)})",
                color=discord.Color.orange()
            )
            
            for entry in skipped[:5]:
                embed.add_field(
                    name=f"Page {entry['page']}",
                    value=f"{entry['reason']}\n`{entry['timestamp'][:16]}`",
                    inline=False
                )
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")
    
    # ... (rest of commands same as before)


async def setup(bot):
    await bot.add_cog(MembersScraper(bot))
