"""
Alliance Leaderboard System for Missionchief USA
Displays daily and monthly top 10 rankings for earned credits and treasury contributions.
Uses NEW scraper_databases structure (members_v2.db, income_v2.db)
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path

import discord
import aiosqlite
from redbot.core import commands, Config, checks
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path
import pytz

log = logging.getLogger("red.leaderboard")

DEFAULTS = {
    "daily_earned_channel": None,
    "daily_contrib_channel": None,
    "monthly_earned_channel": None,
    "monthly_contrib_channel": None,
}

MEDALS = {
    1: "🥇",
    2: "🥈", 
    3: "🥉"
}

# Blacklist for corrupted/invalid entries
BLACKLISTED_USER_IDS = [
    "26856671065906104064",  # Corrupted user ID
]

BLACKLISTED_USERNAMES = [
    "Yeehaw12121212212112",  # Invalid username pattern
    "52525255252",  # Invalid username (looks like ID)
]

# Sanity check: INT64_MAX indicates parsing error
INT64_MAX = 9223372036854775807


class Leaderboard(commands.Cog):
    """Alliance leaderboard system - daily and monthly top 10 rankings."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0x4C45414442, force_registration=True)
        self.config.register_global(**DEFAULTS)
        
        # NEW: Use scraper_databases folder
        base_path = cog_data_path(raw_name="scraper_databases")
        self.members_db_path = base_path / "members_v2.db"
        self.income_db_path = base_path / "income_v2.db"
        
        self._daily_task: Optional[asyncio.Task] = None
        self._monthly_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        """Start background tasks on cog load."""
        self._daily_task = asyncio.create_task(self._daily_loop())
        self._monthly_task = asyncio.create_task(self._monthly_loop())
        log.info("Leaderboard cog loaded - tasks started")

    async def cog_unload(self):
        """Cancel background tasks on cog unload."""
        if self._daily_task:
            self._daily_task.cancel()
        if self._monthly_task:
            self._monthly_task.cancel()
        log.info("Leaderboard cog unloaded")

    # ==================== BACKGROUND TASKS ====================

    async def _daily_loop(self):
        """Daily leaderboard posting loop - runs at 06:00 Amsterdam time."""
        await self.bot.wait_until_ready()
        tz = pytz.timezone('Europe/Amsterdam')
        
        while True:
            try:
                now = datetime.now(tz)
                # Calculate next 06:00
                target = now.replace(hour=6, minute=0, second=0, microsecond=0)
                if now >= target:
                    target += timedelta(days=1)
                
                wait_seconds = (target - now).total_seconds()
                log.info(f"Daily leaderboard: waiting {wait_seconds:.0f}s until {target}")
                
                await asyncio.sleep(wait_seconds)
                
                # Post daily leaderboards
                await self._post_daily_leaderboards()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception(f"Error in daily leaderboard loop: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error

    async def _monthly_loop(self):
        """Monthly leaderboard posting loop - runs on last day of month at 06:00 Amsterdam time."""
        await self.bot.wait_until_ready()
        tz = pytz.timezone('Europe/Amsterdam')
        
        while True:
            try:
                now = datetime.now(tz)
                
                # Calculate next last day of month at 06:00
                # Try this month first
                last_day = self._get_last_day_of_month(now.year, now.month)
                target = now.replace(day=last_day, hour=6, minute=0, second=0, microsecond=0)
                
                # If we're past that time, go to next month
                if now >= target:
                    next_month = now.month + 1 if now.month < 12 else 1
                    next_year = now.year if now.month < 12 else now.year + 1
                    last_day = self._get_last_day_of_month(next_year, next_month)
                    target = datetime(next_year, next_month, last_day, 6, 0, 0, tzinfo=tz)
                
                wait_seconds = (target - now).total_seconds()
                log.info(f"Monthly leaderboard: waiting {wait_seconds:.0f}s until {target}")
                
                await asyncio.sleep(wait_seconds)
                
                # Post monthly leaderboards
                await self._post_monthly_leaderboards()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception(f"Error in monthly leaderboard loop: {e}")
                await asyncio.sleep(300)

    def _get_last_day_of_month(self, year: int, month: int) -> int:
        """Get last day of given month."""
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        last_day = next_month - timedelta(days=1)
        return last_day.day

    # ==================== POSTING LOGIC ====================

    async def _post_daily_leaderboards(self):
        """Post both daily earned credits and contributions leaderboards."""
        log.info("Posting daily leaderboards...")
        
        # Get earned credits data
        earned_data = await self._get_earned_credits_rankings(period='daily')
        if earned_data:
            earned_embed = self._create_leaderboard_embed(
                title="🏆 Daily Top 10 - Earned Credits",
                current_rankings=earned_data['current'],
                previous_rankings=earned_data['previous'],
                period='daily',
                metric='earned_credits'
            )
            channel_id = await self.config.daily_earned_channel()
            if channel_id:
                await self._send_to_channel(channel_id, earned_embed)
        
        # Get treasury contributions data
        contrib_data = await self._get_treasury_rankings(period='daily')
        if contrib_data:
            contrib_embed = self._create_leaderboard_embed(
                title="💰 Daily Top 10 - Treasury Contributions",
                current_rankings=contrib_data['current'],
                previous_rankings=contrib_data['previous'],
                period='daily',
                metric='contributions'
            )
            channel_id = await self.config.daily_contrib_channel()
            if channel_id:
                await self._send_to_channel(channel_id, contrib_embed)

    async def _post_monthly_leaderboards(self):
        """Post both monthly earned credits and contributions leaderboards."""
        log.info("Posting monthly leaderboards...")
        
        # Get earned credits data
        earned_data = await self._get_earned_credits_rankings(period='monthly')
        if earned_data:
            earned_embed = self._create_leaderboard_embed(
                title="🏆 Monthly Top 10 - Earned Credits",
                current_rankings=earned_data['current'],
                previous_rankings=earned_data['previous'],
                period='monthly',
                metric='earned_credits'
            )
            channel_id = await self.config.monthly_earned_channel()
            if channel_id:
                await self._send_to_channel(channel_id, earned_embed)
        
        # Get treasury contributions data
        contrib_data = await self._get_treasury_rankings(period='monthly')
        if contrib_data:
            contrib_embed = self._create_leaderboard_embed(
                title="💰 Monthly Top 10 - Treasury Contributions",
                current_rankings=contrib_data['current'],
                previous_rankings=contrib_data['previous'],
                period='monthly',
                metric='contributions'
            )
            channel_id = await self.config.monthly_contrib_channel()
            if channel_id:
                await self._send_to_channel(channel_id, contrib_embed)

    async def _send_to_channel(self, channel_id: int, embed: discord.Embed):
        """Send embed to specified channel."""
        try:
            channel = self.bot.get_channel(channel_id)
            if channel:
                await channel.send(embed=embed)
                log.info(f"Posted leaderboard to channel {channel_id}")
            else:
                log.warning(f"Channel {channel_id} not found")
        except Exception as e:
            log.exception(f"Error sending to channel {channel_id}: {e}")

    # ==================== FILTERING ====================

    def _filter_invalid_entries(self, rankings: List[Dict], metric: str) -> List[Dict]:
        """Filter out corrupted/invalid entries from rankings."""
        filtered = []
        for entry in rankings:
            member_id = str(entry.get('member_id', ''))
            username = entry.get('username', '')
            
            # Check blacklists
            if member_id in BLACKLISTED_USER_IDS:
                log.warning(f"Filtered blacklisted member_id: {member_id}")
                continue
            
            if username in BLACKLISTED_USERNAMES:
                log.warning(f"Filtered blacklisted username: {username}")
                continue
            
            # Check for INT64_MAX (parsing error indicator)
            if metric == 'earned_credits':
                value = entry.get('earned_credits', 0)
            else:
                value = entry.get('amount', 0)
            
            if value >= INT64_MAX:
                log.warning(f"Filtered INT64_MAX value for {username}: {value}")
                continue
            
            # Check for suspiciously numeric usernames (likely IDs)
            if username and username.isdigit() and len(username) > 10:
                log.warning(f"Filtered numeric username (likely ID): {username}")
                continue
            
            filtered.append(entry)
        
        return filtered

    # ==================== DATA RETRIEVAL ====================

    async def _get_earned_credits_rankings(self, period: str) -> Optional[Dict]:
        """
        Get earned credits rankings from members_v2.db.
        Calculates DELTA between current and previous timestamps.
        Returns dict with 'current' and 'previous' rankings.
        """
        if not self.members_db_path.exists():
            log.error(f"Database not found at {self.members_db_path}")
            return None

        async with aiosqlite.connect(self.members_db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Get most recent timestamp
            cur = await db.execute("""
                SELECT MAX(timestamp) as latest FROM members
            """)
            row = await cur.fetchone()
            if not row or not row['latest']:
                return None
            
            current_time = row['latest']
            
            # Calculate previous time based on period
            if period == 'daily':
                dt = datetime.fromisoformat(current_time)
                previous_dt = dt - timedelta(days=1)
            else:  # monthly
                dt = datetime.fromisoformat(current_time)
                previous_dt = dt - timedelta(days=30)
            
            previous_time = previous_dt.isoformat()
            
            # Find closest previous timestamp
            cur = await db.execute("""
                SELECT timestamp FROM members
                WHERE timestamp <= ?
                GROUP BY timestamp
                ORDER BY timestamp DESC
                LIMIT 1
            """, (previous_time,))
            prev_time_row = await cur.fetchone()
            
            if not prev_time_row:
                log.warning("No previous period data found for comparison")
                return None
            
            previous_scrape_time = prev_time_row['timestamp']
            
            # Get current period data
            cur = await db.execute("""
                SELECT member_id, username, earned_credits, timestamp
                FROM members
                WHERE timestamp = ?
                ORDER BY earned_credits DESC
            """, (current_time,))
            current_data = {row['member_id']: dict(row) for row in await cur.fetchall()}
            
            # Get previous period data
            cur = await db.execute("""
                SELECT member_id, username, earned_credits, timestamp
                FROM members
                WHERE timestamp = ?
                ORDER BY earned_credits DESC
            """, (previous_scrape_time,))
            previous_data = {row['member_id']: dict(row) for row in await cur.fetchall()}
            
            # Calculate deltas (growth in the period)
            deltas = []
            for member_id, current_entry in current_data.items():
                current_credits = current_entry['earned_credits']
                
                # Get previous credits (or 0 if new member)
                previous_credits = 0
                if member_id in previous_data:
                    previous_credits = previous_data[member_id]['earned_credits']
                
                # Calculate delta
                delta = current_credits - previous_credits
                
                if delta > 0:  # Only include members with positive growth
                    deltas.append({
                        'member_id': member_id,
                        'username': current_entry['username'],
                        'earned_credits': delta,
                        'timestamp': current_entry['timestamp']
                    })
            
            # Sort by delta and get top entries
            deltas.sort(key=lambda x: x['earned_credits'], reverse=True)
            current_raw = deltas[:20]
            current = self._filter_invalid_entries(current_raw, 'earned_credits')[:10]
            
            # For previous rankings, calculate deltas from one period before that
            # Find timestamp before previous
            if period == 'daily':
                dt = datetime.fromisoformat(previous_scrape_time)
                before_previous_dt = dt - timedelta(days=1)
            else:
                dt = datetime.fromisoformat(previous_scrape_time)
                before_previous_dt = dt - timedelta(days=30)
            
            before_previous_time = before_previous_dt.isoformat()
            
            cur = await db.execute("""
                SELECT timestamp FROM members
                WHERE timestamp <= ?
                GROUP BY timestamp
                ORDER BY timestamp DESC
                LIMIT 1
            """, (before_previous_time,))
            before_prev_row = await cur.fetchone()
            
            previous = []
            if before_prev_row:
                # Get data from before previous period
                cur = await db.execute("""
                    SELECT member_id, username, earned_credits, timestamp
                    FROM members
                    WHERE timestamp = ?
                """, (before_prev_row['timestamp'],))
                before_previous_data = {row['member_id']: dict(row) for row in await cur.fetchall()}
                
                # Calculate deltas for previous period
                prev_deltas = []
                for member_id, prev_entry in previous_data.items():
                    prev_credits = prev_entry['earned_credits']
                    
                    before_credits = 0
                    if member_id in before_previous_data:
                        before_credits = before_previous_data[member_id]['earned_credits']
                    
                    delta = prev_credits - before_credits
                    
                    if delta > 0:
                        prev_deltas.append({
                            'member_id': member_id,
                            'username': prev_entry['username'],
                            'earned_credits': delta,
                            'timestamp': prev_entry['timestamp']
                        })
                
                prev_deltas.sort(key=lambda x: x['earned_credits'], reverse=True)
                previous_raw = prev_deltas[:30]
                previous = self._filter_invalid_entries(previous_raw, 'earned_credits')[:20]
            
            return {
                'current': current,
                'previous': previous
            }

    async def _get_treasury_rankings(self, period: str) -> Optional[Dict]:
        """
        Get treasury contribution rankings from income_v2.db.
        NOTE: IncomeScraper stores member contributions as 'expense' type!
        Returns dict with 'current' and 'previous' rankings.
        """
        if not self.income_db_path.exists():
            log.error(f"Database not found at {self.income_db_path}")
            return None

        async with aiosqlite.connect(self.income_db_path) as db:
            db.row_factory = aiosqlite.Row
            
            period_type = 'daily' if period == 'daily' else 'monthly'
            
            # NOTE: Using 'expense' because IncomeScraper stores contributions as expenses!
            # Get most recent timestamp for this period
            cur = await db.execute("""
                SELECT MAX(timestamp) as latest 
                FROM income 
                WHERE period = ? AND entry_type = 'expense'
            """, (period_type,))
            row = await cur.fetchone()
            if not row or not row['latest']:
                return None
            
            current_time = row['latest']
            
            # Get current rankings
            cur = await db.execute("""
                SELECT username, amount, timestamp
                FROM income
                WHERE period = ? AND entry_type = 'expense' AND timestamp = ?
                ORDER BY amount DESC
                LIMIT 20
            """, (period_type, current_time))
            current_raw = [dict(row) for row in await cur.fetchall()]
            current = self._filter_invalid_entries(current_raw, 'contributions')[:10]
            
            # Get previous period timestamp
            if period == 'daily':
                dt = datetime.fromisoformat(current_time)
                previous_dt = dt - timedelta(days=1)
                previous_time = previous_dt.isoformat()
            else:
                dt = datetime.fromisoformat(current_time)
                previous_dt = dt - timedelta(days=30)
                previous_time = previous_dt.isoformat()
            
            cur = await db.execute("""
                SELECT timestamp FROM income
                WHERE period = ? AND entry_type = 'expense' AND timestamp <= ?
                GROUP BY timestamp
                ORDER BY timestamp DESC
                LIMIT 1
            """, (period_type, previous_time))
            prev_time_row = await cur.fetchone()
            
            previous = []
            if prev_time_row:
                cur = await db.execute("""
                    SELECT username, amount, timestamp
                    FROM income
                    WHERE period = ? AND entry_type = 'expense' AND timestamp = ?
                    ORDER BY amount DESC
                    LIMIT 30
                """, (period_type, prev_time_row['timestamp']))
                previous_raw = [dict(row) for row in await cur.fetchall()]
                previous = self._filter_invalid_entries(previous_raw, 'contributions')[:20]
            
            return {
                'current': current,
                'previous': previous
            }

    # ==================== EMBED CREATION ====================

    def _create_leaderboard_embed(
        self,
        title: str,
        current_rankings: List[Dict],
        previous_rankings: List[Dict],
        period: str,
        metric: str
    ) -> discord.Embed:
        """Create a formatted leaderboard embed with position changes."""
        
        embed = discord.Embed(
            title=title,
            color=discord.Color.gold(),
            timestamp=datetime.utcnow()
        )
        
        # Create previous rankings lookup by username (income_v2.db doesn't have member_id)
        prev_lookup = {}
        for idx, player in enumerate(previous_rankings, 1):
            username = player.get('username', '')
            prev_lookup[username] = idx
        
        # Build leaderboard text
        lines = []
        for idx, player in enumerate(current_rankings, 1):
            medal = MEDALS.get(idx, f"`#{idx:02d}`")
            name = player.get('username', 'Unknown')[:20]  # Truncate long names
            
            # Determine value based on metric
            if metric == 'earned_credits':
                value = player.get('earned_credits', 0)
            else:  # contributions
                value = player.get('amount', 0)
            
            value_str = f"{value:,}"
            
            # Calculate position change
            prev_pos = prev_lookup.get(name)
            
            if prev_pos is None:
                change = "🆕"
            else:
                diff = prev_pos - idx
                if diff > 0:
                    change = f"▲ +{diff}"
                elif diff < 0:
                    change = f"▼ {diff}"
                else:
                    change = "━"
            
            # Format line
            line = f"{medal} **{name}** - {value_str} credits `{change}`"
            lines.append(line)
        
        # Add all lines to embed
        if lines:
            embed.description = "\n".join(lines)
        else:
            embed.description = "No data available for this period."
        
        # Add footer
        period_text = "last 24 hours" if period == 'daily' else "this month"
        embed.set_footer(text=f"Rankings based on {period_text} • Using new scraper_databases")
        
        return embed

    # ==================== COMMANDS ====================

    @commands.group(name="topplayers")
    @checks.admin_or_permissions(manage_guild=True)
    async def topplayers(self, ctx):
        """Top players leaderboard configuration commands."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @topplayers.command(name="dailyearnedchannel")
    async def set_daily_earned_channel(self, ctx, channel: discord.TextChannel):
        """Set channel for daily earned credits leaderboard."""
        await self.config.daily_earned_channel.set(channel.id)
        await ctx.send(f"✅ Daily earned credits leaderboard will be posted in {channel.mention}")

    @topplayers.command(name="dailycontribchannel")
    async def set_daily_contrib_channel(self, ctx, channel: discord.TextChannel):
        """Set channel for daily treasury contributions leaderboard."""
        await self.config.daily_contrib_channel.set(channel.id)
        await ctx.send(f"✅ Daily treasury contributions leaderboard will be posted in {channel.mention}")

    @topplayers.command(name="monthlyearnedchannel")
    async def set_monthly_earned_channel(self, ctx, channel: discord.TextChannel):
        """Set channel for monthly earned credits leaderboard."""
        await self.config.monthly_earned_channel.set(channel.id)
        await ctx.send(f"✅ Monthly earned credits leaderboard will be posted in {channel.mention}")

    @topplayers.command(name="monthlycontribchannel")
    async def set_monthly_contrib_channel(self, ctx, channel: discord.TextChannel):
        """Set channel for monthly treasury contributions leaderboard."""
        await self.config.monthly_contrib_channel.set(channel.id)
        await ctx.send(f"✅ Monthly treasury contributions leaderboard will be posted in {channel.mention}")

    @topplayers.command(name="settings")
    async def show_settings(self, ctx):
        """Show current top players leaderboard settings."""
        daily_earned = await self.config.daily_earned_channel()
        daily_contrib = await self.config.daily_contrib_channel()
        monthly_earned = await self.config.monthly_earned_channel()
        monthly_contrib = await self.config.monthly_contrib_channel()
        
        embed = discord.Embed(
            title="Top Players Leaderboard Settings",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="Daily Earned Credits",
            value=f"<#{daily_earned}>" if daily_earned else "Not set",
            inline=False
        )
        embed.add_field(
            name="Daily Contributions",
            value=f"<#{daily_contrib}>" if daily_contrib else "Not set",
            inline=False
        )
        embed.add_field(
            name="Monthly Earned Credits",
            value=f"<#{monthly_earned}>" if monthly_earned else "Not set",
            inline=False
        )
        embed.add_field(
            name="Monthly Contributions",
            value=f"<#{monthly_contrib}>" if monthly_contrib else "Not set",
            inline=False
        )
        
        # Database status
        members_exists = "✅" if self.members_db_path.exists() else "❌"
        income_exists = "✅" if self.income_db_path.exists() else "❌"
        
        embed.add_field(
            name="Database Status",
            value=f"{members_exists} members_v2.db\n{income_exists} income_v2.db",
            inline=False
        )
        
        embed.set_footer(text="Posts daily at 06:00 Amsterdam time | Monthly on last day of month at 06:00")
        
        await ctx.send(embed=embed)

    @topplayers.command(name="debug")
    @checks.is_owner()
    async def debug_databases(self, ctx):
        """Debug: Check database contents and structure."""
        embed = discord.Embed(title="🔍 Database Debug Info", color=discord.Color.blue())
        
        # Check members_v2.db
        if self.members_db_path.exists():
            try:
                async with aiosqlite.connect(self.members_db_path) as db:
                    # Count total records
                    cur = await db.execute("SELECT COUNT(*) FROM members")
                    total = (await cur.fetchone())[0]
                    
                    # Get latest timestamp
                    cur = await db.execute("SELECT MAX(timestamp) FROM members")
                    latest = (await cur.fetchone())[0]
                    
                    # Get unique timestamps
                    cur = await db.execute("SELECT COUNT(DISTINCT timestamp) FROM members")
                    timestamps = (await cur.fetchone())[0]
                    
                    # Count members with credits > 0
                    cur = await db.execute("SELECT COUNT(*) FROM members WHERE earned_credits > 0")
                    with_credits = (await cur.fetchone())[0]
                    
                    # Sample data WITH CREDITS
                    cur = await db.execute("""
                        SELECT member_id, username, earned_credits, timestamp 
                        FROM members 
                        WHERE earned_credits > 0
                        ORDER BY timestamp DESC, earned_credits DESC 
                        LIMIT 5
                    """)
                    sample = await cur.fetchall()
                    
                    if sample:
                        sample_text = "\n".join([f"• {row[1]}: {row[2]:,} credits" for row in sample])
                    else:
                        sample_text = "⚠️ NO MEMBERS WITH CREDITS > 0!"
                    
                    embed.add_field(
                        name="✅ members_v2.db",
                        value=f"**Total records:** {total:,}\n"
                              f"**With credits > 0:** {with_credits:,}\n"
                              f"**Unique timestamps:** {timestamps}\n"
                              f"**Latest scrape:** {latest[:19] if latest else 'N/A'}\n"
                              f"**Top earners (latest):**\n{sample_text}",
                        inline=False
                    )
            except Exception as e:
                embed.add_field(name="❌ members_v2.db", value=f"Error: {str(e)}", inline=False)
        else:
            embed.add_field(name="❌ members_v2.db", value="File not found", inline=False)
        
        # Check income_v2.db
        if self.income_db_path.exists():
            try:
                async with aiosqlite.connect(self.income_db_path) as db:
                    # Count ALL records (not just income type)
                    cur = await db.execute("SELECT COUNT(*) FROM income")
                    total_all = (await cur.fetchone())[0]
                    
                    # Count by entry_type
                    cur = await db.execute("SELECT entry_type, COUNT(*) FROM income GROUP BY entry_type")
                    by_type = await cur.fetchall()
                    type_text = "\n".join([f"  • {t[0]}: {t[1]:,}" for t in by_type]) if by_type else "  ⚠️ No data"
                    
                    # Count income records specifically
                    cur = await db.execute("SELECT COUNT(*) FROM income WHERE entry_type='income'")
                    total_income = (await cur.fetchone())[0]
                    
                    # Get latest timestamp (any type)
                    cur = await db.execute("SELECT MAX(timestamp) FROM income")
                    latest = (await cur.fetchone())[0]
                    
                    # Count by period
                    cur = await db.execute("SELECT period, COUNT(*) FROM income WHERE entry_type='income' GROUP BY period")
                    periods = await cur.fetchall()
                    
                    if periods:
                        period_text = "\n".join([f"  • {p[0]}: {p[1]:,}" for p in periods])
                    else:
                        period_text = "  ⚠️ No income data - check if scraped as 'expense'?"
                    
                    # Sample data (show ANY type if income is empty)
                    if total_income > 0:
                        cur = await db.execute("SELECT username, amount, period, entry_type, timestamp FROM income WHERE entry_type='income' ORDER BY timestamp DESC LIMIT 3")
                        sample = await cur.fetchall()
                        sample_text = "\n".join([f"• {row[0]}: {row[1]:,} ({row[2]}, {row[3]})" for row in sample])
                    else:
                        # Show ANY data to debug
                        cur = await db.execute("SELECT username, amount, period, entry_type, timestamp FROM income ORDER BY timestamp DESC LIMIT 5")
                        sample = await cur.fetchall()
                        if sample:
                            sample_text = "⚠️ Showing ALL types (no 'income' found):\n" + "\n".join([f"• {row[0]}: {row[1]:,} ({row[2]}, type={row[3]})" for row in sample])
                        else:
                            sample_text = "⚠️ No data at all"
                    
                    status = "✅" if total_income > 0 else "⚠️"
                    embed.add_field(
                        name=f"{status} income_v2.db",
                        value=f"**Total records:** {total_all:,}\n"
                              f"**By type:**\n{type_text}\n"
                              f"**Income records:** {total_income:,}\n"
                              f"**Latest scrape:** {latest[:19] if latest else 'N/A'}\n"
                              f"**Income by period:**\n{period_text}\n"
                              f"**Sample:**\n{sample_text}",
                        inline=False
                    )
            except Exception as e:
                embed.add_field(name="❌ income_v2.db", value=f"Error: {str(e)}", inline=False)
        else:
            embed.add_field(name="❌ income_v2.db", value="File not found", inline=False)
        
        await ctx.send(embed=embed)

    @topplayers.command(name="testnow")
    @checks.is_owner()
    async def test_now(self, ctx, leaderboard_type: str):
        """
        Manually trigger a leaderboard post for testing.
        Types: daily_earned, daily_contrib, monthly_earned, monthly_contrib
        """
        async with ctx.typing():
            try:
                if leaderboard_type == "daily_earned":
                    data = await self._get_earned_credits_rankings(period='daily')
                    if data:
                        embed = self._create_leaderboard_embed(
                            title="🏆 Daily Top 10 - Earned Credits (TEST)",
                            current_rankings=data['current'],
                            previous_rankings=data['previous'],
                            period='daily',
                            metric='earned_credits'
                        )
                        await ctx.send(embed=embed)
                    else:
                        await ctx.send("❌ No data available")
                
                elif leaderboard_type == "daily_contrib":
                    data = await self._get_treasury_rankings(period='daily')
                    if data:
                        embed = self._create_leaderboard_embed(
                            title="💰 Daily Top 10 - Treasury Contributions (TEST)",
                            current_rankings=data['current'],
                            previous_rankings=data['previous'],
                            period='daily',
                            metric='contributions'
                        )
                        await ctx.send(embed=embed)
                    else:
                        await ctx.send("❌ No data available")
                
                elif leaderboard_type == "monthly_earned":
                    data = await self._get_earned_credits_rankings(period='monthly')
                    if data:
                        embed = self._create_leaderboard_embed(
                            title="🏆 Monthly Top 10 - Earned Credits (TEST)",
                            current_rankings=data['current'],
                            previous_rankings=data['previous'],
                            period='monthly',
                            metric='earned_credits'
                        )
                        await ctx.send(embed=embed)
                    else:
                        await ctx.send("❌ No data available")
                
                elif leaderboard_type == "monthly_contrib":
                    data = await self._get_treasury_rankings(period='monthly')
                    if data:
                        embed = self._create_leaderboard_embed(
                            title="💰 Monthly Top 10 - Treasury Contributions (TEST)",
                            current_rankings=data['current'],
                            previous_rankings=data['previous'],
                            period='monthly',
                            metric='contributions'
                        )
                        await ctx.send(embed=embed)
                    else:
                        await ctx.send("❌ No data available")
                
                else:
                    await ctx.send("❌ Invalid type. Use: daily_earned, daily_contrib, monthly_earned, monthly_contrib")
                    
            except Exception as e:
                log.exception(f"Error in test command: {e}")
                await ctx.send(f"❌ Error: {str(e)}")


async def setup(bot: Red):
    """Add cog to bot."""
    await bot.add_cog(Leaderboard(bot))
