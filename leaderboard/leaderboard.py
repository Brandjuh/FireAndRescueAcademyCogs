"""
Alliance Leaderboard System for Missionchief USA
Displays daily and monthly top 10 rankings for earned credits and treasury contributions.
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


class Leaderboard(commands.Cog):
    """Alliance leaderboard system - daily and monthly top 10 rankings."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0x4C45414442, force_registration=True)
        self.config.register_global(**DEFAULTS)
        
        # Get AllianceScraper database path
        scraper_cog = self.bot.get_cog("AllianceScraper")
        if scraper_cog and hasattr(scraper_cog, 'db_path'):
            self.db_path = scraper_cog.db_path
        else:
            # Fallback: try to guess path
            data_path = cog_data_path(raw_name="AllianceScraper")
            self.db_path = data_path / "alliance.db"
        
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

    # ==================== DATA RETRIEVAL ====================

    async def _get_earned_credits_rankings(self, period: str) -> Optional[Dict]:
        """
        Get earned credits rankings for current and previous period.
        Returns dict with 'current' and 'previous' rankings.
        """
        if not self.db_path.exists():
            log.error(f"Database not found at {self.db_path}")
            return None

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Get most recent scrape
            cur = await db.execute("""
                SELECT MAX(scraped_at) as latest FROM members_history
            """)
            row = await cur.fetchone()
            if not row or not row['latest']:
                return None
            
            current_time = row['latest']
            
            # Calculate previous time based on period
            if period == 'daily':
                # Parse timestamp and subtract 24 hours
                dt = datetime.fromisoformat(current_time.replace('Z', '+00:00'))
                previous_dt = dt - timedelta(days=1)
            else:  # monthly
                dt = datetime.fromisoformat(current_time.replace('Z', '+00:00'))
                # Go back approximately 30 days
                previous_dt = dt - timedelta(days=30)
            
            previous_time = previous_dt.isoformat()
            
            # Get current period rankings
            cur = await db.execute("""
                SELECT user_id, name, earned_credits, scraped_at
                FROM members_history
                WHERE scraped_at = ?
                ORDER BY earned_credits DESC
                LIMIT 10
            """, (current_time,))
            current = [dict(row) for row in await cur.fetchall()]
            
            # Get previous period rankings - find closest timestamp
            cur = await db.execute("""
                SELECT DISTINCT scraped_at
                FROM members_history
                WHERE scraped_at <= ?
                ORDER BY scraped_at DESC
                LIMIT 1
            """, (previous_time,))
            prev_time_row = await cur.fetchone()
            
            previous = []
            if prev_time_row:
                cur = await db.execute("""
                    SELECT user_id, name, earned_credits, scraped_at
                    FROM members_history
                    WHERE scraped_at = ?
                    ORDER BY earned_credits DESC
                    LIMIT 20
                """, (prev_time_row['scraped_at'],))
                previous = [dict(row) for row in await cur.fetchall()]
            
            return {
                'current': current,
                'previous': previous
            }

    async def _get_treasury_rankings(self, period: str) -> Optional[Dict]:
        """
        Get treasury contribution rankings for current and previous period.
        Returns dict with 'current' and 'previous' rankings.
        """
        if not self.db_path.exists():
            log.error(f"Database not found at {self.db_path}")
            return None

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            period_type = 'daily' if period == 'daily' else 'monthly'
            
            # Get most recent scrape for this period
            cur = await db.execute("""
                SELECT MAX(scraped_at) as latest 
                FROM treasury_income 
                WHERE period = ?
            """, (period_type,))
            row = await cur.fetchone()
            if not row or not row['latest']:
                return None
            
            current_time = row['latest']
            
            # Get current rankings
            cur = await db.execute("""
                SELECT user_id, user_name as name, credits, scraped_at
                FROM treasury_income
                WHERE period = ? AND scraped_at = ?
                ORDER BY credits DESC
                LIMIT 10
            """, (period_type, current_time))
            current = [dict(row) for row in await cur.fetchall()]
            
            # Get previous period rankings
            # For treasury, we need to go back to previous scrape
            if period == 'daily':
                # Get scrapes from yesterday
                dt = datetime.fromisoformat(current_time.replace('Z', '+00:00'))
                previous_dt = dt - timedelta(days=1)
                previous_time = previous_dt.isoformat()
            else:
                # Get scrapes from previous month
                dt = datetime.fromisoformat(current_time.replace('Z', '+00:00'))
                previous_dt = dt - timedelta(days=30)
                previous_time = previous_dt.isoformat()
            
            cur = await db.execute("""
                SELECT DISTINCT scraped_at
                FROM treasury_income
                WHERE period = ? AND scraped_at <= ?
                ORDER BY scraped_at DESC
                LIMIT 1
            """, (period_type, previous_time))
            prev_time_row = await cur.fetchone()
            
            previous = []
            if prev_time_row:
                cur = await db.execute("""
                    SELECT user_id, user_name as name, credits, scraped_at
                    FROM treasury_income
                    WHERE period = ? AND scraped_at = ?
                    ORDER BY credits DESC
                    LIMIT 20
                """, (period_type, prev_time_row['scraped_at']))
                previous = [dict(row) for row in await cur.fetchall()]
            
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
        
        # Create previous rankings lookup by user_id
        prev_lookup = {}
        for idx, player in enumerate(previous_rankings, 1):
            prev_lookup[player['user_id']] = idx
        
        # Build leaderboard text
        lines = []
        for idx, player in enumerate(current_rankings, 1):
            medal = MEDALS.get(idx, f"`#{idx:02d}`")
            name = player['name'][:20]  # Truncate long names
            
            # Determine value based on metric
            if metric == 'earned_credits':
                value = player.get('earned_credits', 0)
            else:  # contributions
                value = player.get('credits', 0)
            
            value_str = f"{value:,}"
            
            # Calculate position change
            user_id = player['user_id']
            prev_pos = prev_lookup.get(user_id)
            
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
        embed.set_footer(text=f"Rankings based on {period_text}")
        
        return embed

    # ==================== COMMANDS ====================

    @commands.group(name="topplayers")
    @checks.admin_or_permissions(manage_guild=True)
    async def topplayers_group(self, ctx):
        """Top players leaderboard configuration commands."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @topplayers_group.command(name="dailyearnedchannel")
    async def set_daily_earned_channel(self, ctx, channel: discord.TextChannel):
        """Set channel for daily earned credits leaderboard."""
        await self.config.daily_earned_channel.set(channel.id)
        await ctx.send(f"✅ Daily earned credits leaderboard will be posted in {channel.mention}")

    @topplayers_group.command(name="dailycontribchannel")
    async def set_daily_contrib_channel(self, ctx, channel: discord.TextChannel):
        """Set channel for daily treasury contributions leaderboard."""
        await self.config.daily_contrib_channel.set(channel.id)
        await ctx.send(f"✅ Daily treasury contributions leaderboard will be posted in {channel.mention}")

    @topplayers_group.command(name="monthlyearnedchannel")
    async def set_monthly_earned_channel(self, ctx, channel: discord.TextChannel):
        """Set channel for monthly earned credits leaderboard."""
        await self.config.monthly_earned_channel.set(channel.id)
        await ctx.send(f"✅ Monthly earned credits leaderboard will be posted in {channel.mention}")

    @topplayers_group.command(name="monthlycontribchannel")
    async def set_monthly_contrib_channel(self, ctx, channel: discord.TextChannel):
        """Set channel for monthly treasury contributions leaderboard."""
        await self.config.monthly_contrib_channel.set(channel.id)
        await ctx.send(f"✅ Monthly treasury contributions leaderboard will be posted in {channel.mention}")

    @topplayers_group.command(name="settings")
    async def show_settings(self, ctx):
        """Show current top players leaderboard settings."""
        daily_earned = await self.config.daily_earned_channel()
        daily_contrib = await self.config.daily_contrib_channel()
        monthly_earned = await self.config.monthly_earned_channel()
        monthly_contrib = await self.config.monthly_contrib_channel()
        
        embed = discord.Embed(
            title="Leaderboard Settings",
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
        
        embed.set_footer(text="Posts daily at 06:00 Amsterdam time | Monthly on last day of month at 06:00")
        
        await ctx.send(embed=embed)

    @leaderboard_group.command(name="testnow")
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
