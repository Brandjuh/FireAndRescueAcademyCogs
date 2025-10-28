"""
Alliance Leaderboard System for Missionchief USA
Displays daily and monthly top 10 rankings for earned credits and treasury contributions.
Uses NEW scraper_databases structure (members_v2.db, income_v2.db)
Posts at 06:00 Amsterdam time (00:00 New York - after daily reset)
Daily: Shows full previous day (00:01-23:59 NY time)
Monthly: Shows full previous month (1st 00:01 - last day 23:59 NY time)
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import aiosqlite
import discord
from redbot.core import commands, checks, Config
from redbot.core.data_manager import cog_data_path
import pytz

logger = logging.getLogger("red.leaderboard")

MEDALS = {
    1: "🥇",
    2: "🥈", 
    3: "🥉"
}

# Blacklist for corrupted/invalid entries (EMPTY - scraper is fixed!)
BLACKLISTED_USER_IDS = []

BLACKLISTED_USERNAMES = []

class Leaderboard(commands.Cog):
    """Daily and monthly top 10 rankings for alliance contributions and earned credits."""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890123, force_registration=True)
        
        default_guild = {
            "daily_earned_channel": 544461383358480385,
            "daily_contrib_channel": 544461383358480385,
            "monthly_earned_channel": 544461383358480385,
            "monthly_contrib_channel": 544461383358480385,
        }
        self.config.register_guild(**default_guild)
        
        # Database paths for new scraper_databases structure
        base_path = cog_data_path(raw_name="scraper_databases")
        self.members_db_path = base_path / "members_v2.db"
        self.income_db_path = base_path / "income_v2.db"
        
        # Timezones
        self.tz_amsterdam = pytz.timezone('Europe/Amsterdam')
        self.tz_ny = pytz.timezone('America/New_York')
        
        # Start scheduled tasks
        self.daily_task = self.bot.loop.create_task(self._daily_leaderboard_loop())
        self.monthly_task = self.bot.loop.create_task(self._monthly_leaderboard_loop())
        
        logger.info("Leaderboard cog initialized with new database structure")
    
    def cog_unload(self):
        """Cancel tasks on unload."""
        if self.daily_task:
            self.daily_task.cancel()
        if self.monthly_task:
            self.monthly_task.cancel()
    
    def _filter_invalid_entries(self, entries: List[Dict]) -> List[Dict]:
        """
        Filter out invalid/corrupted entries.
        Returns list of valid entries.
        """
        valid_entries = []
        
        for entry in entries:
            username = entry.get("username", "")
            user_id = entry.get("user_id", "")
            credits = entry.get("credits", 0)
            
            # Check blacklists
            if user_id in BLACKLISTED_USER_IDS:
                logger.info(f"Filtered blacklisted user_id: {user_id}")
                continue
                
            if username in BLACKLISTED_USERNAMES:
                logger.info(f"Filtered blacklisted username: {username}")
                continue
            
            # Filter INT64_MAX (parsing error fallback)
            if credits == 9223372036854775807:
                logger.info(f"Filtered INT64_MAX value for {username}")
                continue
            
            # Filter usernames that are ONLY digits and longer than 10 chars
            # (likely scraped IDs instead of usernames)
            if username.replace(",", "").replace(".", "").isdigit() and len(username.replace(",", "").replace(".", "")) > 10:
                logger.info(f"Filtered numeric username (likely ID): {username}")
                continue
            
            # NEW: Filter if username contains large numbers that match the credit amount
            # This catches cases where scraper parsed username as credits
            username_digits = ''.join(c for c in username if c.isdigit())
            if username_digits and len(username_digits) >= 5:
                # Check if the digits in username roughly match the credit amount
                try:
                    username_number = int(username_digits)
                    # If username number is within 10% of credits, likely parsing error
                    if credits > 0 and abs(username_number - credits) / credits < 0.1:
                        logger.info(f"Filtered username/credit mismatch: {username} has {credits:,} credits (username contains {username_number:,})")
                        continue
                except ValueError:
                    pass
            
            valid_entries.append(entry)
        
        return valid_entries
    
    def _get_period_boundaries(self, period: str, current_time: datetime) -> Tuple[datetime, datetime, datetime, datetime]:
        """
        Get period boundaries in NY timezone for current and previous periods.
        Returns: (current_start, current_end, previous_start, previous_end) in UTC
        
        For daily: full previous day (00:00:00 - 23:59:59 NY time)
        For monthly: full previous month (1st 00:00:00 - last day 23:59:59 NY time)
        """
        # Convert current time to NY timezone
        ny_time = current_time.astimezone(self.tz_ny)
        
        if period == "daily":
            # Yesterday in NY timezone
            yesterday = ny_time.date() - timedelta(days=1)
            current_start = self.tz_ny.localize(datetime.combine(yesterday, datetime.min.time()))
            current_end = self.tz_ny.localize(datetime.combine(yesterday, datetime.max.time()))
            
            # Day before yesterday
            day_before = yesterday - timedelta(days=1)
            previous_start = self.tz_ny.localize(datetime.combine(day_before, datetime.min.time()))
            previous_end = self.tz_ny.localize(datetime.combine(day_before, datetime.max.time()))
            
        else:  # monthly
            # Previous month in NY timezone
            # Get first day of current month, then subtract 1 day to get last day of previous month
            first_of_current = ny_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            last_of_previous = first_of_current - timedelta(days=1)
            first_of_previous = last_of_previous.replace(day=1)
            
            current_start = self.tz_ny.localize(datetime.combine(first_of_previous.date(), datetime.min.time()))
            current_end = self.tz_ny.localize(datetime.combine(last_of_previous.date(), datetime.max.time()))
            
            # Month before that
            first_of_prev_month = first_of_previous - timedelta(days=1)
            first_of_prev_month = first_of_prev_month.replace(day=1)
            last_of_prev_month = first_of_previous - timedelta(days=1)
            
            previous_start = self.tz_ny.localize(datetime.combine(first_of_prev_month.date(), datetime.min.time()))
            previous_end = self.tz_ny.localize(datetime.combine(last_of_prev_month.date(), datetime.max.time()))
        
        # Convert all to UTC for database queries
        return (
            current_start.astimezone(pytz.UTC),
            current_end.astimezone(pytz.UTC),
            previous_start.astimezone(pytz.UTC),
            previous_end.astimezone(pytz.UTC)
        )
    
    async def _get_scrapes_in_period(self, db, start_time: datetime, end_time: datetime) -> List[str]:
        """
        Get all scrape timestamps within the specified period.
        Returns list of timestamps sorted chronologically.
        """
        start_iso = start_time.isoformat()
        end_iso = end_time.isoformat()
        
        query = """
            SELECT DISTINCT timestamp 
            FROM members 
            WHERE timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC
        """
        
        async with db.execute(query, (start_iso, end_iso)) as cursor:
            results = await cursor.fetchall()
            return [row[0] for row in results]
    
    async def _get_earned_credits_rankings(self, period: str) -> Optional[Dict]:
        """
        Get earned credits rankings from members_v2.db.
        Finds ALL scrapes within the period and compares FIRST vs LAST scrape.
        Returns dict with 'current' and 'previous' lists of {username, credits, rank}
        """
        if not self.members_db_path.exists():
            logger.error("members_v2.db not found")
            return None
        
        try:
            async with aiosqlite.connect(self.members_db_path) as db:
                # Get period boundaries
                now = datetime.now(self.tz_amsterdam)
                current_start, current_end, previous_start, previous_end = self._get_period_boundaries(period, now)
                
                logger.info(f"Earned credits {period} - Current period: {current_start} to {current_end}")
                logger.info(f"Earned credits {period} - Previous period: {previous_start} to {previous_end}")
                
                # Find ALL scrapes within current period
                current_scrapes = await self._get_scrapes_in_period(db, current_start, current_end)
                
                if len(current_scrapes) < 2:
                    logger.warning(f"Not enough scrapes in current {period} period (found {len(current_scrapes)})")
                    return None
                
                # Use FIRST and LAST scrape of the period
                start_scrape = current_scrapes[0]
                end_scrape = current_scrapes[-1]
                
                logger.info(f"Using {len(current_scrapes)} scrapes: first={start_scrape}, last={end_scrape}")
                
                # Get credits at start of period
                query = """
                    SELECT member_id, username, earned_credits
                    FROM members
                    WHERE timestamp = ?
                """
                
                async with db.execute(query, (start_scrape,)) as cursor:
                    start_data = await cursor.fetchall()
                
                # Get credits at end of period
                async with db.execute(query, (end_scrape,)) as cursor:
                    end_data = await cursor.fetchall()
                
                if not start_data or not end_data:
                    logger.warning("No data found in start or end scrapes")
                    return None
                
                # Build start credits map
                start_map = {member_id: (username, credits) for member_id, username, credits in start_data}
                
                # Calculate deltas
                delta_list = []
                for member_id, username, end_credits in end_data:
                    if member_id in start_map:
                        start_credits = start_map[member_id][1]
                        delta = end_credits - start_credits
                    else:
                        # New member during period
                        delta = end_credits
                    
                    if delta > 0:
                        delta_list.append({
                            "username": username,
                            "credits": delta,
                            "user_id": str(member_id)
                        })
                
                if not delta_list:
                    logger.warning("No members with positive credit growth")
                    return None
                
                logger.info(f"Found {len(delta_list)} members with positive growth")
                
                # Filter invalid entries
                delta_filtered = self._filter_invalid_entries(delta_list)
                
                logger.info(f"After filtering: {len(delta_filtered)} members")
                
                # Sort by delta and get top 10
                delta_filtered.sort(key=lambda x: x["credits"], reverse=True)
                current_rankings = delta_filtered[:10]
                
                # Add ranks
                for i, entry in enumerate(current_rankings, 1):
                    entry["rank"] = i
                
                # Do the same for previous period
                previous_scrapes = await self._get_scrapes_in_period(db, previous_start, previous_end)
                
                previous_rankings = []
                if len(previous_scrapes) >= 2:
                    prev_start_scrape = previous_scrapes[0]
                    prev_end_scrape = previous_scrapes[-1]
                    
                    logger.info(f"Previous period: {len(previous_scrapes)} scrapes, first={prev_start_scrape}, last={prev_end_scrape}")
                    
                    async with db.execute(query, (prev_start_scrape,)) as cursor:
                        prev_start_data = await cursor.fetchall()
                    
                    async with db.execute(query, (prev_end_scrape,)) as cursor:
                        prev_end_data = await cursor.fetchall()
                    
                    if prev_start_data and prev_end_data:
                        prev_start_map = {member_id: (username, credits) for member_id, username, credits in prev_start_data}
                        
                        prev_delta_list = []
                        for member_id, username, end_credits in prev_end_data:
                            if member_id in prev_start_map:
                                start_credits = prev_start_map[member_id][1]
                                delta = end_credits - start_credits
                            else:
                                delta = end_credits
                            
                            if delta > 0:
                                prev_delta_list.append({
                                    "username": username,
                                    "credits": delta,
                                    "user_id": str(member_id)
                                })
                        
                        prev_delta_filtered = self._filter_invalid_entries(prev_delta_list)
                        prev_delta_filtered.sort(key=lambda x: x["credits"], reverse=True)
                        previous_rankings = prev_delta_filtered[:20]
                        
                        for i, entry in enumerate(previous_rankings, 1):
                            entry["rank"] = i
                
                return {
                    "current": current_rankings,
                    "previous": previous_rankings
                }
                
        except Exception as e:
            logger.error(f"Error getting earned credits rankings: {e}", exc_info=True)
            return None
    
    async def _get_treasury_rankings(self, period: str) -> Optional[Dict]:
        """
        Get treasury contribution rankings from income_v2.db.
        Uses the most recent scrape from the specified period.
        Returns dict with 'current' and 'previous' lists of {username, credits, rank}
        """
        if not self.income_db_path.exists():
            logger.error("income_v2.db not found")
            return None
        
        try:
            async with aiosqlite.connect(self.income_db_path) as db:
                # Get the two most recent timestamps for this period
                query = """
                    SELECT DISTINCT timestamp
                    FROM income
                    WHERE entry_type = 'income' 
                    AND period = ?
                    ORDER BY timestamp DESC
                    LIMIT 2
                """
                
                async with db.execute(query, (period,)) as cursor:
                    timestamps = await cursor.fetchall()
                
                if not timestamps:
                    logger.warning(f"No timestamp found for treasury {period}")
                    return None
                
                current_ts = timestamps[0][0]
                previous_ts = timestamps[1][0] if len(timestamps) > 1 else None
                
                logger.info(f"Treasury {period} - Current: {current_ts}, Previous: {previous_ts}")
                
                # Get current period rankings (fetch more to account for filtering)
                query = """
                    SELECT username, amount as credits
                    FROM income
                    WHERE entry_type = 'income'
                    AND period = ?
                    AND timestamp = ?
                    ORDER BY amount DESC
                    LIMIT 30
                """
                
                async with db.execute(query, (period, current_ts)) as cursor:
                    current_raw = await cursor.fetchall()
                
                # Convert to dict format and filter
                current_data = [
                    {"username": username, "credits": credits}
                    for username, credits in current_raw
                ]
                current_filtered = self._filter_invalid_entries(current_data)
                current_rankings = current_filtered[:10]  # Take top 10 after filtering
                
                # Add ranks
                for i, entry in enumerate(current_rankings, 1):
                    entry["rank"] = i
                
                # Get previous period rankings if available
                previous_rankings = []
                if previous_ts:
                    async with db.execute(query, (period, previous_ts)) as cursor:
                        previous_raw = await cursor.fetchall()
                    
                    previous_data = [
                        {"username": username, "credits": credits}
                        for username, credits in previous_raw
                    ]
                    previous_filtered = self._filter_invalid_entries(previous_data)
                    previous_rankings = previous_filtered[:20]  # Keep more for comparison
                    
                    for i, entry in enumerate(previous_rankings, 1):
                        entry["rank"] = i
                
                return {
                    "current": current_rankings,
                    "previous": previous_rankings
                }
                
        except Exception as e:
            logger.error(f"Error getting treasury rankings: {e}", exc_info=True)
            return None
    
    def _calculate_rank_changes(self, current: List[Dict], previous: List[Dict]) -> List[Tuple[Dict, str]]:
        """
        Calculate rank changes between current and previous periods.
        Returns list of (entry, change_indicator) tuples.
        """
        # Build previous rank map
        prev_map = {entry["username"]: entry["rank"] for entry in previous}
        
        results = []
        for entry in current:
            username = entry["username"]
            current_rank = entry["rank"]
            
            if username not in prev_map:
                # New in top 10
                indicator = "🆕"
            else:
                prev_rank = prev_map[username]
                rank_change = prev_rank - current_rank
                
                if rank_change > 0:
                    # Moved up
                    indicator = f"▲ +{rank_change}"
                elif rank_change < 0:
                    # Moved down
                    indicator = f"▼ {rank_change}"
                else:
                    # Same position
                    indicator = "━"
            
            results.append((entry, indicator))
        
        return results
    
    def _create_leaderboard_embed(self, title: str, rankings_data: Optional[Dict], 
                                  leaderboard_type: str, is_test: bool = False) -> discord.Embed:
        """
        Create a leaderboard embed from rankings data.
        leaderboard_type: 'earned' or 'contrib'
        """
        if is_test:
            title += " (TEST)"
        
        embed = discord.Embed(
            title=f"🏆 {title}",
            color=discord.Color.gold(),
            timestamp=datetime.now(self.tz_amsterdam)
        )
        
        if not rankings_data or not rankings_data.get("current"):
            embed.description = "No data available for this period."
            embed.set_footer(text="Rankings based on full day/month in NY timezone • Using new scraper_databases")
            return embed
        
        current = rankings_data["current"]
        previous = rankings_data["previous"]
        
        # Calculate rank changes
        rankings_with_changes = self._calculate_rank_changes(current, previous)
        
        # Build leaderboard text
        lines = []
        for entry, change_indicator in rankings_with_changes:
            rank = entry["rank"]
            username = entry["username"]
            credits = entry["credits"]
            
            # Medal for top 3
            medal = MEDALS.get(rank, "")
            
            # Format: 🥇 Username - 1,234,567 credits ▲ +2
            rank_str = f"#{rank:02d}" if rank > 3 else medal
            line = f"{rank_str} **{username}** - {credits:,} credits `{change_indicator}`"
            lines.append(line)
        
        embed.description = "\n".join(lines)
        
        period_text = "full previous day (NY time)" if "Daily" in title else "full previous month (NY time)"
        embed.set_footer(text=f"Rankings based on {period_text} • Using new scraper_databases")
        
        return embed
    
    async def _post_leaderboard(self, guild: discord.Guild, channel_id: int, 
                               embed: discord.Embed) -> bool:
        """Post leaderboard embed to specified channel."""
        if not channel_id:
            return False
        
        channel = guild.get_channel(channel_id)
        if not channel:
            logger.warning(f"Channel {channel_id} not found in guild {guild.id}")
            return False
        
        try:
            await channel.send(embed=embed)
            return True
        except discord.Forbidden:
            logger.error(f"No permission to post in channel {channel_id}")
            return False
        except Exception as e:
            logger.error(f"Error posting leaderboard: {e}", exc_info=True)
            return False
    
    async def _daily_leaderboard_loop(self):
        """Daily leaderboard posting at 06:00 Amsterdam time (00:00 NY time)."""
        await self.bot.wait_until_ready()
        
        while not self.bot.is_closed():
            try:
                now = datetime.now(self.tz_amsterdam)
                
                # Calculate next 06:00
                target = now.replace(hour=6, minute=0, second=0, microsecond=0)
                if now >= target:
                    target += timedelta(days=1)
                
                wait_seconds = (target - now).total_seconds()
                logger.info(f"Next daily leaderboard in {wait_seconds/3600:.1f} hours at {target.strftime('%Y-%m-%d %H:%M %Z')}")
                
                await asyncio.sleep(wait_seconds)
                
                # Post to all guilds
                for guild in self.bot.guilds:
                    guild_config = await self.config.guild(guild).all()
                    
                    # Daily Earned Credits
                    if guild_config["daily_earned_channel"]:
                        data = await self._get_earned_credits_rankings("daily")
                        embed = self._create_leaderboard_embed(
                            "Daily Top 10 - Earned Credits",
                            data,
                            "earned"
                        )
                        await self._post_leaderboard(guild, guild_config["daily_earned_channel"], embed)
                    
                    # Daily Contributions
                    if guild_config["daily_contrib_channel"]:
                        data = await self._get_treasury_rankings("daily")
                        embed = self._create_leaderboard_embed(
                            "Daily Top 10 - Treasury Contributions",
                            data,
                            "contrib"
                        )
                        await self._post_leaderboard(guild, guild_config["daily_contrib_channel"], embed)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in daily leaderboard loop: {e}", exc_info=True)
                await asyncio.sleep(3600)  # Wait 1 hour on error
    
    async def _monthly_leaderboard_loop(self):
        """Monthly leaderboard posting on 1st day of month at 06:00 Amsterdam time."""
        await self.bot.wait_until_ready()
        
        while not self.bot.is_closed():
            try:
                now = datetime.now(self.tz_amsterdam)
                
                # Calculate next 1st of month at 06:00
                if now.day == 1 and now.hour < 6:
                    # We're on the 1st but before 06:00
                    target = now.replace(hour=6, minute=0, second=0, microsecond=0)
                else:
                    # Move to next month
                    if now.month == 12:
                        target = now.replace(year=now.year + 1, month=1, day=1, hour=6, minute=0, second=0, microsecond=0)
                    else:
                        target = now.replace(month=now.month + 1, day=1, hour=6, minute=0, second=0, microsecond=0)
                
                wait_seconds = (target - now).total_seconds()
                logger.info(f"Next monthly leaderboard on {target.strftime('%Y-%m-%d %H:%M %Z')} ({wait_seconds/86400:.1f} days)")
                
                await asyncio.sleep(wait_seconds)
                
                # Post to all guilds
                for guild in self.bot.guilds:
                    guild_config = await self.config.guild(guild).all()
                    
                    # Monthly Earned Credits
                    if guild_config["monthly_earned_channel"]:
                        data = await self._get_earned_credits_rankings("monthly")
                        embed = self._create_leaderboard_embed(
                            "Monthly Top 10 - Earned Credits",
                            data,
                            "earned"
                        )
                        await self._post_leaderboard(guild, guild_config["monthly_earned_channel"], embed)
                    
                    # Monthly Contributions
                    if guild_config["monthly_contrib_channel"]:
                        data = await self._get_treasury_rankings("monthly")
                        embed = self._create_leaderboard_embed(
                            "Monthly Top 10 - Treasury Contributions",
                            data,
                            "contrib"
                        )
                        await self._post_leaderboard(guild, guild_config["monthly_contrib_channel"], embed)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monthly leaderboard loop: {e}", exc_info=True)
                await asyncio.sleep(86400)  # Wait 24 hours on error
    
    @commands.group(name="topplayers")
    @checks.admin_or_permissions(manage_guild=True)
    async def topplayers(self, ctx):
        """Top players leaderboard configuration commands."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @topplayers.command(name="dailyearnedchannel")
    async def set_daily_earned_channel(self, ctx, channel: discord.TextChannel):
        """Set the channel for daily earned credits leaderboard."""
        await self.config.guild(ctx.guild).daily_earned_channel.set(channel.id)
        await ctx.send(f"✅ Daily earned credits leaderboard will be posted in {channel.mention}")
    
    @topplayers.command(name="dailycontribchannel")
    async def set_daily_contrib_channel(self, ctx, channel: discord.TextChannel):
        """Set the channel for daily treasury contributions leaderboard."""
        await self.config.guild(ctx.guild).daily_contrib_channel.set(channel.id)
        await ctx.send(f"✅ Daily treasury contributions leaderboard will be posted in {channel.mention}")
    
    @topplayers.command(name="monthlyearnedchannel")
    async def set_monthly_earned_channel(self, ctx, channel: discord.TextChannel):
        """Set the channel for monthly earned credits leaderboard."""
        await self.config.guild(ctx.guild).monthly_earned_channel.set(channel.id)
        await ctx.send(f"✅ Monthly earned credits leaderboard will be posted in {channel.mention}")
    
    @topplayers.command(name="monthlycontribchannel")
    async def set_monthly_contrib_channel(self, ctx, channel: discord.TextChannel):
        """Set the channel for monthly treasury contributions leaderboard."""
        await self.config.guild(ctx.guild).monthly_contrib_channel.set(channel.id)
        await ctx.send(f"✅ Monthly treasury contributions leaderboard will be posted in {channel.mention}")
    
    @topplayers.command(name="settings")
    async def show_settings(self, ctx):
        """Show current leaderboard settings."""
        guild_config = await self.config.guild(ctx.guild).all()
        
        embed = discord.Embed(
            title="⚙️ Top Players Leaderboard Settings",
            color=discord.Color.blue()
        )
        
        # Channel settings
        channels = []
        for key, label in [
            ("daily_earned_channel", "Daily Earned Credits"),
            ("daily_contrib_channel", "Daily Contributions"),
            ("monthly_earned_channel", "Monthly Earned Credits"),
            ("monthly_contrib_channel", "Monthly Contributions"),
        ]:
            channel_id = guild_config[key]
            if channel_id:
                channel = ctx.guild.get_channel(channel_id)
                channel_text = channel.mention if channel else f"⚠️ Channel not found (ID: {channel_id})"
            else:
                channel_text = "Not set"
            channels.append(f"**{label}:** {channel_text}")
        
        embed.add_field(
            name="📢 Post Channels",
            value="\n".join(channels),
            inline=False
        )
        
        # Database status
        db_status = []
        db_status.append(f"members_v2.db: {'✅ Found' if self.members_db_path.exists() else '❌ Not found'}")
        db_status.append(f"income_v2.db: {'✅ Found' if self.income_db_path.exists() else '❌ Not found'}")
        
        embed.add_field(
            name="💾 Database Status",
            value="\n".join(db_status),
            inline=False
        )
        
        # Schedule info
        now_adam = datetime.now(self.tz_amsterdam)
        now_ny = datetime.now(self.tz_ny)
        embed.add_field(
            name="⏰ Schedule",
            value=f"**Daily:** 06:00 Amsterdam (00:00 NY) - shows full previous day\n"
                  f"**Monthly:** 1st of month at 06:00 - shows full previous month\n"
                  f"**Current time:** {now_adam.strftime('%Y-%m-%d %H:%M %Z')} / {now_ny.strftime('%H:%M %Z')}",
            inline=False
        )
        
        await ctx.send(embed=embed)
    
    @topplayers.command(name="testnow")
    @checks.is_owner()
    async def test_now(self, ctx, leaderboard_type: str):
        """
        Test a leaderboard immediately (owner only).
        Types: daily_earned, daily_contrib, monthly_earned, monthly_contrib
        """
        type_map = {
            "daily_earned": ("Daily Top 10 - Earned Credits", "daily", "earned"),
            "daily_contrib": ("Daily Top 10 - Treasury Contributions", "daily", "contrib"),
            "monthly_earned": ("Monthly Top 10 - Earned Credits", "monthly", "earned"),
            "monthly_contrib": ("Monthly Top 10 - Treasury Contributions", "monthly", "contrib"),
        }
        
        if leaderboard_type not in type_map:
            await ctx.send(f"❌ Invalid type. Use: {', '.join(type_map.keys())}")
            return
        
        title, period, board_type = type_map[leaderboard_type]
        
        # Get data
        if board_type == "earned":
            data = await self._get_earned_credits_rankings(period)
        else:
            data = await self._get_treasury_rankings(period)
        
        # Create embed
        embed = self._create_leaderboard_embed(title, data, board_type, is_test=True)
        
        await ctx.send(embed=embed)
    
    @topplayers.command(name="timestamps")
    @checks.is_owner()
    async def show_timestamps(self, ctx):
        """Show recent timestamps in both databases."""
        embed = discord.Embed(title="🕐 Recent Timestamps", color=discord.Color.blue())
        
        # Members database
        if self.members_db_path.exists():
            try:
                async with aiosqlite.connect(self.members_db_path) as db:
                    query = "SELECT DISTINCT timestamp FROM members ORDER BY timestamp DESC LIMIT 10"
                    async with db.execute(query) as cursor:
                        timestamps = await cursor.fetchall()
                    
                    ts_text = "\n".join([f"• {ts[0]}" for ts in timestamps])
                    embed.add_field(
                        name="members_v2.db (last 10 scrapes)",
                        value=ts_text or "No data",
                        inline=False
                    )
            except Exception as e:
                embed.add_field(name="members_v2.db", value=f"Error: {str(e)}", inline=False)
        else:
            embed.add_field(name="members_v2.db", value="❌ Not found", inline=False)
        
        # Income database
        if self.income_db_path.exists():
            try:
                async with aiosqlite.connect(self.income_db_path) as db:
                    query = """
                        SELECT DISTINCT timestamp, period 
                        FROM income 
                        WHERE entry_type = 'income' AND period IN ('daily', 'monthly')
                        ORDER BY timestamp DESC 
                        LIMIT 10
                    """
                    async with db.execute(query) as cursor:
                        timestamps = await cursor.fetchall()
                    
                    ts_text = "\n".join([f"• {ts[0]} ({ts[1]})" for ts in timestamps])
                    embed.add_field(
                        name="income_v2.db (last 10 scrapes, income/daily+monthly)",
                        value=ts_text or "No data",
                        inline=False
                    )
            except Exception as e:
                embed.add_field(name="income_v2.db", value=f"Error: {str(e)}", inline=False)
        else:
            embed.add_field(name="income_v2.db", value="❌ Not found", inline=False)
        
        await ctx.send(embed=embed)
    
    @topplayers.command(name="debugscrapes")
    @checks.is_owner()
    async def debug_scrapes(self, ctx, period: str = "daily"):
        """Debug: Show which scrapes would be used for a period calculation."""
        if period not in ["daily", "monthly"]:
            await ctx.send("❌ Period must be 'daily' or 'monthly'")
            return
        
        if not self.members_db_path.exists():
            await ctx.send("❌ members_v2.db not found!")
            return
        
        try:
            now = datetime.now(self.tz_amsterdam)
            current_start, current_end, previous_start, previous_end = self._get_period_boundaries(period, now)
            
            async with aiosqlite.connect(self.members_db_path) as db:
                current_scrapes = await self._get_scrapes_in_period(db, current_start, current_end)
                previous_scrapes = await self._get_scrapes_in_period(db, previous_start, previous_end)
            
            embed = discord.Embed(
                title=f"🔍 Scrapes Debug ({period})",
                color=discord.Color.blue()
            )
            
            # Current period
            if current_scrapes:
                scrape_text = f"**Total scrapes:** {len(current_scrapes)}\n"
                scrape_text += f"**First:** {current_scrapes[0]}\n"
                scrape_text += f"**Last:** {current_scrapes[-1]}\n"
                if len(current_scrapes) > 2:
                    scrape_text += f"\n**All scrapes:**\n"
                    scrape_text += "\n".join([f"• {s}" for s in current_scrapes[:10]])
                    if len(current_scrapes) > 10:
                        scrape_text += f"\n... and {len(current_scrapes) - 10} more"
            else:
                scrape_text = "❌ No scrapes found!"
            
            embed.add_field(
                name=f"Current Period ({current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')})",
                value=scrape_text,
                inline=False
            )
            
            # Previous period
            if previous_scrapes:
                prev_text = f"**Total scrapes:** {len(previous_scrapes)}\n"
                prev_text += f"**First:** {previous_scrapes[0]}\n"
                prev_text += f"**Last:** {previous_scrapes[-1]}"
            else:
                prev_text = "❌ No scrapes found!"
            
            embed.add_field(
                name=f"Previous Period ({previous_start.strftime('%Y-%m-%d')} to {previous_end.strftime('%Y-%m-%d')})",
                value=prev_text,
                inline=False
            )
            
            await ctx.send(embed=embed)
            
        except Exception as e:
            await ctx.send(f"❌ Error: {str(e)}")
            logger.error(f"Debug scrapes error: {e}", exc_info=True)
    
    @topplayers.command(name="debug")
    @checks.is_owner()
    async def debug_databases(self, ctx):
        """Debug: Check database contents and structure."""
        embed = discord.Embed(title="🔍 Database Debug Info", color=discord.Color.blue())
        
        # Check members_v2.db
        if self.members_db_path.exists():
            try:
                async with aiosqlite.connect(self.members_db_path) as db:
                    # Total records
                    async with db.execute("SELECT COUNT(*) FROM members") as cursor:
                        total = (await cursor.fetchone())[0]
                    
                    # Records with credits > 0
                    async with db.execute("SELECT COUNT(*) FROM members WHERE earned_credits > 0") as cursor:
                        with_credits = (await cursor.fetchone())[0]
                    
                    # Unique timestamps
                    async with db.execute("SELECT COUNT(DISTINCT timestamp) FROM members") as cursor:
                        unique_ts = (await cursor.fetchone())[0]
                    
                    # Latest scrape
                    async with db.execute("SELECT MAX(timestamp) FROM members") as cursor:
                        latest = (await cursor.fetchone())[0]
                    
                    # Top earners from latest scrape
                    async with db.execute("""
                        SELECT username, earned_credits 
                        FROM members 
                        WHERE timestamp = ? 
                        ORDER BY earned_credits DESC 
                        LIMIT 5
                    """, (latest,)) as cursor:
                        top_earners = await cursor.fetchall()
                    
                    top_text = "\n".join([f"• {name}: {credits:,} credits" for name, credits in top_earners])
                    
                    embed.add_field(
                        name="📊 members_v2.db",
                        value=f"**Total records:** {total:,}\n"
                              f"**With credits > 0:** {with_credits:,}\n"
                              f"**Unique timestamps:** {unique_ts:,}\n"
                              f"**Latest scrape:** {latest}\n"
                              f"**Top earners (latest):**\n{top_text}",
                        inline=False
                    )
            except Exception as e:
                embed.add_field(name="members_v2.db", value=f"❌ Error: {str(e)}", inline=False)
        else:
            embed.add_field(name="members_v2.db", value="❌ Not found", inline=False)
        
        # Check income_v2.db
        if self.income_db_path.exists():
            try:
                async with aiosqlite.connect(self.income_db_path) as db:
                    # Total records
                    async with db.execute("SELECT COUNT(*) FROM income") as cursor:
                        total = (await cursor.fetchone())[0]
                    
                    # By type
                    async with db.execute("SELECT entry_type, COUNT(*) FROM income GROUP BY entry_type") as cursor:
                        by_type = await cursor.fetchall()
                    
                    # Income records specifically
                    async with db.execute("SELECT COUNT(*) FROM income WHERE entry_type = 'income' AND period IN ('daily', 'monthly')") as cursor:
                        contrib_count = (await cursor.fetchone())[0]
                    
                    # Latest scrape
                    async with db.execute("SELECT MAX(timestamp) FROM income WHERE entry_type = 'income' AND period = 'daily'") as cursor:
                        latest = (await cursor.fetchone())[0]
                    
                    # By period
                    async with db.execute("SELECT period, COUNT(*) FROM income WHERE entry_type = 'income' GROUP BY period") as cursor:
                        by_period = await cursor.fetchall()
                    
                    # Sample data
                    async with db.execute("""
                        SELECT username, amount, period, entry_type
                        FROM income 
                        WHERE entry_type = 'income' AND period = 'daily'
                        ORDER BY timestamp DESC, amount DESC
                        LIMIT 5
                    """) as cursor:
                        sample = await cursor.fetchall()
                    
                    type_text = "\n".join([f"• {t}: {c:,}" for t, c in by_type])
                    period_text = "\n".join([f"• {p}: {c:,}" for p, c in by_period])
                    
                    if sample:
                        sample_text = "\n".join([f"• {name}: {amt:,} ({per})" for name, amt, per, typ in sample])
                    else:
                        sample_text = "No data"
                    
                    embed.add_field(
                        name="💰 income_v2.db",
                        value=f"**Total records:** {total:,}\n"
                              f"**By type:**\n{type_text}\n"
                              f"**Contribution records (daily/monthly):** {contrib_count:,}\n"
                              f"**Latest scrape:** {latest or 'N/A'}\n"
                              f"**By period:**\n{period_text}\n"
                              f"**Sample:**\n{sample_text}",
                        inline=False
                    )
            except Exception as e:
                embed.add_field(name="income_v2.db", value=f"❌ Error: {str(e)}", inline=False)
        else:
            embed.add_field(name="income_v2.db", value="❌ Not found", inline=False)
        
        await ctx.send(embed=embed)
