"""
Alliance Leaderboard System for Missionchief USA
Displays daily and monthly top 10 rankings for earned credits and treasury contributions.
Uses NEW scraper_databases structure (members_v2.db, income_v2.db)
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
            "daily_earned_channel": None,
            "daily_contrib_channel": None,
            "monthly_earned_channel": None,
            "monthly_contrib_channel": None,
        }
        self.config.register_guild(**default_guild)
        
        # Database paths for new scraper_databases structure
        base_path = cog_data_path(raw_name="scraper_databases")
        self.members_db_path = base_path / "members_v2.db"
        self.income_db_path = base_path / "income_v2.db"
        
        # Timezone for Amsterdam
        self.tz = pytz.timezone('Europe/Amsterdam')
        
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
    
    async def _get_earned_credits_rankings(self, period: str) -> Optional[Dict]:
        """
        Get earned credits rankings from members_v2.db.
        Calculates DELTA between current and previous timestamps.
        Returns dict with 'current' and 'previous' lists of {username, credits, rank}
        """
        if not self.members_db_path.exists():
            logger.error("members_v2.db not found")
            return None
        
        try:
            async with aiosqlite.connect(self.members_db_path) as db:
                # Determine lookback period
                lookback_hours = 24 if period == "daily" else 30 * 24  # 30 days for monthly
                
                # Get the two most recent timestamps
                query = """
                    SELECT DISTINCT timestamp
                    FROM members
                    ORDER BY timestamp DESC
                    LIMIT 2
                """
                
                async with db.execute(query) as cursor:
                    timestamps = await cursor.fetchall()
                
                if len(timestamps) < 2:
                    logger.warning("Not enough timestamps for earned credits comparison")
                    return None
                
                current_ts = timestamps[0][0]
                
                # Calculate lookback timestamp
                current_dt = datetime.fromisoformat(current_ts)
                lookback_dt = current_dt - timedelta(hours=lookback_hours)
                
                logger.info(f"Earned credits {period} - Current: {current_ts}, Lookback: {lookback_dt.isoformat()}")
                
                # Get current credits for all members
                query = """
                    SELECT member_id, username, earned_credits
                    FROM members
                    WHERE timestamp = ?
                    AND earned_credits > 0
                """
                
                async with db.execute(query, (current_ts,)) as cursor:
                    current_data = await cursor.fetchall()
                
                if not current_data:
                    logger.warning("No current data found")
                    return None
                
                # Get credits from lookback period (find closest timestamp)
                query = """
                    SELECT DISTINCT timestamp
                    FROM members
                    WHERE timestamp <= ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                """
                
                async with db.execute(query, (lookback_dt.isoformat(),)) as cursor:
                    lookback_result = await cursor.fetchone()
                
                if not lookback_result:
                    logger.warning("No lookback timestamp found")
                    return None
                
                lookback_ts = lookback_result[0]
                logger.info(f"Using lookback timestamp: {lookback_ts}")
                
                # Get lookback credits
                query = """
                    SELECT member_id, earned_credits
                    FROM members
                    WHERE timestamp = ?
                """
                
                async with db.execute(query, (lookback_ts,)) as cursor:
                    lookback_data = await cursor.fetchall()
                
                # Build lookback map
                lookback_map = {member_id: credits for member_id, credits in lookback_data}
                
                # Calculate deltas
                delta_list = []
                for member_id, username, current_credits in current_data:
                    lookback_credits = lookback_map.get(member_id, current_credits)
                    delta = current_credits - lookback_credits
                    
                    if delta > 0:  # Only include members with positive growth
                        delta_list.append({
                            "username": username,
                            "credits": delta,
                            "user_id": str(member_id)
                        })
                
                if not delta_list:
                    logger.warning("No members with positive credit growth")
                    return None
                
                # Filter invalid entries
                delta_filtered = self._filter_invalid_entries(delta_list)
                
                # Sort by delta and get top 10
                delta_filtered.sort(key=lambda x: x["credits"], reverse=True)
                current_rankings = delta_filtered[:10]
                
                # Add ranks
                for i, entry in enumerate(current_rankings, 1):
                    entry["rank"] = i
                
                # For previous period, do the same calculation but with older timestamps
                # Get timestamp for previous period
                previous_lookback_dt = lookback_dt - timedelta(hours=lookback_hours)
                
                query = """
                    SELECT DISTINCT timestamp
                    FROM members
                    WHERE timestamp <= ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                """
                
                async with db.execute(query, (previous_lookback_dt.isoformat(),)) as cursor:
                    prev_lookback_result = await cursor.fetchone()
                
                previous_rankings = []
                if prev_lookback_result:
                    prev_lookback_ts = prev_lookback_result[0]
                    
                    # Get previous period data
                    async with db.execute("SELECT member_id, username, earned_credits FROM members WHERE timestamp = ?", 
                                        (lookback_ts,)) as cursor:
                        prev_current_data = await cursor.fetchall()
                    
                    async with db.execute("SELECT member_id, earned_credits FROM members WHERE timestamp = ?",
                                        (prev_lookback_ts,)) as cursor:
                        prev_lookback_data = await cursor.fetchall()
                    
                    prev_lookback_map = {member_id: credits for member_id, credits in prev_lookback_data}
                    
                    prev_delta_list = []
                    for member_id, username, credits in prev_current_data:
                        prev_lookback_credits = prev_lookback_map.get(member_id, credits)
                        delta = credits - prev_lookback_credits
                        
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
        NOTE: IncomeScraper stores member contributions as 'income' type (not 'expense')!
        We filter out period='paginated' which contains actual expenses (African Prison, etc.)
        Returns dict with 'current' and 'previous' lists of {username, credits, rank}
        """
        if not self.income_db_path.exists():
            logger.error("income_v2.db not found")
            return None
        
        try:
            async with aiosqlite.connect(self.income_db_path) as db:
                # Get the two most recent timestamps for this period
                # NOTE: Using entry_type='income' (not 'expense')!
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
            timestamp=datetime.now(self.tz)
        )
        
        if not rankings_data or not rankings_data.get("current"):
            embed.description = "No data available for this period."
            embed.set_footer(text="Rankings based on last 24 hours • Using new scraper_databases")
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
        
        period_text = "last 24 hours" if "Daily" in title else "last 30 days"
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
        """Daily leaderboard posting at 06:00 Amsterdam time."""
        await self.bot.wait_until_ready()
        
        while not self.bot.is_closed():
            try:
                now = datetime.now(self.tz)
                
                # Calculate next 06:00
                target = now.replace(hour=6, minute=0, second=0, microsecond=0)
                if now >= target:
                    target += timedelta(days=1)
                
                wait_seconds = (target - now).total_seconds()
                logger.info(f"Next daily leaderboard in {wait_seconds/3600:.1f} hours")
                
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
        """Monthly leaderboard posting on last day of month at 06:00 Amsterdam time."""
        await self.bot.wait_until_ready()
        
        while not self.bot.is_closed():
            try:
                now = datetime.now(self.tz)
                
                # Calculate next last day of month at 06:00
                # Move to next month, then subtract 1 day
                if now.month == 12:
                    next_month = now.replace(year=now.year + 1, month=1, day=1)
                else:
                    next_month = now.replace(month=now.month + 1, day=1)
                
                last_day = next_month - timedelta(days=1)
                target = last_day.replace(hour=6, minute=0, second=0, microsecond=0)
                
                # If we're past this month's target, calculate for next month
                if now >= target:
                    if target.month == 12:
                        next_month = target.replace(year=target.year + 1, month=1, day=1)
                    else:
                        next_month = target.replace(month=target.month + 1, day=1)
                    
                    last_day = next_month - timedelta(days=1)
                    target = last_day.replace(hour=6, minute=0, second=0, microsecond=0)
                
                wait_seconds = (target - now).total_seconds()
                logger.info(f"Next monthly leaderboard on {target.date()} ({wait_seconds/86400:.1f} days)")
                
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
        now = datetime.now(self.tz)
        embed.add_field(
            name="⏰ Schedule",
            value=f"**Daily:** 06:00 Amsterdam time\n**Monthly:** Last day of month at 06:00\n**Current time:** {now.strftime('%Y-%m-%d %H:%M:%S %Z')}",
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
                        WHERE entry_type = 'expense' AND period IN ('daily', 'monthly')
                        ORDER BY timestamp DESC 
                        LIMIT 10
                    """
                    async with db.execute(query) as cursor:
                        timestamps = await cursor.fetchall()
                    
                    ts_text = "\n".join([f"• {ts[0]} ({ts[1]})" for ts in timestamps])
                    embed.add_field(
                        name="income_v2.db (last 10 scrapes, expense/daily)",
                        value=ts_text or "No data",
                        inline=False
                    )
            except Exception as e:
                embed.add_field(name="income_v2.db", value=f"Error: {str(e)}", inline=False)
        else:
            embed.add_field(name="income_v2.db", value="❌ Not found", inline=False)
        
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
                    async with db.execute("SELECT COUNT(*) FROM income WHERE entry_type = 'expense' AND period IN ('daily', 'monthly')") as cursor:
                        contrib_count = (await cursor.fetchone())[0]
                    
                    # Latest scrape
                    async with db.execute("SELECT MAX(timestamp) FROM income WHERE entry_type = 'expense' AND period = 'daily'") as cursor:
                        latest = (await cursor.fetchone())[0]
                    
                    # By period
                    async with db.execute("SELECT period, COUNT(*) FROM income WHERE entry_type = 'expense' GROUP BY period") as cursor:
                        by_period = await cursor.fetchall()
                    
                    # Sample data (show ALL types if no income)
                    async with db.execute("""
                        SELECT username, amount, period, entry_type
                        FROM income 
                        WHERE entry_type = 'expense' AND period = 'daily'
                        ORDER BY timestamp DESC, amount DESC
                        LIMIT 5
                    """) as cursor:
                        sample = await cursor.fetchall()
                    
                    type_text = "\n".join([f"• {t}: {c:,}" for t, c in by_type])
                    period_text = "\n".join([f"• {p}: {c:,}" for p, c in by_period])
                    
                    if sample:
                        sample_text = "\n".join([f"• {name}: {amt:,} ({per}, type={typ})" for name, amt, per, typ in sample])
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
    
    @topplayers.command(name="checklatest")
    @checks.is_owner()
    async def check_latest(self, ctx, period: str = "daily", username: str = None):
        """
        Check what's in the latest scrape for a specific period, optionally search for specific username.
        NOTE: Only shows contribution data (period='daily' or 'monthly'), NOT expenses (period='paginated')
        """
        if not self.income_db_path.exists():
            await ctx.send("❌ income_v2.db not found!")
            return
        
        if period not in ["daily", "monthly"]:
            await ctx.send("❌ Period must be 'daily' or 'monthly'")
            return
        
        try:
            async with aiosqlite.connect(self.income_db_path) as db:
                # First, let's see ALL distinct periods and entry_types
                debug_query = "SELECT DISTINCT entry_type, period, COUNT(*) FROM income GROUP BY entry_type, period"
                async with db.execute(debug_query) as cursor:
                    all_data = await cursor.fetchall()
                    await ctx.send(f"🔍 DEBUG - Data in database:\n" + "\n".join([f"  • {etype} / {per}: {cnt} records" for etype, per, cnt in all_data]))
                
                # Get latest timestamp for this period (contributions only, not paginated expenses)
                query = """
                    SELECT MAX(timestamp) as latest_ts
                    FROM income
                    WHERE entry_type = 'income' AND period = ?
                """
                async with db.execute(query, (period,)) as cursor:
                    result = await cursor.fetchone()
                    if not result or not result[0]:
                        await ctx.send(f"❌ No data found for period: {period}\nTry checking if entry_type='income' and period='{period}' exists in debug info above.")
                        return
                    
                    latest_ts = result[0]
                
                # If searching for specific username
                if username:
                    query = """
                        SELECT username, amount
                        FROM income
                        WHERE entry_type = 'income' 
                        AND period = ?
                        AND timestamp = ?
                        AND username LIKE ?
                        ORDER BY amount DESC
                    """
                    async with db.execute(query, (period, latest_ts, f"%{username}%")) as cursor:
                        results = await cursor.fetchall()
                        
                        if not results:
                            await ctx.send(f"❌ Username '{username}' not found in latest {period} scrape (timestamp: {latest_ts})")
                            return
                        
                        embed = discord.Embed(
                            title=f"🔍 Search Results for '{username}'",
                            description=f"Period: {period}\nTimestamp: {latest_ts}",
                            color=discord.Color.blue()
                        )
                        
                        for i, (uname, amount) in enumerate(results, 1):
                            embed.add_field(
                                name=f"{i}. {uname}",
                                value=f"{amount:,} credits",
                                inline=False
                            )
                        
                        await ctx.send(embed=embed)
                        return
                
                # Get ALL contributors (not just top 15) to verify complete data
                query = """
                    SELECT username, amount
                    FROM income
                    WHERE entry_type = 'income' 
                    AND period = ?
                    AND timestamp = ?
                    ORDER BY amount DESC
                """
                
                async with db.execute(query, (period, latest_ts)) as cursor:
                    results = await cursor.fetchall()
                
                if not results:
                    await ctx.send(f"❌ No contributors found for {period}")
                    return
                
                embed = discord.Embed(
                    title=f"📊 Latest {period.upper()} scrape (RAW DATA)",
                    description=f"Timestamp: {latest_ts}\nTotal contributors: {len(results)}",
                    color=discord.Color.green()
                )
                
                # Show top 20
                contributors_text = "\n".join([
                    f"{i}. **{username}** - {amount:,} credits"
                    for i, (username, amount) in enumerate(results[:20], 1)
                ])
                
                embed.add_field(
                    name="Top 20 Contributors (unfiltered)",
                    value=contributors_text,
                    inline=False
                )
                
                if len(results) > 20:
                    embed.add_field(
                        name="Note",
                        value=f"... and {len(results) - 20} more contributors",
                        inline=False
                    )
                
                await ctx.send(embed=embed)
                
        except Exception as e:
            logger.error(f"Error checking latest scrape: {e}", exc_info=True)
            await ctx.send(f"❌ Error: {str(e)}")
