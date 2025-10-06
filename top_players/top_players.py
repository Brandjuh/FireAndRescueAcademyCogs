# leaderboard.py
from __future__ import annotations

import asyncio
import aiosqlite
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, time
from zoneinfo import ZoneInfo

import discord
from redbot.core import commands, checks, Config
from redbot.core.bot import Red

log = logging.getLogger("red.FARA.Leaderboard")

DEFAULTS = {
    "leaderboard_channel_id": None,
    "daily_enabled": True,
    "monthly_enabled": True,
}


class TopPlayers(commands.Cog):
    """Posts daily and monthly top 10 contribution leaderboards."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA11B0A8D, force_registration=True)
        self.config.register_global(**DEFAULTS)
        self._task: Optional[asyncio.Task] = None
        self._scraper_cog = None

    async def cog_load(self):
        """Start background task when cog loads."""
        await self._start_task()

    async def cog_unload(self):
        """Cancel background task when cog unloads."""
        if self._task:
            self._task.cancel()

    def _get_scraper(self):
        """Get AllianceScraper cog instance."""
        if self._scraper_cog is None:
            self._scraper_cog = self.bot.get_cog("AllianceScraper")
        return self._scraper_cog

    async def _get_db_path(self) -> Optional[str]:
        """Get database path from AllianceScraper cog."""
        scraper = self._get_scraper()
        if not scraper:
            log.error("AllianceScraper cog not found")
            return None
        try:
            return str(scraper.db_path)
        except AttributeError:
            log.error("AllianceScraper has no db_path attribute")
            return None

    async def _fetch_daily_top10(self) -> List[Dict[str, Any]]:
        """Fetch top 10 daily contributors based on credits gained today."""
        db_path = await self._get_db_path()
        if not db_path:
            return []

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Get today's and yesterday's date ranges (in EDT/New York time)
            now = datetime.now(ZoneInfo("America/New_York"))
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Calculate yesterday properly
            from datetime import timedelta
            yesterday_start = today_start - timedelta(days=1)
            day_before_yesterday = yesterday_start - timedelta(days=1)
            
            # Get latest snapshot from today for each user
            cur = await db.execute("""
                SELECT 
                    user_id,
                    name,
                    earned_credits as today_credits
                FROM members_history
                WHERE scraped_at >= ?
                GROUP BY user_id
                HAVING MAX(scraped_at)
            """, (today_start.isoformat(),))
            
            today_data = {row['user_id']: dict(row) for row in await cur.fetchall()}
            
            # Get latest snapshot from yesterday for each user
            cur = await db.execute("""
                SELECT 
                    user_id,
                    earned_credits as yesterday_credits
                FROM members_history
                WHERE scraped_at >= ? AND scraped_at < ?
                GROUP BY user_id
                HAVING MAX(scraped_at)
            """, (yesterday_start.isoformat(), today_start.isoformat()))
            
            yesterday_data = {row['user_id']: row['yesterday_credits'] for row in await cur.fetchall()}
            
            # Get day before yesterday for comparison
            cur = await db.execute("""
                SELECT 
                    user_id,
                    earned_credits as dby_credits
                FROM members_history
                WHERE scraped_at >= ? AND scraped_at < ?
                GROUP BY user_id
                HAVING MAX(scraped_at)
            """, (day_before_yesterday.isoformat(), yesterday_start.isoformat()))
            
            dby_data = {row['user_id']: row['dby_credits'] for row in await cur.fetchall()}
            
            # Calculate daily gains and changes
            result = []
            for user_id, data in today_data.items():
                today_credits = data['today_credits']
                yesterday_credits = yesterday_data.get(user_id, today_credits)
                dby_credits = dby_data.get(user_id, yesterday_credits)
                
                # Today's gain
                daily_gain = today_credits - yesterday_credits
                
                # Yesterday's gain (for percentage comparison)
                yesterday_gain = yesterday_credits - dby_credits
                
                # Calculate percentage change in daily gain
                if yesterday_gain > 0:
                    change_pct = ((daily_gain - yesterday_gain) / yesterday_gain) * 100
                elif daily_gain > 0:
                    change_pct = 100.0  # New contribution
                else:
                    change_pct = 0.0
                
                result.append({
                    'user_id': user_id,
                    'name': data['name'],
                    'earned_credits': today_credits,
                    'credits_gained': daily_gain,
                    'change_percentage': change_pct
                })
            
            # Sort by daily gain (not total credits)
            result.sort(key=lambda x: x['credits_gained'], reverse=True)
            
            return result[:10]

    async def _fetch_monthly_top10(self) -> List[Dict[str, Any]]:
        """Fetch top 10 monthly contributors based on credits gained this month."""
        db_path = await self._get_db_path()
        if not db_path:
            return []

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Get this month's and last month's date ranges (in EDT/New York time)
            now = datetime.now(ZoneInfo("America/New_York"))
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            # Calculate last month start
            if month_start.month == 1:
                last_month_start = month_start.replace(year=month_start.year - 1, month=12)
                two_months_ago = last_month_start.replace(month=11)
            else:
                last_month_start = month_start.replace(month=month_start.month - 1)
                if last_month_start.month == 1:
                    two_months_ago = last_month_start.replace(year=last_month_start.year - 1, month=12)
                else:
                    two_months_ago = last_month_start.replace(month=last_month_start.month - 1)
            
            # Get latest snapshot from this month for each user
            cur = await db.execute("""
                SELECT 
                    user_id,
                    name,
                    earned_credits as month_credits
                FROM members_history
                WHERE scraped_at >= ?
                GROUP BY user_id
                HAVING MAX(scraped_at)
            """, (month_start.isoformat(),))
            
            month_data = {row['user_id']: dict(row) for row in await cur.fetchall()}
            
            # Get latest snapshot from end of last month
            cur = await db.execute("""
                SELECT 
                    user_id,
                    earned_credits as last_month_credits
                FROM members_history
                WHERE scraped_at >= ? AND scraped_at < ?
                GROUP BY user_id
                HAVING MAX(scraped_at)
            """, (last_month_start.isoformat(), month_start.isoformat()))
            
            last_month_data = {row['user_id']: row['last_month_credits'] for row in await cur.fetchall()}
            
            # Get two months ago for comparison
            cur = await db.execute("""
                SELECT 
                    user_id,
                    earned_credits as two_months_credits
                FROM members_history
                WHERE scraped_at >= ? AND scraped_at < ?
                GROUP BY user_id
                HAVING MAX(scraped_at)
            """, (two_months_ago.isoformat(), last_month_start.isoformat()))
            
            two_months_data = {row['user_id']: row['two_months_credits'] for row in await cur.fetchall()}
            
            # Calculate monthly gains and changes
            result = []
            for user_id, data in month_data.items():
                month_credits = data['month_credits']
                last_month_credits = last_month_data.get(user_id, month_credits)
                two_months_credits = two_months_data.get(user_id, last_month_credits)
                
                # This month's gain
                monthly_gain = month_credits - last_month_credits
                
                # Last month's gain (for percentage comparison)
                last_month_gain = last_month_credits - two_months_credits
                
                # Calculate percentage change in monthly gain
                if last_month_gain > 0:
                    change_pct = ((monthly_gain - last_month_gain) / last_month_gain) * 100
                elif monthly_gain > 0:
                    change_pct = 100.0  # New contribution
                else:
                    change_pct = 0.0
                
                result.append({
                    'user_id': user_id,
                    'name': data['name'],
                    'earned_credits': month_credits,
                    'credits_gained': monthly_gain,
                    'change_percentage': change_pct
                })
            
            # Sort by monthly gain (not total credits)
            result.sort(key=lambda x: x['credits_gained'], reverse=True)
            
            return result[:10]

    def _format_leaderboard_embed(
        self, 
        data: List[Dict[str, Any]], 
        title: str,
        color: discord.Color
    ) -> discord.Embed:
        """Create an embed for the leaderboard."""
        embed = discord.Embed(
            title=title,
            color=color,
            timestamp=datetime.now(ZoneInfo("America/New_York"))
        )
        
        if not data:
            embed.description = "No data available."
            return embed
        
        leaderboard_text = ""
        for idx, member in enumerate(data, start=1):
            name = member.get("name", "Unknown")
            credits = member.get("earned_credits", 0)
            change_pct = member.get("change_percentage", 0.0)
            credits_gained = member.get("credits_gained", 0)
            
            # Format credits with thousand separators
            credits_fmt = f"{credits:,}".replace(",", ".")
            gained_fmt = f"{credits_gained:,}".replace(",", ".")
            
            # Format change indicator
            if change_pct > 0:
                change_indicator = f"+{change_pct:.1f}%"
            elif change_pct < 0:
                change_indicator = f"{change_pct:.1f}%"
            else:
                change_indicator = "0.0%"
            
            leaderboard_text += f"**{idx}.** {name}\n"
            leaderboard_text += f"     Credits: {credits_fmt} | Gained: {gained_fmt} ({change_indicator})\n\n"
        
        embed.description = leaderboard_text
        embed.set_footer(text="FARA Alliance Statistics")
        
        return embed

    async def _post_daily_leaderboard(self):
        """Post daily top 10 leaderboard."""
        channel_id = await self.config.leaderboard_channel_id()
        if not channel_id:
            log.warning("No leaderboard channel configured")
            return

        if not await self.config.daily_enabled():
            log.debug("Daily leaderboard disabled")
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            log.error(f"Channel {channel_id} not found")
            return

        try:
            data = await self._fetch_daily_top10()
            
            now = datetime.now(ZoneInfo("America/New_York"))
            title = f"Daily Top 10 - {now.strftime('%d-%m-%Y')}"
            embed = self._format_leaderboard_embed(data, title, discord.Color.blue())
            
            await channel.send(embed=embed)
            log.info("Posted daily leaderboard")
        except discord.Forbidden as e:
            log.error(f"No permission to post in channel {channel_id}: {e}")
        except discord.HTTPException as e:
            log.error(f"Failed to post daily leaderboard: {e}")
        except Exception as e:
            log.exception(f"Unexpected error posting daily leaderboard: {e}")

    async def _post_monthly_leaderboard(self):
        """Post monthly top 10 leaderboard."""
        channel_id = await self.config.leaderboard_channel_id()
        if not channel_id:
            log.warning("No leaderboard channel configured")
            return

        if not await self.config.monthly_enabled():
            log.debug("Monthly leaderboard disabled")
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            log.error(f"Channel {channel_id} not found")
            return

        try:
            data = await self._fetch_monthly_top10()
            
            now = datetime.now(ZoneInfo("America/New_York"))
            title = f"Monthly Top 10 - {now.strftime('%B %Y')}"
            embed = self._format_leaderboard_embed(data, title, discord.Color.gold())
            
            await channel.send(embed=embed)
            log.info("Posted monthly leaderboard")
        except discord.Forbidden as e:
            log.error(f"No permission to post in channel {channel_id}: {e}")
        except discord.HTTPException as e:
            log.error(f"Failed to post monthly leaderboard: {e}")
        except Exception as e:
            log.exception(f"Unexpected error posting monthly leaderboard: {e}")

    def _is_last_day_of_month(self, dt: datetime) -> bool:
        """Check if given datetime is the last day of the month."""
        # Check if tomorrow would be day 1
        next_day = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        try:
            next_day = next_day.replace(day=dt.day + 1)
        except ValueError:
            # Day doesn't exist, so today is last day of month
            return True
        return next_day.day == 1

    async def _scheduler_loop(self):
        """Main scheduler loop."""
        await self.bot.wait_until_red_ready()
        
        tz = ZoneInfo("America/New_York")
        target_time = time(23, 50, 0)  # 23:50 EDT/EST (New York time)
        
        log.info("Leaderboard scheduler started")
        
        while True:
            try:
                now = datetime.now(tz)
                
                # Calculate next run time (today or tomorrow at 23:50)
                next_run = now.replace(
                    hour=target_time.hour,
                    minute=target_time.minute,
                    second=target_time.second,
                    microsecond=0
                )
                
                # If we've passed today's time, schedule for tomorrow
                if now >= next_run:
                    # Move to tomorrow
                    next_day = now.day + 1
                    try:
                        next_run = next_run.replace(day=next_day)
                    except ValueError:
                        # End of month, go to next month
                        if now.month == 12:
                            next_run = next_run.replace(year=now.year + 1, month=1, day=1)
                        else:
                            next_run = next_run.replace(month=now.month + 1, day=1)
                
                wait_seconds = (next_run - now).total_seconds()
                log.debug(f"Next leaderboard post at {next_run}, waiting {wait_seconds:.0f}s")
                
                await asyncio.sleep(wait_seconds)
                
                # Check again to ensure we're at the right time
                now = datetime.now(tz)
                
                # Post daily leaderboard
                await self._post_daily_leaderboard()
                
                # Check if it's last day of month for monthly leaderboard
                if self._is_last_day_of_month(now):
                    log.info("Last day of month detected, posting monthly leaderboard")
                    await self._post_monthly_leaderboard()
                
                # Sleep a bit to avoid double-posting
                await asyncio.sleep(120)
                
            except asyncio.CancelledError:
                log.info("Scheduler loop cancelled")
                raise
            except Exception as e:
                log.exception(f"Error in scheduler loop: {e}")
                await asyncio.sleep(60)

    async def _start_task(self):
        """Start the scheduler task."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._scheduler_loop())
            log.info("Leaderboard task started")

    # ============ Commands ============

    @commands.group(name="leaderboard", aliases=["lb"])
    @checks.is_owner()
    async def leaderboard_group(self, ctx: commands.Context):
        """Leaderboard configuration and controls."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

    @leaderboard_group.command(name="channel")
    async def set_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel for leaderboard posts."""
        await self.config.leaderboard_channel_id.set(channel.id)
        await ctx.send(f"Leaderboard channel set to {channel.mention}")

    @leaderboard_group.command(name="daily")
    async def toggle_daily(self, ctx: commands.Context, enabled: bool):
        """Enable or disable daily leaderboards."""
        await self.config.daily_enabled.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"Daily leaderboards {status}")

    @leaderboard_group.command(name="monthly")
    async def toggle_monthly(self, ctx: commands.Context, enabled: bool):
        """Enable or disable monthly leaderboards."""
        await self.config.monthly_enabled.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"Monthly leaderboards {status}")

    @leaderboard_group.command(name="status")
    async def show_status(self, ctx: commands.Context):
        """Show current leaderboard configuration."""
        channel_id = await self.config.leaderboard_channel_id()
        daily = await self.config.daily_enabled()
        monthly = await self.config.monthly_enabled()
        
        channel = self.bot.get_channel(channel_id) if channel_id else None
        channel_text = channel.mention if channel else "Not configured"
        
        embed = discord.Embed(
            title="Leaderboard Configuration",
            color=discord.Color.blue()
        )
        embed.add_field(name="Channel", value=channel_text, inline=False)
        embed.add_field(name="Daily", value="Enabled" if daily else "Disabled", inline=True)
        embed.add_field(name="Monthly", value="Enabled" if monthly else "Disabled", inline=True)
        embed.add_field(
            name="Schedule", 
            value="23:50 EDT/EST (America/New_York)", 
            inline=False
        )
        
        await ctx.send(embed=embed)

    @leaderboard_group.command(name="testdaily")
    async def test_daily(self, ctx: commands.Context):
        """Test the daily leaderboard (posts immediately)."""
        channel_id = await self.config.leaderboard_channel_id()
        if not channel_id:
            await ctx.send("❌ No leaderboard channel configured. Use `[p]leaderboard channel #channel` first.")
            return
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            await ctx.send(f"❌ Channel with ID {channel_id} not found.")
            return
        
        # Check permissions
        perms = channel.permissions_for(channel.guild.me)
        if not perms.send_messages:
            await ctx.send(f"❌ Bot doesn't have permission to send messages in {channel.mention}")
            return
        if not perms.embed_links:
            await ctx.send(f"❌ Bot doesn't have permission to embed links in {channel.mention}")
            return
        
        await ctx.send(f"Fetching daily data from database...")
        data = await self._fetch_daily_top10()
        await ctx.send(f"Found {len(data)} players with daily contributions.")
        
        if not data:
            await ctx.send("⚠️ No data found. Check if AllianceScraper has data in members_history table.")
            return
        
        await ctx.send(f"Creating embed and posting to {channel.mention}...")
        
        try:
            now = datetime.now(ZoneInfo("America/New_York"))
            title = f"Daily Top 10 - {now.strftime('%d-%m-%Y')}"
            embed = self._format_leaderboard_embed(data, title, discord.Color.blue())
            
            msg = await channel.send(embed=embed)
            await ctx.send(f"✅ Daily leaderboard posted! Message ID: {msg.id}")
        except discord.Forbidden as e:
            await ctx.send(f"❌ Permission error: {e}")
        except discord.HTTPException as e:
            await ctx.send(f"❌ Discord API error: {e}")
        except Exception as e:
            await ctx.send(f"❌ Unexpected error: {e}")
            import traceback
            await ctx.send(f"```\n{traceback.format_exc()[:1900]}\n```")

    @leaderboard_group.command(name="testmonthly")
    async def test_monthly(self, ctx: commands.Context):
        """Test the monthly leaderboard (posts immediately)."""
        channel_id = await self.config.leaderboard_channel_id()
        if not channel_id:
            await ctx.send("❌ No leaderboard channel configured. Use `[p]leaderboard channel #channel` first.")
            return
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            await ctx.send(f"❌ Channel with ID {channel_id} not found.")
            return
        
        # Check permissions
        perms = channel.permissions_for(channel.guild.me)
        if not perms.send_messages:
            await ctx.send(f"❌ Bot doesn't have permission to send messages in {channel.mention}")
            return
        if not perms.embed_links:
            await ctx.send(f"❌ Bot doesn't have permission to embed links in {channel.mention}")
            return
        
        await ctx.send(f"Fetching monthly data from database...")
        data = await self._fetch_monthly_top10()
        await ctx.send(f"Found {len(data)} players with monthly contributions.")
        
        if not data:
            await ctx.send("⚠️ No data found. Check if AllianceScraper has data in members_history table.")
            return
        
        await ctx.send(f"Creating embed and posting to {channel.mention}...")
        
        try:
            now = datetime.now(ZoneInfo("America/New_York"))
            title = f"Monthly Top 10 - {now.strftime('%B %Y')}"
            embed = self._format_leaderboard_embed(data, title, discord.Color.gold())
            
            msg = await channel.send(embed=embed)
            await ctx.send(f"✅ Monthly leaderboard posted! Message ID: {msg.id}")
        except discord.Forbidden as e:
            await ctx.send(f"❌ Permission error: {e}")
        except discord.HTTPException as e:
            await ctx.send(f"❌ Discord API error: {e}")
        except Exception as e:
            await ctx.send(f"❌ Unexpected error: {e}")
            import traceback
            await ctx.send(f"```\n{traceback.format_exc()[:1900]}\n```")

    @leaderboard_group.command(name="debug")
    async def debug_data(self, ctx: commands.Context):
        """Debug: Show database contents and configuration."""
        # Check config
        channel_id = await self.config.leaderboard_channel_id()
        channel = self.bot.get_channel(channel_id) if channel_id else None
        
        await ctx.send(f"**Configuration:**\nChannel ID: {channel_id}\nChannel: {channel.mention if channel else 'Not found'}")
        
        # Check database
        db_path = await self._get_db_path()
        if not db_path:
            await ctx.send("❌ Cannot get database path. Is AllianceScraper loaded?")
            return
        
        await ctx.send(f"**Database:** {db_path}")
        
        # Check members_history table
        async with aiosqlite.connect(db_path) as db:
            # Total rows
            cur = await db.execute("SELECT COUNT(*) FROM members_history")
            total = (await cur.fetchone())[0]
            await ctx.send(f"**Total history rows:** {total}")
            
            # Recent rows
            cur = await db.execute("""
                SELECT scraped_at, COUNT(*) as cnt 
                FROM members_history 
                GROUP BY DATE(scraped_at) 
                ORDER BY scraped_at DESC 
                LIMIT 5
            """)
            rows = await cur.fetchall()
            
            if rows:
                dates_info = "\n".join([f"  {row[0][:10]}: {row[1]} entries" for row in rows])
                await ctx.send(f"**Recent scrapes:**\n{dates_info}")
            
            # Sample data
            cur = await db.execute("""
                SELECT user_id, name, earned_credits, scraped_at 
                FROM members_history 
                ORDER BY scraped_at DESC 
                LIMIT 3
            """)
            rows = await cur.fetchall()
            
            if rows:
                sample = "\n".join([f"  {r[1]}: {r[2]:,} credits @ {r[3][:16]}" for r in rows])
                await ctx.send(f"**Sample data:**\n{sample}")
        
        # Test fetch daily data
        await ctx.send("\n**Testing daily fetch...**")
        try:
            data = await self._fetch_daily_top10()
            await ctx.send(f"Found {len(data)} players")
            
            if data:
                top3 = "\n".join([
                    f"  {i+1}. {p['name']}: gained {p['credits_gained']:,} ({p['change_percentage']:+.1f}%)"
                    for i, p in enumerate(data[:3])
                ])
                await ctx.send(f"**Top 3 daily:**\n{top3}")
                
                # Check embed size
                now = datetime.now(ZoneInfo("America/New_York"))
                title = f"Daily Top 10 - {now.strftime('%d-%m-%Y')}"
                embed = self._format_leaderboard_embed(data, title, discord.Color.blue())
                
                desc_len = len(embed.description) if embed.description else 0
                await ctx.send(f"**Embed info:**\nDescription length: {desc_len} chars\nTitle: {embed.title}")
        except Exception as e:
            await ctx.send(f"❌ Error fetching daily data: {e}")
            import traceback
            await ctx.send(f"```\n{traceback.format_exc()[:1900]}\n```")

    @leaderboard_group.command(name="restart")
    async def restart_scheduler(self, ctx: commands.Context):
        """Restart the scheduler task."""
        if self._task:
            self._task.cancel()
        await self._start_task()
        await ctx.send("Scheduler restarted!")


async def setup(bot: Red):
    """Load the TopPlayers cog."""
    cog = TopPlayers(bot)
    await bot.add_cog(cog)
