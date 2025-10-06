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


class Leaderboard(commands.Cog):
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
        """Fetch top 10 contributors from today's data."""
        db_path = await self._get_db_path()
        if not db_path:
            return []

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Get today's date range (in EDT/New York time)
            now = datetime.now(ZoneInfo("America/New_York"))
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            
            cur = await db.execute("""
                SELECT 
                    user_id,
                    name,
                    earned_credits,
                    contribution_rate,
                    MAX(scraped_at) as latest_scrape
                FROM members_history
                WHERE scraped_at >= ?
                GROUP BY user_id
                ORDER BY earned_credits DESC
                LIMIT 10
            """, (today_start,))
            
            rows = await cur.fetchall()
            return [dict(row) for row in rows]

    async def _fetch_monthly_top10(self) -> List[Dict[str, Any]]:
        """Fetch top 10 contributors from this month's data."""
        db_path = await self._get_db_path()
        if not db_path:
            return []

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Get this month's date range
            now = datetime.now(ZoneInfo("Europe/Amsterdam"))
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
            
            cur = await db.execute("""
                SELECT 
                    user_id,
                    name,
                    earned_credits,
                    contribution_rate,
                    MAX(scraped_at) as latest_scrape
                FROM members_history
                WHERE scraped_at >= ?
                GROUP BY user_id
                ORDER BY earned_credits DESC
                LIMIT 10
            """, (month_start,))
            
            rows = await cur.fetchall()
            return [dict(row) for row in rows]

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
            timestamp=datetime.now(ZoneInfo("Europe/Amsterdam"))
        )
        
        if not data:
            embed.description = "Geen data beschikbaar."
            return embed

        medals = ["🥇", "🥈", "🥉"]
        
        leaderboard_text = ""
        for idx, member in enumerate(data, start=1):
            medal = medals[idx - 1] if idx <= 3 else f"**{idx}.**"
            name = member.get("name", "Unknown")
            credits = member.get("earned_credits", 0)
            rate = member.get("contribution_rate", 0.0)
            
            # Format credits with thousand separators
            credits_fmt = f"{credits:,}".replace(",", ".")
            
            leaderboard_text += f"{medal} **{name}**\n"
            leaderboard_text += f"    💰 {credits_fmt} credits | 📈 {rate:.1f}%\n\n"
        
        embed.description = leaderboard_text
        embed.set_footer(text="FARA Alliance Stats")
        
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

        data = await self._fetch_daily_top10()
        
        now = datetime.now(ZoneInfo("Europe/Amsterdam"))
        title = f"📊 Dagelijkse Top 10 - {now.strftime('%d-%m-%Y')}"
        embed = self._format_leaderboard_embed(data, title, discord.Color.blue())
        
        try:
            await channel.send(embed=embed)
            log.info("Posted daily leaderboard")
        except discord.HTTPException as e:
            log.error(f"Failed to post daily leaderboard: {e}")

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

        data = await self._fetch_monthly_top10()
        
        now = datetime.now(ZoneInfo("Europe/Amsterdam"))
        title = f"🏆 Maandelijkse Top 10 - {now.strftime('%B %Y')}"
        embed = self._format_leaderboard_embed(data, title, discord.Color.gold())
        
        try:
            await channel.send(embed=embed)
            log.info("Posted monthly leaderboard")
        except discord.HTTPException as e:
            log.error(f"Failed to post monthly leaderboard: {e}")

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
        
        tz = ZoneInfo("Europe/Amsterdam")
        target_time = time(23, 50, 0)  # 23:50 EDT/Amsterdam time
        
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
        await ctx.send(f"✅ Leaderboard kanaal ingesteld op {channel.mention}")

    @leaderboard_group.command(name="daily")
    async def toggle_daily(self, ctx: commands.Context, enabled: bool):
        """Enable or disable daily leaderboards."""
        await self.config.daily_enabled.set(enabled)
        status = "ingeschakeld" if enabled else "uitgeschakeld"
        await ctx.send(f"✅ Dagelijkse leaderboards {status}")

    @leaderboard_group.command(name="monthly")
    async def toggle_monthly(self, ctx: commands.Context, enabled: bool):
        """Enable or disable monthly leaderboards."""
        await self.config.monthly_enabled.set(enabled)
        status = "ingeschakeld" if enabled else "uitgeschakeld"
        await ctx.send(f"✅ Maandelijkse leaderboards {status}")

    @leaderboard_group.command(name="status")
    async def show_status(self, ctx: commands.Context):
        """Show current leaderboard configuration."""
        channel_id = await self.config.leaderboard_channel_id()
        daily = await self.config.daily_enabled()
        monthly = await self.config.monthly_enabled()
        
        channel = self.bot.get_channel(channel_id) if channel_id else None
        channel_text = channel.mention if channel else "Niet ingesteld"
        
        embed = discord.Embed(
            title="⚙️ Leaderboard Configuratie",
            color=discord.Color.blue()
        )
        embed.add_field(name="Kanaal", value=channel_text, inline=False)
        embed.add_field(name="Dagelijks", value="✅ Aan" if daily else "❌ Uit", inline=True)
        embed.add_field(name="Maandelijks", value="✅ Aan" if monthly else "❌ Uit", inline=True)
        embed.add_field(
            name="Tijdstip", 
            value="23:50 (Europe/Amsterdam)", 
            inline=False
        )
        
        await ctx.send(embed=embed)

    @leaderboard_group.command(name="testdaily")
    async def test_daily(self, ctx: commands.Context):
        """Test the daily leaderboard (posts immediately)."""
        await ctx.send("📊 Dagelijkse leaderboard wordt gepost...")
        await self._post_daily_leaderboard()
        await ctx.send("✅ Dagelijkse leaderboard gepost!")

    @leaderboard_group.command(name="testmonthly")
    async def test_monthly(self, ctx: commands.Context):
        """Test the monthly leaderboard (posts immediately)."""
        await ctx.send("🏆 Maandelijkse leaderboard wordt gepost...")
        await self._post_monthly_leaderboard()
        await ctx.send("✅ Maandelijkse leaderboard gepost!")

    @leaderboard_group.command(name="restart")
    async def restart_scheduler(self, ctx: commands.Context):
        """Restart the scheduler task."""
        if self._task:
            self._task.cancel()
        await self._start_task()
        await ctx.send("✅ Scheduler herstart!")


async def setup(bot: Red):
    """Load the Leaderboard cog."""
    cog = Leaderboard(bot)
    await bot.add_cog(cog)
