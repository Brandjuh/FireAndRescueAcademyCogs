# top_players.py
from __future__ import annotations

import asyncio
import aiosqlite
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import discord
from redbot.core import commands, checks, Config
from redbot.core.bot import Red

log = logging.getLogger("red.FARA.TopPlayers")

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
            
            now = datetime.now(ZoneInfo("America/New_York"))
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday_start = today_start - timedelta(days=1)
            day_before_yesterday = yesterday_start - timedelta(days=1)
            
            # Get latest snapshot from today
            cur = await db.execute("""
                SELECT user_id, name, earned_credits as today_credits
                FROM members_history
                WHERE scraped_at >= ?
                GROUP BY user_id
                HAVING MAX(scraped_at)
            """, (today_start.isoformat(),))
            today_data = {row['user_id']: dict(row) for row in await cur.fetchall()}
            
            # Get latest from yesterday
            cur = await db.execute("""
                SELECT user_id, earned_credits as yesterday_credits
                FROM members_history
                WHERE scraped_at >= ? AND scraped_at < ?
                GROUP BY user_id
                HAVING MAX(scraped_at)
            """, (yesterday_start.isoformat(), today_start.isoformat()))
            yesterday_data = {row['user_id']: row['yesterday_credits'] for row in await cur.fetchall()}
            
            # Get day before yesterday
            cur = await db.execute("""
                SELECT user_id, earned_credits as dby_credits
                FROM members_history
                WHERE scraped_at >= ? AND scraped_at < ?
                GROUP BY user_id
                HAVING MAX(scraped_at)
            """, (day_before_yesterday.isoformat(), yesterday_start.isoformat()))
            dby_data = {row['user_id']: row['dby_credits'] for row in await cur.fetchall()}
            
            result = []
            for user_id, data in today_data.items():
                today_credits = data['today_credits']
                yesterday_credits = yesterday_data.get(user_id, today_credits)
                dby_credits = dby_data.get(user_id, yesterday_credits)
                
                daily_gain = today_credits - yesterday_credits
                yesterday_gain = yesterday_credits - dby_credits
                
                if yesterday_gain > 0:
                    change_pct = ((daily_gain - yesterday_gain) / yesterday_gain) * 100
                elif daily_gain > 0:
                    change_pct = 100.0
                else:
                    change_pct = 0.0
                
                result.append({
                    'user_id': user_id,
                    'name': data['name'],
                    'earned_credits': today_credits,
                    'credits_gained': daily_gain,
                    'change_percentage': change_pct
                })
            
            result.sort(key=lambda x: x['credits_gained'], reverse=True)
            return result[:10]

    async def _fetch_daily_top10_by_contribution(self) -> List[Dict[str, Any]]:
        """Fetch top 10 daily contributors based on alliance contribution using contribution_amount."""
        db_path = await self._get_db_path()
        if not db_path:
            return []

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            now = datetime.now(ZoneInfo("America/New_York"))
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Get contribution data from logs using the contribution_amount column
            cur = await db.execute("""
                SELECT 
                    executed_mc_id,
                    executed_name,
                    SUM(contribution_amount) as total_contribution
                FROM logs
                WHERE scraped_at >= ?
                AND contribution_amount > 0
                GROUP BY executed_mc_id
                HAVING total_contribution > 0
                ORDER BY total_contribution DESC
                LIMIT 10
            """, (today_start.isoformat(),))
            
            result = []
            for row in await cur.fetchall():
                result.append({
                    'user_id': row['executed_mc_id'],
                    'name': row['executed_name'],
                    'contribution': row['total_contribution'],
                })
            
            return result

    async def _fetch_monthly_top10(self) -> List[Dict[str, Any]]:
        """Fetch top 10 monthly contributors based on credits gained this month."""
        db_path = await self._get_db_path()
        if not db_path:
            return []

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            now = datetime.now(ZoneInfo("America/New_York"))
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            # Calculate last month
            if month_start.month == 1:
                last_month_start = month_start.replace(year=month_start.year - 1, month=12)
                two_months_ago = last_month_start.replace(month=11)
            else:
                last_month_start = month_start.replace(month=month_start.month - 1)
                if last_month_start.month == 1:
                    two_months_ago = last_month_start.replace(year=last_month_start.year - 1, month=12)
                else:
                    two_months_ago = last_month_start.replace(month=last_month_start.month - 1)
            
            # Get latest snapshot from this month
            cur = await db.execute("""
                SELECT user_id, name, earned_credits as month_credits
                FROM members_history
                WHERE scraped_at >= ?
                GROUP BY user_id
                HAVING MAX(scraped_at)
            """, (month_start.isoformat(),))
            month_data = {row['user_id']: dict(row) for row in await cur.fetchall()}
            
            # Get latest from last month
            cur = await db.execute("""
                SELECT user_id, earned_credits as last_month_credits
                FROM members_history
                WHERE scraped_at >= ? AND scraped_at < ?
                GROUP BY user_id
                HAVING MAX(scraped_at)
            """, (last_month_start.isoformat(), month_start.isoformat()))
            last_month_data = {row['user_id']: row['last_month_credits'] for row in await cur.fetchall()}
            
            # Get two months ago
            cur = await db.execute("""
                SELECT user_id, earned_credits as two_months_credits
                FROM members_history
                WHERE scraped_at >= ? AND scraped_at < ?
                GROUP BY user_id
                HAVING MAX(scraped_at)
            """, (two_months_ago.isoformat(), last_month_start.isoformat()))
            two_months_data = {row['user_id']: row['two_months_credits'] for row in await cur.fetchall()}
            
            result = []
            for user_id, data in month_data.items():
                month_credits = data['month_credits']
                last_month_credits = last_month_data.get(user_id, month_credits)
                two_months_credits = two_months_data.get(user_id, last_month_credits)
                
                monthly_gain = month_credits - last_month_credits
                last_month_gain = last_month_credits - two_months_credits
                
                if last_month_gain > 0:
                    change_pct = ((monthly_gain - last_month_gain) / last_month_gain) * 100
                elif monthly_gain > 0:
                    change_pct = 100.0
                else:
                    change_pct = 0.0
                
                result.append({
                    'user_id': user_id,
                    'name': data['name'],
                    'earned_credits': month_credits,
                    'credits_gained': monthly_gain,
                    'change_percentage': change_pct
                })
            
            result.sort(key=lambda x: x['credits_gained'], reverse=True)
            return result[:10]

    async def _fetch_monthly_top10_by_contribution(self) -> List[Dict[str, Any]]:
        """Fetch top 10 monthly contributors based on alliance contribution using contribution_amount."""
        db_path = await self._get_db_path()
        if not db_path:
            return []

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            now = datetime.now(ZoneInfo("America/New_York"))
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            # Get contribution data from logs using the contribution_amount column
            cur = await db.execute("""
                SELECT 
                    executed_mc_id,
                    executed_name,
                    SUM(contribution_amount) as total_contribution
                FROM logs
                WHERE scraped_at >= ?
                AND contribution_amount > 0
                GROUP BY executed_mc_id
                HAVING total_contribution > 0
                ORDER BY total_contribution DESC
                LIMIT 10
            """, (month_start.isoformat(),))
            
            result = []
            for row in await cur.fetchall():
                result.append({
                    'user_id': row['executed_mc_id'],
                    'name': row['executed_name'],
                    'contribution': row['total_contribution'],
                })
            
            return result

    def _format_leaderboard_embed(
        self, 
        data: List[Dict[str, Any]], 
        title: str,
        color: discord.Color,
        by_contribution: bool = False
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
        
        if by_contribution:
            for idx, member in enumerate(data, start=1):
                name = member.get("name", "Unknown")
                contribution = member.get("contribution", 0)
                contrib_fmt = f"{contribution:,}".replace(",", ".")
                
                leaderboard_text += f"**{idx}.** {name}\n"
                leaderboard_text += f"     Contribution: {contrib_fmt}\n\n"
        else:
            for idx, member in enumerate(data, start=1):
                name = member.get("name", "Unknown")
                credits = member.get("earned_credits", 0)
                change_pct = member.get("change_percentage", 0.0)
                credits_gained = member.get("credits_gained", 0)
                
                credits_fmt = f"{credits:,}".replace(",", ".")
                gained_fmt = f"{credits_gained:,}".replace(",", ".")
                
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
        """Post daily top 10 leaderboards (by credits and by contribution)."""
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
            data_credits = await self._fetch_daily_top10()
            data_contribution = await self._fetch_daily_top10_by_contribution()
            
            now = datetime.now(ZoneInfo("America/New_York"))
            
            title_credits = f"Daily Top 10 - {now.strftime('%d-%m-%Y')}\nBy Earned Credits"
            embed_credits = self._format_leaderboard_embed(
                data_credits, title_credits, discord.Color.blue(), by_contribution=False
            )
            
            title_contribution = f"Daily Top 10 - {now.strftime('%d-%m-%Y')}\nBy Alliance Contribution"
            embed_contribution = self._format_leaderboard_embed(
                data_contribution, title_contribution, discord.Color.green(), by_contribution=True
            )
            
            await channel.send(embed=embed_credits)
            await channel.send(embed=embed_contribution)
            
            log.info("Posted daily leaderboards")
        except discord.Forbidden as e:
            log.error(f"No permission to post in channel {channel_id}: {e}")
        except discord.HTTPException as e:
            log.error(f"Failed to post daily leaderboard: {e}")
        except Exception as e:
            log.exception(f"Unexpected error posting daily leaderboard: {e}")

    async def _post_monthly_leaderboard(self):
        """Post monthly top 10 leaderboards (by credits and by contribution)."""
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
            data_credits = await self._fetch_monthly_top10()
            data_contribution = await self._fetch_monthly_top10_by_contribution()
            
            now = datetime.now(ZoneInfo("America/New_York"))
            
            title_credits = f"Monthly Top 10 - {now.strftime('%B %Y')}\nBy Earned Credits"
            embed_credits = self._format_leaderboard_embed(
                data_credits, title_credits, discord.Color.gold(), by_contribution=False
            )
            
            title_contribution = f"Monthly Top 10 - {now.strftime('%B %Y')}\nBy Alliance Contribution"
            embed_contribution = self._format_leaderboard_embed(
                data_contribution, title_contribution, discord.Color.orange(), by_contribution=True
            )
            
            await channel.send(embed=embed_credits)
            await channel.send(embed=embed_contribution)
            
            log.info("Posted monthly leaderboards")
        except discord.Forbidden as e:
            log.error(f"No permission to post in channel {channel_id}: {e}")
        except discord.HTTPException as e:
            log.error(f"Failed to post monthly leaderboard: {e}")
        except Exception as e:
            log.exception(f"Unexpected error posting monthly leaderboard: {e}")

    def _is_last_day_of_month(self, dt: datetime) -> bool:
        """Check if given datetime is the last day of the month."""
        try:
            dt.replace(day=dt.day + 1)
            return False
        except ValueError:
            return True

    async def _scheduler_loop(self):
        """Main scheduler loop."""
        await self.bot.wait_until_red_ready()
        
        tz = ZoneInfo("America/New_York")
        target_time = time(23, 50, 0)
        
        log.info("Leaderboard scheduler started")
        
        while True:
            try:
                now = datetime.now(tz)
                next_run = now.replace(
                    hour=target_time.hour,
                    minute=target_time.minute,
                    second=target_time.second,
                    microsecond=0
                )
                
                if now >= next_run:
                    next_run = next_run + timedelta(days=1)
                
                wait_seconds = (next_run - now).total_seconds()
                log.debug(f"Next leaderboard post at {next_run}, waiting {wait_seconds:.0f}s")
                
                await asyncio.sleep(wait_seconds)
                
                now = datetime.now(tz)
                await self._post_daily_leaderboard()
                
                if self._is_last_day_of_month(now):
                    log.info("Last day of month detected, posting monthly leaderboard")
                    await self._post_monthly_leaderboard()
                
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

    # Commands omitted for brevity - same as before
    @commands.group(name="leaderboard", aliases=["lb"])
    @checks.is_owner()
    async def leaderboard_group(self, ctx: commands.Context):
        """Leaderboard configuration and controls."""
        pass

    @leaderboard_group.command(name="channel")
    async def set_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.leaderboard_channel_id.set(channel.id)
        await ctx.send(f"Leaderboard channel set to {channel.mention}")

    @leaderboard_group.command(name="testdaily")
    async def test_daily(self, ctx: commands.Context):
        channel_id = await self.config.leaderboard_channel_id()
        if not channel_id:
            await ctx.send("No channel configured")
            return
        await ctx.send("Posting daily leaderboard...")
        await self._post_daily_leaderboard()
        await ctx.send("Done!")


async def setup(bot: Red):
    cog = TopPlayers(bot)
    await bot.add_cog(cog)
