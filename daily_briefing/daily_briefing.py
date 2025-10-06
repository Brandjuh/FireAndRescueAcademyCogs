# daily_briefing.py
from __future__ import annotations

import asyncio
import aiosqlite
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from collections import Counter

import discord
from redbot.core import commands, checks, Config
from redbot.core.bot import Red

log = logging.getLogger("red.FARA.DailyBriefing")

DEFAULTS = {
    "briefing_channel_id": None,
    "briefing_enabled": True,
}


class DailyBriefing(commands.Cog):
    """Posts daily alliance briefing with statistics."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA11B81EF, force_registration=True)
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

    async def _get_member_stats(self) -> Dict[str, Any]:
        """Get new and left members for today."""
        db_path = await self._get_db_path()
        if not db_path:
            return {"new": 0, "left": 0}

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            now = datetime.now(ZoneInfo("America/New_York"))
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Get new members (added_to_alliance action)
            cur = await db.execute("""
                SELECT COUNT(DISTINCT affected_mc_id) 
                FROM logs
                WHERE action_key = 'added_to_alliance'
                AND ts >= ?
            """, (today_start.isoformat(),))
            new_members = (await cur.fetchone())[0]
            
            # Get left members (left_alliance + kicked_from_alliance actions)
            cur = await db.execute("""
                SELECT COUNT(DISTINCT affected_mc_id)
                FROM logs
                WHERE action_key IN ('left_alliance', 'kicked_from_alliance')
                AND ts >= ?
            """, (today_start.isoformat(),))
            left_members = (await cur.fetchone())[0]
            
            return {"new": new_members, "left": left_members}

    async def _get_building_stats(self) -> Dict[str, Any]:
        """Get building statistics for today and yesterday."""
        db_path = await self._get_db_path()
        if not db_path:
            return {
                "built_today": 0, "built_yesterday": 0,
                "destroyed_today": 0, "destroyed_yesterday": 0,
                "expansions_today": 0, "expansions_yesterday": 0
            }

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            now = datetime.now(ZoneInfo("America/New_York"))
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday_start = today_start - timedelta(days=1)
            
            # Buildings constructed today
            cur = await db.execute("""
                SELECT COUNT(*)
                FROM logs
                WHERE action_key = 'building_constructed'
                AND ts >= ?
            """, (today_start.isoformat(),))
            built_today = (await cur.fetchone())[0]
            
            # Buildings constructed yesterday
            cur = await db.execute("""
                SELECT COUNT(*)
                FROM logs
                WHERE action_key = 'building_constructed'
                AND ts >= ? AND ts < ?
            """, (yesterday_start.isoformat(), today_start.isoformat()))
            built_yesterday = (await cur.fetchone())[0]
            
            # Buildings destroyed today
            cur = await db.execute("""
                SELECT COUNT(*)
                FROM logs
                WHERE action_key = 'building_destroyed'
                AND ts >= ?
            """, (today_start.isoformat(),))
            destroyed_today = (await cur.fetchone())[0]
            
            # Buildings destroyed yesterday
            cur = await db.execute("""
                SELECT COUNT(*)
                FROM logs
                WHERE action_key = 'building_destroyed'
                AND ts >= ? AND ts < ?
            """, (yesterday_start.isoformat(), today_start.isoformat()))
            destroyed_yesterday = (await cur.fetchone())[0]
            
            # Expansions today
            cur = await db.execute("""
                SELECT COUNT(*)
                FROM logs
                WHERE action_key IN ('extension_started', 'expansion_finished')
                AND ts >= ?
            """, (today_start.isoformat(),))
            expansions_today = (await cur.fetchone())[0]
            
            # Expansions yesterday
            cur = await db.execute("""
                SELECT COUNT(*)
                FROM logs
                WHERE action_key IN ('extension_started', 'expansion_finished')
                AND ts >= ? AND ts < ?
            """, (yesterday_start.isoformat(), today_start.isoformat()))
            expansions_yesterday = (await cur.fetchone())[0]
            
            return {
                "built_today": built_today,
                "built_yesterday": built_yesterday,
                "destroyed_today": destroyed_today,
                "destroyed_yesterday": destroyed_yesterday,
                "expansions_today": expansions_today,
                "expansions_yesterday": expansions_yesterday
            }

    async def _get_training_stats(self) -> Dict[str, Any]:
        """Get training statistics for today and yesterday."""
        db_path = await self._get_db_path()
        if not db_path:
            return {
                "completed_today": 0,
                "completed_yesterday": 0,
                "by_course_completed": {},
                "by_course_created": {}
            }

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            
            now = datetime.now(ZoneInfo("America/New_York"))
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday_start = today_start - timedelta(days=1)
            
            # Total completed today
            cur = await db.execute("""
                SELECT COUNT(*)
                FROM logs
                WHERE action_key = 'course_completed'
                AND ts >= ?
            """, (today_start.isoformat(),))
            completed_today = (await cur.fetchone())[0]
            
            # Total completed yesterday
            cur = await db.execute("""
                SELECT COUNT(*)
                FROM logs
                WHERE action_key = 'course_completed'
                AND ts >= ? AND ts < ?
            """, (yesterday_start.isoformat(), today_start.isoformat()))
            completed_yesterday = (await cur.fetchone())[0]
            
            # Breakdown by course completed (today)
            cur = await db.execute("""
                SELECT description, COUNT(*) as cnt
                FROM logs
                WHERE action_key = 'course_completed'
                AND ts >= ?
                GROUP BY description
                ORDER BY cnt DESC
            """, (today_start.isoformat(),))
            
            by_course_completed = {}
            for row in await cur.fetchall():
                desc = row['description']
                # Extract course name - remove "course completed:" prefix
                course_name = desc.replace("course completed:", "").replace("course completed", "").strip()
                if course_name and course_name.lower() != "course completed":
                    by_course_completed[course_name] = row['cnt']
            
            # Breakdown by course created (today)
            cur = await db.execute("""
                SELECT description, COUNT(*) as cnt
                FROM logs
                WHERE action_key = 'created_course'
                AND ts >= ?
                GROUP BY description
                ORDER BY cnt DESC
            """, (today_start.isoformat(),))
            
            by_course_created = {}
            for row in await cur.fetchall():
                desc = row['description']
                # Extract course name - remove "created a course:" or similar prefix
                course_name = desc.replace("created a course:", "").replace("created course:", "").replace("created a course", "").strip()
                if course_name and course_name.lower() not in ["created a course", "created course"]:
                    by_course_created[course_name] = row['cnt']
            
            return {
                "completed_today": completed_today,
                "completed_yesterday": completed_yesterday,
                "by_course_completed": by_course_completed,
                "by_course_created": by_course_created
            }

    def _format_diff(self, today: int, yesterday: int) -> str:
        """Format difference with + or - indicator."""
        diff = today - yesterday
        if diff > 0:
            return f"+{diff}"
        elif diff < 0:
            return f"{diff}"
        else:
            return "±0"

    async def _create_members_buildings_embed(
        self, 
        member_stats: Dict[str, Any],
        building_stats: Dict[str, Any]
    ) -> discord.Embed:
        """Create combined embed for members and buildings."""
        now = datetime.now(ZoneInfo("America/New_York"))
        
        embed = discord.Embed(
            title=f"Daily Briefing - {now.strftime('%d %B %Y')}",
            color=discord.Color.blue(),
            timestamp=now
        )
        
        # Member section
        new_members = member_stats["new"]
        left_members = member_stats["left"]
        net_change = new_members - left_members
        net_str = f"+{net_change}" if net_change >= 0 else str(net_change)
        
        member_text = f"**New Members:** {new_members}\n"
        member_text += f"**Left Members:** {left_members}\n"
        member_text += f"**Net Change:** {net_str}"
        
        embed.add_field(
            name="👥 Member Activity",
            value=member_text,
            inline=False
        )
        
        # Building section
        built_diff = self._format_diff(building_stats["built_today"], building_stats["built_yesterday"])
        destroyed_diff = self._format_diff(building_stats["destroyed_today"], building_stats["destroyed_yesterday"])
        expansion_diff = self._format_diff(building_stats["expansions_today"], building_stats["expansions_yesterday"])
        
        building_text = f"**Built:** {building_stats['built_today']} ({built_diff})\n"
        building_text += f"**Destroyed:** {building_stats['destroyed_today']} ({destroyed_diff})\n"
        building_text += f"**Expansions:** {building_stats['expansions_today']} ({expansion_diff})"
        
        embed.add_field(
            name="🏗️ Building Activity",
            value=building_text,
            inline=False
        )
        
        embed.set_footer(text="FARA Alliance Statistics")
        
        return embed

    async def _create_training_embed(self, training_stats: Dict[str, Any]) -> discord.Embed:
        """Create embed for training statistics."""
        now = datetime.now(ZoneInfo("America/New_York"))
        
        embed = discord.Embed(
            title="Training Activity",
            color=discord.Color.green(),
            timestamp=now
        )
        
        completed_today = training_stats["completed_today"]
        completed_yesterday = training_stats["completed_yesterday"]
        diff = self._format_diff(completed_today, completed_yesterday)
        
        summary_text = f"**Total Completed:** {completed_today} ({diff})"
        
        embed.add_field(
            name="📚 Training Summary",
            value=summary_text,
            inline=False
        )
        
        # Add breakdown by course
        by_course = training_stats["by_course"]
        if by_course:
            # Sort by count and take top 10
            sorted_courses = sorted(by_course.items(), key=lambda x: x[1], reverse=True)[:10]
            
            course_text = ""
            for course, count in sorted_courses:
                # Truncate long course names
                display_name = course[:40] + "..." if len(course) > 40 else course
                course_text += f"**{count}x** {display_name}\n"
            
            if course_text:
                embed.add_field(
                    name="📖 Courses Completed Today",
                    value=course_text,
                    inline=False
                )
        else:
            embed.add_field(
                name="📖 Courses Completed Today",
                value="No courses completed",
                inline=False
            )
        
        embed.set_footer(text="FARA Alliance Statistics")
        
        return embed

    async def _post_daily_briefing(self):
        """Post daily briefing to configured channel."""
        channel_id = await self.config.briefing_channel_id()
        if not channel_id:
            log.warning("No briefing channel configured")
            return

        if not await self.config.briefing_enabled():
            log.debug("Daily briefing disabled")
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            log.error(f"Channel {channel_id} not found")
            return

        try:
            # Fetch all statistics
            member_stats = await self._get_member_stats()
            building_stats = await self._get_building_stats()
            training_stats = await self._get_training_stats()
            
            # Create embeds
            members_buildings_embed = await self._create_members_buildings_embed(
                member_stats, building_stats
            )
            training_embed = await self._create_training_embed(training_stats)
            
            # Post embeds
            await channel.send(embed=members_buildings_embed)
            await channel.send(embed=training_embed)
            
            log.info("Posted daily briefing")
        except discord.Forbidden as e:
            log.error(f"No permission to post in channel {channel_id}: {e}")
        except discord.HTTPException as e:
            log.error(f"Failed to post daily briefing: {e}")
        except Exception as e:
            log.exception(f"Unexpected error posting daily briefing: {e}")

    async def _scheduler_loop(self):
        """Main scheduler loop."""
        await self.bot.wait_until_red_ready()
        
        tz = ZoneInfo("America/New_York")
        target_time = time(23, 50, 0)  # 23:50 EDT/EST
        
        log.info("Daily briefing scheduler started")
        
        while True:
            try:
                now = datetime.now(tz)
                
                # Calculate next run time
                next_run = now.replace(
                    hour=target_time.hour,
                    minute=target_time.minute,
                    second=target_time.second,
                    microsecond=0
                )
                
                # If we've passed today's time, schedule for tomorrow
                if now >= next_run:
                    next_run = next_run + timedelta(days=1)
                
                wait_seconds = (next_run - now).total_seconds()
                log.debug(f"Next briefing at {next_run}, waiting {wait_seconds:.0f}s")
                
                await asyncio.sleep(wait_seconds)
                
                # Post briefing
                await self._post_daily_briefing()
                
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
            log.info("Daily briefing task started")

    # ============ Commands ============

    @commands.group(name="briefing")
    @checks.is_owner()
    async def briefing_group(self, ctx: commands.Context):
        """Daily briefing configuration and controls."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

    @briefing_group.command(name="channel")
    async def set_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel for daily briefings."""
        await self.config.briefing_channel_id.set(channel.id)
        await ctx.send(f"Daily briefing channel set to {channel.mention}")

    @briefing_group.command(name="toggle")
    async def toggle_briefing(self, ctx: commands.Context, enabled: bool):
        """Enable or disable daily briefings."""
        await self.config.briefing_enabled.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"Daily briefings {status}")

    @briefing_group.command(name="status")
    async def show_status(self, ctx: commands.Context):
        """Show current briefing configuration."""
        channel_id = await self.config.briefing_channel_id()
        enabled = await self.config.briefing_enabled()
        
        channel = self.bot.get_channel(channel_id) if channel_id else None
        channel_text = channel.mention if channel else "Not configured"
        
        embed = discord.Embed(
            title="Daily Briefing Configuration",
            color=discord.Color.blue()
        )
        embed.add_field(name="Channel", value=channel_text, inline=False)
        embed.add_field(name="Status", value="Enabled" if enabled else "Disabled", inline=True)
        embed.add_field(
            name="Schedule", 
            value="23:50 EDT/EST (America/New_York)", 
            inline=False
        )
        
        await ctx.send(embed=embed)

    @briefing_group.command(name="test")
    async def test_briefing(self, ctx: commands.Context):
        """Test the daily briefing (posts immediately)."""
        channel_id = await self.config.briefing_channel_id()
        if not channel_id:
            await ctx.send("❌ No briefing channel configured. Use `[p]briefing channel #channel` first.")
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
        
        await ctx.send(f"Fetching statistics from database...")
        
        try:
            member_stats = await self._get_member_stats()
            building_stats = await self._get_building_stats()
            training_stats = await self._get_training_stats()
            
            await ctx.send(f"**Stats:**\n"
                          f"New members: {member_stats['new']}, Left: {member_stats['left']}\n"
                          f"Buildings built: {building_stats['built_today']}\n"
                          f"Trainings: {training_stats['completed_today']}")
            
            await ctx.send(f"Posting briefing to {channel.mention}...")
            await self._post_daily_briefing()
            await ctx.send("✅ Daily briefing posted!")
            
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")
            import traceback
            await ctx.send(f"```\n{traceback.format_exc()[:1900]}\n```")

    @briefing_group.command(name="restart")
    async def restart_scheduler(self, ctx: commands.Context):
        """Restart the scheduler task."""
        if self._task:
            self._task.cancel()
        await self._start_task()
        await ctx.send("Scheduler restarted!")


async def setup(bot: Red):
    """Load the DailyBriefing cog."""
    cog = DailyBriefing(bot)
    await bot.add_cog(cog)
