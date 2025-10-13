"""
AllianceReports - Main Cog
Comprehensive reporting system for Fire & Rescue Academy alliance data.
"""

import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional

import discord
from redbot.core import commands, checks, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box, pagify

from .config_manager import ConfigManager
from .scheduler import ReportScheduler

log = logging.getLogger("red.FARA.AllianceReports")

__version__ = "1.0.0"


class AllianceReports(commands.Cog):
    """
    Comprehensive reporting system for alliance data.
    
    Generates daily and monthly reports from:
    - AllianceScraper (members, logs, treasury)
    - MemberSync (verifications)
    - BuildingManager (building requests)
    - SanctionsManager (discipline)
    - TrainingManager (training requests)
    """
    
    def __init__(self, bot: Red):
        """Initialize the AllianceReports cog."""
        self.bot = bot
        self.config = Config.get_conf(
            self,
            identifier=0xFA11A9CE00,
            force_registration=True
        )
        self.config.register_global(**ConfigManager.get_defaults())
        
        self.config_manager = ConfigManager(self.config, bot)
        self.scheduler = ReportScheduler(bot, self.config_manager)
        
        self._ready = False
    
    async def cog_load(self):
        """Called when cog is loaded."""
        log.info(f"AllianceReports v{__version__} loading...")
        
        db_paths = await self.config_manager.detect_database_paths()
        if db_paths:
            log.info(f"Detected {len(db_paths)} database(s)")
            
            for key, path in db_paths.items():
                current = await self.config.get_raw(key, default=None)
                if not current and path:
                    await self.config.set_raw(key, value=str(path))
                    log.info(f"Saved database path: {key}")
        else:
            log.warning("No databases detected - reports may not work")
        
        asyncio.create_task(self._delayed_start())
        
        self._ready = True
        log.info("AllianceReports loaded successfully")
    
    async def _delayed_start(self):
        """Start scheduler after bot is ready."""
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)
        await self.scheduler.start()
    
    async def cog_unload(self):
        """Called when cog is unloaded."""
        log.info("AllianceReports unloading...")
        await self.scheduler.stop()
        log.info("AllianceReports unloaded")
    
    async def _is_authorized(self, ctx: commands.Context) -> bool:
        """Check if user is authorized to use reportset commands."""
        if await self.bot.is_owner(ctx.author):
            return True
        
        if not ctx.guild:
            return False
        
        admin_role_id = await self.config.admin_role_id()
        if not admin_role_id:
            return False
        
        admin_role = ctx.guild.get_role(int(admin_role_id))
        if not admin_role:
            return False
        
        return admin_role in ctx.author.roles
    
    @commands.group(name="reportset", aliases=["areportset"])
    async def reportset(self, ctx: commands.Context):
        """Configure AllianceReports settings."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @reportset.command(name="status")
    async def reportset_status(self, ctx: commands.Context):
        """Show current configuration and system status."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        try:
            settings = await self.config_manager.get_all_settings()
            formatted = self.config_manager.format_settings_display(settings)
            
            next_runs = await self.scheduler.get_next_run_times()
            scheduler_status = f"\n⏰ NEXT SCHEDULED RUNS\n"
            
            if next_runs.get("daily"):
                daily_str = next_runs["daily"].strftime("%Y-%m-%d %H:%M:%S UTC")
                scheduler_status += f"  Daily: {daily_str}\n"
            else:
                scheduler_status += f"  Daily: Not scheduled\n"
            
            if next_runs.get("monthly"):
                monthly_str = next_runs["monthly"].strftime("%Y-%m-%d %H:%M:%S UTC")
                scheduler_status += f"  Monthly: {monthly_str}\n"
            else:
                scheduler_status += f"  Monthly: Not scheduled\n"
            
            scheduler_status += f"\n  Scheduler: {'🟢 Running' if self.scheduler.is_running() else '🔴 Stopped'}"
            
            full_output = formatted + "\n" + scheduler_status
            
            for page in pagify(full_output, delims=["\n"], page_length=1900):
                await ctx.send(box(page, lang="ini"))
        
        except Exception as e:
            log.exception(f"Error showing status: {e}")
            await ctx.send(f"❌ Error retrieving status: {e}")
    
    @reportset.group(name="channel")
    async def reportset_channel(self, ctx: commands.Context):
        """Configure report channels."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @reportset_channel.command(name="dailymember")
    async def channel_daily_member(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel for daily member reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.daily_member_channel.set(channel.id)
        await ctx.send(f"✅ Daily member reports will be posted to {channel.mention}")
    
    @reportset_channel.command(name="dailyadmin")
    async def channel_daily_admin(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel for daily admin reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.daily_admin_channel.set(channel.id)
        await ctx.send(f"✅ Daily admin reports will be posted to {channel.mention}")
    
    @reportset_channel.command(name="monthlymember")
    async def channel_monthly_member(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel for monthly member reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.monthly_member_channel.set(channel.id)
        await ctx.send(f"✅ Monthly member reports will be posted to {channel.mention}")
    
    @reportset_channel.command(name="monthlyadmin")
    async def channel_monthly_admin(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel for monthly admin reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.monthly_admin_channel.set(channel.id)
        await ctx.send(f"✅ Monthly admin reports will be posted to {channel.mention}")
    
    @reportset_channel.command(name="error")
    async def channel_error(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel for error notifications."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.error_channel.set(channel.id)
        await ctx.send(f"✅ Error notifications will be sent to {channel.mention}")
    
    @reportset.group(name="time")
    async def reportset_time(self, ctx: commands.Context):
        """Configure report generation times."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @reportset_time.command(name="daily")
    async def time_daily(self, ctx: commands.Context, time_str: str):
        """Set daily report generation time (HH:MM format, 24-hour)."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        if not await self.config_manager.validate_time_format(time_str):
            await ctx.send("❌ Invalid time format. Use HH:MM (24-hour), e.g., 06:00")
            return
        
        await self.config.daily_time.set(time_str)
        tz = await self.config.timezone()
        await ctx.send(f"✅ Daily reports will generate at {time_str} {tz}")
        
        await self.scheduler.stop()
        await asyncio.sleep(1)
        await self.scheduler.start()
    
    @reportset_time.command(name="monthly")
    async def time_monthly(self, ctx: commands.Context, day: int, time_str: str):
        """Set monthly report generation day and time."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        if not 1 <= day <= 31:
            await ctx.send("❌ Day must be between 1 and 31")
            return
        
        if not await self.config_manager.validate_time_format(time_str):
            await ctx.send("❌ Invalid time format. Use HH:MM (24-hour), e.g., 06:00")
            return
        
        await self.config.monthly_day.set(day)
        await self.config.monthly_time.set(time_str)
        tz = await self.config.timezone()
        await ctx.send(f"✅ Monthly reports will generate on day {day} at {time_str} {tz}")
        
        await self.scheduler.stop()
        await asyncio.sleep(1)
        await self.scheduler.start()
    
    @reportset_time.command(name="timezone")
    async def time_timezone(self, ctx: commands.Context, timezone: str):
        """Set timezone for report generation."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        try:
            ZoneInfo(timezone)
            await self.config.timezone.set(timezone)
            await ctx.send(f"✅ Timezone set to {timezone}")
            
            await self.scheduler.stop()
            await asyncio.sleep(1)
            await self.scheduler.start()
        except Exception as e:
            await ctx.send(f"❌ Invalid timezone: {e}")
    
    @reportset.command(name="adminrole")
    async def reportset_adminrole(self, ctx: commands.Context, role: discord.Role):
        """Set the admin role that can configure reports."""
        if not await self.bot.is_owner(ctx.author):
            await ctx.send("❌ Only the bot owner can set the admin role.")
            return
        
        await self.config.admin_role_id.set(role.id)
        await ctx.send(f"✅ Admin role set to {role.mention}")
    
    @reportset.command(name="testmode")
    async def reportset_testmode(self, ctx: commands.Context, enabled: bool):
        """Enable/disable test mode."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.test_mode.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"✅ Test mode {status}")
    
    @reportset.group(name="database")
    async def reportset_database(self, ctx: commands.Context):
        """Manage database paths."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @reportset_database.command(name="detect")
    async def database_detect(self, ctx: commands.Context):
        """Re-detect all database paths."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.send("🔍 Detecting database paths...")
        
        try:
            self.config_manager._db_cache = {}
            db_paths = await self.config_manager.detect_database_paths()
            
            if not db_paths:
                await ctx.send("❌ No databases found")
                return
            
            found = 0
            for key, path in db_paths.items():
                if path:
                    await self.config.set_raw(key, value=str(path))
                    found += 1
            
            await ctx.send(f"✅ Found {found}/{len(db_paths)} databases")
            await self.reportset_status(ctx)
        
        except Exception as e:
            log.exception(f"Error detecting databases: {e}")
            await ctx.send(f"❌ Error: {e}")
    
    @reportset.command(name="version")
    async def reportset_version(self, ctx: commands.Context):
        """Show cog version."""
        await ctx.send(f"**AllianceReports** version `{__version__}`")
    
    @commands.group(name="report")
    async def report_group(self, ctx: commands.Context):
        """Manually trigger report generation."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @report_group.command(name="test")
    async def report_test(self, ctx: commands.Context):
        """Generate all reports in test mode (no posting)."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.send("🔄 Test mode - reports will be generated but not posted...")
        await ctx.send("✅ (Report generation not yet implemented - Phase 2+)")
