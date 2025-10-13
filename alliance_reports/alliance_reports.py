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
        
        # Initialize managers
        self.config_manager = ConfigManager(self.config, bot)
        self.scheduler = ReportScheduler(bot, self.config_manager)
        
        # State
        self._ready = False
    
    async def cog_load(self):
        """Called when cog is loaded."""
        log.info(f"AllianceReports v{__version__} loading...")
        
        # Detect database paths
        db_paths = await self.config_manager.detect_database_paths()
        if db_paths:
            log.info(f"Detected {len(db_paths)} database(s)")
            
            # Save detected paths if not manually configured
            for key, path in db_paths.items():
                current = await self.config.get_raw(key, default=None)
                if not current and path:
                    await self.config.set_raw(key, value=str(path))
                    log.info(f"Saved database path: {key}")
        else:
            log.warning("No databases detected - reports may not work")
        
        # Start scheduler
        asyncio.create_task(self._delayed_start())
        
        self._ready = True
        log.info("AllianceReports loaded successfully")
    
    async def _delayed_start(self):
        """Start scheduler after bot is ready."""
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)  # Give other cogs time to load
        await self.scheduler.start()
    
    async def cog_unload(self):
        """Called when cog is unloaded."""
        log.info("AllianceReports unloading...")
        await self.scheduler.stop()
        log.info("AllianceReports unloaded")
    
    # ==================== PERMISSION CHECKS ====================
    
    async def _is_authorized(self, ctx: commands.Context) -> bool:
        """
        Check if user is authorized to use reportset commands.
        
        Args:
            ctx: Command context
        
        Returns:
            True if authorized (bot owner or has admin role)
        """
        # Bot owner always authorized
        if await self.bot.is_owner(ctx.author):
            return True
        
        # Check for configured admin role
        if not ctx.guild:
            return False
        
        admin_role_id = await self.config.admin_role_id()
        if not admin_role_id:
            return False
        
        admin_role = ctx.guild.get_role(int(admin_role_id))
        if not admin_role:
            return False
        
        return admin_role in ctx.author.roles
    
    # ==================== COMMAND GROUP ====================
    
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
            
            # Add scheduler status
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
            
            # Send in pages if too long
            for page in pagify(full_output, delims=["\n"], page_length=1900):
                await ctx.send(box(page, lang="ini"))
        
        except Exception as e:
            log.exception(f"Error showing status: {e}")
            await ctx.send(f"❌ Error retrieving status: {e}")
    
    # ==================== CHANNEL CONFIGURATION ====================
    
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
    
    # ==================== TIMING CONFIGURATION ====================
    
    @reportset.group(name="time")
    async def reportset_time(self, ctx: commands.Context):
        """Configure report generation times."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @reportset_time.command(name="daily")
    async def time_daily(self, ctx: commands.Context, time_str: str):
        """
        Set daily report generation time (HH:MM format, 24-hour).
        
        Example: [p]reportset time daily 06:00
        """
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        if not await self.config_manager.validate_time_format(time_str):
            await ctx.send("❌ Invalid time format. Use HH:MM (24-hour), e.g., 06:00")
            return
        
        await self.config.daily_time.set(time_str)
        tz = await self.config.timezone()
        await ctx.send(f"✅ Daily reports will generate at {time_str} {tz}")
        
        # Restart scheduler to pick up new time
        await self.scheduler.stop()
        await asyncio.sleep(1)
        await self.scheduler.start()
    
    @reportset_time.command(name="monthly")
    async def time_monthly(self, ctx: commands.Context, day: int, time_str: str):
        """
        Set monthly report generation day and time.
        
        Example: [p]reportset time monthly 1 06:00
        """
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
        
        # Restart scheduler
        await self.scheduler.stop()
        await asyncio.sleep(1)
        await self.scheduler.start()
    
    @reportset_time.command(name="timezone")
    async def time_timezone(self, ctx: commands.Context, timezone: str):
        """
        Set timezone for report generation.
        
        Example: [p]reportset time timezone Europe/Amsterdam
        """
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        try:
            # Validate timezone
            ZoneInfo(timezone)
            await self.config.timezone.set(timezone)
            await ctx.send(f"✅ Timezone set to {timezone}")
            
            # Restart scheduler
            await self.scheduler.stop()
            await asyncio.sleep(1)
            await self.scheduler.start()
        except Exception as e:
            await ctx.send(f"❌ Invalid timezone: {e}\nSee: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones")
    
    # ==================== ENABLE/DISABLE REPORTS ====================
    
    @reportset.group(name="enable")
    async def reportset_enable(self, ctx: commands.Context):
        """Enable specific reports."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @reportset_enable.command(name="dailymember")
    async def enable_daily_member(self, ctx: commands.Context):
        """Enable daily member reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.daily_member_enabled.set(True)
        await ctx.send("✅ Daily member reports enabled")
    
    @reportset_enable.command(name="dailyadmin")
    async def enable_daily_admin(self, ctx: commands.Context):
        """Enable daily admin reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.daily_admin_enabled.set(True)
        await ctx.send("✅ Daily admin reports enabled")
    
    @reportset_enable.command(name="monthlymember")
    async def enable_monthly_member(self, ctx: commands.Context):
        """Enable monthly member reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.monthly_member_enabled.set(True)
        await ctx.send("✅ Monthly member reports enabled")
    
    @reportset_enable.command(name="monthlyadmin")
    async def enable_monthly_admin(self, ctx: commands.Context):
        """Enable monthly admin reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.monthly_admin_enabled.set(True)
        await ctx.send("✅ Monthly admin reports enabled")
    
    @reportset.group(name="disable")
    async def reportset_disable(self, ctx: commands.Context):
        """Disable specific reports."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @reportset_disable.command(name="dailymember")
    async def disable_daily_member(self, ctx: commands.Context):
        """Disable daily member reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.daily_member_enabled.set(False)
        await ctx.send("✅ Daily member reports disabled")
    
    @reportset_disable.command(name="dailyadmin")
    async def disable_daily_admin(self, ctx: commands.Context):
        """Disable daily admin reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.daily_admin_enabled.set(False)
        await ctx.send("✅ Daily admin reports disabled")
    
    @reportset_disable.command(name="monthlymember")
    async def disable_monthly_member(self, ctx: commands.Context):
        """Disable monthly member reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.monthly_member_enabled.set(False)
        await ctx.send("✅ Monthly member reports disabled")
    
    @reportset_disable.command(name="monthlyadmin")
    async def disable_monthly_admin(self, ctx: commands.Context):
        """Disable monthly admin reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.monthly_admin_enabled.set(False)
        await ctx.send("✅ Monthly admin reports disabled")
    
    # ==================== ADMIN ROLE ====================
    
    @reportset.command(name="adminrole")
    async def reportset_adminrole(self, ctx: commands.Context, role: discord.Role):
        """Set the admin role that can configure reports."""
        if not await self.bot.is_owner(ctx.author):
            await ctx.send("❌ Only the bot owner can set the admin role.")
            return
        
        await self.config.admin_role_id.set(role.id)
        await ctx.send(f"✅ Admin role set to {role.mention}")
    
    # ==================== TEST MODE ====================
    
    @reportset.command(name="testmode")
    async def reportset_testmode(self, ctx: commands.Context, enabled: bool):
        """
        Enable/disable test mode.
        
        In test mode, reports are generated but not posted to channels.
        """
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await self.config.test_mode.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"✅ Test mode {status}")
    
    # ==================== MANUAL REPORT TRIGGERS ====================
    
    @commands.group(name="report")
    async def report_group(self, ctx: commands.Context):
        """Manually trigger report generation."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @report_group.group(name="daily")
    async def report_daily(self, ctx: commands.Context):
        """Manually trigger daily reports."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @report_daily.command(name="member")
    async def report_daily_member(self, ctx: commands.Context):
        """Generate daily member report now."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.send("🔄 Generating daily member report...")
        
        try:
            # TODO: Implement in Phase 2
            await ctx.send("✅ Daily member report generation triggered (not yet implemented)")
        except Exception as e:
            log.exception(f"Error generating daily member report: {e}")
            await ctx.send(f"❌ Error: {e}")
    
    @report_daily.command(name="admin")
    async def report_daily_admin(self, ctx: commands.Context):
        """Generate daily admin report now."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.send("🔄 Generating daily admin report...")
        
        try:
            # TODO: Implement in Phase 3
            await ctx.send("✅ Daily admin report generation triggered (not yet implemented)")
        except Exception as e:
            log.exception(f"Error generating daily admin report: {e}")
            await ctx.send(f"❌ Error: {e}")
    
    @report_group.group(name="monthly")
    async def report_monthly(self, ctx: commands.Context):
        """Manually trigger monthly reports."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @report_monthly.command(name="member")
    async def report_monthly_member(self, ctx: commands.Context):
        """Generate monthly member report now."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.send("🔄 Generating monthly member report...")
        
        try:
            # TODO: Implement in Phase 4
            await ctx.send("✅ Monthly member report generation triggered (not yet implemented)")
        except Exception as e:
            log.exception(f"Error generating monthly member report: {e}")
            await ctx.send(f"❌ Error: {e}")
    
    @report_monthly.command(name="admin")
    async def report_monthly_admin(self, ctx: commands.Context):
        """Generate monthly admin report now."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.send("🔄 Generating monthly admin report...")
        
        try:
            # TODO: Implement in Phase 5
            await ctx.send("✅ Monthly admin report generation triggered (not yet implemented)")
        except Exception as e:
            log.exception(f"Error generating monthly admin report: {e}")
            await ctx.send(f"❌ Error: {e}")
    
    @report_group.command(name="test")
    async def report_test(self, ctx: commands.Context):
        """Generate all reports in test mode (no posting)."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.send("🔄 Generating all reports in test mode...")
        
        try:
            # Save current test mode state
            original_test_mode = await self.config.test_mode()
            
            # Enable test mode
            await self.config.test_mode.set(True)
            
            # TODO: Generate all reports
            await ctx.send("✅ Test report generation complete (not yet implemented)")
            
            # Restore original test mode
            await self.config.test_mode.set(original_test_mode)
        
        except Exception as e:
            log.exception(f"Error in test report generation: {e}")
            await ctx.send(f"❌ Error: {e}")
    
    # ==================== DATABASE MANAGEMENT ====================
    
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
            # Clear cache
            self.config_manager._db_cache = {}
            
            # Re-detect
            db_paths = await self.config_manager.detect_database_paths()
            
            if not db_paths:
                await ctx.send("❌ No databases found")
                return
            
            # Save detected paths
            found = 0
            for key, path in db_paths.items():
                if path:
                    await self.config.set_raw(key, value=str(path))
                    found += 1
            
            await ctx.send(f"✅ Found {found}/{len(db_paths)} databases")
            
            # Show status
            await self.reportset_status(ctx)
        
        except Exception as e:
            log.exception(f"Error detecting databases: {e}")
            await ctx.send(f"❌ Error: {e}")
    
    # ==================== RESET ====================
    
    @reportset.command(name="reset")
    async def reportset_reset(self, ctx: commands.Context):
        """Reset all settings to defaults."""
        if not await self.bot.is_owner(ctx.author):
            await ctx.send("❌ Only the bot owner can reset settings.")
            return
        
        await ctx.send("⚠️ This will reset ALL settings to defaults. Type `yes` to confirm.")
        
        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel
        
        try:
            msg = await self.bot.wait_for("message", check=check, timeout=30.0)
            if msg.content.lower() != "yes":
                await ctx.send("❌ Reset cancelled")
                return
        except asyncio.TimeoutError:
            await ctx.send("❌ Reset cancelled (timeout)")
            return
        
        try:
            await self.config_manager.reset_to_defaults()
            
            # Restart scheduler
            await self.scheduler.stop()
            await asyncio.sleep(1)
            await self.scheduler.start()
            
            await ctx.send("✅ Settings reset to defaults. Scheduler restarted.")
        except Exception as e:
            log.exception(f"Error resetting settings: {e}")
            await ctx.send(f"❌ Error: {e}")
    
    # ==================== VERSION ====================
    
    @reportset.command(name="version")
    async def reportset_version(self, ctx: commands.Context):
        """Show cog version."""
        await ctx.send(f"**AllianceReports** version `{__version__}`")
