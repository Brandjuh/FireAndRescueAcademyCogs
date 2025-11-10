"""
AllianceReports - Main Cog (V2 + Last Day of Month Support)
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
    - V2 Scraper Databases (members_v2, logs_v2, income_v2, buildings_v2)
    - AllianceScraper (treasury only)
    - MemberSync (verifications)
    - BuildingManager (building requests)
    - SanctionsManager (discipline)
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
        
        # Detect database paths
        db_paths = await self.config_manager.detect_database_paths()
        if db_paths:
            log.info(f"Detected {len(db_paths)} database(s)")
            
            # Save detected paths to config
            for key, path in db_paths.items():
                current = await self.config.get_raw(key, default=None)
                if not current and path:
                    await self.config.set_raw(key, value=str(path))
                    log.info(f"Saved database path: {key}")
        else:
            log.warning("No databases detected - reports may not work")
        
        # Start scheduler after a delay
        asyncio.create_task(self._delayed_start())
        
        self._ready = True
        log.info("AllianceReports loaded successfully")
    
    async def _delayed_start(self):
        """Start scheduler after bot is ready."""
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)  # Wait 5 seconds after bot ready
        await self.scheduler.start()
    
    async def cog_unload(self):
        """Called when cog is unloaded."""
        log.info("AllianceReports unloading...")
        await self.scheduler.stop()
        log.info("AllianceReports unloaded")
    
    async def _is_authorized(self, ctx: commands.Context) -> bool:
        """Check if user is authorized to use reportset commands."""
        # Bot owner always authorized
        if await self.bot.is_owner(ctx.author):
            return True
        
        # Check admin role
        if not ctx.guild:
            return False
        
        admin_role_id = await self.config.admin_role_id()
        if not admin_role_id:
            return False
        
        admin_role = ctx.guild.get_role(int(admin_role_id))
        if not admin_role:
            return False
        
        return admin_role in ctx.author.roles
    
    # ==================== REPORTSET COMMANDS ====================
    
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
                daily_str = next_runs["daily"].strftime("%Y-%m-%d %H:%M:%S %Z")
                scheduler_status += f"  Daily: {daily_str}\n"
            else:
                scheduler_status += f"  Daily: Not scheduled\n"
            
            if next_runs.get("monthly"):
                monthly_str = next_runs["monthly"].strftime("%Y-%m-%d %H:%M:%S %Z")
                scheduler_status += f"  Monthly: {monthly_str}\n"
            else:
                scheduler_status += f"  Monthly: Not scheduled\n"
            
            scheduler_status += f"\n  Scheduler: {'🟢 Running' if self.scheduler.is_running() else '🔴 Stopped'}"
            
            full_output = formatted + "\n" + scheduler_status
            
            # Pagify for long output
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
        """Set daily report generation time (HH:MM format, 24-hour).
        
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
        
        # Restart scheduler to apply changes
        await self.scheduler.stop()
        await asyncio.sleep(1)
        await self.scheduler.start()
    
    @reportset_time.command(name="monthly")
    async def time_monthly(self, ctx: commands.Context, day: int, time_str: str):
        """Set monthly report generation day and time.
        
        Special values:
        - Day 0 or -1: Last day of month (recommended for month-end reports)
        - Day 1-28: Safe for all months
        - Day 29-31: May skip shorter months
        
        Examples:
        [p]reportset time monthly 0 06:00    (last day of every month)
        [p]reportset time monthly 1 06:00    (first day of every month)
        [p]reportset time monthly 15 06:00   (15th of every month)
        """
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        # NEW: Use the validation method from config_manager
        if not self.config_manager.validate_monthly_day(day):
            await ctx.send("❌ Day must be between -1 (or 0 for last day) and 31\n"
                          "💡 Use `0` or `-1` for last day of month (handles leap years!)\n"
                          "💡 Use `1-31` for specific day")
            return
        
        if not await self.config_manager.validate_time_format(time_str):
            await ctx.send("❌ Invalid time format. Use HH:MM (24-hour), e.g., 06:00")
            return
        
        await self.config.monthly_day.set(day)
        await self.config.monthly_time.set(time_str)
        tz = await self.config.timezone()
        
        # User-friendly message
        if day <= 0:
            await ctx.send(f"✅ Monthly reports will generate on the **last day of each month** at {time_str} {tz}\n"
                          f"📅 This handles leap years and varying month lengths automatically!")
        else:
            await ctx.send(f"✅ Monthly reports will generate on day **{day}** at {time_str} {tz}\n"
                          f"⚠️ Note: Day {day} doesn't exist in all months (Feb has 28/29, some months have 30)")
        
        # Restart scheduler
        await self.scheduler.stop()
        await asyncio.sleep(1)
        await self.scheduler.start()
    
    @reportset_time.command(name="timezone")
    async def time_timezone(self, ctx: commands.Context, timezone: str):
        """Set timezone for report generation.
        
        Example: [p]reportset time timezone Europe/Amsterdam
        """
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        try:
            # Test if timezone is valid
            ZoneInfo(timezone)
            await self.config.timezone.set(timezone)
            await ctx.send(f"✅ Timezone set to {timezone}")
            
            # Restart scheduler
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
        """Enable/disable test mode (generate without posting)."""
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
            # Clear cache
            self.config_manager._db_cache = {}
            
            # Detect
            db_paths = await self.config_manager.detect_database_paths()
            
            if not db_paths:
                await ctx.send("❌ No databases found")
                return
            
            # Save to config
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
    
    @reportset.command(name="version")
    async def reportset_version(self, ctx: commands.Context):
        """Show cog version."""
        await ctx.send(f"**AllianceReports** version `{__version__}` - V2 Database Support + Smart Last-Day Scheduling")
    
    # ==================== REPORT COMMANDS ====================
    
    @commands.group(name="report")
    async def report_group(self, ctx: commands.Context):
        """Manually trigger report generation."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @report_group.command(name="dailymember")
    async def report_daily_member(self, ctx: commands.Context):
        """Generate daily member report now."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.send("📄 Generating daily member report...")
        
        try:
            from .templates.daily_member import DailyMemberReport
            
            report_gen = DailyMemberReport(self.bot, self.config_manager)
            embed = await report_gen.generate()
            
            if not embed:
                await ctx.send("❌ Failed to generate report")
                return
            
            channel_id = await self.config.daily_member_channel()
            if not channel_id:
                await ctx.send("ℹ️ No channel configured, posting here:")
                await ctx.send(embed=embed)
                await ctx.send("✅ Set channel with `[p]reportset channel dailymember #channel`")
                return
            
            channel = self.bot.get_channel(int(channel_id))
            if not channel:
                await ctx.send(f"❌ Configured channel not found (ID: {channel_id})")
                return
            
            success = await report_gen.post(channel)
            
            if success:
                await ctx.send(f"✅ Daily member report posted to {channel.mention}")
            else:
                await ctx.send("❌ Failed to post report (check logs)")
        
        except Exception as e:
            log.exception(f"Error generating daily member report: {e}")
            await ctx.send(f"❌ Error: {e}")
    
    @report_group.command(name="dailyadmin")
    async def report_daily_admin(self, ctx: commands.Context):
        """Generate daily admin report now."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.send("📄 Generating daily admin report...")
        
        try:
            from .templates.daily_admin import DailyAdminReport
            
            report_gen = DailyAdminReport(self.bot, self.config_manager)
            embed = await report_gen.generate()
            
            if not embed:
                await ctx.send("❌ Failed to generate report")
                return
            
            channel_id = await self.config.daily_admin_channel()
            if not channel_id:
                await ctx.send("ℹ️ No channel configured, posting here:")
                await ctx.send(embed=embed)
                await ctx.send("✅ Set channel with `[p]reportset channel dailyadmin #channel`")
                return
            
            channel = self.bot.get_channel(int(channel_id))
            if not channel:
                await ctx.send(f"❌ Configured channel not found (ID: {channel_id})")
                return
            
            success = await report_gen.post(channel)
            
            if success:
                await ctx.send(f"✅ Daily admin report posted to {channel.mention}")
            else:
                await ctx.send("❌ Failed to post report (check logs)")
        
        except Exception as e:
            log.exception(f"Error generating daily admin report: {e}")
            await ctx.send(f"❌ Error: {e}")
    
    @report_group.command(name="monthlymember")
    async def report_monthly_member(self, ctx: commands.Context):
        """Generate monthly member report now."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.send("📄 Generating monthly member report...")
        
        try:
            from .templates.monthly_member import MonthlyMemberReport
            
            report_gen = MonthlyMemberReport(self.bot, self.config_manager)
            embeds = await report_gen.generate()
            
            if not embeds:
                await ctx.send("❌ Failed to generate report")
                return
            
            channel_id = await self.config.monthly_member_channel()
            if not channel_id:
                await ctx.send("ℹ️ No channel configured, posting here:")
                for embed in embeds:
                    await ctx.send(embed=embed)
                await ctx.send("✅ Set channel with `[p]reportset channel monthlymember #channel`")
                return
            
            channel = self.bot.get_channel(int(channel_id))
            if not channel:
                await ctx.send(f"❌ Configured channel not found (ID: {channel_id})")
                return
            
            success = await report_gen.post(channel)
            
            if success:
                await ctx.send(f"✅ Monthly member report posted to {channel.mention}")
            else:
                await ctx.send("❌ Failed to post report (check logs)")
        
        except Exception as e:
            log.exception(f"Error generating monthly member report: {e}")
            await ctx.send(f"❌ Error: {e}")
    
    @report_group.command(name="debug")
    async def report_debug(self, ctx: commands.Context):
        """Debug data aggregation."""
        if not await self._is_authorized(ctx):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.send("🔍 Testing V2 database connections...")
        
        try:
            from .data_aggregator import DataAggregator
            
            aggregator = DataAggregator(self.config_manager)
            
            # Test V2 database connections
            await ctx.send("\n🔌 Testing V2 databases...")
            
            conn_members = aggregator._get_db_connection("members_v2")
            if conn_members:
                await ctx.send("✅ Members V2 DB connected")
                conn_members.close()
            else:
                await ctx.send("❌ Members V2 DB connection failed!")
            
            conn_logs = aggregator._get_db_connection("logs_v2")
            if conn_logs:
                await ctx.send("✅ Logs V2 DB connected")
                conn_logs.close()
            else:
                await ctx.send("❌ Logs V2 DB connection failed!")
            
            conn_income = aggregator._get_db_connection("income_v2")
            if conn_income:
                await ctx.send("✅ Income V2 DB connected")
                conn_income.close()
            else:
                await ctx.send("❌ Income V2 DB connection failed!")
            
            conn_buildings = aggregator._get_db_connection("buildings_v2")
            if conn_buildings:
                await ctx.send("✅ Buildings V2 DB connected")
                conn_buildings.close()
            else:
                await ctx.send("❌ Buildings V2 DB connection failed!")
            
            # Test legacy databases
            await ctx.send("\n🔌 Testing legacy databases...")
            
            conn_alliance = aggregator._get_db_connection("alliance")
            if conn_alliance:
                await ctx.send("✅ Alliance DB (treasury) connected")
                conn_alliance.close()
            else:
                await ctx.send("⚠️ Alliance DB connection failed")
            
            await ctx.send("\n✅ Database connection test complete!")
            
        except Exception as e:
            log.exception(f"Error in debug: {e}")
            await ctx.send(f"❌ Error: {str(e)}")


async def setup(bot: Red):
    """Load the AllianceReports cog."""
    await bot.add_cog(AllianceReports(bot))
