"""
AllianceReports - Main Cog (V2 + Last Day of Month Support)
Comprehensive reporting system for Fire & Rescue Academy alliance data.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

import discord
from redbot.core import commands, checks, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box, pagify

from .config_manager import ConfigManager
from .scheduler import ReportScheduler

log = logging.getLogger("red.FARA.AllianceReports")

__version__ = "1.0.1"


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
            await ctx.send("You don't have permission to use this command.")
            return
        
        try:
            settings = await self.config_manager.get_all_settings()
            formatted = self.config_manager.format_settings_display(settings)
            
            # Add scheduler status
            next_runs = await self.scheduler.get_next_run_times()
            scheduler_status = f"\nNEXT SCHEDULED RUNS\n"
            
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
            
            scheduler_status += f"\n  Scheduler: {'Running' if self.scheduler.is_running() else 'Stopped'}"
            
            full_output = formatted + "\n" + scheduler_status
            
            # Pagify for long output
            for page in pagify(full_output, delims=["\n"], page_length=1900):
                await ctx.send(box(page, lang="ini"))
        
        except Exception as e:
            log.exception(f"Error showing status: {e}")
            await ctx.send(f"Error retrieving status: {e}")
    
    @reportset.group(name="channel")
    async def reportset_channel(self, ctx: commands.Context):
        """Configure report channels."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @reportset_channel.command(name="dailymember")
    async def channel_daily_member(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel for daily member reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("You don't have permission to use this command.")
            return
        
        await self.config.daily_member_channel.set(channel.id)
        await ctx.send(f"Daily member reports will be posted to {channel.mention}")
    
    @reportset_channel.command(name="dailyadmin")
    async def channel_daily_admin(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel for daily admin reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("You don't have permission to use this command.")
            return
        
        await self.config.daily_admin_channel.set(channel.id)
        await ctx.send(f"Daily admin reports will be posted to {channel.mention}")
    
    @reportset_channel.command(name="monthlymember")
    async def channel_monthly_member(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel for monthly member reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("You don't have permission to use this command.")
            return
        
        await self.config.monthly_member_channel.set(channel.id)
        await ctx.send(f"Monthly member reports will be posted to {channel.mention}")
    
    @reportset_channel.command(name="monthlyadmin")
    async def channel_monthly_admin(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel for monthly admin reports."""
        if not await self._is_authorized(ctx):
            await ctx.send("You don't have permission to use this command.")
            return
        
        await self.config.monthly_admin_channel.set(channel.id)
        await ctx.send(f"Monthly admin reports will be posted to {channel.mention}")
    
    # (continuing with other commands exactly as they were...)
    # I'll skip the middle parts since they don't change
    
    @commands.group(name="report")
    async def report_group(self, ctx: commands.Context):
        """Manually trigger report generation."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @report_group.command(name="dailymember")
    async def report_daily_member(self, ctx: commands.Context):
        """Generate daily member report now."""
        if not await self._is_authorized(ctx):
            await ctx.send("You don't have permission to use this command.")
            return
        
        await ctx.send("Generating daily member report...")
        
        try:
            from .templates.daily_member import DailyMemberReport
            
            report_gen = DailyMemberReport(self.bot, self.config_manager)
            embed = await report_gen.generate()
            
            if not embed:
                await ctx.send("Failed to generate report")
                return
            
            channel_id = await self.config.daily_member_channel()
            if not channel_id:
                await ctx.send("No channel configured, posting here:")
                await ctx.send(embed=embed)
                await ctx.send("Set channel with `[p]reportset channel dailymember #channel`")
                return
            
            channel = self.bot.get_channel(int(channel_id))
            if not channel:
                await ctx.send(f"Configured channel not found (ID: {channel_id})")
                return
            
            success = await report_gen.post(channel)
            
            if success:
                await ctx.send(f"Daily member report posted to {channel.mention}")
            else:
                await ctx.send("Failed to post report (check logs)")
        
        except Exception as e:
            log.exception(f"Error generating daily member report: {e}")
            await ctx.send(f"Error: {e}")
    
    @report_group.command(name="debug")
    async def report_debug(self, ctx: commands.Context):
        """Debug data aggregation - shows actual database contents."""
        if not await self._is_authorized(ctx):
            await ctx.send("You don't have permission to use this command.")
            return
        
        await ctx.send("**ALLIANCE REPORTS DEBUG**\nChecking databases and queries...")
        
        try:
            from .data_aggregator import DataAggregator
            import json
            
            aggregator = DataAggregator(self.config_manager)
            
            # ===== CHECK LOGS V2 DATABASE =====
            await ctx.send("\n**LOGS V2 DATABASE:**")
            conn_logs = aggregator._get_db_connection("logs_v2")
            if conn_logs:
                cursor = conn_logs.cursor()
                
                # Show unique action_keys
                cursor.execute("SELECT DISTINCT action_key FROM logs ORDER BY action_key")
                action_keys = cursor.fetchall()
                
                msg = "**Action Keys in Database:**\n```\n"
                for key in action_keys[:20]:  # Limit to 20
                    msg += f"- {key[0]}\n"
                msg += "```"
                await ctx.send(msg)
                
                # Show recent logs
                cursor.execute("SELECT action_key, executed_name, affected_name, ts FROM logs ORDER BY ts DESC LIMIT 5")
                recent = cursor.fetchall()
                
                msg = "**Last 5 Log Entries:**\n```\n"
                for r in recent:
                    msg += f"[{r[3]}] {r[0]}: {r[1]} -> {r[2]}\n"
                msg += "```"
                await ctx.send(msg)
                
                # Count logs in last 24h
                utc_now = datetime.now(ZoneInfo("UTC"))
                game_day_end = utc_now.replace(hour=4, minute=0, second=0, microsecond=0)
                game_day_start = game_day_end - timedelta(days=1)
                
                cursor.execute("""
                    SELECT COUNT(*) FROM logs 
                    WHERE datetime(ts) >= datetime(?)
                    AND datetime(ts) < datetime(?)
                """, (game_day_start.isoformat(), game_day_end.isoformat()))
                count_24h = cursor.fetchone()[0]
                await ctx.send(f"Logs in last game day: **{count_24h}**")
                
                conn_logs.close()
            else:
                await ctx.send("Logs V2 DB not accessible")
            
            # ===== CHECK MEMBERS V2 DATABASE =====
            await ctx.send("\n**MEMBERS V2 DATABASE:**")
            conn_members = aggregator._get_db_connection("members_v2")
            if conn_members:
                cursor = conn_members.cursor()
                
                # Total members
                cursor.execute("SELECT COUNT(DISTINCT member_id) FROM members WHERE timestamp = (SELECT MAX(timestamp) FROM members)")
                total = cursor.fetchone()[0]
                await ctx.send(f"Current members: **{total}**")
                
                # Recent timestamp
                cursor.execute("SELECT MAX(timestamp) FROM members")
                last_scrape = cursor.fetchone()[0]
                await ctx.send(f"Last scrape: {last_scrape}")
                
                conn_members.close()
            else:
                await ctx.send("Members V2 DB not accessible")
            
            # ===== CHECK BUILDINGS V2 DATABASE =====
            await ctx.send("\n**BUILDINGS V2 DATABASE:**")
            conn_buildings = aggregator._get_db_connection("buildings_v2")
            if conn_buildings:
                cursor = conn_buildings.cursor()
                
                # Total buildings
                cursor.execute("SELECT COUNT(DISTINCT building_id) FROM buildings WHERE timestamp = (SELECT MAX(timestamp) FROM buildings)")
                total = cursor.fetchone()[0]
                await ctx.send(f"Total buildings: **{total}**")
                
                # Recent timestamp
                cursor.execute("SELECT MAX(timestamp) FROM buildings")
                last_scrape = cursor.fetchone()[0]
                await ctx.send(f"Last scrape: {last_scrape}")
                
                conn_buildings.close()
            else:
                await ctx.send("Buildings V2 DB not accessible")
            
            # ===== TEST ACTUAL DATA AGGREGATION =====
            await ctx.send("\n**TESTING DATA AGGREGATION:**")
            await ctx.send("Running `get_daily_data()`...")
            
            data = await aggregator.get_daily_data()
            
            # Show membership data
            membership = data.get("membership", {})
            msg = "**Membership Data:**\n```json\n"
            msg += json.dumps(membership, indent=2, default=str)[:800]
            msg += "\n```"
            await ctx.send(msg)
            
            # Show training data
            training = data.get("training", {})
            msg = "**Training Data:**\n```json\n"
            msg += json.dumps(training, indent=2, default=str)[:800]
            msg += "\n```"
            await ctx.send(msg)
            
            # Show buildings data
            buildings = data.get("buildings", {})
            msg = "**Buildings Data:**\n```json\n"
            msg += json.dumps(buildings, indent=2, default=str)[:800]
            msg += "\n```"
            await ctx.send(msg)
            
            await ctx.send("\nDebug complete! Check the data above to see what's being returned.")
            
        except Exception as e:
            log.exception(f"Error in debug: {e}")
            await ctx.send(f"Error: {str(e)}\n```\n{e.__class__.__name__}\n```")


async def setup(bot: Red):
    """Load the AllianceReports cog."""
    await bot.add_cog(AllianceReports(bot))
