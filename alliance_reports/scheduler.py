"""
Report Scheduler - LAST DAY OF MONTH SUPPORT
Handles time-based report generation with timezone support and smart month-end scheduling
"""

import asyncio
import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Dict
import calendar

import discord

log = logging.getLogger("red.FARA.AllianceReports.Scheduler")


class ReportScheduler:
    """Manages scheduled report generation with smart last-day-of-month handling."""
    
    def __init__(self, bot, config_manager):
        """Initialize scheduler."""
        self.bot = bot
        self.config_manager = config_manager
        
        self._daily_task: Optional[asyncio.Task] = None
        self._monthly_task: Optional[asyncio.Task] = None
        self._running = False
    
    def _get_last_day_of_month(self, year: int, month: int) -> int:
        """
        Get the last day of a specific month (handles leap years).
        
        Args:
            year: Year (e.g., 2025)
            month: Month (1-12)
        
        Returns:
            Last day of month (28, 29, 30, or 31)
        """
        return calendar.monthrange(year, month)[1]
    
    def _calculate_monthly_target_day(self, config_day: int, now: datetime) -> int:
        """
        Calculate the actual target day for monthly reports.
        
        Args:
            config_day: Configured day (0/-1 = last day, 1-31 = specific day)
            now: Current datetime
        
        Returns:
            Actual day to use for this month
        """
        # Special value: 0 or -1 means "last day of month"
        if config_day <= 0:
            return self._get_last_day_of_month(now.year, now.month)
        
        # Regular day: ensure it exists in this month
        last_day = self._get_last_day_of_month(now.year, now.month)
        return min(config_day, last_day)
    
    async def start(self):
        """Start the scheduler."""
        if self._running:
            log.warning("Scheduler already running")
            return
        
        log.info("Starting report scheduler...")
        self._running = True
        
        # Start daily and monthly loops
        self._daily_task = asyncio.create_task(self._daily_loop())
        self._monthly_task = asyncio.create_task(self._monthly_loop())
        
        log.info("Report scheduler started")
    
    async def stop(self):
        """Stop the scheduler."""
        if not self._running:
            return
        
        log.info("Stopping report scheduler...")
        self._running = False
        
        # Cancel tasks
        if self._daily_task and not self._daily_task.done():
            self._daily_task.cancel()
            try:
                await self._daily_task
            except asyncio.CancelledError:
                pass
        
        if self._monthly_task and not self._monthly_task.done():
            self._monthly_task.cancel()
            try:
                await self._monthly_task
            except asyncio.CancelledError:
                pass
        
        log.info("Report scheduler stopped")
    
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running
    
    async def get_next_run_times(self) -> Dict[str, datetime]:
        """Get next scheduled run times."""
        try:
            tz_str = await self.config_manager.config.timezone()
            tz = ZoneInfo(tz_str)
            now = datetime.now(tz)
            
            # Daily
            daily_time_str = await self.config_manager.config.daily_time()
            daily_hour, daily_minute = map(int, daily_time_str.split(":"))
            daily_target = now.replace(hour=daily_hour, minute=daily_minute, second=0, microsecond=0)
            if daily_target <= now:
                daily_target += timedelta(days=1)
            
            # Monthly (with last-day-of-month support)
            config_day = await self.config_manager.config.monthly_day()
            monthly_time_str = await self.config_manager.config.monthly_time()
            monthly_hour, monthly_minute = map(int, monthly_time_str.split(":"))
            
            # Calculate actual day for this month
            target_day = self._calculate_monthly_target_day(config_day, now)
            
            monthly_target = now.replace(
                day=target_day,
                hour=monthly_hour,
                minute=monthly_minute,
                second=0,
                microsecond=0
            )
            
            # If this month's date passed, go to next month
            if monthly_target <= now:
                # Move to next month
                if now.month == 12:
                    next_month_date = datetime(now.year + 1, 1, 1, tzinfo=tz)
                else:
                    next_month_date = datetime(now.year, now.month + 1, 1, tzinfo=tz)
                
                # Calculate target day for next month
                next_target_day = self._calculate_monthly_target_day(config_day, next_month_date)
                
                monthly_target = next_month_date.replace(
                    day=next_target_day,
                    hour=monthly_hour,
                    minute=monthly_minute
                )
            
            return {
                "daily": daily_target,
                "monthly": monthly_target
            }
        
        except Exception as e:
            log.exception(f"Error calculating next run times: {e}")
            return {}
    
    async def _daily_loop(self):
        """Daily report generation loop."""
        await self.bot.wait_until_ready()
        
        log.info("Daily report loop started")
        
        while self._running:
            try:
                # Get config
                tz_str = await self.config_manager.config.timezone()
                tz = ZoneInfo(tz_str)
                now = datetime.now(tz)
                
                # Get target time
                time_str = await self.config_manager.config.daily_time()
                target_hour, target_minute = map(int, time_str.split(":"))
                
                # Calculate next run
                target = now.replace(
                    hour=target_hour,
                    minute=target_minute,
                    second=0,
                    microsecond=0
                )
                
                # If target is in the past, schedule for tomorrow
                if target <= now:
                    target += timedelta(days=1)
                
                # Calculate sleep time
                sleep_seconds = (target - now).total_seconds()
                
                log.info(f"Next daily report: {target.strftime('%Y-%m-%d %H:%M:%S %Z')} (in {sleep_seconds:.0f}s)")
                
                # Sleep until target time
                await asyncio.sleep(sleep_seconds)
                
                # Execute reports
                if self._running:
                    await self._execute_daily_reports()
            
            except asyncio.CancelledError:
                log.info("Daily report loop cancelled")
                break
            except Exception as e:
                log.exception(f"Error in daily report loop: {e}")
                # Sleep 5 minutes before retry
                await asyncio.sleep(300)
        
        log.info("Daily report loop stopped")
    
    async def _monthly_loop(self):
        """Monthly report generation loop with last-day-of-month support."""
        await self.bot.wait_until_ready()
        
        log.info("Monthly report loop started")
        
        while self._running:
            try:
                # Get config
                tz_str = await self.config_manager.config.timezone()
                tz = ZoneInfo(tz_str)
                now = datetime.now(tz)
                
                # Get target day and time
                config_day = await self.config_manager.config.monthly_day()
                time_str = await self.config_manager.config.monthly_time()
                target_hour, target_minute = map(int, time_str.split(":"))
                
                # Calculate actual target day for THIS month
                target_day = self._calculate_monthly_target_day(config_day, now)
                
                log.info(f"Monthly config: day={config_day}, actual_day={target_day} for {now.strftime('%B %Y')}")
                
                # Calculate next run
                try:
                    target = now.replace(
                        day=target_day,
                        hour=target_hour,
                        minute=target_minute,
                        second=0,
                        microsecond=0
                    )
                except ValueError as e:
                    # Day doesn't exist in this month
                    log.warning(f"Day {target_day} doesn't exist in {now.strftime('%B %Y')}: {e}")
                    target = now.replace(day=1, hour=target_hour, minute=target_minute, second=0, microsecond=0)
                    # Move to next month
                    if now.month == 12:
                        target = target.replace(year=now.year + 1, month=1)
                    else:
                        target = target.replace(month=now.month + 1)
                    
                    # Recalculate target day for next month
                    next_target_day = self._calculate_monthly_target_day(config_day, target)
                    target = target.replace(day=next_target_day)
                
                # If target is in the past, schedule for next month
                if target <= now:
                    # Move to next month
                    if now.month == 12:
                        next_month_date = datetime(now.year + 1, 1, 1, tzinfo=tz)
                    else:
                        next_month_date = datetime(now.year, now.month + 1, 1, tzinfo=tz)
                    
                    # Calculate target day for next month
                    next_target_day = self._calculate_monthly_target_day(config_day, next_month_date)
                    
                    target = next_month_date.replace(
                        day=next_target_day,
                        hour=target_hour,
                        minute=target_minute
                    )
                
                # Calculate sleep time
                sleep_seconds = (target - now).total_seconds()
                
                # Special message for last-day-of-month
                if config_day <= 0:
                    log.info(f"Next monthly report (LAST DAY): {target.strftime('%Y-%m-%d %H:%M:%S %Z')} (in {sleep_seconds:.0f}s)")
                else:
                    log.info(f"Next monthly report: {target.strftime('%Y-%m-%d %H:%M:%S %Z')} (in {sleep_seconds:.0f}s)")
                
                # Sleep until target time
                await asyncio.sleep(sleep_seconds)
                
                # Execute reports
                if self._running:
                    await self._execute_monthly_reports()
            
            except asyncio.CancelledError:
                log.info("Monthly report loop cancelled")
                break
            except Exception as e:
                log.exception(f"Error in monthly report loop: {e}")
                # Sleep 1 hour before retry
                await asyncio.sleep(3600)
        
        log.info("Monthly report loop stopped")
    
    async def _execute_daily_reports(self):
        """Execute daily report generation."""
        log.info("Executing daily reports...")
        
        try:
            # Get channel IDs
            member_channel_id = await self.config_manager.config.daily_member_channel()
            admin_channel_id = await self.config_manager.config.daily_admin_channel()
            error_channel_id = await self.config_manager.config.error_channel()
            
            test_mode = await self.config_manager.config.test_mode()
            
            # Daily Member Report
            if member_channel_id and await self.config_manager.config.daily_member_enabled():
                try:
                    from .templates.daily_member import DailyMemberReport
                    
                    report_gen = DailyMemberReport(self.bot, self.config_manager)
                    
                    if test_mode:
                        embed = await report_gen.generate()
                        if embed:
                            log.info("Daily member report generated (test mode - not posted)")
                        else:
                            log.error("Failed to generate daily member report")
                    else:
                        channel = self.bot.get_channel(int(member_channel_id))
                        if channel:
                            success = await report_gen.post(channel)
                            if success:
                                log.info("Daily member report posted successfully")
                            else:
                                log.error("Failed to post daily member report")
                                await self._send_error_notification(
                                    error_channel_id,
                                    "Failed to post daily member report"
                                )
                        else:
                            log.error(f"Daily member channel not found: {member_channel_id}")
                
                except Exception as e:
                    log.exception(f"Error generating daily member report: {e}")
                    await self._send_error_notification(
                        error_channel_id,
                        f"Error in daily member report: {e}"
                    )
            
            # Daily Admin Report
            if admin_channel_id and await self.config_manager.config.daily_admin_enabled():
                try:
                    from .templates.daily_admin import DailyAdminReport
                    
                    report_gen = DailyAdminReport(self.bot, self.config_manager)
                    
                    if test_mode:
                        embed = await report_gen.generate()
                        if embed:
                            log.info("Daily admin report generated (test mode - not posted)")
                        else:
                            log.error("Failed to generate daily admin report")
                    else:
                        channel = self.bot.get_channel(int(admin_channel_id))
                        if channel:
                            success = await report_gen.post(channel)
                            if success:
                                log.info("Daily admin report posted successfully")
                            else:
                                log.error("Failed to post daily admin report")
                                await self._send_error_notification(
                                    error_channel_id,
                                    "Failed to post daily admin report"
                                )
                        else:
                            log.error(f"Daily admin channel not found: {admin_channel_id}")
                
                except Exception as e:
                    log.exception(f"Error generating daily admin report: {e}")
                    await self._send_error_notification(
                        error_channel_id,
                        f"Error in daily admin report: {e}"
                    )
            
            log.info("Daily reports execution completed")
        
        except Exception as e:
            log.exception(f"Critical error in daily reports execution: {e}")
            error_channel_id = await self.config_manager.config.error_channel()
            await self._send_error_notification(
                error_channel_id,
                f"Critical error in daily reports: {e}"
            )
    
    async def _execute_monthly_reports(self):
        """Execute monthly report generation."""
        log.info("Executing monthly reports...")
        
        try:
            # Get channel IDs
            member_channel_id = await self.config_manager.config.monthly_member_channel()
            admin_channel_id = await self.config_manager.config.monthly_admin_channel()
            error_channel_id = await self.config_manager.config.error_channel()
            
            test_mode = await self.config_manager.config.test_mode()
            
            # Monthly Member Report
            if member_channel_id and await self.config_manager.config.monthly_member_enabled():
                try:
                    from .templates.monthly_member import MonthlyMemberReport
                    
                    report_gen = MonthlyMemberReport(self.bot, self.config_manager)
                    
                    if test_mode:
                        embeds = await report_gen.generate()
                        if embeds:
                            log.info(f"Monthly member report generated ({len(embeds)} embeds, test mode - not posted)")
                        else:
                            log.error("Failed to generate monthly member report")
                    else:
                        channel = self.bot.get_channel(int(member_channel_id))
                        if channel:
                            success = await report_gen.post(channel)
                            if success:
                                log.info("Monthly member report posted successfully")
                            else:
                                log.error("Failed to post monthly member report")
                                await self._send_error_notification(
                                    error_channel_id,
                                    "Failed to post monthly member report"
                                )
                        else:
                            log.error(f"Monthly member channel not found: {member_channel_id}")
                
                except Exception as e:
                    log.exception(f"Error generating monthly member report: {e}")
                    await self._send_error_notification(
                        error_channel_id,
                        f"Error in monthly member report: {e}"
                    )
            
            # Monthly Admin Report
            if admin_channel_id and await self.config_manager.config.monthly_admin_enabled():
                try:
                    from .templates.monthly_admin import MonthlyAdminReport
                    
                    report_gen = MonthlyAdminReport(self.bot, self.config_manager)
                    
                    if test_mode:
                        embeds = await report_gen.generate()
                        if embeds:
                            log.info(f"Monthly admin report generated ({len(embeds)} embeds, test mode - not posted)")
                        else:
                            log.error("Failed to generate monthly admin report")
                    else:
                        channel = self.bot.get_channel(int(admin_channel_id))
                        if channel:
                            success = await report_gen.post(channel)
                            if success:
                                log.info("Monthly admin report posted successfully")
                            else:
                                log.error("Failed to post monthly admin report")
                                await self._send_error_notification(
                                    error_channel_id,
                                    "Failed to post monthly admin report"
                                )
                        else:
                            log.error(f"Monthly admin channel not found: {admin_channel_id}")
                
                except Exception as e:
                    log.exception(f"Error generating monthly admin report: {e}")
                    await self._send_error_notification(
                        error_channel_id,
                        f"Error in monthly admin report: {e}"
                    )
            
            log.info("Monthly reports execution completed")
        
        except Exception as e:
            log.exception(f"Critical error in monthly reports execution: {e}")
            error_channel_id = await self.config_manager.config.error_channel()
            await self._send_error_notification(
                error_channel_id,
                f"Critical error in monthly reports: {e}"
            )
    
    async def _send_error_notification(self, error_channel_id: Optional[int], message: str):
        """Send error notification to configured error channel."""
        if not error_channel_id:
            log.warning("No error channel configured")
            return
        
        try:
            channel = self.bot.get_channel(int(error_channel_id))
            if not channel:
                log.error(f"Error channel not found: {error_channel_id}")
                return
            
            embed = discord.Embed(
                title="⚠️ Report Generation Error",
                description=message,
                color=discord.Color.red(),
                timestamp=datetime.now()
            )
            
            await channel.send(embed=embed)
            log.info(f"Error notification sent to channel {error_channel_id}")
        
        except Exception as e:
            log.exception(f"Failed to send error notification: {e}")
