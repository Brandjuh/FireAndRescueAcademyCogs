"""
Scheduler for AllianceReports
Handles time-based report generation with timezone support.
"""

import asyncio
import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

log = logging.getLogger("red.FARA.AllianceReports.Scheduler")


class ReportScheduler:
    """Manages scheduled report generation."""
    
    def __init__(self, bot, config_manager):
        """Initialize scheduler."""
        self.bot = bot
        self.config_manager = config_manager
        self._daily_task = None
        self._monthly_task = None
        self._running = False
    
    async def start(self):
        """Start all scheduled tasks."""
        if self._running:
            log.warning("Scheduler already running")
            return
        
        self._running = True
        self._daily_task = asyncio.create_task(self._daily_loop())
        self._monthly_task = asyncio.create_task(self._monthly_loop())
        log.info("Report scheduler started")
    
    async def stop(self):
        """Stop all scheduled tasks."""
        self._running = False
        
        tasks = [self._daily_task, self._monthly_task]
        for task in tasks:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        log.info("Report scheduler stopped")
    
    async def _daily_loop(self):
        """Background loop for daily reports."""
        await self.bot.wait_until_ready()
        log.info("Daily report loop started")
        
        while self._running:
            try:
                next_run = await self._calculate_next_daily()
                
                if next_run is None:
                    log.error("Could not calculate next daily run time")
                    await asyncio.sleep(3600)
                    continue
                
                now = datetime.now(ZoneInfo("UTC"))
                wait_seconds = (next_run - now).total_seconds()
                
                if wait_seconds > 0:
                    log.info(f"Next daily report in {wait_seconds / 3600:.2f} hours")
                    await asyncio.sleep(wait_seconds)
                
                if self._running:
                    await self._execute_daily_reports()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception(f"Error in daily loop: {e}")
                await asyncio.sleep(300)
    
    async def _monthly_loop(self):
        """Background loop for monthly reports."""
        await self.bot.wait_until_ready()
        log.info("Monthly report loop started")
        
        while self._running:
            try:
                next_run = await self._calculate_next_monthly()
                
                if next_run is None:
                    log.error("Could not calculate next monthly run time")
                    await asyncio.sleep(86400)
                    continue
                
                now = datetime.now(ZoneInfo("UTC"))
                wait_seconds = (next_run - now).total_seconds()
                
                if wait_seconds > 0:
                    log.info(f"Next monthly report in {wait_seconds / 86400:.2f} days")
                    await asyncio.sleep(wait_seconds)
                
                if self._running:
                    await self._execute_monthly_reports()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception(f"Error in monthly loop: {e}")
                await asyncio.sleep(3600)
    
    async def _calculate_next_daily(self):
        """Calculate next daily report time."""
        try:
            time_str = await self.config_manager.config.daily_time()
            tz_str = await self.config_manager.config.timezone()
            
            hour, minute = map(int, time_str.split(":"))
            target_time = time(hour=hour, minute=minute)
            
            tz = ZoneInfo(tz_str)
            now_tz = datetime.now(tz)
            today_target = datetime.combine(now_tz.date(), target_time, tzinfo=tz)
            
            if now_tz >= today_target:
                next_run = today_target + timedelta(days=1)
            else:
                next_run = today_target
            
            return next_run.astimezone(ZoneInfo("UTC"))
        except Exception as e:
            log.exception(f"Error calculating next daily time: {e}")
            return None
    
    async def _calculate_next_monthly(self):
        """Calculate next monthly report time."""
        try:
            day = await self.config_manager.config.monthly_day()
            time_str = await self.config_manager.config.monthly_time()
            tz_str = await self.config_manager.config.timezone()
            
            hour, minute = map(int, time_str.split(":"))
            target_time = time(hour=hour, minute=minute)
            
            tz = ZoneInfo(tz_str)
            now_tz = datetime.now(tz)
            
            try:
                this_month = datetime(
                    year=now_tz.year,
                    month=now_tz.month,
                    day=day,
                    hour=target_time.hour,
                    minute=target_time.minute,
                    tzinfo=tz
                )
                
                if now_tz >= this_month:
                    if now_tz.month == 12:
                        next_run = datetime(now_tz.year + 1, 1, day, target_time.hour, target_time.minute, tzinfo=tz)
                    else:
                        next_run = datetime(now_tz.year, now_tz.month + 1, day, target_time.hour, target_time.minute, tzinfo=tz)
                else:
                    next_run = this_month
            except ValueError:
                if now_tz.month == 12:
                    next_run = datetime(now_tz.year + 1, 1, day, target_time.hour, target_time.minute, tzinfo=tz)
                else:
                    next_run = datetime(now_tz.year, now_tz.month + 1, day, target_time.hour, target_time.minute, tzinfo=tz)
            
            return next_run.astimezone(ZoneInfo("UTC"))
        except Exception as e:
            log.exception(f"Error calculating next monthly time: {e}")
            return None
    
    async def _execute_daily_reports(self):
        """Execute daily reports."""
        log.info("Executing daily reports (not yet implemented)")
    
    async def _execute_monthly_reports(self):
        """Execute monthly reports."""
        log.info("Executing monthly reports (not yet implemented)")
    
    def is_running(self):
        """Check if scheduler is running."""
        return self._running
    
    async def get_next_run_times(self):
        """Get next run times."""
        return {
            "daily": await self._calculate_next_daily(),
            "monthly": await self._calculate_next_monthly(),
        }
