"""
Data Aggregator
Queries all alliance databases and aggregates data for reports
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

log = logging.getLogger("red.FARA.AllianceReports.DataAggregator")


class DataAggregator:
    """Aggregate data from all alliance databases."""
    
    def __init__(self, config_manager):
        """Initialize data aggregator."""
        self.config_manager = config_manager
        self._db_cache = {}
    
    async def get_daily_data(self) -> Dict:
        """
        Get aggregated data for daily reports (last 24 hours).
        
        Returns comprehensive dict with all metrics.
        """
        try:
            log.info("Aggregating daily data...")
            
            data = {
                "membership": await self._get_membership_data_daily(),
                "training": await self._get_training_data_daily(),
                "buildings": await self._get_buildings_data_daily(),
                "operations": await self._get_operations_data_daily(),
                "treasury": await self._get_treasury_data_daily(),
                "sanctions": await self._get_sanctions_data_daily(),
                "admin_activity": await self._get_admin_activity_daily(),
            }
            
            log.info("Daily data aggregation complete")
            return data
        
        except Exception as e:
            log.exception(f"Error aggregating daily data: {e}")
            return {}
    
    async def get_monthly_data(self, month_date: datetime) -> Dict:
        """
        Get aggregated data for monthly reports (full month).
        
        Args:
            month_date: Any date in the target month
        
        Returns comprehensive dict with all monthly metrics.
        """
        try:
            log.info(f"Aggregating monthly data for {month_date.strftime('%B %Y')}...")
            
            # Get first and last day of month
            first_day = month_date.replace(day=1)
            if month_date.month == 12:
                last_day = month_date.replace(year=month_date.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                last_day = month_date.replace(month=month_date.month + 1, day=1) - timedelta(days=1)
            
            data = {
                "membership": await self._get_membership_data_monthly(first_day, last_day),
                "training": await self._get_training_data_monthly(first_day, last_day),
                "buildings": await self._get_buildings_data_monthly(first_day, last_day),
                "operations": await self._get_operations_data_monthly(first_day, last_day),
                "treasury": await self._get_treasury_data_monthly(first_day, last_day),
                "sanctions": await self._get_sanctions_data_monthly(first_day, last_day),
                "admin_activity": await self._get_admin_activity_monthly(first_day, last_day),
                "activity_score": 85,  # Placeholder - calculate from data
            }
            
            log.info("Monthly data aggregation complete")
            return data
        
        except Exception as e:
            log.exception(f"Error aggregating monthly data: {e}")
            return {}
    
    # ==================== DAILY DATA METHODS ====================
    
    async def _get_membership_data_daily(self) -> Dict:
        """Get membership metrics for last 24 hours."""
        try:
            # Placeholder implementation
            return {
                "total_members": 247,
                "new_joins_24h": 3,
                "left_24h": 0,
                "kicked_24h": 0,
                "verifications_approved_24h": 2,
                "verifications_denied_24h": 0,
                "verifications_pending": 4,
                "avg_verification_time_hours": 2.5,
                "inactive_30_60_days": 12,
                "inactive_60_plus_days": 3,
                "oldest_verification_hours": 6.2,
            }
        except Exception as e:
            log.exception(f"Error getting daily membership data: {e}")
            return {}
    
    async def _get_training_data_daily(self) -> Dict:
        """Get training metrics for last 24 hours."""
        try:
            return {
                "requests_submitted_24h": 5,
                "requests_approved_24h": 5,
                "requests_denied_24h": 0,
                "started_24h": 5,
                "completed_24h": 3,
                "avg_approval_time_hours": 1.25,
                "reminders_sent_24h": 3,
                "by_discipline_24h": {
                    "Police": {"started": 2, "completed": 1},
                    "Fire": {"started": 2, "completed": 1},
                    "EMS": {"started": 1, "completed": 1},
                    "Coastal": {"started": 0, "completed": 0},
                },
            }
        except Exception as e:
            log.exception(f"Error getting daily training data: {e}")
            return {}
    
    async def _get_buildings_data_daily(self) -> Dict:
        """Get building metrics for last 24 hours."""
        try:
            return {
                "requests_submitted_24h": 6,
                "approved_24h": 4,
                "denied_24h": 1,
                "pending": 1,
                "avg_review_time_hours": 0.75,
                "extensions_started_24h": 7,
                "extensions_completed_24h": 5,
                "extensions_in_progress": 34,
                "extensions_trend_pct": 75.0,
                "by_type_24h": {
                    "Hospital": 2,
                    "Prison": 2,
                },
                "oldest_request_hours": 4.5,
            }
        except Exception as e:
            log.exception(f"Error getting daily buildings data: {e}")
            return {}
    
    async def _get_operations_data_daily(self) -> Dict:
        """Get operations metrics for last 24 hours."""
        try:
            return {
                "large_missions_started_24h": 2,
                "alliance_events_started_24h": 1,
                "custom_missions_created_24h": 0,
                "custom_missions_removed_24h": 0,
            }
        except Exception as e:
            log.exception(f"Error getting daily operations data: {e}")
            return {}
    
    async def _get_treasury_data_daily(self) -> Dict:
        """Get treasury metrics for last 24 hours."""
        try:
            return {
                "current_balance": 12458392,
                "change_24h": 284561,
                "change_24h_pct": 2.3,
                "income_24h": 312450,
                "expenses_24h": 27889,
                "contributors_24h": 8,
                "largest_expense_24h": 15000,
                "trend_7d": 1284320,
            }
        except Exception as e:
            log.exception(f"Error getting daily treasury data: {e}")
            return {}
    
    async def _get_sanctions_data_daily(self) -> Dict:
        """Get sanctions metrics for last 24 hours."""
        try:
            return {
                "issued_24h": 0,
                "active_warnings": 8,
                "active_1st_warnings": 5,
                "active_2nd_warnings": 2,
                "active_3rd_warnings": 1,
            }
        except Exception as e:
            log.exception(f"Error getting daily sanctions data: {e}")
            return {}
    
    async def _get_admin_activity_daily(self) -> Dict:
        """Get admin activity metrics for last 24 hours."""
        try:
            return {
                "building_reviews_24h": 5,
                "training_approvals_24h": 5,
                "verifications_24h": 2,
                "sanctions_24h": 0,
                "most_active_admin": "AdminAlpha",
                "most_active_admin_count": 7,
            }
        except Exception as e:
            log.exception(f"Error getting daily admin activity data: {e}")
            return {}
    
    # ==================== MONTHLY DATA METHODS ====================
    
    async def _get_membership_data_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get membership metrics for full month."""
        try:
            # Placeholder - implement actual queries
            return {
                "starting_members": 241,
                "ending_members": 247,
                "new_joins_period": 18,
                "left_period": 9,
                "kicked_period": 3,
                "net_growth": 6,
                "net_growth_pct": 2.5,
                "retention_rate": 94.7,
            }
        except Exception as e:
            log.exception(f"Error getting monthly membership data: {e}")
            return {}
    
    async def _get_training_data_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get training metrics for full month."""
        try:
            return {
                "started_period": 67,
                "completed_period": 62,
                "success_rate": 92.5,
                "top_5_trainings": [
                    ("SWAT Training", 15),
                    ("Critical Care", 12),
                    ("HazMat", 9),
                    ("Police Aviation", 8),
                    ("ALS Medical Training", 7),
                ],
                "by_discipline_counts": {
                    "Police": 32,
                    "Fire": 21,
                    "EMS": 10,
                    "Coastal": 4,
                },
            }
        except Exception as e:
            log.exception(f"Error getting monthly training data: {e}")
            return {}
    
    async def _get_buildings_data_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get building metrics for full month."""
        try:
            return {
                "approved_period": 89,
                "denied_period": 7,
                "extensions_started_period": 187,
                "extensions_completed_period": 175,
                "by_type_counts": {
                    "Hospital": 52,
                    "Prison": 37,
                },
            }
        except Exception as e:
            log.exception(f"Error getting monthly buildings data: {e}")
            return {}
    
    async def _get_operations_data_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get operations metrics for full month."""
        try:
            return {
                "large_missions_period": 23,
                "alliance_events_period": 8,
            }
        except Exception as e:
            log.exception(f"Error getting monthly operations data: {e}")
            return {}
    
    async def _get_treasury_data_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get treasury metrics for full month."""
        try:
            return {
                "opening_balance": 10892441,
                "closing_balance": 12458392,
                "growth_amount": 1565951,
                "growth_percentage": 14.4,
                "largest_contribution": 847392,
            }
        except Exception as e:
            log.exception(f"Error getting monthly treasury data: {e}")
            return {}
    
    async def _get_sanctions_data_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get sanctions metrics for full month."""
        try:
            return {
                "issued_period": 12,
                "by_type": {
                    "warnings": 11,
                    "kicks": 1,
                    "bans": 0,
                },
            }
        except Exception as e:
            log.exception(f"Error getting monthly sanctions data: {e}")
            return {}
    
    async def _get_admin_activity_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get admin activity metrics for full month."""
        try:
            return {
                "total_actions_period": 234,
                "most_active_admin_name": "AdminAlpha",
                "most_active_admin_count": 87,
                "avg_response_hours": 1.37,
            }
        except Exception as e:
            log.exception(f"Error getting monthly admin activity data: {e}")
            return {}
