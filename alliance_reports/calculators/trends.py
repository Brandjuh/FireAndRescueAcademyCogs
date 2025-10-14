"""
Trends Calculator
Analyzes historical data for week-over-week, month-over-month, and year-over-year comparisons
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple
from zoneinfo import ZoneInfo

log = logging.getLogger("red.FARA.AllianceReports.Trends")


class TrendsCalculator:
    """Calculate trends and comparisons across time periods."""
    
    def __init__(self, data_aggregator):
        """Initialize trends calculator."""
        self.aggregator = data_aggregator
    
    async def calculate_monthly_trends(
        self,
        current_data: Dict,
        tz: ZoneInfo
    ) -> Dict[str, any]:
        """
        Calculate all trends for monthly report.
        
        Returns comprehensive trend data including:
        - Week-over-week comparisons
        - Month-over-month comparisons
        - Year-over-year comparisons
        - Growth rates and percentages
        """
        now = datetime.now(tz)
        
        trends = {
            "wow": {},  # Week over week
            "mom": {},  # Month over month
            "yoy": {},  # Year over year
        }
        
        try:
            # Get historical data
            last_week_data = await self._get_period_data(now - timedelta(days=7), now - timedelta(days=1), tz)
            last_month_data = await self._get_period_data(
                (now.replace(day=1) - timedelta(days=1)).replace(day=1),
                now.replace(day=1) - timedelta(days=1),
                tz
            )
            last_year_data = await self._get_period_data(
                now.replace(year=now.year - 1, day=1),
                (now.replace(year=now.year - 1, day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1),
                tz
            )
            
            # Calculate WoW trends
            trends["wow"] = self._calculate_comparison(current_data, last_week_data, "week")
            
            # Calculate MoM trends
            trends["mom"] = self._calculate_comparison(current_data, last_month_data, "month")
            
            # Calculate YoY trends
            trends["yoy"] = self._calculate_comparison(current_data, last_year_data, "year")
            
            log.info("Monthly trends calculated successfully")
            return trends
        
        except Exception as e:
            log.exception(f"Error calculating monthly trends: {e}")
            return trends
    
    async def _get_period_data(
        self,
        start: datetime,
        end: datetime,
        tz: ZoneInfo
    ) -> Dict:
        """Get aggregated data for a specific time period."""
        try:
            # This would query historical data from databases
            # For now, return structure - implement actual queries based on your data
            
            data = {
                "membership": {
                    "total_members": 0,
                    "new_joins": 0,
                    "left": 0,
                    "kicked": 0,
                },
                "training": {
                    "started": 0,
                    "completed": 0,
                },
                "buildings": {
                    "approved": 0,
                    "extensions_started": 0,
                    "extensions_completed": 0,
                },
                "treasury": {
                    "balance": 0,
                    "income": 0,
                    "expenses": 0,
                },
                "operations": {
                    "large_missions": 0,
                    "alliance_events": 0,
                },
            }
            
            # TODO: Implement actual historical data queries
            # This is a placeholder structure
            
            return data
        
        except Exception as e:
            log.exception(f"Error getting period data: {e}")
            return {}
    
    def _calculate_comparison(
        self,
        current: Dict,
        previous: Dict,
        period_name: str
    ) -> Dict:
        """Calculate comparison between two periods."""
        comparison = {}
        
        try:
            # Membership comparisons
            curr_members = current.get("membership", {}).get("total_members", 0)
            prev_members = previous.get("membership", {}).get("total_members", 0)
            comparison["members"] = self._calc_change(curr_members, prev_members)
            
            # Training comparisons
            curr_trainings = current.get("training", {}).get("started_period", 0)
            prev_trainings = previous.get("training", {}).get("started", 0)
            comparison["trainings"] = self._calc_change(curr_trainings, prev_trainings)
            
            # Building comparisons
            curr_buildings = current.get("buildings", {}).get("approved_period", 0)
            prev_buildings = previous.get("buildings", {}).get("approved", 0)
            comparison["buildings"] = self._calc_change(curr_buildings, prev_buildings)
            
            # Extension comparisons
            curr_extensions = current.get("buildings", {}).get("extensions_started_period", 0)
            prev_extensions = previous.get("buildings", {}).get("extensions_started", 0)
            comparison["extensions"] = self._calc_change(curr_extensions, prev_extensions)
            
            # Treasury comparisons
            curr_balance = current.get("treasury", {}).get("current_balance", 0)
            prev_balance = previous.get("treasury", {}).get("balance", 0)
            comparison["treasury"] = self._calc_change(curr_balance, prev_balance)
            
            log.debug(f"{period_name} comparison calculated")
            return comparison
        
        except Exception as e:
            log.exception(f"Error calculating {period_name} comparison: {e}")
            return comparison
    
    def _calc_change(self, current: float, previous: float) -> Dict:
        """Calculate absolute and percentage change."""
        absolute = current - previous
        
        if previous == 0:
            percentage = 0 if current == 0 else 100
        else:
            percentage = (absolute / previous) * 100
        
        return {
            "current": current,
            "previous": previous,
            "absolute": absolute,
            "percentage": percentage,
            "trend": "↗️" if absolute > 0 else "↘️" if absolute < 0 else "➡️"
        }
    
    def format_trend(self, trend_data: Dict, metric_name: str = "value") -> str:
        """Format trend data into readable string."""
        if not trend_data:
            return "N/A"
        
        absolute = trend_data.get("absolute", 0)
        percentage = trend_data.get("percentage", 0)
        trend_icon = trend_data.get("trend", "➡️")
        
        return f"{trend_icon} {absolute:+,} ({percentage:+.1f}%)"
    
    def get_4week_pattern(self, weekly_data: List[Dict]) -> str:
        """
        Generate 4-week activity pattern visualization.
        
        Args:
            weekly_data: List of 4 weeks of data, most recent first
        """
        if not weekly_data or len(weekly_data) < 4:
            return "Insufficient data"
        
        try:
            # Normalize to 0-10 scale
            max_val = max(w.get("value", 0) for w in weekly_data)
            
            pattern = []
            for i, week in enumerate(reversed(weekly_data)):  # Oldest to newest
                value = week.get("value", 0)
                normalized = int((value / max_val * 10)) if max_val > 0 else 0
                bar = "▓" * normalized + "░" * (10 - normalized)
                pattern.append(f"W{i+1}: {bar} {value}")
            
            # Determine trend
            if weekly_data[0]["value"] > weekly_data[-1]["value"]:
                trend = "Strong finish! ↗️"
            elif weekly_data[0]["value"] < weekly_data[-1]["value"]:
                trend = "Slowing down ↘️"
            else:
                trend = "Steady ➡️"
            
            return "\n".join(pattern) + f"\n\nTrend: {trend}"
        
        except Exception as e:
            log.exception(f"Error generating 4-week pattern: {e}")
            return "Error generating pattern"
