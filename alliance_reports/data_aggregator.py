"""
Data Aggregator for AllianceReports
Queries all databases and aggregates data for reports.
"""

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

log = logging.getLogger("red.FARA.AllianceReports.DataAggregator")


class DataAggregator:
    """Aggregates data from all alliance databases."""
    
    def __init__(self, config_manager):
        """Initialize data aggregator."""
        self.config_manager = config_manager
        self._cache = {}
    
    def _connect_db(self, db_path: Path) -> Optional[sqlite3.Connection]:
        """Create database connection."""
        try:
            if not db_path or not db_path.exists():
                return None
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            log.exception(f"Error connecting to {db_path}: {e}")
            return None
    
    async def get_daily_data(self) -> Dict[str, Any]:
        """Get all data needed for daily reports."""
        data = {
            "timestamp": datetime.now(timezone.utc),
            "membership": await self._get_membership_data_24h(),
            "training": await self._get_training_data_24h(),
            "buildings": await self._get_building_data_24h(),
            "operations": await self._get_operations_data_24h(),
            "treasury": await self._get_treasury_snapshot(),
        }
        return data
    
    async def _get_membership_data_24h(self) -> Dict[str, Any]:
        """Get membership data for last 24 hours."""
        db_path = await self.config_manager.get_db_path("alliance_db_path")
        if not db_path:
            return {"error": "Database not found"}
        
        conn = self._connect_db(db_path)
        if not conn:
            return {"error": "Connection failed"}
        
        try:
            now = datetime.now(timezone.utc)
            yesterday = now - timedelta(days=1)
            
            # Current member count
            cursor = conn.execute("SELECT COUNT(*) as count FROM members_current")
            current_count = cursor.fetchone()["count"]
            
            # Get logs from last 24h
            cursor = conn.execute("""
                SELECT action_key, COUNT(*) as count
                FROM logs
                WHERE datetime(scraped_at) >= datetime(?, 'unixepoch')
                AND action_key IN ('added_to_alliance', 'left_alliance', 'kicked_from_alliance')
                GROUP BY action_key
            """, (yesterday.timestamp(),))
            
            actions = {row["action_key"]: row["count"] for row in cursor.fetchall()}
            
            new_members = actions.get("added_to_alliance", 0)
            left_members = actions.get("left_alliance", 0)
            kicked_members = actions.get("kicked_from_alliance", 0)
            
            # Get verification data from MemberSync
            membersync_path = await self.config_manager.get_db_path("membersync_db_path")
            verifications_approved = 0
            if membersync_path:
                ms_conn = self._connect_db(membersync_path)
                if ms_conn:
                    cursor = ms_conn.execute("""
                        SELECT COUNT(*) as count FROM links
                        WHERE status = 'approved'
                        AND datetime(updated_at) >= datetime(?, 'unixepoch')
                    """, (yesterday.timestamp(),))
                    verifications_approved = cursor.fetchone()["count"]
                    ms_conn.close()
            
            # Calculate change from yesterday
            # Try to get count from 48h ago for comparison
            two_days_ago = now - timedelta(days=2)
            cursor = conn.execute("""
                SELECT COUNT(DISTINCT user_id) as count
                FROM members_history
                WHERE datetime(scraped_at) >= datetime(?, 'unixepoch')
                AND datetime(scraped_at) < datetime(?, 'unixepoch')
            """, (two_days_ago.timestamp(), yesterday.timestamp()))
            
            previous_count_row = cursor.fetchone()
            previous_count = previous_count_row["count"] if previous_count_row else current_count
            
            day_over_day_change = current_count - previous_count if previous_count > 0 else 0
            
            return {
                "total_members": current_count,
                "new_joins": new_members,
                "left": left_members,
                "kicked": kicked_members,
                "net_change": new_members - left_members - kicked_members,
                "verifications_approved": verifications_approved,
                "day_over_day_change": day_over_day_change,
            }
        
        except Exception as e:
            log.exception(f"Error getting membership data: {e}")
            return {"error": str(e)}
        finally:
            conn.close()
    
    async def _get_training_data_24h(self) -> Dict[str, Any]:
        """Get training data for last 24 hours."""
        db_path = await self.config_manager.get_db_path("alliance_db_path")
        if not db_path:
            return {"error": "Database not found"}
        
        conn = self._connect_db(db_path)
        if not conn:
            return {"error": "Connection failed"}
        
        try:
            now = datetime.now(timezone.utc)
            yesterday = now - timedelta(days=1)
            
            # Count courses created and completed
            cursor = conn.execute("""
                SELECT 
                    SUM(CASE WHEN action_key = 'created_a_course' THEN 1 ELSE 0 END) as started,
                    SUM(CASE WHEN action_key = 'course_completed' THEN 1 ELSE 0 END) as completed
                FROM logs
                WHERE datetime(scraped_at) >= datetime(?, 'unixepoch')
                AND action_key IN ('created_a_course', 'course_completed')
            """, (yesterday.timestamp(),))
            
            row = cursor.fetchone()
            started = row["started"] or 0
            completed = row["completed"] or 0
            
            # Get previous day data for comparison
            two_days_ago = now - timedelta(days=2)
            cursor = conn.execute("""
                SELECT 
                    SUM(CASE WHEN action_key = 'created_a_course' THEN 1 ELSE 0 END) as started,
                    SUM(CASE WHEN action_key = 'course_completed' THEN 1 ELSE 0 END) as completed
                FROM logs
                WHERE datetime(scraped_at) >= datetime(?, 'unixepoch')
                AND datetime(scraped_at) < datetime(?, 'unixepoch')
                AND action_key IN ('created_a_course', 'course_completed')
            """, (two_days_ago.timestamp(), yesterday.timestamp()))
            
            prev_row = cursor.fetchone()
            prev_started = prev_row["started"] or 0
            prev_completed = prev_row["completed"] or 0
            
            return {
                "started": started,
                "completed": completed,
                "day_over_day_started": started - prev_started,
                "day_over_day_completed": completed - prev_completed,
            }
        
        except Exception as e:
            log.exception(f"Error getting training data: {e}")
            return {"error": str(e)}
        finally:
            conn.close()
    
    async def _get_building_data_24h(self) -> Dict[str, Any]:
        """Get building data for last 24 hours."""
        # Get from AllianceScraper logs
        alliance_db = await self.config_manager.get_db_path("alliance_db_path")
        
        # Get from BuildingManager
        building_db = await self.config_manager.get_db_path("building_db_path")
        
        result = {
            "approved": 0,
            "extensions_started": 0,
            "extensions_completed": 0,
            "day_over_day_approved": 0,
            "day_over_day_extensions_started": 0,
            "day_over_day_extensions_completed": 0,
        }
        
        if not alliance_db:
            return result
        
        conn = self._connect_db(alliance_db)
        if not conn:
            return result
        
        try:
            now = datetime.now(timezone.utc)
            yesterday = now - timedelta(days=1)
            two_days_ago = now - timedelta(days=2)
            
            # Buildings constructed
            cursor = conn.execute("""
                SELECT COUNT(*) as count FROM logs
                WHERE datetime(scraped_at) >= datetime(?, 'unixepoch')
                AND action_key = 'building_constructed'
            """, (yesterday.timestamp(),))
            approved = cursor.fetchone()["count"]
            
            # Extensions started
            cursor = conn.execute("""
                SELECT COUNT(*) as count FROM logs
                WHERE datetime(scraped_at) >= datetime(?, 'unixepoch')
                AND action_key = 'extension_started'
            """, (yesterday.timestamp(),))
            extensions_started = cursor.fetchone()["count"]
            
            # Extensions completed
            cursor = conn.execute("""
                SELECT COUNT(*) as count FROM logs
                WHERE datetime(scraped_at) >= datetime(?, 'unixepoch')
                AND action_key = 'expansion_finished'
            """, (yesterday.timestamp(),))
            extensions_completed = cursor.fetchone()["count"]
            
            # Previous day for comparison
            cursor = conn.execute("""
                SELECT 
                    SUM(CASE WHEN action_key = 'building_constructed' THEN 1 ELSE 0 END) as approved,
                    SUM(CASE WHEN action_key = 'extension_started' THEN 1 ELSE 0 END) as ext_started,
                    SUM(CASE WHEN action_key = 'expansion_finished' THEN 1 ELSE 0 END) as ext_completed
                FROM logs
                WHERE datetime(scraped_at) >= datetime(?, 'unixepoch')
                AND datetime(scraped_at) < datetime(?, 'unixepoch')
                AND action_key IN ('building_constructed', 'extension_started', 'expansion_finished')
            """, (two_days_ago.timestamp(), yesterday.timestamp()))
            
            prev_row = cursor.fetchone()
            prev_approved = prev_row["approved"] or 0
            prev_ext_started = prev_row["ext_started"] or 0
            prev_ext_completed = prev_row["ext_completed"] or 0
            
            result.update({
                "approved": approved,
                "extensions_started": extensions_started,
                "extensions_completed": extensions_completed,
                "day_over_day_approved": approved - prev_approved,
                "day_over_day_extensions_started": extensions_started - prev_ext_started,
                "day_over_day_extensions_completed": extensions_completed - prev_ext_completed,
            })
            
        except Exception as e:
            log.exception(f"Error getting building data: {e}")
            result["error"] = str(e)
        finally:
            conn.close()
        
        return result
    
    async def _get_operations_data_24h(self) -> Dict[str, Any]:
        """Get operations data for last 24 hours."""
        db_path = await self.config_manager.get_db_path("alliance_db_path")
        if not db_path:
            return {"error": "Database not found"}
        
        conn = self._connect_db(db_path)
        if not conn:
            return {"error": "Connection failed"}
        
        try:
            now = datetime.now(timezone.utc)
            yesterday = now - timedelta(days=1)
            
            cursor = conn.execute("""
                SELECT 
                    SUM(CASE WHEN action_key = 'large_mission_started' THEN 1 ELSE 0 END) as missions,
                    SUM(CASE WHEN action_key = 'alliance_event_started' THEN 1 ELSE 0 END) as events
                FROM logs
                WHERE datetime(scraped_at) >= datetime(?, 'unixepoch')
                AND action_key IN ('large_mission_started', 'alliance_event_started')
            """, (yesterday.timestamp(),))
            
            row = cursor.fetchone()
            missions = row["missions"] or 0
            events = row["events"] or 0
            
            # Previous day
            two_days_ago = now - timedelta(days=2)
            cursor = conn.execute("""
                SELECT 
                    SUM(CASE WHEN action_key = 'large_mission_started' THEN 1 ELSE 0 END) as missions,
                    SUM(CASE WHEN action_key = 'alliance_event_started' THEN 1 ELSE 0 END) as events
                FROM logs
                WHERE datetime(scraped_at) >= datetime(?, 'unixepoch')
                AND datetime(scraped_at) < datetime(?, 'unixepoch')
                AND action_key IN ('large_mission_started', 'alliance_event_started')
            """, (two_days_ago.timestamp(), yesterday.timestamp()))
            
            prev_row = cursor.fetchone()
            prev_missions = prev_row["missions"] or 0
            prev_events = prev_row["events"] or 0
            
            return {
                "large_missions_started": missions,
                "alliance_events_started": events,
                "day_over_day_missions": missions - prev_missions,
                "day_over_day_events": events - prev_events,
            }
        
        except Exception as e:
            log.exception(f"Error getting operations data: {e}")
            return {"error": str(e)}
        finally:
            conn.close()
    
    async def _get_treasury_snapshot(self) -> Dict[str, Any]:
        """Get current treasury status."""
        db_path = await self.config_manager.get_db_path("alliance_db_path")
        if not db_path:
            return {"error": "Database not found"}
        
        conn = self._connect_db(db_path)
        if not conn:
            return {"error": "Connection failed"}
        
        try:
            # Latest balance
            cursor = conn.execute("""
                SELECT total_funds, scraped_at
                FROM treasury_balance
                ORDER BY scraped_at DESC
                LIMIT 1
            """)
            
            balance_row = cursor.fetchone()
            current_balance = balance_row["total_funds"] if balance_row else 0
            
            # Balance from 24h ago for comparison
            now = datetime.now(timezone.utc)
            yesterday = now - timedelta(days=1)
            
            cursor = conn.execute("""
                SELECT total_funds
                FROM treasury_balance
                WHERE datetime(scraped_at) <= datetime(?, 'unixepoch')
                ORDER BY scraped_at DESC
                LIMIT 1
            """, (yesterday.timestamp(),))
            
            prev_balance_row = cursor.fetchone()
            previous_balance = prev_balance_row["total_funds"] if prev_balance_row else current_balance
            
            change_24h = current_balance - previous_balance
            change_percent = (change_24h / previous_balance * 100) if previous_balance > 0 else 0
            
            # Count contributors today
            cursor = conn.execute("""
                SELECT COUNT(DISTINCT executed_mc_id) as count
                FROM logs
                WHERE datetime(scraped_at) >= datetime(?, 'unixepoch')
                AND action_key = 'contributed_to_alliance'
                AND executed_mc_id IS NOT NULL
                AND executed_mc_id != ''
            """, (yesterday.timestamp(),))
            
            contributors_row = cursor.fetchone()
            contributors = contributors_row["count"] if contributors_row else 0
            
            return {
                "current_balance": current_balance,
                "change_24h": change_24h,
                "change_percent": change_percent,
                "contributors_24h": contributors,
            }
        
        except Exception as e:
            log.exception(f"Error getting treasury data: {e}")
            return {"error": str(e)}
        finally:
            conn.close()
