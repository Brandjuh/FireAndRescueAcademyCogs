"""
Data Aggregator - V2 DATABASE QUERIES
Queries all V2 alliance databases and aggregates data for reports.

V2 Database Structure:
- members_v2.db: members table (member_id, username, rank, earned_credits, online_status, timestamp)
- logs_v2.db: logs table (id, hash, ts, action_key, executed_name, affected_name, description, contribution_amount, event_timestamp)
- income_v2.db: income table (entry_type, period, username, amount, description, timestamp)
- buildings_v2.db: buildings table (building_id, owner_name, building_type, classrooms, timestamp)
- membersync.db: links (LEGACY)
- building_manager.db: building_requests, building_actions (LEGACY)
- sanctions.db: sanctions (LEGACY)
"""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
from zoneinfo import ZoneInfo

log = logging.getLogger("red.FARA.AllianceReports.DataAggregator")


class DataAggregator:
    """Aggregate data from all V2 alliance databases."""

    DB_CONFIG_KEYS = {
        "members_v2": "members_v2_db_path",
        "logs_v2": "logs_v2_db_path",
        "income_v2": "income_v2_db_path",
        "buildings_v2": "buildings_v2_db_path",
        "alliance": "alliance_db_path",
        "membersync": "membersync_db_path",
        "building_manager": "building_manager_db_path",
        "sanctions": "sanctions_db_path",
    }
    
    def __init__(self, config_manager):
        """Initialize data aggregator."""
        self.config_manager = config_manager
        self._db_cache = {}
    
    def _get_db_connection(self, db_name: str) -> Optional[sqlite3.Connection]:
        """Get database connection."""
        try:
            config_key = self.DB_CONFIG_KEYS.get(db_name)
            if not config_key:
                log.error(f"Unknown database: {db_name}")
                return None
            
            configured_path = self.config_manager._db_cache.get(config_key)
            if not configured_path:
                log.error(f"Database path is not configured: {config_key}")
                return None

            db_path = Path(configured_path)
            
            if not db_path.exists():
                log.error(f"Database not found: {db_path}")
                return None
            
            return sqlite3.connect(str(db_path))
        
        except Exception as e:
            log.exception(f"Error connecting to {db_name} database: {e}")
            return None

    @staticmethod
    def _has_log_event_coverage(
        connection: sqlite3.Connection,
        start: datetime,
        end: datetime,
    ) -> bool:
        """Return whether confirmed log event timestamps exist in the period."""
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT 1 FROM logs
            WHERE datetime(event_timestamp) >= datetime(?)
            AND datetime(event_timestamp) < datetime(?)
            LIMIT 1
            """,
            (start.isoformat(), end.isoformat()),
        )
        return cursor.fetchone() is not None
    
    @staticmethod
    def _get_game_day_window(utc_now: datetime) -> tuple[datetime, datetime]:
        """Return the current MissionChief US game day as UTC datetimes."""
        eastern = ZoneInfo("America/New_York")
        local_now = utc_now.astimezone(eastern)
        local_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        return local_start.astimezone(ZoneInfo("UTC")), utc_now

    @staticmethod
    def _get_game_month_window(month_date: datetime) -> tuple[datetime, datetime]:
        """Return MissionChief's New York month boundaries as UTC datetimes."""
        eastern = ZoneInfo("America/New_York")
        utc = ZoneInfo("UTC")
        local_start = datetime(month_date.year, month_date.month, 1, tzinfo=eastern)
        if month_date.month == 12:
            local_end = datetime(month_date.year + 1, 1, 1, tzinfo=eastern)
        else:
            local_end = datetime(month_date.year, month_date.month + 1, 1, tzinfo=eastern)
        return local_start.astimezone(utc), local_end.astimezone(utc)

    async def get_daily_data(self) -> Dict:
        """Get aggregated data for the current MissionChief US game day."""
        try:
            log.info("Aggregating daily data (US Eastern game day)...")
            utc_now = datetime.now(ZoneInfo("UTC"))
            game_day_start, game_day_end = self._get_game_day_window(utc_now)
            
            log.info(f"Game Day: {game_day_start.isoformat()} to {game_day_end.isoformat()} (UTC)")
            
            data = {
                "membership": await self._get_membership_data_daily(game_day_start, game_day_end),
                "training": await self._get_training_data_daily(game_day_start, game_day_end),
                "buildings": await self._get_buildings_data_daily(game_day_start, game_day_end),
                "operations": await self._get_operations_data_daily(game_day_start, game_day_end),
                "sanctions": await self._get_sanctions_data_daily(game_day_start, game_day_end),
                "admin_activity": await self._get_admin_activity_daily(game_day_start, game_day_end),
            }
            
            log.info("Daily data aggregation complete")
            return data
        
        except Exception as e:
            log.exception(f"Error aggregating daily data: {e}")
            return {}
    
    async def get_monthly_data(self, month_date: datetime) -> Dict:
        """Get aggregated data for monthly reports (full month)."""
        try:
            log.info(f"Aggregating monthly data for {month_date.strftime('%B %Y')}...")
            
            first_day, period_end = self._get_game_month_window(month_date)
            
            data = {
                "membership": await self._get_membership_data_monthly(first_day, period_end),
                "training": await self._get_training_data_monthly(first_day, period_end),
                "buildings": await self._get_buildings_data_monthly(first_day, period_end),
                "operations": await self._get_operations_data_monthly(first_day, period_end),
                "sanctions": await self._get_sanctions_data_monthly(first_day, period_end),
                "admin_activity": await self._get_admin_activity_monthly(first_day, period_end),
            }
            
            log.info("Monthly data aggregation complete")
            return data
        
        except Exception as e:
            log.exception(f"Error aggregating monthly data: {e}")
            return {}
    
    # ==================== DAILY DATA METHODS (V2) ====================
    
    async def _get_membership_data_daily(self, game_day_start: datetime, game_day_end: datetime) -> Dict:
        """Get membership metrics for last game day using V2 members_v2.db."""
        try:
            conn = self._get_db_connection("members_v2")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            
            # Total current members (latest snapshot)
            cursor.execute("SELECT COUNT(DISTINCT member_id) FROM members WHERE timestamp = (SELECT MAX(timestamp) FROM members)")
            total_members = cursor.fetchone()[0]
            
            # New joins from logs_v2.db
            conn_logs = self._get_db_connection("logs_v2")
            new_joins = 0
            left = 0
            
            if conn_logs:
                cursor_logs = conn_logs.cursor()
                
                # New joins (action_key = 'added_to_alliance')
                cursor_logs.execute("""
                    SELECT COUNT(*) FROM logs 
                    WHERE action_key = 'added_to_alliance' 
                    AND datetime(event_timestamp) >= datetime(?)
                    AND datetime(event_timestamp) < datetime(?)
                """, (game_day_start.isoformat(), game_day_end.isoformat()))
                new_joins = cursor_logs.fetchone()[0]
                
                # Leaves (action_key = 'left_alliance')
                cursor_logs.execute("""
                    SELECT COUNT(*) FROM logs 
                    WHERE action_key = 'left_alliance' 
                    AND datetime(event_timestamp) >= datetime(?)
                    AND datetime(event_timestamp) < datetime(?)
                """, (game_day_start.isoformat(), game_day_end.isoformat()))
                left = cursor_logs.fetchone()[0]
                
                conn_logs.close()
            
            # Kicks from sanctions.db
            conn_s = self._get_db_connection("sanctions")
            kicked = 0
            if conn_s:
                cursor_s = conn_s.cursor()
                cursor_s.execute("""
                    SELECT COUNT(*) FROM sanctions 
                    WHERE sanction_type = 'Kick' 
                    AND created_at >= ?
                    AND created_at < ?
                """, (int(game_day_start.timestamp()), int(game_day_end.timestamp())))
                kicked = cursor_s.fetchone()[0]
                conn_s.close()
            
            conn.close()
            
            # Verifications from membersync.db
            conn_ms = self._get_db_connection("membersync")
            verif_approved = 0
            verif_pending = 0
            
            if conn_ms:
                cursor_ms = conn_ms.cursor()
                
                # Approved verifications
                cursor_ms.execute("""
                    SELECT COUNT(*) FROM links 
                    WHERE status = 'approved' 
                    AND datetime(updated_at) >= datetime(?)
                    AND datetime(updated_at) < datetime(?)
                """, (game_day_start.isoformat(), game_day_end.isoformat()))
                verif_approved = cursor_ms.fetchone()[0]
                
                # Pending verifications
                cursor_ms.execute("SELECT COUNT(*) FROM links WHERE status = 'pending'")
                verif_pending = cursor_ms.fetchone()[0]
                
                conn_ms.close()
            
            return {
                "total_members": total_members,
                "new_joins_24h": new_joins,
                "left_24h": left,
                "kicked_24h": kicked,
                "verifications_approved_24h": verif_approved,
                "verifications_pending": verif_pending,
            }
        
        except Exception as e:
            log.exception(f"Error getting daily membership data: {e}")
            return {"error": str(e)}
    
    async def _get_training_data_daily(self, game_day_start: datetime, game_day_end: datetime) -> Dict:
        """Get training metrics from logs_v2.db."""
        try:
            conn = self._get_db_connection("logs_v2")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            
            # Training courses started (action_key = 'created_course')
            cursor.execute("""
                SELECT COUNT(*) FROM logs 
                WHERE action_key = 'created_course' 
                AND datetime(event_timestamp) >= datetime(?)
                AND datetime(event_timestamp) < datetime(?)
            """, (game_day_start.isoformat(), game_day_end.isoformat()))
            started = cursor.fetchone()[0]
            
            # Training courses completed (action_key = 'course_completed')
            cursor.execute("""
                SELECT COUNT(*) FROM logs 
                WHERE action_key = 'course_completed' 
                AND datetime(event_timestamp) >= datetime(?)
                AND datetime(event_timestamp) < datetime(?)
            """, (game_day_start.isoformat(), game_day_end.isoformat()))
            completed = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                "started_24h": started,
                "completed_24h": completed,
            }
        
        except Exception as e:
            log.exception(f"Error getting daily training data: {e}")
            return {"error": str(e)}
    
    async def _get_buildings_data_daily(self, game_day_start: datetime, game_day_end: datetime) -> Dict:
        """Get building metrics from building_manager.db."""
        try:
            conn = self._get_db_connection("building_manager")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            game_day_start_ts = int(game_day_start.timestamp())
            game_day_end_ts = int(game_day_end.timestamp())
            
            # Approved buildings
            cursor.execute("""
                SELECT COUNT(*) FROM building_requests 
                WHERE status = 'approved' 
                AND updated_at >= ?
                AND updated_at < ?
            """, (game_day_start_ts, game_day_end_ts))
            approved = cursor.fetchone()[0]
            
            # Denied buildings
            cursor.execute("""
                SELECT COUNT(*) FROM building_requests 
                WHERE status = 'denied' 
                AND updated_at >= ?
                AND updated_at < ?
            """, (game_day_start_ts, game_day_end_ts))
            denied = cursor.fetchone()[0]
            
            # Pending buildings
            cursor.execute("SELECT COUNT(*) FROM building_requests WHERE status = 'pending'")
            pending = cursor.fetchone()[0]
            
            # By type
            cursor.execute("""
                SELECT building_type, COUNT(*) 
                FROM building_requests 
                WHERE status = 'approved' 
                AND updated_at >= ?
                AND updated_at < ?
                GROUP BY building_type
            """, (game_day_start_ts, game_day_end_ts))
            by_type = dict(cursor.fetchall())
            
            conn.close()
            
            # Extensions from logs_v2.db
            conn_logs = self._get_db_connection("logs_v2")
            ext_started = 0
            ext_completed = 0
            
            if conn_logs:
                cursor_logs = conn_logs.cursor()
                
                cursor_logs.execute("""
                    SELECT COUNT(*) FROM logs 
                    WHERE action_key = 'extension_started' 
                    AND datetime(event_timestamp) >= datetime(?)
                    AND datetime(event_timestamp) < datetime(?)
                """, (game_day_start.isoformat(), game_day_end.isoformat()))
                ext_started = cursor_logs.fetchone()[0]
                
                cursor_logs.execute("""
                    SELECT COUNT(*) FROM logs 
                    WHERE action_key = 'expansion_finished' 
                    AND datetime(event_timestamp) >= datetime(?)
                    AND datetime(event_timestamp) < datetime(?)
                """, (game_day_start.isoformat(), game_day_end.isoformat()))
                ext_completed = cursor_logs.fetchone()[0]
                
                conn_logs.close()
            
            return {
                "processed_24h": approved + denied,
                "approved_24h": approved,
                "denied_24h": denied,
                "pending": pending,
                "extensions_started_24h": ext_started,
                "extensions_completed_24h": ext_completed,
                "by_type_24h": by_type,
            }
        
        except Exception as e:
            log.exception(f"Error getting daily buildings data: {e}")
            return {"error": str(e)}
    
    async def _get_operations_data_daily(self, game_day_start: datetime, game_day_end: datetime) -> Dict:
        """Get operations metrics from logs_v2.db."""
        try:
            conn = self._get_db_connection("logs_v2")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            
            # Large missions
            cursor.execute("""
                SELECT COUNT(*) FROM logs 
                WHERE action_key = 'large_mission_started' 
                AND datetime(event_timestamp) >= datetime(?)
                AND datetime(event_timestamp) < datetime(?)
            """, (game_day_start.isoformat(), game_day_end.isoformat()))
            large_missions = cursor.fetchone()[0]
            
            # Alliance events
            cursor.execute("""
                SELECT COUNT(*) FROM logs 
                WHERE action_key = 'alliance_event_started' 
                AND datetime(event_timestamp) >= datetime(?)
                AND datetime(event_timestamp) < datetime(?)
            """, (game_day_start.isoformat(), game_day_end.isoformat()))
            events = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                "large_missions_started_24h": large_missions,
                "alliance_events_started_24h": events,
                "custom_missions_created_24h": 0,
                "custom_missions_removed_24h": 0,
            }
        
        except Exception as e:
            log.exception(f"Error getting daily operations data: {e}")
            return {"error": str(e)}
    
    async def _get_treasury_data_daily(self, game_day_start: datetime, game_day_end: datetime) -> Dict:
        """Get treasury metrics from alliance.db (LEGACY - still used)."""
        try:
            conn = self._get_db_connection("alliance")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            
            # Current balance
            cursor.execute("SELECT total_funds FROM treasury_balance ORDER BY scraped_at DESC LIMIT 1")
            result = cursor.fetchone()
            current_balance = result[0] if result else 0
            
            # Balance 24h ago
            cursor.execute("""
                SELECT total_funds FROM treasury_balance 
                WHERE datetime(scraped_at) <= datetime(?)
                ORDER BY scraped_at DESC LIMIT 1
            """, (game_day_start.isoformat(),))
            result = cursor.fetchone()
            balance_24h_ago = result[0] if result else current_balance
            
            change_24h = current_balance - balance_24h_ago
            change_pct = (change_24h / balance_24h_ago * 100) if balance_24h_ago > 0 else 0
            
            # Income (from treasury_income table)
            cursor.execute("""
                SELECT SUM(credits) FROM treasury_income 
                WHERE period = 'daily'
            """)
            result = cursor.fetchone()
            income_24h = result[0] if result and result[0] else 0
            
            # Contributors
            cursor.execute("""
                SELECT COUNT(DISTINCT user_id) FROM treasury_income 
                WHERE period = 'daily' AND credits > 0
            """)
            result = cursor.fetchone()
            contributors = result[0] if result else 0
            
            # Expenses
            cursor.execute("""
                SELECT SUM(credits) FROM treasury_expenses 
                WHERE datetime(scraped_at) >= datetime(?)
                AND datetime(scraped_at) < datetime(?)
            """, (game_day_start.isoformat(), game_day_end.isoformat()))
            result = cursor.fetchone()
            expenses_24h = result[0] if result and result[0] else 0
            
            # Largest expense
            cursor.execute("""
                SELECT MAX(credits) FROM treasury_expenses 
                WHERE datetime(scraped_at) >= datetime(?)
                AND datetime(scraped_at) < datetime(?)
            """, (game_day_start.isoformat(), game_day_end.isoformat()))
            result = cursor.fetchone()
            largest_expense = result[0] if result and result[0] else 0
            
            conn.close()
            
            return {
                "current_balance": current_balance,
                "change_24h": change_24h,
                "change_24h_pct": change_pct,
                "income_24h": income_24h,
                "expenses_24h": expenses_24h,
                "contributors_24h": contributors,
                "largest_expense_24h": largest_expense,
            }
        
        except Exception as e:
            log.exception(f"Error getting daily treasury data: {e}")
            return {"error": str(e)}
    
    async def _get_sanctions_data_daily(self, game_day_start: datetime, game_day_end: datetime) -> Dict:
        """Get sanctions metrics from sanctions.db."""
        try:
            conn = self._get_db_connection("sanctions")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            game_day_start_ts = int(game_day_start.timestamp())
            game_day_end_ts = int(game_day_end.timestamp())
            
            # Sanctions issued
            cursor.execute("""
                SELECT COUNT(*) FROM sanctions 
                WHERE created_at >= ?
                AND created_at < ?
            """, (game_day_start_ts, game_day_end_ts))
            issued = cursor.fetchone()[0]
            
            # Active warnings
            cursor.execute("""
                SELECT COUNT(*) FROM sanctions 
                WHERE status = 'active' 
                AND sanction_type LIKE '%Warning%'
            """)
            active_warnings = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                "issued_24h": issued,
                "active_warnings": active_warnings,
            }
        
        except Exception as e:
            log.exception(f"Error getting daily sanctions data: {e}")
            return {"error": str(e)}
    
    async def _get_admin_activity_daily(self, game_day_start: datetime, game_day_end: datetime) -> Dict:
        """Get admin activity metrics."""
        try:
            conn = self._get_db_connection("building_manager")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            game_day_start_ts = int(game_day_start.timestamp())
            game_day_end_ts = int(game_day_end.timestamp())
            
            # Building reviews
            cursor.execute("""
                SELECT COUNT(*) FROM building_actions 
                WHERE timestamp >= ?
                AND timestamp < ?
            """, (game_day_start_ts, game_day_end_ts))
            building_reviews = cursor.fetchone()[0]
            
            # Most active admin
            cursor.execute("""
                SELECT admin_username, COUNT(*) as count 
                FROM building_actions 
                WHERE timestamp >= ?
                AND timestamp < ?
                GROUP BY admin_username 
                ORDER BY count DESC 
                LIMIT 1
            """, (game_day_start_ts, game_day_end_ts))
            result = cursor.fetchone()
            most_active = result[0] if result else "N/A"
            most_active_count = result[1] if result else 0
            
            conn.close()
            
            # Sanction actions
            conn_s = self._get_db_connection("sanctions")
            sanction_actions = 0
            if conn_s:
                cursor_s = conn_s.cursor()
                cursor_s.execute("""
                    SELECT COUNT(*) FROM sanctions 
                    WHERE created_at >= ?
                    AND created_at < ?
                """, (game_day_start_ts, game_day_end_ts))
                sanction_actions = cursor_s.fetchone()[0]
                conn_s.close()
            
            return {
                "building_reviews_24h": building_reviews,
                "sanctions_24h": sanction_actions,
                "most_active_admin": most_active,
                "most_active_admin_count": most_active_count,
            }
        
        except Exception as e:
            log.exception(f"Error getting daily admin activity data: {e}")
            return {"error": str(e)}
    
    # ==================== MONTHLY DATA METHODS (V2) ====================
    
    async def _get_membership_data_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get membership metrics for full month using members_v2.db."""
        try:
            conn = self._get_db_connection("members_v2")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()

            cursor.execute("PRAGMA table_info(members)")
            member_columns = {row[1] for row in cursor.fetchall()}
            if "snapshot_source" not in member_columns:
                conn.close()
                return {"error": "Live member snapshot markers are unavailable"}
            
            cursor.execute(
                "SELECT MAX(timestamp) FROM members "
                "WHERE snapshot_source = 'live' AND datetime(timestamp) < datetime(?)",
                (start.isoformat(),),
            )
            starting_snapshot = cursor.fetchone()[0]
            cursor.execute(
                "SELECT MAX(timestamp) FROM members "
                "WHERE snapshot_source = 'live' AND datetime(timestamp) < datetime(?)",
                (end.isoformat(),),
            )
            ending_snapshot = cursor.fetchone()[0]
            if not starting_snapshot or not ending_snapshot:
                conn.close()
                return {"error": "Required live monthly member snapshots are unavailable"}

            # Use actual snapshots for the starting and ending member counts.
            cursor.execute(
                "SELECT COUNT(DISTINCT member_id) FROM members WHERE timestamp = ?",
                (starting_snapshot,),
            )
            starting_members = cursor.fetchone()[0]

            cursor.execute(
                "SELECT COUNT(DISTINCT member_id) FROM members WHERE timestamp = ?",
                (ending_snapshot,),
            )
            ending_members = cursor.fetchone()[0]
            
            conn.close()
            
            # Get joins/leaves from logs_v2
            conn_logs = self._get_db_connection("logs_v2")
            new_joins = 0
            left = 0
            log_activity_available = False
            
            if conn_logs:
                cursor_logs = conn_logs.cursor()
                log_activity_available = self._has_log_event_coverage(conn_logs, start, end)
                
                cursor_logs.execute("""
                    SELECT COUNT(*) FROM logs 
                    WHERE action_key = 'added_to_alliance' 
                    AND datetime(event_timestamp) >= datetime(?)
                    AND datetime(event_timestamp) < datetime(?)
                """, (start.isoformat(), end.isoformat()))
                new_joins = cursor_logs.fetchone()[0]
                
                cursor_logs.execute("""
                    SELECT COUNT(*) FROM logs 
                    WHERE action_key = 'left_alliance' 
                    AND datetime(event_timestamp) >= datetime(?)
                    AND datetime(event_timestamp) < datetime(?)
                """, (start.isoformat(), end.isoformat()))
                left = cursor_logs.fetchone()[0]
                
                conn_logs.close()
            
            # Kicks from sanctions
            conn_s = self._get_db_connection("sanctions")
            kicked = 0
            if conn_s:
                cursor_s = conn_s.cursor()
                cursor_s.execute("""
                    SELECT COUNT(*) FROM sanctions 
                    WHERE sanction_type = 'Kick' 
                    AND created_at >= ?
                    AND created_at < ?
                """, (int(start.timestamp()), int(end.timestamp())))
                kicked = cursor_s.fetchone()[0]
                conn_s.close()
            
            net_growth = ending_members - starting_members
            
            return {
                "starting_members": starting_members,
                "ending_members": ending_members,
                "new_joins_period": new_joins,
                "left_period": left,
                "kicked_period": kicked,
                "net_growth": net_growth,
                "log_activity_available": log_activity_available,
            }
        
        except Exception as e:
            log.exception(f"Error getting monthly membership data: {e}")
            return {"error": str(e)}
    
    async def _get_training_data_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get training metrics for full month from logs_v2.db."""
        try:
            conn = self._get_db_connection("logs_v2")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            if not self._has_log_event_coverage(conn, start, end):
                conn.close()
                return {"error": "Log event timestamps are unavailable for this period"}
            
            # Courses started
            cursor.execute("""
                SELECT COUNT(*) FROM logs 
                WHERE action_key = 'created_course' 
                AND datetime(event_timestamp) >= datetime(?)
                AND datetime(event_timestamp) < datetime(?)
            """, (start.isoformat(), end.isoformat()))
            started = cursor.fetchone()[0]
            
            # Courses completed
            cursor.execute("""
                SELECT COUNT(*) FROM logs 
                WHERE action_key = 'course_completed' 
                AND datetime(event_timestamp) >= datetime(?)
                AND datetime(event_timestamp) < datetime(?)
            """, (start.isoformat(), end.isoformat()))
            completed = cursor.fetchone()[0]
            
            success_rate = (completed / started * 100) if started > 0 else 0
            
            conn.close()
            
            return {
                "started_period": started,
                "completed_period": completed,
                "success_rate": success_rate,
                "top_5_trainings": [],
                "by_discipline_counts": {},
            }
        
        except Exception as e:
            log.exception(f"Error getting monthly training data: {e}")
            return {"error": str(e)}
    
    async def _get_buildings_data_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get building metrics for full month."""
        try:
            conn = self._get_db_connection("building_manager")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            start_ts = int(start.timestamp())
            end_ts = int(end.timestamp())
            
            # Approved
            cursor.execute("""
                SELECT COUNT(*) FROM building_requests 
                WHERE status = 'approved' 
                AND updated_at >= ?
                AND updated_at < ?
            """, (start_ts, end_ts))
            approved = cursor.fetchone()[0]
            
            # Denied
            cursor.execute("""
                SELECT COUNT(*) FROM building_requests 
                WHERE status = 'denied' 
                AND updated_at >= ?
                AND updated_at < ?
            """, (start_ts, end_ts))
            denied = cursor.fetchone()[0]
            
            # By type
            cursor.execute("""
                SELECT building_type, COUNT(*) 
                FROM building_requests 
                WHERE status = 'approved' 
                AND updated_at >= ?
                AND updated_at < ?
                GROUP BY building_type
            """, (start_ts, end_ts))
            by_type = dict(cursor.fetchall())
            
            conn.close()
            
            # Extensions from logs_v2
            conn_logs = self._get_db_connection("logs_v2")
            ext_started = 0
            ext_completed = 0
            extension_activity_available = False
            
            if conn_logs:
                cursor_logs = conn_logs.cursor()
                extension_activity_available = self._has_log_event_coverage(
                    conn_logs,
                    start,
                    end,
                )
                
                cursor_logs.execute("""
                    SELECT COUNT(*) FROM logs 
                    WHERE action_key = 'extension_started' 
                    AND datetime(event_timestamp) >= datetime(?)
                    AND datetime(event_timestamp) < datetime(?)
                """, (start.isoformat(), end.isoformat()))
                ext_started = cursor_logs.fetchone()[0]
                
                cursor_logs.execute("""
                    SELECT COUNT(*) FROM logs 
                    WHERE action_key = 'expansion_finished' 
                    AND datetime(event_timestamp) >= datetime(?)
                    AND datetime(event_timestamp) < datetime(?)
                """, (start.isoformat(), end.isoformat()))
                ext_completed = cursor_logs.fetchone()[0]
                
                conn_logs.close()
            
            return {
                "approved_period": approved,
                "denied_period": denied,
                "extensions_started_period": ext_started,
                "extensions_completed_period": ext_completed,
                "by_type_counts": by_type,
                "extension_activity_available": extension_activity_available,
            }
        
        except Exception as e:
            log.exception(f"Error getting monthly buildings data: {e}")
            return {"error": str(e)}
    
    async def _get_operations_data_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get operations metrics for full month."""
        try:
            conn = self._get_db_connection("logs_v2")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            if not self._has_log_event_coverage(conn, start, end):
                conn.close()
                return {"error": "Log event timestamps are unavailable for this period"}
            
            # Large missions
            cursor.execute("""
                SELECT COUNT(*) FROM logs 
                WHERE action_key = 'large_mission_started' 
                AND datetime(event_timestamp) >= datetime(?)
                AND datetime(event_timestamp) < datetime(?)
            """, (start.isoformat(), end.isoformat()))
            missions = cursor.fetchone()[0]
            
            # Events
            cursor.execute("""
                SELECT COUNT(*) FROM logs 
                WHERE action_key = 'alliance_event_started' 
                AND datetime(event_timestamp) >= datetime(?)
                AND datetime(event_timestamp) < datetime(?)
            """, (start.isoformat(), end.isoformat()))
            events = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                "large_missions_period": missions,
                "alliance_events_period": events,
            }
        
        except Exception as e:
            log.exception(f"Error getting monthly operations data: {e}")
            return {"error": str(e)}
    
    async def _get_treasury_data_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get treasury metrics for full month from alliance.db."""
        try:
            conn = self._get_db_connection("alliance")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            
            # Opening balance
            cursor.execute("""
                SELECT total_funds FROM treasury_balance 
                WHERE datetime(scraped_at) <= datetime(?)
                ORDER BY scraped_at DESC LIMIT 1
            """, (start.isoformat(),))
            result = cursor.fetchone()
            opening = result[0] if result else 0
            
            # Closing balance from the latest snapshot inside the requested month.
            cursor.execute("""
                SELECT total_funds FROM treasury_balance
                WHERE datetime(scraped_at) < datetime(?)
                ORDER BY scraped_at DESC LIMIT 1
            """, (end.isoformat(),))
            result = cursor.fetchone()
            closing = result[0] if result else 0
            
            growth = closing - opening
            growth_pct = (growth / opening * 100) if opening > 0 else 0
            
            # Largest contribution
            cursor.execute("""
                SELECT MAX(credits) FROM treasury_expenses 
                WHERE datetime(scraped_at) >= datetime(?)
                AND datetime(scraped_at) < datetime(?)
            """, (start.isoformat(), end.isoformat()))
            result = cursor.fetchone()
            largest_contribution = result[0] if result and result[0] else 0
            
            conn.close()
            
            return {
                "opening_balance": opening,
                "closing_balance": closing,
                "growth_amount": growth,
                "growth_percentage": growth_pct,
                "largest_contribution": largest_contribution,
            }
        
        except Exception as e:
            log.exception(f"Error getting monthly treasury data: {e}")
            return {"error": str(e)}
    
    async def _get_sanctions_data_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get sanctions metrics for full month."""
        try:
            conn = self._get_db_connection("sanctions")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            start_ts = int(start.timestamp())
            end_ts = int(end.timestamp())
            
            # Total issued
            cursor.execute("""
                SELECT COUNT(*) FROM sanctions 
                WHERE created_at >= ?
                AND created_at < ?
            """, (start_ts, end_ts))
            issued = cursor.fetchone()[0]
            
            # By type
            cursor.execute("""
                SELECT 
                    CASE 
                        WHEN sanction_type LIKE '%Warning%' THEN 'warnings'
                        WHEN sanction_type LIKE '%Kick%' THEN 'kicks'
                        WHEN sanction_type LIKE '%Ban%' THEN 'bans'
                        ELSE 'other'
                    END as type,
                    COUNT(*) 
                FROM sanctions 
                WHERE created_at >= ?
                AND created_at < ?
                GROUP BY type
            """, (start_ts, end_ts))
            by_type = dict(cursor.fetchall())
            
            conn.close()
            
            return {
                "issued_period": issued,
                "by_type": by_type,
            }
        
        except Exception as e:
            log.exception(f"Error getting monthly sanctions data: {e}")
            return {"error": str(e)}
    
    async def _get_admin_activity_monthly(self, start: datetime, end: datetime) -> Dict:
        """Get admin activity metrics for full month."""
        try:
            conn = self._get_db_connection("building_manager")
            if not conn:
                return {"error": "Database not found"}
            
            cursor = conn.cursor()
            start_ts = int(start.timestamp())
            end_ts = int(end.timestamp())
            
            # Total actions
            cursor.execute("""
                SELECT COUNT(*) FROM building_actions 
                WHERE timestamp >= ?
                AND timestamp < ?
            """, (start_ts, end_ts))
            total_actions = cursor.fetchone()[0]
            
            # Most active admin
            cursor.execute("""
                SELECT admin_username, COUNT(*) as count 
                FROM building_actions 
                WHERE timestamp >= ?
                AND timestamp < ?
                GROUP BY admin_username 
                ORDER BY count DESC 
                LIMIT 1
            """, (start_ts, end_ts))
            result = cursor.fetchone()
            most_active = result[0] if result else "N/A"
            most_active_count = result[1] if result else 0
            
            conn.close()
            
            conn_s = self._get_db_connection("sanctions")
            if conn_s:
                cursor_s = conn_s.cursor()
                cursor_s.execute("""
                    SELECT COUNT(*) FROM sanctions 
                    WHERE created_at >= ?
                    AND created_at < ?
                """, (start_ts, end_ts))
                sanction_actions = cursor_s.fetchone()[0]
                total_actions += sanction_actions
                conn_s.close()
            
            return {
                "total_actions_period": total_actions,
                "most_active_admin_name": most_active,
                "most_active_admin_count": most_active_count,
                "avg_response_hours": None,
            }
        
        except Exception as e:
            log.exception(f"Error getting monthly admin activity data: {e}")
            return {"error": str(e)}
