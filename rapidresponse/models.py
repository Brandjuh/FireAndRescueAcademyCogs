"""
Database models and schema for Rapid Response Dispatch
"""
import aiosqlite
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path

log = logging.getLogger("red.rapidresponse.models")


class Database:
    """Database manager for Rapid Response Dispatch"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        
    async def initialize(self):
        """Initialize database with required tables"""
        async with aiosqlite.connect(str(self.db_path)) as db:
            # Players table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    user_id INTEGER PRIMARY KEY,
                    guild_id INTEGER NOT NULL,
                    station_level INTEGER DEFAULT 1,
                    xp INTEGER DEFAULT 0,
                    credits INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 0,
                    stat_response INTEGER DEFAULT 10,
                    stat_tactics INTEGER DEFAULT 10,
                    stat_logistics INTEGER DEFAULT 10,
                    stat_medical INTEGER DEFAULT 10,
                    stat_command INTEGER DEFAULT 10,
                    morale INTEGER DEFAULT 100,
                    last_mission_time TEXT,
                    current_cooldown_until TEXT,
                    mission_streak INTEGER DEFAULT 0,
                    total_missions INTEGER DEFAULT 0,
                    successful_missions INTEGER DEFAULT 0,
                    failed_missions INTEGER DEFAULT 0,
                    ignored_missions INTEGER DEFAULT 0,
                    thread_id INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Active missions table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS active_missions (
                    mission_instance_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    mission_id INTEGER NOT NULL,
                    mission_name TEXT NOT NULL,
                    mission_data TEXT NOT NULL,
                    tier INTEGER NOT NULL,
                    difficulty INTEGER NOT NULL,
                    status TEXT DEFAULT 'pending',
                    assigned_time TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    stage INTEGER DEFAULT 1,
                    max_stage INTEGER DEFAULT 1,
                    stage_data TEXT,
                    message_id INTEGER,
                    FOREIGN KEY (user_id) REFERENCES players (user_id)
                )
            """)
            
            # Mission history table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS mission_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    mission_id INTEGER NOT NULL,
                    mission_name TEXT NOT NULL,
                    tier INTEGER NOT NULL,
                    outcome TEXT NOT NULL,
                    credits_earned INTEGER DEFAULT 0,
                    xp_earned INTEGER DEFAULT 0,
                    morale_change INTEGER DEFAULT 0,
                    completed_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES players (user_id)
                )
            """)
            
            # Training table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS training (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    stat_type TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completes_at TEXT NOT NULL,
                    is_complete INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES players (user_id)
                )
            """)
            
            # Mission cache table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS mission_cache (
                    id INTEGER PRIMARY KEY,
                    data TEXT NOT NULL,
                    cached_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Config table for bot settings
            await db.execute("""
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            
            await db.commit()
            log.info("Database initialized successfully")
    
    async def get_player(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get player data"""
        async with aiosqlite.connect(str(self.db_path)) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM players WHERE user_id = ?", (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None
    
    async def create_player(self, user_id: int, guild_id: int) -> Dict[str, Any]:
        """Create a new player"""
        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute("""
                INSERT INTO players (user_id, guild_id)
                VALUES (?, ?)
            """, (user_id, guild_id))
            await db.commit()
        
        return await self.get_player(user_id)
    
    async def update_player(self, user_id: int, **kwargs):
        """Update player fields"""
        if not kwargs:
            return
        
        # Add updated_at timestamp
        kwargs['updated_at'] = datetime.utcnow().isoformat()
        
        fields = ", ".join(f"{k} = ?" for k in kwargs.keys())
        values = list(kwargs.values()) + [user_id]
        
        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute(
                f"UPDATE players SET {fields} WHERE user_id = ?",
                values
            )
            await db.commit()
    
    async def set_active(self, user_id: int, active: bool):
        """Set player active/inactive status"""
        await self.update_player(user_id, is_active=1 if active else 0)
        if not active:
            # Reset ignored missions counter when manually going inactive
            await self.update_player(user_id, ignored_missions=0)
    
    async def add_xp(self, user_id: int, xp: int) -> Dict[str, Any]:
        """Add XP and handle level ups"""
        player = await self.get_player(user_id)
        if not player:
            return {}
        
        new_xp = player['xp'] + xp
        old_level = player['station_level']
        new_level = new_xp // 1000  # XP_PER_LEVEL from config
        
        updates = {'xp': new_xp, 'station_level': new_level}
        
        # Level up bonus stats
        if new_level > old_level:
            levels_gained = new_level - old_level
            stat_bonus = levels_gained * 5  # LEVEL_STAT_BONUS from config
            # Distribute evenly across all stats
            bonus_per_stat = stat_bonus // 5
            updates.update({
                'stat_response': player['stat_response'] + bonus_per_stat,
                'stat_tactics': player['stat_tactics'] + bonus_per_stat,
                'stat_logistics': player['stat_logistics'] + bonus_per_stat,
                'stat_medical': player['stat_medical'] + bonus_per_stat,
                'stat_command': player['stat_command'] + bonus_per_stat,
            })
        
        await self.update_player(user_id, **updates)
        
        return {
            'leveled_up': new_level > old_level,
            'old_level': old_level,
            'new_level': new_level,
            'levels_gained': new_level - old_level if new_level > old_level else 0
        }
    
    async def create_mission(
        self,
        user_id: int,
        mission_id: int,
        mission_name: str,
        mission_data: str,
        tier: int,
        difficulty: int,
        timeout_seconds: int,
        max_stage: int = 1
    ) -> int:
        """Create a new active mission"""
        now = datetime.utcnow()
        expires_at = now + timedelta(seconds=timeout_seconds)
        
        async with aiosqlite.connect(str(self.db_path)) as db:
            cursor = await db.execute("""
                INSERT INTO active_missions (
                    user_id, mission_id, mission_name, mission_data,
                    tier, difficulty, assigned_time, expires_at, max_stage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id, mission_id, mission_name, mission_data,
                tier, difficulty, now.isoformat(), expires_at.isoformat(), max_stage
            ))
            await db.commit()
            return cursor.lastrowid
    
    async def get_active_mission(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get player's current active mission"""
        async with aiosqlite.connect(str(self.db_path)) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM active_missions
                WHERE user_id = ? AND status = 'pending'
                ORDER BY assigned_time DESC LIMIT 1
            """, (user_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None
    
    async def get_mission_by_id(self, mission_instance_id: int) -> Optional[Dict[str, Any]]:
        """Get mission by instance ID"""
        async with aiosqlite.connect(str(self.db_path)) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM active_missions WHERE mission_instance_id = ?",
                (mission_instance_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None
    
    async def update_mission(self, mission_instance_id: int, **kwargs):
        """Update mission fields"""
        if not kwargs:
            return
        
        fields = ", ".join(f"{k} = ?" for k in kwargs.keys())
        values = list(kwargs.values()) + [mission_instance_id]
        
        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute(
                f"UPDATE active_missions SET {fields} WHERE mission_instance_id = ?",
                values
            )
            await db.commit()
    
    async def complete_mission(
        self,
        mission_instance_id: int,
        outcome: str,
        credits_earned: int,
        xp_earned: int,
        morale_change: int
    ):
        """Complete a mission and record history"""
        mission = await self.get_mission_by_id(mission_instance_id)
        if not mission:
            return
        
        async with aiosqlite.connect(str(self.db_path)) as db:
            # Update mission status
            await db.execute(
                "UPDATE active_missions SET status = ? WHERE mission_instance_id = ?",
                (outcome, mission_instance_id)
            )
            
            # Record history
            await db.execute("""
                INSERT INTO mission_history (
                    user_id, mission_id, mission_name, tier, outcome,
                    credits_earned, xp_earned, morale_change
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                mission['user_id'], mission['mission_id'], mission['mission_name'],
                mission['tier'], outcome, credits_earned, xp_earned, morale_change
            ))
            
            await db.commit()
    
    async def start_training(self, user_id: int, stat_type: str) -> int:
        """Start a training session"""
        now = datetime.utcnow()
        completes_at = now + timedelta(hours=1)  # TRAINING_DURATION_HOURS
        
        async with aiosqlite.connect(str(self.db_path)) as db:
            cursor = await db.execute("""
                INSERT INTO training (user_id, stat_type, started_at, completes_at)
                VALUES (?, ?, ?, ?)
            """, (user_id, stat_type, now.isoformat(), completes_at.isoformat()))
            await db.commit()
            return cursor.lastrowid
    
    async def get_active_training(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Get active training session"""
        async with aiosqlite.connect(str(self.db_path)) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM training
                WHERE user_id = ? AND is_complete = 0
                ORDER BY started_at DESC LIMIT 1
            """, (user_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None
    
    async def complete_training(self, training_id: int):
        """Mark training as complete"""
        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute(
                "UPDATE training SET is_complete = 1 WHERE id = ?",
                (training_id,)
            )
            await db.commit()
    
    async def get_leaderboard(self, stat: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get leaderboard"""
        valid_stats = {
            'level': 'station_level',
            'missions': 'total_missions',
            'streak': 'mission_streak',
            'credits': 'credits',
            'success_rate': 'successful_missions'
        }
        
        if stat not in valid_stats:
            return []
        
        order_by = valid_stats[stat]
        
        async with aiosqlite.connect(str(self.db_path)) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(f"""
                SELECT user_id, station_level, total_missions, mission_streak,
                       credits, successful_missions, failed_missions
                FROM players
                WHERE total_missions > 0
                ORDER BY {order_by} DESC
                LIMIT ?
            """, (limit,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    async def cache_missions(self, missions: List[Dict[str, Any]]):
        """Cache mission data"""
        import json
        
        async with aiosqlite.connect(str(self.db_path)) as db:
            # Clear old cache
            await db.execute("DELETE FROM mission_cache")
            
            # Insert new cache
            for mission in missions:
                await db.execute("""
                    INSERT INTO mission_cache (id, data)
                    VALUES (?, ?)
                """, (mission.get('id'), json.dumps(mission)))
            
            await db.commit()
    
    async def get_cached_missions(self) -> List[Dict[str, Any]]:
        """Get cached missions"""
        import json
        
        async with aiosqlite.connect(str(self.db_path)) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM mission_cache") as cursor:
                rows = await cursor.fetchall()
                return [json.loads(row['data']) for row in rows]
    
    async def get_config(self, key: str) -> Optional[str]:
        """Get config value"""
        async with aiosqlite.connect(str(self.db_path)) as db:
            async with db.execute(
                "SELECT value FROM config WHERE key = ?", (key,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return row[0]
                return None
    
    async def set_config(self, key: str, value: str):
        """Set config value"""
        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute("""
                INSERT OR REPLACE INTO config (key, value)
                VALUES (?, ?)
            """, (key, value))
            await db.commit()
    
    async def clean_expired_missions(self):
        """Clean up expired missions"""
        now = datetime.utcnow().isoformat()
        
        async with aiosqlite.connect(str(self.db_path)) as db:
            # Get expired missions that are still pending
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT mission_instance_id, user_id FROM active_missions
                WHERE status = 'pending' AND expires_at < ?
            """, (now,)) as cursor:
                expired = await cursor.fetchall()
            
            # Mark as timeout and update player stats
            for mission in expired:
                await db.execute("""
                    UPDATE active_missions SET status = 'timeout'
                    WHERE mission_instance_id = ?
                """, (mission['mission_instance_id'],))
                
                # Increment ignored missions
                await db.execute("""
                    UPDATE players SET ignored_missions = ignored_missions + 1
                    WHERE user_id = ?
                """, (mission['user_id'],))
            
            await db.commit()
            
            return len(expired)
