"""
Database operations for the missions database system.
"""

import aiosqlite
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List


class MissionsDatabase:
    """Handle all database operations for mission tracking."""
    
    def __init__(self, db_path: Path):
        """
        Initialize the database handler.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
    
    async def initialize(self):
        """Create database tables if they don't exist."""
        async with aiosqlite.connect(self.db_path) as db:
            # Mission posts tracking table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS mission_posts (
                    mission_id TEXT PRIMARY KEY,
                    thread_id TEXT NOT NULL,
                    mission_data_hash TEXT NOT NULL,
                    posted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP,
                    last_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Configuration table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS mission_sync_config (
                    guild_id TEXT PRIMARY KEY,
                    forum_channel_id TEXT NOT NULL,
                    admin_alert_channel_id TEXT,
                    auto_sync_enabled INTEGER DEFAULT 1,
                    last_full_sync TIMESTAMP,
                    missions_tag_name TEXT DEFAULT 'Missions'
                )
            """)
            
            await db.commit()
    
    async def set_config(self, guild_id: int, forum_channel_id: int, 
                        admin_alert_channel_id: Optional[int] = None):
        """
        Set or update guild configuration.
        
        Args:
            guild_id: Discord guild ID
            forum_channel_id: Forum channel ID for posting missions
            admin_alert_channel_id: Optional channel ID for admin alerts
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO mission_sync_config 
                (guild_id, forum_channel_id, admin_alert_channel_id)
                VALUES (?, ?, ?)
                ON CONFLICT(guild_id) DO UPDATE SET
                    forum_channel_id = excluded.forum_channel_id,
                    admin_alert_channel_id = excluded.admin_alert_channel_id
            """, (str(guild_id), str(forum_channel_id), 
                  str(admin_alert_channel_id) if admin_alert_channel_id else None))
            await db.commit()
    
    async def get_config(self, guild_id: int) -> Optional[Dict]:
        """
        Get guild configuration.
        
        Args:
            guild_id: Discord guild ID
            
        Returns:
            Dictionary with config data or None if not configured
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM mission_sync_config WHERE guild_id = ?
            """, (str(guild_id),)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None
    
    async def set_auto_sync(self, guild_id: int, enabled: bool):
        """
        Enable or disable automatic syncing.
        
        Args:
            guild_id: Discord guild ID
            enabled: Whether auto-sync should be enabled
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                UPDATE mission_sync_config 
                SET auto_sync_enabled = ?
                WHERE guild_id = ?
            """, (1 if enabled else 0, str(guild_id)))
            await db.commit()
    
    async def update_last_sync(self, guild_id: int):
        """
        Update the last full sync timestamp.
        
        Args:
            guild_id: Discord guild ID
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                UPDATE mission_sync_config 
                SET last_full_sync = ?
                WHERE guild_id = ?
            """, (datetime.utcnow().isoformat(), str(guild_id)))
            await db.commit()
    
    async def add_mission_post(self, mission_id: str, thread_id: int, 
                              mission_data_hash: str):
        """
        Add a new mission post to the database.
        
        Args:
            mission_id: Mission ID from the JSON
            thread_id: Discord thread/post ID
            mission_data_hash: Hash of the mission data for change detection
        """
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.utcnow().isoformat()
            await db.execute("""
                INSERT INTO mission_posts 
                (mission_id, thread_id, mission_data_hash, posted_at, last_check)
                VALUES (?, ?, ?, ?, ?)
            """, (mission_id, str(thread_id), mission_data_hash, now, now))
            await db.commit()
    
    async def update_mission_post(self, mission_id: str, thread_id: int, 
                                 mission_data_hash: str):
        """
        Update an existing mission post.
        
        Args:
            mission_id: Mission ID from the JSON
            thread_id: New Discord thread/post ID
            mission_data_hash: New hash of the mission data
        """
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.utcnow().isoformat()
            await db.execute("""
                UPDATE mission_posts
                SET thread_id = ?, 
                    mission_data_hash = ?,
                    last_updated = ?,
                    last_check = ?
                WHERE mission_id = ?
            """, (str(thread_id), mission_data_hash, now, now, mission_id))
            await db.commit()
    
    async def get_mission_post(self, mission_id: str) -> Optional[Dict]:
        """
        Get mission post data.
        
        Args:
            mission_id: Mission ID from the JSON
            
        Returns:
            Dictionary with post data or None if not found
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM mission_posts WHERE mission_id = ?
            """, (mission_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(row)
                return None
    
    async def get_all_mission_posts(self) -> List[Dict]:
        """
        Get all mission posts.
        
        Returns:
            List of dictionaries with post data
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("""
                SELECT * FROM mission_posts ORDER BY mission_id
            """) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]
    
    async def update_last_check(self, mission_id: str):
        """
        Update the last check timestamp for a mission.
        
        Args:
            mission_id: Mission ID from the JSON
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                UPDATE mission_posts
                SET last_check = ?
                WHERE mission_id = ?
            """, (datetime.utcnow().isoformat(), mission_id))
            await db.commit()
    
    async def delete_mission_post(self, mission_id: str):
        """
        Delete a mission post from the database.
        
        Args:
            mission_id: Mission ID from the JSON
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                DELETE FROM mission_posts WHERE mission_id = ?
            """, (mission_id,))
            await db.commit()
    
    async def clear_all_missions(self):
        """Delete all mission posts from the database."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM mission_posts")
            await db.commit()
    
    async def get_statistics(self) -> Dict:
        """
        Get database statistics.
        
        Returns:
            Dictionary with statistics
        """
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("""
                SELECT COUNT(*) as total_missions FROM mission_posts
            """) as cursor:
                row = await cursor.fetchone()
                total_missions = row[0] if row else 0
            
            async with db.execute("""
                SELECT COUNT(*) as updated_missions 
                FROM mission_posts 
                WHERE last_updated IS NOT NULL
            """) as cursor:
                row = await cursor.fetchone()
                updated_missions = row[0] if row else 0
            
            return {
                "total_missions": total_missions,
                "updated_missions": updated_missions
            }
