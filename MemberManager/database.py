"""
Database layer for MemberManager
Handles all SQLite operations with aiosqlite + Audit Log System
"""

import aiosqlite
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any

log = logging.getLogger("red.FARA.MemberManager.database")


def _timestamp() -> int:
    """Get current Unix timestamp."""
    return int(datetime.now(timezone.utc).timestamp())


def _generate_ref_code(prefix: str, year: int, sequence: int) -> str:
    """Generate reference code like N2025-000123 or INF-DC-2025-000123."""
    return f"{prefix}{year}-{sequence:06d}"


def _hash_content(text: str) -> str:
    """Generate SHA256 hash for tamper detection."""
    return hashlib.sha256(text.encode()).hexdigest()


class MemberDatabase:
    """SQLite database for MemberManager with full audit trail."""
    
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self._conn: Optional[aiosqlite.Connection] = None
    
    async def initialize(self):
        """Initialize database and create tables."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row
        
        await self._create_tables()
        
        log.info(f"Database initialized at {self.db_path}")
    
    async def close(self):
        """Close database connection."""
        if self._conn:
            await self._conn.close()
    
    async def _create_tables(self):
        """Create all database tables."""
        # Notes table
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS notes (
                note_id INTEGER PRIMARY KEY AUTOINCREMENT,
                ref_code TEXT UNIQUE NOT NULL,
                guild_id INTEGER NOT NULL,
                discord_id INTEGER,
                mc_user_id TEXT,
                note_text TEXT NOT NULL,
                author_id INTEGER NOT NULL,
                author_name TEXT NOT NULL,
                infraction_ref TEXT,
                sanction_ref INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER,
                updated_by INTEGER,
                updated_by_name TEXT,
                expires_at INTEGER,
                content_hash TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                is_pinned INTEGER DEFAULT 0,
                tags TEXT
            )
        """)
        
        # Infractions table
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS infractions (
                infraction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                ref_code TEXT UNIQUE NOT NULL,
                guild_id INTEGER NOT NULL,
                discord_id INTEGER,
                mc_user_id TEXT,
                target_name TEXT,
                platform TEXT NOT NULL,
                infraction_type TEXT NOT NULL,
                reason TEXT NOT NULL,
                duration INTEGER,
                moderator_id INTEGER NOT NULL,
                moderator_name TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER,
                revoked_at INTEGER,
                revoked_by INTEGER,
                revoke_reason TEXT,
                severity_score INTEGER DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'active'
            )
        """)
        
        # Member events table
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS member_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                discord_id INTEGER,
                mc_user_id TEXT,
                event_type TEXT NOT NULL,
                event_data TEXT NOT NULL,
                triggered_by TEXT,
                actor_id INTEGER,
                timestamp INTEGER NOT NULL,
                notes TEXT
            )
        """)
        
        # 🆕 AUDIT LOG TABLE
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                discord_id INTEGER,
                mc_user_id TEXT,
                action_type TEXT NOT NULL,
                action_target TEXT NOT NULL,
                actor_id INTEGER NOT NULL,
                actor_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                timestamp INTEGER NOT NULL,
                metadata TEXT
            )
        """)
        
        # Watchlist table
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS watchlist (
                watchlist_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                discord_id INTEGER,
                mc_user_id TEXT,
                reason TEXT NOT NULL,
                added_by INTEGER NOT NULL,
                added_at INTEGER NOT NULL,
                watch_type TEXT NOT NULL,
                alert_threshold TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                resolved_at INTEGER,
                resolved_by INTEGER,
                resolution_notes TEXT,
                UNIQUE(guild_id, discord_id, mc_user_id, status)
            )
        """)
        
        # Role history table
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS role_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mc_user_id TEXT NOT NULL,
                old_role TEXT,
                new_role TEXT NOT NULL,
                detected_at INTEGER NOT NULL,
                source TEXT NOT NULL DEFAULT 'scraper',
                notes TEXT
            )
        """)
        
        # Create indices
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_notes_discord ON notes(discord_id)")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_notes_mc ON notes(mc_user_id)")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_notes_ref ON notes(ref_code)")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_infractions_discord ON infractions(discord_id)")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_infractions_mc ON infractions(mc_user_id)")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_events_discord ON member_events(discord_id)")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_events_mc ON member_events(mc_user_id)")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_discord ON audit_log(discord_id)")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_mc ON audit_log(mc_user_id)")
        await self._conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_target ON audit_log(action_target)")
        
        await self._conn.commit()
    
    # ==================== AUDIT LOG ====================
    
    async def log_action(
        self,
        guild_id: int,
        action_type: str,
        action_target: str,
        actor_id: int,
        actor_name: str,
        discord_id: Optional[int] = None,
        mc_user_id: Optional[str] = None,
        old_value: Optional[str] = None,
        new_value: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Log an action to audit trail.
        
        Action types: note_created, note_edited, note_deleted, infraction_added, etc.
        """
        metadata_json = json.dumps(metadata) if metadata else None
        
        cursor = await self._conn.execute(
            """
            INSERT INTO audit_log (
                guild_id, discord_id, mc_user_id, action_type, action_target,
                actor_id, actor_name, old_value, new_value, timestamp, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id, discord_id, mc_user_id, action_type, action_target,
                actor_id, actor_name, old_value, new_value, _timestamp(), metadata_json
            )
        )
        await self._conn.commit()
        
        return cursor.lastrowid
    
    async def get_audit_log(
        self,
        discord_id: Optional[int] = None,
        mc_user_id: Optional[str] = None,
        action_type: Optional[str] = None,
        action_target: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get audit log entries."""
        query = "SELECT * FROM audit_log WHERE 1=1"
        params = []
        
        if discord_id:
            query += " AND discord_id=?"
            params.append(discord_id)
        if mc_user_id:
            query += " AND mc_user_id=?"
            params.append(mc_user_id)
        if action_type:
            query += " AND action_type=?"
            params.append(action_type)
        if action_target:
            query += " AND action_target=?"
            params.append(action_target)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    
    # ==================== NOTES ====================
    
    async def add_note(
        self,
        guild_id: int,
        discord_id: Optional[int],
        mc_user_id: Optional[str],
        note_text: str,
        author_id: int,
        author_name: str,
        infraction_ref: Optional[str] = None,
        expires_days: Optional[int] = None,
        tags: Optional[List[str]] = None
    ) -> str:
        """Add a new note. Returns ref_code."""
        # Generate ref code
        year = datetime.now().year
        cursor = await self._conn.execute(
            "SELECT COUNT(*) FROM notes WHERE ref_code LIKE ?",
            (f"N{year}-%",)
        )
        count = (await cursor.fetchone())[0]
        ref_code = _generate_ref_code("N", year, count + 1)
        
        # Calculate expiry
        expires_at = None
        if expires_days:
            expires_at = _timestamp() + (expires_days * 86400)
        
        # Generate hash
        content_hash = _hash_content(note_text)
        
        # Serialize tags
        tags_json = json.dumps(tags) if tags else None
        
        await self._conn.execute(
            """
            INSERT INTO notes (
                ref_code, guild_id, discord_id, mc_user_id, note_text,
                author_id, author_name, infraction_ref, created_at,
                expires_at, content_hash, tags
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ref_code, guild_id, discord_id, mc_user_id, note_text,
                author_id, author_name, infraction_ref, _timestamp(),
                expires_at, content_hash, tags_json
            )
        )
        await self._conn.commit()
        
        # 🆕 LOG TO AUDIT
        await self.log_action(
            guild_id=guild_id,
            action_type="note_created",
            action_target=ref_code,
            actor_id=author_id,
            actor_name=author_name,
            discord_id=discord_id,
            mc_user_id=mc_user_id,
            new_value=note_text[:100]  # Preview
        )
        
        log.info(f"Created note {ref_code} for discord={discord_id}, mc={mc_user_id}")
        return ref_code
    
    async def get_notes(
        self,
        discord_id: Optional[int] = None,
        mc_user_id: Optional[str] = None,
        ref_code: Optional[str] = None,
        status: str = "active",
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get notes for a member."""
        query = "SELECT * FROM notes WHERE 1=1"
        params = []
        
        if ref_code:
            query += " AND ref_code=?"
            params.append(ref_code)
        else:
            if discord_id:
                query += " AND discord_id=?"
                params.append(discord_id)
            if mc_user_id:
                query += " AND mc_user_id=?"
                params.append(mc_user_id)
            if status:
                query += " AND status=?"
                params.append(status)
        
        query += " ORDER BY is_pinned DESC, created_at DESC LIMIT ?"
        params.append(limit)
        
        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    
    async def update_note(
        self,
        ref_code: str,
        new_text: str,
        updated_by: int,
        updated_by_name: str
    ) -> bool:
        """Update note text with audit trail."""
        # Get old text first
        cursor = await self._conn.execute(
            "SELECT note_text, discord_id, mc_user_id, guild_id FROM notes WHERE ref_code=?",
            (ref_code,)
        )
        old_row = await cursor.fetchone()
        
        if not old_row:
            return False
        
        old_text = old_row["note_text"]
        new_hash = _hash_content(new_text)
        
        result = await self._conn.execute(
            """
            UPDATE notes 
            SET note_text=?, updated_at=?, updated_by=?, updated_by_name=?, content_hash=?
            WHERE ref_code=?
            """,
            (new_text, _timestamp(), updated_by, updated_by_name, new_hash, ref_code)
        )
        await self._conn.commit()
        
        if result.rowcount > 0:
            # 🆕 LOG TO AUDIT
            await self.log_action(
                guild_id=old_row["guild_id"],
                action_type="note_edited",
                action_target=ref_code,
                actor_id=updated_by,
                actor_name=updated_by_name,
                discord_id=old_row["discord_id"],
                mc_user_id=old_row["mc_user_id"],
                old_value=old_text[:100],
                new_value=new_text[:100]
            )
        
        return result.rowcount > 0
    
    async def delete_note(
        self,
        ref_code: str,
        deleted_by: int,
        deleted_by_name: str
    ) -> bool:
        """Soft delete a note."""
        # Get note info first
        cursor = await self._conn.execute(
            "SELECT discord_id, mc_user_id, guild_id, note_text FROM notes WHERE ref_code=?",
            (ref_code,)
        )
        row = await cursor.fetchone()
        
        if not row:
            return False
        
        result = await self._conn.execute(
            "UPDATE notes SET status='deleted' WHERE ref_code=?",
            (ref_code,)
        )
        await self._conn.commit()
        
        if result.rowcount > 0:
            # 🆕 LOG TO AUDIT
            await self.log_action(
                guild_id=row["guild_id"],
                action_type="note_deleted",
                action_target=ref_code,
                actor_id=deleted_by,
                actor_name=deleted_by_name,
                discord_id=row["discord_id"],
                mc_user_id=row["mc_user_id"],
                old_value=row["note_text"][:100]
            )
        
        return result.rowcount > 0
    
    # ==================== INFRACTIONS ====================
    
    async def add_infraction(
        self,
        guild_id: int,
        discord_id: Optional[int],
        mc_user_id: Optional[str],
        target_name: str,
        platform: str,
        infraction_type: str,
        reason: str,
        moderator_id: int,
        moderator_name: str,
        duration: Optional[int] = None,
        severity_score: int = 1
    ) -> str:
        """Add a new infraction. Returns ref_code."""
        # Generate ref code
        year = datetime.now().year
        platform_prefix = "DC" if platform == "discord" else "MC"
        prefix = f"INF-{platform_prefix}-"
        
        cursor = await self._conn.execute(
            "SELECT COUNT(*) FROM infractions WHERE ref_code LIKE ?",
            (f"{prefix}{year}-%",)
        )
        count = (await cursor.fetchone())[0]
        ref_code = f"{prefix}{year}-{count + 1:06d}"
        
        # Calculate expiry for temp punishments
        expires_at = None
        if duration:
            expires_at = _timestamp() + duration
        
        await self._conn.execute(
            """
            INSERT INTO infractions (
                ref_code, guild_id, discord_id, mc_user_id, target_name,
                platform, infraction_type, reason, duration, moderator_id,
                moderator_name, created_at, expires_at, severity_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ref_code, guild_id, discord_id, mc_user_id, target_name,
                platform, infraction_type, reason, duration, moderator_id,
                moderator_name, _timestamp(), expires_at, severity_score
            )
        )
        await self._conn.commit()
        
        # 🆕 LOG TO AUDIT
        await self.log_action(
            guild_id=guild_id,
            action_type="infraction_added",
            action_target=ref_code,
            actor_id=moderator_id,
            actor_name=moderator_name,
            discord_id=discord_id,
            mc_user_id=mc_user_id,
            new_value=f"{infraction_type}: {reason[:50]}"
        )
        
        log.info(f"Created infraction {ref_code} for discord={discord_id}, mc={mc_user_id}")
        return ref_code
    
    async def get_infractions(
        self,
        discord_id: Optional[int] = None,
        mc_user_id: Optional[str] = None,
        platform: Optional[str] = None,
        status: str = "active",
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get infractions for a member."""
        query = "SELECT * FROM infractions WHERE 1=1"
        params = []
        
        if discord_id:
            query += " AND discord_id=?"
            params.append(discord_id)
        if mc_user_id:
            query += " AND mc_user_id=?"
            params.append(mc_user_id)
        if platform:
            query += " AND platform=?"
            params.append(platform)
        if status:
            query += " AND status=?"
            params.append(status)
        
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        
        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    
    async def revoke_infraction(
        self,
        ref_code: str,
        revoked_by: int,
        revoked_by_name: str,
        reason: str
    ) -> bool:
        """Revoke an infraction."""
        # Get infraction info first
        cursor = await self._conn.execute(
            "SELECT discord_id, mc_user_id, guild_id FROM infractions WHERE ref_code=?",
            (ref_code,)
        )
        row = await cursor.fetchone()
        
        if not row:
            return False
        
        result = await self._conn.execute(
            """
            UPDATE infractions 
            SET status='revoked', revoked_at=?, revoked_by=?, revoke_reason=?
            WHERE ref_code=?
            """,
            (_timestamp(), revoked_by, reason, ref_code)
        )
        await self._conn.commit()
        
        if result.rowcount > 0:
            # 🆕 LOG TO AUDIT
            await self.log_action(
                guild_id=row["guild_id"],
                action_type="infraction_revoked",
                action_target=ref_code,
                actor_id=revoked_by,
                actor_name=revoked_by_name,
                discord_id=row["discord_id"],
                mc_user_id=row["mc_user_id"],
                new_value=reason
            )
        
        return result.rowcount > 0
    
    async def expire_old_infractions(self) -> int:
        """Auto-expire infractions that have passed their expiry date."""
        result = await self._conn.execute(
            """
            UPDATE infractions 
            SET status='expired'
            WHERE expires_at IS NOT NULL 
            AND expires_at < ? 
            AND status='active'
            """,
            (_timestamp(),)
        )
        await self._conn.commit()
        
        return result.rowcount
    
    # ==================== EVENTS ====================
    
    async def add_event(
        self,
        guild_id: int,
        discord_id: Optional[int],
        mc_user_id: Optional[str],
        event_type: str,
        event_data: Dict[str, Any],
        triggered_by: str,
        actor_id: Optional[int] = None,
        notes: Optional[str] = None
    ) -> int:
        """Add a member event. Returns event_id."""
        event_data_json = json.dumps(event_data)
        
        cursor = await self._conn.execute(
            """
            INSERT INTO member_events (
                guild_id, discord_id, mc_user_id, event_type, event_data,
                triggered_by, actor_id, timestamp, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id, discord_id, mc_user_id, event_type, event_data_json,
                triggered_by, actor_id, _timestamp(), notes
            )
        )
        await self._conn.commit()
        
        return cursor.lastrowid
    
    async def get_events(
        self,
        discord_id: Optional[int] = None,
        mc_user_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Get member events."""
        query = "SELECT * FROM member_events WHERE 1=1"
        params = []
        
        if discord_id:
            query += " AND discord_id=?"
            params.append(discord_id)
        if mc_user_id:
            query += " AND mc_user_id=?"
            params.append(mc_user_id)
        if event_type:
            query += " AND event_type=?"
            params.append(event_type)
        
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        
        cursor = await self._conn.execute(query, params)
        rows = await cursor.fetchall()
        return [dict(row) for row in rows]
    
    # ==================== STATS ====================
    
    async def get_member_stats(
        self,
        discord_id: Optional[int] = None,
        mc_user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get aggregated stats for a member."""
        stats = {
            "total_notes": 0,
            "active_notes": 0,
            "pinned_notes": 0,
            "total_infractions": 0,
            "active_infractions": 0,
            "severity_score": 0,
            "events_count": 0,
            "on_watchlist": False
        }
        
        # Notes stats
        query = "SELECT COUNT(*) as total, SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as active, SUM(is_pinned) as pinned FROM notes WHERE"
        params = []
        conditions = []
        
        if discord_id:
            conditions.append("discord_id=?")
            params.append(discord_id)
        if mc_user_id:
            conditions.append("mc_user_id=?")
            params.append(mc_user_id)
        
        if conditions:
            query += " " + " OR ".join(conditions)
            cursor = await self._conn.execute(query, params)
            row = await cursor.fetchone()
            if row:
                stats["total_notes"] = row["total"] or 0
                stats["active_notes"] = row["active"] or 0
                stats["pinned_notes"] = row["pinned"] or 0
        
        # Infractions stats
        query = "SELECT COUNT(*) as total, SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as active, SUM(severity_score) as severity FROM infractions WHERE"
        params = []
        conditions = []
        
        if discord_id:
            conditions.append("discord_id=?")
            params.append(discord_id)
        if mc_user_id:
            conditions.append("mc_user_id=?")
            params.append(mc_user_id)
        
        if conditions:
            query += " " + " OR ".join(conditions)
            cursor = await self._conn.execute(query, params)
            row = await cursor.fetchone()
            if row:
                stats["total_infractions"] = row["total"] or 0
                stats["active_infractions"] = row["active"] or 0
                stats["severity_score"] = row["severity"] or 0
        
        # Events count
        query = "SELECT COUNT(*) FROM member_events WHERE"
        params = []
        conditions = []
        
        if discord_id:
            conditions.append("discord_id=?")
            params.append(discord_id)
        if mc_user_id:
            conditions.append("mc_user_id=?")
            params.append(mc_user_id)
        
        if conditions:
            query += " " + " OR ".join(conditions)
            cursor = await self._conn.execute(query, params)
            row = await cursor.fetchone()
            if row:
                stats["events_count"] = row[0] or 0
        
        return stats
