"""
SQLite storage for the MissionChief possible missions publisher.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite


def utc_now() -> str:
    """Return an ISO timestamp with timezone information."""
    return datetime.now(timezone.utc).isoformat()


class MissionsDatabase:
    """Store guild configuration and Discord publication tracking."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)

    async def initialize(self) -> None:
        """Create the v2 tables without relying on the old forum-only schema."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS mission_config_v2 (
                    guild_id TEXT PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    auto_sync_enabled INTEGER NOT NULL DEFAULT 0,
                    last_sync_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS mission_publications_v2 (
                    guild_id TEXT NOT NULL,
                    mission_key TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    target_kind TEXT NOT NULL,
                    message_id TEXT,
                    thread_id TEXT,
                    content_hash TEXT NOT NULL,
                    title TEXT NOT NULL,
                    detail_url TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    PRIMARY KEY (guild_id, mission_key)
                )
                """
            )
            await db.commit()

    async def set_config(self, guild_id: int, channel_id: int) -> None:
        now = utc_now()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO mission_config_v2
                    (guild_id, channel_id, auto_sync_enabled, created_at, updated_at)
                VALUES (?, ?, 0, ?, ?)
                ON CONFLICT(guild_id) DO UPDATE SET
                    channel_id = excluded.channel_id,
                    updated_at = excluded.updated_at
                """,
                (str(guild_id), str(channel_id), now, now),
            )
            await db.commit()

    async def get_config(self, guild_id: int) -> dict[str, Any] | None:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM mission_config_v2 WHERE guild_id = ?",
                (str(guild_id),),
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def set_auto_sync(self, guild_id: int, enabled: bool) -> None:
        now = utc_now()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE mission_config_v2
                SET auto_sync_enabled = ?, updated_at = ?
                WHERE guild_id = ?
                """,
                (1 if enabled else 0, now, str(guild_id)),
            )
            await db.commit()

    async def update_last_sync(self, guild_id: int) -> None:
        now = utc_now()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE mission_config_v2
                SET last_sync_at = ?, updated_at = ?
                WHERE guild_id = ?
                """,
                (now, now, str(guild_id)),
            )
            await db.commit()

    async def get_publication(self, guild_id: int, mission_key: str) -> dict[str, Any] | None:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT * FROM mission_publications_v2
                WHERE guild_id = ? AND mission_key = ?
                """,
                (str(guild_id), mission_key),
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def upsert_publication(
        self,
        *,
        guild_id: int,
        mission_key: str,
        channel_id: int,
        target_kind: str,
        message_id: int | None,
        thread_id: int | None,
        content_hash: str,
        title: str,
        detail_url: str,
    ) -> None:
        now = utc_now()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO mission_publications_v2 (
                    guild_id,
                    mission_key,
                    channel_id,
                    target_kind,
                    message_id,
                    thread_id,
                    content_hash,
                    title,
                    detail_url,
                    created_at,
                    updated_at,
                    last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(guild_id, mission_key) DO UPDATE SET
                    channel_id = excluded.channel_id,
                    target_kind = excluded.target_kind,
                    message_id = excluded.message_id,
                    thread_id = excluded.thread_id,
                    content_hash = excluded.content_hash,
                    title = excluded.title,
                    detail_url = excluded.detail_url,
                    updated_at = excluded.updated_at,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    str(guild_id),
                    mission_key,
                    str(channel_id),
                    target_kind,
                    str(message_id) if message_id is not None else None,
                    str(thread_id) if thread_id is not None else None,
                    content_hash,
                    title,
                    detail_url,
                    now,
                    now,
                    now,
                ),
            )
            await db.commit()

    async def touch_publication(self, guild_id: int, mission_key: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE mission_publications_v2
                SET last_seen_at = ?
                WHERE guild_id = ? AND mission_key = ?
                """,
                (utc_now(), str(guild_id), mission_key),
            )
            await db.commit()

    async def get_all_publications(self, guild_id: int) -> list[dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """
                SELECT * FROM mission_publications_v2
                WHERE guild_id = ?
                ORDER BY mission_key
                """,
                (str(guild_id),),
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def get_statistics(self, guild_id: int) -> dict[str, int]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN target_kind = 'message' THEN 1 ELSE 0 END) AS messages,
                    SUM(CASE WHEN target_kind = 'forum_thread' THEN 1 ELSE 0 END) AS threads
                FROM mission_publications_v2
                WHERE guild_id = ?
                """,
                (str(guild_id),),
            ) as cursor:
                row = await cursor.fetchone()

        if not row:
            return {"total": 0, "messages": 0, "threads": 0}

        return {
            "total": int(row[0] or 0),
            "messages": int(row[1] or 0),
            "threads": int(row[2] or 0),
        }
