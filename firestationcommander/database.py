"""SQLite persistence for FireStationCommander."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite

from .constants import (
    DEFAULT_MORALE_SCORE,
    DEFAULT_SAFETY_SCORE,
    DEFAULT_START_CASH,
    INCIDENT_STATUS_ACTIVE,
    INCIDENT_STATUS_COMPLETED,
    INCIDENT_STATUS_IGNORED,
    VEHICLE_STATUS_AVAILABLE,
)
from .models import Incident, IncidentReport, Personnel, Player, Station, Vehicle

log = logging.getLogger("red.firestationcommander.database")


def utc_now() -> str:
    """Return an ISO timestamp in UTC."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class FireStationCommanderDatabase:
    """Small aiosqlite data access layer for the FireStationCommander game."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self._conn: aiosqlite.Connection | None = None

    async def initialize(self) -> None:
        """Open the database and create all tables if they do not exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA foreign_keys = ON")
        await self._conn.execute("PRAGMA journal_mode = WAL")
        await self._create_tables()
        await self._conn.commit()
        log.info("FireStationCommander database initialized at %s", self.db_path)

    async def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        """Return the active connection or raise if initialize was not called."""
        if self._conn is None:
            raise RuntimeError("FireStationCommander database is not initialized.")
        return self._conn

    async def _create_tables(self) -> None:
        """Create the MVP schema. This method is safe to run repeatedly."""
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                cash INTEGER NOT NULL,
                reputation INTEGER NOT NULL DEFAULT 0,
                command_level INTEGER NOT NULL DEFAULT 1,
                xp INTEGER NOT NULL DEFAULT 0,
                safety_score INTEGER NOT NULL DEFAULT 75,
                morale_score INTEGER NOT NULL DEFAULT 75,
                created_at TEXT NOT NULL,
                UNIQUE(guild_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS stations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL UNIQUE,
                name TEXT NOT NULL,
                level INTEGER NOT NULL DEFAULT 1,
                garage_slots INTEGER NOT NULL,
                storage_slots INTEGER NOT NULL,
                has_workshop INTEGER NOT NULL DEFAULT 0,
                has_training_room INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS personnel (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                rank TEXT NOT NULL,
                contract_type TEXT NOT NULL,
                salary INTEGER NOT NULL,
                xp INTEGER NOT NULL DEFAULT 0,
                condition_score INTEGER NOT NULL DEFAULT 100,
                stress_score INTEGER NOT NULL DEFAULT 0,
                morale_score INTEGER NOT NULL DEFAULT 75,
                leadership INTEGER NOT NULL DEFAULT 1,
                technical INTEGER NOT NULL DEFAULT 1,
                medical INTEGER NOT NULL DEFAULT 0,
                discipline INTEGER NOT NULL DEFAULT 1,
                available INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS personnel_trainings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                personnel_id INTEGER NOT NULL,
                training_key TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                UNIQUE(personnel_id, training_key),
                FOREIGN KEY(personnel_id) REFERENCES personnel(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS vehicles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                template_key TEXT NOT NULL,
                callsign TEXT NOT NULL,
                condition_score INTEGER NOT NULL DEFAULT 100,
                reliability_score INTEGER NOT NULL DEFAULT 100,
                fuel INTEGER NOT NULL DEFAULT 100,
                damage INTEGER NOT NULL DEFAULT 0,
                mileage INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'available',
                created_at TEXT NOT NULL,
                FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS equipment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL,
                template_key TEXT NOT NULL,
                condition_score INTEGER NOT NULL DEFAULT 100,
                assigned_vehicle_id INTEGER,
                created_at TEXT NOT NULL,
                FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
                FOREIGN KEY(assigned_vehicle_id) REFERENCES vehicles(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS incidents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                template_key TEXT NOT NULL,
                title TEXT NOT NULL,
                risk_level INTEGER NOT NULL,
                status TEXT NOT NULL,
                required_tags_json TEXT NOT NULL,
                required_vehicle_types_json TEXT NOT NULL,
                required_trainings_json TEXT NOT NULL,
                base_reward INTEGER NOT NULL,
                base_xp INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                completed_at TEXT,
                FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS incident_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                incident_id INTEGER NOT NULL,
                player_id INTEGER NOT NULL,
                score INTEGER NOT NULL,
                cash_reward INTEGER NOT NULL,
                xp_reward INTEGER NOT NULL,
                reputation_delta INTEGER NOT NULL,
                safety_delta INTEGER NOT NULL,
                summary TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(incident_id) REFERENCES incidents(id) ON DELETE CASCADE,
                FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE
            );
            """
        )

    async def get_player(self, guild_id: int, user_id: int) -> Player | None:
        """Return a player by guild and user ID."""
        row = await self._fetchone(
            "SELECT * FROM players WHERE guild_id = ? AND user_id = ?",
            (guild_id, user_id),
        )
        return Player.from_row(row) if row else None

    async def get_player_by_id(self, player_id: int) -> Player | None:
        """Return a player by primary key."""
        row = await self._fetchone("SELECT * FROM players WHERE id = ?", (player_id,))
        return Player.from_row(row) if row else None

    async def create_player(self, guild_id: int, user_id: int) -> Player:
        """Create and return a player row."""
        now = utc_now()
        cursor = await self.conn.execute(
            """
            INSERT INTO players (
                guild_id, user_id, cash, reputation, command_level, xp,
                safety_score, morale_score, created_at
            )
            VALUES (?, ?, ?, 0, 1, 0, ?, ?, ?)
            """,
            (guild_id, user_id, DEFAULT_START_CASH, DEFAULT_SAFETY_SCORE, DEFAULT_MORALE_SCORE, now),
        )
        await self.conn.commit()
        return (await self.get_player_by_id(int(cursor.lastrowid)))  # type: ignore[return-value]

    async def get_or_create_player(self, guild_id: int, user_id: int) -> tuple[Player, bool]:
        """Return an existing player or create one."""
        player = await self.get_player(guild_id, user_id)
        if player is not None:
            return player, False
        return await self.create_player(guild_id, user_id), True

    async def create_station(
        self,
        player_id: int,
        name: str,
        garage_slots: int,
        storage_slots: int,
    ) -> Station:
        """Create the player's starter station if it does not exist."""
        existing = await self.get_station(player_id)
        if existing is not None:
            return existing
        cursor = await self.conn.execute(
            """
            INSERT INTO stations (
                player_id, name, level, garage_slots, storage_slots,
                has_workshop, has_training_room, created_at
            )
            VALUES (?, ?, 1, ?, ?, 0, 0, ?)
            """,
            (player_id, name, garage_slots, storage_slots, utc_now()),
        )
        await self.conn.commit()
        return (await self.get_station_by_id(int(cursor.lastrowid)))  # type: ignore[return-value]

    async def get_station(self, player_id: int) -> Station | None:
        """Return the station for a player."""
        row = await self._fetchone("SELECT * FROM stations WHERE player_id = ?", (player_id,))
        return Station.from_row(row) if row else None

    async def get_station_by_id(self, station_id: int) -> Station | None:
        """Return a station by primary key."""
        row = await self._fetchone("SELECT * FROM stations WHERE id = ?", (station_id,))
        return Station.from_row(row) if row else None

    async def add_personnel(self, player_id: int, payload: dict[str, Any]) -> int:
        """Add one personnel member and return the row ID."""
        cursor = await self.conn.execute(
            """
            INSERT INTO personnel (
                player_id, name, rank, contract_type, salary, xp, condition_score,
                stress_score, morale_score, leadership, technical, medical,
                discipline, available, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                player_id,
                payload["name"],
                payload.get("rank", "Firefighter"),
                payload.get("contract_type", "volunteer"),
                int(payload.get("salary", 0)),
                int(payload.get("xp", 0)),
                int(payload.get("condition_score", 100)),
                int(payload.get("stress_score", 0)),
                int(payload.get("morale_score", 75)),
                int(payload.get("leadership", 1)),
                int(payload.get("technical", 1)),
                int(payload.get("medical", 0)),
                int(payload.get("discipline", 1)),
                1 if payload.get("available", True) else 0,
                utc_now(),
            ),
        )
        await self.conn.commit()
        return int(cursor.lastrowid)

    async def add_personnel_training(self, personnel_id: int, training_key: str) -> None:
        """Add one completed training to a personnel member."""
        await self.conn.execute(
            """
            INSERT OR IGNORE INTO personnel_trainings (personnel_id, training_key, completed_at)
            VALUES (?, ?, ?)
            """,
            (personnel_id, training_key, utc_now()),
        )
        await self.conn.commit()

    async def add_vehicle(self, player_id: int, payload: dict[str, Any]) -> int:
        """Add one vehicle and return the row ID."""
        cursor = await self.conn.execute(
            """
            INSERT INTO vehicles (
                player_id, template_key, callsign, condition_score, reliability_score,
                fuel, damage, mileage, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                player_id,
                payload["template_key"],
                payload["callsign"],
                int(payload.get("condition_score", 100)),
                int(payload.get("reliability_score", 100)),
                int(payload.get("fuel", 100)),
                int(payload.get("damage", 0)),
                int(payload.get("mileage", 0)),
                payload.get("status", VEHICLE_STATUS_AVAILABLE),
                utc_now(),
            ),
        )
        await self.conn.commit()
        return int(cursor.lastrowid)

    async def add_equipment(self, player_id: int, template_key: str, condition_score: int = 100) -> int:
        """Add one equipment item and return the row ID."""
        cursor = await self.conn.execute(
            """
            INSERT INTO equipment (player_id, template_key, condition_score, assigned_vehicle_id, created_at)
            VALUES (?, ?, ?, NULL, ?)
            """,
            (player_id, template_key, condition_score, utc_now()),
        )
        await self.conn.commit()
        return int(cursor.lastrowid)

    async def list_personnel(self, player_id: int) -> list[Personnel]:
        """List all personnel for a player."""
        rows = await self._fetchall(
            "SELECT * FROM personnel WHERE player_id = ? ORDER BY id",
            (player_id,),
        )
        return [Personnel.from_row(row) for row in rows]

    async def list_vehicles(self, player_id: int) -> list[Vehicle]:
        """List all vehicles for a player."""
        rows = await self._fetchall(
            "SELECT * FROM vehicles WHERE player_id = ? ORDER BY id",
            (player_id,),
        )
        return [Vehicle.from_row(row) for row in rows]

    async def list_equipment(self, player_id: int) -> list[aiosqlite.Row]:
        """List all equipment for a player."""
        return await self._fetchall(
            "SELECT * FROM equipment WHERE player_id = ? ORDER BY id",
            (player_id,),
        )

    async def trainings_for_personnel(self, personnel_id: int) -> list[str]:
        """Return completed training keys for a personnel member."""
        rows = await self._fetchall(
            "SELECT training_key FROM personnel_trainings WHERE personnel_id = ? ORDER BY training_key",
            (personnel_id,),
        )
        return [str(row["training_key"]) for row in rows]

    async def create_incident(
        self,
        guild_id: int,
        player_id: int,
        template: dict[str, Any],
        expires_at: str,
    ) -> Incident:
        """Create an active incident from a template."""
        cursor = await self.conn.execute(
            """
            INSERT INTO incidents (
                guild_id, player_id, template_key, title, risk_level, status,
                required_tags_json, required_vehicle_types_json, required_trainings_json,
                base_reward, base_xp, started_at, expires_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                guild_id,
                player_id,
                template["key"],
                template["title"],
                int(template.get("risk_level", 1)),
                INCIDENT_STATUS_ACTIVE,
                json.dumps(template.get("required_tags", [])),
                json.dumps(template.get("required_vehicle_types", [])),
                json.dumps(template.get("required_trainings", [])),
                int(template.get("base_reward", 0)),
                int(template.get("base_xp", 0)),
                utc_now(),
                expires_at,
            ),
        )
        await self.conn.commit()
        return (await self.get_incident(int(cursor.lastrowid)))  # type: ignore[return-value]

    async def get_incident(self, incident_id: int) -> Incident | None:
        """Return an incident by ID."""
        row = await self._fetchone("SELECT * FROM incidents WHERE id = ?", (incident_id,))
        return Incident.from_row(row) if row else None

    async def get_active_incident(self, player_id: int) -> Incident | None:
        """Return the player's active incident, if any."""
        row = await self._fetchone(
            """
            SELECT * FROM incidents
            WHERE player_id = ? AND status = ?
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (player_id, INCIDENT_STATUS_ACTIVE),
        )
        return Incident.from_row(row) if row else None

    async def mark_incident_ignored(self, incident_id: int) -> None:
        """Mark an active incident as ignored."""
        await self.conn.execute(
            "UPDATE incidents SET status = ?, completed_at = ? WHERE id = ?",
            (INCIDENT_STATUS_IGNORED, utc_now(), incident_id),
        )
        await self.conn.commit()

    async def complete_incident(
        self,
        incident_id: int,
        player_id: int,
        score: int,
        cash_reward: int,
        xp_reward: int,
        reputation_delta: int,
        safety_delta: int,
        summary: str,
    ) -> IncidentReport:
        """Persist incident completion, create a report, and return it."""
        now = utc_now()
        await self.conn.execute(
            "UPDATE incidents SET status = ?, completed_at = ? WHERE id = ?",
            (INCIDENT_STATUS_COMPLETED, now, incident_id),
        )
        cursor = await self.conn.execute(
            """
            INSERT INTO incident_reports (
                incident_id, player_id, score, cash_reward, xp_reward,
                reputation_delta, safety_delta, summary, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                incident_id,
                player_id,
                score,
                cash_reward,
                xp_reward,
                reputation_delta,
                safety_delta,
                summary,
                now,
            ),
        )
        await self.conn.commit()
        return (await self.get_report(int(cursor.lastrowid)))  # type: ignore[return-value]

    async def get_report(self, report_id: int) -> IncidentReport | None:
        """Return one incident report."""
        row = await self._fetchone("SELECT * FROM incident_reports WHERE id = ?", (report_id,))
        return IncidentReport.from_row(row) if row else None

    async def latest_report(self, player_id: int) -> IncidentReport | None:
        """Return the newest incident report for a player."""
        row = await self._fetchone(
            """
            SELECT * FROM incident_reports
            WHERE player_id = ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            (player_id,),
        )
        return IncidentReport.from_row(row) if row else None

    async def update_player_after_incident(
        self,
        player_id: int,
        cash_delta: int,
        xp_delta: int,
        reputation_delta: int,
        safety_delta: int,
        new_level: int,
    ) -> Player:
        """Apply rewards and score changes after an incident."""
        await self.conn.execute(
            """
            UPDATE players
            SET cash = MAX(0, cash + ?),
                xp = xp + ?,
                command_level = ?,
                reputation = reputation + ?,
                safety_score = MAX(0, MIN(100, safety_score + ?))
            WHERE id = ?
            """,
            (cash_delta, xp_delta, new_level, reputation_delta, safety_delta, player_id),
        )
        await self.conn.commit()
        return (await self.get_player_by_id(player_id))  # type: ignore[return-value]

    async def spend_cash(self, player_id: int, amount: int) -> bool:
        """Spend player cash when enough is available."""
        player = await self.get_player_by_id(player_id)
        if player is None or player.cash < amount:
            return False
        await self.conn.execute(
            "UPDATE players SET cash = cash - ? WHERE id = ?",
            (amount, player_id),
        )
        await self.conn.commit()
        return True

    async def update_vehicle_after_incident(
        self,
        vehicle_id: int,
        condition_delta: int,
        fuel_delta: int,
        mileage_delta: int,
    ) -> None:
        """Apply wear to a dispatched vehicle."""
        await self.conn.execute(
            """
            UPDATE vehicles
            SET condition_score = MAX(0, MIN(100, condition_score + ?)),
                fuel = MAX(0, MIN(100, fuel + ?)),
                mileage = mileage + ?,
                damage = MAX(0, 100 - MAX(0, MIN(100, condition_score + ?))),
                status = ?
            WHERE id = ?
            """,
            (
                condition_delta,
                fuel_delta,
                mileage_delta,
                condition_delta,
                VEHICLE_STATUS_AVAILABLE,
                vehicle_id,
            ),
        )
        await self.conn.commit()

    async def update_personnel_after_incident(self, player_id: int, stress_delta: int) -> None:
        """Apply stress and condition changes to available personnel."""
        await self.conn.execute(
            """
            UPDATE personnel
            SET stress_score = MAX(0, MIN(100, stress_score + ?)),
                condition_score = MAX(0, MIN(100, condition_score - 2))
            WHERE player_id = ? AND available = 1
            """,
            (stress_delta, player_id),
        )
        await self.conn.commit()

    async def repair_vehicle(self, vehicle_id: int) -> None:
        """Restore one vehicle to operational condition."""
        await self.conn.execute(
            """
            UPDATE vehicles
            SET condition_score = 100,
                reliability_score = 100,
                fuel = 100,
                damage = 0,
                status = ?
            WHERE id = ?
            """,
            (VEHICLE_STATUS_AVAILABLE, vehicle_id),
        )
        await self.conn.commit()

    async def repair_equipment(self, equipment_id: int) -> None:
        """Restore one equipment item to full condition."""
        await self.conn.execute(
            "UPDATE equipment SET condition_score = 100 WHERE id = ?",
            (equipment_id,),
        )
        await self.conn.commit()

    async def _fetchone(self, query: str, params: tuple[Any, ...] = ()) -> aiosqlite.Row | None:
        cursor = await self.conn.execute(query, params)
        return await cursor.fetchone()

    async def _fetchall(self, query: str, params: tuple[Any, ...] = ()) -> list[aiosqlite.Row]:
        cursor = await self.conn.execute(query, params)
        rows = await cursor.fetchall()
        return list(rows)
