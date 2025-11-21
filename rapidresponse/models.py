"""
Database models for RapidResponse
Author: BrandjuhNL
"""

import aiosqlite
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict
import logging

log = logging.getLogger("red.rapidresponse.models")


class RapidResponseDB:
    """Database handler for Rapid Response game."""
    
    def __init__(self, db_path: Path):
        """Initialize database handler."""
        self.db_path = db_path
    
    async def initialize(self):
        """Create database tables if they don't exist."""
        async with aiosqlite.connect(self.db_path) as db:
            # Games table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS games (
                    game_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    channel_id INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    mode TEXT NOT NULL,
                    solo INTEGER NOT NULL,
                    entry_fee INTEGER NOT NULL,
                    total_pot INTEGER NOT NULL,
                    status TEXT NOT NULL
                )
            """)
            
            # Game players table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS game_players (
                    game_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    joined_at TEXT NOT NULL,
                    paid_entry INTEGER NOT NULL,
                    final_score REAL,
                    is_winner INTEGER DEFAULT 0,
                    winnings INTEGER DEFAULT 0,
                    PRIMARY KEY (game_id, user_id),
                    FOREIGN KEY (game_id) REFERENCES games(game_id)
                )
            """)
            
            # Rounds table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS rounds (
                    round_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id INTEGER NOT NULL,
                    mission_id TEXT NOT NULL,
                    mission_name TEXT NOT NULL,
                    requirements TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    time_limit INTEGER NOT NULL,
                    FOREIGN KEY (game_id) REFERENCES games(game_id)
                )
            """)
            
            # Round answers table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS round_answers (
                    round_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    score REAL NOT NULL,
                    answer_data TEXT NOT NULL,
                    perfect_match INTEGER DEFAULT 0,
                    PRIMARY KEY (round_id, user_id),
                    FOREIGN KEY (round_id) REFERENCES rounds(round_id)
                )
            """)
            
            await db.commit()
        
        log.info(f"RapidResponse database initialized at {self.db_path}")
    
    async def create_game(self, guild_id: int, channel_id: int, mode: str, 
                         solo: bool, entry_fee: int, total_pot: int) -> int:
        """Create a new game and return its ID."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                INSERT INTO games (guild_id, channel_id, started_at, mode, solo, 
                                 entry_fee, total_pot, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (guild_id, channel_id, datetime.utcnow().isoformat(), mode,
                  1 if solo else 0, entry_fee, total_pot, 'lobby'))
            await db.commit()
            return cursor.lastrowid
    
    async def add_player(self, game_id: int, user_id: int, entry_fee: int):
        """Add a player to a game."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO game_players (game_id, user_id, joined_at, paid_entry)
                VALUES (?, ?, ?, ?)
            """, (game_id, user_id, datetime.utcnow().isoformat(), entry_fee))
            await db.commit()
    
    async def create_round(self, game_id: int, mission_id: str, mission_name: str,
                          requirements: Dict, time_limit: int) -> int:
        """Create a new round and return its ID."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                INSERT INTO rounds (game_id, mission_id, mission_name, requirements,
                                  started_at, time_limit)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (game_id, mission_id, mission_name, json.dumps(requirements),
                  datetime.utcnow().isoformat(), time_limit))
            await db.commit()
            return cursor.lastrowid
    
    async def save_answer(self, round_id: int, user_id: int, score: float,
                         answer_data: Dict, perfect_match: bool):
        """Save a player's answer."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO round_answers 
                (round_id, user_id, score, answer_data, perfect_match)
                VALUES (?, ?, ?, ?, ?)
            """, (round_id, user_id, score, json.dumps(answer_data),
                  1 if perfect_match else 0))
            await db.commit()
    
    async def end_round(self, round_id: int):
        """Mark a round as ended."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                UPDATE rounds SET ended_at = ? WHERE round_id = ?
            """, (datetime.utcnow().isoformat(), round_id))
            await db.commit()
    
    async def update_player_score(self, game_id: int, user_id: int, score: float):
        """Update a player's final score."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                UPDATE game_players SET final_score = ? 
                WHERE game_id = ? AND user_id = ?
            """, (score, game_id, user_id))
            await db.commit()
    
    async def set_winner(self, game_id: int, user_id: int, winnings: int):
        """Mark a player as winner and set their winnings."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                UPDATE game_players 
                SET is_winner = 1, winnings = ?
                WHERE game_id = ? AND user_id = ?
            """, (winnings, game_id, user_id))
            await db.commit()
    
    async def end_game(self, game_id: int, status: str = 'completed'):
        """Mark a game as ended."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                UPDATE games SET ended_at = ?, status = ?
                WHERE game_id = ?
            """, (datetime.utcnow().isoformat(), status, game_id))
            await db.commit()
    
    async def get_unfinished_games(self, guild_id: int) -> List[Dict]:
        """Get all unfinished games for a guild."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("""
                SELECT * FROM games 
                WHERE guild_id = ? AND status IN ('lobby', 'running')
            """, (guild_id,))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def get_game_players(self, game_id: int) -> List[Dict]:
        """Get all players in a game."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("""
                SELECT * FROM game_players WHERE game_id = ?
            """, (game_id,))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def get_player_stats(self, user_id: int, guild_id: Optional[int] = None) -> Dict:
        """Get player statistics."""
        async with aiosqlite.connect(self.db_path) as db:
            # Base query
            where_clause = "WHERE gp.user_id = ?"
            params = [user_id]
            
            if guild_id:
                where_clause += " AND g.guild_id = ?"
                params.append(guild_id)
            
            # Total games
            cursor = await db.execute(f"""
                SELECT COUNT(*) as total_games
                FROM game_players gp
                JOIN games g ON gp.game_id = g.game_id
                {where_clause} AND g.status = 'completed'
            """, params)
            total_games = (await cursor.fetchone())[0]
            
            # Total wins
            cursor = await db.execute(f"""
                SELECT COUNT(*) as total_wins
                FROM game_players gp
                JOIN games g ON gp.game_id = g.game_id
                {where_clause} AND gp.is_winner = 1 AND g.status = 'completed'
            """, params)
            total_wins = (await cursor.fetchone())[0]
            
            # Total credits won
            cursor = await db.execute(f"""
                SELECT COALESCE(SUM(winnings), 0) as total_winnings
                FROM game_players gp
                JOIN games g ON gp.game_id = g.game_id
                {where_clause} AND g.status = 'completed'
            """, params)
            total_winnings = (await cursor.fetchone())[0]
            
            # Average score
            cursor = await db.execute(f"""
                SELECT AVG(final_score) as avg_score
                FROM game_players gp
                JOIN games g ON gp.game_id = g.game_id
                {where_clause} AND g.status = 'completed' AND gp.final_score IS NOT NULL
            """, params)
            result = await cursor.fetchone()
            avg_score = result[0] if result[0] else 0.0
            
            # Perfect rounds
            cursor = await db.execute(f"""
                SELECT COUNT(*) as perfect_rounds
                FROM round_answers ra
                JOIN rounds r ON ra.round_id = r.round_id
                JOIN games g ON r.game_id = g.game_id
                WHERE ra.user_id = ? AND ra.perfect_match = 1 AND g.status = 'completed'
            """ + (" AND g.guild_id = ?" if guild_id else ""), 
            params if not guild_id else [user_id, guild_id])
            perfect_rounds = (await cursor.fetchone())[0]
            
            return {
                'total_games': total_games,
                'total_wins': total_wins,
                'total_winnings': total_winnings,
                'average_score': avg_score,
                'perfect_rounds': perfect_rounds,
                'win_rate': (total_wins / total_games * 100) if total_games > 0 else 0.0
            }
