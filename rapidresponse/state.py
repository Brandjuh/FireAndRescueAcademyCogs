"""
Game state management for RapidResponse
Author: BrandjuhNL
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Set, Optional
import asyncio


@dataclass
class PlayerAnswer:
    """Represents a player's answer."""
    user_id: int
    vehicles: Dict[str, int] = field(default_factory=dict)
    last_message_time: float = 0.0
    
    def add_vehicles(self, new_vehicles: Dict[str, int]):
        """Add vehicles to the player's answer (accumulative)."""
        for vehicle_type, count in new_vehicles.items():
            self.vehicles[vehicle_type] = self.vehicles.get(vehicle_type, 0) + count


@dataclass
class GameState:
    """Represents the state of an active game."""
    game_id: int
    guild_id: int
    channel_id: int
    lobby_message_id: Optional[int] = None
    
    # Players
    players: Set[int] = field(default_factory=set)
    player_answers: Dict[int, PlayerAnswer] = field(default_factory=dict)
    
    # Game settings
    entry_fee: int = 1000
    lobby_duration: int = 60
    round_duration: int = 60
    
    # Game status
    status: str = 'lobby'  # lobby, running, completed, cancelled
    solo: bool = False
    total_pot: int = 0
    
    # Round info
    round_id: Optional[int] = None
    mission_id: Optional[str] = None
    mission_name: Optional[str] = None
    mission_requirements: Dict[str, int] = field(default_factory=dict)
    round_start_time: Optional[float] = None
    round_message_id: Optional[int] = None
    
    # Tasks
    lobby_task: Optional[asyncio.Task] = None
    round_task: Optional[asyncio.Task] = None
    
    def add_player(self, user_id: int):
        """Add a player to the game."""
        self.players.add(user_id)
        self.player_answers[user_id] = PlayerAnswer(user_id=user_id)
        self.total_pot += self.entry_fee
    
    def remove_player(self, user_id: int):
        """Remove a player from the game."""
        if user_id in self.players:
            self.players.remove(user_id)
            if user_id in self.player_answers:
                del self.player_answers[user_id]
            self.total_pot = max(0, self.total_pot - self.entry_fee)
    
    def can_player_answer(self, user_id: int, current_time: float, rate_limit: float = 2.0) -> bool:
        """Check if a player can submit an answer (rate limiting)."""
        if user_id not in self.player_answers:
            return False
        
        answer = self.player_answers[user_id]
        return (current_time - answer.last_message_time) >= rate_limit
    
    def record_answer(self, user_id: int, vehicles: Dict[str, int], current_time: float):
        """Record a player's answer."""
        if user_id in self.player_answers:
            answer = self.player_answers[user_id]
            answer.add_vehicles(vehicles)
            answer.last_message_time = current_time
    
    def cancel_tasks(self):
        """Cancel all running tasks."""
        if self.lobby_task and not self.lobby_task.done():
            self.lobby_task.cancel()
        if self.round_task and not self.round_task.done():
            self.round_task.cancel()


class GameManager:
    """Manages all active games across guilds."""
    
    def __init__(self):
        self.games: Dict[int, GameState] = {}  # channel_id -> GameState
        self.guild_games: Dict[int, Set[int]] = {}  # guild_id -> set of channel_ids
    
    def create_game(self, game_id: int, guild_id: int, channel_id: int,
                   entry_fee: int = 1000, lobby_duration: int = 60, 
                   round_duration: int = 60) -> GameState:
        """Create a new game."""
        game = GameState(
            game_id=game_id,
            guild_id=guild_id,
            channel_id=channel_id,
            entry_fee=entry_fee,
            lobby_duration=lobby_duration,
            round_duration=round_duration
        )
        
        self.games[channel_id] = game
        
        if guild_id not in self.guild_games:
            self.guild_games[guild_id] = set()
        self.guild_games[guild_id].add(channel_id)
        
        return game
    
    def get_game(self, channel_id: int) -> Optional[GameState]:
        """Get a game by channel ID."""
        return self.games.get(channel_id)
    
    def get_guild_games(self, guild_id: int) -> list[GameState]:
        """Get all games in a guild."""
        if guild_id not in self.guild_games:
            return []
        return [self.games[cid] for cid in self.guild_games[guild_id] if cid in self.games]
    
    def remove_game(self, channel_id: int):
        """Remove a game."""
        if channel_id in self.games:
            game = self.games[channel_id]
            game.cancel_tasks()
            
            # Remove from guild tracking
            if game.guild_id in self.guild_games:
                self.guild_games[game.guild_id].discard(channel_id)
                if not self.guild_games[game.guild_id]:
                    del self.guild_games[game.guild_id]
            
            del self.games[channel_id]
    
    def has_active_game(self, channel_id: int) -> bool:
        """Check if there's an active game in a channel."""
        return channel_id in self.games
