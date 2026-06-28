"""Dataclasses used by FireStationCommander services and commands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _value(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


@dataclass(slots=True)
class Player:
    id: int
    guild_id: int
    user_id: int
    cash: int
    reputation: int
    command_level: int
    xp: int
    safety_score: int
    morale_score: int

    @classmethod
    def from_row(cls, row: Any) -> "Player":
        return cls(
            id=int(_value(row, "id")),
            guild_id=int(_value(row, "guild_id")),
            user_id=int(_value(row, "user_id")),
            cash=int(_value(row, "cash", 0)),
            reputation=int(_value(row, "reputation", 0)),
            command_level=int(_value(row, "command_level", 1)),
            xp=int(_value(row, "xp", 0)),
            safety_score=int(_value(row, "safety_score", 75)),
            morale_score=int(_value(row, "morale_score", 75)),
        )


@dataclass(slots=True)
class Station:
    id: int
    player_id: int
    name: str
    level: int
    garage_slots: int
    storage_slots: int
    has_workshop: bool
    has_training_room: bool

    @classmethod
    def from_row(cls, row: Any) -> "Station":
        return cls(
            id=int(_value(row, "id")),
            player_id=int(_value(row, "player_id")),
            name=str(_value(row, "name", "Station")),
            level=int(_value(row, "level", 1)),
            garage_slots=int(_value(row, "garage_slots", 1)),
            storage_slots=int(_value(row, "storage_slots", 0)),
            has_workshop=bool(_value(row, "has_workshop", 0)),
            has_training_room=bool(_value(row, "has_training_room", 0)),
        )


@dataclass(slots=True)
class Vehicle:
    id: int
    player_id: int
    template_key: str
    callsign: str
    condition_score: int
    reliability_score: int
    fuel: int
    damage: int
    mileage: int
    status: str

    @classmethod
    def from_row(cls, row: Any) -> "Vehicle":
        return cls(
            id=int(_value(row, "id")),
            player_id=int(_value(row, "player_id")),
            template_key=str(_value(row, "template_key", "")),
            callsign=str(_value(row, "callsign", "")),
            condition_score=int(_value(row, "condition_score", 100)),
            reliability_score=int(_value(row, "reliability_score", 100)),
            fuel=int(_value(row, "fuel", 100)),
            damage=int(_value(row, "damage", 0)),
            mileage=int(_value(row, "mileage", 0)),
            status=str(_value(row, "status", "available")),
        )


@dataclass(slots=True)
class Personnel:
    id: int
    player_id: int
    name: str
    rank: str
    contract_type: str
    salary: int
    xp: int
    condition_score: int
    stress_score: int
    morale_score: int
    leadership: int
    technical: int
    medical: int
    discipline: int
    available: bool

    @classmethod
    def from_row(cls, row: Any) -> "Personnel":
        return cls(
            id=int(_value(row, "id")),
            player_id=int(_value(row, "player_id")),
            name=str(_value(row, "name", "")),
            rank=str(_value(row, "rank", "")),
            contract_type=str(_value(row, "contract_type", "")),
            salary=int(_value(row, "salary", 0)),
            xp=int(_value(row, "xp", 0)),
            condition_score=int(_value(row, "condition_score", 100)),
            stress_score=int(_value(row, "stress_score", 0)),
            morale_score=int(_value(row, "morale_score", 75)),
            leadership=int(_value(row, "leadership", 0)),
            technical=int(_value(row, "technical", 0)),
            medical=int(_value(row, "medical", 0)),
            discipline=int(_value(row, "discipline", 0)),
            available=bool(_value(row, "available", 1)),
        )


@dataclass(slots=True)
class Incident:
    id: int
    guild_id: int
    player_id: int
    template_key: str
    title: str
    risk_level: int
    status: str
    required_tags_json: str
    required_vehicle_types_json: str
    required_trainings_json: str
    base_reward: int
    base_xp: int

    @classmethod
    def from_row(cls, row: Any) -> "Incident":
        return cls(
            id=int(_value(row, "id")),
            guild_id=int(_value(row, "guild_id")),
            player_id=int(_value(row, "player_id")),
            template_key=str(_value(row, "template_key", "")),
            title=str(_value(row, "title", "")),
            risk_level=int(_value(row, "risk_level", 1)),
            status=str(_value(row, "status", "")),
            required_tags_json=str(_value(row, "required_tags_json", "[]")),
            required_vehicle_types_json=str(_value(row, "required_vehicle_types_json", "[]")),
            required_trainings_json=str(_value(row, "required_trainings_json", "[]")),
            base_reward=int(_value(row, "base_reward", 0)),
            base_xp=int(_value(row, "base_xp", 0)),
        )


@dataclass(slots=True)
class IncidentReport:
    id: int
    incident_id: int
    player_id: int
    score: int
    cash_reward: int
    xp_reward: int
    reputation_delta: int
    safety_delta: int
    summary: str

    @classmethod
    def from_row(cls, row: Any) -> "IncidentReport":
        return cls(
            id=int(_value(row, "id")),
            incident_id=int(_value(row, "incident_id")),
            player_id=int(_value(row, "player_id")),
            score=int(_value(row, "score", 0)),
            cash_reward=int(_value(row, "cash_reward", 0)),
            xp_reward=int(_value(row, "xp_reward", 0)),
            reputation_delta=int(_value(row, "reputation_delta", 0)),
            safety_delta=int(_value(row, "safety_delta", 0)),
            summary=str(_value(row, "summary", "")),
        )
