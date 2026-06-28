"""Incident generation and scoring for FireStationCommander."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from ..models import Personnel, Player, Vehicle
from .personnel import PersonnelService
from .training import TrainingService
from .vehicles import VehicleService


@dataclass(slots=True)
class IncidentScore:
    score: int
    summary: str
    breakdown: dict[str, int]


class IncidentService:
    """Generate incident templates and calculate response scores."""

    def __init__(
        self,
        incident_templates: list[dict[str, Any]],
        vehicle_service: VehicleService,
        training_service: TrainingService,
        personnel_service: PersonnelService,
    ):
        self.templates = incident_templates
        self.vehicle_service = vehicle_service
        self.training_service = training_service
        self.personnel_service = personnel_service

    def choose_template(self, player: Player) -> dict[str, Any]:
        """Choose an incident template for the player's command level."""
        eligible = [
            template
            for template in self.templates
            if int(template.get("min_level", 1)) <= player.command_level
        ]
        if not eligible:
            eligible = list(self.templates)
        return random.choice(eligible)

    @staticmethod
    def expires_at(template: dict[str, Any]) -> str:
        """Return an ISO expiry timestamp for a new incident."""
        minutes = int(template.get("time_limit_minutes", 30))
        return (timedelta(minutes=minutes) + _now_datetime()).isoformat().replace("+00:00", "Z")

    def calculate_score(
        self,
        *,
        template: dict[str, Any],
        selected_vehicles: list[Vehicle],
        available_personnel: list[Personnel],
        equipment_rows: list[Any],
        equipment_templates: dict[str, dict[str, Any]],
        training_map: dict[int, list[str]],
        random_modifier: int | None = None,
    ) -> IncidentScore:
        """Calculate a 0-100 incident result score."""
        required_types = [str(value).upper() for value in template.get("required_vehicle_types", [])]
        selected_types = [self.vehicle_service.vehicle_type(vehicle) for vehicle in selected_vehicles]
        vehicle_score = _coverage_score(required_types, selected_types)

        required_staff = int(template.get("required_staff", max(2, len(required_types) * 2)))
        total_vehicle_capacity = sum(
            self.vehicle_service.crew_capacity(vehicle) for vehicle in selected_vehicles
        )
        available_count = len(available_personnel)
        staffing_score = min(100.0, (min(total_vehicle_capacity, available_count) / required_staff) * 100)

        training_score = self.training_service.coverage_score(
            [str(value) for value in template.get("required_trainings", [])],
            training_map,
        )

        required_tags = {str(tag) for tag in template.get("required_tags", [])}
        covered_tags = set()
        for vehicle in selected_vehicles:
            covered_tags.update(self.vehicle_service.vehicle_tags(vehicle))
        for row in equipment_rows:
            template_key = str(row["template_key"])
            equipment = equipment_templates.get(template_key, {})
            covered_tags.update(str(tag) for tag in equipment.get("tags", []))
        tag_score = _set_coverage_score(required_tags, covered_tags)

        vehicle_health_score = self.vehicle_service.health_score(selected_vehicles)
        fuel_score = self.vehicle_service.fuel_score(selected_vehicles)
        personnel_score = self.personnel_service.wellness_score(available_personnel)
        modifier = random.randint(-5, 5) if random_modifier is None else random_modifier

        raw = (
            vehicle_score * 0.22
            + staffing_score * 0.18
            + training_score * 0.18
            + tag_score * 0.14
            + vehicle_health_score * 0.12
            + fuel_score * 0.08
            + personnel_score * 0.08
            + modifier
        )
        score = int(round(max(0.0, min(100.0, raw))))
        breakdown = {
            "vehicles": int(round(vehicle_score)),
            "staffing": int(round(staffing_score)),
            "training": int(round(training_score)),
            "equipment": int(round(tag_score)),
            "vehicle_condition": int(round(vehicle_health_score)),
            "fuel": int(round(fuel_score)),
            "personnel": int(round(personnel_score)),
            "random": modifier,
        }
        return IncidentScore(score=score, summary=_score_summary(score), breakdown=breakdown)


def _now_datetime():
    return datetime.now(timezone.utc)


def _coverage_score(required: list[str], actual: list[str]) -> float:
    if not required:
        return 100.0
    actual_counts: dict[str, int] = {}
    for value in actual:
        actual_counts[value] = actual_counts.get(value, 0) + 1
    matched = 0
    for value in required:
        if actual_counts.get(value, 0) > 0:
            matched += 1
            actual_counts[value] -= 1
    return (matched / len(required)) * 100


def _set_coverage_score(required: set[str], actual: set[str]) -> float:
    if not required:
        return 100.0
    return (len(required & actual) / len(required)) * 100


def _score_summary(score: int) -> str:
    if score >= 90:
        return "The incident was handled cleanly with strong command decisions."
    if score >= 70:
        return "The incident was controlled with some operational pressure."
    if score >= 50:
        return "The incident was stabilized, but several gaps slowed the response."
    return "The incident outcome was poor and requires review before the next alarm."
