"""Vehicle catalog and readiness helpers for FireStationCommander."""

from __future__ import annotations

from ..constants import STARTER_VEHICLE_KEY, VEHICLE_STATUS_AVAILABLE
from ..models import Vehicle


class VehicleService:
    """Work with vehicle templates and owned vehicle readiness."""

    def __init__(self, vehicle_templates: list[dict]):
        self.templates = {template["key"]: template for template in vehicle_templates}

    def template(self, template_key: str) -> dict:
        """Return a vehicle template by key."""
        return self.templates[template_key]

    def starter_vehicle(self) -> dict[str, object]:
        """Return the starter vehicle payload."""
        template = self.template(STARTER_VEHICLE_KEY)
        return {
            "template_key": STARTER_VEHICLE_KEY,
            "callsign": "Engine 1",
            "condition_score": 100,
            "reliability_score": int(template.get("base_reliability", 90)),
            "fuel": 100,
            "damage": 0,
            "mileage": 0,
            "status": VEHICLE_STATUS_AVAILABLE,
        }

    @staticmethod
    def available(vehicles: list[Vehicle]) -> list[Vehicle]:
        """Return vehicles that can be dispatched."""
        return [vehicle for vehicle in vehicles if vehicle.status == VEHICLE_STATUS_AVAILABLE]

    def vehicle_type(self, vehicle: Vehicle) -> str:
        """Return the response type for an owned vehicle."""
        return str(self.template(vehicle.template_key).get("type", vehicle.template_key)).upper()

    def vehicle_tags(self, vehicle: Vehicle) -> set[str]:
        """Return capability tags for an owned vehicle."""
        tags = self.template(vehicle.template_key).get("tags", [])
        return {str(tag) for tag in tags if tag}

    def crew_capacity(self, vehicle: Vehicle) -> int:
        """Return the crew capacity for an owned vehicle."""
        return int(self.template(vehicle.template_key).get("crew_capacity", 0))

    def health_score(self, vehicles: list[Vehicle]) -> float:
        """Calculate average vehicle condition and reliability."""
        if not vehicles:
            return 0.0
        total = sum((vehicle.condition_score * 0.7) + (vehicle.reliability_score * 0.3) for vehicle in vehicles)
        return max(0.0, min(100.0, total / len(vehicles)))

    @staticmethod
    def fuel_score(vehicles: list[Vehicle]) -> float:
        """Calculate average vehicle fuel readiness."""
        if not vehicles:
            return 0.0
        return max(0.0, min(100.0, sum(vehicle.fuel for vehicle in vehicles) / len(vehicles)))
