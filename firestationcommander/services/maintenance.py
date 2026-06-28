"""Maintenance cost and repair helpers for FireStationCommander."""

from __future__ import annotations

from ..models import Vehicle


class MaintenanceService:
    """Find worn assets and calculate repair costs."""

    @staticmethod
    def vehicle_repair_cost(vehicle: Vehicle) -> int:
        """Return the repair and refuel cost for one vehicle."""
        condition_cost = max(0, 100 - vehicle.condition_score) * 25
        fuel_cost = max(0, 100 - vehicle.fuel) * 5
        damage_cost = max(0, vehicle.damage) * 10
        return condition_cost + fuel_cost + damage_cost

    @staticmethod
    def equipment_repair_cost(condition_score: int) -> int:
        """Return the repair cost for one equipment item."""
        return max(0, 100 - condition_score) * 10

    def vehicles_needing_work(self, vehicles: list[Vehicle]) -> list[tuple[Vehicle, int]]:
        """Return vehicles that need maintenance with their repair cost."""
        needed = []
        for vehicle in vehicles:
            cost = self.vehicle_repair_cost(vehicle)
            if cost > 0 or vehicle.fuel < 90:
                needed.append((vehicle, cost))
        return needed
