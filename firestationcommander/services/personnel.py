"""Personnel generation and readiness helpers for FireStationCommander."""

from __future__ import annotations

from ..models import Personnel


STARTER_CREW = (
    ("Alex Morgan", "Crew Commander", ("basic_firefighting", "crew_command")),
    ("Jamie Carter", "Driver/Operator", ("basic_firefighting", "driver_operator")),
    ("Robin Hayes", "Pump Operator", ("basic_firefighting", "pump_operator")),
    ("Casey Brooks", "Firefighter", ("basic_firefighting", "breathing_apparatus")),
    ("Taylor Reed", "Firefighter", ("basic_firefighting",)),
    ("Jordan Ellis", "Firefighter", ("basic_firefighting",)),
)


class PersonnelService:
    """Create starter crews and calculate personnel readiness."""

    @staticmethod
    def starter_personnel() -> list[dict[str, object]]:
        """Return starter personnel payloads including their initial trainings."""
        payloads: list[dict[str, object]] = []
        for index, (name, rank, trainings) in enumerate(STARTER_CREW):
            payloads.append(
                {
                    "name": name,
                    "rank": rank,
                    "contract_type": "volunteer",
                    "salary": 0,
                    "condition_score": 96 - index,
                    "stress_score": 5,
                    "morale_score": 78,
                    "leadership": 3 if "crew_command" in trainings else 1,
                    "technical": 3 if index in {1, 2, 3} else 2,
                    "medical": 1,
                    "discipline": 3 if index in {0, 1, 2, 3} else 2,
                    "available": True,
                    "trainings": trainings,
                }
            )
        return payloads

    @staticmethod
    def available(personnel: list[Personnel]) -> list[Personnel]:
        """Return personnel currently available for response."""
        return [member for member in personnel if member.available]

    @staticmethod
    def wellness_score(personnel: list[Personnel]) -> float:
        """Calculate a 0-100 wellness score from condition, stress, and morale."""
        if not personnel:
            return 0.0
        total = 0.0
        for member in personnel:
            total += (member.condition_score * 0.45) + ((100 - member.stress_score) * 0.35)
            total += member.morale_score * 0.20
        return max(0.0, min(100.0, total / len(personnel)))
