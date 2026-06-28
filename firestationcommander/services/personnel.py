"""Personnel generation and readiness helpers for FireStationCommander."""

from __future__ import annotations

from ..models import Personnel


STARTER_CREW = (
    ("Alex Morgan", "Bevelvoerder", ("basis_brandbestrijding", "bevelvoerder")),
    ("Jamie Carter", "Brandweervrijwilliger", ("basis_brandbestrijding", "chauffeur_ts")),
    ("Robin Hayes", "Brandweervrijwilliger", ("basis_brandbestrijding", "pompbediener")),
    ("Casey Brooks", "Brandweervrijwilliger", ("basis_brandbestrijding", "ademlucht")),
    ("Taylor Reed", "Brandweervrijwilliger", ("basis_brandbestrijding",)),
    ("Jordan Ellis", "Brandweervrijwilliger", ("basis_brandbestrijding",)),
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
                    "leadership": 3 if "bevelvoerder" in trainings else 1,
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
