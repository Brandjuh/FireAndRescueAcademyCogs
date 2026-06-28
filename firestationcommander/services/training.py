"""Training coverage helpers for FireStationCommander."""

from __future__ import annotations


TRAINING_KEY_ALIASES = {
    "basis_brandbestrijding": "basic_firefighting",
    "ademlucht": "scba",
    "chauffeur_ts": "engine_driver",
    "pompbediener": "pump_operator",
    "technische_hulpverlening": "technical_rescue",
    "bevelvoerder": "incident_commander",
}


class TrainingService:
    """Calculate whether personnel cover required incident trainings."""

    @staticmethod
    def normalize_key(training_key: str) -> str:
        """Return the canonical English training key."""
        return TRAINING_KEY_ALIASES.get(str(training_key), str(training_key))

    @staticmethod
    def covered_trainings(training_map: dict[int, list[str]]) -> set[str]:
        """Return all training keys covered by the selected personnel."""
        covered: set[str] = set()
        for trainings in training_map.values():
            covered.update(TrainingService.normalize_key(str(training)) for training in trainings)
        return covered

    def coverage_score(self, required_trainings: list[str], training_map: dict[int, list[str]]) -> float:
        """Return 0-100 coverage for required training keys."""
        if not required_trainings:
            return 100.0
        covered = self.covered_trainings(training_map)
        required = [self.normalize_key(training) for training in required_trainings]
        matched = sum(1 for training in required if training in covered)
        return (matched / len(required_trainings)) * 100
