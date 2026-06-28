"""Training coverage helpers for FireStationCommander."""

from __future__ import annotations


class TrainingService:
    """Calculate whether personnel cover required incident trainings."""

    @staticmethod
    def covered_trainings(training_map: dict[int, list[str]]) -> set[str]:
        """Return all training keys covered by the selected personnel."""
        covered: set[str] = set()
        for trainings in training_map.values():
            covered.update(str(training) for training in trainings)
        return covered

    def coverage_score(self, required_trainings: list[str], training_map: dict[int, list[str]]) -> float:
        """Return 0-100 coverage for required training keys."""
        if not required_trainings:
            return 100.0
        covered = self.covered_trainings(training_map)
        matched = sum(1 for training in required_trainings if training in covered)
        return (matched / len(required_trainings)) * 100
