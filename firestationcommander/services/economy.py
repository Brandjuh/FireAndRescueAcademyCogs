"""Economy and progression helpers for FireStationCommander."""

from __future__ import annotations

from dataclasses import dataclass

from ..constants import XP_PER_LEVEL


@dataclass(slots=True)
class RewardResult:
    cash_reward: int
    xp_reward: int
    reputation_delta: int
    safety_delta: int
    new_level: int
    leveled_up: bool


class EconomyService:
    """Calculate rewards, reputation, safety changes, and command levels."""

    @staticmethod
    def command_level_for_xp(xp: int) -> int:
        """Return a command level from total XP."""
        return max(1, min(50, 1 + max(0, xp) // XP_PER_LEVEL))

    def rewards(
        self,
        *,
        base_reward: int,
        base_xp: int,
        score: int,
        current_xp: int,
    ) -> RewardResult:
        """Calculate incident rewards from a 0-100 score."""
        score_percentage = max(0, min(100, score)) / 100
        cash_reward = int(round(base_reward * score_percentage))
        xp_reward = int(round(base_xp * score_percentage))

        if score >= 90:
            reputation_delta = 3
        elif score >= 70:
            reputation_delta = 1
        elif score >= 50:
            reputation_delta = 0
        else:
            reputation_delta = -2

        if score < 40:
            safety_delta = -3
        elif score < 70:
            safety_delta = -1
        else:
            safety_delta = 0

        old_level = self.command_level_for_xp(current_xp)
        new_level = self.command_level_for_xp(current_xp + xp_reward)
        return RewardResult(
            cash_reward=cash_reward,
            xp_reward=xp_reward,
            reputation_delta=reputation_delta,
            safety_delta=safety_delta,
            new_level=new_level,
            leveled_up=new_level > old_level,
        )
