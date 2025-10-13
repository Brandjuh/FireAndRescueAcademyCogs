"""
Activity Score Calculator
Calculates overall alliance activity score from daily metrics.
"""

import logging
from typing import Dict, Any

log = logging.getLogger("red.FARA.AllianceReports.ActivityScore")


class ActivityScoreCalculator:
    """Calculates alliance activity scores."""
    
    def __init__(self, weights: Dict[str, int]):
        """
        Initialize calculator with weights.
        
        Args:
            weights: Dictionary of weights (must sum to 100)
        """
        self.weights = weights
        
        # Validate weights sum to 100
        total = sum(weights.values())
        if total != 100:
            log.warning(f"Weights sum to {total}, not 100. Normalizing...")
            factor = 100 / total
            self.weights = {k: int(v * factor) for k, v in weights.items()}
    
    def calculate_daily_score(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate daily activity score.
        
        Args:
            data: Daily data from DataAggregator
        
        Returns:
            Dictionary with overall score and component scores
        """
        scores = {}
        
        # Membership score (0-100)
        membership_data = data.get("membership", {})
        scores["membership"] = self._calculate_membership_score(membership_data)
        
        # Training score (0-100)
        training_data = data.get("training", {})
        scores["training"] = self._calculate_training_score(training_data)
        
        # Buildings score (0-100)
        buildings_data = data.get("buildings", {})
        scores["buildings"] = self._calculate_buildings_score(buildings_data)
        
        # Treasury score (0-100)
        treasury_data = data.get("treasury", {})
        scores["treasury"] = self._calculate_treasury_score(treasury_data)
        
        # Operations score (0-100)
        operations_data = data.get("operations", {})
        scores["operations"] = self._calculate_operations_score(operations_data)
        
        # Calculate weighted overall score
        overall = 0
        for category, score in scores.items():
            weight = self.weights.get(category, 0)
            overall += (score * weight / 100)
        
        overall = int(min(100, max(0, overall)))
        
        # Calculate change from ideal (all 80+)
        ideal_overall = sum(self.weights.values())  # Should be 100
        change = overall - 80  # 80 is "good" baseline
        
        return {
            "overall": overall,
            "components": scores,
            "change": change,
            "trend": "up" if change > 0 else "down" if change < 0 else "stable",
        }
    
    def _calculate_membership_score(self, data: Dict[str, Any]) -> int:
        """Calculate membership activity score (0-100)."""
        if "error" in data:
            return 50  # Neutral score on error
        
        score = 50  # Base score
        
        # Positive factors
        new_joins = data.get("new_joins", 0)
        verifications = data.get("verifications_approved", 0)
        net_change = data.get("net_change", 0)
        
        # Each new join adds points
        score += min(20, new_joins * 5)
        
        # Each verification adds points
        score += min(15, verifications * 3)
        
        # Net positive growth adds points
        if net_change > 0:
            score += min(15, net_change * 5)
        elif net_change < 0:
            score -= abs(net_change) * 3  # Penalty for net loss
        
        # Kicked members penalty
        kicked = data.get("kicked", 0)
        if kicked > 0:
            score -= kicked * 5
        
        return int(min(100, max(0, score)))
    
    def _calculate_training_score(self, data: Dict[str, Any]) -> int:
        """Calculate training activity score (0-100)."""
        if "error" in data:
            return 50
        
        score = 30  # Lower base - training needs activity
        
        started = data.get("started", 0)
        completed = data.get("completed", 0)
        
        # Started trainings
        score += min(35, started * 7)
        
        # Completed trainings (more valuable)
        score += min(35, completed * 10)
        
        # Bonus for completion rate
        if started > 0:
            completion_rate = completed / started
            if completion_rate > 0.8:
                score += 10  # High completion bonus
        
        return int(min(100, max(0, score)))
    
    def _calculate_buildings_score(self, data: Dict[str, Any]) -> int:
        """Calculate buildings activity score (0-100)."""
        if "error" in data:
            return 50
        
        score = 40  # Base score
        
        approved = data.get("approved", 0)
        ext_started = data.get("extensions_started", 0)
        ext_completed = data.get("extensions_completed", 0)
        
        # New buildings
        score += min(20, approved * 5)
        
        # Extensions started
        score += min(20, ext_started * 3)
        
        # Extensions completed
        score += min(20, ext_completed * 4)
        
        return int(min(100, max(0, score)))
    
    def _calculate_treasury_score(self, data: Dict[str, Any]) -> int:
        """Calculate treasury health score (0-100)."""
        if "error" in data:
            return 50
        
        score = 50  # Base score
        
        change_24h = data.get("change_24h", 0)
        change_percent = data.get("change_percent", 0)
        contributors = data.get("contributors_24h", 0)
        
        # Positive balance change
        if change_24h > 0:
            # Scale based on percent change
            if change_percent > 3:
                score += 25  # Excellent growth
            elif change_percent > 1:
                score += 15  # Good growth
            elif change_percent > 0:
                score += 5   # Modest growth
        else:
            # Negative change penalty
            if change_percent < -2:
                score -= 20  # Concerning decline
            elif change_percent < 0:
                score -= 5   # Small decline
        
        # Contributor activity
        score += min(25, contributors * 3)
        
        return int(min(100, max(0, score)))
    
    def _calculate_operations_score(self, data: Dict[str, Any]) -> int:
        """Calculate operations activity score (0-100)."""
        if "error" in data:
            return 50
        
        score = 40  # Base score
        
        missions = data.get("large_missions_started", 0)
        events = data.get("alliance_events_started", 0)
        
        # Large missions
        score += min(30, missions * 15)
        
        # Alliance events (more rare, more valuable)
        score += min(30, events * 30)
        
        return int(min(100, max(0, score)))
