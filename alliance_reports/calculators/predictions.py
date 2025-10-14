"""
Predictions Calculator
Generates forecasts based on historical trends
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from zoneinfo import ZoneInfo

log = logging.getLogger("red.FARA.AllianceReports.Predictions")


class PredictionsCalculator:
    """Calculate predictions and forecasts for next period."""
    
    def __init__(self):
        """Initialize predictions calculator."""
        pass
    
    async def generate_predictions(
        self,
        monthly_data: Dict,
        trends: Dict,
        confidence: str = "medium"
    ) -> Dict[str, any]:
        """
        Generate predictions for next month.
        
        Args:
            monthly_data: Current month's data
            trends: Trend analysis data
            confidence: Prediction confidence level (low/medium/high)
        
        Returns:
            Dictionary with predictions for various metrics
        """
        predictions = {}
        
        try:
            # Member growth prediction
            predictions["members"] = await self._predict_members(monthly_data, trends, confidence)
            
            # Training volume prediction
            predictions["trainings"] = await self._predict_trainings(monthly_data, trends, confidence)
            
            # Building activity prediction
            predictions["buildings"] = await self._predict_buildings(monthly_data, trends, confidence)
            
            # Treasury prediction
            predictions["treasury"] = await self._predict_treasury(monthly_data, trends, confidence)
            
            # Overall activity prediction
            predictions["activity_score"] = await self._predict_activity(monthly_data, trends, confidence)
            
            log.info(f"Predictions generated with {confidence} confidence")
            return predictions
        
        except Exception as e:
            log.exception(f"Error generating predictions: {e}")
            return predictions
    
    async def _predict_members(
        self,
        data: Dict,
        trends: Dict,
        confidence: str
    ) -> Dict:
        """Predict member count for next month."""
        try:
            membership = data.get("membership", {})
            current = membership.get("ending_members", 0)
            
            # Get growth trends
            mom_trend = trends.get("mom", {}).get("members", {})
            mom_change = mom_trend.get("absolute", 0)
            
            # Apply confidence modifier
            modifier = self._get_confidence_modifier(confidence)
            
            # Simple linear projection
            predicted_change = mom_change * modifier
            predicted = int(current + predicted_change)
            
            # Ensure reasonable bounds
            predicted = max(current - 20, min(current + 30, predicted))
            
            return {
                "current": current,
                "predicted": predicted,
                "change": predicted - current,
                "confidence": confidence,
            }
        
        except Exception as e:
            log.exception(f"Error predicting members: {e}")
            return {"current": 0, "predicted": 0, "change": 0, "confidence": confidence}
    
    async def _predict_trainings(
        self,
        data: Dict,
        trends: Dict,
        confidence: str
    ) -> Dict:
        """Predict training volume for next month."""
        try:
            training = data.get("training", {})
            current = training.get("started_period", 0)
            
            # Get trends
            mom_trend = trends.get("mom", {}).get("trainings", {})
            mom_pct = mom_trend.get("percentage", 0)
            
            # Apply trend with confidence modifier
            modifier = self._get_confidence_modifier(confidence)
            growth_rate = (mom_pct / 100) * modifier
            
            predicted = int(current * (1 + growth_rate))
            
            # Ensure reasonable bounds (±40% of current)
            predicted = max(int(current * 0.6), min(int(current * 1.4), predicted))
            
            return {
                "current": current,
                "predicted": predicted,
                "change": predicted - current,
                "confidence": confidence,
            }
        
        except Exception as e:
            log.exception(f"Error predicting trainings: {e}")
            return {"current": 0, "predicted": 0, "change": 0, "confidence": confidence}
    
    async def _predict_buildings(
        self,
        data: Dict,
        trends: Dict,
        confidence: str
    ) -> Dict:
        """Predict building approvals for next month."""
        try:
            buildings = data.get("buildings", {})
            current = buildings.get("approved_period", 0)
            
            # Get trends
            mom_trend = trends.get("mom", {}).get("buildings", {})
            mom_pct = mom_trend.get("percentage", 0)
            
            # Apply trend with confidence modifier
            modifier = self._get_confidence_modifier(confidence)
            growth_rate = (mom_pct / 100) * modifier
            
            predicted = int(current * (1 + growth_rate))
            
            # Ensure reasonable bounds (±50% of current)
            predicted = max(int(current * 0.5), min(int(current * 1.5), predicted))
            
            return {
                "current": current,
                "predicted": predicted,
                "change": predicted - current,
                "confidence": confidence,
            }
        
        except Exception as e:
            log.exception(f"Error predicting buildings: {e}")
            return {"current": 0, "predicted": 0, "change": 0, "confidence": confidence}
    
    async def _predict_treasury(
        self,
        data: Dict,
        trends: Dict,
        confidence: str
    ) -> Dict:
        """Predict treasury balance for next month."""
        try:
            treasury = data.get("treasury", {})
            current = treasury.get("closing_balance", 0)
            
            # Get growth trend
            mom_trend = trends.get("mom", {}).get("treasury", {})
            mom_pct = mom_trend.get("percentage", 0)
            
            # Apply trend with confidence modifier
            modifier = self._get_confidence_modifier(confidence)
            growth_rate = (mom_pct / 100) * modifier
            
            predicted = int(current * (1 + growth_rate))
            
            # Ensure reasonable bounds (±20% of current)
            predicted = max(int(current * 0.8), min(int(current * 1.2), predicted))
            
            return {
                "current": current,
                "predicted": predicted,
                "change": predicted - current,
                "confidence": confidence,
            }
        
        except Exception as e:
            log.exception(f"Error predicting treasury: {e}")
            return {"current": 0, "predicted": 0, "change": 0, "confidence": confidence}
    
    async def _predict_activity(
        self,
        data: Dict,
        trends: Dict,
        confidence: str
    ) -> Dict:
        """Predict overall activity score for next month."""
        try:
            current = data.get("activity_score", 0)
            
            # Average trend across all metrics
            mom = trends.get("mom", {})
            trend_pcts = []
            
            for metric in ["members", "trainings", "buildings", "treasury"]:
                pct = mom.get(metric, {}).get("percentage", 0)
                trend_pcts.append(pct)
            
            avg_trend = sum(trend_pcts) / len(trend_pcts) if trend_pcts else 0
            
            # Apply to current score
            modifier = self._get_confidence_modifier(confidence)
            growth_rate = (avg_trend / 100) * modifier * 0.5  # More conservative for score
            
            predicted = int(current * (1 + growth_rate))
            
            # Keep within 0-100 bounds
            predicted = max(0, min(100, predicted))
            
            return {
                "current": current,
                "predicted": predicted,
                "change": predicted - current,
                "confidence": confidence,
            }
        
        except Exception as e:
            log.exception(f"Error predicting activity: {e}")
            return {"current": 0, "predicted": 0, "change": 0, "confidence": confidence}
    
    def _get_confidence_modifier(self, confidence: str) -> float:
        """Get confidence modifier for predictions."""
        modifiers = {
            "low": 0.5,
            "medium": 0.75,
            "high": 1.0,
        }
        return modifiers.get(confidence.lower(), 0.75)
    
    def format_prediction(self, prediction: Dict, metric_name: str = "value") -> str:
        """Format prediction into readable string."""
        if not prediction:
            return "N/A"
        
        current = prediction.get("current", 0)
        predicted = prediction.get("predicted", 0)
        change = prediction.get("change", 0)
        
        sign = "+" if change >= 0 else ""
        
        if metric_name == "treasury":
            return f"{predicted:,} credits ({sign}{change:,})"
        else:
            return f"{predicted} ({sign}{change})"
    
    def generate_challenge(self, predictions: Dict) -> str:
        """Generate a challenge message based on predictions."""
        try:
            # Find the metric with highest predicted growth
            highest_growth = {
                "members": predictions.get("members", {}).get("change", 0),
                "trainings": predictions.get("trainings", {}).get("change", 0),
                "buildings": predictions.get("buildings", {}).get("change", 0),
            }
            
            top_metric = max(highest_growth.items(), key=lambda x: x[1])
            metric_name, growth = top_metric
            
            if growth > 0:
                messages = {
                    "members": f"Can we beat the prediction and gain even more members? Let's aim for {predictions['members']['predicted'] + 5}! 🎯",
                    "trainings": f"Training is trending up! Can we exceed {predictions['trainings']['predicted']} trainings next month? 🚀",
                    "buildings": f"Building boom continues! Let's push past {predictions['buildings']['predicted']} approvals! 🏗️",
                }
                return messages.get(metric_name, "Let's keep this momentum going! 💪")
            else:
                return "Let's work together to turn these predictions around! 💪"
        
        except Exception as e:
            log.exception(f"Error generating challenge: {e}")
            return "Can we beat these predictions? Let's find out! 🚀"
