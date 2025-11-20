"""
Game logic for mission resolution and outcome calculation
"""
import random
import logging
from typing import Dict, Any, Tuple, Optional
from datetime import datetime
from . import config

log = logging.getLogger("red.rapidresponse.game_logic")


class GameLogic:
    """Handles game mechanics and mission resolution"""
    
    def __init__(self, database, mission_manager):
        self.db = database
        self.mission_manager = mission_manager
    
    def calculate_mission_timeout(self, station_level: int, tier: int) -> int:
        """Calculate mission timeout based on player level and tier"""
        if station_level <= 5:
            base_timeout = config.MISSION_TIMEOUT_BASE
        elif station_level <= 20:
            base_timeout = config.MISSION_TIMEOUT_ADVANCED
        else:
            base_timeout = config.MISSION_TIMEOUT_EXPERT
        
        # Harder missions get slightly more time
        tier_bonus = (tier - 1) * 10
        
        return base_timeout + tier_bonus
    
    def calculate_success_chance(
        self,
        player: Dict[str, Any],
        mission_data: Dict[str, Any],
        tier: int,
        difficulty: int,
        response_type: str
    ) -> float:
        """Calculate success chance based on player stats and mission difficulty"""
        
        # Base success chance
        chance = config.BASE_SUCCESS_CHANCE
        
        # Category-specific stat bonus
        category = self.mission_manager.get_mission_category(mission_data)
        
        stat_map = {
            "fire": "stat_tactics",
            "medical": "stat_medical",
            "police": "stat_command",
            "rescue": "stat_response",
            "general": "stat_logistics"
        }
        
        primary_stat = stat_map.get(category, "stat_tactics")
        stat_value = player.get(primary_stat, 10)
        
        # Add stat bonus
        chance += stat_value * config.STAT_IMPACT_PER_POINT
        
        # Add secondary stat bonuses (smaller impact)
        chance += player.get('stat_response', 10) * 0.2
        chance += player.get('stat_logistics', 10) * 0.2
        
        # Difficulty penalty
        chance -= (tier - 1) * config.DIFFICULTY_PENALTY_PER_TIER
        chance -= (difficulty - 50) * 0.3  # Normalize around 50 difficulty
        
        # Response type modifier
        response_mod = config.RESPONSE_TYPES[response_type]['success_mod']
        chance += response_mod
        
        # Morale impact
        morale = player.get('morale', 100)
        if morale < config.LOW_MORALE_THRESHOLD:
            chance -= config.LOW_MORALE_PENALTY
        elif morale >= 80:
            chance += 5  # High morale bonus
        
        # Streak bonus
        streak = player.get('mission_streak', 0)
        if streak > 0:
            streak_bonus = min(streak * config.STREAK_BONUS_PER_MISSION * 100, 
                              config.MAX_STREAK_BONUS * 100)
            chance += streak_bonus
        
        # Clamp between 5% and 95%
        return max(5.0, min(95.0, chance))
    
    def roll_outcome(
        self,
        success_chance: float,
        tier: int,
        stage: int
    ) -> str:
        """Roll for mission outcome"""
        roll = random.uniform(0, 100)
        
        if roll <= success_chance:
            # Success - but check for escalation on multi-stage missions
            if stage == 1:
                escalation_chance = config.ESCALATION_CHANCE_BASE + (tier - 1) * config.ESCALATION_CHANCE_PER_TIER
                if random.random() < escalation_chance:
                    return config.OUTCOME_ESCALATION
            return config.OUTCOME_FULL_SUCCESS
        elif roll <= success_chance + 20:
            # Partial success
            return config.OUTCOME_PARTIAL_SUCCESS
        else:
            # Failure
            return config.OUTCOME_FAILURE
    
    async def resolve_mission(
        self,
        mission_instance_id: int,
        response_type: str,
        player: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Resolve a mission and calculate rewards/penalties"""
        
        mission = await self.db.get_mission_by_id(mission_instance_id)
        if not mission:
            return {"error": "Mission not found"}
        
        # Check if expired
        expires_at = datetime.fromisoformat(mission['expires_at'])
        if datetime.utcnow() > expires_at:
            return {"error": "Mission expired"}
        
        # Parse mission data
        import json
        mission_data = json.loads(mission['mission_data'])
        
        # Calculate success chance
        success_chance = self.calculate_success_chance(
            player,
            mission_data,
            mission['tier'],
            mission['difficulty'],
            response_type
        )
        
        # Roll outcome
        outcome = self.roll_outcome(
            success_chance,
            mission['tier'],
            mission['stage']
        )
        
        # Calculate rewards based on outcome
        base_credits = mission_data.get('average_credits')
        if base_credits is None:
            base_credits = 500  # Default credits if not specified
        
        tier_info = config.MISSION_TIERS[mission['tier']]
        base_xp = int(base_credits * tier_info['xp_mult'])
        
        # Apply response type cost multiplier
        cost_mult = config.RESPONSE_TYPES[response_type]['cost_mult']
        
        if outcome == config.OUTCOME_FULL_SUCCESS:
            credits = int(base_credits * config.FULL_SUCCESS_CREDIT_MULT * cost_mult)
            xp = int(base_xp * config.FULL_SUCCESS_XP_MULT)
            morale_change = config.MORALE_SUCCESS_GAIN
            description = self._generate_success_description(mission_data, response_type)
            
        elif outcome == config.OUTCOME_PARTIAL_SUCCESS:
            credits = int(base_credits * config.PARTIAL_SUCCESS_CREDIT_MULT * cost_mult)
            xp = int(base_xp * config.PARTIAL_SUCCESS_XP_MULT)
            morale_change = -config.MORALE_PARTIAL_LOSS
            description = self._generate_partial_description(mission_data, response_type)
            
        elif outcome == config.OUTCOME_FAILURE:
            credits = int(base_credits * config.FAILURE_CREDIT_MULT * cost_mult)
            xp = int(base_xp * config.FAILURE_XP_MULT)
            morale_change = -config.MORALE_FAILURE_LOSS
            description = self._generate_failure_description(mission_data, response_type)
            
        elif outcome == config.OUTCOME_ESCALATION:
            # Mission escalates to next stage
            next_stage = mission['stage'] + 1
            
            # Partial rewards for current stage
            credits = int(base_credits * 0.4 * cost_mult)
            xp = int(base_xp * 0.4)
            morale_change = 0
            description = self._generate_escalation_description(mission_data, next_stage)
            
            # Update mission to next stage
            await self.db.update_mission(
                mission_instance_id,
                stage=next_stage,
                status='pending',  # Keep pending for next stage
                stage_data=json.dumps({
                    'previous_stage': mission['stage'],
                    'previous_response': response_type
                })
            )
            
            return {
                "outcome": outcome,
                "description": description,
                "credits": credits,
                "xp": xp,
                "morale_change": morale_change,
                "success_chance": success_chance,
                "escalated": True,
                "next_stage": next_stage,
                "max_stage": mission['max_stage']
            }
        else:
            credits = 0
            xp = 0
            morale_change = 0
            description = "Unknown outcome"
        
        # Complete mission
        await self.db.complete_mission(
            mission_instance_id,
            outcome,
            credits,
            xp,
            morale_change
        )
        
        # Update player stats
        new_morale = max(config.MORALE_MIN, 
                        min(config.MORALE_MAX, player['morale'] + morale_change))
        
        updates = {
            'morale': new_morale,
            'last_mission_time': datetime.utcnow().isoformat(),
            'total_missions': player['total_missions'] + 1,
        }
        
        # Update streak
        if outcome == config.OUTCOME_FULL_SUCCESS:
            updates['mission_streak'] = player['mission_streak'] + 1
            updates['successful_missions'] = player['successful_missions'] + 1
        elif outcome in [config.OUTCOME_PARTIAL_SUCCESS, config.OUTCOME_FAILURE]:
            updates['mission_streak'] = 0
            if outcome == config.OUTCOME_FAILURE:
                updates['failed_missions'] = player['failed_missions'] + 1
        
        await self.db.update_player(player['user_id'], **updates)
        
        # Add XP (handles level ups)
        level_info = await self.db.add_xp(player['user_id'], xp)
        
        return {
            "outcome": outcome,
            "description": description,
            "credits": credits,
            "xp": xp,
            "morale_change": morale_change,
            "success_chance": success_chance,
            "level_info": level_info,
            "new_morale": new_morale,
            "escalated": False
        }
    
    def _generate_success_description(self, mission_data: Dict, response_type: str) -> str:
        """Generate description for successful mission"""
        descriptions = [
            "Your units arrived promptly and handled the situation professionally. All objectives completed.",
            "Excellent response! The incident was resolved with minimal complications.",
            "Mission accomplished! Your team's quick action prevented further escalation.",
            "Outstanding work! All units performed admirably and the situation is under control.",
            "Perfect execution! Your strategic response made all the difference.",
        ]
        return random.choice(descriptions)
    
    def _generate_partial_description(self, mission_data: Dict, response_type: str) -> str:
        """Generate description for partial success"""
        descriptions = [
            "The incident was resolved, but not without complications. Some minor injuries reported.",
            "Mission completed, though the response could have been more efficient. Lessons learned.",
            "The situation is under control, but there were some setbacks along the way.",
            "Units managed to contain the incident, though not as smoothly as hoped.",
            "The mission was completed, but with some damage and delays.",
        ]
        return random.choice(descriptions)
    
    def _generate_failure_description(self, mission_data: Dict, response_type: str) -> str:
        """Generate description for failed mission"""
        descriptions = [
            "The response was insufficient. The situation escalated beyond control. Major complications.",
            "Mission failed. Your units were overwhelmed and unable to contain the incident.",
            "Critical failure! The incident spiraled out of control. Significant damage reported.",
            "The response strategy was ineffective. The situation deteriorated rapidly.",
            "Your units were unable to handle the severity of the incident. Major losses incurred.",
        ]
        return random.choice(descriptions)
    
    def _generate_escalation_description(self, mission_data: Dict, next_stage: int) -> str:
        """Generate description for escalation"""
        descriptions = [
            f"🚨 **ESCALATION!** The situation has worsened! Stage {next_stage} initiated.",
            f"⚠️ **COMPLICATIONS!** Additional units needed! Escalating to stage {next_stage}.",
            f"🔥 **SITUATION DEVELOPING!** New challenges emerging. Stage {next_stage} activated.",
            f"📢 **BACKUP REQUIRED!** The incident is expanding. Moving to stage {next_stage}.",
            f"🆘 **CRITICAL UPDATE!** The situation requires additional response. Stage {next_stage}.",
        ]
        return random.choice(descriptions)
    
    async def handle_timeout(self, mission_instance_id: int, player: Dict[str, Any]) -> Dict[str, Any]:
        """Handle mission timeout"""
        mission = await self.db.get_mission_by_id(mission_instance_id)
        if not mission:
            return {"error": "Mission not found"}
        
        # Apply penalties
        morale_change = -config.TIMEOUT_PENALTY_MORALE
        new_morale = max(config.MORALE_MIN, player['morale'] + morale_change)
        
        # Update player
        updates = {
            'morale': new_morale,
            'ignored_missions': player['ignored_missions'] + 1,
            'mission_streak': 0,
        }
        
        # Auto-inactive if too many ignored
        if player['ignored_missions'] + 1 >= config.MAX_IGNORED_MISSIONS:
            updates['is_active'] = 0
        
        await self.db.update_player(player['user_id'], **updates)
        
        # Complete mission as timeout
        await self.db.complete_mission(
            mission_instance_id,
            config.OUTCOME_TIMEOUT,
            0,
            0,
            morale_change
        )
        
        return {
            "outcome": config.OUTCOME_TIMEOUT,
            "morale_change": morale_change,
            "new_morale": new_morale,
            "auto_inactive": updates.get('is_active', 1) == 0
        }
    
    async def calculate_training_cost(self, player: Dict[str, Any], stat_type: str) -> int:
        """Calculate cost of training based on current stat level"""
        current_stat = player.get(f'stat_{stat_type}', 10)
        cost = int(config.TRAINING_COST_BASE * (config.TRAINING_COST_MULTIPLIER ** (current_stat // 10)))
        return cost
    
    async def complete_training_for_player(self, player: Dict[str, Any], training: Dict[str, Any]) -> Dict[str, Any]:
        """Complete a training session and apply stat increase"""
        stat_type = training['stat_type']
        stat_key = f'stat_{stat_type}'
        
        current_stat = player.get(stat_key, 10)
        new_stat = current_stat + config.TRAINING_STAT_GAIN
        
        await self.db.update_player(player['user_id'], **{stat_key: new_stat})
        await self.db.complete_training(training['id'])
        
        return {
            "stat_type": stat_type,
            "old_value": current_stat,
            "new_value": new_stat,
            "gain": config.TRAINING_STAT_GAIN
        }
