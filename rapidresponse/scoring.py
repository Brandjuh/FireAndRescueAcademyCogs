"""
Scoring system for RapidResponse
Author: BrandjuhNL
"""

from typing import Dict


def calculate_score(requirements: Dict[str, int], player_answer: Dict[str, int]) -> tuple[float, bool]:
    """
    Calculate player score based on requirements and their answer.
    
    Scoring rules:
    - Vehicle type used at least once: +2 points
    - Correct count per vehicle: +1 point per matched vehicle
    - Over-deploy penalty: -0.5 points per extra vehicle
    - Extra types not in requirements: -1 point per type
    - Perfect match bonus: +4 points if all correct and no extras
    
    Args:
        requirements: Dict of required vehicles {vehicle_key: count}
        player_answer: Dict of player's vehicles {vehicle_key: count}
        
    Returns:
        Tuple of (score, is_perfect_match)
    """
    score = 0.0
    is_perfect = True
    
    # Track which types we've seen
    required_types = set(requirements.keys())
    answered_types = set(player_answer.keys())
    
    # Calculate score for required types
    for vehicle_type in required_types:
        required_count = requirements[vehicle_type]
        answered_count = player_answer.get(vehicle_type, 0)
        
        if answered_count > 0:
            # Player used this type: +2 points
            score += 2
            
            # Correct count bonus: +1 per matched vehicle
            matched = min(required_count, answered_count)
            score += matched
            
            # Over-deploy penalty
            if answered_count > required_count:
                over = answered_count - required_count
                score -= (0.5 * over)
                is_perfect = False
        else:
            # Player didn't use this required type
            is_perfect = False
    
    # Penalty for extra types not in requirements
    extra_types = answered_types - required_types
    if extra_types:
        score -= len(extra_types)
        is_perfect = False
    
    # Perfect match bonus
    if is_perfect:
        score += 4
    
    # Never go below 0
    score = max(0.0, score)
    
    return score, is_perfect


def format_score_breakdown(requirements: Dict[str, int], player_answer: Dict[str, int]) -> str:
    """
    Generate a detailed score breakdown for display.
    
    Args:
        requirements: Dict of required vehicles
        player_answer: Dict of player's vehicles
        
    Returns:
        Formatted string showing score breakdown
    """
    from .parsing import get_vehicle_display_name
    
    breakdown = []
    total_score = 0.0
    
    required_types = set(requirements.keys())
    answered_types = set(player_answer.keys())
    
    # Score required types
    for vehicle_type in sorted(required_types):
        required_count = requirements[vehicle_type]
        answered_count = player_answer.get(vehicle_type, 0)
        vehicle_name = get_vehicle_display_name(vehicle_type)
        
        if answered_count > 0:
            # Type used bonus
            breakdown.append(f"✅ {vehicle_name} used: +2")
            total_score += 2
            
            # Correct count
            matched = min(required_count, answered_count)
            breakdown.append(f"   Correct count: +{matched} ({matched}/{required_count})")
            total_score += matched
            
            # Over-deploy
            if answered_count > required_count:
                over = answered_count - required_count
                penalty = 0.5 * over
                breakdown.append(f"   Over-deployed: -{penalty:.1f} ({over} extra)")
                total_score -= penalty
        else:
            breakdown.append(f"❌ {vehicle_name} missing (needed {required_count})")
    
    # Extra types
    extra_types = answered_types - required_types
    if extra_types:
        for vehicle_type in sorted(extra_types):
            vehicle_name = get_vehicle_display_name(vehicle_type)
            count = player_answer[vehicle_type]
            breakdown.append(f"⚠️ {vehicle_name}: -1 (not required, sent {count})")
            total_score -= 1
    
    # Perfect bonus
    score, is_perfect = calculate_score(requirements, player_answer)
    if is_perfect:
        breakdown.append(f"🎯 Perfect match bonus: +4")
    
    breakdown.append(f"\n**Total Score: {score:.1f}**")
    
    return "\n".join(breakdown)
