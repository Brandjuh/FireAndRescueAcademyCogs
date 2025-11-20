"""
Configuration constants for Rapid Response Dispatch game
"""

# Server where the game can be played
GAME_SERVER_ID = 543935264234536960

# MissionChief JSON endpoint
MISSIONCHIEF_JSON_URL = "https://www.missionchief.com/einsaetze.json"
MISSION_CACHE_REFRESH_HOURS = 6

# XP and Leveling
XP_PER_LEVEL = 1000
LEVEL_STAT_BONUS = 5  # Points to distribute per level

# Mission assignment timing (in minutes)
MISSION_CHECK_INTERVAL = 5  # How often to check for new missions
BASE_MISSION_COOLDOWN_MIN = 30  # Starting cooldown between missions
BASE_MISSION_COOLDOWN_MAX = 45
ADVANCED_MISSION_COOLDOWN_MIN = 15  # For high-level players
ADVANCED_MISSION_COOLDOWN_MAX = 25

# Mission timeout (in seconds)
MISSION_TIMEOUT_BASE = 120  # 2 minutes for beginners
MISSION_TIMEOUT_ADVANCED = 90  # 1.5 minutes for advanced
MISSION_TIMEOUT_EXPERT = 60  # 1 minute for experts

# Training
TRAINING_DURATION_HOURS = 1
TRAINING_STAT_GAIN = 10
TRAINING_COST_BASE = 500
TRAINING_COST_MULTIPLIER = 1.5  # Cost increases with stat level

# Mission difficulty tiers
MISSION_TIERS = {
    1: {"name": "Routine", "min_credits": 0, "max_credits": 1000, "xp_mult": 1.0},
    2: {"name": "Standard", "min_credits": 1000, "max_credits": 3000, "xp_mult": 1.5},
    3: {"name": "Complex", "min_credits": 3000, "max_credits": 6000, "xp_mult": 2.0},
    4: {"name": "Critical", "min_credits": 6000, "max_credits": 15000, "xp_mult": 3.0},
}

# Success calculation
BASE_SUCCESS_CHANCE = 60  # Base 60% success
STAT_IMPACT_PER_POINT = 0.5  # Each stat point adds 0.5% success
DIFFICULTY_PENALTY_PER_TIER = 10  # Each tier above 1 reduces success by 10%

# Outcomes
OUTCOME_FULL_SUCCESS = "full_success"
OUTCOME_PARTIAL_SUCCESS = "partial_success"
OUTCOME_FAILURE = "failure"
OUTCOME_ESCALATION = "escalation"
OUTCOME_TIMEOUT = "timeout"

# Rewards multipliers
FULL_SUCCESS_CREDIT_MULT = 1.0
FULL_SUCCESS_XP_MULT = 1.0
PARTIAL_SUCCESS_CREDIT_MULT = 0.6
PARTIAL_SUCCESS_XP_MULT = 0.7
FAILURE_CREDIT_MULT = 0.2
FAILURE_XP_MULT = 0.3
TIMEOUT_PENALTY_CREDITS = 100
TIMEOUT_PENALTY_MORALE = 10

# Morale system
MORALE_MAX = 100
MORALE_MIN = 0
MORALE_SUCCESS_GAIN = 5
MORALE_PARTIAL_LOSS = 2
MORALE_FAILURE_LOSS = 10
LOW_MORALE_THRESHOLD = 30
LOW_MORALE_PENALTY = 15  # % reduction in success chance

# Streak bonuses
STREAK_BONUS_PER_MISSION = 0.02  # 2% bonus per streak
MAX_STREAK_BONUS = 0.20  # Max 20% bonus

# Auto-inactive after ignoring missions
MAX_IGNORED_MISSIONS = 3

# Response types (used in buttons)
RESPONSE_TYPES = {
    "minimal": {"label": "Minimal Response", "cost_mult": 0.5, "success_mod": -15},
    "standard": {"label": "Standard Response", "cost_mult": 1.0, "success_mod": 0},
    "full": {"label": "Full Response", "cost_mult": 1.5, "success_mod": 10},
    "overwhelming": {"label": "Overwhelming Force", "cost_mult": 2.5, "success_mod": 20},
}

# Escalation chances
ESCALATION_CHANCE_BASE = 0.15  # 15% base chance
ESCALATION_CHANCE_PER_TIER = 0.10  # +10% per tier

# Database file
DB_FILE = "rapidresponse.db"

# Embed colors
COLOR_SUCCESS = 0x00FF00
COLOR_PARTIAL = 0xFFA500
COLOR_FAILURE = 0xFF0000
COLOR_INFO = 0x3498DB
COLOR_WARNING = 0xFFFF00
