"""Constants for the FireStationCommander MVP."""

from __future__ import annotations

from pathlib import Path


PACKAGE_ROOT = Path(__file__).parent
DATA_DIR = PACKAGE_ROOT / "data"

DEFAULT_START_CASH = 5000
DEFAULT_SAFETY_SCORE = 75
DEFAULT_MORALE_SCORE = 75
BASE_GARAGE_SLOTS = 2
BASE_STORAGE_SLOTS = 8

INCIDENT_TIMEOUT_MINUTES = 30
XP_PER_LEVEL = 250

VEHICLE_STATUS_AVAILABLE = "available"
VEHICLE_STATUS_ASSIGNED = "assigned"
VEHICLE_STATUS_MAINTENANCE = "maintenance"

INCIDENT_STATUS_ACTIVE = "active"
INCIDENT_STATUS_COMPLETED = "completed"
INCIDENT_STATUS_IGNORED = "ignored"

STARTER_VEHICLE_KEY = "ts"
STARTER_EQUIPMENT_KEYS = (
    "breathing_apparatus",
    "thermal_camera",
    "hose_pack",
    "aed",
)
