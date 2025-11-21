"""
Vehicle parsing system for RapidResponse
Author: BrandjuhNL
"""

import re
from typing import Dict

# Comprehensive vehicle synonyms based on MissionChief USA
VEHICLE_SYNONYMS = {
    "firetrucks": {
        "codes": ["FT", "E", "ENG"],
        "names": ["fire truck", "fire engine", "engine", "pumper", "firetruck"]
    },
    "battalion_chief_vehicles": {
        "codes": ["BC", "CHIEF", "CMD"],
        "names": ["battalion chief", "chief", "command", "battalion", "bc vehicle"]
    },
    "platform_trucks": {
        "codes": ["PT", "PLAT", "LADDER", "L"],
        "names": ["platform truck", "ladder", "aerial", "tower ladder", "platform"]
    },
    "heavy_rescue_vehicles": {
        "codes": ["HR", "RESCUE", "R"],
        "names": ["heavy rescue", "rescue truck", "rescue", "heavy rescue vehicle"]
    },
    "mobile_command_vehicles": {
        "codes": ["MCV", "MC", "CMD"],
        "names": ["mobile command", "command vehicle", "mobile command vehicle", "mcv"]
    },
    "mobile_air_vehicles": {
        "codes": ["MAV", "AIR", "MA"],
        "names": ["mobile air", "air vehicle", "mobile air vehicle", "mav", "air unit"]
    },
    "water_tankers": {
        "codes": ["WT", "TANKER", "T"],
        "names": ["water tanker", "tanker", "water", "tender"]
    },
    "hazmat_vehicles": {
        "codes": ["HM", "HAZMAT", "HAZ"],
        "names": ["hazmat", "hazmat vehicle", "hazmat truck", "haz mat"]
    },
    "fire_investigation": {
        "codes": ["FI", "INV", "FIRE INV"],
        "names": ["fire investigation", "investigator", "fire inv", "investigation"]
    },
    "light_supply": {
        "codes": ["LS", "LIGHT"],
        "names": ["light supply", "lighting", "light unit", "lights"]
    },
    "technical_rescue": {
        "codes": ["TR", "TECH"],
        "names": ["technical rescue", "tech rescue", "technical"]
    },
    "police_cars": {
        "codes": ["PC", "POLICE", "P", "COP"],
        "names": ["police car", "police", "patrol", "cop car", "police unit"]
    },
    "police_helicopters": {
        "codes": ["PH", "HELI", "CHOPPER"],
        "names": ["police helicopter", "police heli", "helicopter", "chopper", "air support"]
    },
    "k9": {
        "codes": ["K9", "DOG"],
        "names": ["k9", "k-9", "dog", "police dog", "canine"]
    },
    "ambulances": {
        "codes": ["AMB", "A", "AMBO"],
        "names": ["ambulance", "ambo", "medic", "ems"]
    },
    "fwk": {
        "codes": ["FWK", "CRANE"],
        "names": ["fwk", "fire crane", "crane", "fire equipment"]
    },
}


def parse_vehicle_input(input_text: str) -> Dict[str, int]:
    """
    Parse player input for vehicle requirements.
    
    Args:
        input_text: Raw input text from player
        
    Returns:
        Dictionary mapping canonical vehicle names to counts
    """
    input_text = input_text.lower().strip()
    vehicle_counts = {}
    
    # Pattern 1: Code + Number (e.g., "FT2", "BC1")
    code_pattern = r'([a-z]+)(\d+)'
    for match in re.finditer(code_pattern, input_text, re.IGNORECASE):
        code = match.group(1).upper()
        count = int(match.group(2))
        
        # Find matching vehicle
        for vehicle_key, synonyms in VEHICLE_SYNONYMS.items():
            if code in synonyms["codes"]:
                vehicle_counts[vehicle_key] = vehicle_counts.get(vehicle_key, 0) + count
                break
    
    # Pattern 2: Number + Vehicle name (e.g., "2 fire trucks")
    # Pattern 3: Vehicle name + Number (e.g., "fire trucks 2")
    for vehicle_key, synonyms in VEHICLE_SYNONYMS.items():
        for name in synonyms["names"]:
            # Look for "number vehicle" or "vehicle number"
            patterns = [
                rf'(\d+)\s+{re.escape(name)}s?',  # "2 fire trucks"
                rf'{re.escape(name)}s?\s+(\d+)',  # "fire trucks 2"
            ]
            
            for pattern in patterns:
                for match in re.finditer(pattern, input_text, re.IGNORECASE):
                    count = int(match.group(1))
                    vehicle_counts[vehicle_key] = vehicle_counts.get(vehicle_key, 0) + count
            
            # Look for vehicle name without number (assume 1)
            standalone_pattern = rf'\b{re.escape(name)}s?\b'
            if re.search(standalone_pattern, input_text, re.IGNORECASE):
                # Only count if not already counted with a number
                if vehicle_key not in vehicle_counts:
                    vehicle_counts[vehicle_key] = 1
    
    return vehicle_counts


def get_vehicle_display_name(vehicle_key: str) -> str:
    """Get a friendly display name for a vehicle key."""
    if vehicle_key in VEHICLE_SYNONYMS:
        return VEHICLE_SYNONYMS[vehicle_key]["names"][0].title()
    return vehicle_key.replace("_", " ").title()
