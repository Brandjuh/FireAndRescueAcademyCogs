"""
Mappings for MissionChief buildings, vehicles, equipment, and trainings.
Data derived from LSSM (Leitstellenspiel Manager) sources.
"""

# Building type ID to name mapping
BUILDINGS = {
    0: "Fire Station",
    1: "Dispatch Center",
    2: "Hospital",
    3: "Ambulance Station",
    4: "Fire Academy",
    5: "Police Station",
    6: "Medical Helicopter Station",
    7: "Police Academy",
    8: "Police Aviation",
    9: "Staging Area",
    10: "Prison",
    11: "Fire Boat Dock",
    12: "Rescue Boat Dock",
    13: "Fire Station (Small Station)",
    14: "Clinic",
    15: "Police Station (Small Station)",
    16: "Ambulance Station (Small Station)",
    17: "Firefighting Plane Station",
    18: "Federal Police Station",
    19: "Rescue (EMS) Academy",
    22: "Fire Marshal's Office",
    23: "Coastal Rescue Station",
    24: "Coastal Rescue School",
    25: "Coastal Air Station",
    26: "Lifeguard Post",
    27: "Tow Truck Station"
}

# Vehicle type mappings (caption from vehicle data)
VEHICLES = {
    0: "Type 1 Fire Engine",
    1: "Type 2 Fire Engine",
    2: "Platform Truck",
    3: "Battalion Chief Unit",
    4: "Heavy Rescue Vehicle",
    5: "ALS Ambulance",
    6: "Mobile Air",
    7: "Water Tanker",
    8: "Utility Unit",
    9: "HazMat",
    10: "Patrol Car",
    11: "HEMS",
    12: "Mobile Command Vehicle",
    13: "Quint",
    14: "Police Helicopter",
    15: "Fly-Car",
    16: "SWAT Armoured Vehicle",
    17: "Large ARFF Crash Tender",
    18: "Rescue Engine",
    19: "K-9 Unit",
    20: "Mass Casualty Unit",
    21: "Heavy Rescue + Light Boat",
    22: "Light Boat Trailer",
    23: "Police Motorcycle",
    24: "Large Fireboat",
    25: "Large Rescue Boat",
    26: "SWAT SUV",
    27: "BLS Ambulance",
    28: "EMS Rescue",
    29: "EMS Chief",
    30: "Type 3 Engine",
    31: "Type 5 Engine",
    32: "Type 7 Engine",
    33: "Pumper Tanker",
    34: "Crew Carrier",
    35: "Water Drop Helicopter",
    36: "Air Tanker",
    37: "Heavy Air Tanker",
    38: "Type 4 Engine",
    39: "Type 6 Engine",
    40: "Dozer Trailer",
    41: "Crew Cab Semi",
    42: "FBI Unit",
    43: "FBI Investigation Wagon",
    44: "FBI Mobile Command Center",
    45: "FBI Bomb Technician Vehicle",
    46: "FBI Surveillance Drone",
    47: "Police Supervisor / Sheriff Unit",
    48: "EMS Fire Engine/Ambulance",
    49: "Tactical Ambulance",
    50: "Hazmat Ambulance",
    51: "DEA Unit",
    52: "DEA Clan Lab",
    53: "ATF Unit",
    54: "ATF Lab Vehicle",
    55: "Patrol Boat",
    56: "Warden's Truck",
    57: "EMS Mass Casualty Trailer (Large)",
    58: "EMS Mass Casualty Trailer (Small)",
    59: "EMS Operations Support",
    60: "EMS Mobile Command Unit",
    61: "ALS Rescue Ambulance",
    62: "Fire Investigator Unit",
    63: "Fire Prevention Unit",
    64: "Foam Tender",
    65: "Foam Trailer",
    66: "Lifeguard Truck",
    67: "Lifeguard Rescue",
    68: "Lifeguard Supervisor",
    69: "Small Coastal Boat",
    70: "Large Coastal Boat",
    71: "Coastal Helicopter",
    72: "Coastal Guard Plane",
    73: "Small Coastal Boat Trailer",
    74: "Wildfire MCC",
    75: "Wildland Lead Plane",
    76: "Smoke Jumper Plane",
    77: "Tanker Semi Truck Trailer",
    78: "Tanker Trailer",
    79: "Small ARFF Crash Tender",
    80: "Medium ARFF Crash Tender",
    81: "Small K9 Carrier",
    82: "Large K9 Carrier",
    83: "Riot Police Van",
    84: "Riot Police Bus",
    85: "Riot Police Trailer",
    86: "Police Crew Carrier",
    87: "Police Prisoner Van",
    88: "Police ATV Trailer",
    89: "Police MCV",
    90: "Tactical Rescue Truck",
    91: "Flood Equipment Trailer",
    92: "Mobile Air Trailer",
    93: "Light Tower Trailer",
    94: "Energy Generator Trailer",
    95: "Double Light Boat Trailer",
    96: "Small Heavy Rescue Trailer",
    97: "Large Heavy Rescue Trailer",
    98: "Small HazMat Trailer",
    99: "Large HazMat Trailer",
    100: "Tiller Ladder Trailer",
    101: "Police Traffic Control Unit",
    102: "Police Traffic Blocker Unit",
    103: "Fire Traffic Control Unit",
    104: "Fire Traffic Blocker Unit",
    105: "Wrecker",
    106: "Flatbed Carrier",
    107: "Fire Wrecker",
    108: "Police Wrecker",
    109: "CCTU",
    110: "Tactical Rescue Truck with Boat",
    111: "Police Water Rescue Boat Trailer",
    112: "Police Water Rescue Double Boat Trailer",
    113: "Rotator Truck"
}

# Equipment mappings
EQUIPMENT = {
    "breathing_protection": "Mobile Air Equipment",
    "flood_equipment": "Flood Equipment",
    "fire_rescue": "Heavy Rescue Equipment",
    "hose": "Water Tank Equipment",
    "light_supply": "Light Tower Equipment",
    "hazmat": "HazMat Equipment",
    "energy_supply": "Energy Generator Equipment",
    "foam_carrier": "Foam Tank Equipment",
    "fire_engine": "Fire Hose Equipment",
    "wildfire_engine": "Wildland Fire Engine Equipment",
    "search_and_rescue": "Search and Rescue Equipment",
    "technical_rescue": "Technical Rescue Equipment",
    "fire_water_carrier": "Small Portable Pond",
    "fire_water_carrier_2": "Medium Portable Pond",
    "fire_water_carrier_3": "Large Portable Pond",
    "fire_ladder": "Ladder Rack",
    "water_rescue_boat": "Swift Water Rescue Boat",
    "fire_command_advanced": "Radio Equipment",
    "fire_crane": "Fire Crane"
}

# Hospital extensions/specializations (for patient requirements)
HOSPITAL_SPECIALIZATIONS = {
    0: "General Internal",
    1: "General Surgeon",
    2: "Gynecology",
    3: "Urology",
    4: "Traumatology",
    5: "Neurology",
    6: "Neurosurgery",
    7: "Cardiology",
    8: "Cardiac Surgery"
}

# Training/Schooling names
TRAININGS = {
    "gw_gefahrgut": "HazMat",
    "elw2": "Mobile Command",
    "arff": "ARFF-Training",
    "gw_wasserrettung": "Swift Water Rescue",
    "ocean_navigation": "Ocean Navigation",
    "airborne_firefighting": "Airborne Firefighting",
    "heavy_machinery": "Heavy Machinery Operating",
    "truck_drivers_license": "Truck Driver's License",
    "ambulance_fire_truck": "ALS Medical Training for Fire Apparatus",
    "ambulance_police_car": "Tactical Medic Training",
    "ems_mobile_command": "EMS Mobile Command",
    "fire_investigator": "Law Enforcement for Arson Investigation",
    "coastal_command": "Lifeguard Supervisor",
    "coastal_rescue": "Lifeguard Training",
    "brush_air_command": "Wildland Lead Pilot Training",
    "elw3": "Wildland Mobile Command Center Training",
    "hotshot": "Hotshot Crew Training",
    "smoke_jumper": "Smoke Jumper Training",
    "traffic_control": "Traffic Control Training",
    "search_and_rescue": "Search and Rescue Training",
    "technical_rescue": "Technical Rescue Training",
    "critical_care": "Critical Care",
    "polizeihubschrauber": "Police Aviation",
    "swat": "SWAT",
    "k9": "K-9",
    "police_motorcycle": "Police Motorcycle",
    "fbi_mcc": "FBI Mobile Center Commander",
    "fbi_bomb_tech": "FBI Bomb Technician",
    "fbi_drone_operator": "FBI Drone Operator",
    "sheriff": "Police Supervisor / Sheriff",
    "game_warden": "Environmental Game Warden",
    "riot_police": "Riot Police Training",
    "elw_police": "Police Operations Management",
    "tactical_medic": "Tactical Rescue Training",
    "sniper": "Sharpshooter Training",
    "coastal_rescue_pilot": "Coastal Air Rescue Operations",
    "law_enforcement_marine": "Law Enforcement Marine (TACLET)"
}

# Extension name mappings for prerequisites
# Format: "prerequisite_field_name": "Extension Name"
EXTENSION_NAMES = {
    "airport_extension": "Airport Extension",
    "water_rescue_extension": "Water Rescue Extension",
    "forestry_expansion": "Forestry Expansion",
    "fire_investigation_extension": "Fire Investigation Extension",
    "foam_extension": "Foam Extension",
    "lifeguard_extension": "Lifeguard Extension",
    "wildland_command_extension": "Wildland Command Extension",
    "flood_control_extension": "Flood Control Extension",
    "disaster_response_count": "Disaster Response Extension",
    "disaster_response_extension": "Disaster Response Extension",
    "traffic_control_extension": "Traffic Control Extension",
    "tow_truck_extension": "Tow Truck Extension",
    "tow_trucks": "Tow Truck Extension",
    "water_police_extension": "Water Police Extension",
    "game_warden_office": "Game Warden Office",
    "k9_carrier_extension": "K9 Carrier Extension",
    "riot_police_extension": "Riot Police Extension",
    "detention_unit_extension": "Detention Unit Extension",
    "federal_police_extension": "Federal Police Extension",
    "bomb_squad_extension": "Bomb Squad Extension",
    "bomb_disposal_count": "Bomb Squad Extension",
    "police_water_rescue": "Police Water Rescue Extension",
    "smoke_jumper_extension": "Smoke Jumper Extension",
    "atf_expansion": "ATF Expansion",
    "dea_expansion": "DEA Expansion",
    "ambulance_extension": "Ambulance Extension",
    "mass_casualty_trailer_extension": "Mass Casualty Trailer Extension"
}


def format_field_name(field_name: str) -> str:
    """
    Convert a snake_case field name to a human-readable format.
    Example: 'fire_stations' -> 'Fire Stations'
    """
    return field_name.replace('_', ' ').title()


def get_building_name(building_id: int) -> str:
    """Get building name from ID, or return formatted ID if not found."""
    return BUILDINGS.get(building_id, f"Building #{building_id}")


def get_vehicle_name(vehicle_key: str) -> str:
    """
    Get vehicle name from requirement key.
    Handles both plurals and special cases.
    """
    # Try direct lookup first (for equipment)
    if vehicle_key in EQUIPMENT:
        return EQUIPMENT[vehicle_key]
    
    # Common vehicle key patterns and abbreviations
    vehicle_mappings = {
        "firetrucks": "Fire Trucks",
        "platform_trucks": "Platform Trucks",
        "battalion_chief_vehicles": "Battalion Chief Vehicles",
        "heavy_rescue_vehicles": "Heavy Rescue Vehicles",
        "police_cars": "Patrol Cars",
        "ambulances": "Ambulances",
        "fly_cars": "Fly-Cars",
        "mobile_air_vehicles": "Mobile Air",
        "water_tankers": "Water Tankers",
        "utility_vehicles": "Utility Units",
        "hazmat_vehicles": "HazMat",
        "quints": "Quints",
        "rescue_engines": "Rescue Engines",
        "k9_units": "K-9 Units",
        "swat_vehicles": "SWAT Vehicles",
        "swat_armoured_vehicles": "SWAT Armoured Vehicles",
        "swat_suvs": "SWAT SUVs",
        "police_motorcycles": "Police Motorcycles",
        "sheriff_units": "Sheriff Units",
        "mass_casualty_units": "Mass Casualty Units",
        "ems_chiefs": "EMS Chiefs",
        "mobile_command_vehicles": "Mobile Command Vehicles",
        "hems": "HEMS",
        "police_helicopters": "Police Helicopters",
        "fbi_units": "FBI Units",
        "fbi_investigation_wagons": "FBI Investigation Wagons",
        "fbi_mobile_command_centers": "FBI Mobile Command Centers",
        "fbi_bomb_technician_vehicles": "FBI Bomb Technician Vehicles",
        "fbi_surveillance_drones": "FBI Surveillance Drones",
        "dea_units": "DEA Units",
        "dea_clan_labs": "DEA Clan Labs",
        "atf_units": "ATF Units",
        "atf_lab_vehicles": "ATF Lab Vehicles",
        "patrol_boats": "Patrol Boats",
        "wardens_trucks": "Warden's Trucks",
        "riot_police_vans": "Riot Police Vans",
        "riot_police_buses": "Riot Police Buses",
        "police_prisoner_vans": "Police Prisoner Vans",
        "tow_trucks": "Tow Trucks",
        "wreckers": "Wreckers",
        "flatbed_carriers": "Flatbed Carriers",
        # German/European abbreviations
        "elw2": "Mobile Command Vehicle",
        "elw3": "Wildfire MCC",
        "gw_gefahrgut": "HazMat",
        "gw_wasserrettung": "Heavy Rescue + Light Boat",
        "fwk": "Crew Carrier",
        "gwm": "Mobile Air",
        "rw": "Heavy Rescue Vehicle",
        "dlk": "Platform Truck"
    }
    
    if vehicle_key in vehicle_mappings:
        return vehicle_mappings[vehicle_key]
    
    # Fallback: format the field name
    return format_field_name(vehicle_key)


def get_extension_name(extension_key: str) -> str:
    """Get extension name from prerequisite field key."""
    if extension_key in EXTENSION_NAMES:
        return EXTENSION_NAMES[extension_key]
    return format_field_name(extension_key)


def get_specialization_name(spec_id: int) -> str:
    """Get hospital specialization name from ID."""
    return HOSPITAL_SPECIALIZATIONS.get(spec_id, f"Specialization #{spec_id}")
