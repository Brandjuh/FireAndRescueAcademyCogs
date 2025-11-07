"""
Mission formatter for creating Discord forum posts.
"""

from typing import Dict, List
from .mappings import (
    get_building_name,
    get_vehicle_name,
    get_extension_name,
    get_specialization_name,
    format_field_name,
    EQUIPMENT
)


class MissionFormatter:
    """Format mission data into Discord forum posts."""
    
    @staticmethod
    def format_mission_post(mission_data: Dict) -> str:
        """
        Format mission data into a complete forum post.
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            Formatted post content
        """
        sections = []
        
        # Header - Mission Name (bold)
        mission_name = mission_data.get('name', mission_data.get('caption', 'Unknown Mission'))
        sections.append(f"**{mission_name}**")
        sections.append("")
        
        # Compact info line: ID • Credits (both bold)
        mission_id = MissionFormatter._format_mission_id(mission_data)
        avg_credits = mission_data.get('average_credits', 0)
        
        sections.append(f"**ID:** {mission_id} • **Avg. Credits:** {avg_credits:,}")
        
        # Categories (bold header)
        categories = mission_data.get('mission_categories', [])
        if categories:
            formatted_categories = [cat.replace('_', ' ').title() for cat in categories]
            sections.append(f"**Categories:** {' | '.join(formatted_categories)}")
        
        sections.append("")
        
        # Prerequisites - compact format
        prerequisites_section = MissionFormatter._format_prerequisites(mission_data)
        if prerequisites_section:
            sections.append(prerequisites_section)
            sections.append("")
        
        # Requirements - separate vehicles and equipment
        requirements_section = MissionFormatter._format_requirements(mission_data)
        if requirements_section:
            sections.append(requirements_section)
            sections.append("")
        
        # Patients
        patient_section = MissionFormatter._format_patients(mission_data)
        if patient_section:
            sections.append(patient_section)
            sections.append("")
        
        # Prisoners
        prisoner_section = MissionFormatter._format_prisoners(mission_data)
        if prisoner_section:
            sections.append(prisoner_section)
            sections.append("")
        
        # Chances
        chances_section = MissionFormatter._format_chances(mission_data)
        if chances_section:
            sections.append(chances_section)
            sections.append("")
        
        # POI (at the end)
        poi_section = MissionFormatter._format_poi(mission_data)
        if poi_section:
            sections.append(poi_section)
        
        return "\n".join(sections)
    
    @staticmethod
    def _format_mission_id(mission_data: Dict) -> str:
        """Format the mission ID with overlay information if present."""
        base_id = mission_data.get('base_mission_id')
        overlay = mission_data.get('additive_overlays', '')
        mission_id = mission_data.get('id', '')
        
        if base_id is not None and overlay:
            return f"{base_id}/{overlay.upper()}"
        
        return str(mission_id)
    
    @staticmethod
    def _format_poi(mission_data: Dict) -> str:
        """Format POI (Point of Interest) information."""
        places = mission_data.get('place_array', [])
        if not places:
            place = mission_data.get('place', '')
            if place:
                places = [place]
        
        if not places:
            return ""
        
        # Compact format: **POI:** Various or specific list
        if len(places) == 1:
            return f"**POI:** {places[0]}"
        else:
            return f"**POI:** {', '.join(places)}"
    
    @staticmethod
    def _format_requirements(mission_data: Dict) -> str:
        """Format vehicle and equipment requirements with separate sections."""
        requirements = mission_data.get('requirements', {})
        if not requirements:
            return ""
        
        vehicles = []
        equipment = []
        
        for req_key, req_value in requirements.items():
            req_name = get_vehicle_name(req_key)
            
            # Check if this is equipment
            if req_key in EQUIPMENT or 'equipment' in req_key.lower() or req_key in [
                'light_supply', 'hose', 'breathing_protection', 'flood_equipment',
                'fire_rescue', 'hazmat', 'energy_supply', 'foam_carrier', 
                'fire_engine', 'wildfire_engine', 'search_and_rescue',
                'technical_rescue', 'fire_water_carrier', 'fire_water_carrier_2',
                'fire_water_carrier_3', 'fire_ladder', 'water_rescue_boat',
                'fire_command_advanced', 'fire_crane'
            ]:
                # Handle "oneof" format
                if 'oneof' in str(req_value).lower():
                    # Parse "1 oneof fire engine or rescue" -> "1 Fire Engine OR Rescue"
                    equipment.append(f"- {MissionFormatter._format_oneof(req_value, req_name)}")
                else:
                    equipment.append(f"- {req_value} {req_name}")
            else:
                # It's a vehicle
                if 'oneof' in str(req_value).lower():
                    vehicles.append(f"- {MissionFormatter._format_oneof(req_value, req_name)}")
                else:
                    vehicles.append(f"- {req_value} {req_name}")
        
        lines = []
        
        if vehicles:
            lines.append("**Vehicle Requirements:**")
            lines.extend(vehicles)
        
        if equipment:
            if vehicles:
                lines.append("")  # Empty line between sections
            lines.append("**Equipment Requirements:**")
            lines.extend(equipment)
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_oneof(req_value, req_name: str) -> str:
        """
        Format 'oneof' requirements like '1 oneof fire engine or rescue' 
        into '1 Fire Engine OR Rescue'
        """
        # If it's already a simple number, just return formatted normally
        if isinstance(req_value, (int, float)):
            return f"{req_value} {req_name}"
        
        # Convert to string and parse
        req_str = str(req_value).lower()
        
        if 'oneof' not in req_str:
            return f"{req_value} {req_name}"
        
        # Parse "1 oneof fire engine or rescue"
        # Extract the number
        parts = req_str.split('oneof')
        count = parts[0].strip() if len(parts) > 0 else "1"
        
        # Extract the options after "oneof"
        if len(parts) > 1:
            options_str = parts[1].strip()
            # Split by "or" and capitalize each option
            options = [opt.strip().title() for opt in options_str.split(' or ')]
            return f"{count} {' OR '.join(options)}"
        
        return f"{req_value} {req_name}"
    
    @staticmethod
    def _format_patients(mission_data: Dict) -> str:
        """Format patient information with transport requirements."""
        additional = mission_data.get('additional', {})
        
        possible_patients = additional.get('possible_patient', 0)
        if possible_patients == 0:
            return ""
        
        lines = ["**Patients:**"]
        lines.append(f"- Up to {possible_patients}")
        
        # Specializations
        spec_captions = additional.get('patient_specialization_captions', [])
        if spec_captions:
            lines.append(f"- Required Spec: {', '.join(spec_captions)}")
        
        # US Codes with transport type
        us_codes = additional.get('patient_us_code_possible', [])
        if us_codes:
            transport_types = []
            for code in us_codes:
                transport_type = MissionFormatter._get_transport_type_from_code(code)
                transport_types.append(transport_type)
            
            # Remove duplicates while preserving order
            unique_transports = []
            for t in transport_types:
                if t not in unique_transports:
                    unique_transports.append(t)
            
            lines.append(f"- US Code: {', '.join(us_codes)} ({', '.join(unique_transports)})")
        
        return "\n".join(lines)
    
    @staticmethod
    def _get_transport_type_from_code(us_code: str) -> str:
        """
        Determine transport type (ALS/BLS/HEMS) from US medical code.
        
        US code format: XX-Y-Z where:
        - First digit indicates severity/transport type
        """
        code_upper = us_code.upper()
        
        # Parse the code
        parts = code_upper.split('-')
        if len(parts) < 2:
            return "ALS"
        
        # First number indicates priority/severity
        try:
            priority = int(parts[0])
            
            # High priority codes typically need ALS or HEMS
            if priority >= 30:  # Echo/Delta level
                return "HEMS"
            elif priority >= 20:  # Charlie level
                return "ALS"
            else:  # Alpha/Bravo level
                return "BLS"
        except:
            # Default to ALS if we can't parse
            return "ALS"
    
    @staticmethod
    def _format_prisoners(mission_data: Dict) -> str:
        """Format prisoner information."""
        additional = mission_data.get('additional', {})
        
        # Check for possible prisoner count
        possible_prisoners_min = additional.get('possible_prisoner_count_min', 0)
        possible_prisoners_max = additional.get('possible_prisoner_count_max', 0)
        possible_prisoners = additional.get('possible_prisoner_count', 0)
        
        # Use whichever field has data
        if possible_prisoners_min > 0 or possible_prisoners_max > 0:
            if possible_prisoners_min == possible_prisoners_max:
                return f"**Prisoners:** {possible_prisoners_min}"
            else:
                return f"**Prisoners:** {possible_prisoners_min} - {possible_prisoners_max}"
        elif possible_prisoners > 0:
            return f"**Prisoners:** Up to {possible_prisoners}"
        
        return ""
    
    @staticmethod
    def _format_chances(mission_data: Dict) -> str:
        """Format mission chances/probabilities."""
        chances = mission_data.get('chances', {})
        if not chances:
            return ""
        
        lines = ["**Chances:**"]
        
        # Format each chance type
        chance_labels = {
            'patient_transport': 'hospital transport',
            'prisoner': 'prisoners',
            'hazmat': 'hazmat',
            'fire': 'fire',
            'heavy_rescue': 'heavy rescue'
        }
        
        for chance_key, chance_value in chances.items():
            label = chance_labels.get(chance_key, format_field_name(chance_key).lower())
            lines.append(f"- {chance_value}% chance of {label}")
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_prerequisites(mission_data: Dict) -> str:
        """Format unlock prerequisites."""
        prerequisites = mission_data.get('prerequisites', {})
        if not prerequisites:
            return ""
        
        lines = []
        
        # Main building (always show if present and not -1)
        main_building = prerequisites.get('main_building')
        if main_building is not None and main_building != -1:
            building_name = get_building_name(main_building)
            lines.append(f"**Generated by:** {building_name}")
        elif main_building == -1:
            lines.append(f"**Generated by:** Any Station")
        
        # Build the "Unlocks at" line with all other requirements
        unlock_parts = []
        
        for prereq_key, prereq_value in prerequisites.items():
            if prereq_key == 'main_building':
                continue  # Already handled
            
            # Abbreviate building names for compact display
            if '_extension' in prereq_key or '_count' in prereq_key or prereq_key in ['tow_trucks']:
                # Extension requirement
                extension_name = get_extension_name(prereq_key)
                # Shorten "Extension" to "Ext."
                extension_name = extension_name.replace(' Extension', ' Ext.')
                unlock_parts.append(f"{prereq_value} {extension_name}")
            else:
                # Building count requirement - abbreviate
                building_abbrev = MissionFormatter._abbreviate_building(prereq_key)
                unlock_parts.append(f"{prereq_value} {building_abbrev}")
        
        if unlock_parts:
            lines.append(f"**Unlocks at:** {' • '.join(unlock_parts)}")
        
        return "\n".join(lines)
    
    @staticmethod
    def _abbreviate_building(building_key: str) -> str:
        """
        Abbreviate building names for compact display.
        Examples: fire_stations -> Fire Stations, police_stations -> Police Stations
        """
        abbreviations = {
            'fire_stations': 'Fire Stations',
            'police_stations': 'Police Stations',
            'ambulance_stations': 'Ambulance Stations',
            'rescue_stations': 'Rescue Stations',
            'hospitals': 'Hospitals',
            'fire_academies': 'Fire Academies',
            'police_academies': 'Police Academies',
            'rescue_academies': 'EMS Academies',
            'dispatch_centers': 'Dispatch Centers',
            'staging_areas': 'Staging Areas',
            'prisons': 'Prisons',
            'fire_boat_docks': 'Fire Boat Docks',
            'rescue_boat_docks': 'Rescue Boat Docks',
            'coastal_rescue_stations': 'Coastal Rescue Stations',
            'coastal_air_stations': 'Coastal Air Stations',
            'lifeguard_posts': 'Lifeguard Posts',
            'tow_truck_stations': 'Tow Truck Stations',
            'federal_police_stations': 'FBI Stations',
            'firefighting_plane_stations': 'Firefighting Plane Stations',
            'fire_marshals_offices': "Fire Marshal's Offices",
            'medical_helicopter_stations': 'Medical Helicopter Stations',
            'police_aviation': 'Police Aviation Stations'
        }
        
        return abbreviations.get(building_key, format_field_name(building_key))
    
    @staticmethod
    def get_mission_title(mission_data: Dict) -> str:
        """
        Get the mission title for the forum thread.
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            Mission title string
        """
        return mission_data.get('name', mission_data.get('caption', 'Unknown Mission'))
