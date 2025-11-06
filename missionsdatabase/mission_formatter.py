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
        
        # Compact info line: ID • Credits • Categories
        mission_id = MissionFormatter._format_mission_id(mission_data)
        avg_credits = mission_data.get('average_credits', 0)
        
        info_parts = [f"ID: {mission_id}", f"Avg. Credits: {avg_credits:,}"]
        sections.append(" • ".join(info_parts))
        
        # Categories on separate line with pipes
        categories = mission_data.get('mission_categories', [])
        if categories:
            formatted_categories = [cat.replace('_', ' ').title() for cat in categories]
            sections.append(f"Categories: {' | '.join(formatted_categories)}")
        
        sections.append("")
        
        # Prerequisites - compact format
        prerequisites_section = MissionFormatter._format_prerequisites(mission_data)
        if prerequisites_section:
            sections.append(prerequisites_section)
            sections.append("")
        
        # Requirements
        requirements_section = MissionFormatter._format_requirements(mission_data)
        if requirements_section:
            sections.append(requirements_section)
            sections.append("")
        
        # Patients
        patient_section = MissionFormatter._format_patients(mission_data)
        if patient_section:
            sections.append(patient_section)
            sections.append("")
        
        # Chances
        chances_section = MissionFormatter._format_chances(mission_data)
        if chances_section:
            sections.append(chances_section)
            sections.append("")
        
        # Locations (at the end)
        location_section = MissionFormatter._format_locations(mission_data)
        if location_section:
            sections.append(location_section)
        
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
    def _format_locations(mission_data: Dict) -> str:
        """Format location information."""
        places = mission_data.get('place_array', [])
        if not places:
            place = mission_data.get('place', '')
            if place:
                places = [place]
        
        if not places:
            return ""
        
        # Compact format: **Locations:** Various or specific list
        if len(places) == 1:
            return f"**Locations:** {places[0]}"
        else:
            return f"**Locations:** {', '.join(places)}"
    
    @staticmethod
    def _format_requirements(mission_data: Dict) -> str:
        """Format vehicle and equipment requirements."""
        requirements = mission_data.get('requirements', {})
        if not requirements:
            return ""
        
        lines = ["**Requirements:**"]
        
        for req_key, req_value in requirements.items():
            # Get the proper name for this requirement
            req_name = get_vehicle_name(req_key)
            lines.append(f"- {req_value} {req_name}")
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_patients(mission_data: Dict) -> str:
        """Format patient information."""
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
        
        # US Codes
        us_codes = additional.get('patient_us_code_possible', [])
        if us_codes:
            lines.append(f"- US Code: {', '.join(us_codes)}")
        
        return "\n".join(lines)
    
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
        
        # Main building (always show if present)
        main_building = prerequisites.get('main_building')
        if main_building is not None:
            building_name = get_building_name(main_building)
            lines.append(f"**Main Building:** {building_name}")
        
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
        Examples: fire_stations -> Fire, police_stations -> Police
        """
        abbreviations = {
            'fire_stations': 'Fire',
            'police_stations': 'Police',
            'ambulance_stations': 'Ambulance',
            'rescue_stations': 'Rescue',
            'hospitals': 'Hospitals',
            'fire_academies': 'Fire Academies',
            'police_academies': 'Police Academies',
            'rescue_academies': 'EMS Academies',
            'dispatch_centers': 'Dispatch',
            'staging_areas': 'Staging Areas',
            'prisons': 'Prisons',
            'fire_boat_docks': 'Fire Boat Docks',
            'rescue_boat_docks': 'Rescue Boat Docks',
            'coastal_rescue_stations': 'Coastal Rescue',
            'coastal_air_stations': 'Coastal Air',
            'lifeguard_posts': 'Lifeguard Posts',
            'tow_truck_stations': 'Tow Stations',
            'federal_police_stations': 'Federal Police',
            'firefighting_plane_stations': 'Firefighting Planes',
            'fire_marshals_offices': "Fire Marshal's Offices"
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
