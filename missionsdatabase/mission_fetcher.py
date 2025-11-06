"""
Mission data fetcher and parser for MissionChief einsaetze.json
"""

import aiohttp
import hashlib
import json
from typing import Dict, List, Optional


class MissionFetcher:
    """Fetch and parse mission data from MissionChief."""
    
    MISSIONS_URL = "https://www.missionchief.com/einsaetze.json"
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def _ensure_session(self):
        """Ensure aiohttp session exists."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
    
    async def close(self):
        """Close the aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def fetch_missions(self) -> List[Dict]:
        """
        Fetch all missions from MissionChief einsaetze.json
        
        Returns:
            List of mission dictionaries
            
        Raises:
            aiohttp.ClientError: If the request fails
        """
        await self._ensure_session()
        
        async with self.session.get(self.MISSIONS_URL) as response:
            response.raise_for_status()
            missions = await response.json()
            
            # Check if it's already a list or a dict
            if isinstance(missions, list):
                # Already a list, just return it
                return missions
            elif isinstance(missions, dict):
                # It's a dict where keys are mission IDs
                # Convert to list with ID included
                mission_list = []
                for mission_id, mission_data in missions.items():
                    mission_data['id'] = mission_id
                    mission_list.append(mission_data)
                return mission_list
            else:
                # Unexpected format
                raise ValueError(f"Unexpected JSON format: {type(missions)}")
    
    @staticmethod
    def calculate_hash(mission_data: Dict) -> str:
        """
        Calculate a hash of mission data for change detection.
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            SHA256 hash of the mission data
        """
        # Create a stable JSON string (sorted keys)
        mission_json = json.dumps(mission_data, sort_keys=True)
        return hashlib.sha256(mission_json.encode()).hexdigest()
    
    @staticmethod
    def parse_mission_id(mission_data: Dict) -> str:
        """
        Extract the mission ID from mission data.
        Handles both simple IDs and overlay IDs (e.g., "88/a")
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            Mission ID as string
        """
        mission_id = str(mission_data.get('id', ''))
        
        # If there's a base_mission_id and additive_overlays, format as "base/overlay"
        base_id = mission_data.get('base_mission_id')
        overlay = mission_data.get('additive_overlays', '')
        
        if base_id is not None and overlay:
            return f"{base_id}/{overlay}"
        
        return mission_id
    
    @staticmethod
    def get_mission_name(mission_data: Dict) -> str:
        """
        Get the mission name.
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            Mission name
        """
        return mission_data.get('name', mission_data.get('caption', 'Unknown Mission'))
    
    @staticmethod
    def get_average_credits(mission_data: Dict) -> int:
        """
        Get average credits for the mission.
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            Average credits amount
        """
        return mission_data.get('average_credits', 0)
    
    @staticmethod
    def get_locations(mission_data: Dict) -> List[str]:
        """
        Get mission locations.
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            List of location strings
        """
        places = mission_data.get('place_array', [])
        if places:
            return places
        
        # Fallback to single place field
        place = mission_data.get('place', '')
        if place:
            return [place]
        
        return []
    
    @staticmethod
    def get_requirements(mission_data: Dict) -> Dict:
        """
        Get mission requirements (vehicles and equipment).
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            Dictionary of requirements
        """
        return mission_data.get('requirements', {})
    
    @staticmethod
    def get_chances(mission_data: Dict) -> Dict:
        """
        Get mission chances (probabilities for various events).
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            Dictionary of chances
        """
        return mission_data.get('chances', {})
    
    @staticmethod
    def get_additional_info(mission_data: Dict) -> Dict:
        """
        Get additional mission information.
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            Dictionary of additional information
        """
        return mission_data.get('additional', {})
    
    @staticmethod
    def get_prerequisites(mission_data: Dict) -> Dict:
        """
        Get mission prerequisites (unlock requirements).
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            Dictionary of prerequisites
        """
        return mission_data.get('prerequisites', {})
    
    @staticmethod
    def get_categories(mission_data: Dict) -> List[str]:
        """
        Get mission categories.
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            List of category strings
        """
        return mission_data.get('mission_categories', [])
    
    @staticmethod
    def get_patient_info(mission_data: Dict) -> Dict:
        """
        Extract patient-related information from mission data.
        
        Args:
            mission_data: Mission dictionary
            
        Returns:
            Dictionary with patient information
        """
        additional = mission_data.get('additional', {})
        chances = mission_data.get('chances', {})
        
        patient_info = {
            'possible_patients': additional.get('possible_patient', 0),
            'transport_chance': chances.get('patient_transport', 0),
            'specializations': [],
            'us_codes': []
        }
        
        # Get specialization captions (human-readable names)
        spec_captions = additional.get('patient_specialization_captions', [])
        if spec_captions:
            patient_info['specializations'] = spec_captions
        
        # Get US codes
        us_codes = additional.get('patient_us_code_possible', [])
        if us_codes:
            patient_info['us_codes'] = us_codes
        
        return patient_info
