"""
Mission Manager for fetching and processing MissionChief missions
"""
import aiohttp
import logging
import random
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from . import config

log = logging.getLogger("red.rapidresponse.mission_manager")


class MissionManager:
    """Manages MissionChief mission data"""
    
    def __init__(self, database):
        self.db = database
        self.missions: List[Dict[str, Any]] = []
        self.last_fetch: Optional[datetime] = None
        
    async def fetch_missions(self) -> bool:
        """Fetch missions from MissionChief JSON endpoint"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(config.MISSIONCHIEF_JSON_URL, timeout=30) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.missions = data
                        self.last_fetch = datetime.utcnow()
                        
                        # Cache in database
                        await self.db.cache_missions(data)
                        
                        log.info(f"Fetched {len(self.missions)} missions from MissionChief")
                        return True
                    else:
                        log.error(f"Failed to fetch missions: HTTP {resp.status}")
                        return False
        except Exception as e:
            log.error(f"Error fetching missions: {e}", exc_info=True)
            return False
    
    async def load_missions(self):
        """Load missions from cache or fetch if needed"""
        # Try loading from cache first
        cached = await self.db.get_cached_missions()
        
        if cached:
            self.missions = cached
            log.info(f"Loaded {len(self.missions)} missions from cache")
            
            # Check if cache is stale
            last_fetch_str = await self.db.get_config('last_mission_fetch')
            if last_fetch_str:
                self.last_fetch = datetime.fromisoformat(last_fetch_str)
                time_since_fetch = datetime.utcnow() - self.last_fetch
                
                if time_since_fetch > timedelta(hours=config.MISSION_CACHE_REFRESH_HOURS):
                    log.info("Mission cache is stale, fetching fresh data")
                    await self.fetch_missions()
            else:
                await self.fetch_missions()
        else:
            # No cache, fetch fresh
            log.info("No cached missions, fetching from API")
            await self.fetch_missions()
        
        # Store last fetch time
        if self.last_fetch:
            await self.db.set_config('last_mission_fetch', self.last_fetch.isoformat())
    
    async def refresh_if_needed(self):
        """Refresh missions if cache is stale"""
        if not self.last_fetch:
            await self.load_missions()
            return
        
        time_since_fetch = datetime.utcnow() - self.last_fetch
        if time_since_fetch > timedelta(hours=config.MISSION_CACHE_REFRESH_HOURS):
            log.info("Refreshing mission cache")
            await self.fetch_missions()
    
    def calculate_mission_tier(self, mission: Dict[str, Any]) -> int:
        """Calculate mission tier based on credits and requirements"""
        avg_credits = mission.get('average_credits')
        
        # Handle None or missing average_credits
        if avg_credits is None:
            avg_credits = 500  # Default to tier 1
        
        # Determine tier based on credits
        for tier, info in config.MISSION_TIERS.items():
            if info['min_credits'] <= avg_credits < info['max_credits']:
                return tier
        
        # Fallback to highest tier if above max
        return max(config.MISSION_TIERS.keys())
    
    def calculate_difficulty(self, mission: Dict[str, Any], tier: int) -> int:
        """Calculate difficulty score for a mission"""
        difficulty = tier * 25  # Base difficulty from tier
        
        # Add difficulty based on requirements
        requirements = mission.get('requirements', {})
        if requirements:
            # Count total units required
            total_units = sum(
                v for k, v in requirements.items()
                if isinstance(v, int) and k not in ['coins', 'credits']
            )
            difficulty += total_units * 2
        
        # Factor in special requirements
        if mission.get('prerequisites'):
            difficulty += 10
        
        # Factor in patients/prisoners if medical/police
        additional = mission.get('additional', {})
        if additional.get('max_patients', 0) > 0:
            difficulty += additional['max_patients'] * 3
        if additional.get('possible_prisoner_transport', False):
            difficulty += 5
        
        return min(difficulty, 100)  # Cap at 100
    
    def get_mission_category(self, mission: Dict[str, Any]) -> str:
        """Determine primary category of mission"""
        # Check mission categories array
        categories = mission.get('mission_categories', [])
        
        if not categories:
            return "general"
        
        # Map MissionChief categories to game categories
        category_map = {
            1: "fire",
            2: "medical",
            3: "police",
            4: "thw",  # Technical
            5: "rescue",
            6: "water",
        }
        
        # Return first recognized category
        for cat_id in categories:
            if cat_id in category_map:
                return category_map[cat_id]
        
        return "general"
    
    def select_mission_for_player(self, station_level: int) -> Optional[Dict[str, Any]]:
        """Select an appropriate mission for a player's level"""
        if not self.missions:
            return None
        
        # Weight missions by tier appropriateness
        weighted_missions = []
        
        for mission in self.missions:
            # Skip missions with invalid data
            avg_credits = mission.get('average_credits')
            if avg_credits is None:
                # Skip missions without credits defined
                continue
            
            tier = self.calculate_mission_tier(mission)
            
            # Calculate weight based on level
            # Lower levels get more lower tier missions
            # Higher levels get more higher tier missions
            if station_level <= 5:
                # Beginner: mostly tier 1-2
                weights = {1: 50, 2: 30, 3: 15, 4: 5}
            elif station_level <= 15:
                # Intermediate: mostly tier 2-3
                weights = {1: 20, 2: 40, 3: 30, 4: 10}
            elif station_level <= 30:
                # Advanced: mostly tier 3-4
                weights = {1: 10, 2: 20, 3: 40, 4: 30}
            else:
                # Expert: mostly tier 3-4
                weights = {1: 5, 2: 15, 3: 35, 4: 45}
            
            weight = weights.get(tier, 1)
            
            # Filter out event missions if they're not active
            additional = mission.get('additional', {})
            if additional and 'date_start' in additional and 'date_end' in additional:
                try:
                    # Check if event is currently active
                    # MissionChief uses timestamps
                    start = datetime.fromtimestamp(additional['date_start'])
                    end = datetime.fromtimestamp(additional['date_end'])
                    now = datetime.utcnow()
                    
                    if not (start <= now <= end):
                        continue  # Skip inactive event missions
                except:
                    pass  # If date parsing fails, include mission anyway
            
            weighted_missions.append((mission, weight))
        
        if not weighted_missions:
            log.warning(f"No valid missions found for player level {station_level}")
            return None
        
        # Random selection based on weights
        missions, weights = zip(*weighted_missions)
        selected = random.choices(missions, weights=weights, k=1)[0]
        
        return selected
    
    def generate_mission_description(self, mission: Dict[str, Any]) -> str:
        """Generate a description for the mission"""
        name = mission.get('name', 'Unknown Incident')
        place = mission.get('place', '')
        category = self.get_mission_category(mission)
        
        # Category-specific descriptions
        descriptions = {
            "fire": [
                f"A fire emergency at {place if place else 'a location'}. Flames and smoke reported.",
                f"Structure fire in progress at {place if place else 'the scene'}. Multiple units may be required.",
                f"Fire alarm activation at {place if place else 'a building'}. Investigation needed.",
            ],
            "medical": [
                f"Medical emergency at {place if place else 'a location'}. Patient requires immediate attention.",
                f"EMS response needed at {place if place else 'the scene'}. Vitals unknown.",
                f"Medical assistance requested at {place if place else 'a facility'}.",
            ],
            "police": [
                f"Police response required at {place if place else 'a location'}. Situation developing.",
                f"Law enforcement needed at {place if place else 'the scene'}. Details emerging.",
                f"Officers requested for incident at {place if place else 'the area'}.",
            ],
            "rescue": [
                f"Rescue operation at {place if place else 'a location'}. Specialized equipment may be needed.",
                f"Technical rescue required at {place if place else 'the scene'}.",
                f"Extraction needed at {place if place else 'a location'}.",
            ],
            "general": [
                f"Emergency response needed at {place if place else 'a location'}.",
                f"Incident reported at {place if place else 'the scene'}. Units requested.",
                f"Response required at {place if place else 'the area'}.",
            ]
        }
        
        category_descriptions = descriptions.get(category, descriptions["general"])
        base_description = random.choice(category_descriptions)
        
        # Add additional context
        additional = mission.get('additional', {})
        context_parts = []
        
        if additional.get('max_patients', 0) > 0:
            patients = additional['max_patients']
            context_parts.append(f"{patients} patient{'s' if patients > 1 else ''} involved")
        
        if additional.get('possible_prisoner_transport', False):
            context_parts.append("possible arrests")
        
        if additional.get('hazmat', False):
            context_parts.append("hazmat situation")
        
        if context_parts:
            base_description += f"\n**Additional info:** {', '.join(context_parts)}"
        
        return base_description
    
    def get_mission_requirements_text(self, mission: Dict[str, Any]) -> str:
        """Get formatted requirements text"""
        requirements = mission.get('requirements', {})
        
        if not requirements:
            return "No specific requirements"
        
        # Format requirements nicely
        req_parts = []
        for key, value in requirements.items():
            if isinstance(value, int) and value > 0:
                # Format key nicely
                formatted_key = key.replace('_', ' ').title()
                req_parts.append(f"{value}x {formatted_key}")
        
        if req_parts:
            return "\n".join(f"• {part}" for part in req_parts)
        else:
            return "Standard response units"
    
    def determine_max_stages(self, mission: Dict[str, Any], tier: int) -> int:
        """Determine if mission should be multi-stage"""
        # Higher tier missions have more chance of being multi-stage
        if tier == 1:
            stages = 1
        elif tier == 2:
            stages = random.choices([1, 2], weights=[70, 30])[0]
        elif tier == 3:
            stages = random.choices([1, 2, 3], weights=[50, 35, 15])[0]
        else:  # tier 4
            stages = random.choices([1, 2, 3], weights=[30, 45, 25])[0]
        
        return stages
