import aiohttp
import re
import json
import logging
from typing import Dict, List, Any, Optional, Tuple

log = logging.getLogger("red.assetmanager.github")

GITHUB_URLS = {
    "vehicles": "https://raw.githubusercontent.com/LSS-Manager/LSSM-V.4/dev/src/i18n/en_US/vehicles.ts",
    "buildings": "https://raw.githubusercontent.com/LSS-Manager/LSSM-V.4/dev/src/i18n/en_US/buildings.ts",
    "equipment": "https://raw.githubusercontent.com/LSS-Manager/LSSM-V.4/dev/src/i18n/en_US/equipment.ts",
    "educations": "https://raw.githubusercontent.com/LSS-Manager/LSSM-V.4/dev/src/i18n/en_US/schoolings.ts"
}


class GitHubSync:
    """Handles fetching and parsing data from GitHub."""
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def create_session(self):
        """Create aiohttp session."""
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
            
    async def close_session(self):
        """Close aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
            
    async def fetch_file(self, url: str) -> Optional[str]:
        """Fetch a file from GitHub."""
        await self.create_session()
        
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with self.session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    content = await response.text()
                    log.info(f"Successfully fetched: {url}")
                    return content
                else:
                    log.error(f"Failed to fetch {url}: Status {response.status}")
                    return None
        except aiohttp.ClientError as e:
            log.error(f"Network error fetching {url}: {e}")
            return None
        except Exception as e:
            log.error(f"Unexpected error fetching {url}: {e}")
            return None
    
    def parse_typescript_export(self, content: str) -> Optional[Dict[int, Any]]:
        """Parse TypeScript export default object to Python dict."""
        try:
            # Remove comments
            content = re.sub(r'//.*', '', content)
            content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
            content = re.sub(r'import\s+.*?;', '', content, flags=re.DOTALL)
            
            # Find the export default block
            match = re.search(r'export\s+default\s+(\{.+\})', content, re.DOTALL)
            if not match:
                log.error("Could not find export default pattern")
                return None
            
            obj_str = match.group(1)
            
            # Remove numeric separators
            obj_str = re.sub(r'(\d)_(\d)', r'\1\2', obj_str)
            
            # Convert single quotes to double quotes
            obj_str = obj_str.replace("'", '"')
            
            # Quote numeric keys at start of lines
            obj_str = re.sub(r'([\n\r]\s*)(\d+)(\s*):', r'\1"\2"\3:', obj_str)
            
            # Quote alphabetic keys more carefully
            lines = obj_str.split('\n')
            result_lines = []
            
            for line in lines:
                # Match pattern: whitespace + word + whitespace + colon
                # Only at the start of properties (after { or ,)
                new_line = re.sub(
                    r'^(\s*)([a-zA-Z_][a-zA-Z0-9_]*)(\s*):',
                    r'\1"\2"\3:',
                    line
                )
                # Also match after comma on same line
                new_line = re.sub(
                    r'(,\s*)([a-zA-Z_][a-zA-Z0-9_]*)(\s*):',
                    r'\1"\2"\3:',
                    new_line
                )
                # Match after opening brace
                new_line = re.sub(
                    r'(\{\s*)([a-zA-Z_][a-zA-Z0-9_]*)(\s*):',
                    r'\1"\2"\3:',
                    new_line
                )
                result_lines.append(new_line)
            
            obj_str = '\n'.join(result_lines)
            
            # Remove trailing commas
            obj_str = re.sub(r',(\s*[\}\]])', r'\1', obj_str)
            
            # Parse to JSON
            parsed = json.loads(obj_str)
            
            # Convert string keys to int keys
            result = {}
            for key, value in parsed.items():
                try:
                    result[int(key)] = value
                except ValueError:
                    result[key] = value
            
            return result
            
        except json.JSONDecodeError as e:
            log.error(f"JSON decode error at position {e.pos}: {e.msg}")
            if obj_str and hasattr(e, 'pos'):
                start = max(0, e.pos - 200)
                end = min(len(obj_str), e.pos + 200)
                context = obj_str[start:end]
                log.error(f"Context: {context}")
            return None
        except Exception as e:
            log.error(f"Parse error: {e}", exc_info=True)
            return None
    
    async def fetch_vehicles(self) -> Optional[Dict[int, Dict[str, Any]]]:
        """Fetch and parse vehicles data."""
        content = await self.fetch_file(GITHUB_URLS["vehicles"])
        if not content:
            return None
        
        parsed = self.parse_typescript_export(content)
        if parsed:
            log.info(f"Parsed {len(parsed)} vehicles")
        return parsed
    
    async def fetch_buildings(self) -> Optional[Dict[int, Dict[str, Any]]]:
        """Fetch and parse buildings data."""
        content = await self.fetch_file(GITHUB_URLS["buildings"])
        if not content:
            return None
        
        parsed = self.parse_typescript_export(content)
        if parsed:
            log.info(f"Parsed {len(parsed)} buildings")
        return parsed
    
    async def fetch_equipment(self) -> Optional[Dict[int, Dict[str, Any]]]:
        """Fetch and parse equipment data."""
        content = await self.fetch_file(GITHUB_URLS["equipment"])
        if not content:
            return None
        
        parsed = self.parse_typescript_export(content)
        if parsed:
            log.info(f"Parsed {len(parsed)} equipment items")
        return parsed
    
    async def fetch_educations(self) -> Optional[Dict[int, Dict[str, Any]]]:
        """Fetch and parse educations data."""
        content = await self.fetch_file(GITHUB_URLS["educations"])
        if not content:
            return None
        
        parsed = self.parse_typescript_export(content)
        if parsed:
            log.info(f"Parsed {len(parsed)} educations")
        return parsed
    
    async def fetch_all(self) -> Dict[str, Optional[Dict[int, Dict[str, Any]]]]:
        """Fetch all data sources."""
        return {
            "vehicles": await self.fetch_vehicles(),
            "buildings": await self.fetch_buildings(),
            "equipment": await self.fetch_equipment(),
            "educations": await self.fetch_educations()
        }
    
    def normalize_vehicle_data(self, game_id: int, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize vehicle data from GitHub format to database format."""
        staff = raw_data.get('staff', {})
        if isinstance(staff, dict):
            min_personnel = staff.get('min')
            max_personnel = staff.get('max')
        else:
            min_personnel = raw_data.get('minPersonnel')
            max_personnel = raw_data.get('maxPersonnel')
        
        return {
            'game_id': game_id,
            'name': raw_data.get('caption', f'Vehicle {game_id}'),
            'min_personnel': min_personnel,
            'max_personnel': max_personnel,
            'price': raw_data.get('credits'),
            'water_tank': raw_data.get('waterTank'),
            'foam_tank': raw_data.get('foamTank'),
            'pump_capacity': raw_data.get('pumpCapacity'),
            'specials': {
                'pumpType': raw_data.get('pumpType'),
                'special': raw_data.get('special'),
                'color': raw_data.get('color'),
                'icon': raw_data.get('icon'),
                'equipmentCapacity': raw_data.get('equipmentCapacity'),
                'possibleBuildings': raw_data.get('possibleBuildings', [])
            }
        }
    
    def normalize_building_data(self, game_id: int, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize building data from GitHub format to database format."""
        return {
            'game_id': game_id,
            'name': raw_data.get('caption', f'Building {game_id}'),
            'caption': raw_data.get('caption')
        }
    
    def normalize_equipment_data(self, game_id: int, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize equipment data from GitHub format to database format."""
        return {
            'game_id': game_id,
            'name': raw_data.get('caption', f'Equipment {game_id}'),
            'size': raw_data.get('size')
        }
    
    def normalize_education_data(self, game_id: int, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize education data from GitHub format to database format."""
        return {
            'game_id': game_id,
            'name': raw_data.get('caption', f'Education {game_id}'),
            'duration': raw_data.get('duration'),
            'cost': raw_data.get('cost')
        }
    
    def detect_changes(self, old_data: List[Dict], new_data: Dict[int, Dict]) -> Dict[str, List]:
        """Detect changes between old and new data."""
        old_ids = {item['game_id'] for item in old_data}
        new_ids = set(new_data.keys())
        
        added = list(new_ids - old_ids)
        removed = list(old_ids - new_ids)
        
        updated = []
        for item in old_data:
            game_id = item['game_id']
            if game_id in new_data:
                if item.get('name') != new_data[game_id].get('caption'):
                    updated.append(game_id)
        
        return {
            'added': added,
            'updated': updated,
            'removed': removed
        }
