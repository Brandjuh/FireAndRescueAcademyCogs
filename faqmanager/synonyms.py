"""
Synonym Management for FAQ Search
Handles synonym expansion for better search results.
"""

import json
from typing import Dict, List, Set
from pathlib import Path


# Default synonym dictionary (from provided JSON)
DEFAULT_SYNONYMS: Dict[str, List[str]] = {
    "arr": [
        "alarm and response regulation",
        "alarm & response regulation",
        "alarm and response rules",
        "alarm response",
        "arr rules",
        "a&r",
        "arr setup",
        "alarm setup",
        "alarm rule"
    ],
    "poi": [
        "points of interest",
        "point of interest",
        "location marker",
        "mission spawn point",
        "spawn area",
        "poi marker"
    ],
    "credits": [
        "money",
        "cash",
        "income",
        "reward",
        "mission payout",
        "earnings",
        "credit",
        "payout"
    ],
    "tax": [
        "alliance tax",
        "donation",
        "contribution",
        "percentage",
        "alliance fee",
        "member tax"
    ],
    "training": [
        "education",
        "course",
        "schooling",
        "class",
        "academy",
        "classroom",
        "trainings"
    ],
    "expansion": [
        "building expansion",
        "building extension",
        "extra space",
        "station expansion",
        "expansions"
    ],
    "dispatch": [
        "control center",
        "dispatch center",
        "alarm center",
        "command center",
        "control building",
        "dispatch building"
    ],
    "vehicle": [
        "truck",
        "engine",
        "unit",
        "apparatus",
        "car",
        "van",
        "vehicle type",
        "vehicles"
    ],
    "station": [
        "building",
        "fire station",
        "ems station",
        "police station",
        "prison",
        "medical building",
        "facility"
    ],
    "missions": [
        "calls",
        "incidents",
        "jobs",
        "tasks",
        "mission list",
        "mission type"
    ],
    "shared missions": [
        "alliance mission",
        "cooperative mission",
        "shared call",
        "joint mission",
        "shared callout",
        "shared event"
    ],
    "credits boost": [
        "double credits",
        "2x event",
        "2x credits",
        "credit boost",
        "bonus event",
        "special event"
    ],
    "rank": [
        "promotion",
        "levels",
        "roles",
        "ranking",
        "player level",
        "rank up"
    ],
    "staff": [
        "employees",
        "personnel",
        "workers",
        "crew",
        "hiring",
        "recruitment"
    ],
    "school": [
        "training center",
        "academy",
        "education building",
        "fire academy",
        "police academy",
        "ems school"
    ],
    "mission requirement": [
        "needed vehicles",
        "vehicle requirement",
        "minimum vehicles",
        "mission details",
        "needed units"
    ],
    "building cost": [
        "price",
        "build price",
        "construction cost",
        "costs",
        "buy building"
    ],
    "fuel": [
        "gas station",
        "fuel depot",
        "petrol station",
        "fuel base"
    ],
    "prisoner transport": [
        "prison transport",
        "police transport",
        "jail transport",
        "transporting prisoners",
        "transfer prisoner"
    ],
    "ems": [
        "ambulance",
        "medical",
        "paramedic",
        "medic unit",
        "emergency medical service",
        "rescue medic"
    ],
    "fire": [
        "fire department",
        "firefighting",
        "fire engine",
        "fire station",
        "firehouse",
        "fd"
    ],
    "police": [
        "law enforcement",
        "cop",
        "officer",
        "pd",
        "police car",
        "police department"
    ],
    "wildland": [
        "forest fire",
        "brush fire",
        "wildfire",
        "wildland unit",
        "wildland firefighting"
    ],
    "airport": [
        "airfield",
        "runway",
        "aircraft",
        "plane",
        "hangar",
        "aviation",
        "arff"
    ],
    "quint": [
        "quints",
        "combination truck",
        "ladder pump",
        "engine ladder",
        "platform pumper",
        "quint engine"
    ],
    "rescue engine": [
        "combination rescue",
        "rescue pumper",
        "engine rescue",
        "rescue truck",
        "heavy rescue with pump"
    ],
    "mobile command": [
        "command post",
        "mobile headquarters",
        "command unit",
        "incident command",
        "mobile command center"
    ],
    "hazmat": [
        "hazardous materials",
        "chemical incident",
        "decontamination",
        "hazmat unit",
        "hazmat truck"
    ],
    "sar": [
        "search and rescue",
        "coastal rescue",
        "sea rescue",
        "lifeguard",
        "boat rescue",
        "rescue swimmer"
    ],
    "k9": [
        "police dog",
        "canine unit",
        "dog unit",
        "k-9"
    ],
    "swat": [
        "special weapons and tactics",
        "tactical unit",
        "swat team",
        "swat suv",
        "tactical response"
    ],
    "riot": [
        "riot police",
        "crowd control",
        "riot unit",
        "riot team",
        "riot training"
    ],
    "air unit": [
        "helicopter",
        "air rescue",
        "police helicopter",
        "ems helicopter",
        "air ambulance"
    ],
    "trailer": [
        "utility trailer",
        "special trailer",
        "boat trailer",
        "hazmat trailer"
    ],
    "event": [
        "seasonal event",
        "special event",
        "holiday event",
        "limited time event",
        "event missions"
    ],
    "map": [
        "location",
        "world map",
        "game map",
        "map view",
        "area view"
    ],
    "coastal": [
        "beach",
        "ocean",
        "sea",
        "lifeguard",
        "coastguard"
    ]
}


class SynonymManager:
    """
    Manages synonym expansion for search queries.
    Supports both default synonyms and custom per-guild additions.
    """
    
    def __init__(self):
        self.synonyms: Dict[str, List[str]] = DEFAULT_SYNONYMS.copy()
        self._reverse_map: Dict[str, str] = {}  # Maps synonym -> key
        self._build_reverse_map()
    
    def _build_reverse_map(self):
        """Build a reverse lookup map from synonyms to their keys."""
        self._reverse_map.clear()
        for key, synonyms in self.synonyms.items():
            # Add the key itself
            self._reverse_map[key.lower()] = key
            # Add all synonyms
            for synonym in synonyms:
                self._reverse_map[synonym.lower()] = key
    
    def expand_query(self, query: str) -> Set[str]:
        """
        Expand a query with synonyms.
        
        Args:
            query: Original search query
            
        Returns:
            Set of query variations including original and synonyms
        """
        expanded = {query.lower()}
        query_lower = query.lower()
        
        # Check if query matches any key or synonym
        if query_lower in self._reverse_map:
            key = self._reverse_map[query_lower]
            # Add all synonyms for this key
            expanded.update(syn.lower() for syn in self.synonyms[key])
            expanded.add(key.lower())
        
        # Also check for partial matches in multi-word queries
        words = query_lower.split()
        for word in words:
            if word in self._reverse_map:
                key = self._reverse_map[word]
                expanded.update(syn.lower() for syn in self.synonyms[key])
                expanded.add(key.lower())
        
        return expanded
    
    def add_synonym(self, key: str, synonym: str):
        """
        Add a custom synonym to the dictionary.
        
        Args:
            key: Synonym group key
            synonym: New synonym to add
        """
        key_lower = key.lower()
        if key_lower not in self.synonyms:
            self.synonyms[key_lower] = []
        
        if synonym.lower() not in [s.lower() for s in self.synonyms[key_lower]]:
            self.synonyms[key_lower].append(synonym.lower())
            self._reverse_map[synonym.lower()] = key_lower
    
    def remove_synonym(self, key: str, synonym: str) -> bool:
        """
        Remove a synonym from a group.
        
        Args:
            key: Synonym group key
            synonym: Synonym to remove
            
        Returns:
            True if removed, False if not found
        """
        key_lower = key.lower()
        if key_lower not in self.synonyms:
            return False
        
        original_count = len(self.synonyms[key_lower])
        self.synonyms[key_lower] = [
            s for s in self.synonyms[key_lower]
            if s.lower() != synonym.lower()
        ]
        
        if len(self.synonyms[key_lower]) < original_count:
            # Rebuild reverse map
            self._build_reverse_map()
            return True
        return False
    
    def get_all_synonyms(self) -> Dict[str, List[str]]:
        """Get a copy of all synonyms."""
        return self.synonyms.copy()
    
    def load_from_json(self, json_path: Path):
        """
        Load synonyms from a JSON file.
        
        Args:
            json_path: Path to JSON file with synonym dictionary
        """
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.synonyms.update(data)
                self._build_reverse_map()
        except Exception as e:
            raise ValueError(f"Failed to load synonyms from {json_path}: {e}")
    
    def save_to_json(self, json_path: Path):
        """
        Save current synonyms to a JSON file.
        
        Args:
            json_path: Path to save JSON file
        """
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(self.synonyms, f, indent=2, ensure_ascii=False)
        except Exception as e:
            raise ValueError(f"Failed to save synonyms to {json_path}: {e}")


# Test cases for development
def _test_synonym_expansion():
    """Test synonym expansion functionality."""
    manager = SynonymManager()
    
    # Test 1: Basic expansion
    result = manager.expand_query("arr")
    assert "alarm and response regulation" in result
    assert "arr rules" in result
    print(f"✓ Test 1 passed: 'arr' expanded to {len(result)} variations")
    
    # Test 2: Multi-word query
    result = manager.expand_query("how to set up arr")
    assert "arr" in result or "alarm and response regulation" in result
    print(f"✓ Test 2 passed: Multi-word query expanded")
    
    # Test 3: Credits synonym
    result = manager.expand_query("money")
    assert "credits" in result
    assert "income" in result
    print(f"✓ Test 3 passed: 'money' linked to credits group")
    
    # Test 4: Case insensitivity
    result1 = manager.expand_query("POI")
    result2 = manager.expand_query("poi")
    assert result1 == result2
    print(f"✓ Test 4 passed: Case insensitive matching")
    
    # Test 5: Add custom synonym
    manager.add_synonym("arr", "alarm system")
    result = manager.expand_query("arr")
    assert "alarm system" in result
    print(f"✓ Test 5 passed: Custom synonym added")
    
    print("\n✅ All synonym tests passed!")


if __name__ == "__main__":
    _test_synonym_expansion()
