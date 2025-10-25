import sqlite3
import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime
from pathlib import Path

log = logging.getLogger("red.assetmanager.database")


class AssetDatabase:
    """Handles all SQLite database operations for AssetManager."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        
    def connect(self):
        """Establish database connection."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        log.info(f"Connected to database: {self.db_path}")
        
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            log.info("Database connection closed")
            
    def initialize_tables(self):
        """Create all necessary tables if they don't exist."""
        cursor = self.conn.cursor()
        
        # Vehicles table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vehicles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER UNIQUE NOT NULL,
                name TEXT NOT NULL,
                min_personnel INTEGER,
                max_personnel INTEGER,
                price INTEGER,
                water_tank INTEGER,
                foam_tank INTEGER,
                pump_capacity INTEGER,
                specials TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Buildings table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS buildings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER UNIQUE NOT NULL,
                name TEXT NOT NULL,
                caption TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Equipment table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS equipment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER UNIQUE NOT NULL,
                name TEXT NOT NULL,
                size INTEGER,
                credits INTEGER,
                coins INTEGER,
                min_staff INTEGER,
                max_staff INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Educations table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS educations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER UNIQUE NOT NULL,
                name TEXT NOT NULL,
                duration TEXT,
                cost INTEGER,
                building_type TEXT,
                key TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Relation tables
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vehicle_buildings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vehicle_id INTEGER NOT NULL,
                building_id INTEGER NOT NULL,
                FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE CASCADE,
                FOREIGN KEY (building_id) REFERENCES buildings(id) ON DELETE CASCADE,
                UNIQUE(vehicle_id, building_id)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS vehicle_educations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vehicle_id INTEGER NOT NULL,
                education_id INTEGER NOT NULL,
                FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE CASCADE,
                FOREIGN KEY (education_id) REFERENCES educations(id) ON DELETE CASCADE,
                UNIQUE(vehicle_id, education_id)
            )
        """)
        
        # Sync history table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sync_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source TEXT NOT NULL,
                changes TEXT,
                success BOOLEAN NOT NULL,
                error_message TEXT
            )
        """)
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_vehicles_name ON vehicles(name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_buildings_name ON buildings(name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_equipment_name ON equipment(name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_educations_name ON educations(name)")
        
        self.conn.commit()
        log.info("Database tables initialized successfully")
        
    # ========== VEHICLE OPERATIONS ==========
    
    def insert_vehicle(self, vehicle_data: Dict[str, Any]) -> int:
        """Insert or update a vehicle."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO vehicles 
            (game_id, name, min_personnel, max_personnel, price, water_tank, 
             foam_tank, pump_capacity, specials, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            vehicle_data.get('game_id'),
            vehicle_data.get('name'),
            vehicle_data.get('min_personnel'),
            vehicle_data.get('max_personnel'),
            vehicle_data.get('price'),
            vehicle_data.get('water_tank'),
            vehicle_data.get('foam_tank'),
            vehicle_data.get('pump_capacity'),
            json.dumps(vehicle_data.get('specials', {})),
            datetime.utcnow()
        ))
        
        self.conn.commit()
        return cursor.lastrowid
    
    def get_vehicle_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get vehicle by exact name."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM vehicles WHERE name = ? COLLATE NOCASE", (name,))
        row = cursor.fetchone()
        
        if row:
            return self._row_to_dict(row)
        return None
    
    def get_vehicle_by_id(self, vehicle_id: int) -> Optional[Dict[str, Any]]:
        """Get vehicle by database ID."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM vehicles WHERE id = ?", (vehicle_id,))
        row = cursor.fetchone()
        
        if row:
            return self._row_to_dict(row)
        return None
    
    def search_vehicles(self, query: str) -> List[Dict[str, Any]]:
        """Search vehicles by name (fuzzy search)."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM vehicles WHERE name LIKE ? COLLATE NOCASE ORDER BY name",
            (f"%{query}%",)
        )
        return [self._row_to_dict(row) for row in cursor.fetchall()]
    
    def get_all_vehicles(self) -> List[Dict[str, Any]]:
        """Get all vehicles."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM vehicles ORDER BY name")
        return [self._row_to_dict(row) for row in cursor.fetchall()]
    
    def get_vehicle_buildings(self, vehicle_id: int) -> List[Dict[str, Any]]:
        """Get all buildings associated with a vehicle."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT b.* FROM buildings b
            JOIN vehicle_buildings vb ON b.id = vb.building_id
            WHERE vb.vehicle_id = ?
            ORDER BY b.name
        """, (vehicle_id,))
        return [self._row_to_dict(row) for row in cursor.fetchall()]
    
    def get_vehicle_educations(self, vehicle_id: int) -> List[Dict[str, Any]]:
        """Get all educations required for a vehicle."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT e.* FROM educations e
            JOIN vehicle_educations ve ON e.id = ve.education_id
            WHERE ve.vehicle_id = ?
            ORDER BY e.name
        """, (vehicle_id,))
        return [self._row_to_dict(row) for row in cursor.fetchall()]
    
    def link_vehicle_building(self, vehicle_id: int, building_id: int):
        """Create link between vehicle and building."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO vehicle_buildings (vehicle_id, building_id)
            VALUES (?, ?)
        """, (vehicle_id, building_id))
        self.conn.commit()
    
    def link_vehicle_education(self, vehicle_id: int, education_id: int):
        """Create link between vehicle and education."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO vehicle_educations (vehicle_id, education_id)
            VALUES (?, ?)
        """, (vehicle_id, education_id))
        self.conn.commit()
    
    # ========== BUILDING OPERATIONS ==========
    
    def insert_building(self, building_data: Dict[str, Any]) -> int:
        """Insert or update a building."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO buildings 
            (game_id, name, caption, last_updated)
            VALUES (?, ?, ?, ?)
        """, (
            building_data.get('game_id'),
            building_data.get('name'),
            building_data.get('caption'),
            datetime.utcnow()
        ))
        
        self.conn.commit()
        return cursor.lastrowid
    
    def get_building_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get building by exact name."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM buildings WHERE name = ? COLLATE NOCASE", (name,))
        row = cursor.fetchone()
        
        if row:
            return self._row_to_dict(row)
        return None
    
    def get_all_buildings(self) -> List[Dict[str, Any]]:
        """Get all buildings."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM buildings ORDER BY name")
        return [self._row_to_dict(row) for row in cursor.fetchall()]
    
    # ========== EQUIPMENT OPERATIONS ==========
    
    def insert_equipment(self, equipment_data: Dict[str, Any]) -> int:
        """Insert or update equipment."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO equipment 
            (game_id, name, size, credits, coins, min_staff, max_staff, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            equipment_data.get('game_id'),
            equipment_data.get('name'),
            equipment_data.get('size'),
            equipment_data.get('credits'),
            equipment_data.get('coins'),
            equipment_data.get('min_staff'),
            equipment_data.get('max_staff'),
            datetime.utcnow()
        ))
        
        self.conn.commit()
        return cursor.lastrowid
    
    def get_all_equipment(self) -> List[Dict[str, Any]]:
        """Get all equipment."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM equipment ORDER BY name")
        return [self._row_to_dict(row) for row in cursor.fetchall()]
    
    # ========== EDUCATION OPERATIONS ==========
    
    def insert_education(self, education_data: Dict[str, Any]) -> int:
        """Insert or update education."""
        cursor = self.conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO educations 
            (game_id, name, duration, cost, building_type, key, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            education_data.get('game_id'),
            education_data.get('name'),
            education_data.get('duration'),
            education_data.get('cost'),
            education_data.get('building_type'),
            education_data.get('key'),
            datetime.utcnow()
        ))
        
        self.conn.commit()
        return cursor.lastrowid
    
    def get_all_educations(self) -> List[Dict[str, Any]]:
        """Get all educations."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM educations ORDER BY name")
        return [self._row_to_dict(row) for row in cursor.fetchall()]
    
    # ========== SYNC HISTORY OPERATIONS ==========
    
    def log_sync(self, source: str, changes: Dict[str, Any], success: bool, error_message: str = None):
        """Log a sync operation."""
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO sync_history (sync_date, source, changes, success, error_message)
            VALUES (?, ?, ?, ?, ?)
        """, (
            datetime.utcnow(),
            source,
            json.dumps(changes),
            success,
            error_message
        ))
        self.conn.commit()
    
    def get_last_sync(self) -> Optional[Dict[str, Any]]:
        """Get the last successful sync."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM sync_history 
            WHERE success = 1 
            ORDER BY sync_date DESC 
            LIMIT 1
        """)
        row = cursor.fetchone()
        
        if row:
            return self._row_to_dict(row)
        return None
    
    # ========== UTILITY METHODS ==========
    
    def _row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        """Convert SQLite row to dictionary."""
        data = dict(row)
        
        # Parse JSON fields
        if 'specials' in data and data['specials']:
            try:
                data['specials'] = json.loads(data['specials'])
            except json.JSONDecodeError:
                data['specials'] = {}
        
        if 'changes' in data and data['changes']:
            try:
                data['changes'] = json.loads(data['changes'])
            except json.JSONDecodeError:
                data['changes'] = {}
        
        return data
    
    def clear_all_relations(self, vehicle_id: int):
        """Clear all relations for a vehicle (used before re-linking)."""
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM vehicle_buildings WHERE vehicle_id = ?", (vehicle_id,))
        cursor.execute("DELETE FROM vehicle_educations WHERE vehicle_id = ?", (vehicle_id,))
        self.conn.commit()
