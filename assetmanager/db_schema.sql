PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS source_commits (
  id INTEGER PRIMARY KEY,
  repo TEXT NOT NULL,
  path TEXT NOT NULL,
  commit_sha TEXT NOT NULL,
  fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS buildings (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS schoolings (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  duration_days INTEGER,
  department TEXT
);

CREATE TABLE IF NOT EXISTS equipment (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  size TEXT,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS vehicles (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  min_personnel INTEGER DEFAULT 0 NOT NULL,
  max_personnel INTEGER DEFAULT 0 NOT NULL,
  price_credits INTEGER,
  price_coins INTEGER,
  rank_required TEXT,
  water_tank INTEGER,
  foam_tank INTEGER,
  pump_gpm INTEGER,
  speed INTEGER,
  specials TEXT
);

-- Normalized relations
CREATE TABLE IF NOT EXISTS vehicle_possible_buildings (
  vehicle_id INTEGER NOT NULL REFERENCES vehicles(id) ON DELETE CASCADE,
  building_id INTEGER NOT NULL REFERENCES buildings(id) ON DELETE CASCADE,
  PRIMARY KEY (vehicle_id, building_id)
);

CREATE TABLE IF NOT EXISTS vehicle_required_schoolings (
  vehicle_id INTEGER NOT NULL REFERENCES vehicles(id) ON DELETE CASCADE,
  schooling_id INTEGER NOT NULL REFERENCES schoolings(id) ON DELETE CASCADE,
  required_count INTEGER DEFAULT 1 NOT NULL,
  PRIMARY KEY (vehicle_id, schooling_id)
);

CREATE TABLE IF NOT EXISTS vehicle_equipment_compat (
  vehicle_id INTEGER NOT NULL REFERENCES vehicles(id) ON DELETE CASCADE,
  equipment_id INTEGER NOT NULL REFERENCES equipment(id) ON DELETE CASCADE,
  compatible INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (vehicle_id, equipment_id)
);

-- Roles for multi-role vehicles (e.g., "Fire Truck", "Platform Truck", "Heavy Rescue")
CREATE TABLE IF NOT EXISTS vehicle_roles (
  id INTEGER PRIMARY KEY,
  role TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS vehicle_role_map (
  vehicle_id INTEGER NOT NULL REFERENCES vehicles(id) ON DELETE CASCADE,
  role_id INTEGER NOT NULL REFERENCES vehicle_roles(id) ON DELETE CASCADE,
  PRIMARY KEY (vehicle_id, role_id)
);

-- FTS index for fuzzy-ish matching (we also use rapidfuzz for scoring)
CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
  type,       -- 'vehicle' | 'equipment' | 'schooling' | 'building'
  ref_id,     -- integer id as text
  name,
  body
);
