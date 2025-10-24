from __future__ import annotations

import pathlib

# SQLite database file
DB_PATH = pathlib.Path(__file__).parent / "assets.db"

# Raw URLs on dev branch (we fetch the raw TS)
RAW_BASE = "https://raw.githubusercontent.com/LSS-Manager/LSSM-V.4/dev/src/i18n/en_US"
SRC_FILES = {
    "buildings":  f"{RAW_BASE}/buildings.ts",
    "equipment":  f"{RAW_BASE}/equipment.ts",
    "schoolings": f"{RAW_BASE}/schoolings.ts",
    "vehicles":   f"{RAW_BASE}/vehicles.ts",
}

# Local temp folder for fetched sources and JSON dumps
DATA_DIR = pathlib.Path(__file__).parent / "_data"
DATA_DIR.mkdir(exist_ok=True)

# JSON Schemas
SCHEMA_DIR = pathlib.Path(__file__).parent / "schemas"

# Node converter
NODE_BIN = "node"
TS_TO_JSON = pathlib.Path(__file__).parent / "ts_to_json.mjs"

# Simple search knobs
FUZZY_MIN_SCORE = 55
