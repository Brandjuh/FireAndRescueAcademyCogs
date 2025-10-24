from pathlib import Path

BASE_DIR = Path(__file__).parent  # .../assetmanager
DB_PATH = BASE_DIR / "assets.db"

# Directory that will contain JSON schemas
SCHEMA_DIR = BASE_DIR / "schemas"

# Node binary (absolute path, because services love to forget PATH)
NODE_BIN = "/usr/bin/node"

# Source TS files we fetch
SRC_FILES = {
    "buildings": "https://raw.githubusercontent.com/LSS-Manager/LSSM-V.4/dev/src/i18n/en_US/buildings.ts",
    "equipment": "https://raw.githubusercontent.com/LSS-Manager/LSSM-V.4/dev/src/i18n/en_US/equipment.ts",
    "schoolings": "https://raw.githubusercontent.com/LSS-Manager/LSSM-V.4/dev/src/i18n/en_US/schoolings.ts",
    "vehicles":  "https://raw.githubusercontent.com/LSS-Manager/LSSM-V.4/dev/src/i18n/en_US/vehicles.ts",
}

# Fuzzy threshold
FUZZY_MIN_SCORE = 60
