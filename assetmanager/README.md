# AssetManager (MissionChief / LSS-Manager ETL)

Pulls canonical asset data from LSS-Manager V4 i18n sources (en_US), converts TS → JSON, validates, and upserts into a local SQLite database `assets.db`.

## Sources (dev branch)
- Buildings:  https://github.com/LSS-Manager/LSSM-V.4/blob/dev/src/i18n/en_US/buildings.ts
- Equipment:  https://github.com/LSS-Manager/LSSM-V.4/blob/dev/src/i18n/en_US/equipment.ts
- Schoolings: https://github.com/LSS-Manager/LSSM-V.4/blob/dev/src/i18n/en_US/schoolings.ts
- Vehicles:   https://github.com/LSS-Manager/LSSM-V.4/blob/dev/src/i18n/en_US/vehicles.ts

## What this does
1. Fetch raw TS files from GitHub (dev branch).
2. Convert `export default { ... }` objects to JSON using a tiny Node script.
3. Validate using JSON Schemas (loose but structured).
4. Diff per entity and upsert into SQLite under a single truth DB: `assets.db`.
5. Build/refresh an FTS5 index for fast lookups.
6. Provide a simple CLI for search with fuzzy ranking.

## Prereqs
- Python 3.10+
- Node 18+ (for TS→JSON conversion)
- SQLite3 with FTS5 (default on modern distros)

## Install
```bash
cd assetmanager
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

# optional but recommended: pin Node via nvm, then install deps (none needed)
