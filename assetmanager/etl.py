from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

# --- optional stdlib fallback if 'requests' is missing during first import ---
try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    import urllib.request as _ul

    class _Resp:
        def __init__(self, data: bytes, headers: dict[str, str]):
            self._data = data
            self.headers = headers

        def raise_for_status(self):
            return

        @property
        def text(self) -> str:
            return self._data.decode("utf-8", errors="replace")

    class requests:  # tiny shim with get()
        @staticmethod
        def get(url: str, timeout: int = 30, headers: dict[str, str] | None = None):
            req = _ul.Request(url, headers=headers or {})
            with _ul.urlopen(req, timeout=timeout) as r:
                data = r.read()
                hdrs = {k.lower(): v for k, v in r.headers.items()}
                return _Resp(data, hdrs)

from jsonschema import validate, ValidationError

# ---- robust dual-mode import for cogs vs. standalone runs ----
try:
    # running as package (inside Red cog)
    from .config import DB_PATH, DATA_DIR, SRC_FILES, TS_TO_JSON, NODE_BIN, SCHEMA_DIR
except Exception:
    # fallback when executed directly: `python assetmanager/etl.py`
    from config import DB_PATH, DATA_DIR, SRC_FILES, TS_TO_JSON, NODE_BIN, SCHEMA_DIR  # type: ignore

# --------------- Utilities ---------------

UA = "FARA-AssetManager/1.0 (+https://missionchief.local)"

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as con:
        con.executescript(Path(__file__).with_name("db_schema.sql").read_text(encoding="utf-8"))
        con.commit()

def _which_node() -> str:
    node = shutil.which(str(NODE_BIN)) if NODE_BIN else None
    if not node:
        raise RuntimeError(
            "Node.js niet gevonden. Installeer Node 18+ en zorg dat 'node' in PATH staat, "
            "of pas NODE_BIN aan in config.py."
        )
    return node

def fetch_raw(url: str, out_path: Path, *, retries: int = 3, backoff: float = 1.5) -> str:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=30, headers={"User-Agent": UA, "Accept": "text/plain"})
            r.raise_for_status()
            out_path.write_text(r.text, encoding="utf-8")
            headers = {k.lower(): v for k, v in (getattr(r, "headers", {}) or {}).items()}
            commit = (
                headers.get("x-github-request-id")
                or headers.get("etag")
                or headers.get("x-cache")
                or "dev"
            )
            return str(commit)
        except Exception as e:
            last_err = e
            if attempt >= retries:
                break
            time.sleep(backoff ** attempt)
    raise RuntimeError(f"Download mislukt voor {url}: {last_err}")

def ts_to_json(ts_path: Path, json_path: Path) -> None:
    node = _which_node()
    res = subprocess.run([node, str(TS_TO_JSON), str(ts_path)], capture_output=True, text=True)
    if res.returncode != 0:
        snippet = (res.stderr or res.stdout or "").strip()
        if len(snippet) > 600:
            snippet = snippet[:600] + " …"
        raise RuntimeError(
            f"ts_to_json faalde voor {ts_path.name}. Controleer ts_to_json.mjs en Node-installatie. "
            f"Details: {snippet}"
        )
    json_path.write_text(res.stdout, encoding="utf-8")

def load_and_validate(json_path: Path, schema_name: str) -> Dict[str, Any]:
    obj = json.loads(json_path.read_text(encoding="utf-8"))
    schema = json.loads((SCHEMA_DIR / f"{schema_name}.json").read_text(encoding="utf-8"))
    try:
        validate(obj, schema)
    except ValidationError as e:
        raise RuntimeError(f"Validatie faalde voor {schema_name}: {e.message}")
    return obj

# --------------- Mapping / Transform ---------------

def norm_int(x: Any) -> int | None:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None

def collect_text(*vals: Any) -> str | None:
    s = " | ".join(str(v) for v in vals if v not in (None, "", 0))
    return s or None

def transform_buildings(src: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    out = {}
    for k, v in src.items():
        try:
            bid = int(k)
        except Exception:
            continue
        name = v.get("name") or v.get("caption") or f"Building {bid}"
        category = v.get("category")
        notes = v.get("notes")
        out[bid] = {
            "id": bid,
            "name": name,
            "category": category,
            "notes": notes,
        }
    return out

def transform_schoolings(src: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    out = {}
    for k, v in src.items():
        try:
            sid = int(k)
        except Exception:
            continue
        name = v.get("name") or f"Schooling {sid}"
        department = v.get("department") or v.get("dept")
        duration = norm_int(v.get("duration_days") or v.get("duration") or v.get("days"))
        out[sid] = {
            "id": sid,
            "name": name,
            "department": department,
            "duration_days": duration,
        }
    return out

def transform_equipment(src: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    out = {}
    for k, v in src.items():
        try:
            eid = int(k)
        except Exception:
            continue
        name = v.get("name") or f"Equipment {eid}"
        description = v.get("description") or v.get("desc")
        size = str(v.get("size")) if v.get("size") is not None else None
        notes = v.get("notes")
        out[eid] = {
            "id": eid,
            "name": name,
            "description": description,
            "size": size,
            "notes": notes,
        }
    return out

def transform_vehicles(src: Dict[str, Any]) -> Tuple[
    Dict[int, Dict[str, Any]],
    Dict[int, set],
    Dict[int, list],
    Dict[int, set],
    Dict[str, int],
    Dict[int, set]
]:
    vehicles = {}
    vp_buildings = {}
    v_schoolings = {}
    v_equipment = {}
    role_names = set()
    v_roles = {}

    for k, v in src.items():
        try:
            vid = int(k)
        except Exception:
            continue

        name = v.get("name") or f"Vehicle {vid}"
        min_p = norm_int(v.get("min_personnel") or v.get("minPersonnel") or v.get("min_crew"))
        max_p = norm_int(v.get("max_personnel") or v.get("maxPersonnel") or v.get("max_crew"))
        price_credits = norm_int(v.get("price_credits") or v.get("credits") or v.get("price"))
        price_coins = norm_int(v.get("price_coins") or v.get("coins"))
        rank_required = v.get("rank_required") or v.get("rank")
        water_tank = norm_int(v.get("water_tank") or v.get("waterTank"))
        foam_tank = norm_int(v.get("foam_tank") or v.get("foamTank"))
        pump_gpm = norm_int(v.get("pump_gpm") or v.get("gpm") or v.get("pump"))
        speed = norm_int(v.get("speed"))
        specials = collect_text(v.get("specials"), v.get("acts_as"), v.get("notes"))

        vehicles[vid] = {
            "id": vid,
            "name": name,
            "min_personnel": min_p or 0,
            "max_personnel": max_p or 0,
            "price_credits": price_credits,
            "price_coins": price_coins,
            "rank_required": rank_required,
            "water_tank": water_tank,
            "foam_tank": foam_tank,
            "pump_gpm": pump_gpm,
            "speed": speed,
            "specials": specials,
        }

        pbs = v.get("possible_buildings") or v.get("possibleBuildings") or v.get("building_ids")
        if isinstance(pbs, list):
            vp_buildings[vid] = set(int(x) for x in pbs if isinstance(x, (int, str)) and str(x).isdigit())

        reqs = v.get("required_schoolings") or v.get("schoolings") or []
        arr = []
        if isinstance(reqs, list):
            for it in reqs:
                if not isinstance(it, dict):
                    continue
                sid = it.get("schooling_id") or it.get("id")
                if sid is None:
                    continue
                cnt = it.get("count") or 1
                try:
                    arr.append({"schooling_id": int(sid), "count": int(cnt)})
                except Exception:
                    pass
        if arr:
            v_schoolings[vid] = arr

        ec = v.get("equipment_compat") or v.get("equipment") or []
        if isinstance(ec, list):
            v_equipment[vid] = set(int(x) for x in ec if isinstance(x, (int, str)) and str(x).isdigit())

        roles = v.get("roles") or v.get("acts_as") or v.get("role")
        role_set = set()
        if isinstance(roles, list):
            role_set = set(str(r).strip() for r in roles if r)
        elif isinstance(roles, str):
            parts = re.split(r"[+,/]|and", roles, flags=re.I)
            role_set = set(p.strip() for p in parts if p.strip())
        if role_set:
            v_roles[vid] = role_set
            role_names.update(role_set)

    role_index = {name: 0 for name in sorted(role_names)}
    return vehicles, vp_buildings, v_schoolings, v_equipment, role_index, v_roles

# --------------- Upserts ---------------

def upsert_buildings(con: sqlite3.Connection, rows: Dict[int, Dict[str, Any]]):
    for r in rows.values():
        con.execute("""
            INSERT INTO buildings (id, name, category, notes)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              name=excluded.name,
              category=excluded.category,
              notes=excluded.notes
        """, (r["id"], r["name"], r["category"], r["notes"]))

def upsert_schoolings(con: sqlite3.Connection, rows: Dict[int, Dict[str, Any]]):
    for r in rows.values():
        con.execute("""
            INSERT INTO schoolings (id, name, duration_days, department)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              name=excluded.name,
              duration_days=excluded.duration_days,
              department=excluded.department
        """, (r["id"], r["name"], r["duration_days"], r["department"]))

def upsert_equipment(con: sqlite3.Connection, rows: Dict[int, Dict[str, Any]]):
    for r in rows.values():
        con.execute("""
            INSERT INTO equipment (id, name, description, size, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              name=excluded.name,
              description=excluded.description,
              size=excluded.size,
              notes=excluded.notes
        """, (r["id"], r["name"], r["description"], r["size"], r["notes"]))

def upsert_vehicles(con: sqlite3.Connection, rows: Dict[int, Dict[str, Any]]):
    for r in rows.values():
        con.execute("""
            INSERT INTO vehicles (
              id, name, min_personnel, max_personnel, price_credits, price_coins,
              rank_required, water_tank, foam_tank, pump_gpm, speed, specials
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              name=excluded.name,
              min_personnel=excluded.min_personnel,
              max_personnel=excluded.max_personnel,
              price_credits=excluded.price_credits,
              price_coins=excluded.price_coins,
              rank_required=excluded.rank_required,
              water_tank=excluded.water_tank,
              foam_tank=excluded.foam_tank,
              pump_gpm=excluded.pump_gpm,
              speed=excluded.speed,
              specials=excluded.specials
        """, (
            r["id"], r["name"], r["min_personnel"], r["max_personnel"],
            r["price_credits"], r["price_coins"], r["rank_required"],
            r["water_tank"], r["foam_tank"], r["pump_gpm"], r["speed"], r["specials"]
        ))

def replace_relations(con: sqlite3.Connection,
                      vp_buildings, v_schoolings, v_equipment,
                      role_index, v_roles):
    con.execute("DELETE FROM vehicle_possible_buildings")
    for vid, bids in vp_buildings.items():
        for b in bids:
            con.execute("""
                INSERT OR IGNORE INTO vehicle_possible_buildings (vehicle_id, building_id)
                VALUES (?, ?)
            """, (vid, b))

    con.execute("DELETE FROM vehicle_required_schoolings")
    for vid, arr in v_schoolings.items():
        for it in arr:
            con.execute("""
                INSERT OR REPLACE INTO vehicle_required_schoolings (vehicle_id, schooling_id, required_count)
                VALUES (?, ?, ?)
            """, (vid, it["schooling_id"], it.get("count", 1)))

    con.execute("DELETE FROM vehicle_equipment_compat")
    for vid, eids in v_equipment.items():
        for eid in eids:
            con.execute("""
                INSERT OR REPLACE INTO vehicle_equipment_compat (vehicle_id, equipment_id, compatible)
                VALUES (?, ?, 1)
            """, (vid, eid))

    role_id_map = {}
    for role_name in role_index.keys():
        con.execute("INSERT INTO vehicle_roles (role) VALUES (?) ON CONFLICT(role) DO NOTHING", (role_name,))
        cur = con.execute("SELECT id FROM vehicle_roles WHERE role=?", (role_name,))
        rid = cur.fetchone()[0]
        role_id_map[role_name] = rid

    con.execute("DELETE FROM vehicle_role_map")
    for vid, roles in v_roles.items():
        for role_name in roles:
            rid = role_id_map.get(role_name)
            if rid:
                con.execute("""
                    INSERT OR IGNORE INTO vehicle_role_map (vehicle_id, role_id)
                    VALUES (?, ?)
                """, (vid, rid))

def rebuild_fts(con: sqlite3.Connection):
    con.execute("DELETE FROM search_index")
    for row in con.execute("SELECT id, name, specials FROM vehicles"):
        body = row[2] or ""
        con.execute("INSERT INTO search_index (type, ref_id, name, body) VALUES ('vehicle', ?, ?, ?)",
                    (str(row[0]), row[1], body))
    for row in con.execute("SELECT id, name, description FROM equipment"):
        con.execute("INSERT INTO search_index (type, ref_id, name, body) VALUES ('equipment', ?, ?, ?)",
                    (str(row[0]), row[1], row[2] or ""))
    for row in con.execute("SELECT id, name, department FROM schoolings"):
        con.execute("INSERT INTO search_index (type, ref_id, name, body) VALUES ('schooling', ?, ?, ?)",
                    (str(row[0]), row[1], row[2] or ""))
    for row in con.execute("SELECT id, name, category FROM buildings"):
        con.execute("INSERT INTO search_index (type, ref_id, name, body) VALUES ('building', ?, ?, ?)",
                    (str(row[0]), row[1], row[2] or ""))

# --------------- Main ETL ---------------

def run_etl():
    ensure_db()
    tmpdir = Path(tempfile.mkdtemp(prefix="am_ts_"))

    try:
        results = {}
        for key, url in SRC_FILES.items():
            ts_path = tmpdir / f"{key}.ts"}
            commit_sha = fetch_raw(url, ts_path)
            json_path = tmpdir / f"{key}.json"
            ts_to_json(ts_path, json_path)
            obj = load_and_validate(json_path, key)
            results[key] = (obj, commit_sha)

        buildings = transform_buildings(results["buildings"][0])
        schoolings = transform_schoolings(results["schoolings"][0])
        equipment = transform_equipment(results["equipment"][0])
        vehicles, vp_buildings, v_schoolings, v_equipment, role_index, v_roles = transform_vehicles(results["vehicles"][0])

        with sqlite3.connect(DB_PATH) as con:
            con.execute("PRAGMA foreign_keys=ON")
            cur = con.cursor()
            upsert_buildings(con, buildings)
            upsert_schoolings(con, schoolings)
            upsert_equipment(con, equipment)
            upsert_vehicles(con, vehicles)
            replace_relations(con, vp_buildings, v_schoolings, v_equipment, role_index, v_roles)

            fetched_at = utcnow_iso()
            for key in ("buildings", "equipment", "schoolings", "vehicles"):
                cur.execute("""
                    INSERT INTO source_commits (repo, path, commit_sha, fetched_at)
                    VALUES (?, ?, ?, ?)
                """, ("LSSM-V.4", f"en_US/{key}.ts", results[key][1], fetched_at))

            rebuild_fts(con)
            con.commit()

        print(f"[OK] ETL finished. {len(vehicles)} vehicles, {len(equipment)} equipment, {len(schoolings)} schoolings, {len(buildings)} buildings.")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == "__main__":
    run_etl()
