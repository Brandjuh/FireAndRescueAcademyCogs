from __future__ import annotations

import sqlite3
from typing import Iterable, List, Tuple

from rapidfuzz import fuzz, process

# ---- robust dual-mode import for cogs vs. standalone runs ----
try:
    # running as package (inside Red cog)
    from .config import DB_PATH, FUZZY_MIN_SCORE
except Exception:
    # fallback when executed directly: `python assetmanager/search.py`
    from config import DB_PATH, FUZZY_MIN_SCORE  # type: ignore


def _fetch_all(con: sqlite3.Connection, q: str, args: Iterable):
    cur = con.execute(q, args)
    cols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        yield dict(zip(cols, row))


def fts_search(q: str, type_filter: str | None = None, limit: int = 25):
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        if type_filter:
            rows = list(_fetch_all(
                con,
                """
                SELECT type, ref_id, name, body
                FROM search_index
                WHERE search_index MATCH ?
                  AND type = ?
                LIMIT ?
                """,
                (q, type_filter, limit),
            ))
        else:
            rows = list(_fetch_all(
                con,
                """
                SELECT type, ref_id, name, body
                FROM search_index
                WHERE search_index MATCH ?
                LIMIT ?
                """,
                (q, limit),
            ))
    return rows


def fuzzy_search(query: str, type_filter: str | None = None, limit: int = 20):
    # Haal eerst een pool op (met of zonder type-filter), rank daarna met fuzzy.
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        if type_filter:
            pool = list(_fetch_all(
                con,
                """
                SELECT type, ref_id, name, body
                FROM search_index
                WHERE type = ?
                """,
                (type_filter,),
            ))
        else:
            pool = list(_fetch_all(
                con,
                """
                SELECT type, ref_id, name, body
                FROM search_index
                """,
                (),
            ))

    choices = {f'{r["type"]}:{r["ref_id"]}': r["name"] for r in pool}
    if not choices:
        return []

    scored = process.extract(query, choices, scorer=fuzz.WRatio, limit=None)
    out = []
    for (key, _name, score) in scored:
        if score < FUZZY_MIN_SCORE:
            continue
        typ, ref_id = key.split(":", 1)
        rec = next((r for r in pool if r["type"] == typ and r["ref_id"] == ref_id), None)
        if rec:
            out.append({"type": typ, "ref_id": ref_id, "name": rec["name"], "score": score})
        if len(out) >= limit:
            break
    return out


def get_vehicle(con: sqlite3.Connection, vid: int):
    v = _fetch_all(con, "SELECT * FROM vehicles WHERE id=?", (vid,))
    v = next(v, None)
    if not v:
        return None

    buildings = list(_fetch_all(
        con,
        """
        SELECT b.id, b.name
        FROM vehicle_possible_buildings pb
        JOIN buildings b ON b.id = pb.building_id
        WHERE pb.vehicle_id=? ORDER BY b.id
        """,
        (vid,),
    ))

    schoolings = list(_fetch_all(
        con,
        """
        SELECT s.id, s.name, vrs.required_count
        FROM vehicle_required_schoolings vrs
        JOIN schoolings s ON s.id = vrs.schooling_id
        WHERE vrs.vehicle_id=? ORDER BY s.id
        """,
        (vid,),
    ))

    equipment = list(_fetch_all(
        con,
        """
        SELECT e.id, e.name
        FROM vehicle_equipment_compat vec
        JOIN equipment e ON e.id = vec.equipment_id
        WHERE vec.vehicle_id=? AND vec.compatible=1 ORDER BY e.id
        """,
        (vid,),
    ))

    roles = list(_fetch_all(
        con,
        """
        SELECT r.role
        FROM vehicle_role_map vrm
        JOIN vehicle_roles r ON r.id = vrm.role_id
        WHERE vrm.vehicle_id=?
        ORDER BY r.role
        """,
        (vid,),
    ))

    v["possible_buildings"] = buildings
    v["required_schoolings"] = schoolings
    v["equipment_compat"] = equipment
    v["roles"] = [r["role"] for r in roles]
    return v
