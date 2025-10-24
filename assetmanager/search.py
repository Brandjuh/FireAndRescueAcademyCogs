from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Dual import (cog vs standalone)
try:
    from .config import DB_PATH, FUZZY_MIN_SCORE as _CFG_MIN
except Exception:  # pragma: no cover
    from config import DB_PATH, FUZZY_MIN_SCORE as _CFG_MIN  # type: ignore

# ---- Robust config handling ----
def _to_int(val: Any, default: int) -> int:
    try:
        return int(val)
    except Exception:
        return default

MIN_SCORE: int = _to_int(_CFG_MIN, 60)

# ---- Optional fuzzy engines ----
try:
    from rapidfuzz import fuzz  # type: ignore

    def _score(a: str, b: str) -> float:
        return float(fuzz.WRatio(a, b))
except Exception:  # fallback to stdlib
    from difflib import SequenceMatcher

    def _score(a: str, b: str) -> float:
        return float(SequenceMatcher(None, a, b).ratio() * 100.0)

# ---- DB helpers ----
def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def _try_match_query(con: sqlite3.Connection, q: str, t_filter: Optional[str]) -> Iterable[sqlite3.Row]:
    """
    Prefer FTS MATCH. If that fails (no FTS compiled or table not FTS), fallback to LIKE.
    Correctly handles presence/absence of WHERE when appending conditions.
    """
    base = "SELECT type, ref_id, name, body FROM search_index"
    conds: List[str] = []
    args: List[Any] = []

    if t_filter and t_filter != "any":
        conds.append("type = ?")
        args.append(t_filter)

    # FTS MATCH path
    try:
        match_sql = base
        if conds:
            match_sql += " WHERE " + " AND ".join(conds) + " AND search_index MATCH ?"
        else:
            match_sql += " WHERE search_index MATCH ?"
        cur = con.execute(match_sql, args + [q])
        for row in cur:
            yield row
        return
    except Exception:
        pass  # fall back

    # LIKE fallback
    like_sql = base
    like_args = list(args)
    like_cond = "(name LIKE ? OR body LIKE ?)"
    like_args += [f"%{q}%", f"%{q}%"]
    if conds:
        like_sql += " WHERE " + " AND ".join(conds) + " AND " + like_cond
    else:
        like_sql += " WHERE " + like_cond

    cur = con.execute(like_sql, like_args)
    for row in cur:
        yield row

def _best_excerpt(body: Optional[str], q: str, max_len: int = 160) -> str:
    if not body:
        return ""
    b = body.strip()
    ql = q.lower()
    i = b.lower().find(ql)
    if i < 0:
        return (b[: max_len - 1] + "…") if len(b) > max_len else b
    start = max(0, i - 40)
    end = min(len(b), i + len(q) + 120)
    snippet = b[start:end]
    return ("…" if start > 0 else "") + snippet + ("…" if end < len(b) else "")

# ---- Public API ----
def fuzzy_search(
    query: str,
    type_filter: Optional[str] = None,  # 'vehicle' | 'equipment' | 'schooling' | 'building' | None
    limit: int = 20,
    min_score: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Returns a list of {type, id, name, score, snippet}
    """
    query = (query or "").strip()
    if not query:
        return []

    threshold = _to_int(min_score, MIN_SCORE)

    results: List[Tuple[float, Dict[str, Any]]] = []
    seen: set[Tuple[str, str]] = set()

    with _connect() as con:
        pool = list(_try_match_query(con, query, type_filter))
        pool = pool[:1000]

        for row in pool:
            typ = row["type"]
            rid = str(row["ref_id"])
            name = row["name"] or ""
            body = row["body"] or ""

            key = (typ, rid)
            if key in seen:
                continue
            seen.add(key)

            s_name = _score(query, name)
            s_body = _score(query, body[:512]) if body else 0.0
            score = max(s_name, s_body)

            if score < float(threshold):
                continue

            results.append(
                (
                    score,
                    {
                        "type": typ,
                        "id": rid,
                        "name": name,
                        "score": round(float(score), 2),
                        "snippet": _best_excerpt(body, query),
                    },
                )
            )

    results.sort(key=lambda t: (-t[0], t[1]["name"].lower()))
    return [r for _, r in results[: max(1, int(limit))]]

def get_vehicle(vehicle_id: int | str) -> Optional[Dict[str, Any]]:
    vid = int(vehicle_id)
    with _connect() as con:
        row = con.execute(
            """
            SELECT id, name, min_personnel, max_personnel, price_credits, price_coins,
                   rank_required, water_tank, foam_tank, pump_gpm, speed, specials
            FROM vehicles
            WHERE id = ?
            """,
            (vid,),
        ).fetchone()
        if not row:
            return None

        roles = [
            r["role"]
            for r in con.execute(
                """
                SELECT vr.role
                FROM vehicle_role_map vrm
                JOIN vehicle_roles vr ON vrm.role_id = vr.id
                WHERE vrm.vehicle_id = ?
                """,
                (vid,),
            )
        ]

        buildings = [
            dict(con.execute("SELECT id, name, category FROM buildings WHERE id = ?", (b_id,)).fetchone())
            for (b_id,) in con.execute(
                "SELECT building_id FROM vehicle_possible_buildings WHERE vehicle_id = ?",
                (vid,),
            )
        ]

        schoolings = [
            dict(
                id=sid,
                name=con.execute("SELECT name FROM schoolings WHERE id = ?", (sid,)).fetchone()[0],
                required_count=req,
            )
            for sid, req in con.execute(
                "SELECT schooling_id, required_count FROM vehicle_required_schoolings WHERE vehicle_id = ?",
                (vid,),
            )
        ]

        equipment = [
            dict(con.execute("SELECT id, name FROM equipment WHERE id = ?", (eid,)).fetchone())
            for (eid,) in con.execute(
                "SELECT equipment_id FROM vehicle_equipment_compat WHERE vehicle_id = ? AND compatible = 1",
                (vid,),
            )
        ]

        return {
            **dict(row),
            "roles": roles,
            "possible_buildings": buildings,
            "required_schoolings": schoolings,
            "equipment_compat": equipment,
        }

def get_equipment(equipment_id: int | str) -> Optional[Dict[str, Any]]:
    eid = int(equipment_id)
    with _connect() as con:
        row = con.execute(
            "SELECT id, name, description, size, notes FROM equipment WHERE id = ?", (eid,)
        ).fetchone()
        if not row:
            return None
        return dict(row)

def get_schooling(schooling_id: int | str) -> Optional[Dict[str, Any]]:
    sid = int(schooling_id)
    with _connect() as con:
        row = con.execute(
            "SELECT id, name, department, duration_days FROM schoolings WHERE id = ?", (sid,)
        ).fetchone()
        if not row:
            return None
        return dict(row)

def get_building(building_id: int | str) -> Optional[Dict[str, Any]]:
    bid = int(building_id)
    with _connect() as con:
        row = con.execute(
            "SELECT id, name, category, notes FROM buildings WHERE id = ?", (bid,)
        ).fetchone()
        if not row:
            return None
        return dict(row)
