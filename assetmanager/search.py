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
        # WRatio is tolerant and strong for short queries
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
    """
    base = "SELECT type, ref_id, name, body FROM search_index"
    args: List[Any] = []
    if t_filter and t_filter != "any":
        # Restrict type in outer WHERE to allow FTS index usage when present
        where_type = " WHERE type = ?"
        args.append(t_filter)
    else:
        where_type = ""

    # Try FTS MATCH
    try:
        cur = con.execute(f"{base}{where_type} AND search_index MATCH ?", args + [q])
        for row in cur:
            yield row
        return
    except Exception:
        # fall back below
        pass

    # LIKE fallback (looser, but works everywhere)
    like = f"{base}{where_type} AND (name LIKE ? OR body LIKE ?)"
    like_args = args + [f"%{q}%", f"%{q}%"]
    cur = con.execute(like, like_args)
    for row in cur:
        yield row

def _best_excerpt(body: Optional[str], q: str, max_len: int = 160) -> str:
    if not body:
        return ""
    b = body.strip()
    ql = q.lower()
    i = b.lower().find(ql)
    if i < 0:
        # no direct hit, just clip
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
        # Pull a reasonably big candidate pool; scoring will filter
        pool = list(_try_match_query(con, query, type_filter))
        # If MATCH yielded zero, LIKE fallback may have; but either way, cap the pool
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

            # Compute fuzzy score using name and a small body slice
            s_name = _score(query, name)
            # partial body score gives better hit for long descriptions; cheap slice
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

    # sort by score desc, then name asc for stability
    results.sort(key=lambda t: (-t[0], t[1]["name"].lower()))
    return [r for _, r in results[: max(1, int(limit))]]

def get_vehicle(vehicle_id: int | str) -> Optional[Dict[str, Any]]:
    """
    Return a single vehicle row with joined roles and possible buildings bundled.
    """
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

        # roles
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

        # possible buildings
        buildings = [
            dict(con.execute("SELECT id, name FROM buildings WHERE id = ?", (b_id,)).fetchone())
            for (b_id,) in con.execute(
                "SELECT building_id FROM vehicle_possible_buildings WHERE vehicle_id = ?",
                (vid,),
            )
        ]

        # required schoolings
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

        # equipment compatibility
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
