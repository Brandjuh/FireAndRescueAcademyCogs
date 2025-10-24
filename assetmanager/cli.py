from __future__ import annotations

import argparse
import sqlite3
from tabulate import tabulate

from config import DB_PATH
from search import fts_search, fuzzy_search, get_vehicle

def do_search(args):
    query = args.query
    typ = None if args.type == "any" else args.type
    results = fuzzy_search(query, typ, limit=args.limit)
    if not results:
        print("No results.")
        return
    print(tabulate(results, headers="keys", tablefmt="github"))

def do_where(args):
    # Toy filter demo. Extend later as needed.
    # Example: type:vehicle role:"Heavy Rescue"
    where = args.filter
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        if where.startswith("type:vehicle"):
            # naive role filter
            m = None
            if "role:" in where:
                role = where.split("role:", 1)[1].strip().strip('"').strip("'")
                rows = con.execute("""
                    SELECT v.id, v.name
                    FROM vehicles v
                    JOIN vehicle_role_map vrm ON vrm.vehicle_id = v.id
                    JOIN vehicle_roles r ON r.id = vrm.role_id
                    WHERE r.role = ?
                    ORDER BY v.id
                """, (role,)).fetchall()
            else:
                rows = con.execute("SELECT id, name FROM vehicles ORDER BY id LIMIT 50").fetchall()
            print(tabulate([dict(r) for r in rows], headers="keys", tablefmt="github"))
        else:
            print("Unsupported filter demo. Use: type:vehicle role:\"Heavy Rescue\"")

def do_vehicle(args):
    vid = int(args.id)
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        v = get_vehicle(con, vid)
        if not v:
            print("Not found.")
            return
        print(tabulate([[k, v[k]] for k in (
            "id","name","min_personnel","max_personnel","price_credits","price_coins",
            "rank_required","water_tank","foam_tank","pump_gpm","speed","specials"
        ) if k in v], headers=["field","value"], tablefmt="github"))

        if v.get("roles"):
            print("\nRoles:")
            for r in v["roles"]:
                print(f" - {r}")

        if v.get("possible_buildings"):
            print("\nPossible buildings:")
            for b in v["possible_buildings"]:
                print(f" - {b['id']}: {b['name']}")

        if v.get("required_schoolings"):
            print("\nRequired schoolings:")
            for s in v["required_schoolings"]:
                print(f" - {s['id']}: {s['name']} x{s['required_count']}")

        if v.get("equipment_compat"):
            print("\nCompatible equipment:")
            for e in v["equipment_compat"]:
                print(f" - {e['id']}: {e['name']}")

def main():
    p = argparse.ArgumentParser(prog="assetmgr")
    sub = p.add_subparsers(required=True)

    s = sub.add_parser("search", help="Fuzzy search assets")
    s.add_argument("type", choices=["any", "vehicle", "equipment", "schooling", "building"])
    s.add_argument("query")
    s.add_argument("--limit", type=int, default=20)
    s.set_defaults(func=do_search)

    w = sub.add_parser("where", help="Toy filter demo")
    w.add_argument("filter")
    w.set_defaults(func=do_where)

    v = sub.add_parser("vehicle", help="Show detailed vehicle")
    v.add_argument("id")
    v.set_defaults(func=do_vehicle)

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
