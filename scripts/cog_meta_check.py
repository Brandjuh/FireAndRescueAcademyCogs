#!/usr/bin/env python3
import json, re, sys
from pathlib import Path

FAIL = 0
def err(msg):  # minimal drama, maximaal nut
    global FAIL
    print(f"::error::{msg}")
    FAIL = 1

def main():
    if len(sys.argv) < 2: 
        err("Usage: cog_meta_check.py <cog_path>")
        sys.exit(1)

    cog = Path(sys.argv[1])
    if not cog.exists():
        err(f"Cog pad bestaat niet: {cog}")
        sys.exit(1)

    # info.json
    info = cog / "info.json"
    if not info.exists():
        err(f"{cog}: info.json ontbreekt")
    else:
        try:
            data = json.loads(info.read_text(encoding="utf-8"))
            for field in ("name","version","min_bot_version","author"):
                if field not in data:
                    err(f"{cog}: info.json mist veld '{field}'")
            # simpele semver check
            if not re.match(r"^\d+\.\d+\.\d+$", data.get("version","")):
                err(f"{cog}: info.json version is geen semver (x.y.z)")
        except Exception as e:
            err(f"{cog}: info.json ongeldig JSON: {e}")

    # __init__.py basiscontrole
    init = cog / "__init__.py"
    if not init.exists():
        err(f"{cog}: __init__.py ontbreekt")
    else:
        txt = init.read_text(encoding="utf-8")
        if "async def setup(bot)" not in txt:
            err(f"{cog}: 'async def setup(bot)' ontbreekt")
        if not re.search(r'logging\.getLogger\(["\']red\.FARA\.[^"\']+["\']\)', txt):
            err(f"{cog}: loggernaam moet 'red.FARA.<cog>' zijn")

    # Dubbele commands binnen dezelfde cog (heel basaal)
    commands = set()
    for p in cog.rglob("*.py"):
        t = p.read_text(encoding="utf-8", errors="ignore")
        for m in re.finditer(r'@(commands\.command|app_commands\.command)\([^)]*name=["\']([^"\']+)["\']', t):
            name = m.group(2).lower()
            if name in commands:
                err(f"{cog}: dubbele commandnaam '{name}'")
            commands.add(name)

        # Simpele import-gate
        if re.search(r'\b(eval|exec)\b', t):
            err(f"{p}: bevat eval/exec")
        if re.search(r'\bimport\s+subprocess\b', t):
            err(f"{p}: subprocess gebruik niet toegestaan zonder whitelist")

    sys.exit(FAIL)

if __name__ == "__main__":
    main()
