from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import discord
from redbot.core import commands, checks
from redbot.core.utils.chat_formatting import box

# Imports uit onze module
try:
    from .search import fuzzy_search, get_vehicle
except Exception:  # pragma: no cover
    from search import fuzzy_search, get_vehicle  # type: ignore


def _fmt_score(s: float) -> str:
    return f"{s:.0f}"

# --- Agency inference --------------------------------------------------------

AGENCY_ORDER = ("Fire", "Police", "EMS", "Rescue", "Tow", "FBI", "Other")

_KEYWORDS = {
    "Fire": (
        "fire", "engine", "pumper", "ladder", "aerial", "quint", "tanker", "foam",
        "brush", "wildland", "hazmat", "platform", "water", "rescue engine",
    ),
    "Police": (
        "police", "sheriff", "patrol", "k9", "k-9", "swat", "riot", "traffic unit",
        "highway", "interceptor", "law", "cop",
    ),
    "EMS": (
        "ambulance", "ems", "paramedic", "hems", "medical", "icu", "rescue ambulance",
    ),
    "Rescue": (
        "rescue", "heavy rescue", "technical rescue", "usar",
    ),
    "Tow": (
        "tow", "wrecker", "flatbed", "recovery",
    ),
    "FBI": (
        "fbi", "federal", "bureau",
    ),
}

_BUILDING_HINTS = {
    "Fire": ("fire", "aerial", "rescue", "wildland", "hazmat"),
    "Police": ("police", "sheriff", "law"),
    "EMS": ("ambulance", "clinic", "medical", "hospital", "ems"),
    "Tow": ("tow", "recovery"),
    "FBI": ("fbi", "federal"),
}

_ROLE_HINTS = {
    "Fire": ("engine", "ladder", "hazmat", "foam", "tanker", "quint", "brush"),
    "Police": ("patrol", "k9", "riot", "swat", "traffic"),
    "EMS": ("ambulance", "paramedic", "medic"),
    "Rescue": ("rescue", "usar"),
    "Tow": ("tow",),
    "FBI": ("fbi",),
}

def infer_agency(name: str, roles: List[str], building_names: List[str], building_categories: List[str]) -> str:
    n_low = (name or "").lower()

    # 1) keywords in vehicle name
    hits: List[str] = []
    for agency, kws in _KEYWORDS.items():
        if any(k in n_low for k in kws):
            hits.append(agency)
    if hits:
        # return the first according to preferred order
        for a in AGENCY_ORDER:
            if a in hits:
                return a

    # 2) roles
    rl = " ".join(roles).lower() if roles else ""
    if rl:
        hits = []
        for agency, rk in _ROLE_HINTS.items():
            if any(k in rl for k in rk):
                hits.append(agency)
        if hits:
            for a in AGENCY_ORDER:
                if a in hits:
                    return a

    # 3) buildings: names + categories
    b_text = " ".join(building_names + building_categories).lower()
    if b_text:
        hits = []
        for agency, bk in _BUILDING_HINTS.items():
            if any(k in b_text for k in bk):
                hits.append(agency)
        if hits:
            for a in AGENCY_ORDER:
                if a in hits:
                    return a

    # 4) last resort: try basic trigrams
    if "pol" in n_low:
        return "Police"
    if "amb" in n_low or "med" in n_low:
        return "EMS"
    if "rescue" in n_low:
        return "Rescue"

    return "Other"

# --- Cog ---------------------------------------------------------------------

class AssetManager(commands.Cog):
    """Asset database search and details."""

    def __init__(self, bot):
        self.bot = bot

    # Main group: default fuzzy search
    @commands.group(name="assets", invoke_without_command=True)
    async def assets_group(self, ctx: commands.Context, *, query: str):
        """Search assets (vehicles, equipment, schoolings, buildings)."""
        q = (query or "").strip()
        if not q:
            await ctx.send("Give me something to search for.")
            return

        # Run fuzzy search in a thread
        items = await asyncio.to_thread(fuzzy_search, q, None, 20)

        if not items:
            await ctx.send("No results.")
            return

        # Build a compact embed list
        embed = discord.Embed(title=f"Assets search: {q}", color=discord.Color.blurple())
        # Show top 10 inline, rest as clipped footer line
        top = items[:10]
        for it in top:
            typ = it["type"]
            vid = it["id"]
            name = it.get("name") or f"{typ.capitalize()} {vid}"
            score = _fmt_score(float(it.get("score", 0)))
            snippet = it.get("snippet") or ""
            # Trim snippet to not break mobile
            if len(snippet) > 180:
                snippet = snippet[:177] + "…"
            embed.add_field(
                name=f"{name}  ·  {typ} #{vid}  ·  {score}",
                value=snippet or "\u200b",
                inline=False,
            )
        if len(items) > len(top):
            embed.set_footer(text=f"{len(items)} results, showing first {len(top)}.")

        await ctx.send(embed=embed)

    # Subcommand: vehicle details
    @assets_group.command(name="vehicle")
    async def assets_vehicle(self, ctx: commands.Context, vehicle_id: int):
        """Show detailed info about a vehicle by ID."""
        def _get() -> Optional[Dict[str, Any]]:
            return get_vehicle(vehicle_id)  # search.get_vehicle has single-arg signature

        v = await asyncio.to_thread(_get)
        if not v:
            await ctx.send(f"Vehicle {vehicle_id} not found.")
            return

        name = v.get("name") or f"Vehicle {vehicle_id}"
        roles = list(v.get("roles") or [])
        buildings = list(v.get("possible_buildings") or [])
        schoolings = list(v.get("required_schoolings") or [])
        equipment = list(v.get("equipment_compat") or [])

        b_names = [b.get("name", "") for b in buildings if isinstance(b, dict)]
        # some schemas may have category in building; if not, keep empty
        b_cats = []
        for b in buildings:
            if isinstance(b, dict):
                cat = b.get("category")
                if cat:
                    b_cats.append(str(cat))

        agency = infer_agency(
            name=name,
            roles=roles,
            building_names=b_names,
            building_categories=b_cats,
        )

        em = discord.Embed(
            title=f"{name}  ·  #{vehicle_id}",
            color=discord.Color.dark_teal(),
        )
        em.add_field(name="Agency", value=agency, inline=True)
        em.add_field(
            name="Crew",
            value=f"{v.get('min_personnel', 0)}–{v.get('max_personnel', 0)}",
            inline=True,
        )
        price_parts = []
        if v.get("price_credits") is not None:
            price_parts.append(f"{v['price_credits']} credits")
        if v.get("price_coins") is not None:
            price_parts.append(f"{v['price_coins']} coins")
        em.add_field(name="Price", value=", ".join(price_parts) or "—", inline=True)

        tanks = []
        if v.get("water_tank"):
            tanks.append(f"Water {v['water_tank']}")
        if v.get("foam_tank"):
            tanks.append(f"Foam {v['foam_tank']}")
        if v.get("pump_gpm"):
            tanks.append(f"Pump {v['pump_gpm']} GPM")
        em.add_field(name="Capabilities", value=", ".join(tanks) or "—", inline=True)

        if v.get("rank_required"):
            em.add_field(name="Rank required", value=str(v["rank_required"]), inline=True)
        if v.get("speed"):
            em.add_field(name="Speed", value=str(v["speed"]), inline=True)

        if v.get("specials"):
            em.add_field(name="Specials", value=str(v["specials"]), inline=False)

        if roles:
            em.add_field(name="Roles", value=", ".join(sorted(roles)), inline=False)

        if buildings:
            em.add_field(
                name="Possible buildings",
                value=", ".join(sorted(set(b_names))) or "—",
                inline=False,
            )

        if schoolings:
            em.add_field(
                name="Required schoolings",
                value=", ".join(f"{s.get('name','?')} ×{s.get('required_count',1)}" for s in schoolings),
                inline=False,
            )

        if equipment:
            em.add_field(
                name="Equipment compat",
                value=", ".join(sorted(e.get("name","?") for e in equipment)),
                inline=False,
            )

        await ctx.send(embed=em)

    # Subcommand: status
    @assets_group.command(name="status")
    @checks.is_owner()
    async def assets_status(self, ctx: commands.Context):
        """Quick status to verify DB/FTS."""
        # super lightweight check: ensure search returns at least something for a common term
        probe = await asyncio.to_thread(fuzzy_search, "engine", None, 5)
        ok = bool(probe)
        lines = [
            f"Search probe: {'OK' if ok else 'EMPTY'}",
            "Try `assets vehicle 1` or another known ID if you expect data.",
        ]
        await ctx.send(box("\n".join(lines), "ini"))
