from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import discord
from redbot.core import commands, checks
from redbot.core.utils.chat_formatting import box

try:
    from .search import (
        fuzzy_search,
        get_vehicle,
        get_equipment,
        get_schooling,
        get_building,
    )
except Exception:  # pragma: no cover
    from search import (  # type: ignore
        fuzzy_search,
        get_vehicle,
        get_equipment,
        get_schooling,
        get_building,
    )

# ---------- Helpers ----------

def _fmt_score(s: float) -> str:
    try:
        return f"{float(s):.0f}"
    except Exception:
        return "0"

def _clip(text: Optional[str], limit: int) -> str:
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"

def _crew_text(v: Dict[str, Any]) -> str:
    mn = v.get("min_personnel") or 0
    mx = v.get("max_personnel") or 0
    if mn == 0 and mx == 0:
        return "—"
    return f"{mn}–{mx}"

# Agency inference (zelfde als eerder)
AGENCY_ORDER = ("Fire", "Police", "EMS", "Rescue", "Tow", "FBI", "Other")

_KEYWORDS = {
    "Fire": ("fire", "engine", "ladder", "quint", "tanker", "foam", "brush", "wildland", "hazmat", "aerial", "platform", "pumper"),
    "Police": ("police", "sheriff", "patrol", "k9", "k-9", "swat", "riot", "traffic", "highway", "interceptor", "law", "cop"),
    "EMS": ("ambulance", "ems", "paramedic", "hems", "medical", "icu"),
    "Rescue": ("rescue", "usar", "technical"),
    "Tow": ("tow", "wrecker", "flatbed", "recovery"),
    "FBI": ("fbi", "federal", "bureau"),
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

    hits: List[str] = []
    for agency, kws in _KEYWORDS.items():
        if any(k in n_low for k in kws):
            hits.append(agency)
    if hits:
        for a in AGENCY_ORDER:
            if a in hits:
                return a

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

    if "pol" in n_low:
        return "Police"
    if "amb" in n_low or "med" in n_low:
        return "EMS"
    if "rescue" in n_low:
        return "Rescue"

    return "Other"

# “kleur per agency” zolang we geen echte kleurvelden uit de data hebben
AGENCY_COLORS = {
    "Fire": discord.Color.red(),
    "Police": discord.Color.blue(),
    "EMS": discord.Color.green(),
    "Rescue": discord.Color.dark_orange(),
    "Tow": discord.Color.dark_gold(),
    "FBI": discord.Color.dark_teal(),
    "Other": discord.Color.blurple(),
}

MONEY_EMOJI = "💰"
COIN_EMOJI = "🪙"


# ---------- Cog ----------

class AssetManager(commands.Cog):
    """Asset database."""

    def __init__(self, bot):
        self.bot = bot

    @commands.group(name="assets", invoke_without_command=True)
    async def assets_group(self, ctx: commands.Context):
        """Show available asset commands."""
        p = ctx.clean_prefix
        lines = [
            f"{p}assets search <query>      · Fuzzy search across everything",
            f"{p}assets vehicle <id>        · Vehicle details",
            f"{p}assets equipment <id>      · Equipment details",
            f"{p}assets schooling <id>      · Schooling details",
            f"{p}assets building <id>       · Building details",
            f"{p}assets status              · Quick ETL/index sanity check",
        ]
        await ctx.send(box("\n".join(lines), "ini"))

    @assets_group.command(name="search")
    async def assets_search(self, ctx: commands.Context, *, query: str):
        """Fuzzy search vehicles, equipment, schoolings, buildings."""
        q = (query or "").strip()
        if not q:
            await ctx.send("Usage: `assets search <query>`.")
            return

        try:
            items = await asyncio.to_thread(fuzzy_search, q, None, 20)
        except Exception as e:
            await ctx.send(box(f"Search failed: {e}", "ini"))
            return

        if not items:
            await ctx.send("No results.")
            return

        title = _clip(f"Assets search: {q}", 256)
        embed = discord.Embed(title=title, color=discord.Color.blurple())

        total_chars = len(title)
        max_fields = 10
        for it in items[:max_fields]:
            typ = str(it.get("type", "item"))
            rid = str(it.get("id", "?"))
            name = it.get("name") or f"{typ.capitalize()} {rid}"
            score = _fmt_score(float(it.get("score", 0)))
            snippet = it.get("snippet") or "\u200b"

            fname = _clip(f"{name} · {typ} #{rid} · {score}", 256)
            fval = _clip(snippet, 1024)

            if total_chars + len(fname) + len(fval) + 10 > 5900:
                break

            embed.add_field(name=fname, value=fval, inline=False)
            total_chars += len(fname) + len(fval) + 10

        if len(items) > max_fields:
            embed.set_footer(text=f"{len(items)} results, showing first {embed.fields.__len__()}.")

        await ctx.send(embed=embed)

    @assets_group.command(name="vehicle")
    async def assets_vehicle(self, ctx: commands.Context, vehicle_id: int):
        """Show detailed info about a vehicle by ID."""
        def _get() -> Optional[Dict[str, Any]]:
            return get_vehicle(vehicle_id)

        try:
            v = await asyncio.to_thread(_get)
        except Exception as e:
            await ctx.send(box(f"Lookup failed: {e}", "ini"))
            return

        if not v:
            await ctx.send(f"Vehicle {vehicle_id} not found.")
            return

        name = v.get("name") or f"Vehicle {vehicle_id}"
        roles = list(v.get("roles") or [])
        buildings = list(v.get("possible_buildings") or [])
        schoolings = list(v.get("required_schoolings") or [])
        equipment = list(v.get("equipment_compat") or [])

        b_names = [b.get("name", "") for b in buildings if isinstance(b, dict)]
        b_cats: List[str] = []
        for b in buildings:
            if isinstance(b, dict):
                cat = b.get("category")
                if cat:
                    b_cats.append(str(cat))

        agency = infer_agency(name, roles, b_names, b_cats)
        color = AGENCY_COLORS.get(agency, discord.Color.blurple())

        em = discord.Embed(
            title=_clip(name, 256),
            color=color,
        )
        em.set_footer(text=f"#{vehicle_id}")

        # Crew
        em.add_field(name="Crew", value=_crew_text(v), inline=True)

        # Price with emojis
        price_parts: List[str] = []
        if v.get("price_credits") is not None:
            price_parts.append(f"{MONEY_EMOJI} {v['price_credits']}")
        if v.get("price_coins") is not None:
            price_parts.append(f"{COIN_EMOJI} {v['price_coins']}")
        em.add_field(name="Price", value=", ".join(price_parts) or "—", inline=True)

        # Agency field
        em.add_field(name="Agency", value=agency, inline=True)

        # Capabilities
        caps: List[str] = []
        if v.get("water_tank"):
            caps.append(f"Water {v['water_tank']}")
        if v.get("foam_tank"):
            caps.append(f"Foam {v['foam_tank']}")
        if v.get("pump_gpm"):
            caps.append(f"Pump {v['pump_gpm']} GPM")
        em.add_field(name="Capabilities", value=", ".join(caps) or "—", inline=False)

        if v.get("rank_required"):
            em.add_field(name="Rank required", value=str(v["rank_required"]), inline=True)
        if v.get("speed"):
            em.add_field(name="Speed", value=str(v["speed"]), inline=True)

        if v.get("specials"):
            em.add_field(name="Specials", value=_clip(str(v["specials"]), 1024), inline=False)

        if roles:
            em.add_field(name="Roles", value=_clip(", ".join(sorted(roles)), 1024), inline=False)

        if buildings:
            em.add_field(
                name="Possible buildings",
                value=_clip(", ".join(sorted(set(b_names))) or "—", 1024),
                inline=False,
            )

        if schoolings:
            em.add_field(
                name="Required schoolings",
                value=_clip(", ".join(f"{s.get('name','?')} ×{s.get('required_count',1)}" for s in schoolings), 1024),
                inline=False,
            )

        if equipment:
            em.add_field(
                name="Equipment compat",
                value=_clip(", ".join(sorted(e.get("name","?") for e in equipment)), 1024),
                inline=False,
            )

        await ctx.send(embed=em)

    @assets_group.command(name="equipment")
    async def assets_equipment(self, ctx: commands.Context, equipment_id: int):
        """Show equipment details by ID."""
        eq = await asyncio.to_thread(lambda: get_equipment(equipment_id))
        if not eq:
            await ctx.send(f"Equipment {equipment_id} not found.")
            return
        # Geen kleur in DB? Kies nette vaste kleur.
        em = discord.Embed(
            title=_clip(eq.get("name","Equipment"), 256),
            color=discord.Color.dark_gold(),
        )
        em.set_footer(text=f"#{equipment_id}")
        desc = _clip(eq.get("description") or "", 2048)
        if desc:
            em.description = desc
        if eq.get("size"):
            em.add_field(name="Size", value=str(eq["size"]), inline=True)
        if eq.get("notes"):
            em.add_field(name="Notes", value=_clip(str(eq["notes"]), 1024), inline=False)
        await ctx.send(embed=em)

    @assets_group.command(name="schooling")
    async def assets_schooling(self, ctx: commands.Context, schooling_id: int):
        """Show schooling details by ID."""
        sc = await asyncio.to_thread(lambda: get_schooling(schooling_id))
        if not sc:
            await ctx.send(f"Schooling {schooling_id} not found.")
            return
        em = discord.Embed(
            title=_clip(sc.get("name","Schooling"), 256),
            color=discord.Color.dark_green(),
        )
        em.set_footer(text=f"#{schooling_id}")
        if sc.get("department"):
            em.add_field(name="Department", value=str(sc["department"]), inline=True)
        if sc.get("duration_days") is not None:
            em.add_field(name="Duration (days)", value=str(sc["duration_days"]), inline=True)
        await ctx.send(embed=em)

    @assets_group.command(name="building")
    async def assets_building(self, ctx: commands.Context, building_id: int):
        """Show building details by ID."""
        b = await asyncio.to_thread(lambda: get_building(building_id))
        if not b:
            await ctx.send(f"Building {building_id} not found.")
            return
        # We hebben (voor nu) geen 'color' kolom in de DB. Vaste kleur tot we dat opslaan.
        em = discord.Embed(
            title=_clip(b.get("name","Building"), 256),
            color=discord.Color.dark_purple(),
        )
        em.set_footer(text=f"#{building_id}")
        if b.get("category"):
            em.add_field(name="Category", value=str(b["category"]), inline=True)
        if b.get("notes"):
            em.add_field(name="Notes", value=_clip(str(b["notes"]), 1024), inline=False)
        await ctx.send(embed=em)

    @assets_group.command(name="status")
    @checks.is_owner()
    async def assets_status(self, ctx: commands.Context):
        """Quick status to verify DB/FTS."""
        try:
            probe = await asyncio.to_thread(fuzzy_search, "engine", None, 5)
            ok = bool(probe)
            lines = [
                f"Search probe: {'OK' if ok else 'EMPTY'}",
                "Try: assets search engine",
                "Try: assets vehicle 1",
            ]
            await ctx.send(box("\n".join(lines), "ini"))
        except Exception as e:
            await ctx.send(box(f"Status failed: {e}", "ini"))
