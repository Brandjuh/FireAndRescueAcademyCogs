from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional

import discord
from redbot.core import commands, checks, Config

# Local modules
from . import etl
from .search import fuzzy_search, get_vehicle
from .config import DB_PATH

log = logging.getLogger("red.FARA.AssetManager")

DEFAULTS_GUILD = {
    "enabled": True,
    "interval_hours": 24,
    "announce_channel_id": None,
    "last_run_iso": None,
    "last_ok_iso": None,
    "last_error": None,
}


class AssetManager(commands.Cog):
    """Keeps assets.db up-to-date and provides fuzzy search and details."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xA55E7A, force_registration=True)
        self.config.register_guild(**DEFAULTS_GUILD)

        self._task: asyncio.Task | None = None
        self._ready = asyncio.Event()

    # ---------- Lifecycle ----------

    async def cog_load(self):
        self._task = asyncio.create_task(self._runner(), name="assetmanager_runner")

    async def cog_unload(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    @commands.Cog.listener()
    async def on_red_ready(self):
        self._ready.set()

    # ---------- DB helpers ----------

    async def _ensure_schema(self):
        await asyncio.to_thread(etl.ensure_db)

    def _table_exists(self, table: str) -> bool:
        try:
            with sqlite3.connect(str(DB_PATH)) as con:
                cur = con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                )
                return cur.fetchone() is not None
        except Exception:
            return False

    def _rowcount(self, table: str) -> int:
        try:
            with sqlite3.connect(str(DB_PATH)) as con:
                cur = con.execute(f"SELECT COUNT(1) FROM {table}")
                return int(cur.fetchone()[0])
        except Exception:
            return 0

    # ---------- Background Runner ----------

    async def _runner(self):
        await self._ready.wait()
        await asyncio.sleep(5)

        # Ensure schema once on startup
        try:
            await self._ensure_schema()
        except Exception:
            log.exception("Could not initialize schema")

        # Initial ETL if DB is empty or index is missing
        need_initial = not self._table_exists("search_index") or self._rowcount("vehicles") == 0
        if need_initial:
            try:
                await asyncio.to_thread(etl.run_etl)
                log.info("AssetManager: initial ETL executed on startup")
            except Exception:
                log.exception("Initial ETL on startup failed")

        while True:
            try:
                for guild in self.bot.guilds:
                    try:
                        await self._maybe_run_for_guild(guild)
                    except Exception as e:
                        log.exception("Guild ETL error in %s: %s", guild.id, e)
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("AssetManager runner loop error")

    async def _maybe_run_for_guild(self, guild: discord.Guild):
        conf = self.config.guild(guild)
        enabled = await conf.enabled()
        if not enabled:
            return

        interval_h = await conf.interval_hours()
        last_iso = await conf.last_run_iso()
        now = datetime.now(timezone.utc)

        due = True
        if last_iso:
            try:
                last = datetime.fromisoformat(last_iso)
                due = (now - last) >= timedelta(hours=interval_h)
            except Exception:
                due = True

        if not due:
            return

        await conf.last_run_iso.set(now.isoformat())
        try:
            await asyncio.to_thread(etl.run_etl)
            await conf.last_ok_iso.set(datetime.now(timezone.utc).isoformat())
            await conf.last_error.set(None)
            await self._announce(guild, f"Assets ETL OK. DB `{DB_PATH.name}` is up-to-date.")
        except Exception as e:
            await conf.last_error.set(str(e))
            await self._announce(guild, f"Assets ETL failed: `{e}`")

    async def _announce(self, guild: discord.Guild, message: str):
        channel_id = await self.config.guild(guild).announce_channel_id()
        if not channel_id:
            return
        ch = guild.get_channel(channel_id)
        if not ch:
            return
        try:
            await ch.send(message)
        except Exception:
            log.debug("Failed to announce in %s", channel_id)

    # ---------- Commands ----------

    @commands.group(name="assets", invoke_without_command=True)
    async def assets_group(self, ctx: commands.Context, *, query: str | None = None):
        """
        Main command: fuzzy search across all assets.
        Usage: [p]assets <query>
        Subcommands: status, enable, interval, announce, reload, vehicle
        """
        if query is None:
            return await ctx.send("Usage: `[p]assets <query>` or `[p]assets status`.")

        await self._ensure_schema()
        if not self._table_exists("search_index"):
            return await ctx.send("The assets database has not been built yet. Run `[p]assets reload` first.")

        if self._rowcount("vehicles") == 0 and self._rowcount("buildings") == 0:
            return await ctx.send("No assets found. Run `[p]assets reload` to fetch data.")

        try:
            items = await asyncio.to_thread(fuzzy_search, query, None, 20)
        except sqlite3.OperationalError:
            return await ctx.send("Search index missing or corrupt. Run `[p]assets reload` to rebuild.")

        if not items:
            return await ctx.send("No results.")
        lines = [f"`{i['type']}` #{i['ref_id']} — **{i['name']}** (score {i['score']})" for i in items[:20]]
        await ctx.send("\n".join(lines))

    @assets_group.command(name="status")
    @checks.admin_or_permissions(manage_guild=True)
    async def assets_status(self, ctx: commands.Context):
        """Show status and row counts."""
        await self._ensure_schema()
        conf = self.config.guild(ctx.guild)

        counts = {
            "vehicles": self._rowcount("vehicles"),
            "equipment": self._rowcount("equipment"),
            "schoolings": self._rowcount("schoolings"),
            "buildings": self._rowcount("buildings"),
            "search_index": self._rowcount("search_index") if self._table_exists("search_index") else 0,
        }
        data = {
            "enabled": await conf.enabled(),
            "interval_hours": await conf.interval_hours(),
            "announce_channel_id": await conf.announce_channel_id(),
            "last_run_iso": await conf.last_run_iso(),
            "last_ok_iso": await conf.last_ok_iso(),
            "last_error": await conf.last_error(),
            "db_path": str(DB_PATH),
            "counts": counts,
        }
        lines = [f"**{k}**: {v}" for k, v in data.items()]
        await ctx.send("\n".join(lines))

    @assets_group.command(name="enable")
    @checks.admin_or_permissions(manage_guild=True)
    async def assets_enable(self, ctx: commands.Context, value: bool):
        """Enable or disable scheduled ETL."""
        await self.config.guild(ctx.guild).enabled.set(value)
        await ctx.send(f"Scheduled ETL is now {'enabled' if value else 'disabled'}.")

    @assets_group.command(name="interval")
    @checks.admin_or_permissions(manage_guild=True)
    async def assets_interval(self, ctx: commands.Context, hours: int):
        """Set ETL interval in hours (1..168)."""
        hours = max(1, min(168, hours))
        await self.config.guild(ctx.guild).interval_hours.set(hours)
        await ctx.send(f"Interval set to {hours}h.")

    @assets_group.command(name="announce")
    @checks.admin_or_permissions(manage_guild=True)
    async def assets_announce(self, ctx: commands.Context, channel: Optional[discord.TextChannel]):
        """Set or clear an announce channel for ETL results."""
        await self.config.guild(ctx.guild).announce_channel_id.set(channel.id if channel else None)
        await ctx.send(f"Announce channel set to {channel.mention if channel else 'None'}.")

    @assets_group.command(name="reload")
    @checks.admin_or_permissions(manage_guild=True)
    async def assets_reload(self, ctx: commands.Context):
        """Run ETL now."""
        await self._ensure_schema()
        msg = await ctx.send("Running ETL...")
        try:
            await asyncio.to_thread(etl.run_etl)
            await self.config.guild(ctx.guild).last_ok_iso.set(datetime.now(timezone.utc).isoformat())
            await self.config.guild(ctx.guild).last_error.set(None)
            await msg.edit(content="ETL done. DB updated.")
        except Exception as e:
            await self.config.guild(ctx.guild).last_error.set(str(e))
            await msg.edit(content=f"ETL failed: {e}")

    @assets_group.command(name="vehicle")
    async def assets_vehicle(self, ctx: commands.Context, vehicle_id: int):
        """Show vehicle details by ID."""
        await self._ensure_schema()
        if not self._table_exists("vehicles"):
            return await ctx.send("The assets database has not been built yet. Run `[p]assets reload` first.")
        if self._rowcount("vehicles") == 0:
            return await ctx.send("No vehicles found. Run `[p]assets reload` to fetch data.")

        def _get():
            con = sqlite3.connect(DB_PATH)
            con.row_factory = sqlite3.Row
            with con:
                return get_vehicle(con, vehicle_id)

        try:
            v = await asyncio.to_thread(_get)
        except sqlite3.OperationalError:
            return await ctx.send("Database or index missing. Run `[p]assets reload` to rebuild.")

        if not v:
            return await ctx.send("Not found.")

        emb = discord.Embed(title=f"{v['name']} (#{v['id']})", colour=discord.Colour.blurple())
        fields = [
            ("Min Personnel", v["min_personnel"]),
            ("Max Personnel", v["max_personnel"]),
            ("Price (credits)", v.get("price_credits")),
            ("Price (coins)", v.get("price_coins")),
            ("Rank required", v.get("rank_required")),
            ("Water tank", v.get("water_tank")),
            ("Foam tank", v.get("foam_tank")),
            ("Pump GPM", v.get("pump_gpm")),
            ("Speed", v.get("speed")),
            ("Specials", v.get("specials")),
        ]
        for k, val in fields:
            if val not in (None, "", 0):
                emb.add_field(name=k, value=str(val), inline=True)

        roles = v.get("roles") or []
        if roles:
            emb.add_field(name="Roles", value=", ".join(roles), inline=False)

        pbs = v.get("possible_buildings") or []
        if pbs:
            emb.add_field(
                name="Possible buildings",
                value="\n".join(f"- #{b['id']} {b['name']}" for b in pbs[:12]) + ("…" if len(pbs) > 12 else ""),
                inline=False,
            )

        reqs = v.get("required_schoolings") or []
        if reqs:
            emb.add_field(
                name="Required schoolings",
                value="\n".join(f"- #{s['id']} {s['name']} x{s['required_count']}" for s in reqs),
                inline=False,
            )

        eq = v.get("equipment_compat") or []
        if eq:
            emb.add_field(
                name="Compatible equipment",
                value="\n".join(f"- #{e['id']} {e['name']}" for e in eq[:12]) + ("…" if len(eq) > 12 else ""),
                inline=False,
            )

        await ctx.send(embed=emb)
