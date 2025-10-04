
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import discord
from redbot.core import commands, Config

__version__ = "0.1.0"

NY_TZ = "America/New_York"
NL = "\n"

def _parse_hhmm(hhmm: str):
    hhmm = (hhmm or "").strip()
    try:
        h, m = hhmm.split(":")
        return int(h), int(m)
    except Exception:
        return 23, 50

class TopPlayers(commands.Cog):
    """Daily/Monthly Top Players (wraps DailyBriefing embed rendering)."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xD1A7_70PP_14, force_registration=True)
        default_guild = {
            "channels": {"daily": None, "monthly": None},
            "times": {"daily": "23:50", "monthly": "23:50"},
            "top_n": 10,
            "hot_refresh": True,
        }
        self.config.register_guild(**default_guild)
        self._bg_task = None
        self.bot.loop.create_task(self._maybe_start())

    async def cog_unload(self):
        if self._bg_task:
            self._bg_task.cancel()

    async def _maybe_start(self):
        await self.bot.wait_until_red_ready()
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._bg_loop())

    # ---- Time helpers ----
    def _now_tz(self, tzname: str):
        return datetime.now(ZoneInfo(tzname))

    def _next_fire_after(self, tzname: str, hhmm: str, now: datetime | None = None) -> datetime:
        tz = ZoneInfo(tzname)
        now = now.astimezone(tz) if now else datetime.now(tz)
        h, m = _parse_hhmm(hhmm)
        candidate = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if candidate <= now:
            candidate = candidate + timedelta(days=1)
        return candidate

    async def _bg_loop(self):
        while True:
            try:
                cfg = await self.config.guild(self.bot.guilds[0]).all() if self.bot.guilds else await self.config.all_guilds()
                # We loop per guild to support per-guild scheduling
                tasks = []
                for g in self.bot.guilds:
                    gc = await self.config.guild(g).all()
                    tz = NY_TZ
                    nd = self._next_fire_after(tz, gc["times"]["daily"])
                    nm = self._next_fire_after(tz, gc["times"]["monthly"])
                    next_fire = min(nd, nm)
                    tasks.append((g.id, next_fire, nd, nm))
                # Sleep until the earliest next_fire across guilds
                if not tasks:
                    await asyncio.sleep(60)
                    continue
                global_next = min(t[1] for t in tasks)
                sleep_s = max(5, (global_next - self._now_tz(NY_TZ)).total_seconds())
                await asyncio.sleep(sleep_s)

                # Fire for guilds that reached their moment (with ±120s tolerance)
                now_tz = self._now_tz(NY_TZ).replace(second=0, microsecond=0)
                for gid, _, nd, nm in tasks:
                    gc = await self.config.guild_from_id(gid).all()
                    topn = int(gc.get("top_n", 10))
                    if abs((now_tz - nd.replace(tzinfo=ZoneInfo(NY_TZ))).total_seconds()) <= 120:
                        await self._post_daily_for_gid(gid, now_tz, topn)
                    if abs((now_tz - nm.replace(tzinfo=ZoneInfo(NY_TZ))).total_seconds()) <= 120:
                        await self._post_monthly_for_gid(gid, now_tz, topn)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(30)

    # ---- Rendering via DailyBriefing ----
    def _get_db_cog(self):
        return self.bot.get_cog("DailyBriefing")

    async def _render_daily_top(self, ny_now: datetime, top_n: int):
        dbc = self._get_db_cog()
        if dbc and hasattr(dbc, "_render_daily_top_embed"):
            try:
                return await dbc._render_daily_top_embed(ny_now, top_n)
            except Exception:
                pass
        # Fallback
        e = discord.Embed(title="Daily Top Players", description="DailyBriefing cog not available.", color=0x2B88FF)
        return e

    async def _render_monthly_top(self, ny_now: datetime, top_n: int):
        dbc = self._get_db_cog()
        if dbc and hasattr(dbc, "_render_monthly_top_embed"):
            try:
                return await dbc._render_monthly_top_embed(ny_now, top_n)
            except Exception:
                pass
        # Fallback
        ym = ny_now.strftime("%Y-%m")
        e = discord.Embed(title="Monthly Top Players", description=f"{ym} — DailyBriefing cog not available.", color=0x2B88FF)
        return e

    # ---- Posting helpers ----
    async def _send_embed(self, dest, e):
        # Support list or single embed, and field/value splitting >1024
        items = e if isinstance(e, list) else [e]
        for ee in items:
            # enforce max lengths
            if ee.title and len(ee.title) > 256: ee.title = ee.title[:256]
            if ee.description and len(ee.description) > 4096: ee.description = ee.description[:4096]
            new_fields = []
            for f in ee.fields:
                name = f.name[:256] if f.name else ""
                value = f.value or ""
                inline = f.inline
                # split value if too long
                while len(value) > 1024:
                    new_fields.append((name, value[:1024], inline))
                    value = value[1024:]
                    name = "…"  # continuation marker
                new_fields.append((name, value, inline))
            # rebuild embed if we had to split fields
            if len(new_fields) != len(ee.fields) or any(len(v) > 1024 for _, v, _ in new_fields):
                rebuilt = discord.Embed(title=ee.title, description=ee.description, color=ee.color)
                for n, v, inl in new_fields:
                    rebuilt.add_field(name=n, value=v, inline=inl)
                ee = rebuilt
            await dest.send(embed=ee)

    async def _post_daily_for_gid(self, gid: int, ny_now: datetime, topn: int):
        gc = await self.config.guild_from_id(gid).all()
        channel_id = (gc.get("channels") or {}).get("daily")
        ch = self.bot.get_channel(channel_id) if channel_id else None
        if not ch:
            return 0
        e = await self._render_daily_top(ny_now, topn)
        await self._send_embed(ch, e)
        return 1

    async def _post_monthly_for_gid(self, gid: int, ny_now: datetime, topn: int):
        gc = await self.config.guild_from_id(gid).all()
        channel_id = (gc.get("channels") or {}).get("monthly")
        ch = self.bot.get_channel(channel_id) if channel_id else None
        if not ch:
            return 0
        e = await self._render_monthly_top(ny_now, topn)
        await self._send_embed(ch, e)
        return 1

    # ---- Commands ----
    @commands.group(name="tplayers")
    @commands.admin_or_permissions(manage_guild=True)
    @commands.guild_only()
    async def tp_group(self, ctx: commands.Context):
        """TopPlayers configuration and testing."""

    @tp_group.command(name="version")
    async def version(self, ctx: commands.Context):
        await ctx.send(f"TopPlayers v{__version__} (NY time {NY_TZ})")

    @tp_group.command(name="setchannel")
    async def setchannel(self, ctx: commands.Context, which: str, channel: discord.TextChannel):
        which = (which or "").lower()
        if which not in {"daily", "monthly"}:
            await ctx.send("Which must be 'daily' or 'monthly'.")
            return
        async with self.config.guild(ctx.guild).channels() as chs:
            chs[which] = channel.id
        await ctx.send(f"{which.capitalize()} channel set to {channel.mention}")

    @tp_group.command(name="settime")
    async def settime(self, ctx: commands.Context, which: str, hhmm: str):
        which = (which or "").lower()
        if which not in {"daily", "monthly"}:
            await ctx.send("Which must be 'daily' or 'monthly'.")
            return
        if ":" not in hhmm:
            await ctx.send("Time must be HH:MM (24h) in America/New_York.")
            return
        async with self.config.guild(ctx.guild).times() as t:
            t[which] = hhmm
        await ctx.send(f"{which.capitalize()} time set to {hhmm} (NY time)")

    @tp_group.group(name="preview")
    async def preview(self, ctx: commands.Context):
        """Preview embeds without posting to configured channels."""

    @preview.command(name="daily")
    async def preview_daily(self, ctx: commands.Context, date_str: str | None = None, topn: int | None = None):
        tz = ZoneInfo(NY_TZ)
        ny = datetime.now(tz) if not date_str else datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=tz)
        if topn is None:
            topn = int((await self.config.guild(ctx.guild).top_n()) or 10)
        e = await self._render_daily_top(ny, int(topn))
        await self._send_embed(ctx, e)

    @preview.command(name="monthly")
    async def preview_monthly(self, ctx: commands.Context, ym: str | None = None, topn: int | None = None):
        tz = ZoneInfo(NY_TZ)
        now = datetime.now(tz)
        if ym:
            y, m = map(int, ym.split("-"))
            now = now.replace(year=y, month=m, day=1)
        if topn is None:
            topn = int((await self.config.guild(ctx.guild).top_n()) or 10)
        e = await self._render_monthly_top(now, int(topn))
        await self._send_embed(ctx, e)

    @tp_group.command(name="runnow")
    async def runnow(self, ctx: commands.Context, which: str = "daily"):
        which = (which or "daily").lower()
        tz = ZoneInfo(NY_TZ)
        ny = datetime.now(tz)
        topn = int((await self.config.guild(ctx.guild).top_n()) or 10)
        if which == "daily":
            e = await self._render_daily_top(ny, topn)
        else:
            e = await self._render_monthly_top(ny, topn)
        await self._send_embed(ctx, e)
