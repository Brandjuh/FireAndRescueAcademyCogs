\
import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from typing import List, Optional, Tuple

import discord
from redbot.core import commands, Config
from zoneinfo import ZoneInfo

__version__ = "0.2.0"

log = logging.getLogger("red.FARA.TopPlayers")

NY_TZ = "America/New_York"

@dataclass
class PlayerDelta:
    member_id: str
    name: str
    delta: int
    mc_user_id: Optional[str]
    profile_href: Optional[str]
    discord_id: Optional[str]

class TopPlayers(commands.Cog):
    """Daily & Monthly Top Players from alliance.db snapshots."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0x70F10AD1, force_registration=True)
        defaults = {
            "channels": {"daily": None, "monthly": None},
            "times": {"daily": "23:50", "monthly": "23:50"},
            "db_path": "/home/brand/.local/share/Red-DiscordBot/data/frab/cogs/AllianceScraper/alliance.db",
            "top_n": 10,
            "last_published": {"daily": None, "monthly": None},
        }
        self.config.register_guild(**defaults)
        self._bg_task = None

    # ---------- helpers ----------
    def _connect(self, guild_id: int) -> sqlite3.Connection:
        # One connection per call is fine; queries are short.
        # Row access by name
        con = sqlite3.connect(self._db_path_for(guild_id))
        con.row_factory = sqlite3.Row
        return con

    def _db_path_for(self, guild_id: int) -> str:
        # stored per-guild; fallback to default
        # (we store string in guild config)
        return self._guild_cache.get(guild_id, {}).get("db_path") or \
               "/home/brand/.local/share/Red-DiscordBot/data/frab/cogs/AllianceScraper/alliance.db"

    @staticmethod
    def _ny_now() -> datetime:
        return datetime.now(ZoneInfo(NY_TZ))

    @staticmethod
    def _ny_day_from_str(s: Optional[str]) -> datetime:
        if s:
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=ZoneInfo(NY_TZ))
        return TopPlayers._ny_now()

    @staticmethod
    def _ym_from_str(s: Optional[str]) -> Tuple[int, int]:
        if s:
            dt = datetime.strptime(s, "%Y-%m").replace(tzinfo=ZoneInfo(NY_TZ))
        else:
            dt = TopPlayers._ny_now()
        return dt.year, dt.month

    @staticmethod
    def _utc_window_for_day_ny(ny_dt: datetime) -> Tuple[str, str]:
        tz = ZoneInfo(NY_TZ)
        ny = ny_dt.astimezone(tz)
        start = ny.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(ZoneInfo("UTC"))
        end = ny.replace(hour=23, minute=50, second=0, microsecond=0).astimezone(ZoneInfo("UTC"))
        return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _utc_window_for_month_ny(ny_dt: datetime) -> Tuple[str, str]:
        tz = ZoneInfo(NY_TZ)
        ny = ny_dt.astimezone(tz).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # first day next month in NY, then -10 minutes (23:50 of last day)
        if ny.month == 12:
            next_month = ny.replace(year=ny.year + 1, month=1)
        else:
            next_month = ny.replace(month=ny.month + 1)
        end_ny = (next_month - timedelta(minutes=10))
        start = ny.astimezone(ZoneInfo("UTC"))
        end = end_ny.astimezone(ZoneInfo("UTC"))
        return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _mc_link(mc_user_id: Optional[str], href: Optional[str]) -> Optional[str]:
        if href:
            return href
        if mc_user_id:
            return f"https://www.missionchief.com/profile/{mc_user_id}"
        return None

    # ---------- DB logic ----------
    def _query_top_players(self, guild_id: int, start_utc: str, end_utc: str, top_n: int) -> List[PlayerDelta]:
        sql = """
        WITH rng(start,end) AS (VALUES (?, ?)),
        hist AS (
          SELECT member_id, earned_credits, substr(replace(COALESCE(snapshot_utc,''),'T',' '),1,19) AS ts
          FROM members_history
        ),
        before AS (
          SELECT h.member_id, h.earned_credits AS ec_before
          FROM hist h
          JOIN (SELECT member_id, MAX(ts) AS max_ts FROM hist, rng WHERE ts < start GROUP BY member_id) mx
            ON mx.member_id = h.member_id AND mx.max_ts = h.ts
        ),
        after AS (
          SELECT h.member_id, h.earned_credits AS ec_after
          FROM hist h
          JOIN (SELECT member_id, MAX(ts) AS max_ts FROM hist, rng WHERE ts <= end GROUP BY member_id) mx
            ON mx.member_id = h.member_id AND mx.max_ts = h.ts
        ),
        delta AS (
          SELECT a.member_id, COALESCE(a.ec_after,0) - COALESCE(b.ec_before,0) AS delta
          FROM after a
          LEFT JOIN before b ON b.member_id = a.member_id
        )
        SELECT d.member_id, d.delta,
               m.name, m.profile_href, m.mc_user_id, m.user_id
        FROM delta d
        LEFT JOIN members_current m ON m.member_id = d.member_id
        WHERE d.delta > 0
        ORDER BY d.delta DESC, m.name COLLATE NOCASE ASC
        LIMIT ?;
        """

        con = self._connect(guild_id)
        try:
            cur = con.execute(sql, (start_utc, end_utc, int(top_n)))
            rows = cur.fetchall()
            out: List[PlayerDelta] = []
            for r in rows:
                out.append(PlayerDelta(
                    member_id=r["member_id"],
                    name=r["name"] or "(unknown)",
                    delta=int(r["delta"] or 0),
                    mc_user_id=r["mc_user_id"],
                    profile_href=r["profile_href"],
                    discord_id=r["user_id"],
                ))
            return out
        finally:
            con.close()

    # ---------- render ----------
    @staticmethod
    def _lines_for(players: List[PlayerDelta]) -> List[str]:
        lines = []
        for i, p in enumerate(players, start=1):
            link = TopPlayers._mc_link(p.mc_user_id, p.profile_href)
            name = p.name or "(unknown)"
            if link:
                name_txt = f"[{name}]({link})"
            else:
                name_txt = name
            dtag = f" [D: <@{p.discord_id}>]" if p.discord_id else ""
            lines.append(f"**{i}.** {name_txt}{dtag} — **+{p.delta:,}**")
        return lines

    def _embed_for_daily(self, guild: discord.Guild, day_ny: datetime, players: List[PlayerDelta]) -> discord.Embed:
        day_str = day_ny.strftime("%Y-%m-%d")
        e = discord.Embed(
            title="Daily Top Players",
            description=f"`{day_str}` (America/New_York)",
        )
        e.set_footer(text=f"Top {len(players)} • Generated by TopPlayers {__version__}")
        body = "\n".join(self._lines_for(players)) or "_No data_"
        # keep it simple: description only (max 4096)
        if len(body) <= 4096:
            e.description += "\n\n" + body
        else:
            # extremely unlikely for top 10, but split if needed
            e.add_field(name="Leaders", value=body[:1024], inline=False)
            rest = body[1024:2048]
            if rest:
                e.add_field(name="\u200b", value=rest[:1024], inline=False)
        return e

    def _embed_for_monthly(self, guild: discord.Guild, ym_ny: datetime, players: List[PlayerDelta]) -> discord.Embed:
        e = discord.Embed(
            title="Monthly Top Players",
            description=f"`{ym_ny.strftime('%Y-%m')}` (America/New_York)",
        )
        e.set_footer(text=f"Top {len(players)} • Generated by TopPlayers {__version__}")
        body = "\n".join(self._lines_for(players)) or "_No data_"
        if len(body) <= 4096:
            e.description += "\n\n" + body
        else:
            e.add_field(name="Leaders", value=body[:1024], inline=False)
            rest = body[1024:2048]
            if rest:
                e.add_field(name="\u200b", value=rest[:1024], inline=False)
        return e

    async def _post_to_channel(self, channel_id: Optional[int], embed: discord.Embed) -> bool:
        if not channel_id:
            return False
        ch = self.bot.get_channel(int(channel_id))
        if not ch or not isinstance(ch, (discord.TextChannel, discord.Thread)):
            return False
        try:
            await ch.send(embed=embed)
            return True
        except Exception:
            log.exception("Failed to send embed to channel %s", channel_id)
            return False

    # ---------- scheduler ----------
    def _next_fire_after(self, tzname: str, hhmm: str, now: Optional[datetime] = None) -> datetime:
        tz = ZoneInfo(tzname)
        now = (now or datetime.now(tz)).astimezone(tz).replace(second=0, microsecond=0)
        try:
            h, m = map(int, hhmm.split(":"))
        except Exception:
            h, m = 23, 50
        candidate = now.replace(hour=h, minute=m)
        if candidate <= now:
            candidate += timedelta(days=1)
        return candidate

    async def _bg_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                # we scan all guilds where this cog is loaded
                for guild in self.bot.guilds:
                    cfg = await self.config.guild(guild).all()
                    # keep a small local cache
                    self._guild_cache[guild.id] = cfg
                    t_daily = cfg["times"]["daily"]
                    t_month = cfg["times"]["monthly"]
                    nd = self._next_fire_after(NY_TZ, t_daily, self._ny_now())
                    nm = self._next_fire_after(NY_TZ, t_month, self._ny_now())

                    now_ny = self._ny_now()
                    # Fire when in window ±2min to be robust
                    if abs((now_ny - nd).total_seconds()) <= 120 and cfg["channels"]["daily"]:
                        start, end = self._utc_window_for_day_ny(now_ny)
                        top = self._query_top_players(guild.id, start, end, cfg["top_n"])
                        e = self._embed_for_daily(guild, now_ny, top)
                        await self._post_to_channel(cfg["channels"]["daily"], e)
                    if abs((now_ny - nm).total_seconds()) <= 120 and cfg["channels"]["monthly"]:
                        start, end = self._utc_window_for_month_ny(now_ny)
                        top = self._query_top_players(guild.id, start, end, cfg["top_n"])
                        e = self._embed_for_monthly(guild, now_ny, top)
                        await self._post_to_channel(cfg["channels"]["monthly"], e)
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("TopPlayers background loop error")
                await asyncio.sleep(30)

    async def cog_load(self):
        # small local cache of config values to avoid disk on every query
        self._guild_cache = {}
        for g in self.bot.guilds:
            self._guild_cache[g.id] = await self.config.guild(g).all()
        if not self._bg_task:
            self._bg_task = asyncio.create_task(self._bg_loop())

    async def cog_unload(self):
        if self._bg_task:
            self._bg_task.cancel()
            self._bg_task = None

    # ---------- commands ----------
    @commands.group(name="tplayers")
    @commands.guild_only()
    async def tplayers_group(self, ctx: commands.Context):
        """TopPlayers configuration and previews."""

    @tplayers_group.command(name="version")
    async def version(self, ctx: commands.Context):
        await ctx.send(f"TopPlayers v{__version__}")

    @tplayers_group.command(name="status")
    async def status(self, ctx: commands.Context):
        cfg = await self.config.guild(ctx.guild).all()
        lines = [
            f"DB: `{cfg['db_path']}`",
            f"Channels: daily={cfg['channels']['daily']} monthly={cfg['channels']['monthly']}",
            f"Times (NY): daily={cfg['times']['daily']} monthly={cfg['times']['monthly']}",
            f"TopN: {cfg['top_n']}",
        ]
        await ctx.send("\n".join(lines))

    @tplayers_group.command(name="setdb")
    @commands.admin_or_permissions(manage_guild=True)
    async def setdb(self, ctx: commands.Context, path: str):
        await self.config.guild(ctx.guild).db_path.set(path)
        self._guild_cache.setdefault(ctx.guild.id, {})["db_path"] = path
        await ctx.send(f"DB path set to `{path}`")

    @tplayers_group.command(name="setchannel")
    @commands.admin_or_permissions(manage_guild=True)
    async def setchannel(self, ctx: commands.Context, which: str, channel: discord.TextChannel):
        which = which.lower()
        if which not in {"daily","monthly"}:
            await ctx.send("Which must be `daily` or `monthly`.")
            return
        cfg = await self.config.guild(ctx.guild).all()
        cfg["channels"][which] = channel.id
        await self.config.guild(ctx.guild).channels.set(cfg["channels"])
        self._guild_cache.setdefault(ctx.guild.id, {}).setdefault("channels", {})[which] = channel.id
        await ctx.send(f"{which.capitalize()} channel set to {channel.mention}")

    @tplayers_group.command(name="settime")
    @commands.admin_or_permissions(manage_guild=True)
    async def settime(self, ctx: commands.Context, which: str, hhmm: str):
        which = which.lower()
        if which not in {"daily","monthly"}:
            await ctx.send("Which must be `daily` or `monthly`.")
            return
        try:
            h, m = map(int, hhmm.split(":"))
            assert 0 <= h < 24 and 0 <= m < 60
        except Exception:
            await ctx.send("Time must be `HH:MM` (NY time).")
            return
        cfg = await self.config.guild(ctx.guild).all()
        cfg["times"][which] = hhmm
        await self.config.guild(ctx.guild).times.set(cfg["times"])
        self._guild_cache.setdefault(ctx.guild.id, {}).setdefault("times", {})[which] = hhmm
        await ctx.send(f"{which.capitalize()} time set to {hhmm} (NY).")

    @tplayers_group.command(name="settopn")
    @commands.admin_or_permissions(manage_guild=True)
    async def settopn(self, ctx: commands.Context, n: int):
        await self.config.guild(ctx.guild).top_n.set(max(1, min(25, int(n))))
        self._guild_cache.setdefault(ctx.guild.id, {})["top_n"] = max(1, min(25, int(n)))
        await ctx.send(f"TopN set to {max(1, min(25, int(n)))}")

    @tplayers_group.group(name="preview")
    async def preview(self, ctx: commands.Context):
        """Preview commands."""

    @preview.command(name="daily")
    async def preview_daily(self, ctx: commands.Context, day: Optional[str] = None, topn: Optional[int] = None):
        ny_day = self._ny_day_from_str(day)
        start, end = self._utc_window_for_day_ny(ny_day)
        cfg = await self.config.guild(ctx.guild).all()
        players = self._query_top_players(ctx.guild.id, start, end, topn or cfg["top_n"])
        e = self._embed_for_daily(ctx.guild, ny_day, players)
        await ctx.send(embed=e)

    @preview.command(name="monthly")
    async def preview_monthly(self, ctx: commands.Context, ym: Optional[str] = None, topn: Optional[int] = None):
        y, m = self._ym_from_str(ym)
        ny_dt = datetime(y, m, 1, tzinfo=ZoneInfo(NY_TZ))
        start, end = self._utc_window_for_month_ny(ny_dt)
        cfg = await self.config.guild(ctx.guild).all()
        players = self._query_top_players(ctx.guild.id, start, end, topn or cfg["top_n"])
        e = self._embed_for_monthly(ctx.guild, ny_dt, players)
        await ctx.send(embed=e)

    @tplayers_group.command(name="runnow")
    async def runnow(self, ctx: commands.Context, which: str = "daily"):
        which = which.lower()
        cfg = await self.config.guild(ctx.guild).all()
        if which == "monthly":
            now_ny = self._ny_now()
            start, end = self._utc_window_for_month_ny(now_ny)
            players = self._query_top_players(ctx.guild.id, start, end, cfg["top_n"])
            e = self._embed_for_monthly(ctx.guild, now_ny, players)
            ok = await self._post_to_channel(cfg["channels"]["monthly"], e)
            await ctx.send(f"Monthly posted: {ok}")
        else:
            now_ny = self._ny_now()
            start, end = self._utc_window_for_day_ny(now_ny)
            players = self._query_top_players(ctx.guild.id, start, end, cfg["top_n"])
            e = self._embed_for_daily(ctx.guild, now_ny, players)
            ok = await self._post_to_channel(cfg["channels"]["daily"], e)
            await ctx.send(f"Daily posted: {ok}")
