# top_players.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import List, Optional, Tuple
from calendar import monthrange

import aiosqlite
import discord
from redbot.core import Config, checks, commands
from zoneinfo import ZoneInfo


DEFAULT_DB = str(
    Path.home() / ".local/share/Red-DiscordBot/data/frab/cogs/AllianceScraper/alliance.db"
)

GLOBAL_DEFAULTS = {
    "db_path": DEFAULT_DB,
    "topn_daily": 10,
    "topn_monthly": 10,
    "daily_channel_id": 0,
    "monthly_channel_id": 0,
    "daily_enabled": False,
    "monthly_enabled": False,
    # Daily: 23:50 NY
    "daily_post_h": 23,
    "daily_post_m": 50,
    # Monthly: laatste dag 23:50 NY (we berekenen de dag dynamisch)
    "monthly_post_h": 23,
    "monthly_post_m": 50,
    "last_daily_ymd": "",
    "last_monthly_ym": "",
}

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


@dataclass
class TopRow:
    member_id: str
    name: str
    delta: int


class TopPlayers(commands.Cog):
    """Daily/Monthly Top Players uit members_history. Correcte tijdvensters via NY→UTC conversie."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0x70F10AD1, force_registration=True)
        self.config.register_global(**GLOBAL_DEFAULTS)
        self._bg_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    # ---------------- lifecycle ----------------

    async def cog_load(self):
        self._stop.clear()
        self._bg_task = asyncio.create_task(self._scheduler_loop())

    async def cog_unload(self):
        if self._bg_task:
            self._stop.set()
            try:
                await asyncio.wait_for(self._bg_task, timeout=3)
            except Exception:
                pass
            self._bg_task = None

    # ---------------- helpers: NY→UTC ranges ----------------

    def _ny_day_range_utc(self, d: date) -> Tuple[str, str]:
        # members_history.snapshot_utc is UTC; dus vergelijk in UTC.
        s_ny = datetime.combine(d, time(0, 0), NY)
        e_ny = datetime.combine(d, time(23, 50), NY)
        s_utc = s_ny.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
        e_utc = e_ny.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
        return s_utc, e_utc

    def _ny_month_range_utc(self, year: int, month: int) -> Tuple[str, str]:
        s_ny = datetime(year, month, 1, 0, 0, tzinfo=NY)
        last_day = monthrange(year, month)[1]
        e_ny = datetime(year, month, last_day, 23, 50, tzinfo=NY)
        s_utc = s_ny.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
        e_utc = e_ny.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
        return s_utc, e_utc

    async def _get_db_path(self) -> Path:
        return Path((await self.config.db_path()) or DEFAULT_DB).expanduser()

    # ---------------- core SQL ----------------

    async def _compute_top(self, db_path: Path, start_utc: str, end_utc: str, topn: int) -> List[TopRow]:
        """
        Delta per member: (latest ec <= end_utc) - (latest ec < start_utc), beide UTC strings.
        """
        sql = """
WITH members AS (SELECT DISTINCT member_id FROM members_history),
hist AS (
  SELECT member_id,
         COALESCE(earned_credits,0) AS ec,
         substr(replace(COALESCE(snapshot_utc, scraped_at),'T',' '),1,19) AS ts,
         name
  FROM members_history
),
endv AS (
  SELECT m.member_id,
         (SELECT ec FROM hist h WHERE h.member_id=m.member_id AND h.ts <= ? ORDER BY h.ts DESC LIMIT 1) AS endc
  FROM members m
),
startv AS (
  SELECT m.member_id,
         (SELECT ec FROM hist h WHERE h.member_id=m.member_id AND h.ts <  ? ORDER BY h.ts DESC LIMIT 1) AS startc
  FROM members m
),
names AS (
  SELECT member_id, MAX(name) AS name
  FROM hist
  WHERE name <> ''
  GROUP BY member_id
)
SELECT
  m.member_id,
  COALESCE(mc.name, names.name, m.member_id) AS name,
  COALESCE(endv.endc,0)-COALESCE(startv.startc,0) AS delta
FROM members m
LEFT JOIN endv  USING(member_id)
LEFT JOIN startv USING(member_id)
LEFT JOIN members_current mc USING(member_id)
LEFT JOIN names USING(member_id)
WHERE COALESCE(endv.endc,0) > COALESCE(startv.startc,0)
ORDER BY delta DESC, name ASC
LIMIT ?
"""
        out: List[TopRow] = []
        async with aiosqlite.connect(str(db_path)) as db:
            async with db.execute(sql, (end_utc, start_utc, int(topn))) as cur:
                async for mid, name, delta in cur:
                    out.append(TopRow(str(mid), name or str(mid), int(delta or 0)))
        return out

    # ---------------- embed ----------------

    def _embed(self, title: str, subtitle: str, rows: List[TopRow]) -> discord.Embed:
        e = discord.Embed(title=title, description=subtitle)
        if not rows:
            e.add_field(name="Top", value="(geen data gevonden in dit venster)")
            return e

        lines = []
        for i, r in enumerate(rows, 1):
            lines.append(f"**#{i}**  {r.name} — +{r.delta:,}".replace(",", "."))

        value = "\n".join(lines)
        if len(value) <= 1024:
            e.add_field(name="Top", value=value, inline=False)
        else:
            chunk, size = [], 0
            for ln in lines:
                if size + len(ln) + 1 > 1024:
                    e.add_field(name="Top", value="\n".join(chunk), inline=False)
                    chunk, size = [], 0
                chunk.append(ln)
                size += len(ln) + 1
            if chunk:
                e.add_field(name="Top (vervolg)", value="\n".join(chunk), inline=False)
        return e

    # ---------------- posting helpers ----------------

    async def _post_daily(self, when_ny: datetime):
        ch_id = await self.config.daily_channel_id()
        if not ch_id:
            return
        ch = self.bot.get_channel(ch_id)
        if not isinstance(ch, (discord.TextChannel, discord.Thread, discord.ForumChannel)):
            return

        d = when_ny.date()
        start_utc, end_utc = self._ny_day_range_utc(d)
        db = await self._get_db_path()
        topn = await self.config.topn_daily()
        rows = await self._compute_top(db, start_utc, end_utc, topn)
        subtitle = f"{d.isoformat()} (America/New_York) — Top {topn}"
        await ch.send(embed=self._embed("Daily Top Players", subtitle, rows))
        await self.config.last_daily_ymd.set(d.isoformat())

    async def _post_monthly(self, when_ny: datetime):
        ch_id = await self.config.monthly_channel_id()
        if not ch_id:
            return
        ch = self.bot.get_channel(ch_id)
        if not isinstance(ch, (discord.TextChannel, discord.Thread, discord.ForumChannel)):
            return

        y, m = when_ny.year, when_ny.month
        start_utc, end_utc = self._ny_month_range_utc(y, m)
        db = await self._get_db_path()
        topn = await self.config.topn_monthly()
        rows = await self._compute_top(db, start_utc, end_utc, topn)
        subtitle = f"{y}-{m:02d} (America/New_York) — Top {topn}"
        await ch.send(embed=self._embed("Monthly Top Players", subtitle, rows))
        await self.config.last_monthly_ym.set(f"{y}-{m:02d}")

    # ---------------- scheduler ----------------

    async def _scheduler_loop(self):
        await self.bot.wait_until_red_ready()
        while not self._stop.is_set():
            try:
                now_ny = datetime.now(NY)

                # Daily om 23:50 NY
                if await self.config.daily_enabled():
                    dh, dm = await self.config.daily_post_h(), await self.config.daily_post_m()
                    daily_target = now_ny.replace(hour=dh, minute=dm, second=0, microsecond=0)
                    last = await self.config.last_daily_ymd()
                    if now_ny >= daily_target and last != now_ny.date().isoformat():
                        await self._post_daily(now_ny)

                # Monthly op LAATSTE dag 23:50 NY
                if await self.config.monthly_enabled():
                    mh, mm = await self.config.monthly_post_h(), await self.config.monthly_post_m()
                    last_day = monthrange(now_ny.year, now_ny.month)[1]
                    try:
                        monthly_target = now_ny.replace(day=last_day, hour=mh, minute=mm, second=0, microsecond=0)
                    except ValueError:
                        monthly_target = now_ny.replace(day=last_day, hour=mh, minute=mm, second=0, microsecond=0)
                    ym = f"{now_ny.year}-{now_ny.month:02d}"
                    lastm = await self.config.last_monthly_ym()
                    if now_ny >= monthly_target and lastm != ym:
                        await self._post_monthly(now_ny)

            except Exception:
                pass

            try:
                await asyncio.wait_for(self._stop.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass

    # ---------------- commands ----------------

    @commands.group(name="tplayers")
    @checks.is_owner()
    async def tplayers(self, ctx: commands.Context):
        """Top players instellingen en acties."""

    @tplayers.command(name="config")
    async def tp_config(self, ctx: commands.Context):
        db = await self.config.db_path()
        topd = await self.config.topn_daily()
        topm = await self.config.topn_monthly()
        chd = await self.config.daily_channel_id()
        chm = await self.config.monthly_channel_id()
        de  = await self.config.daily_enabled()
        me  = await self.config.monthly_enabled()
        dh, dm = await self.config.daily_post_h(), await self.config.daily_post_m()
        mh, mm = await self.config.monthly_post_h(), await self.config.monthly_post_m()
        lastd = await self.config.last_daily_ymd()
        lastm = await self.config.last_monthly_ym()
        await ctx.send(
            "```\n"
            f"DB path        : {db}\n"
            f"TopN Daily/Mon : {topd} / {topm}\n"
            f"Daily channel  : {chd}\n"
            f"Monthly channel: {chm}\n"
            f"Daily enabled  : {de} (at {dh:02d}:{dm:02d} NY)\n"
            f"Monthly enabled: {me} (last day {mh:02d}:{mm:02d} NY)\n"
            f"Last daily     : {lastd}\n"
            f"Last monthly   : {lastm}\n"
            "```"
        )

    @tplayers.command(name="setdb")
    async def tp_setdb(self, ctx: commands.Context, path: str):
        await self.config.db_path.set(path)
        await ctx.send(f"DB-pad ingesteld op:\n`{path}`")

    @tplayers.group(name="setchannel")
    async def tp_setchannel(self, ctx: commands.Context):
        """Kanaal instellen voor auto-posts."""

    @tp_setchannel.command(name="daily")
    async def tp_setchannel_daily(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.daily_channel_id.set(channel.id)
        await ctx.send(f"Daily kanaal ingesteld op {channel.mention}")

    @tp_setchannel.command(name="monthly")
    async def tp_setchannel_monthly(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.monthly_channel_id.set(channel.id)
        await ctx.send(f"Monthly kanaal ingesteld op {channel.mention}")

    @tplayers.group(name="settopn")
    async def tp_settopn(self, ctx: commands.Context):
        """TopN instellen."""

    @tp_settopn.command(name="daily")
    async def tp_settopn_daily(self, ctx: commands.Context, n: int):
        await self.config.topn_daily.set(max(1, min(50, int(n))))
        await ctx.send(f"TopN daily = {await self.config.topn_daily()}")

    @tp_settopn.command(name="monthly")
    async def tp_settopn_monthly(self, ctx: commands.Context, n: int):
        await self.config.topn_monthly.set(max(1, min(50, int(n))))
        await ctx.send(f"TopN monthly = {await self.config.topn_monthly()}")

    @tplayers.group(name="enable")
    async def tp_enable(self, ctx: commands.Context):
        """Auto-posts aan/uit."""

    @tp_enable.command(name="daily")
    async def tp_enable_daily(self, ctx: commands.Context, value: str):
        v = value.lower() in ("on", "true", "1", "yes", "y")
        await self.config.daily_enabled.set(v)
        await ctx.send(f"Daily auto-post: {v}")

    @tp_enable.command(name="monthly")
    async def tp_enable_monthly(self, ctx: commands.Context, value: str):
        v = value.lower() in ("on", "true", "1", "yes", "y")
        await self.config.monthly_enabled.set(v)
        await ctx.send(f"Monthly auto-post: {v}")

    @tplayers.group(name="preview")
    async def tp_preview(self, ctx: commands.Context):
        """Voorbeeld / test-run zonder te posten in kanaal."""

    @tp_preview.command(name="daily")
    async def preview_daily(self, ctx: commands.Context, ymd: Optional[str] = None, topn: Optional[int] = None):
        d = datetime.now(NY).date() if not ymd else date(*map(int, ymd.split("-")))
        s_utc, e_utc = self._ny_day_range_utc(d)
        db = await self._get_db_path()
        if topn is None:
            topn = await self.config.topn_daily()
        msg = await ctx.send(f"⏳ Berekenen daily top voor **{d.isoformat()}** …")
        rows = await self._compute_top(db, s_utc, e_utc, topn)
        subtitle = f"{d.isoformat()} (America/New_York) — Top {topn}"
        await msg.edit(content="Klaar.", embed=self._embed("Daily Top Players", subtitle, rows))

    @tp_preview.command(name="monthly")
    async def preview_monthly(self, ctx: commands.Context, ym: Optional[str] = None, topn: Optional[int] = None):
        now_ny = datetime.now(NY)
        y, m = (now_ny.year, now_ny.month) if not ym else tuple(map(int, ym.split("-")))
        s_utc, e_utc = self._ny_month_range_utc(y, m)
        db = await self._get_db_path()
        if topn is None:
            topn = await self.config.topn_monthly()
        msg = await ctx.send(f"⏳ Berekenen monthly top voor **{y}-{m:02d}** …")
        rows = await self._compute_top(db, s_utc, e_utc, topn)
        subtitle = f"{y}-{m:02d} (America/New_York) — Top {topn}"
        await msg.edit(content="Klaar.", embed=self._embed("Monthly Top Players", subtitle, rows))

    @tplayers.group(name="run")
    async def tp_run(self, ctx: commands.Context):
        """Post direct in kanaal (zonder te wachten op scheduler)."""

    @tp_run.command(name="daily")
    async def run_daily(self, ctx: commands.Context, ymd: Optional[str] = None):
        d = datetime.now(NY).date() if not ymd else date(*map(int, ymd.split("-")))
        when = datetime(d.year, d.month, d.day, 23, 50, tzinfo=NY)
        await self._post_daily(when)
        await ctx.tick()

    @tp_run.command(name="monthly")
    async def run_monthly(self, ctx: commands.Context, ym: Optional[str] = None):
        now_ny = datetime.now(NY)
        if ym:
            y, m = map(int, ym.split("-"))
            last_day = monthrange(y, m)[1]
            when = datetime(y, m, last_day, 23, 50, tzinfo=NY)
        else:
            y, m = now_ny.year, now_ny.month
            last_day = monthrange(y, m)[1]
            when = datetime(y, m, last_day, 23, 50, tzinfo=NY)
        await self._post_monthly(when)
        await ctx.tick()

    @tplayers.group(name="debug")
    async def tp_debug(self, ctx: commands.Context):
        """Debuginfo over vensters en tellingen."""

    @tp_debug.command(name="daily")
    async def debug_daily(self, ctx: commands.Context, ymd: Optional[str] = None):
        d = datetime.now(NY).date() if not ymd else date(*map(int, ymd.split("-")))
        s_utc, e_utc = self._ny_day_range_utc(d)
        db = await self._get_db_path()
        # Tel snapshots (UTC vergelijkingen)
        sql_counts = """
WITH hist AS (
  SELECT substr(replace(COALESCE(snapshot_utc, scraped_at),'T',' '),1,19) AS ts,
         member_id
  FROM members_history
)
SELECT
  (SELECT COUNT(DISTINCT member_id) FROM hist WHERE ts <= ?) AS le_end,
  (SELECT COUNT(DISTINCT member_id) FROM hist WHERE ts <  ?) AS lt_start,
  (SELECT COUNT(*) FROM hist WHERE ts >= ? AND ts <= ?)     AS rows_in_window
"""
        async with aiosqlite.connect(str(db)) as adb:
            async with adb.execute(sql_counts, (e_utc, s_utc, s_utc, e_utc)) as cur:
                le_end, lt_start, rows_in_window = await cur.fetchone()

        # NY-labels + UTC-vensters tonen
        await ctx.send(
            "```\n"
            f"NY dag: {d.isoformat()} (America/New_York)\n"
            f"UTC START={s_utc}  UTC END={e_utc}\n"
            f"snapshots ≤END: {le_end}   snapshots <START: {lt_start}   rows_in_window: {rows_in_window}\n"
            "```"
        )

    @tp_debug.command(name="monthly")
    async def debug_monthly(self, ctx: commands.Context, ym: Optional[str] = None):
        now_ny = datetime.now(NY)
        y, m = (now_ny.year, now_ny.month) if not ym else tuple(map(int, ym.split("-")))
        s_utc, e_utc = self._ny_month_range_utc(y, m)
        db = await self._get_db_path()
        sql_counts = """
WITH hist AS (
  SELECT substr(replace(COALESCE(snapshot_utc, scraped_at),'T',' '),1,19) AS ts,
         member_id
  FROM members_history
)
SELECT
  (SELECT COUNT(DISTINCT member_id) FROM hist WHERE ts <= ?) AS le_end,
  (SELECT COUNT(DISTINCT member_id) FROM hist WHERE ts <  ?) AS lt_start,
  (SELECT COUNT(*) FROM hist WHERE ts >= ? AND ts <= ?)     AS rows_in_window
"""
        async with aiosqlite.connect(str(db)) as adb:
            async with adb.execute(sql_counts, (e_utc, s_utc, s_utc, e_utc)) as cur:
                le_end, lt_start, rows_in_window = await cur.fetchone()

        await ctx.send(
            "```\n"
            f"NY maand: {y}-{m:02d} (America/New_York)\n"
            f"UTC START={s_utc}  UTC END={e_utc}\n"
            f"snapshots ≤END: {le_end}   snapshots <START: {lt_start}   rows_in_window: {rows_in_window}\n"
            "```"
        )


async def setup(bot):
    await bot.add_cog(TopPlayers(bot))
