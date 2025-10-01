# alliance_logs.py v0.2.0 (consumer-only)
from __future__ import annotations

import asyncio
import aiosqlite
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

import discord
from redbot.core import commands, checks, Config
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.FARA.AllianceLogs")

DEFAULTS = {
    "main_channel_id": None,
    "mirrors": {},  # {action_key: {"channel_id": int, "enabled": bool}}
    "interval_minutes": 5,
}

def now_utc() -> str:
    return datetime.utcnow().isoformat()

class AllianceLogs(commands.Cog):
    """Publish alliance logs already scraped by AllianceScraper; no scraping here."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFAL09A12, force_registration=True)
        self.config.register_global(**DEFAULTS)
        self.data_path = cog_data_path(self)
        self.db_path = self.data_path / "state.db"
        self._bg_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        await self._init_db()
        await self._maybe_start_background()

    async def _init_db(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
            CREATE TABLE IF NOT EXISTS state(
                k TEXT PRIMARY KEY,
                v TEXT
            )
            """)
            await db.commit()

    async def _get_last_id(self) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT v FROM state WHERE k='last_id'")
            row = await cur.fetchone()
            return int(row["v"]) if row else 0

    async def _set_last_id(self, v: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("INSERT INTO state(k, v) VALUES('last_id', ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(int(v)),))
            await db.commit()

    def _profile_url(self, mc_user_id: str) -> Optional[str]:
        if not mc_user_id:
            return None
        return f"https://www.missionchief.com/users/{mc_user_id}"

    def _discord_profile_url(self, discord_id: int) -> str:
        return f"https://discord.com/users/{discord_id}"

    async def _discord_id_for_mc(self, mc_user_id: str) -> Optional[int]:
        ms = self.bot.get_cog("MemberSync")
        if not ms or not mc_user_id:
            return None
        try:
            link = await ms.get_link_for_mc(str(mc_user_id))
            if link and link.get("status") == "approved":
                return int(link["discord_id"])
        except Exception as e:
            log.debug("MemberSync lookup failed: %s", e)
        return None

    async def _publish_rows(self, rows: List[Dict[str, Any]]) -> int:
        guild = self.bot.guilds[0] if self.bot.guilds else None
        if not guild:
            return 0
        ch_id = await self.config.main_channel_id()
        if not ch_id:
            return 0
        main_ch = guild.get_channel(int(ch_id))
        if not isinstance(main_ch, discord.TextChannel):
            return 0
        mirrors = await self.config.mirrors()
        posted = 0
        for row in rows:
            e = discord.Embed(title=row["action_text"], color=discord.Color.blurple(), timestamp=datetime.utcnow())
            e.add_field(name="Date", value=f"`{row['ts']}`", inline=False)
            exec_text = row["executed_name"]
            if row.get("executed_mc_id"):
                url = self._profile_url(str(row["executed_mc_id"]))
                exec_text = f"[{row['executed_name']}]({url})"
                did = await self._discord_id_for_mc(str(row["executed_mc_id"]))
                if did:
                    exec_text += f" [[D]]({self._discord_profile_url(did)})"
            e.add_field(name="Executed by", value=exec_text, inline=False)
            e.add_field(name="Description", value=row.get("description") or "-", inline=False)
            aff_name = row.get("affected_name") or ""
            aff_url = row.get("affected_url") or ""
            if aff_name or aff_url:
                aff_text = aff_name or "-"
                if aff_url:
                    aff_text = f"[{aff_name}]({aff_url})" if aff_name else f"[link]({aff_url})"
                if str(row.get("affected_type") or "") == "user" and row.get("affected_mc_id"):
                    did = await self._discord_id_for_mc(str(row["affected_mc_id"]))
                    if did:
                        aff_text += f" [[D]]({self._discord_profile_url(did)})"
                e.add_field(name="Affected", value=aff_text, inline=False)
            try:
                msg = await main_ch.send(embed=e)
                posted += 1
            except Exception as ex:
                log.warning("Failed to post main embed: %s", ex)
                continue
            # Mirror
            m = mirrors.get(str(row.get("action_key") or ""))
            if m and m.get("enabled"):
                mch = guild.get_channel(int(m.get("channel_id") or 0))
                if isinstance(mch, discord.TextChannel):
                    e2 = discord.Embed(title=row["action_text"], color=discord.Color.dark_grey(), timestamp=datetime.utcnow())
                    for f in e.fields:
                        e2.add_field(name=f.name, value=f.value, inline=f.inline)
                    try:
                        await mch.send(embed=e2)
                    except Exception as mex:
                        log.debug("Mirror failed: %s", mex)
        return posted

    async def _tick_once(self) -> int:
        sc = self.bot.get_cog("AllianceScraper")
        if not sc or not hasattr(sc, "get_logs_after"):
            log.debug("AllianceScraper with get_logs_after not available")
            return 0
        last_id = await self._get_last_id()
        try:
            rows = await sc.get_logs_after(int(last_id), limit=500)  # type: ignore
        except Exception as e:
            log.debug("get_logs_after failed: %s", e)
            return 0
        if not rows:
            return 0
        posted = await self._publish_rows(rows)
        newest = max((int(r["id"]) for r in rows), default=last_id)
        if posted > 0:
            await self._set_last_id(newest)
        return posted

    async def _maybe_start_background(self):
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._bg_loop())

    async def _bg_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                await self._tick_once()
            except Exception as e:
                log.warning("AllianceLogs tick error: %s", e)
            mins = max(1, int(await self.config.interval_minutes()))
            await asyncio.sleep(mins * 60)

    # -------------- Commands --------------
    @commands.group(name="alog")
    @checks.admin_or_permissions(manage_guild=True)
    async def alog_group(self, ctx: commands.Context):
        """AllianceLogs publisher (consumer mode)."""

    @alog_group.command(name="status")
    async def status(self, ctx: commands.Context):
        last_id = await self._get_last_id()
        cfg = await self.config.all()
        await ctx.send("```\n"
                       f"Mode: consumer\n"
                       f"Main channel: {cfg['main_channel_id']}\n"
                       f"Interval minutes: {cfg['interval_minutes']}\n"
                       f"Last seen id: {last_id}\n"
                       "```")

    @alog_group.command(name="setchannel")
    async def setchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.main_channel_id.set(int(channel.id))
        await ctx.send(f"Main alliance log channel set to {channel.mention}")

    @alog_group.command(name="setinterval")
    async def setinterval(self, ctx: commands.Context, minutes: int):
        await self.config.interval_minutes.set(max(1, int(minutes)))
        await ctx.send(f"Interval set to {minutes} minute(s)")

    @alog_group.command(name="setmirror")
    async def setmirror(self, ctx: commands.Context, action_key: str, channel: discord.TextChannel, enabled: bool):
        mirrors = await self.config.mirrors()
        mirrors[str(action_key)] = {"channel_id": int(channel.id), "enabled": bool(enabled)}
        await self.config.mirrors.set(mirrors)
        await ctx.send(f"Mirror for `{action_key}` set to {channel.mention} (enabled={bool(enabled)})")

    @alog_group.command(name="mirrorstatus")
    async def mirrorstatus(self, ctx: commands.Context):
        mirrors = await self.config.mirrors()
        lines = []
        for k, v in mirrors.items():
            lines.append(f"{k}: channel={v.get('channel_id')} enabled={v.get('enabled')}")
        await ctx.send("```\n" + ("\n".join(lines) if lines else "No mirrors configured.") + "\n```")

    @alog_group.command(name="run")
    async def run(self, ctx: commands.Context):
        n = await self._tick_once()
        await ctx.send(f"Posted {n} new log(s).")

    @alog_group.command(name="setlastid")
    async def setlastid(self, ctx: commands.Context, last_id: int):
        await self._set_last_id(int(last_id))
        await ctx.send(f"Last seen id set to {last_id}")

async def setup(bot):
    cog = AllianceLogs(bot)
    await bot.add_cog(cog)
