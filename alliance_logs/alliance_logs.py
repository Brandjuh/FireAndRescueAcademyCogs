# alliance_logs.py v0.3.1
from __future__ import annotations

import asyncio
import aiosqlite
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import discord
from redbot.core import commands, checks, Config
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.FARA.AllianceLogs")

DEFAULTS = {
    "main_channel_id": None,
    "mirrors": {},
    "interval_minutes": 5,
    "style": "compact",
    "emoji_titles": True,
    "title_mode": "normalized",
    "colors": {},
    "icons": {},
}

PALETTE = {
    "green": 0x2ECC71,
    "red": 0xE74C3C,
    "orange": 0xE67E22,
    "blue": 0x3498DB,
    "purple": 0x9B59B6,
    "gold": 0xF1C40F,
    "amber": 0xF39C12,
    "grey": 0x95A5A6,
}

DISPLAY = {
    "added_to_alliance": ("Added to the alliance", "green", "✅"),
    "application_denied": ("Application denied", "red", "❌"),
    "left_alliance": ("Left the alliance", "orange", "🚪"),
    "kicked_from_alliance": ("Kicked from the alliance", "red", "🥾"),
    "set_transport_admin": ("Set as transport request admin", "blue", "🚚"),
    "removed_transport_admin": ("Removed as transport request admin", "orange", "🚚❌"),
    "removed_admin": ("Removed as admin", "orange", "🛡️❌"),
    "set_admin": ("Set as admin", "blue", "🛡️"),
    "removed_education_admin": ("Removed as education admin", "orange", "🎓❌"),
    "set_education_admin": ("Set as education admin", "purple", "🎓"),
    "set_finance_admin": ("Set as finance admin", "gold", "💰"),
    "removed_finance_admin": ("Removed as finance admin", "orange", "💰❌"),
    "set_co_admin": ("Set as co admin", "blue", "🤝"),
    "removed_co_admin": ("Removed as co admin", "orange", "🤝❌"),
    "set_mod_action_admin": ("Set as moderator action admin", "blue", "⚙️"),
    "removed_mod_action_admin": ("Removed as moderator action admin", "orange", "⚙️❌"),
    "chat_ban_removed": ("Chat ban removed", "green", "✅"),
    "chat_ban_set": ("Chat ban set", "red", "⛔"),
    "allowed_to_apply": ("Allowed to apply for the alliance", "green", "✅"),
    "not_allowed_to_apply": ("Not allowed to apply for the alliance", "red", "🚫"),
    "created_course": ("Created a course", "purple", "🧑‍🏫"),
    "course_completed": ("Course completed", "green", "🎓✅"),
    "building_destroyed": ("Building destroyed", "red", "💥"),
    "building_constructed": ("Building constructed", "green", "🏗️"),
    "extension_started": ("Extension started", "blue", "⏳"),
    "expansion_finished": ("Expansion finished", "green", "✅"),
    "large_mission_started": ("Large mission started", "amber", "🎯"),
    "alliance_event_started": ("Alliance event started", "amber", "🎪"),
    "set_as_staff": ("Set as staff", "blue", "🧑‍💼"),
    "removed_as_staff": ("Removed as staff", "orange", "🧹"),
    "removed_event_manager": ("Removed as Event Manager", "orange", "🎟️❌"),
    "removed_custom_large_mission": ("Removed custom large scale mission", "orange", "🗑️"),
    "promoted_event_manager": ("Promoted to Event Manager", "green", "🎟️"),
}

def now_utc() -> str:
    return datetime.utcnow().isoformat()

def _humanize_key(key: str) -> str:
    k = (key or "").strip().replace("_", " ")
    return k[:1].upper() + k[1:] if k else "Alliance log"

class AllianceLogs(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA109A14, force_registration=True)
        self.config.register_global(**DEFAULTS)
        self.data_path = cog_data_path(self)
        self.db_path = self.data_path / "state.db"
        self._bg_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        await self._init_db()
        await self._maybe_start_background()
        await self._maybe_migrate_mirrors()

    async def _init_db(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("CREATE TABLE IF NOT EXISTS state(k TEXT PRIMARY KEY, v TEXT)")
            await db.commit()

    async def _maybe_migrate_mirrors(self):
        mirrors = await self.config.mirrors()
        changed = False
        for k, v in list(mirrors.items()):
            if isinstance(v, dict) and "channel_id" in v:
                mirrors[k] = {"enabled": bool(v.get("enabled", True)), "channels": [int(v.get("channel_id"))] if v.get("channel_id") else []}
                changed = True
        if changed:
            await self.config.mirrors.set(mirrors)

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

    async def _build_description(self, row: Dict[str, Any]) -> str:
        lines = []
        by = row.get("executed_name") or "-"
        if row.get("executed_mc_id"):
            url = self._profile_url(str(row["executed_mc_id"]))
            by = f"[{by}]({url})"
            did = await self._discord_id_for_mc(str(row["executed_mc_id"]))
            if did:
                by += f" [[D]]({self._discord_profile_url(did)})"
        lines.append(f"**By:** {by}")
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
            lines.append(f"**Affected:** {aff_text}")
        details = row.get("description") or "-"
        lines.append(f"**Details:** {details}")
        ts = row.get("ts") or "-"
        lines.append(f"**Date:** `{ts}`")
        return "\n".join(lines)

    def _resolve_title_color_icon(self, row: Dict[str, Any], title_mode: str) -> Tuple[str, int, str]:
        key = str(row.get("action_key") or "").lower().strip()
        default_title, color_key, default_icon = DISPLAY.get(key, ("Alliance log", "grey", "ℹ️"))
        if title_mode == "raw":
            raw = str(row.get("action_text") or "").strip().replace("\n", " ")
            title = raw[:200] if raw else default_title
        else:
            title = default_title if key in DISPLAY else _humanize_key(key)
        base_color = PALETTE.get(color_key, PALETTE["grey"])
        return title, base_color, default_icon

    async def _apply_overrides(self, action_key: str, title: str, color: int, icon: str) -> Tuple[str, int, str]:
        key = (action_key or "").lower().strip()
        colors = await self.config.colors()
        icons = await self.config.icons()
        if isinstance(colors.get(key), int):
            color = int(colors[key])
        if icons.get(key):
            icon = str(icons[key])
        return title, color, icon

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
        style = (await self.config.style()).lower()
        emoji_titles = bool(await self.config.emoji_titles())
        title_mode = (await self.config.title_mode()).lower()

        posted = 0
        for row in rows:
            key = str(row.get("action_key") or "").lower().strip()
            title, color, icon = self._resolve_title_color_icon(row, title_mode)
            title, color, icon = await self._apply_overrides(key, title, color, icon)
            title_text = f"{icon} {title}" if emoji_titles and icon else title

            if style == "compact":
                desc = await self._build_description(row)
                e = discord.Embed(title=title_text, description=desc, color=color, timestamp=datetime.utcnow())
            else:
                e = discord.Embed(title=title_text, color=color, timestamp=datetime.utcnow())
                # make it visibly "fields" style
                by = (await self._build_description(row)).split("\n")[0].replace("**By:** ", "")
                e.add_field(name="By", value=by, inline=False)
                if "Affected:" in await self._build_description(row):
                    aff = (await self._build_description(row)).split("\n")[1].replace("**Affected:** ", "")
                else:
                    aff = "-"
                e.add_field(name="Affected", value=aff, inline=False)
                e.add_field(name="Details", value=row.get("description") or "-", inline=False)
                e.add_field(name="Date", value=f"`{row.get('ts') or '-'}`", inline=False)

            try:
                await main_ch.send(embed=e)
                posted += 1
            except Exception as ex:
                log.warning("Failed to post main embed: %s", ex)
                continue

            m = mirrors.get(key, {})
            if not m or not m.get("enabled"):
                continue
            for cid in m.get("channels") or []:
                mch = guild.get_channel(int(cid))
                if not isinstance(mch, discord.TextChannel):
                    continue
                try:
                    await mch.send(embed=e)
                except Exception as mex:
                    log.debug("Mirror failed to %s: %s", cid, mex)

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

    @commands.group(name="alog")
    @checks.admin_or_permissions(manage_guild=True)
    async def alog_group(self, ctx: commands.Context):
        """AllianceLogs publisher (consumer mode)."""

    @alog_group.command(name="status")
    async def status(self, ctx: commands.Context):
        last_id = await self._get_last_id()
        cfg = await self.config.all()
        mirrors = cfg.get("mirrors", {})
        mirrors_count = sum(len((v or {}).get("channels") or []) for v in mirrors.values())
        await ctx.send("```\n"
                       f"Main channel: {cfg['main_channel_id']}\n"
                       f"Interval minutes: {cfg['interval_minutes']}\n"
                       f"Style: {cfg['style']}  Emoji titles: {cfg['emoji_titles']}  Title mode: {cfg['title_mode']}\n"
                       f"Last seen id: {last_id}\n"
                       f"Mirrors (actions): {len(mirrors)} total mirror channels: {mirrors_count}\n"
                       "```")

    @alog_group.command(name="setchannel")
    async def setchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.main_channel_id.set(int(channel.id))
        await ctx.send(f"Main alliance log channel set to {channel.mention}")

    @alog_group.command(name="setinterval")
    async def setinterval(self, ctx: commands.Context, minutes: int):
        await self.config.interval_minutes.set(max(1, int(minutes)))
        await ctx.send(f"Interval set to {minutes} minute(s)")

    @alog_group.command(name="setstyle")
    async def setstyle(self, ctx: commands.Context, style: str):
        style = style.lower().strip()
        if style not in {"compact", "fields"}:
            await ctx.send("Style must be `compact` or `fields`.")
            return
        await self.config.style.set(style)
        await ctx.send(f"Style set to {style}")

    @alog_group.command(name="setemojititles")
    async def setemojititles(self, ctx: commands.Context, enabled: bool):
        await self.config.emoji_titles.set(bool(enabled))
        await ctx.send(f"Emoji in titles set to {bool(enabled)}")

    @alog_group.command(name="settitlemode")
    async def settitlemode(self, ctx: commands.Context, mode: str):
        mode = mode.lower().strip()
        if mode not in {"normalized", "raw"}:
            await ctx.send("Title mode must be `normalized` or `raw`.")
            return
        await self.config.title_mode.set(mode)
        await ctx.send(f"Title mode set to {mode}")

    @alog_group.command(name="setcolor")
    async def setcolor(self, ctx: commands.Context, action_key: str, hex_color: str):
        try:
            if hex_color.startswith("#"):
                hex_color = hex_color[1:]
            val = int(hex_color, 16)
        except Exception:
            await ctx.send("Invalid hex color. Example: #2ECC71")
            return
        colors = await self.config.colors()
        colors[str(action_key).lower()] = val
        await self.config.colors.set(colors)
        await ctx.send(f"Color for `{action_key}` set to #{val:06X}")

    @alog_group.command(name="seticon")
    async def seticon(self, ctx: commands.Context, action_key: str, *, emoji: str):
        icons = await self.config.icons()
        icons[str(action_key).lower()] = emoji.strip()
        await self.config.icons.set(icons)
        await ctx.send(f"Icon for `{action_key}` set to {emoji}")

    @alog_group.group(name="mirror")
    async def mirror_group(self, ctx: commands.Context):
        """Manage per-action mirror channels."""

    @mirror_group.command(name="add")
    async def mirror_add(self, ctx: commands.Context, action_key: str, channel: discord.TextChannel):
        mirrors = await self.config.mirrors()
        m = mirrors.get(action_key, {"enabled": True, "channels": []})
        if int(channel.id) not in m["channels"]:
            m["channels"].append(int(channel.id))
        m["enabled"] = True
        mirrors[action_key] = m
        await self.config.mirrors.set(mirrors)
        await ctx.send(f"Mirror added for `{action_key}` → {channel.mention} (enabled)")

    @mirror_group.command(name="remove")
    async def mirror_remove(self, ctx: commands.Context, action_key: str, channel: discord.TextChannel):
        mirrors = await self.config.mirrors()
        m = mirrors.get(action_key, {"enabled": True, "channels": []})
        m["channels"] = [cid for cid in m["channels"] if cid != int(channel.id)]
        mirrors[action_key] = m
        await self.config.mirrors.set(mirrors)
        await ctx.send(f"Mirror removed for `{action_key}` from {channel.mention}")

    @mirror_group.command(name="enable")
    async def mirror_enable(self, ctx: commands.Context, action_key: str, enabled: bool):
        mirrors = await self.config.mirrors()
        m = mirrors.get(action_key, {"enabled": bool(enabled), "channels": []})
        m["enabled"] = bool(enabled)
        mirrors[action_key] = m
        await self.config.mirrors.set(mirrors)
        await ctx.send(f"Mirror for `{action_key}` set to enabled={bool(enabled)}")

    @alog_group.command(name="mirrorstatus")
    async def mirrorstatus(self, ctx: commands.Context):
        mirrors = await self.config.mirrors()
        if not mirrors:
            await ctx.send("```\nNo mirrors configured.\n```")
            return
        lines = []
        for k, v in mirrors.items():
            chans = ", ".join(f"<#{cid}>" for cid in (v.get("channels") or []))
            lines.append(f"{k}: enabled={v.get('enabled')} channels=[{chans}]")
        await ctx.send("```\n" + "\n".join(lines) + "\n```")

    @alog_group.command(name="debug")
    async def debug(self, ctx: commands.Context, which: str = "last", n: int = 3):
        sc = self.bot.get_cog("AllianceScraper")
        if not sc or not hasattr(sc, "get_logs_after"):
            await ctx.send("AllianceScraper with get_logs_after not available.")
            return
        try:
            rows = await sc.get_logs_after(0, limit=5000)  # type: ignore
            rows = rows[-max(1, min(n, 10)):] if which == "last" else rows[:max(1, min(n, 10))]
        except Exception as e:
            await ctx.send(f"Debug fetch failed: {e}")
            return
        posted = await self._publish_rows(rows)
        await ctx.send(f"(debug) echoed {posted} item(s) with current formatting.")

    @alog_group.command(name="run")
    async def run(self, ctx: commands.Context):
        n = await self._tick_once()
        await ctx.send(f"Posted {n} new log(s).")

async def setup(bot):
    cog = AllianceLogs(bot)
    await bot.add_cog(cog)
