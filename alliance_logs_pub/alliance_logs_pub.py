# alliance_logs_pub.py v0.4.3
from __future__ import annotations

import asyncio
import aiosqlite
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import discord
from redbot.core import commands, checks, Config
from redbot.core.data_manager import cog_data_path

__version__ = "0.4.3"

log = logging.getLogger("red.FARA.AllianceLogsPub")

NL = "\n"

DEFAULTS = {
    "main_channel_id": None,
    "mirrors": {},
    "interval_minutes": 5,
    "style": "minimal",
    "emoji_titles": True,
    "strict_titles": True,
    "show_executor_minimal": False,
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
    "set_transport_request_admin": ("Set as Transport request admin", "blue", "🚚"),
    "removed_transport_request_admin": ("Removed as Transport request admin", "orange", "🚚❌"),
    "removed_as_admin": ("Removed as admin", "orange", "🛡️❌"),
    "set_as_admin": ("Set as admin", "blue", "🛡️"),
    "removed_as_education_admin": ("Removed as Education admin", "orange", "🎓❌"),
    "set_as_education_admin": ("Set as Education Admin", "purple", "🎓"),
    "set_as_finance_admin": ("Set as Finance Admin", "gold", "💰"),
    "removed_as_finance_admin": ("Removed as Finance Admin", "orange", "💰❌"),
    "set_as_co_admin": ("Set as Co Admin", "blue", "🤝"),
    "removed_as_co_admin": ("Removed as Co Admin", "orange", "🤝❌"),
    "set_as_moderator_action_admin": ("Set as Moderator action admin", "blue", "⚙️"),
    "removed_as_moderator_action_admin": ("Removed as Moderator action admin", "orange", "⚙️❌"),
    "chat_ban_removed": ("Chat ban removed", "green", "✅"),
    "chat_ban_set": ("Chat ban set", "red", "⛔"),
    "allowed_to_apply": ("Allowed to apply for the alliance", "green", "✅"),
    "not_allowed_to_apply": ("Not allowed to apply for the alliance", "red", "🚫"),
    "created_a_course": ("Created a course", "purple", "🧑‍🏫"),
    "course_completed": ("Course completed", "green", "🎓✅"),
    "building_destroyed": ("Building destroyed", "red", "💥"),
    "building_constructed": ("Building constructed", "green", "🏗️"),
    "extension_started": ("Extension started", "blue", "⏳"),
    "expansion_finished": ("Expansion finished", "green", "✅"),
    "large_mission_started": ("Large mission started", "amber", "🎯"),
    "alliance_event_started": ("Alliance event started", "amber", "🎪"),
    "set_as_staff": ("Set as staff", "blue", "🧑‍💼"),
    "removed_as_staff": ("Removed as staff", "orange", "🧹"),
    "removed_as_event_manager": ("Removed as Event Manager", "orange", "🎟️❌"),
    "removed_custom_large_scale_mission": ("Removed custom large scale mission", "orange", "🗑️"),
    "promoted_to_event_manager": ("Promoted to Event Manager", "green", "🎟️"),
}

TITLE_TO_KEY = {v[0].lower(): k for k, v in DISPLAY.items()}

def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def _map_user_action_input(s: str) -> str:
    if not s:
        return ""
    raw = s.strip()
    k = _norm_key(raw)
    if k in DISPLAY:
        return k
    t = raw.lower()
    if t in TITLE_TO_KEY:
        return TITLE_TO_KEY[t]
    nk = _norm_key(raw)
    if nk in DISPLAY:
        return nk
    return ""

def now_utc() -> str:
    return datetime.utcnow().isoformat()

class AllianceLogsPub(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA109AF1, force_registration=True)
        self.config.register_global(**DEFAULTS)
        self.data_path = cog_data_path(self)
        self.db_path = self.data_path / "state_pub.db"
        self._bg_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        await self._init_db()
        await self._maybe_start_background()

    async def _init_db(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("CREATE TABLE IF NOT EXISTS state(k TEXT PRIMARY KEY, v TEXT)")
            await db.commit()

    async def _get_last_id(self) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT v FROM state WHERE k='last_id'")
            row = await cur.fetchone()
            return int(row["v"]) if row else 0

    async def _set_last_id(self, v: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO state(k, v) VALUES('last_id', ?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (str(int(v)),),
            )
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
        except Exception:
            pass
        return None

    def _title_from_row(self, row: Dict[str, Any]) -> Tuple[str, int, str, str]:
        key_raw = str(row.get("action_key") or "")
        key = _map_user_action_input(key_raw) or _map_user_action_input(row.get("action_text") or "")
        if not key:
            return "Alliance log", PALETTE["grey"], "ℹ️", ""
        title, palette_key, emoji = DISPLAY[key]
        color = PALETTE.get(palette_key, PALETTE["grey"])
        return title, color, emoji, key

    async def _desc_minimal(self, row: Dict[str, Any]) -> str:
        lines: List[str] = []
        ts = row.get("ts") or "-"
        lines.append(f"`{ts}` —")
        desc = row.get("description") or row.get("action_text") or "-"
        lines.append(str(desc))
        aff_name = row.get("affected_name") or ""
        aff_url = row.get("affected_url") or ""
        block: Optional[str] = None
        if aff_name or aff_url:
            label = aff_name or "link"
            block = f"→ [{label}]({aff_url})" if aff_url else f"→ {label}"
            if str(row.get("affected_type") or "") == "user" and row.get("affected_mc_id"):
                did = await self._discord_id_for_mc(str(row.get("affected_mc_id")))
                if did:
                    block += f" [[D]]({self._discord_profile_url(did)})"
        else:
            exec_name = row.get("executed_name") or ""
            exec_url = self._profile_url(str(row.get("executed_mc_id"))) if row.get("executed_mc_id") else ""
            if exec_name or exec_url:
                label = exec_name or "profile"
                block = f"→ [{label}]({exec_url})" if exec_url else f"→ {label}"
        if block:
            lines.append(block)
        if await self.config.show_executor_minimal():
            by = row.get("executed_name") or "-"
            if row.get("executed_mc_id"):
                url = self._profile_url(str(row["executed_mc_id"]))
                by = f"[{by}]({url})"
                did = await self._discord_id_for_mc(str(row["executed_mc_id"]))
                if did:
                    by += f" [[D]]({self._discord_profile_url(did)})"
            lines.append(f"*By:* {by}")
        return NL.join(lines)

    async def _desc_compact(self, row: Dict[str, Any]) -> str:
        lines: List[str] = []
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
        details = str(row.get("description") or "-")
        lines.append(f"**Details:** {details}")
        ts = row.get("ts") or "-"
        lines.append(f"**Date:** `{ts}`")
        return NL.join(lines)

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

        posted = 0
        for row in rows:
            title, color, emoji, key = self._title_from_row(row)
            title_text = f"{emoji} {title}" if emoji_titles and emoji else title

            if style == "minimal":
                desc = await self._desc_minimal(row)
                e = discord.Embed(title=title_text, description=desc, color=color, timestamp=datetime.utcnow())
            elif style == "compact":
                desc = await self._desc_compact(row)
                e = discord.Embed(title=title_text, description=desc, color=color, timestamp=datetime.utcnow())
            else:
                e = discord.Embed(title=title_text, color=color, timestamp=datetime.utcnow())
                by = row.get("executed_name") or "-"
                if row.get("executed_mc_id"):
                    url = self._profile_url(str(row["executed_mc_id"]))
                    by = f"[{by}]({url})"
                    did = await self._discord_id_for_mc(str(row["executed_mc_id"]))
                    if did:
                        by += f" [[D]]({self._discord_profile_url(did)})"
                e.add_field(name="By", value=by, inline=False)
                aff_name = row.get("affected_name") or ""
                aff_url = row.get("affected_url") or ""
                aff_value = "-"
                if aff_name or aff_url:
                    acc = f"[{aff_name}]({aff_url})" if aff_url and aff_name else (f"[link]({aff_url})" if aff_url else (aff_name or "link"))
                    if str(row.get("affected_type") or "") == "user" and row.get("affected_mc_id"):
                        did = await self._discord_id_for_mc(str(row["affected_mc_id"]))
                        if did:
                            acc += f" [[D]]({self._discord_profile_url(did)})"
                    aff_value = acc
                e.add_field(name="Affected", value=aff_value, inline=False)
                e.add_field(name="Details", value=str(row.get("description") or "-"), inline=False)
                e.add_field(name="Date", value=f"`{row.get('ts') or '-'}`", inline=False)

            try:
                await main_ch.send(embed=e)
                posted += 1
            except Exception:
                continue

            if key:
                m = mirrors.get(key, {})
                if m and m.get("enabled"):
                    for cid in m.get("channels") or []:
                        mch = guild.get_channel(int(cid))
                        if not isinstance(mch, discord.TextChannel):
                            continue
                        try:
                            await mch.send(embed=e)
                        except Exception:
                            pass

        return posted

    async def _tick_once(self) -> int:
        sc = self.bot.get_cog("AllianceScraper")
        if not sc or not hasattr(sc, "get_logs_after"):
            return 0
        last_id = await self._get_last_id()
        try:
            rows = await sc.get_logs_after(int(last_id), limit=200)  # type: ignore
        except Exception:
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
            except Exception:
                pass
            mins = max(1, int(await self.config.interval_minutes()))
            await asyncio.sleep(mins * 60)

    @commands.group(name="alog")
    @checks.admin_or_permissions(manage_guild=True)
    async def alog_group(self, ctx: commands.Context):
        """AllianceLogs publisher (consumer mode)."""

    @alog_group.command(name="version")
    async def version(self, ctx: commands.Context):
        cfg = await self.config.all()
        await ctx.send(
            "```
"
            f"AllianceLogsPub version: {__version__}
"
            f"Style: {cfg['style']}  Emoji titles: {cfg['emoji_titles']}  Strict titles: {cfg['strict_titles']}
"
            f"Mirrors configured: {len(cfg.get('mirrors', {}))}
"
            "```"
        )

    @alog_group.command(name="listactions")
    async def listactions(self, ctx: commands.Context):
        lines = [f"{t}  —  key: `{k}`" for k, (t, _, _) in DISPLAY.items()]
        msg = "**Valid actions:**
" + NL.join(lines)
        await ctx.send(msg if len(msg) < 1800 else msg[:1800] + "\n...")

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
        if style not in {"minimal", "compact", "fields"}:
            await ctx.send("Style must be `minimal`, `compact`, or `fields`.")
            return
        await self.config.style.set(style)
        await ctx.send(f"Style set to {style}")

    @alog_group.command(name="setemojititles")
    async def setemojititles(self, ctx: commands.Context, enabled: bool):
        await self.config.emoji_titles.set(bool(enabled))
        await ctx.send(f"Emoji in titles set to {bool(enabled)}")

    @alog_group.command(name="resetformat")
    async def resetformat(self, ctx: commands.Context):
        await self.config.style.set(DEFAULTS["style"])
        await self.config.emoji_titles.set(DEFAULTS["emoji_titles"])
        await self.config.strict_titles.set(DEFAULTS["strict_titles"])
        await self.config.show_executor_minimal.set(DEFAULTS["show_executor_minimal"])
        await self.config.colors.set({})
        await self.config.icons.set({})
        await ctx.send("Formatting reset to defaults.")

    @commands.group(name="alogmirror", aliases=["alog_mirror"])
    @checks.admin_or_permissions(manage_guild=True)
    async def mirror_root(self, ctx: commands.Context):
        """Manage per-action mirror channels."""

    @alog_group.group(name="mirror")
    async def mirror_alias_group(self, ctx: commands.Context):
        """Alias group for mirror management."""

    @mirror_root.command(name="add")
    @mirror_alias_group.command(name="add")
    async def mirror_add(self, ctx: commands.Context, action: str, channel: discord.TextChannel):
        key = _map_user_action_input(action)
        if not key:
            await ctx.send("Unknown action. Use `alog listactions` to see valid options.")
            return
        mirrors = await self.config.mirrors()
        m = mirrors.get(key, {"enabled": True, "channels": []})
        if int(channel.id) not in m["channels"]:
            m["channels"].append(int(channel.id))
        m["enabled"] = True
        mirrors[key] = m
        await self.config.mirrors.set(mirrors)
        await ctx.send(f"Mirror added for `{DISPLAY[key][0]}` → {channel.mention} (enabled)")

    @mirror_root.command(name="remove")
    @mirror_alias_group.command(name="remove")
    async def mirror_remove(self, ctx: commands.Context, action: str, channel: discord.TextChannel):
        key = _map_user_action_input(action)
        if not key:
            await ctx.send("Unknown action. Use `alog listactions`.")
            return
        mirrors = await self.config.mirrors()
        m = mirrors.get(key, {"enabled": True, "channels": []})
        m["channels"] = [cid for cid in m["channels"] if cid != int(channel.id)]
        mirrors[key] = m
        await self.config.mirrors.set(mirrors)
        await ctx.send(f"Mirror removed for `{DISPLAY[key][0]}` from {channel.mention}")

    @mirror_root.command(name="enable")
    @mirror_alias_group.command(name="enable")
    async def mirror_enable(self, ctx: commands.Context, action: str, enabled: bool):
        key = _map_user_action_input(action)
        if not key:
            await ctx.send("Unknown action. Use `alog listactions`.")
            return
        mirrors = await self.config.mirrors()
        m = mirrors.get(key, {"enabled": bool(enabled), "channels": []})
        m["enabled"] = bool(enabled)
        mirrors[key] = m
        await self.config.mirrors.set(mirrors)
        await ctx.send(f"Mirror for `{DISPLAY[key][0]}` set to enabled={bool(enabled)}")

    @alog_group.command(name="mirrorstatus")
    async def mirrorstatus(self, ctx: commands.Context):
        mirrors = await self.config.mirrors()
        if not mirrors:
            await ctx.send("```
No mirrors configured.
```")
            return
        lines = []
        for k, v in mirrors.items():
            chans = ", ".join(f"<#{cid}>" for cid in (v.get("channels") or []))
            lines.append(f"{DISPLAY.get(k, ('?', '', ''))[0]}: enabled={v.get('enabled')} channels=[{chans}]")
        await ctx.send("```
" + NL.join(lines) + "
```")

    @alog_group.command(name="testpost")
    async def testpost(self, ctx: commands.Context, *, action: str):
        key = _map_user_action_input(action)
        if not key:
            await ctx.send("Unknown action. Use `alog listactions`.")
            return
        title, color, emoji, _ = self._title_from_row({"action_key": key})
        emoji_titles = await self.config.emoji_titles()
        title_text = f"{emoji} {title}" if emoji_titles and emoji else title
        ch_id = await self.config.main_channel_id()
        if not ch_id:
            await ctx.send("Main channel not set.")
            return
        guild = ctx.guild or (self.bot.guilds[0] if self.bot.guilds else None)
        if not guild:
            await ctx.send("No guild available.")
            return
        ch = guild.get_channel(int(ch_id))
        if not isinstance(ch, discord.TextChannel):
            await ctx.send("Main channel is not a text channel.")
            return
        e = discord.Embed(
            title=title_text,
            description="`01 Oct 06:40` —
Example description
→ [Example link](https://example.com)",
            color=color,
            timestamp=datetime.utcnow(),
        )
        await ch.send(embed=e)
        await ctx.send("Test post sent.")

    @alog_group.command(name="sanity")
    async def sanity(self, ctx: commands.Context):
        cfg = await self.config.all()
        ch = cfg.get("main_channel_id")
        await ctx.send(
            "```
"
            f"AllianceLogsPub {__version__}
"
            f"Main channel: {ch}
"
            f"Style: {cfg['style']}  Emoji titles: {cfg['emoji_titles']}
"
            "```"
        )

    @alog_group.command(name="run")
    async def run(self, ctx: commands.Context):
        n = await self._tick_once()
        await ctx.send(f"Posted {n} new log(s).")

async def setup(bot):
    cog = AllianceLogsPub(bot)
    await bot.add_cog(cog)
