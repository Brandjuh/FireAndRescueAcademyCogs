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

__version__ = "0.6.0"

log = logging.getLogger("red.FARA.AllianceLogsPub")

NL = "\n"

DEFAULTS = {
    "main_channel_id": None,
    "mirrors": {},
    "interval_minutes": 5,
    "style": "minimal",
    "emoji_titles": True,
    "strict_titles": True,
    "show_executor_minimal": True,
    "max_posts_per_run": 50,
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
    "set_as_education_admin": ("Set as Education admin", "blue", "🎓"),
    "set_as_co_admin": ("Set as Co-admin", "blue", "👔"),
    "removed_as_co_admin": ("Removed as Co-admin", "orange", "👔❌"),
    "created_course": ("Started training course", "blue", "📚"),
    "course_completed": ("Completed training course", "green", "🎓"),
    "promoted_to_event_manager": ("Promoted to Event Manager", "green", "🎟️"),
    "contributed_to_alliance": ("Contributed to the alliance", "gold", "💰"),
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
        self._posting_lock = asyncio.Lock()

    async def cog_load(self):
        await self._init_db()
        await self._maybe_start_background()

    async def cog_unload(self):
        if self._bg_task:
            self._bg_task.cancel()

    async def _init_db(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("CREATE TABLE IF NOT EXISTS state(k TEXT PRIMARY KEY, v TEXT)")
            await db.execute("""
            CREATE TABLE IF NOT EXISTS posted_logs(
                log_id INTEGER PRIMARY KEY,
                posted_at TEXT NOT NULL
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
            await db.execute(
                "INSERT INTO state(k, v) VALUES('last_id', ?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (str(int(v)),),
            )
            await db.commit()

    async def _mark_log_posted(self, log_id: int) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            try:
                await db.execute(
                    "INSERT INTO posted_logs(log_id, posted_at) VALUES(?, ?)",
                    (int(log_id), now_utc())
                )
                await db.commit()
                return True
            except aiosqlite.IntegrityError:
                log.warning("Duplicate detected: Log ID %d was already posted", log_id)
                return False

    async def _is_already_posted(self, log_id: int) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT 1 FROM posted_logs WHERE log_id = ?", (int(log_id),))
            return await cur.fetchone() is not None

    def _title_from_row(self, row: Dict[str, Any]) -> Tuple[str, int, str, str]:
        key = str(row.get("action_key") or "")
        if key in DISPLAY:
            title, color_name, emoji = DISPLAY[key]
            color = PALETTE.get(color_name, PALETTE["grey"])
            return title, color, emoji, key
        return "Alliance Activity", PALETTE["grey"], "📋", key

    def _format_timestamp(self, ts_str: str) -> str:
        try:
            dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            return dt.strftime('%d %b %H:%M')
        except:
            return ts_str

    async def _desc_minimal(self, row: Dict[str, Any]) -> str:
        ts = row.get("ts") or "-"
        formatted_ts = self._format_timestamp(ts)
        
        by = row.get("executed_name") or "Unknown"
        show_exec = await self.config.show_executor_minimal()
        
        desc = f"`{formatted_ts}`"
        if show_exec:
            desc += f" — by {by}"
        
        if row.get("description"):
            desc += f"\n{row['description']}"
        
        return desc

    async def _desc_compact(self, row: Dict[str, Any]) -> str:
        lines = []
        ts = row.get("ts") or "-"
        formatted_ts = self._format_timestamp(ts)
        
        by = row.get("executed_name") or "Unknown"
        lines.append(f"`{formatted_ts}` — by {by}")
        
        if row.get("description"):
            lines.append(row["description"])
        
        if row.get("action"):
            lines.append(f"**Action:** {row['action']}")
        
        return NL.join(lines)

    async def _publish_single_log(self, row: Dict[str, Any], main_ch: discord.TextChannel, 
                                   mirrors: Dict, style: str, emoji_titles: bool) -> bool:
        log_id = int(row["id"])
        
        if await self._is_already_posted(log_id):
            log.info("Skipping log ID %d - already posted", log_id)
            return False
        
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
            e.add_field(name="By", value=by, inline=False)
            e.add_field(name="Details", value=str(row.get("description") or "-"), inline=False)

        try:
            await main_ch.send(embed=e)
            
            # Post to mirrors with action filtering
            for mirror_key, mirror_cfg in mirrors.items():
                if not mirror_cfg.get("enabled"):
                    continue
                
                # Check if this action should go to this mirror
                mirror_actions = mirror_cfg.get("actions", [])
                if mirror_actions and key not in mirror_actions:
                    continue  # Skip this mirror for this action
                
                for ch_id_str in mirror_cfg.get("channels", []):
                    try:
                        ch = main_ch.guild.get_channel(int(ch_id_str))
                        if isinstance(ch, discord.TextChannel):
                            await ch.send(embed=e)
                    except Exception as e:
                        log.debug("Failed to post to mirror channel %s: %s", ch_id_str, e)
            
            await self._mark_log_posted(log_id)
            await self._set_last_id(log_id)
            return True
            
        except Exception as e:
            log.exception("Failed to publish log ID %d: %s", log_id, e)
            return False

    async def _tick_once(self) -> int:
        if self._posting_lock.locked():
            log.info("Skipping tick - already posting")
            return 0
        
        async with self._posting_lock:
            sc = self.bot.get_cog("LogsScraper")  # CHANGED FROM AllianceScraper
            if not sc or not hasattr(sc, "get_logs_after"):
                return 0
            
            last_id = await self._get_last_id()
            max_posts = await self.config.max_posts_per_run()
            
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
            
            try:
                rows = await sc.get_logs_after(int(last_id), limit=max_posts)
            except Exception as e:
                log.exception("Failed to fetch logs: %s", e)
                return 0
            
            if not rows:
                return 0
            
            log.info("Fetched %d logs after ID %d", len(rows), last_id)
            
            posted = 0
            for row in rows:
                success = await self._publish_single_log(row, main_ch, mirrors, style, emoji_titles)
                
                if success:
                    posted += 1
                    if (posted % 5) == 0:
                        await asyncio.sleep(1)
                else:
                    log.debug("Failed to post log ID %d, continuing", row["id"])
            
            log.info("Posted %d/%d logs successfully", posted, len(rows))
            return posted

    async def _maybe_start_background(self):
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._bg_loop())

    async def _bg_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                await self._tick_once()
            except asyncio.CancelledError:
                log.info("Background loop cancelled")
                raise
            except Exception as e:
                log.exception("Error in background loop: %s", e)
            
            mins = max(1, int(await self.config.interval_minutes()))
            await asyncio.sleep(mins * 60)

    @commands.group(name="alog")
    @checks.admin_or_permissions(manage_guild=True)
    async def alog_group(self, ctx: commands.Context):
        """AllianceLogs publisher commands"""
        pass

    @alog_group.command(name="status")
    async def status(self, ctx: commands.Context):
        """Show current status and last processed ID"""
        last_id = await self._get_last_id()
        
        sc = self.bot.get_cog("LogsScraper")  # CHANGED FROM AllianceScraper
        scraper_available = sc is not None and hasattr(sc, "get_logs_after")
        
        total_logs = "N/A"
        if scraper_available:
            try:
                import sqlite3
                conn = sqlite3.connect(sc.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*), MAX(log_id) FROM logs")
                row = cursor.fetchone()
                if row:
                    total_logs = f"{row[0]} (max ID: {row[1]})"
                conn.close()
            except Exception as e:
                total_logs = f"Error: {e}"
        
        posted_count = 0
        try:
            async with aiosqlite.connect(self.db_path) as db:
                cur = await db.execute("SELECT COUNT(*) FROM posted_logs")
                row = await cur.fetchone()
                if row:
                    posted_count = row[0]
        except Exception:
            pass
        
        cfg = await self.config.all()
        
        lines = [
            "```",
            "=== AllianceLogsPub Status ===",
            f"Last processed ID: {last_id}",
            f"Total logs in DB: {total_logs}",
            f"Posted logs tracked: {posted_count}",
            f"Scraper available: {scraper_available}",
            f"Max posts per run: {cfg['max_posts_per_run']}",
            f"Interval: {cfg['interval_minutes']} minutes",
            f"Main channel: {cfg.get('main_channel_id')}",
            "```",
        ]
        await ctx.send("\n".join(lines))

    @alog_group.command(name="setlastid")
    async def setlastid(self, ctx: commands.Context, new_id: int):
        """Manually set the last processed ID"""
        old_id = await self._get_last_id()
        await self._set_last_id(int(new_id))
        await ctx.send(f"✅ Updated last_id from {old_id} to {new_id}")
        log.info("Manual last_id update: %d -> %d (by %s)", old_id, new_id, ctx.author)

    @alog_group.command(name="run")
    async def run(self, ctx: commands.Context):
        """Manually trigger a posting run"""
        n = await self._tick_once()
        await ctx.send(f"✅ Posted {n} new log(s).")

    @alog_group.command(name="setchannel")
    async def setchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the main posting channel"""
        await self.config.main_channel_id.set(channel.id)
        await ctx.send(f"✅ Main channel set to {channel.mention}")

    @alog_group.command(name="setinterval")
    async def setinterval(self, ctx: commands.Context, minutes: int):
        """Set posting interval in minutes"""
        if minutes < 1:
            await ctx.send("❌ Interval must be at least 1 minute")
            return
        await self.config.interval_minutes.set(minutes)
        await ctx.send(f"✅ Interval set to {minutes} minutes")

    @alog_group.command(name="setstyle")
    async def setstyle(self, ctx: commands.Context, style: str):
        """Set embed style (minimal/compact/fields)"""
        style = style.lower()
        if style not in ["minimal", "compact", "fields"]:
            await ctx.send("❌ Style must be: minimal, compact, or fields")
            return
        await self.config.style.set(style)
        await ctx.send(f"✅ Style set to: {style}")
    
    @alog_group.command(name="mirrors")
    async def list_mirrors(self, ctx: commands.Context):
        """List all configured mirrors"""
        mirrors = await self.config.mirrors()
        if not mirrors:
            await ctx.send("No mirrors configured")
            return
        
        lines = ["**Configured Mirrors:**"]
        for key, cfg in mirrors.items():
            enabled = "✅" if cfg.get("enabled") else "❌"
            channels = ", ".join([f"<#{ch}>" for ch in cfg.get("channels", [])])
            actions = cfg.get("actions", [])
            action_str = ", ".join(actions) if actions else "all actions"
            lines.append(f"\n**{key}** {enabled}")
            lines.append(f"  Channels: {channels or 'none'}")
            lines.append(f"  Actions: {action_str}")
        
        await ctx.send("\n".join(lines))
    
    @alog_group.command(name="addmirror")
    async def add_mirror(self, ctx: commands.Context, name: str, channel: discord.TextChannel):
        """Add a mirror channel"""
        async with self.config.mirrors() as mirrors:
            if name not in mirrors:
                mirrors[name] = {"enabled": True, "channels": [], "actions": []}
            if channel.id not in mirrors[name]["channels"]:
                mirrors[name]["channels"].append(channel.id)
        await ctx.send(f"✅ Added {channel.mention} to mirror '{name}'")
    
    @alog_group.command(name="removemirror")
    async def remove_mirror(self, ctx: commands.Context, name: str):
        """Remove a mirror entirely"""
        async with self.config.mirrors() as mirrors:
            if name in mirrors:
                del mirrors[name]
                await ctx.send(f"✅ Removed mirror '{name}'")
            else:
                await ctx.send(f"❌ Mirror '{name}' not found")
    
    @alog_group.command(name="setmirroractions")
    async def set_mirror_actions(self, ctx: commands.Context, name: str, *actions: str):
        """Set which actions go to a mirror (leave empty for all)"""
        async with self.config.mirrors() as mirrors:
            if name not in mirrors:
                await ctx.send(f"❌ Mirror '{name}' not found")
                return
            
            valid_actions = []
            for action in actions:
                key = _map_user_action_input(action)
                if key:
                    valid_actions.append(key)
            
            mirrors[name]["actions"] = valid_actions
            
            if valid_actions:
                await ctx.send(f"✅ Mirror '{name}' will receive: {', '.join(valid_actions)}")
            else:
                await ctx.send(f"✅ Mirror '{name}' will receive all actions")
    
    @alog_group.command(name="togglemirror")
    async def toggle_mirror(self, ctx: commands.Context, name: str):
        """Enable/disable a mirror"""
        async with self.config.mirrors() as mirrors:
            if name not in mirrors:
                await ctx.send(f"❌ Mirror '{name}' not found")
                return
            
            mirrors[name]["enabled"] = not mirrors[name].get("enabled", True)
            status = "enabled" if mirrors[name]["enabled"] else "disabled"
            await ctx.send(f"✅ Mirror '{name}' {status}")


async def setup(bot):
    cog = AllianceLogsPub(bot)
    await bot.add_cog(cog)
