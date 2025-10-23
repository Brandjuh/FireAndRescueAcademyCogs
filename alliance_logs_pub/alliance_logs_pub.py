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

__version__ = "0.6.1"

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
    "set_transport_admin": ("Set as Transport admin", "blue", "🚚"),
    "removed_transport_admin": ("Removed as Transport admin", "orange", "🚚❌"),
    "removed_admin": ("Removed as admin", "orange", "🛡️❌"),
    "set_admin": ("Set as admin", "blue", "🛡️"),
    "removed_education_admin": ("Removed as Education admin", "orange", "🎓❌"),
    "set_education_admin": ("Set as Education admin", "blue", "🎓"),
    "set_finance_admin": ("Set as Finance admin", "blue", "💰"),
    "removed_finance_admin": ("Removed as Finance admin", "orange", "💰❌"),
    "set_co_admin": ("Set as Co-Admin", "blue", "👑"),
    "removed_co_admin": ("Removed as Co-Admin", "orange", "👑❌"),
    "set_mod_action_admin": ("Set as Mod Action admin", "blue", "⚖️"),
    "removed_mod_action_admin": ("Removed as Mod Action admin", "orange", "⚖️❌"),
    "chat_ban_removed": ("Chat ban removed", "green", "💬✅"),
    "chat_ban_set": ("Chat ban set", "red", "💬❌"),
    "allowed_to_apply": ("Allowed to apply", "green", "📝✅"),
    "not_allowed_to_apply": ("Not allowed to apply", "red", "📝❌"),
    "created_course": ("Created a course", "blue", "📚"),
    "course_completed": ("Course completed", "green", "🎓✅"),
    "building_destroyed": ("Building destroyed", "red", "🏢💥"),
    "building_constructed": ("Building constructed", "green", "🏢"),
    "extension_started": ("Extension started", "blue", "🔨"),
    "expansion_finished": ("Expansion finished", "green", "✅"),
    "large_mission_started": ("Large scale mission started", "purple", "🚨"),
    "alliance_event_started": ("Alliance event started", "purple", "🎉"),
    "set_as_staff": ("Set as staff", "blue", "⭐"),
    "removed_as_staff": ("Removed as staff", "orange", "⭐❌"),
    "removed_event_manager": ("Removed Event Manager", "orange", "🎟️❌"),
    "removed_custom_large_scale_mission": ("Removed custom large scale mission", "orange", "🗑️"),
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
        """Mark a log as posted. Returns False if already posted (duplicate detection)."""
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
        """Check if a log ID was already posted."""
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT 1 FROM posted_logs WHERE log_id = ?", (int(log_id),))
            return await cur.fetchone() is not None

    def _profile_url(self, mc_user_id: str) -> Optional[str]:
        """Generate profile URL."""
        if not mc_user_id:
            return None
        return f"https://www.missionchief.com/profile/{mc_user_id}"

    def _discord_profile_url(self, discord_id: int) -> str:
        return f"https://discord.com/users/{discord_id}"

    def _safe_markdown_link(self, text: str, url: str) -> str:
        """Create safe markdown link, escaping special chars."""
        if not text or not url:
            return text or url or ""
        safe_text = (text.replace("[", "\\[").replace("]", "\\]")
                        .replace("(", "\\(").replace(")", "\\)"))
        return f"[{safe_text}]({url})"

    def _format_timestamp(self, ts_str: str) -> str:
        """Format timestamp consistently."""
        if not ts_str:
            return "-"
        try:
            dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            return dt.strftime("%d %b %H:%M")
        except:
            return ts_str

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
        
        if not key or key not in DISPLAY:
            log.info("Unknown action_key: '%s' (text: '%s')", 
                     row.get("action_key"), row.get("action_text"))
            return "Alliance log", PALETTE["grey"], "ℹ️", ""
        
        title, palette_key, emoji = DISPLAY[key]
        color = PALETTE.get(palette_key, PALETTE["grey"])
        return title, color, emoji, key

    async def _desc_minimal(self, row: Dict[str, Any]) -> str:
        lines: List[str] = []
        ts = row.get("ts") or "-"
        formatted_ts = self._format_timestamp(ts)
        
        first = f"`{formatted_ts}` —"
        if await self.config.show_executor_minimal():
            by = row.get("executed_name") or "-"
            if row.get("executed_mc_id"):
                url = self._profile_url(str(row["executed_mc_id"]))
                by = self._safe_markdown_link(by, url)
                did = await self._discord_id_for_mc(str(row["executed_mc_id"]))
                if did:
                    by += f" [[D]]({self._discord_profile_url(did)})"
            first += f" by {by}"
        lines.append(first)

        desc = row.get("description") or row.get("action_text") or "-"
        lines.append(str(desc))

        if row.get("contribution_amount", 0) > 0:
            amount = f"{row['contribution_amount']:,}".replace(",", ".")
            lines.append(f"💰 Contributed: {amount} coins")

        aff_name = row.get("affected_name") or ""
        aff_url = row.get("affected_url") or ""
        block: Optional[str] = None
        if aff_name or aff_url:
            label = aff_name or "link"
            block = self._safe_markdown_link(label, aff_url) if aff_url else f"→ {label}"
            if block and not block.startswith("→"):
                block = f"→ {block}"
            if str(row.get("affected_type") or "") == "user" and row.get("affected_mc_id"):
                did = await self._discord_id_for_mc(str(row.get("affected_mc_id")))
                if did:
                    block += f" [[D]]({self._discord_profile_url(did)})"
        else:
            exec_name = row.get("executed_name") or ""
            exec_url = self._profile_url(str(row.get("executed_mc_id"))) if row.get("executed_mc_id") else ""
            if exec_name or exec_url:
                label = exec_name or "profile"
                block = self._safe_markdown_link(label, exec_url) if exec_url else f"→ {label}"
                if block and not block.startswith("→"):
                    block = f"→ {block}"
        if block:
            lines.append(block)

        return NL.join(lines)

    async def _desc_compact(self, row: Dict[str, Any]) -> str:
        lines: List[str] = []
        by = row.get("executed_name") or "-"
        if row.get("executed_mc_id"):
            url = self._profile_url(str(row["executed_mc_id"]))
            by = self._safe_markdown_link(by, url)
            did = await self._discord_id_for_mc(str(row["executed_mc_id"]))
            if did:
                by += f" [[D]]({self._discord_profile_url(did)})"
        lines.append(f"**By:** {by}")
        
        aff_name = row.get("affected_name") or ""
        aff_url = row.get("affected_url") or ""
        if aff_name or aff_url:
            aff_text = aff_name or "-"
            if aff_url:
                aff_text = self._safe_markdown_link(aff_name or "link", aff_url)
            if str(row.get("affected_type") or "") == "user" and row.get("affected_mc_id"):
                did = await self._discord_id_for_mc(str(row["affected_mc_id"]))
                if did:
                    aff_text += f" [[D]]({self._discord_profile_url(did)})"
            lines.append(f"**Affected:** {aff_text}")
        
        details = str(row.get("description") or "-")
        lines.append(f"**Details:** {details}")
        
        if row.get("contribution_amount", 0) > 0:
            amount = f"{row['contribution_amount']:,}".replace(",", ".")
            lines.append(f"**Contributed:** 💰 {amount} coins")
        
        ts = row.get("ts") or "-"
        formatted_ts = self._format_timestamp(ts)
        lines.append(f"**Date:** `{formatted_ts}`")
        return NL.join(lines)

    async def _publish_single_log(self, row: Dict[str, Any], main_ch: discord.TextChannel, 
                                   mirrors: Dict, style: str, emoji_titles: bool) -> bool:
        """
        Publish a single log entry with transactional ID tracking.
        Returns True if successfully posted, False otherwise.
        """
        log_id = int(row["id"])
        
        # CRITICAL: Check if already posted
        if await self._is_already_posted(log_id):
            log.info("Skipping log ID %d - already posted", log_id)
            return False
        
        title, color, emoji, key = self._title_from_row(row)
        title_text = f"{emoji} {title}" if emoji_titles and emoji else title

        # Build embed based on style
        if style == "minimal":
            desc = await self._desc_minimal(row)
            e = discord.Embed(title=title_text, description=desc, color=color, timestamp=datetime.utcnow())
        elif style == "compact":
            desc = await self._desc_compact(row)
            e = discord.Embed(title=title_text, description=desc, color=color, timestamp=datetime.utcnow())
        else:  # fields
            e = discord.Embed(title=title_text, color=color, timestamp=datetime.utcnow())
            by = row.get("executed_name") or "-"
            if row.get("executed_mc_id"):
                url = self._profile_url(str(row["executed_mc_id"]))
                by = self._safe_markdown_link(by, url)
                did = await self._discord_id_for_mc(str(row["executed_mc_id"]))
                if did:
                    by += f" [[D]]({self._discord_profile_url(did)})"
            e.add_field(name="By", value=by, inline=False)
            
            aff_name = row.get("affected_name") or ""
            aff_url = row.get("affected_url") or ""
            aff_value = "-"
            if aff_name or aff_url:
                acc = self._safe_markdown_link(aff_name or "link", aff_url) if aff_url else (aff_name or "link")
                if str(row.get("affected_type") or "") == "user" and row.get("affected_mc_id"):
                    did = await self._discord_id_for_mc(str(row["affected_mc_id"]))
                    if did:
                        acc += f" [[D]]({self._discord_profile_url(did)})"
                aff_value = acc
            e.add_field(name="Affected", value=aff_value, inline=False)
            e.add_field(name="Details", value=str(row.get("description") or "-"), inline=False)
            
            if row.get("contribution_amount", 0) > 0:
                amount = f"{row['contribution_amount']:,}".replace(",", ".")
                e.add_field(name="Contribution", value=f"💰 {amount} coins", inline=False)
            
            ts = row.get("ts") or "-"
            formatted_ts = self._format_timestamp(ts)
            e.add_field(name="Date", value=f"`{formatted_ts}`", inline=False)

        # Post to main channel
        try:
            await main_ch.send(embed=e)
            log.info("Posted log ID %d to main channel", log_id)
        except discord.Forbidden as ex:
            log.error("No permission to post in main channel: %s", ex)
            return False
        except discord.HTTPException as ex:
            log.warning("Failed to post log ID %d to main: %s", log_id, ex)
            return False
        except Exception as ex:
            log.exception("Unexpected error posting log ID %d: %s", log_id, ex)
            return False

        # CRITICAL: Mark as posted immediately after successful main post
        if not await self._mark_log_posted(log_id):
            log.error("RACE CONDITION: Log ID %d was posted by another process!", log_id)
            return False
        
        # Update last_id immediately after successful post
        await self._set_last_id(log_id)
        log.debug("Updated last_id to %d after posting", log_id)

        # FIXED: Mirror filtering - only post to mirrors configured for THIS action_key
        if key and key in mirrors:
            m = mirrors[key]
            if m.get("enabled"):
                guild = main_ch.guild
                for cid in m.get("channels") or []:
                    mch = guild.get_channel(int(cid))
                    if not isinstance(mch, discord.TextChannel):
                        continue
                    try:
                        await mch.send(embed=e)
                        log.debug("Mirrored log ID %d (action: %s) to channel %d", log_id, key, cid)
                    except discord.Forbidden:
                        log.warning("No permission to mirror to channel %d", cid)
                    except discord.HTTPException as ex:
                        log.warning("Failed to mirror to channel %d: %s", cid, ex)
                    except Exception as ex:
                        log.exception("Unexpected error mirroring to channel %d: %s", cid, ex)

        return True

    async def _tick_once(self) -> int:
        """Process new logs one at a time with transactional safety."""
        # Prevent concurrent runs
        if self._posting_lock.locked():
            log.info("Skipping tick - already posting")
            return 0
        
        async with self._posting_lock:
            # FIXED: Use LogsScraper instead of AllianceScraper
            sc = self.bot.get_cog("LogsScraper")
            if not sc or not hasattr(sc, "get_logs_after"):
                return 0
            
            last_id = await self._get_last_id()
            max_posts = await self.config.max_posts_per_run()
            
            # Get channel config
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
                # Fetch logs after last_id
                rows = await sc.get_logs_after(int(last_id), limit=max_posts)
            except Exception as e:
                log.exception("Failed to fetch logs: %s", e)
                return 0
            
            if not rows:
                return 0
            
            log.info("Fetched %d logs after ID %d (IDs: %s)", 
                    len(rows), last_id, [r["id"] for r in rows[:10]])
            
            posted = 0
            for idx, row in enumerate(rows):
                # Post single log with full transaction safety
                success = await self._publish_single_log(row, main_ch, mirrors, style, emoji_titles)
                
                if success:
                    posted += 1
                    # Small delay every 5 posts to avoid rate limits
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
        """AllianceLogs publisher commands."""
        pass

    @alog_group.command(name="removemirror")
    async def remove_mirror(self, ctx: commands.Context, action: str, channel: discord.TextChannel):
        """Remove a mirror channel for a specific action."""
        key = _map_user_action_input(action)
        if not key:
            await ctx.send("❌ Unknown action. Use `!alog listactions`.")
            return
        
        mirrors = await self.config.mirrors()
        m = mirrors.get(key, {"enabled": True, "channels": []})
        m["channels"] = [cid for cid in m["channels"] if cid != int(channel.id)]
        mirrors[key] = m
        await self.config.mirrors.set(mirrors)
        
        action_name = DISPLAY[key][0]
        await ctx.send(f"✅ Mirror removed: **{action_name}** from {channel.mention}")

    @alog_group.command(name="togglemirror")
    async def toggle_mirror(self, ctx: commands.Context, action: str):
        """Toggle a mirror on/off."""
        key = _map_user_action_input(action)
        if not key:
            await ctx.send("❌ Unknown action. Use `!alog listactions`.")
            return
        
        mirrors = await self.config.mirrors()
        m = mirrors.get(key, {"enabled": True, "channels": []})
        m["enabled"] = not m.get("enabled", True)
        mirrors[key] = m
        await self.config.mirrors.set(mirrors)
        
        action_name = DISPLAY[key][0]
        status = "enabled" if m["enabled"] else "disabled"
        await ctx.send(f"✅ Mirror **{action_name}** {status}")

    @alog_group.command(name="listactions")
    async def list_actions(self, ctx: commands.Context):
        """List all available action types."""
        lines = ["**Available Actions:**"]
        for key, (name, _, emoji) in DISPLAY.items():
            lines.append(f"{emoji} `{key}` - {name}")
        
        # Send in chunks if too long
        msg = "\n".join(lines)
        if len(msg) > 1900:
            for chunk in [lines[i:i+20] for i in range(0, len(lines), 20)]:
                await ctx.send("\n".join(chunk))
        else:
            await ctx.send(msg)

    @alog_group.command(name="run")
    async def run(self, ctx: commands.Context):
        """Manually trigger log posting."""
        n = await self._tick_once()
        await ctx.send(f"✅ Posted {n} new log(s).")


async def setup(bot):
    cog = AllianceLogsPub(bot)
    await bot.add_cog(cog)="version")
    async def version(self, ctx: commands.Context):
        cfg = await self.config.all()
        lines = [
            "```",
            f"AllianceLogsPub version: {__version__}",
            f"Style: {cfg['style']}  Emoji titles: {cfg['emoji_titles']}",
            f"Max posts per run: {cfg['max_posts_per_run']}",
            f"Mirrors configured: {len(cfg.get('mirrors', {}))}",
            "```",
        ]
        await ctx.send(NL.join(lines))

    @alog_group.command(name="status")
    async def status(self, ctx: commands.Context):
        """Show current status and last processed ID."""
        last_id = await self._get_last_id()
        
        sc = self.bot.get_cog("LogsScraper")
        scraper_available = sc is not None and hasattr(sc, "get_logs_after")
        
        total_logs = "N/A"
        if scraper_available:
            try:
                import sqlite3
                conn = sqlite3.connect(sc.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*), MAX(id) FROM logs")
                row = cursor.fetchone()
                if row:
                    total_logs = f"{row[0]} (max ID: {row[1]})"
                conn.close()
            except Exception as e:
                total_logs = f"Error: {e}"
        
        # Check posted logs table
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
        """Manually set the last processed ID (use with caution!)."""
        old_id = await self._get_last_id()
        await self._set_last_id(int(new_id))
        await ctx.send(f"✅ Updated last_id from {old_id} to {new_id}")
        log.info("Manual last_id update: %d -> %d (by %s)", old_id, new_id, ctx.author)

    @alog_group.command(name="setchannel")
    async def setchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the main posting channel."""
        await self.config.main_channel_id.set(int(channel.id))
        await ctx.send(f"✅ Main channel set to {channel.mention}")

    @alog_group.command(name="setinterval")
    async def setinterval(self, ctx: commands.Context, minutes: int):
        """Set posting interval in minutes (min: 1)."""
        minutes = max(1, int(minutes))
        await self.config.interval_minutes.set(minutes)
        await ctx.send(f"✅ Interval set to {minutes} minutes")

    @alog_group.command(name="setmaxposts")
    async def setmaxposts(self, ctx: commands.Context, max_posts: int):
        """Set max posts per run (1-100)."""
        max_posts = max(1, min(100, int(max_posts)))
        await self.config.max_posts_per_run.set(max_posts)
        await ctx.send(f"✅ Max posts per run set to {max_posts}")

    @alog_group.command(name="setstyle")
    async def setstyle(self, ctx: commands.Context, style: str):
        """Set embed style (minimal/compact/fields)."""
        style = style.lower().strip()
        if style not in {"minimal", "compact", "fields"}:
            await ctx.send("❌ Style must be `minimal`, `compact`, or `fields`.")
            return
        await self.config.style.set(style)
        await ctx.send(f"✅ Style set to {style}")

    @alog_group.command(name="mirrors")
    async def list_mirrors(self, ctx: commands.Context):
        """List all configured mirrors."""
        mirrors = await self.config.mirrors()
        if not mirrors:
            await ctx.send("```\nNo mirrors configured.\n```")
            return
        
        lines = []
        for k, v in mirrors.items():
            chans = ", ".join(f"<#{cid}>" for cid in (v.get("channels") or []))
            action_name = DISPLAY.get(k, (k, "", ""))[0]
            enabled = "✅" if v.get("enabled") else "❌"
            lines.append(f"{enabled} **{action_name}** → {chans if chans else '(none)'}")
        
        await ctx.send("\n".join(lines))

    @alog_group.command(name="addmirror")
    async def add_mirror(self, ctx: commands.Context, action: str, channel: discord.TextChannel):
        """Add a mirror channel for a specific action."""
        key = _map_user_action_input(action)
        if not key:
            await ctx.send("❌ Unknown action. Use `!alog listactions` to see valid options.")
            return
        
        mirrors = await self.config.mirrors()
        m = mirrors.get(key, {"enabled": True, "channels": []})
        if int(channel.id) not in m["channels"]:
            m["channels"].append(int(channel.id))
        m["enabled"] = True
        mirrors[key] = m
        await self.config.mirrors.set(mirrors)
        
        action_name = DISPLAY[key][0]
        await ctx.send(f"✅ Mirror added: **{action_name}** → {channel.mention}")

    @alog_group.command(name
