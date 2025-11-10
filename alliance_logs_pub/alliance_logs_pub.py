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

__version__ = "0.8.3"

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
    """Robust alliance logs publisher - tracks only last_id, no posted_logs table"""
    
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
        log.info("AllianceLogsPub v%s loaded (robust mode - no posted_logs table)", __version__)

    async def cog_unload(self):
        if self._bg_task:
            self._bg_task.cancel()

    async def _init_db(self):
        """Initialize database - ONLY stores last_id, no posted_logs table"""
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("CREATE TABLE IF NOT EXISTS state(k TEXT PRIMARY KEY, v TEXT)")
            # Clean up old posted_logs table if it exists (migration)
            await db.execute("DROP TABLE IF EXISTS posted_logs")
            await db.commit()
            log.info("Database initialized (posted_logs table removed)")

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
        return f"https://www.missionchief.com/profile/{mc_user_id}"

    def _discord_profile_url(self, discord_id: int) -> str:
        return f"https://discord.com/users/{discord_id}"

    def _safe_markdown_link(self, text: str, url: str) -> str:
        if not text or not url:
            return text or url or ""
        safe_text = (text.replace("[", "\\[").replace("]", "\\]")
                        .replace("(", "\\(").replace(")", "\\)"))
        return f"[{safe_text}]({url})"

    def _format_timestamp(self, ts_str: str) -> str:
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
        """Publish a single log - NO duplicate checking, assumes sequential processing"""
        log_id = int(row["id"])
        
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

        # Update last_id after successful post
        await self._set_last_id(log_id)
        log.debug("Updated last_id to %d after posting", log_id)

        # Mirror to action-specific channels
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
        """Process one batch of logs - sequential, no duplicate checking needed"""
        if self._posting_lock.locked():
            log.info("Skipping tick - already posting")
            return 0
        
        async with self._posting_lock:
            sc = self.bot.get_cog("LogsScraper")
            if not sc or not hasattr(sc, "get_logs_after"):
                log.warning("LogsScraper not available")
                return 0
            
            last_id = await self._get_last_id()
            max_posts = await self.config.max_posts_per_run()
            
            guild = self.bot.guilds[0] if self.bot.guilds else None
            if not guild:
                log.warning("No guild available")
                return 0
            
            ch_id = await self.config.main_channel_id()
            if not ch_id:
                log.warning("No main channel configured")
                return 0
            
            main_ch = guild.get_channel(int(ch_id))
            if not isinstance(main_ch, discord.TextChannel):
                log.warning("Main channel not found or not a text channel")
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
            
            log.info("Fetched %d logs after ID %d (IDs: %s to %s)", 
                    len(rows), last_id, rows[0]["id"], rows[-1]["id"])
            
            posted = 0
            for idx, row in enumerate(rows):
                success = await self._publish_single_log(row, main_ch, mirrors, style, emoji_titles)
                
                if success:
                    posted += 1
                    # Rate limiting every 5 posts
                    if (posted % 5) == 0:
                        await asyncio.sleep(1)
                else:
                    # If posting fails, stop here - don't skip logs
                    log.warning("Failed to post log ID %d, stopping batch", row["id"])
                    break
            
            log.info("Posted %d/%d logs successfully", posted, len(rows))
            return posted

    async def _maybe_start_background(self):
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._bg_loop())

    async def _bg_loop(self):
        await self.bot.wait_until_red_ready()
        log.info("Background posting loop started")
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

    @alog_group.command(name="version")
    async def version(self, ctx: commands.Context):
        cfg = await self.config.all()
        lines = [
            "```",
            f"AllianceLogsPub version: {__version__} (Robust Mode)",
            f"Style: {cfg['style']}  Emoji titles: {cfg['emoji_titles']}",
            f"Max posts per run: {cfg['max_posts_per_run']}",
            f"Mirrors configured: {len(cfg.get('mirrors', {}))}",
            "No posted_logs table - sequential processing only",
            "```",
        ]
        await ctx.send(NL.join(lines))

    @alog_group.command(name="status")
    async def status(self, ctx: commands.Context):
        last_id = await self._get_last_id()
        
        sc = self.bot.get_cog("LogsScraper")
        scraper_available = sc is not None and hasattr(sc, "get_logs_after")
        
        total_logs = "N/A"
        pending = 0
        if scraper_available:
            try:
                import sqlite3
                conn = sqlite3.connect(sc.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*), MAX(id) FROM logs")
                row = cursor.fetchone()
                if row:
                    total_logs = f"{row[0]} (max ID: {row[1]})"
                    pending = row[1] - last_id if row[1] else 0
                conn.close()
            except Exception as e:
                total_logs = f"Error: {e}"
        
        cfg = await self.config.all()
        
        lines = [
            "```",
            "=== AllianceLogsPub Status (Robust Mode) ===",
            f"Last processed ID: {last_id}",
            f"Total logs in DB: {total_logs}",
            f"Pending logs: {pending}",
            f"Scraper available: {scraper_available}",
            f"Max posts per run: {cfg['max_posts_per_run']}",
            f"Interval: {cfg['interval_minutes']} minutes",
            f"Main channel: {cfg.get('main_channel_id')}",
            "",
            "Note: No posted_logs table - sequential only",
            "```",
        ]
        await ctx.send("\n".join(lines))

    @alog_group.command(name="testfetch")
    async def test_fetch(self, ctx: commands.Context, last_id: int = None, limit: int = 10):
        """Test what get_logs_after returns from LogsScraper - DIAGNOSTIC TOOL"""
        if last_id is None:
            last_id = await self._get_last_id()
        
        sc = self.bot.get_cog("LogsScraper")
        if not sc:
            await ctx.send("❌ LogsScraper not loaded!")
            return
        
        if not hasattr(sc, "get_logs_after"):
            await ctx.send("❌ LogsScraper doesn't have get_logs_after method!")
            return
        
        await ctx.send(f"🔍 Testing get_logs_after({last_id}, limit={limit})...")
        
        try:
            rows = await sc.get_logs_after(int(last_id), limit=limit)
            
            if not rows:
                await ctx.send(f"⚠️ get_logs_after returned EMPTY list!")
                
                # Check if LogsScraper has ANY logs at all
                import sqlite3
                conn = sqlite3.connect(sc.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*), MIN(id), MAX(id) FROM logs")
                count, min_id, max_id = cursor.fetchone()
                conn.close()
                
                await ctx.send(f"📊 LogsScraper DB has {count} logs (ID range: {min_id} to {max_id})")
                await ctx.send(f"🔍 You're asking for logs > {last_id}")
                
                if max_id and last_id >= max_id:
                    await ctx.send(f"❌ **PROBLEM**: last_id ({last_id}) >= max_id ({max_id})!")
                    await ctx.send(f"💡 Try: `!alog setlastid {min_id}` to reprocess all logs")
                
                return
            
            await ctx.send(f"✅ Got {len(rows)} rows!")
            
            # Show first 3 logs details
            for i, row in enumerate(rows[:3]):
                details = f"**Log ID {row['id']}:**\n"
                details += f"  Action: {row.get('action_key', 'N/A')}\n"
                details += f"  By: {row.get('executed_name', 'N/A')}\n"
                details += f"  Affected: {row.get('affected_name', 'N/A')}\n"
                details += f"  Timestamp: {row.get('ts', 'N/A')}"
                await ctx.send(details)
            
            if len(rows) > 3:
                await ctx.send(f"... and {len(rows) - 3} more logs")
                
        except Exception as e:
            await ctx.send(f"❌ Exception: {e}")
            import traceback
            tb = traceback.format_exc()
            # Split long tracebacks
            if len(tb) > 1900:
                for chunk in [tb[i:i+1900] for i in range(0, len(tb), 1900)]:
                    await ctx.send(f"```\n{chunk}\n```")
            else:
                await ctx.send(f"```\n{tb}\n```")

    @alog_group.command(name="testpost")
    async def test_post(self, ctx: commands.Context, log_id: int = None):
        """Test posting a single log - FULL DEBUG"""
        if log_id is None:
            # Get first pending log
            last_id = await self._get_last_id()
            sc = self.bot.get_cog("LogsScraper")
            if not sc:
                await ctx.send("❌ LogsScraper not loaded!")
                return
            
            rows = await sc.get_logs_after(int(last_id), limit=1)
            if not rows:
                await ctx.send("❌ No pending logs found!")
                return
            
            log_id = rows[0]["id"]
            await ctx.send(f"🔍 Testing with first pending log: ID {log_id}")
        
        # Get the specific log
        sc = self.bot.get_cog("LogsScraper")
        if not sc:
            await ctx.send("❌ LogsScraper not loaded!")
            return
        
        rows = await sc.get_logs_after(int(log_id - 1), limit=1)
        if not rows or rows[0]["id"] != log_id:
            await ctx.send(f"❌ Log ID {log_id} not found!")
            return
        
        row = rows[0]
        await ctx.send(f"✅ Found log {log_id}: {row.get('action_key')}")
        
        # Get channel
        ch_id = await self.config.main_channel_id()
        if not ch_id:
            await ctx.send("❌ No main channel configured!")
            return
        
        guild = ctx.guild or self.bot.guilds[0]
        main_ch = guild.get_channel(int(ch_id))
        if not isinstance(main_ch, discord.TextChannel):
            await ctx.send(f"❌ Channel {ch_id} not found or not a text channel!")
            return
        
        await ctx.send(f"✅ Target channel: {main_ch.mention}")
        
        # Test permissions
        perms = main_ch.permissions_for(guild.me)
        if not perms.send_messages:
            await ctx.send(f"❌ Bot cannot send messages in {main_ch.mention}!")
            return
        if not perms.embed_links:
            await ctx.send(f"⚠️ Bot cannot embed links in {main_ch.mention}!")
        
        await ctx.send("✅ Bot has send permissions")
        
        # Get settings
        mirrors = await self.config.mirrors()
        style = (await self.config.style()).lower()
        emoji_titles = bool(await self.config.emoji_titles())
        
        await ctx.send(f"⚙️ Style: {style}, Emoji: {emoji_titles}")
        
        # Try to post
        await ctx.send("🚀 Attempting to post...")
        
        try:
            success = await self._publish_single_log(row, main_ch, mirrors, style, emoji_titles)
            
            if success:
                await ctx.send(f"✅ Successfully posted log {log_id} to {main_ch.mention}!")
                await ctx.send(f"💾 last_id should now be: {log_id}")
            else:
                await ctx.send(f"❌ _publish_single_log returned False for log {log_id}")
                await ctx.send("Check console/logs for error details")
        
        except Exception as e:
            await ctx.send(f"💥 Exception during posting: {e}")
            import traceback
            tb = traceback.format_exc()
            if len(tb) > 1900:
                for chunk in [tb[i:i+1900] for i in range(0, len(tb), 1900)]:
                    await ctx.send(f"```\n{chunk}\n```")
            else:
                await ctx.send(f"```\n{tb}\n```")

    @alog_group.command(name="debugrun")
    async def debug_run(self, ctx: commands.Context):
        """Debug version of run - shows exactly what happens in _tick_once"""
        await ctx.send("🔍 **Debug Run Starting...**")
        
        # Step 1: Check lock
        if self._posting_lock.locked():
            await ctx.send("❌ STOP: Posting lock is already locked!")
            return
        await ctx.send("✅ Lock available")
        
        # Step 2: Check LogsScraper
        sc = self.bot.get_cog("LogsScraper")
        if not sc:
            await ctx.send("❌ STOP: LogsScraper cog not found!")
            return
        if not hasattr(sc, "get_logs_after"):
            await ctx.send("❌ STOP: LogsScraper has no get_logs_after method!")
            return
        await ctx.send("✅ LogsScraper available")
        
        # Step 3: Get settings
        last_id = await self._get_last_id()
        max_posts = await self.config.max_posts_per_run()
        await ctx.send(f"✅ last_id={last_id}, max_posts={max_posts}")
        
        # Step 4: Check guild
        guild = self.bot.guilds[0] if self.bot.guilds else None
        if not guild:
            await ctx.send("❌ STOP: No guild available!")
            return
        await ctx.send(f"✅ Guild: {guild.name}")
        
        # Step 5: Check channel
        ch_id = await self.config.main_channel_id()
        if not ch_id:
            await ctx.send("❌ STOP: No main channel configured!")
            return
        
        main_ch = guild.get_channel(int(ch_id))
        if not isinstance(main_ch, discord.TextChannel):
            await ctx.send(f"❌ STOP: Channel {ch_id} not found or not a text channel!")
            return
        await ctx.send(f"✅ Channel: {main_ch.mention}")
        
        # Step 6: Get style settings
        mirrors = await self.config.mirrors()
        style = (await self.config.style()).lower()
        emoji_titles = bool(await self.config.emoji_titles())
        await ctx.send(f"✅ Style: {style}, Emoji: {emoji_titles}, Mirrors: {len(mirrors)}")
        
        # Step 7: Fetch logs
        await ctx.send(f"📡 Fetching logs after ID {last_id}...")
        try:
            rows = await sc.get_logs_after(int(last_id), limit=max_posts)
        except Exception as e:
            await ctx.send(f"❌ EXCEPTION during fetch: {e}")
            import traceback
            tb = traceback.format_exc()
            if len(tb) > 1900:
                await ctx.send(f"```\n{tb[:1900]}\n```")
            else:
                await ctx.send(f"```\n{tb}\n```")
            return
        
        # Step 8: Check if rows returned
        if not rows:
            await ctx.send("❌ STOP: get_logs_after returned EMPTY LIST!")
            await ctx.send(f"🔍 Called: sc.get_logs_after({last_id}, limit={max_posts})")
            await ctx.send("This should have returned data based on testfetch!")
            return
        
        await ctx.send(f"✅ Fetched {len(rows)} logs (IDs: {rows[0]['id']} to {rows[-1]['id']})")
        
        # Step 9: Post logs
        await ctx.send(f"🚀 Starting to post {len(rows)} logs...")
        
        posted = 0
        failed = 0
        
        for idx, row in enumerate(rows):
            success = await self._publish_single_log(row, main_ch, mirrors, style, emoji_titles)
            
            if success:
                posted += 1
                if posted <= 3:
                    await ctx.send(f"  ✅ Posted log {row['id']}")
                elif posted % 10 == 0:
                    await ctx.send(f"  📊 Progress: {posted}/{len(rows)}")
                
                if (posted % 5) == 0:
                    await asyncio.sleep(1)
            else:
                failed += 1
                await ctx.send(f"  ❌ FAILED to post log {row['id']}")
                await ctx.send(f"  🛑 Stopping batch (posted {posted}, failed {failed})")
                break
        
        await ctx.send(f"✅ **COMPLETE**: Posted {posted}/{len(rows)} logs successfully")
        if failed > 0:
            await ctx.send(f"⚠️ {failed} logs failed to post")

    @alog_group.command(name="setlastid")
    async def setlastid(self, ctx: commands.Context, new_id: int):
        """Set last processed ID - use to skip or reprocess logs"""
        old_id = await self._get_last_id()
        await self._set_last_id(int(new_id))
        await ctx.send(f"✅ Updated last_id from {old_id} to {new_id}")
        log.info("Manual last_id update: %d -> %d (by %s)", old_id, new_id, ctx.author)

    @alog_group.command(name="setchannel")
    async def setchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.main_channel_id.set(int(channel.id))
        await ctx.send(f"✅ Main channel set to {channel.mention}")

    @alog_group.command(name="setinterval")
    async def setinterval(self, ctx: commands.Context, minutes: int):
        minutes = max(1, int(minutes))
        await self.config.interval_minutes.set(minutes)
        await ctx.send(f"✅ Interval set to {minutes} minutes")

    @alog_group.command(name="setmaxposts")
    async def setmaxposts(self, ctx: commands.Context, max_posts: int):
        max_posts = max(1, min(100, int(max_posts)))
        await self.config.max_posts_per_run.set(max_posts)
        await ctx.send(f"✅ Max posts per run set to {max_posts}")

    @alog_group.command(name="setstyle")
    async def setstyle(self, ctx: commands.Context, style: str):
        style = style.lower().strip()
        if style not in {"minimal", "compact", "fields"}:
            await ctx.send("❌ Style must be `minimal`, `compact`, or `fields`.")
            return
        await self.config.style.set(style)
        await ctx.send(f"✅ Style set to {style}")

    @alog_group.command(name="mirrors")
    async def list_mirrors(self, ctx: commands.Context):
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

    @alog_group.command(name="removemirror")
    async def remove_mirror(self, ctx: commands.Context, action: str, channel: discord.TextChannel):
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
        lines = ["**Available Actions:**"]
        for key, (name, _, emoji) in DISPLAY.items():
            lines.append(f"{emoji} `{key}` - {name}")
        
        msg = "\n".join(lines)
        if len(msg) > 1900:
            for chunk in [lines[i:i+20] for i in range(0, len(lines), 20)]:
                await ctx.send("\n".join(chunk))
        else:
            await ctx.send(msg)

    @alog_group.command(name="run")
    async def run(self, ctx: commands.Context):
        """Manually trigger one batch of posts"""
        n = await self._tick_once()
        await ctx.send(f"✅ Posted {n} new log(s).")

    @alog_group.command(name="taskstatus")
    async def task_status(self, ctx: commands.Context):
        """Check if background posting task is running"""
        if self._bg_task is None:
            await ctx.send("❌ Background task is NOT running!")
        elif self._bg_task.done():
            await ctx.send("⚠️ Background task exists but is DONE (crashed or completed)")
            try:
                exc = self._bg_task.exception()
                if exc:
                    await ctx.send(f"💥 Task exception: {exc}")
            except:
                pass
        elif self._bg_task.cancelled():
            await ctx.send("⚠️ Background task was CANCELLED")
        else:
            await ctx.send("✅ Background task is running")
            cfg = await self.config.all()
            await ctx.send(f"ℹ️ Runs every {cfg['interval_minutes']} minutes")

    @alog_group.command(name="restarttask")
    async def restart_task(self, ctx: commands.Context):
        """Restart the background posting task"""
        # Cancel old task if exists
        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
            await ctx.send("🛑 Cancelled old task")
            await asyncio.sleep(1)
        
        # Start new task
        self._bg_task = asyncio.create_task(self._bg_loop())
        await ctx.send("✅ Background posting task restarted!")
        log.info("Background task manually restarted by %s", ctx.author)


async def setup(bot):
    cog = AllianceLogsPub(bot)
    await bot.add_cog(cog)
