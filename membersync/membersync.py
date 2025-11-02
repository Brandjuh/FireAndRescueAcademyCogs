from __future__ import annotations
import pathlib
import aiosqlite
import asyncio
import logging
import re
import sqlite3
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple

import discord
from discord import app_commands
from redbot.core import commands, checks, Config
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.FARA.MemberSync")

DEFAULTS = {
    "alliance_db_path": None,
    "review_channel_id": 1421256548977606827,
    "log_channel_id": 668874513663918100,
    "verified_role_id": 565988933113085952,
    "reviewer_role_ids": [544117282167586836],
    "cooldown_seconds": 30,
    "queue": {},
    "prune_min_members": 800,  # NEW: Safety threshold (50% van ~1676)
}

def utcnow_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def _norm(s: Optional[str]) -> str:
    return (s or "").strip()

def _lower(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _mc_profile_url(mc_id: str) -> str:
    return f"https://www.missionchief.com/users/{mc_id}"

class MemberSync(commands.Cog):
    """Synchronises Missionchief members with Discord and handles verification workflow."""

    __version__ = "1.2.0"  # Version bump for critical fixes

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA11A9E5, force_registration=True)
        self.config.register_global(**DEFAULTS)
        self.data_path = cog_data_path(self)
        self.db_path = self.data_path / "membersync.db"
        self.links_db = self.data_path / "membersync.db"
        self._bg_task: Optional[asyncio.Task] = None

    async def cog_load(self) -> None:
        await self._init_db()
        if await self.config.alliance_db_path() is None:
            guess = self._guess_alliance_db()
            if guess:
                await self.config.alliance_db_path.set(str(guess))
                log.info(f"Auto-detected alliance database: {guess}")
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._queue_loop())

    async def cog_unload(self) -> None:
        if self._bg_task:
            self._bg_task.cancel()
            self._bg_task = None

    async def _init_db(self) -> None:
        """Initialize MemberSync local DB (async, no executor)."""
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
            CREATE TABLE IF NOT EXISTS links (
                discord_id     INTEGER NOT NULL,
                mc_user_id     TEXT    NOT NULL,
                status         TEXT    NOT NULL DEFAULT 'pending',
                created_at     TEXT    NOT NULL,
                updated_at     TEXT    NOT NULL,
                reviewer_id    INTEGER
            )""")
            await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_links_discord ON links(discord_id)")
            await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_links_mc ON links(mc_user_id)")
            
            await db.execute("""
            CREATE TABLE IF NOT EXISTS queue (
                discord_id   INTEGER PRIMARY KEY,
                requested_at TEXT    NOT NULL,
                attempts     INTEGER NOT NULL DEFAULT 0
            )""")
            
            await db.execute("""
            CREATE TABLE IF NOT EXISTS member_left_alliance (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                mc_user_id     TEXT    NOT NULL,
                username       TEXT,
                discord_id     INTEGER,
                rank_role      TEXT,
                earned_credits INTEGER,
                contribution_rate REAL,
                last_seen_at   TEXT,
                exit_detected_at TEXT NOT NULL,
                reason         TEXT DEFAULT 'auto-detected',
                role_removed   INTEGER DEFAULT 0,
                notified       INTEGER DEFAULT 0
            )""")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_exit_mc ON member_left_alliance(mc_user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_exit_discord ON member_left_alliance(discord_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_exit_detected ON member_left_alliance(exit_detected_at)")
            
            await db.commit()
            log.info("✅ MemberSync database initialized")

    def _guess_alliance_db(self) -> Optional[pathlib.Path]:
        """Try to find the members database - prioritize members_v2.db from scraper_databases."""
        base = pathlib.Path.home() / ".local" / "share" / "Red-DiscordBot" / "data"
        
        # First try: scraper_databases/members_v2.db (NEW membersscraper location)
        for inst in base.iterdir():
            if not inst.is_dir():
                continue
            p = inst / "cogs" / "scraper_databases" / "members_v2.db"
            if p.exists():
                log.info(f"✅ Found members database at: {p}")
                return p
        
        # Fallback: AllianceScraper/alliance.db (OLD location, if it exists)
        for inst in base.iterdir():
            if not inst.is_dir():
                continue
            p = inst / "cogs" / "AllianceScraper" / "alliance.db"
            if p.exists():
                log.warning(f"⚠️ Using legacy AllianceScraper database at: {p}")
                return p
        
        log.error("❌ No alliance database found!")
        return None

    async def _query_alliance(self, sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
        path = await self.config.alliance_db_path()
        if not path:
            return []
        def _run() -> List[sqlite3.Row]:
            con = sqlite3.connect(path)
            con.row_factory = sqlite3.Row
            try:
                cur = con.execute(sql, params)
                rows = cur.fetchall()
                return rows
            finally:
                con.close()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _run)
    
    async def _latest_snapshot(self) -> Optional[str]:
        """Get the latest snapshot timestamp."""
        rows = await self._query_alliance("SELECT MAX(timestamp) AS s FROM members")
        return rows[0]["s"] if rows and rows[0]["s"] else None

    async def _get_current_member_count(self) -> int:
        """Get count of members in latest scrape - for safety checks."""
        try:
            rows = await self._query_alliance("SELECT COUNT(*) as cnt FROM members_current")
            if rows:
                return rows[0]["cnt"]
        except Exception as e:
            log.error(f"Failed to get member count: {e}")
        return 0

    async def get_link_for_mc(self, mc_user_id: str) -> Optional[Dict[str, Any]]:
        """Public API: returns approved link for given MC ID or None."""
        mc_user_id = str(mc_user_id)
        def _run():
            con = sqlite3.connect(self.links_db)
            con.row_factory = sqlite3.Row
            try:
                r = con.execute("SELECT * FROM links WHERE mc_user_id=? AND status='approved'", (mc_user_id,)).fetchone()
                return dict(r) if r else None
            finally:
                con.close()
        return await asyncio.get_running_loop().run_in_executor(None, _run)

    async def get_link_for_discord(self, discord_id: int) -> Optional[Dict[str, Any]]:
        def _run():
            con = sqlite3.connect(self.links_db)
            con.row_factory = sqlite3.Row
            try:
                r = con.execute("SELECT * FROM links WHERE discord_id=? AND status='approved'", (str(discord_id),)).fetchone()
                return dict(r) if r else None
            finally:
                con.close()
        return await asyncio.get_running_loop().run_in_executor(None, _run)

    async def _find_member_in_db(self, candidate_name: Optional[str], candidate_mc_id: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Find member in database using members_current VIEW.
        
        The VIEW works for BOTH old (alliance.db) and new (members_v2.db) databases!
        """
        name = _lower(candidate_name) if candidate_name else None
        mcid = str(candidate_mc_id) if candidate_mc_id else None

        # Search by MC ID first (most reliable)
        if mcid:
            # Try user_id column
            rows = await self._query_alliance(
                "SELECT * FROM members_current WHERE user_id=?",
                (mcid,)
            )
            if rows:
                r = dict(rows[0])
                r["mc_id"] = mcid
                return r
            
            # Try mc_user_id column (if exists)
            rows = await self._query_alliance(
                "SELECT * FROM members_current WHERE mc_user_id=?",
                (mcid,)
            )
            if rows:
                r = dict(rows[0])
                r["mc_id"] = mcid
                return r
            
            # Try profile_href as fallback (if exists)
            rows = await self._query_alliance(
                "SELECT * FROM members_current WHERE profile_href LIKE ?",
                (f"%/users/{mcid}%",)
            )
            if rows:
                r = dict(rows[0])
                r["mc_id"] = mcid
                return r

        # Search by name
        if name:
            rows = await self._query_alliance(
                "SELECT * FROM members_current WHERE lower(name)=?",
                (name,)
            )
            if rows:
                r = dict(rows[0])
                # Extract MC ID from available columns
                mc = r.get("user_id") or r.get("mc_user_id")
                if not mc:
                    href = r.get("profile_href") or ""
                    m = re.search(r"/users/(\d+)", href or "")
                    if m:
                        mc = m.group(1)
                r["mc_id"] = mc
                return r

        return None

    async def _get_reviewer_roles(self, guild: discord.Guild) -> List[discord.Role]:
        ids = await self.config.reviewer_role_ids()
        roles: List[discord.Role] = []
        for rid in ids:
            r = guild.get_role(int(rid))
            if r:
                roles.append(r)
        return roles

    async def _user_is_reviewer(self, member: discord.Member) -> bool:
        if member.guild_permissions.administrator:
            return True
        reviewer_roles = await self._get_reviewer_roles(member.guild)
        return any(r in member.roles for r in reviewer_roles)

    async def _send_review_embed(self, guild: discord.Guild, requester: discord.Member, mc_id: str, mc_name: str) -> Optional[int]:
        ch_id = await self.config.review_channel_id()
        ch = guild.get_channel(int(ch_id)) if ch_id else None
        if not isinstance(ch, discord.TextChannel):
            return None

        view = discord.ui.View(timeout=None)
        approve_btn = discord.ui.Button(label="Approve", style=discord.ButtonStyle.green, custom_id=f"ms_approve_{mc_id}_{requester.id}")
        deny_btn = discord.ui.Button(label="Deny", style=discord.ButtonStyle.red, custom_id=f"ms_deny_{mc_id}_{requester.id}")
        view.add_item(approve_btn)
        view.add_item(deny_btn)

        async def __ms_approve_cb(interaction: discord.Interaction):
            if not await self._user_is_reviewer(interaction.user):
                await interaction.response.send_message("You are not allowed to approve verifications.", ephemeral=True)
                return
            member_to_approve = guild.get_member(requester.id)
            if not member_to_approve:
                await interaction.response.send_message("Member left the server.", ephemeral=True)
                return
            await interaction.response.defer()
            await self._approve_link(guild, member_to_approve, mc_id, approver=interaction.user if isinstance(interaction.user, discord.Member) else None)
            try:
                await interaction.followup.send("Approved.", ephemeral=True)
            except Exception:
                pass

        approve_btn.callback = __ms_approve_cb

        async def __ms_deny_cb(interaction: discord.Interaction):
            if not await self._user_is_reviewer(interaction.user):
                await interaction.response.send_message("You are not allowed to deny verifications.", ephemeral=True)
                return
            try:
                await interaction.response.defer(ephemeral=True)
                if interaction.message:
                    await interaction.message.delete()
                    await interaction.followup.send("Denied. Use !membersync deny if you need to provide a reason.", ephemeral=True)
                else:
                    await interaction.followup.send("Use the deny flow/command to provide a reason.", ephemeral=True)
            except Exception:
                pass
        
        deny_btn.callback = __ms_deny_cb

        embed = discord.Embed(
            title="Verification request",
            description=f"Discord: {requester.mention} (`{requester.id}`)\nMC: [{mc_name}]({_mc_profile_url(mc_id)}) (`{mc_id}`)",
            color=discord.Color.blurple(),
            timestamp=datetime.utcnow()
        )
        msg = await ch.send(embed=embed, view=view)

        return msg.id

    async def _approve_link(self, guild: discord.Guild, user: discord.Member, mc_id: str, approver: Optional[discord.Member]=None) -> Tuple[bool, str]:
        verified_role_id = await self.config.verified_role_id()
        role = guild.get_role(int(verified_role_id)) if verified_role_id else None

        def _run():
            con = sqlite3.connect(self.links_db)
            try:
                con.execute("""
                INSERT INTO links(discord_id, mc_user_id, status, created_at, approved_by, updated_at)
                VALUES(?, ?, 'approved', ?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET
                  mc_user_id=excluded.mc_user_id,
                  status='approved',
                  approved_by=excluded.approved_by,
                  updated_at=excluded.updated_at
                """, (str(user.id), str(mc_id), utcnow_iso(), str(approver.id if approver else 0), utcnow_iso()))
                con.commit()
            finally:
                con.close()
        await asyncio.get_running_loop().run_in_executor(None, _run)

        if role and role not in user.roles:
            try:
                await user.add_roles(role, reason="MemberSync verified")
            except Exception:
                pass

        try:
            await user.send(f"Your Missionchief account `{mc_id}` has been approved and linked.")
        except Exception:
            pass

        log_ch_id = await self.config.log_channel_id()
        ch = guild.get_channel(int(log_ch_id)) if log_ch_id else None
        if isinstance(ch, discord.TextChannel):
            url = _mc_profile_url(mc_id)
            await ch.send(f"✅ Linked {user.mention} to MC [{mc_id}]({url})")

        return True, "Approved, linked and role granted."

    async def _deny_link(self, guild: discord.Guild, user: discord.Member, mc_id: str, reviewer: Optional[discord.Member], reason: str) -> None:
        def _run():
            con = sqlite3.connect(self.links_db)
            try:
                con.execute("""
                INSERT INTO links(discord_id, mc_user_id, status, created_at, approved_by, updated_at)
                VALUES(?, ?, 'denied', ?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET status='denied', updated_at=excluded.updated_at
                """, (str(user.id), str(mc_id), utcnow_iso(), str(reviewer.id if reviewer else 0), utcnow_iso()))
                con.commit()
            finally:
                con.close()
        await asyncio.get_running_loop().run_in_executor(None, _run)

        try:
            await user.send(f"Your verification for MC `{mc_id}` was denied. Reason: {reason}")
        except Exception:
            pass

        log_ch_id = await self.config.log_channel_id()
        ch = guild.get_channel(int(log_ch_id)) if log_ch_id else None
        if isinstance(ch, discord.TextChannel):
            await ch.send(f"❌ Denied {user.mention} for MC `{mc_id}`: {reason}")

    async def _queue_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                await self._process_queue()
            except Exception as e:
                log.exception("queue loop error: %s", e)
            await asyncio.sleep(120)

    async def _process_queue(self):
        queue = await self.config.queue()
        if not queue:
            return
        done: List[str] = []
        for user_id, data in list(queue.items()):
            guild_id = data.get("guild_id")
            guild = self.bot.get_guild(int(guild_id)) if guild_id else None
            if not guild:
                done.append(user_id)
                continue
            discord_user = guild.get_member(int(user_id))
            if not discord_user:
                done.append(user_id)
                continue

            attempts = data.get("attempts", 0)
            name = data.get("by") or ""
            mc_id = data.get("mc_id") or None

            cand = await self._find_member_in_db(name, mc_id)
            if cand and cand.get("mc_id"):
                await self._send_review_embed(guild, discord_user, str(cand["mc_id"]), str(cand.get("name") or name))
                try:
                    await discord_user.send("We found you! A reviewer will now approve your verification.")
                except Exception:
                    pass
                done.append(user_id)
                continue

            attempts += 1
            data["attempts"] = attempts
            queue[user_id] = data
            if attempts >= 30:
                try:
                    await discord_user.send("Verification queue expired. Please try again later.")
                except Exception:
                    pass
                done.append(user_id)

        for uid in done:
            queue.pop(uid, None)
        await self.config.queue.set(queue)

    @commands.group(name="membersync")
    @checks.admin_or_permissions(manage_guild=True)
    async def membersync_group(self, ctx: commands.Context):
        """MemberSync administration."""
        pass

    @membersync_group.command(name="status")
    async def status(self, ctx: commands.Context):
        """Show configuration and queue status."""
        cfg = await self.config.all()
        
        # Check if VIEW exists in database
        db_path = cfg['alliance_db_path']
        view_status = "❓ Unknown"
        view_count = 0
        latest_timestamp = "Unknown"
        
        if db_path and pathlib.Path(db_path).exists():
            try:
                # Get VIEW count
                rows = await self._query_alliance("SELECT COUNT(*) as cnt FROM members_current")
                if rows:
                    view_count = rows[0]["cnt"]
                
                # Get latest timestamp from VIEW
                rows = await self._query_alliance("SELECT MAX(scraped_at) as latest FROM members_current LIMIT 1")
                if rows and rows[0]["latest"]:
                    latest_timestamp = rows[0]["latest"][:19]
                
                # CRITICAL CHECK: Verify VIEW is using ONLY latest scrape
                rows = await self._query_alliance("SELECT MAX(timestamp) as latest FROM members")
                if rows and rows[0]["latest"]:
                    db_latest = rows[0]["latest"]
                    
                    # Count how many scrapes have this timestamp
                    rows = await self._query_alliance("SELECT COUNT(*) as cnt FROM members WHERE timestamp=?", (db_latest,))
                    actual_latest_count = rows[0]["cnt"] if rows else 0
                    
                    # Compare timestamps (ignore microseconds differences)
                    view_ts = latest_timestamp[:19] if latest_timestamp else ""
                    db_ts = db_latest[:19] if db_latest else ""
                    
                    if view_count == actual_latest_count and view_ts == db_ts:
                        view_status = f"✅ Active ({view_count} members)"
                    elif view_count > actual_latest_count * 1.5:
                        view_status = f"⚠️ DUPLICATES - Shows {view_count} but should be ~{actual_latest_count}"
                    elif actual_latest_count == 0:
                        view_status = f"⚠️ Empty - No data in latest scrape"
                    else:
                        view_status = f"⚠️ STALE - VIEW has {view_count}, DB has {actual_latest_count}"
                else:
                    view_status = f"✅ Active ({view_count} members)"
                    
            except Exception as e:
                view_status = f"❌ Error: {e}"
        
        # Check member_left_alliance table
        def _check_exits():
            con = sqlite3.connect(self.links_db)
            try:
                cur = con.execute("SELECT COUNT(*) FROM member_left_alliance")
                return cur.fetchone()[0]
            finally:
                con.close()
        
        exit_count = await asyncio.get_running_loop().run_in_executor(None, _check_exits)
        
        # Get min threshold
        min_threshold = cfg.get('prune_min_members', 800)
        
        lines = [
            f"**MemberSync Status**",
            f"Version: `{self.__version__}`",
            f"",
            f"**Database:**",
            f"Path: `{cfg['alliance_db_path']}`",
            f"members_current VIEW: {view_status}",
            f"Latest Scrape: `{latest_timestamp}`",
            f"Exit Records: {exit_count}",
            f"",
            f"**Safety:**",
            f"Prune Min Threshold: {min_threshold} members",
            f"",
            f"**Channels:**",
            f"Review: <#{cfg['review_channel_id']}>",
            f"Log: <#{cfg['log_channel_id']}>",
            f"",
            f"**Roles:**",
            f"Verified: <@&{cfg['verified_role_id']}>",
            f"Reviewers: {', '.join(f'<@&{rid}>' for rid in cfg['reviewer_role_ids'])}",
            f"",
            f"**Queue:**",
            f"Size: {len(cfg.get('queue', {}))}",
            f"Cooldown: {cfg['cooldown_seconds']} sec",
        ]
        await ctx.send("\n".join(lines))

    @membersync_group.command(name="debug")
    async def debug_member(self, ctx: commands.Context, mc_id: str):
        """Debug tool: Check if a MC-ID exists in database and how it's stored."""
        await ctx.send(f"🔍 **Debugging MC-ID:** `{mc_id}`\n")
        
        # 1. Check database path
        db_path = await self.config.alliance_db_path()
        await ctx.send(f"**Database Path:**\n`{db_path}`\n")
        
        if not db_path:
            await ctx.send("❌ No database path configured!")
            return
        
        if not pathlib.Path(db_path).exists():
            await ctx.send(f"❌ Database file does not exist!")
            return
        
        await ctx.send("✅ Database file exists\n")
        
        # 2. Check VIEW
        await ctx.send(f"**Checking members_current VIEW:**")
        try:
            schema_rows = await self._query_alliance("PRAGMA table_info(members_current)")
            if schema_rows:
                columns = [dict(r) for r in schema_rows]
                col_names = [c['name'] for c in columns]
                await ctx.send(f"✅ VIEW exists with columns:\n```\n{', '.join(col_names)}\n```\n")
            else:
                await ctx.send(f"❌ members_current VIEW not found!\n")
                return
        except Exception as e:
            await ctx.send(f"❌ Error reading VIEW: {e}\n")
            return
        
        # 3. Check VIEW data quality
        await ctx.send("**Checking VIEW data quality:**")
        try:
            # Get latest from VIEW
            rows = await self._query_alliance("SELECT MAX(scraped_at) as latest FROM members_current")
            view_latest = rows[0]["latest"] if rows and rows[0]["latest"] else "None"
            
            # Get count from VIEW
            rows = await self._query_alliance("SELECT COUNT(*) as cnt FROM members_current")
            view_count = rows[0]["cnt"] if rows else 0
            
            # Get latest from raw table
            rows = await self._query_alliance("SELECT MAX(timestamp) as latest FROM members")
            db_latest = rows[0]["latest"] if rows and rows[0]["latest"] else "None"
            
            # Get count from latest scrape in raw table
            rows = await self._query_alliance("SELECT COUNT(*) as cnt FROM members WHERE timestamp=?", (db_latest,))
            actual_count = rows[0]["cnt"] if rows else 0
            
            if view_latest == db_latest and view_count == actual_count:
                await ctx.send(f"✅ VIEW is CORRECT\n- Timestamp: `{view_latest}`\n- Count: {view_count} members\n")
            else:
                await ctx.send(f"⚠️ **VIEW MISMATCH!**\n- VIEW timestamp: `{view_latest}` ({view_count} members)\n- DB latest: `{db_latest}` ({actual_count} members)\n")
                if view_count > actual_count * 1.5:
                    await ctx.send(f"🚨 **CRITICAL:** VIEW has {view_count / actual_count:.1f}x too many records!\nThis will cause mass role removals!\n")
        except Exception as e:
            await ctx.send(f"❌ Error checking quality: {e}\n")
        
        # 4. Search for member
        await ctx.send(f"**Searching for MC-ID `{mc_id}`:**")
        
        rows = await self._query_alliance("SELECT * FROM members_current WHERE user_id=?", (mc_id,))
        if rows:
            member_data = dict(rows[0])
            await ctx.send(f"✅ **Found!**\n```json\n{json.dumps(member_data, indent=2, default=str)[:1800]}\n```\n")
        else:
            await ctx.send(f"❌ Not found in VIEW\n")
        
        # 5. Check link
        await ctx.send(f"**Checking MemberSync link:**")
        link = await self.get_link_for_mc(mc_id)
        
        if link:
            await ctx.send(f"✅ **Link exists:**\n```json\n{json.dumps(link, indent=2, default=str)}\n```")
        else:
            await ctx.send(f"⚠️ No link found\n")
        
        # 6. Simulate prune
        await ctx.send(f"**Simulating prune for MC-ID `{mc_id}`:**")
        
        try:
            current_ids: set[str] = set()
            rows = await self._query_alliance("SELECT user_id, mc_user_id, profile_href FROM members_current")
            
            for r in rows:
                rd = dict(r)
                mc = rd.get("user_id") or rd.get("mc_user_id")
                if not mc:
                    href = rd.get("profile_href") or ""
                    m = re.search(r"/users/(\d+)", href or "")
                    if m:
                        mc = m.group(1)
                if mc:
                    current_ids.add(str(mc))
            
            if mc_id in current_ids:
                await ctx.send(f"✅ MC-ID `{mc_id}` is SAFE (in whitelist)")
            else:
                await ctx.send(f"❌ MC-ID `{mc_id}` would be REMOVED!")
            
            await ctx.send(f"**Whitelist size:** {len(current_ids)} members")
            
            # Safety check
            min_threshold = await self.config.prune_min_members()
            if len(current_ids) < min_threshold:
                await ctx.send(f"⚠️ **SAFETY TRIGGERED:** Only {len(current_ids)} members (min: {min_threshold})\nPrune would be ABORTED!")
            
        except Exception as e:
            await ctx.send(f"❌ Simulation failed: {e}")

    @membersync_group.command(name="prune")
    async def manual_prune(self, ctx: commands.Context, dry_run: bool = True):
        """
        Manually run the prune check to remove Verified role from members who left the alliance.
        
        dry_run: If True (default), only shows what would happen without actually doing it.
        Set to False to actually remove roles.
        
        Usage:
        [p]membersync prune           - Dry run (show what would happen)
        [p]membersync prune False     - Actually remove roles
        """
        if dry_run:
            await ctx.send("🔍 **DRY RUN MODE** - No roles will be removed\n")
        else:
            await ctx.send("⚠️ **LIVE MODE** - Roles WILL be removed!\n")
        
        await ctx.send("Starting prune check...")
        
        result = await self._prune_once(guild=ctx.guild, dry_run=dry_run, manual=True)
        
        embed = discord.Embed(
            title="🔎 Prune Check Results",
            color=discord.Color.blue() if dry_run else discord.Color.orange()
        )
        embed.add_field(name="Mode", value="DRY RUN" if dry_run else "LIVE", inline=True)
        embed.add_field(name="Checked Links", value=str(result['checked']), inline=True)
        embed.add_field(name="Left Alliance", value=str(result['left_alliance']), inline=True)
        embed.add_field(name="Roles Removed", value=str(result['removed']), inline=True)
        embed.add_field(name="Already Gone", value=str(result['already_gone']), inline=True)
        embed.add_field(name="Errors", value=str(result['errors']), inline=True)
        
        if result.get('safety_triggered'):
            embed.add_field(
                name="⚠️ SAFETY ABORT", 
                value=f"Only {result.get('view_member_count', 0)} members in VIEW (min: {result.get('min_threshold', 800)})\n**Prune aborted to prevent mass removal!**", 
                inline=False
            )
            embed.color = discord.Color.red()
        
        if result['details']:
            details_text = "\n".join(result['details'][:10])
            if len(result['details']) > 10:
                details_text += f"\n... and {len(result['details']) - 10} more"
            embed.add_field(name="Details", value=details_text, inline=False)
        
        if dry_run and result['left_alliance'] > 0 and not result.get('safety_triggered'):
            embed.set_footer(text=f"Run '[p]membersync prune False' to actually remove {result['left_alliance']} roles")
        
        await ctx.send(embed=embed)

    @membersync_group.command(name="exits")
    async def show_exits(self, ctx: commands.Context, limit: int = 20):
        """
        Show recent members who left the alliance.
        
        limit: Number of recent exits to show (default 20, max 50)
        """
        limit = min(max(1, limit), 50)
        
        await ctx.send(f"📊 Fetching last {limit} exits...")
        
        def _get_exits():
            con = sqlite3.connect(self.links_db)
            con.row_factory = sqlite3.Row
            try:
                cur = con.execute("""
                    SELECT * FROM member_left_alliance 
                    ORDER BY exit_detected_at DESC 
                    LIMIT ?
                """, (limit,))
                return [dict(r) for r in cur.fetchall()]
            finally:
                con.close()
        
        exits = await asyncio.get_running_loop().run_in_executor(None, _get_exits)
        
        if not exits:
            await ctx.send("✅ No exit records found! Everyone is still in the alliance.")
            return
        
        # Group by pages of 10
        pages = []
        for i in range(0, len(exits), 10):
            chunk = exits[i:i+10]
            
            embed = discord.Embed(
                title=f"📋 Members Who Left Alliance",
                color=discord.Color.red(),
                description=f"Showing exits {i+1}-{min(i+10, len(exits))} of {len(exits)}"
            )
            
            for exit_record in chunk:
                mc_id = exit_record['mc_user_id']
                username = exit_record.get('username') or 'Unknown'
                discord_id = exit_record.get('discord_id')
                exit_time = exit_record['exit_detected_at'][:16]
                role_removed = "✅" if exit_record.get('role_removed') else "❌"
                
                discord_mention = f"<@{discord_id}>" if discord_id else "Not linked"
                
                field_name = f"{username} (MC: {mc_id})"
                field_value = f"Discord: {discord_mention}\nExit: {exit_time}\nRole Removed: {role_removed}"
                
                embed.add_field(name=field_name, value=field_value, inline=False)
            
            embed.set_footer(text=f"Page {(i//10)+1}/{(len(exits)-1)//10+1}")
            pages.append(embed)
        
        # Send first page
        await ctx.send(embed=pages[0])
        
        # If multiple pages, send rest too
        if len(pages) > 1:
            for page in pages[1:]:
                await ctx.send(embed=page)

    @membersync_group.group(name="config")
    async def config_group(self, ctx: commands.Context):
        """Configure channels, roles and DB path."""
        pass

    @config_group.command(name="setreviewchannel")
    async def setreviewchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where review embeds are posted."""
        await self.config.review_channel_id.set(int(channel.id))
        await ctx.send(f"Review channel set to {channel.mention}")

    @config_group.command(name="setlogchannel")
    async def setlogchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the log channel for approvals/denials/prune notices."""
        await self.config.log_channel_id.set(int(channel.id))
        await ctx.send(f"Log channel set to {channel.mention}")

    @config_group.command(name="setverifiedrole")
    async def setverifiedrole(self, ctx: commands.Context, role: discord.Role):
        """Set the Verified role that is granted on approval."""
        await self.config.verified_role_id.set(int(role.id))
        await ctx.send(f"Verified role set to {role.mention}")

    @config_group.command(name="addreviewerrole")
    async def addreviewerrole(self, ctx: commands.Context, role: discord.Role):
        """Add a role that is allowed to approve/deny verifications."""
        roles = await self.config.reviewer_role_ids()
        if int(role.id) not in roles:
            roles.append(int(role.id))
            await self.config.reviewer_role_ids.set(roles)
        await ctx.send(f"Added reviewer role {role.mention}")

    @config_group.command(name="setalliancedb")
    async def setalliancedb(self, ctx: commands.Context, path: str):
        """Set the path to the alliance database file (alliance.db or members_v2.db)."""
        await self.config.alliance_db_path.set(path)
        await ctx.send(f"Alliance DB path set to `{path}`")

    @config_group.command(name="setminthreshold")
    async def setminthreshold(self, ctx: commands.Context, count: int):
        """
        Set minimum member count threshold for prune safety checks.
        
        If the VIEW returns fewer members than this, prune will abort to prevent mass role removal.
        Recommended: Set to ~50% of your expected member count (e.g., 838 for 1676 members).
        
        Default: 800
        """
        if count < 1:
            await ctx.send("❌ Threshold must be at least 1")
            return
        
        await self.config.prune_min_members.set(count)
        await ctx.send(f"✅ Prune minimum threshold set to **{count} members**\n"
                      f"Prune will abort if VIEW returns fewer than {count} members.")

    @commands.hybrid_command(name="verify")
    @commands.cooldown(1, 30, commands.BucketType.user)
    @commands.guild_only()
    @app_commands.describe(
        mc_id="Your Missionchief User ID (optional, helps if your nickname doesn't match exactly)"
    )
    async def verify(self, ctx: commands.Context, mc_id: Optional[str] = None):
        """Verify yourself as a member of the alliance. Match your server nickname to your MC name or provide your MC-ID."""
        if not isinstance(ctx.author, discord.Member) or not ctx.guild:
            await ctx.send("This can only be used in a server.")
            return

        link = await self.get_link_for_discord(ctx.author.id)
        if link:
            role_id = await self.config.verified_role_id()
            role = ctx.guild.get_role(int(role_id)) if role_id else None
            if role and role not in ctx.author.roles:
                try:
                    await ctx.author.add_roles(role, reason="MemberSync: ensure verified role")
                except Exception:
                    pass
            await ctx.send("You are already verified.")
            return

        name = ctx.author.nick or ctx.author.name
        await ctx.send("Looking you up in the roster... this may take a moment.")

        cand = await self._find_member_in_db(name, mc_id)
        if cand and cand.get("mc_id"):
            rid = await self._send_review_embed(ctx.guild, ctx.author, str(cand["mc_id"]), str(cand.get("name") or name))
            await ctx.send("Found you. A reviewer will approve or deny shortly.")
            return

        q = await self.config.queue()
        q[str(ctx.author.id)] = {
            "attempts": 0,
            "enqueued_at": utcnow_iso(),
            "by": name,
            "mc_id": mc_id or "",
            "guild_id": int(ctx.guild.id),
        }
        await self.config.queue.set(q)
        try:
            await ctx.author.send("We couldn't find you yet. I've queued your verification and will retry automatically for up to ~1 hour.")
        except Exception:
            pass
        await ctx.send("I couldn't find you yet. I've queued your verification and will retry automatically.")

    @membersync_group.group(name="retro")
    async def retro_group(self, ctx: commands.Context):
        """Tools to link existing verified members based on exact nickname matches."""
        pass

    async def _find_by_exact_name(self, name: str) -> Optional[Tuple[str, str]]:
        """Find member by exact name match in members_current VIEW."""
        rows = await self._query_alliance(
            "SELECT name, user_id, mc_user_id, profile_href FROM members_current WHERE lower(name)=?",
            (_lower(name),)
        )
        if not rows:
            return None
        
        r = dict(rows[0])
        mcid = r.get("user_id") or r.get("mc_user_id")
        if not mcid:
            href = r.get("profile_href") or ""
            m = re.search(r"/users/(\d+)", href)
            if m:
                mcid = m.group(1)
        if not mcid:
            return None
        return (r.get("name") or name, str(mcid))

    @retro_group.command(name="scan")
    async def retro_scan(self, ctx: commands.Context):
        """Show how many current Verified members can be linked by exact nickname match."""
        role_id = await self.config.verified_role_id()
        role = ctx.guild.get_role(int(role_id)) if role_id else None
        if not role:
            await ctx.send("Verified role not configured.")
            return
        
        total_verified = len(role.members)
        already_linked = 0
        matchable = 0
        
        for m in role.members:
            if await self.get_link_for_discord(m.id):
                already_linked += 1
                continue
            hit = await self._find_by_exact_name(m.nick or m.name)
            if hit:
                matchable += 1
        
        embed = discord.Embed(
            title="📊 Retro Scan Results",
            color=discord.Color.blue()
        )
        embed.add_field(name="Total Verified", value=f"{total_verified} members", inline=True)
        embed.add_field(name="Already Linked", value=f"{already_linked} members", inline=True)
        embed.add_field(name="Can Auto-Link", value=f"{matchable} members", inline=True)
        embed.add_field(name="No Match", value=f"{total_verified - already_linked - matchable} members", inline=True)
        
        if matchable > 0:
            embed.set_footer(text=f"Use [p]membersync retro apply to link {matchable} members")
        elif total_verified == already_linked:
            embed.set_footer(text="✅ All verified members are already linked!")
        else:
            embed.set_footer(text="⚠️ Some members have nicknames that don't match MC names. Use [p]membersync retro debug for details.")
        
        await ctx.send(embed=embed)
    
    @retro_group.command(name="debug")
    async def retro_debug(self, ctx: commands.Context, limit: int = 10):
        """Debug why retro scan finds no matches. Shows unlinked members and their nicknames."""
        role_id = await self.config.verified_role_id()
        role = ctx.guild.get_role(int(role_id)) if role_id else None
        if not role:
            await ctx.send("Verified role not configured.")
            return
        
        unlinked = []
        
        for m in role.members:
            if await self.get_link_for_discord(m.id):
                continue
            
            nickname = m.nick or m.name
            hit = await self._find_by_exact_name(nickname)
            
            unlinked.append({
                'member': m,
                'nickname': nickname,
                'found': hit is not None,
                'mc_name': hit[0] if hit else None,
                'mc_id': hit[1] if hit else None
            })
            
            if len(unlinked) >= limit:
                break
        
        if not unlinked:
            await ctx.send("✅ All verified members are already linked!")
            return
        
        embed = discord.Embed(
            title=f"🔍 Retro Debug (showing {len(unlinked)} unlinked members)",
            color=discord.Color.orange(),
            description="These members have Verified role but no link:"
        )
        
        for u in unlinked:
            status = "✅ Found" if u['found'] else "❌ Not found"
            value = f"Discord Nickname: `{u['nickname']}`\n"
            if u['found']:
                value += f"MC Name: `{u['mc_name']}`\nMC ID: `{u['mc_id']}`"
            else:
                value += "No exact match in database"
            
            embed.add_field(
                name=f"{status} - {u['member'].mention}",
                value=value,
                inline=False
            )
        
        if len(unlinked) >= limit:
            embed.set_footer(text=f"Showing first {limit} only. Increase limit parameter to see more.")
        
        await ctx.send(embed=embed)

    @retro_group.command(name="apply")
    async def retro_apply(self, ctx: commands.Context):
        """Apply auto-link for existing Verified members with exact nickname matches."""
        role_id = await self.config.verified_role_id()
        role = ctx.guild.get_role(int(role_id)) if role_id else None
        if not role:
            await ctx.send("Verified role not configured.")
            return
        count = 0
        for m in role.members:
            if await self.get_link_for_discord(m.id):
                continue
            hit = await self._find_by_exact_name(m.nick or m.name)
            if not hit:
                continue
            name, mcid = hit
            await self._approve_link(ctx.guild, m, mcid, approver=ctx.author if isinstance(ctx.author, discord.Member) else None)
            count += 1
        await ctx.send(f"Retro applied: {count} link(s).")

    @membersync_group.group(name="bulk")
    async def bulk_group(self, ctx: commands.Context):
        """Bulk verification tools - scan ALL server members (not just verified)."""
        pass

    @bulk_group.command(name="scan")
    async def bulk_scan(self, ctx: commands.Context):
        """
        Scan ALL server members to see how many can be auto-verified by nickname match.
        """
        await ctx.send("🔍 Scanning all server members... this may take a moment.")
        
        matchable = 0
        already_linked = 0
        
        for member in ctx.guild.members:
            if member.bot:
                continue
            
            if await self.get_link_for_discord(member.id):
                already_linked += 1
                continue
            
            name = member.nick or member.name
            hit = await self._find_by_exact_name(name)
            if hit:
                matchable += 1
        
        embed = discord.Embed(
            title="📊 Bulk Verification Scan Results",
            color=discord.Color.blue(),
            description=f"Scanned {len([m for m in ctx.guild.members if not m.bot])} members"
        )
        embed.add_field(name="✅ Already Linked", value=f"{already_linked} members", inline=True)
        embed.add_field(name="🎯 Can Auto-Verify", value=f"{matchable} members", inline=True)
        embed.add_field(name="❓ No Match", value=f"{len([m for m in ctx.guild.members if not m.bot]) - already_linked - matchable} members", inline=True)
        
        if matchable > 0:
            embed.set_footer(text=f"Use [p]membersync bulk apply to auto-verify {matchable} members")
        
        await ctx.send(embed=embed)

    @bulk_group.command(name="apply")
    async def bulk_apply(self, ctx: commands.Context, confirm: str = None):
        """
        Auto-verify ALL server members with matching nicknames.
        
        Usage: [p]membersync bulk apply CONFIRM
        """
        if confirm != "CONFIRM":
            await ctx.send(
                "⚠️ **Warning:** This will auto-verify ALL matching server members!\n\n"
                f"To proceed, run: `{ctx.prefix}membersync bulk apply CONFIRM`"
            )
            return
        
        await ctx.send("🔄 Starting bulk verification... this may take a minute.")
        
        verified = 0
        skipped_already_linked = 0
        skipped_no_match = 0
        errors = 0
        
        for member in ctx.guild.members:
            if member.bot:
                continue
            
            if await self.get_link_for_discord(member.id):
                skipped_already_linked += 1
                continue
            
            name = member.nick or member.name
            hit = await self._find_by_exact_name(name)
            
            if not hit:
                skipped_no_match += 1
                continue
            
            mc_name, mcid = hit
            
            try:
                await self._approve_link(ctx.guild, member, mcid, approver=ctx.author if isinstance(ctx.author, discord.Member) else None)
                verified += 1
                
                if verified % 10 == 0:
                    await ctx.send(f"⏳ Progress: {verified} members verified...")
            except Exception as e:
                log.error(f"Failed to verify {member.name}: {e}")
                errors += 1
        
        embed = discord.Embed(
            title="✅ Bulk Verification Complete",
            color=discord.Color.green()
        )
        embed.add_field(name="✅ Verified", value=f"{verified} members", inline=True)
        embed.add_field(name="⏭️ Already Linked", value=f"{skipped_already_linked} members", inline=True)
        embed.add_field(name="❌ No Match", value=f"{skipped_no_match} members", inline=True)
        
        if errors > 0:
            embed.add_field(name="⚠️ Errors", value=f"{errors} members", inline=True)
            embed.color = discord.Color.orange()
        
        await ctx.send(embed=embed)

    @bulk_group.command(name="list")
    async def bulk_list(self, ctx: commands.Context):
        """
        Show a list of server members who can be auto-verified.
        """
        await ctx.send("🔍 Finding matchable members...")
        
        matches = []
        
        for member in ctx.guild.members:
            if member.bot:
                continue
            
            if await self.get_link_for_discord(member.id):
                continue
            
            name = member.nick or member.name
            hit = await self._find_by_exact_name(name)
            
            if hit:
                mc_name, mcid = hit
                matches.append((member, mc_name, mcid))
                
                if len(matches) >= 20:
                    break
        
        if not matches:
            await ctx.send("No matchable members found.")
            return
        
        embed = discord.Embed(
            title=f"🎯 Matchable Members (showing {len(matches)})",
            color=discord.Color.blue()
        )
        
        description = []
        for member, mc_name, mcid in matches:
            description.append(f"• {member.mention} ↔️ **{mc_name}** (`{mcid}`)")
        
        embed.description = "\n".join(description)
        
        if len(matches) >= 20:
            embed.set_footer(text="Showing first 20 matches only. Use bulk scan for full count.")
        
        await ctx.send(embed=embed)

    @bulk_group.command(name="restoreroles")
    async def bulk_restore_roles(self, ctx: commands.Context):
        """
        Restore Verified role to all members who have approved links in the database.
        """
        role_id = await self.config.verified_role_id()
        role = ctx.guild.get_role(int(role_id)) if role_id else None
        
        if not role:
            await ctx.send("❌ Verified role not configured!")
            return
        
        await ctx.send("🔄 Scanning database for approved links...")
        
        def _run():
            con = sqlite3.connect(self.links_db)
            con.row_factory = sqlite3.Row
            try:
                return [dict(r) for r in con.execute("SELECT * FROM links WHERE status='approved'")]
            finally:
                con.close()
        
        links = await asyncio.get_running_loop().run_in_executor(None, _run)
        
        if not links:
            await ctx.send("❌ No approved links found in database!")
            return
        
        await ctx.send(f"📊 Found {len(links)} approved links. Checking roles...")
        
        restored = 0
        already_has = 0
        not_in_server = 0
        errors = 0
        
        for link in links:
            discord_id = int(link["discord_id"])
            mc_id = link["mc_user_id"]
            
            member = ctx.guild.get_member(discord_id)
            
            if not member:
                not_in_server += 1
                continue
            
            if role in member.roles:
                already_has += 1
                continue
            
            try:
                await member.add_roles(role, reason="MemberSync: Restore verified role from database")
                restored += 1
                
                if restored % 10 == 0:
                    await ctx.send(f"⏳ Progress: {restored} roles restored...")
                
            except Exception as e:
                log.error(f"Failed to restore role for {member.name}: {e}")
                errors += 1
        
        embed = discord.Embed(
            title="✅ Role Restoration Complete",
            color=discord.Color.green()
        )
        embed.add_field(name="✅ Roles Restored", value=f"{restored} members", inline=True)
        embed.add_field(name="⏭️ Already Had Role", value=f"{already_has} members", inline=True)
        embed.add_field(name="❌ Not in Server", value=f"{not_in_server} members", inline=True)
        
        if errors > 0:
            embed.add_field(name="⚠️ Errors", value=f"{errors} members", inline=True)
            embed.color = discord.Color.orange()
        
        embed.set_footer(text=f"Total links in database: {len(links)}")
        
        await ctx.send(embed=embed)

    @bulk_group.command(name="checkroles")
    async def bulk_check_roles(self, ctx: commands.Context):
        """
        Check how many linked members are missing their Verified role.
        """
        role_id = await self.config.verified_role_id()
        role = ctx.guild.get_role(int(role_id)) if role_id else None
        
        if not role:
            await ctx.send("❌ Verified role not configured!")
            return
        
        await ctx.send("🔍 Checking roles for all linked members...")
        
        def _run():
            con = sqlite3.connect(self.links_db)
            con.row_factory = sqlite3.Row
            try:
                return [dict(r) for r in con.execute("SELECT * FROM links WHERE status='approved'")]
            finally:
                con.close()
        
        links = await asyncio.get_running_loop().run_in_executor(None, _run)
        
        has_role = 0
        missing_role = 0
        not_in_server = 0
        
        for link in links:
            discord_id = int(link["discord_id"])
            member = ctx.guild.get_member(discord_id)
            
            if not member:
                not_in_server += 1
                continue
            
            if role in member.roles:
                has_role += 1
            else:
                missing_role += 1
        
        embed = discord.Embed(
            title="📊 Role Status Check",
            color=discord.Color.blue()
        )
        embed.add_field(name="✅ Has Role", value=f"{has_role} members", inline=True)
        embed.add_field(name="❌ Missing Role", value=f"{missing_role} members", inline=True)
        embed.add_field(name="👻 Not in Server", value=f"{not_in_server} members", inline=True)
        
        if missing_role > 0:
            embed.set_footer(text=f"Use [p]membersync bulk restoreroles to restore {missing_role} roles")
        
        await ctx.send(embed=embed)

    @membersync_group.command(name="link")
    async def link(self, ctx: commands.Context, member: discord.Member, mc_id: str, *, display_name: Optional[str] = None):
        """Manually link a Discord member to an MC-ID as approved."""
        if not await self._user_is_reviewer(ctx.author):
            await ctx.send("You are not allowed to do this.")
            return
        await self._approve_link(ctx.guild, member, mc_id, approver=ctx.author if isinstance(ctx.author, discord.Member) else None)
        await ctx.send(f"Linked {member.mention} to MC `{mc_id}`.")

    @commands.Cog.listener()
    async def on_ready(self):
        async def _loop():
            await self.bot.wait_until_red_ready()
            while True:
                try:
                    await self._prune_once()
                except Exception as e:
                    log.exception("prune loop error: %s", e)
                await asyncio.sleep(3600)
        asyncio.create_task(_loop())

    async def _prune_once(self, guild: Optional[discord.Guild] = None, dry_run: bool = False, manual: bool = False) -> Dict[str, Any]:
        """
        FIXED: Prune verified roles from members no longer in the alliance WITH SAFETY CHECKS.
        """
        if not guild:
            guild = self.bot.guilds[0] if self.bot.guilds else None
        
        if not guild:
            return {'checked': 0, 'left_alliance': 0, 'removed': 0, 'already_gone': 0, 'errors': 0, 'details': [], 'safety_triggered': False}
        
        role_id = await self.config.verified_role_id()
        role = guild.get_role(int(role_id)) if role_id else None
        
        if not role:
            log.warning("No verified role configured for prune check")
            return {'checked': 0, 'left_alliance': 0, 'removed': 0, 'already_gone': 0, 'errors': 0, 'details': [], 'safety_triggered': False}

        db_path = await self.config.alliance_db_path()
        if not db_path:
            log.warning("No alliance database configured for prune check")
            return {'checked': 0, 'left_alliance': 0, 'removed': 0, 'already_gone': 0, 'errors': 0, 'details': [], 'safety_triggered': False}
        
        # Get current member IDs from members_current VIEW
        current_ids: set[str] = set()
        current_members_data: Dict[str, Dict] = {}
        
        try:
            rows = await self._query_alliance("SELECT * FROM members_current")
            
            for r in rows:
                rd = dict(r)
                mc = rd.get("user_id") or rd.get("mc_user_id")
                if not mc:
                    href = rd.get("profile_href") or ""
                    m = re.search(r"/users/(\d+)", href or "")
                    if m:
                        mc = m.group(1)
                if mc:
                    current_ids.add(str(mc))
                    current_members_data[str(mc)] = rd
        except Exception as e:
            log.exception("Failed to query current members: %s", e)
            return {'checked': 0, 'left_alliance': 0, 'removed': 0, 'already_gone': 0, 'errors': 0, 'details': [f"Database error: {e}"], 'safety_triggered': False}

        # CRITICAL SAFETY CHECK
        min_threshold = await self.config.prune_min_members()
        if len(current_ids) < min_threshold:
            log.error(f"🚨 PRUNE SAFETY ABORT: Only {len(current_ids)} members in VIEW (min: {min_threshold})")
            return {
                'checked': 0,
                'left_alliance': 0,
                'removed': 0,
                'already_gone': 0,
                'errors': 0,
                'details': [f"🚨 SAFETY ABORT: Only {len(current_ids)} members in VIEW (expected >{min_threshold})"],
                'safety_triggered': True,
                'view_member_count': len(current_ids),
                'min_threshold': min_threshold
            }

        # Get all approved links
        def _run():
            con = sqlite3.connect(self.links_db)
            con.row_factory = sqlite3.Row
            try:
                return [dict(r) for r in con.execute("SELECT * FROM links WHERE status='approved'")]
            finally:
                con.close()
        
        links = await asyncio.get_running_loop().run_in_executor(None, _run)

        log_ch_id = await self.config.log_channel_id()
        ch = guild.get_channel(int(log_ch_id)) if log_ch_id else None

        # Tracking
        checked = len(links)
        left_alliance = 0
        removed = 0
        already_gone = 0
        errors = 0
        details = []
        
        now = utcnow_iso()

        for link in links:
            did = int(link["discord_id"])
            mcid = str(link["mc_user_id"])
            
            if mcid not in current_ids:
                # This person LEFT the alliance!
                left_alliance += 1
                member = guild.get_member(did)
                
                # Get last known data
                last_known_data = current_members_data.get(mcid, {})
                username = last_known_data.get("name") or link.get("mc_name") or "Unknown"
                rank = last_known_data.get("role") or ""
                credits = last_known_data.get("earned_credits") or 0
                rate = last_known_data.get("contribution_rate") or 0.0
                last_seen = last_known_data.get("scraped_at") or now
                
                # Record the exit
                def _record_exit():
                    con = sqlite3.connect(self.links_db)
                    try:
                        cur = con.execute("SELECT id FROM member_left_alliance WHERE mc_user_id=? AND discord_id=?", (mcid, did))
                        existing = cur.fetchone()
                        
                        if not existing:
                            con.execute("""
                                INSERT INTO member_left_alliance 
                                (mc_user_id, username, discord_id, rank_role, earned_credits, contribution_rate, 
                                 last_seen_at, exit_detected_at, reason, role_removed, notified)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (mcid, username, did, rank, credits, rate, last_seen, now, "auto-detected", 0, 0))
                        else:
                            con.execute("""
                                UPDATE member_left_alliance 
                                SET exit_detected_at=?, role_removed=?, username=?, rank_role=?, earned_credits=?, contribution_rate=?
                                WHERE id=?
                            """, (now, 0, username, rank, credits, rate, existing[0]))
                        
                        con.commit()
                    finally:
                        con.close()
                
                try:
                    await asyncio.get_running_loop().run_in_executor(None, _record_exit)
                except Exception as e:
                    log.error(f"Failed to record exit for MC {mcid}: {e}")
                
                if not member or not role or role not in member.roles:
                    already_gone += 1
                    details.append(f"MC {mcid} ({username}) - Already without role")
                    continue
                
                # Remove role (unless dry run)
                if not dry_run:
                    try:
                        await member.remove_roles(role, reason="MemberSync auto-prune: not in alliance anymore")
                        removed += 1
                        details.append(f"✅ Removed role from {member.mention} (MC {mcid} - {username})")
                        
                        # Update exit record
                        def _update_exit():
                            con = sqlite3.connect(self.links_db)
                            try:
                                con.execute("""
                                    UPDATE member_left_alliance 
                                    SET role_removed=1 
                                    WHERE mc_user_id=? AND discord_id=?
                                """, (mcid, did))
                                con.commit()
                            finally:
                                con.close()
                        
                        await asyncio.get_running_loop().run_in_executor(None, _update_exit)
                        
                        # Log to channel
                        if isinstance(ch, discord.TextChannel):
                            embed = discord.Embed(
                                title="🔎 Auto-Prune: Member Left Alliance",
                                color=discord.Color.red(),
                                timestamp=datetime.utcnow()
                            )
                            embed.add_field(name="Discord", value=f"{member.mention} (`{did}`)", inline=False)
                            embed.add_field(name="MC Name", value=username, inline=True)
                            embed.add_field(name="MC ID", value=f"[{mcid}]({_mc_profile_url(mcid)})", inline=True)
                            embed.add_field(name="Last Rank", value=rank or "Unknown", inline=True)
                            embed.add_field(name="Last Credits", value=f"{credits:,}" if credits else "Unknown", inline=True)
                            embed.add_field(name="Action", value="Verified role removed", inline=False)
                            await ch.send(embed=embed)
                        
                    except Exception as e:
                        errors += 1
                        details.append(f"❌ Failed to remove role from {member.mention}: {e}")
                        log.error(f"Failed to remove role from {member.name}: {e}")
                else:
                    # Dry run
                    details.append(f"🔍 Would remove role from {member.mention} (MC {mcid} - {username})")

        if not manual and removed > 0:
            log.info(f"Auto-prune removed {removed} roles from members who left alliance")
        
        return {
            'checked': checked,
            'left_alliance': left_alliance,
            'removed': removed,
            'already_gone': already_gone,
            'errors': errors,
            'details': details,
            'safety_triggered': False
        }


async def setup(bot: Red):
    await bot.add_cog(MemberSync(bot))
