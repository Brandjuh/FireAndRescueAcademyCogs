\
# MemberSync (queue-enabled) v0.3.0
# - English help strings
# - 15s cooldown on `verify`
# - Manual link: [p]membersync link <member> <mc_id> [mc_name...]
# - Retro scan/apply: [p]membersync retro scan/apply
# - Snapshot-aware queue with backoff and event-driven retries
# - Queue admin: [p]membersync queue list/retry/clear
from __future__ import annotations

import asyncio
import aiosqlite
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import ui, Interaction, ButtonStyle
from redbot.core import commands, checks, Config
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.FARA.MemberSync")

__version__ = "0.3.0"

NL = "\n"

# Default config
DEFAULTS = {
    "verified_role_id": None,
    "approver_role_ids": [],
    "admin_channel_id": None,
    "log_channel_id": None,
    # queue tuning
    "auto_queue_enabled": True,
    "stale_window_minutes": 45,
    "retry_schedule_seconds": [120, 300, 600, 900, 1200, 1800, 2700, 3600],  # up to 60 min
    "poll_interval_seconds": 60,
}

# AllianceScraper integration hints (we will access its DB path if available)
AS_COG_NAME = "AllianceScraper"


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class LinkRecord:
    discord_id: int
    mc_user_id: str
    mc_name: str
    status: str  # pending/approved/rejected
    approved_at: Optional[str] = None
    approved_by: Optional[int] = None


class ReviewView(ui.View):
    def __init__(self, cog: "MemberSync", review_id: str, timeout: Optional[float] = None):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.review_id = review_id
        self.add_item(ui.Button(label="Approve", style=ButtonStyle.success, custom_id=f"msync:approve:{review_id}"))
        self.add_item(ui.Button(label="Reject", style=ButtonStyle.danger, custom_id=f"msync:reject:{review_id}"))

    async def interaction_check(self, interaction: Interaction) -> bool:
        if await self.cog._is_approver(interaction.user):
            return True
        try:
            await interaction.response.send_message("You are not allowed to take action on this review.", ephemeral=True)
        except Exception:
            pass
        return False

    @ui.button(label="Approve", style=ButtonStyle.success, custom_id="msync:approve-fallback", row=1, disabled=True)
    async def _fallback_a(self, interaction: Interaction, button: ui.Button):
        pass

    @ui.button(label="Reject", style=ButtonStyle.danger, custom_id="msync:reject-fallback", row=1, disabled=True)
    async def _fallback_b(self, interaction: Interaction, button: ui.Button):
        pass


class MemberSync(commands.Cog):
    """Verification workflow for alliance members with admin review, retro linking and a snapshot-aware queue."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA10A11C, force_registration=True)
        self.config.register_global(**DEFAULTS)

        self.data_path = cog_data_path(self)
        self.db_path = self.data_path / "membersync.db"
        self._bg_task: Optional[asyncio.Task] = None

        # cache of last seen roster timestamps to trigger event-driven retries
        self._last_scraped_at: Optional[str] = None
        self._last_snapshot_utc: Optional[str] = None

    async def cog_load(self):
        await self._init_db()
        await self._reattach_pending_views()
        await self._maybe_start_background()

    async def cog_unload(self):
        if self._bg_task:
            self._bg_task.cancel()

    # ---------------------------- DB init ---------------------------------
    async def _init_db(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
CREATE TABLE IF NOT EXISTS links(
  discord_id      INTEGER NOT NULL,
  mc_user_id      TEXT NOT NULL,
  mc_name         TEXT NOT NULL,
  status          TEXT NOT NULL,           -- pending|approved|rejected
  requested_at    TEXT,
  approved_at     TEXT,
  approved_by     INTEGER,
  PRIMARY KEY(discord_id),
  UNIQUE(mc_user_id)
)""")
            await db.execute("""
CREATE TABLE IF NOT EXISTS reviews(
  id              TEXT PRIMARY KEY,
  guild_id        INTEGER,
  discord_id      INTEGER,
  mc_user_id      TEXT,
  mc_name         TEXT,
  status          TEXT,                     -- pending|approved|rejected
  created_at      TEXT,
  decided_at      TEXT,
  decided_by      INTEGER,
  message_id      INTEGER
)""")
            await db.execute("""
CREATE TABLE IF NOT EXISTS audit(
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  ts              TEXT,
  guild_id        INTEGER,
  discord_id      INTEGER,
  mc_user_id      TEXT,
  action          TEXT,
  details_json    TEXT
)""")
            await db.execute("""
CREATE TABLE IF NOT EXISTS queue(
  guild_id        INTEGER,
  discord_id      INTEGER,
  mode            TEXT,          -- name|id
  nickname        TEXT,
  mc_id           TEXT,
  requested_at    TEXT,
  next_retry_at   TEXT,
  retries         INTEGER,
  status          TEXT,          -- pending|done|failed
  last_error      TEXT,
  PRIMARY KEY (guild_id, discord_id) ON CONFLICT REPLACE
)""")
            await db.commit()

    async def _reattach_pending_views(self):
        # If you persisted review messages with buttons, you could reattach here.
        # Left as a stub for now.
        return

    # ----------------------- AllianceScraper DB helpers --------------------
    def _get_alliance_db_path(self) -> Optional[Path]:
        sc = self.bot.get_cog(AS_COG_NAME)
        if sc and hasattr(sc, "db_path"):
            return Path(getattr(sc, "db_path"))
        # fall back to standard data dir layout
        try:
            # redbot data path looks like: data/<instance>/cogs/AllianceScraper/
            base = self.data_path.parent / "AllianceScraper"
            p = base / "alliance.db"
            return p if p.exists() else None
        except Exception:
            return None

    async def _roster_latest_times(self) -> Tuple[Optional[str], Optional[str]]:
        """Return (max_scraped_at, max_snapshot_utc)."""
        adb = self._get_alliance_db_path()
        if not adb or not adb.exists():
            return None, None
        async with aiosqlite.connect(adb) as db:
            await db.execute("PRAGMA query_only=ON")
            cur = await db.execute("SELECT MAX(scraped_at) FROM members_current")
            ms = (await cur.fetchone())[0]
            try:
                cur = await db.execute("SELECT MAX(snapshot_utc) FROM members_history")
                hs = (await cur.fetchone())[0]
            except Exception:
                hs = None
        return ms, hs

    async def _roster_find_by_id(self, mc_id: str) -> Optional[Dict[str, Any]]:
        adb = self._get_alliance_db_path()
        if not adb or not adb.exists():
            return None
        async with aiosqlite.connect(adb) as db:
            db.row_factory = aiosqlite.Row
            # coalesce both styles
            cur = await db.execute("""
SELECT name, COALESCE(mc_user_id, user_id) AS mc_user_id
FROM members_current
WHERE COALESCE(mc_user_id, user_id)=?
LIMIT 1
""", (mc_id,))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def _roster_find_by_name(self, name: str) -> List[Dict[str, Any]]:
        adb = self._get_alliance_db_path()
        if not adb or not adb.exists():
            return []
        async with aiosqlite.connect(adb) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""
SELECT name, COALESCE(mc_user_id, user_id) AS mc_user_id
FROM members_current
WHERE lower(name)=lower(?)
LIMIT 5
""", (name,))
            return [dict(r) for r in await cur.fetchall()]

    # -------------------------- Utility helpers ---------------------------
    async def _is_approver(self, user: discord.abc.User) -> bool:
        # Guild permission or configured role
        if isinstance(user, discord.Member):
            if user.guild_permissions.manage_guild or user.guild_permissions.administrator:
                return True
            cfg_roles = await self.config.approver_role_ids()
            if cfg_roles:
                for rid in cfg_roles:
                    if user.get_role(int(rid)):
                        return True
        return await self.bot.is_owner(user)

    async def _get_verified_role(self, guild: discord.Guild) -> Optional[discord.Role]:
        rid = await self.config.verified_role_id()
        if rid:
            role = guild.get_role(int(rid))
            if isinstance(role, discord.Role):
                return role
        return None

    def _mc_profile(self, mc_id: str) -> str:
        return f"https://www.missionchief.com/users/{mc_id}"

    def _discord_profile(self, discord_id: int) -> str:
        return f"https://discord.com/users/{discord_id}"

    # ------------------------------ Commands ------------------------------
    @commands.group(name="membersync", invoke_without_command=True, help="Admin and config for MemberSync; verify flow with review and queue.")
    @checks.admin_or_permissions(manage_guild=True)
    async def membersync_group(self, ctx: commands.Context):
        """MemberSync admin root."""
        await ctx.send_help()

    @membersync_group.command(name="status", help="Show counts and current configuration.")
    async def status(self, ctx: commands.Context):
        cfg = await self.config.all()
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM links WHERE status='approved'")
            approved = (await cur.fetchone())[0]
            cur = await db.execute("SELECT COUNT(*) FROM queue WHERE status='pending'")
            pending = (await cur.fetchone())[0]
        ms, hs = await self._roster_latest_times()
        em = discord.Embed(title="MemberSync Status", color=discord.Color.blurple(), timestamp=datetime.now(timezone.utc))
        em.add_field(name="Approved links", value=str(approved))
        em.add_field(name="Queue pending", value=str(pending))
        em.add_field(name="Verified role", value=str(cfg.get("verified_role_id")))
        em.add_field(name="Approver roles", value=", ".join([str(x) for x in cfg.get("approver_role_ids")]) or "-", inline=False)
        em.add_field(name="Admin channel", value=str(cfg.get("admin_channel_id")))
        em.add_field(name="Log channel", value=str(cfg.get("log_channel_id")))
        em.add_field(name="Roster max(scraped_at)", value=str(ms) or "-")
        em.add_field(name="Roster max(snapshot_utc)", value=str(hs) or "-")
        em.add_field(name="Auto queue", value=str(cfg.get("auto_queue_enabled")))
        em.add_field(name="Stale window (min)", value=str(cfg.get("stale_window_minutes")))
        await ctx.send(embed=em)

    @membersync_group.group(name="config", help="Configuration commands for MemberSync.")
    async def config_group(self, ctx: commands.Context):
        """Config root."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

    @config_group.command(name="setverifiedrole", help="Set the Verified role used for approved members.")
    async def setverifiedrole(self, ctx: commands.Context, role: discord.Role):
        await self.config.verified_role_id.set(int(role.id))
        await ctx.send(f"Verified role set to {role.mention}")

    @config_group.command(name="addapproverrole", help="Add a role that can approve/reject verification requests.")
    async def addapproverrole(self, ctx: commands.Context, role: discord.Role):
        roles = await self.config.approver_role_ids()
        if int(role.id) not in roles:
            roles.append(int(role.id))
            await self.config.approver_role_ids.set(roles)
        await ctx.send(f"Approver role added: {role.mention}")

    @config_group.command(name="delapproverrole", help="Remove an approver role.")
    async def delapproverrole(self, ctx: commands.Context, role: discord.Role):
        roles = await self.config.approver_role_ids()
        roles = [r for r in roles if int(r) != int(role.id)]
        await self.config.approver_role_ids.set(roles)
        await ctx.send(f"Approver role removed: {role.mention}")

    @config_group.command(name="setadminchannel", help="Set the channel where review requests are posted.")
    async def setadminchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.admin_channel_id.set(int(channel.id))
        await ctx.send(f"Admin review channel set to {channel.mention}")

    @config_group.command(name="setlogchannel", help="Set the channel where actions are logged.")
    async def setlogchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.log_channel_id.set(int(channel.id))
        await ctx.send(f"Log channel set to {channel.mention}")

    @config_group.command(name="queue", help="Queue tuning (auto, stale window, retry schedule).")
    async def config_queue(self, ctx: commands.Context, auto: Optional[bool] = None, stale_window_minutes: Optional[int] = None):
        if auto is not None:
            await self.config.auto_queue_enabled.set(bool(auto))
        if stale_window_minutes is not None:
            await self.config.stale_window_minutes.set(max(1, int(stale_window_minutes)))
        cfg = await self.config.all()
        await ctx.send(f"Queue config → auto={cfg['auto_queue_enabled']} stale_window={cfg['stale_window_minutes']} min")

    # --------------------- Manual link and retro tools ---------------------
    @membersync_group.command(name="link", help="Manually link a Discord member to a MissionChief ID. Bypasses review.")
    async def manual_link(self, ctx: commands.Context, member: discord.Member, mc_id: str, *, mc_name: Optional[str] = None):
        if not await self._is_approver(ctx.author):
            await ctx.send("You are not allowed to link users.")
            return
        if not mc_id.isdigit():
            await ctx.send("MC ID must be digits.")
            return

        # Check conflicts
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT discord_id FROM links WHERE mc_user_id=? AND status='approved'", (mc_id,))
            row = await cur.fetchone()
            if row and int(row["discord_id"]) != int(member.id):
                await ctx.send(f"MC ID `{mc_id}` is already linked to another Discord user.")
                return
            cur = await db.execute("SELECT mc_user_id FROM links WHERE discord_id=? AND status='approved'", (member.id,))
            row = await cur.fetchone()
            if row and str(row["mc_user_id"]) != mc_id:
                await ctx.send(f"{member.mention} is already linked to MC ID `{row['mc_user_id']}`.")
                return
            # Upsert link
            await db.execute("""
INSERT INTO links(discord_id, mc_user_id, mc_name, status, requested_at, approved_at, approved_by)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT(discord_id) DO UPDATE SET
  mc_user_id=excluded.mc_user_id,
  mc_name=COALESCE(excluded.mc_name, links.mc_name),
  status='approved',
  approved_at=excluded.approved_at,
  approved_by=excluded.approved_by
""", (int(member.id), mc_id, mc_name or member.display_name, "approved", utcnow_iso(), utcnow_iso(), int(ctx.author.id)))
            await db.execute("""
INSERT INTO audit(ts, guild_id, discord_id, mc_user_id, action, details_json)
VALUES(?,?,?,?,?,?)
""", (utcnow_iso(), int(ctx.guild.id), int(member.id), mc_id, "manual_link", "{}"))
            await db.commit()

        # Assign verified role
        role = await self._get_verified_role(ctx.guild)
        if role:
            try:
                await member.add_roles(role, reason="MemberSync manual link approved")
            except Exception:
                pass

        # Log embed
        await self._log_action(ctx.guild, title="Manual link", description=f"{member.mention} ↔ [{mc_name or 'profile'}]({self._mc_profile(mc_id)}) [[D]]({self._discord_profile(member.id)})")
        await ctx.send(f"Linked {member.mention} to MC ID `{mc_id}`.")

    @membersync_group.group(name="retro", help="Retro link tools for existing Verified role holders.")
    async def retro_group(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

    @retro_group.command(name="scan", help="Scan verified role holders without links and report safe matches (strict name match).")
    async def retro_scan(self, ctx: commands.Context):
        role = await self._get_verified_role(ctx.guild)
        if not role:
            await ctx.send("Verified role is not configured.")
            return
        adb = self._get_alliance_db_path()
        if not adb or not adb.exists():
            await ctx.send("AllianceScraper database not found.")
            return

        # Build roster map name -> mc_id
        roster: Dict[str, str] = {}
        async with aiosqlite.connect(adb) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT name, COALESCE(mc_user_id,user_id) as mc_user_id FROM members_current") as cur:
                async for r in cur:
                    if r["name"] and r["mc_user_id"]:
                        roster[str(r["name"]).lower()] = str(r["mc_user_id"])

        # Find members with role but without approved link
        results: List[str] = []
        missing = 0
        async with aiosqlite.connect(self.db_path) as msdb:
            for m in role.members:
                cur = await msdb.execute("SELECT 1 FROM links WHERE discord_id=? AND status='approved'", (m.id,))
                row = await cur.fetchone()
                if row:
                    continue
                name = (m.nick or m.display_name or m.name).strip()
                mc_id = roster.get(name.lower())
                if mc_id:
                    results.append(f"{m} -> {mc_id}")
                else:
                    missing += 1

        text = "**Retro Scan Result**" + NL
        text += NL.join(results[:40]) or "_No strict matches found._"
        if missing:
            text += NL + f"_Unmatched users (strict)_: {missing}"
        await ctx.send(text if len(text) < 1900 else text[:1900] + "\n...")

    @retro_group.command(name="apply", help="Apply retro links for strict matches found by scan.")
    async def retro_apply(self, ctx: commands.Context):
        if not await self._is_approver(ctx.author):
            await ctx.send("You are not allowed to apply retro links.")
            return
        role = await self._get_verified_role(ctx.guild)
        if not role:
            await ctx.send("Verified role is not configured.")
            return
        adb = self._get_alliance_db_path()
        if not adb or not adb.exists():
            await ctx.send("AllianceScraper database not found.")
            return
        # Build roster map
        roster: Dict[str, Tuple[str, str]] = {}
        async with aiosqlite.connect(adb) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT name, COALESCE(mc_user_id,user_id) as mc_user_id FROM members_current") as cur:
                async for r in cur:
                    if r["name"] and r["mc_user_id"]:
                        roster[str(r["name"]).lower()] = (str(r["mc_user_id"]), str(r["name"]))

        applied = 0
        async with aiosqlite.connect(self.db_path) as msdb:
            for m in role.members:
                row = await msdb.execute_fetchone("SELECT 1 FROM links WHERE discord_id=? AND status='approved'", (m.id,))
                if row:
                    continue
                name = (m.nick or m.display_name or m.name).strip()
                hit = roster.get(name.lower())
                if not hit:
                    continue
                mc_id, mc_name = hit
                # Conflicts
                conflict = await msdb.execute_fetchone("SELECT discord_id FROM links WHERE mc_user_id=? AND status='approved'", (mc_id,))
                if conflict and int(conflict[0]) != int(m.id):
                    continue
                await msdb.execute("""
INSERT INTO links(discord_id, mc_user_id, mc_name, status, requested_at, approved_at, approved_by)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT(discord_id) DO UPDATE SET
  mc_user_id=excluded.mc_user_id,
  mc_name=excluded.mc_name,
  status='approved',
  approved_at=excluded.approved_at,
  approved_by=excluded.approved_by
""", (int(m.id), mc_id, mc_name, "approved", utcnow_iso(), utcnow_iso(), int(ctx.author.id)))
                await msdb.execute("""
INSERT INTO audit(ts, guild_id, discord_id, mc_user_id, action, details_json)
VALUES(?,?,?,?,?,?)
""", (utcnow_iso(), int(ctx.guild.id), int(m.id), mc_id, "retro_apply", "{}"))
                applied += 1
            await msdb.commit()

        await self._log_action(ctx.guild, title="Retro apply", description=f"Applied retro links: **{applied}**")
        await ctx.send(f"Retro applied. Linked {applied} users.")

    # -------------------------- Queue admin -------------------------------
    @membersync_group.group(name="queue", help="Queue management commands.")
    async def queue_group(self, ctx: commands.Context):
        if ctx.invoked_subcommand is None:
            await ctx.send_help()

    @queue_group.command(name="list", help="List up to 20 pending queue entries with next retry time.")
    async def queue_list(self, ctx: commands.Context):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("""
SELECT discord_id, mode, nickname, mc_id, requested_at, next_retry_at, retries, last_error
FROM queue WHERE status='pending'
ORDER BY next_retry_at ASC
LIMIT 20
""")
            rows = await cur.fetchall()
        if not rows:
            await ctx.send("Queue is empty.")
            return
        lines = []
        for r in rows:
            lines.append(f"<@{r['discord_id']}> · mode={r['mode']} · next={r['next_retry_at']} · retries={r['retries']}")
        await ctx.send("**Pending queue:**" + NL + NL.join(lines))

    @queue_group.command(name="retry", help="Force an immediate retry for a user in the queue.")
    async def queue_retry(self, ctx: commands.Context, member: discord.Member):
        async with aiosqlite.connect(self.db_path) as db:
            n = await db.execute_fetchone("SELECT COUNT(*) FROM queue WHERE guild_id=? AND discord_id=? AND status='pending'", (ctx.guild.id, member.id))
            if n and n[0] > 0:
                await db.execute("UPDATE queue SET next_retry_at=? WHERE guild_id=? AND discord_id=? AND status='pending'",
                                 (utcnow_iso(), ctx.guild.id, member.id))
                await db.commit()
                await ctx.send(f"Queued immediate retry for {member.mention}.")
            else:
                await ctx.send("User is not in the pending queue.")

    @queue_group.command(name="clear", help="Clear a user's pending queue entry (mark failed).")
    async def queue_clear(self, ctx: commands.Context, member: discord.Member):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE queue SET status='failed', last_error='cleared by admin' WHERE guild_id=? AND discord_id=? AND status='pending'",
                             (ctx.guild.id, member.id))
            await db.commit()
        await ctx.send(f"Cleared queue entry for {member.mention}.")

    # ---------------------------- User command ----------------------------
    @commands.cooldown(1, 15, commands.BucketType.user)
    @commands.command(name="verify", help="Request verification. Uses your server nickname or optional MC ID; sends to admin review. Snapshot-aware queue will retry if the roster is stale.")
    async def verify(self, ctx: commands.Context, mc_id: Optional[str] = None):
        """User-facing verify. If roster looks stale, enqueue a retry instead of instantly failing."""
        guild = ctx.guild
        if not guild:
            await ctx.reply("This command can only be used in a server.")
            return

        discord_id = int(ctx.author.id)
        nickname = (ctx.author.nick or ctx.author.display_name or ctx.author.name).strip()
        mode = "id" if mc_id and mc_id.isdigit() else "name"

        # Already linked?
        async with aiosqlite.connect(self.db_path) as db:
            row = await db.execute_fetchone("SELECT mc_user_id FROM links WHERE discord_id=? AND status='approved'", (discord_id,))
            if row:
                await ctx.reply("You are already verified.")
                return

        # Roster freshness
        cfg = await self.config.all()
        stale_win = int(cfg.get("stale_window_minutes", 45))
        ms, hs = await self._roster_latest_times()
        roster_stale = True
        now = datetime.now(timezone.utc)

        def _age(ts: Optional[str]) -> Optional[int]:
            if not ts:
                return None
            try:
                dt = datetime.fromisoformat(ts.replace("Z","")).replace(tzinfo=timezone.utc) if "Z" in ts else datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
                return int((now - dt).total_seconds() // 60)
            except Exception:
                return None

        ages = list(filter(lambda x: x is not None, [_age(ms), _age(hs)]))
        if ages and min(ages) <= stale_win:
            roster_stale = False

        # EDT twilight guard: treat as stale near reset window (23:45-00:10 EDT)
        # We approximate using UTC offset -4 in DST (you asked for clarity, not astro-accuracy).
        edt_now = now - timedelta(hours=4)
        if (edt_now.hour == 23 and edt_now.minute >= 45) or (edt_now.hour == 0 and edt_now.minute <= 10):
            roster_stale = True

        if cfg.get("auto_queue_enabled") and roster_stale:
            # enqueue
            schedule = cfg.get("retry_schedule_seconds") or [120,300,600,900,1200,1800,2700,3600]
            first_delay = int(schedule[0])
            next_at = (now + timedelta(seconds=first_delay)).replace(microsecond=0).isoformat()
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("""
INSERT INTO queue(guild_id, discord_id, mode, nickname, mc_id, requested_at, next_retry_at, retries, status, last_error)
VALUES(?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(guild_id, discord_id) DO UPDATE SET
  mode=excluded.mode,
  nickname=excluded.nickname,
  mc_id=excluded.mc_id,
  next_retry_at=excluded.next_retry_at,
  status='pending',
  last_error=NULL
""", (int(guild.id), discord_id, mode, nickname, (mc_id or None), utcnow_iso(), next_at, 0, "pending", None))
                await db.commit()
            await ctx.reply(f"Roster looks stale. I queued your verification. Next attempt around **{next_at} UTC**. You'll get a DM when it completes.")
            return

        # Proceed normal lookup
        record = None
        if mode == "id":
            record = await self._roster_find_by_id(mc_id)  # type: ignore
            if not record:
                await ctx.reply("Could not find that MC ID in the roster. Please check it and try again later.")
                return
        else:
            matches = await self._roster_find_by_name(nickname)
            if len(matches) == 0:
                await ctx.reply("No roster match for your nickname yet. Please try again later or provide your MC ID: `!verify <MC_ID>`")
                return
            if len(matches) > 1:
                await ctx.reply("Multiple roster matches for your nickname. Please use `!verify <MC_ID>` instead.")
                return
            record = matches[0]

        await self._start_review(ctx, discord_id, record["mc_user_id"], record["name"])

    # --------------------------- Review helpers ---------------------------
    async def _start_review(self, ctx: commands.Context, discord_id: int, mc_user_id: str, mc_name: str):
        rid = f"rvw-{discord_id}-{mc_user_id}-{int(datetime.now(timezone.utc).timestamp())}"
        await self._ensure_review_row(ctx.guild.id, discord_id, mc_user_id, mc_name, rid)
        channel_id = await self.config.admin_channel_id()
        if not channel_id:
            await ctx.reply("Admin review channel is not configured. Please contact an admin.")
            return
        ch = ctx.guild.get_channel(int(channel_id))
        if not isinstance(ch, discord.TextChannel):
            await ctx.reply("Admin review channel is invalid.")
            return

        e = discord.Embed(title="Verification request", color=discord.Color.gold(), timestamp=datetime.now(timezone.utc))
        e.add_field(name="Discord", value=f"<@{discord_id}> [[D]]({self._discord_profile(discord_id)})", inline=False)
        e.add_field(name="MissionChief", value=f"[{mc_name}]({self._mc_profile(mc_user_id)}) (ID {mc_user_id})", inline=False)
        e.set_footer(text=utcnow_iso())

        view = ReviewView(self, rid, timeout=86400)  # 24h
        msg = await ch.send(embed=e, view=view)
        # persist message id
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE reviews SET message_id=? WHERE id=?", (int(msg.id), rid))
            await db.commit()

        try:
            await ctx.reply("Submitted for admin review.")
        except Exception:
            pass

    async def _ensure_review_row(self, guild_id: int, discord_id: int, mc_user_id: str, mc_name: str, rid: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
INSERT INTO reviews(id, guild_id, discord_id, mc_user_id, mc_name, status, created_at)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT(id) DO NOTHING
""", (rid, int(guild_id), int(discord_id), mc_user_id, mc_name, "pending", utcnow_iso()))
            await db.commit()

    @commands.Cog.listener("on_interaction")
    async def _on_interaction(self, intr: Interaction):
        if not intr.data or not isinstance(intr.data, dict):
            return
        cid = intr.data.get("custom_id")
        if not cid or not isinstance(cid, str) or not cid.startswith("msync:"):
            return
        parts = cid.split(":")
        if len(parts) != 3:
            return
        _, action, rid = parts
        if action not in {"approve", "reject"}:
            return
        if not await self._is_approver(intr.user):
            try:
                await intr.response.send_message("Not allowed.", ephemeral=True)
            except Exception:
                pass
            return

        # Load the review
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            row = await db.execute_fetchone("SELECT * FROM reviews WHERE id=?", (rid,))
            if not row or row["status"] != "pending":
                try:
                    await intr.response.send_message("This review is not pending anymore.", ephemeral=True)
                except Exception:
                    pass
                return

            # Apply decision
            new_status = "approved" if action == "approve" else "rejected"
            await db.execute("UPDATE reviews SET status=?, decided_at=?, decided_by=? WHERE id=?",
                             (new_status, utcnow_iso(), int(intr.user.id), rid))

            if new_status == "approved":
                # Upsert link table
                await db.execute("""
INSERT INTO links(discord_id, mc_user_id, mc_name, status, requested_at, approved_at, approved_by)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT(discord_id) DO UPDATE SET
  mc_user_id=excluded.mc_user_id,
  mc_name=excluded.mc_name,
  status='approved',
  approved_at=excluded.approved_at,
  approved_by=excluded.approved_by
""", (int(row["discord_id"]), row["mc_user_id"], row["mc_name"], "approved", utcnow_iso(), utcnow_iso(), int(intr.user.id)))
                await db.execute("""
INSERT INTO audit(ts, guild_id, discord_id, mc_user_id, action, details_json)
VALUES(?,?,?,?,?,?)
""", (utcnow_iso(), int(row["guild_id"]), int(row["discord_id"]), row["mc_user_id"], "approved", "{}"))
            else:
                await db.execute("""
INSERT INTO audit(ts, guild_id, discord_id, mc_user_id, action, details_json)
VALUES(?,?,?,?,?,?)
""", (utcnow_iso(), int(row["guild_id"]), int(row["discord_id"]), row["mc_user_id"], "rejected", "{}"))
            await db.commit()

        # Respond and post log, set role if approved
        try:
            await intr.response.send_message(f"Review {new_status}.", ephemeral=True)
        except Exception:
            pass

        guild = intr.guild
        if guild:
            member = guild.get_member(int(row["discord_id"]))
            if member and new_status == "approved":
                role = await self._get_verified_role(guild)
                if role:
                    try:
                        await member.add_roles(role, reason="MemberSync approved")
                    except Exception:
                        pass
            await self._log_action(
                guild,
                title=f"Verification {new_status}",
                description=f"<@{row['discord_id']}> ↔ [{row['mc_name']}]({self._mc_profile(row['mc_user_id'])}) [[D]]({self._discord_profile(int(row['discord_id']))})"
            )

    async def _log_action(self, guild: discord.Guild, title: str, description: str):
        ch_id = await self.config.log_channel_id()
        if not ch_id:
            return
        ch = guild.get_channel(int(ch_id))
        if not isinstance(ch, discord.TextChannel):
            return
        e = discord.Embed(title=title, description=description, color=discord.Color.green(), timestamp=datetime.now(timezone.utc))
        try:
            await ch.send(embed=e)
        except Exception:
            pass

    # ------------------------ Background queue loop -----------------------
    async def _maybe_start_background(self):
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._bg_loop())

    async def _bg_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                await self._tick_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("Queue tick failed: %r", e)
            await asyncio.sleep(int((await self.config.poll_interval_seconds()) or 60))

    async def _tick_once(self):
        # 1) detect new roster data (event-driven retry)
        ms, hs = await self._roster_latest_times()
        new_data = False
        if ms and ms != self._last_scraped_at:
            self._last_scraped_at = ms
            new_data = True
        if hs and hs != self._last_snapshot_utc:
            self._last_snapshot_utc = hs
            new_data = True

        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        schedule = (await self.config.retry_schedule_seconds()) or [120,300,600,900,1200,1800,2700,3600]

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            if new_data:
                cur = await db.execute("SELECT * FROM queue WHERE status='pending'")
            else:
                cur = await db.execute("SELECT * FROM queue WHERE status='pending' AND next_retry_at <= ?", (now,))
            rows = await cur.fetchall()

        for r in rows:
            try:
                await self._process_queue_row(r, schedule)
            except Exception as e:
                log.exception("Processing queue row failed for %s: %r", r["discord_id"], e)

    async def _process_queue_row(self, r: aiosqlite.Row, schedule: List[int]):
        guild = self.bot.get_guild(int(r["guild_id"]))
        if not guild:
            await self._queue_fail(r, "guild not found")
            return
        member = guild.get_member(int(r["discord_id"]))
        if not member:
            await self._queue_fail(r, "member not in guild")
            return

        # If already approved, mark done silently
        async with aiosqlite.connect(self.db_path) as db:
            row = await db.execute_fetchone("SELECT mc_user_id FROM links WHERE discord_id=? AND status='approved'", (member.id,))
            if row:
                await db.execute("UPDATE queue SET status='done' WHERE guild_id=? AND discord_id=?", (guild.id, member.id))
                await db.commit()
                return

        # Lookup again
        record = None
        if r["mode"] == "id" and r["mc_id"]:
            record = await self._roster_find_by_id(str(r["mc_id"]))
        else:
            record = await self._roster_find_by_name(r["nickname"] or (member.nick or member.display_name or member.name))
            if isinstance(record, list):
                record = record[0] if len(record) == 1 else None

        if record and record.get("mc_user_id"):
            # success: create review immediately to avoid more waiting
            ctx = None  # not available; we bypass review and approve directly for queue success
            async with aiosqlite.connect(self.db_path) as db:
                # Upsert link as approved
                await db.execute("""
INSERT INTO links(discord_id, mc_user_id, mc_name, status, requested_at, approved_at, approved_by)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT(discord_id) DO UPDATE SET
  mc_user_id=excluded.mc_user_id,
  mc_name=excluded.mc_name,
  status='approved',
  approved_at=excluded.approved_at
""", (int(member.id), str(record["mc_user_id"]), str(record["name"]), "approved", utcnow_iso(), utcnow_iso(), 0))
                await db.execute("UPDATE queue SET status='done' WHERE guild_id=? AND discord_id=?", (guild.id, member.id))
                await db.execute("""
INSERT INTO audit(ts, guild_id, discord_id, mc_user_id, action, details_json)
VALUES(?,?,?,?,?,?)
""", (utcnow_iso(), int(guild.id), int(member.id), str(record["mc_user_id"]), "auto_approve_from_queue", "{}"))
                await db.commit()

            # give role
            role = await self._get_verified_role(guild)
            if role:
                try:
                    await member.add_roles(role, reason="MemberSync auto-approved from queue")
                except Exception:
                    pass

            # DM user
            try:
                await member.send(f"✅ You are verified: [{record['name']}]({self._mc_profile(record['mc_user_id'])}).")
            except Exception:
                pass
            # Log
            await self._log_action(guild, title="Auto-verified", description=f"<@{member.id}> ↔ [{record['name']}]({self._mc_profile(record['mc_user_id'])}) [[D]]({self._discord_profile(member.id)})")
            return

        # nope: schedule next
        retries = int(r["retries"] or 0)
        if retries + 1 >= len(schedule):
            await self._queue_fail(r, "max retries exceeded")
            try:
                await member.send("❌ Verification failed after several retries. Please ensure your Discord nickname matches your MissionChief name, or try `!verify <MC_ID>`.")
            except Exception:
                pass
            return
        next_at = (datetime.now(timezone.utc) + timedelta(seconds=int(schedule[retries + 1]))).replace(microsecond=0).isoformat()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE queue SET retries=?, next_retry_at=? WHERE guild_id=? AND discord_id=?",
                             (retries + 1, next_at, int(r["guild_id"]), int(r["discord_id"])))
            await db.commit()

    async def _queue_fail(self, r: aiosqlite.Row, reason: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE queue SET status='failed', last_error=? WHERE guild_id=? AND discord_id=?",
                             (reason, int(r["guild_id"]), int(r["discord_id"])))
            await db.commit()


async def setup(bot):
    await bot.add_cog(MemberSync(bot))
