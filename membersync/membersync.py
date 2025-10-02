
from __future__ import annotations

import asyncio
import re
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

import discord
from redbot.core import commands, checks, Config
from redbot.core.data_manager import cog_data_path

from pathlib import Path

# aiosqlite compatibility shim: add execute_fetchone/execute_fetchall if missing
try:
    import aiosqlite  # type: ignore
    if not hasattr(aiosqlite.Connection, "execute_fetchone"):
        async def _exec_fetchone(self, sql, params=()):
            cur = await self.execute(sql, params)
            try:
                return await cur.fetchone()
            finally:
                await cur.close()
        async def _exec_fetchall(self, sql, params=()):
            cur = await self.execute(sql, params)
            try:
                return await cur.fetchall()
            finally:
                await cur.close()
        aiosqlite.Connection.execute_fetchone = _exec_fetchone  # type: ignore
        aiosqlite.Connection.execute_fetchall = _exec_fetchall  # type: ignore
except Exception:
    aiosqlite = None  # type: ignore

__version__ = "0.3.1"

BACKOFF_MINUTES = [2, 5, 10, 15, 20, 30, 45, 60]
EDT_OFFSET = -4  # EDT is UTC-4 during DST

def now_utc_str() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # allow naive as UTC
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

class MemberSync(commands.Cog):
    """MemberSync: verify users and link to MissionChief accounts, with queue + retro tools."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA10A1CE, force_registration=True)
        self.config.register_global(
            verified_role_id=None,
            approver_role_ids=[],
            admin_channel_id=None,
            log_channel_id=None,
            queue_auto=True,
            stale_window_minutes=60,
        )
        self.data_path = cog_data_path(self)
        self.db_path = self.data_path / "membersync.db"
        self._bg: Optional[asyncio.Task] = None
        self._last_scraped_at: Optional[str] = None
        self._last_snapshot: Optional[str] = None

    async def cog_load(self):
        await self._init_db()
        self._bg = asyncio.create_task(self._bg_loop())

    async def cog_unload(self):
        if self._bg:
            self._bg.cancel()

    async def _init_db(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        if aiosqlite is None:
            raise RuntimeError("aiosqlite is required for MemberSync")
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS links(
                    discord_id TEXT PRIMARY KEY,
                    mc_user_id TEXT,
                    mc_name TEXT,
                    status TEXT,
                    created_at_utc TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS verify_queue(
                    discord_id TEXT PRIMARY KEY,
                    guild_id   TEXT,
                    requested_at_utc TEXT,
                    attempts    INTEGER,
                    next_attempt_utc TEXT,
                    wanted_mc_id TEXT
                )
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS ix_links_mc ON links(mc_user_id)
            """)
            await db.commit()

    # ---------------- AllianceScraper DB helpers ----------------

    def _get_alliance_db_path(self) -> Optional[Path]:
        sc = self.bot.get_cog("AllianceScraper")
        if sc and getattr(sc, "db_path", None):
            return Path(getattr(sc, "db_path"))
        # fallback: try known folder name
        guess = cog_data_path(self).parent / "AllianceScraper" / "alliance.db"
        return guess if guess.exists() else None

    async def _roster_latest_times(self) -> Tuple[Optional[str], Optional[str]]:
        """Return (members_current.max(scraped_at|updated_at_utc|last_seen_utc), members_history.max(snapshot_utc))."""
        adb = self._get_alliance_db_path()
        if not adb or not adb.exists():
            return None, None
        con = sqlite3.connect(f"file:{adb}?mode=ro", uri=True)
        try:
            cur = con.cursor()

            def col_exists(table: str, col: str) -> bool:
                try:
                    cur.execute(f"PRAGMA table_info({table})")
                    return any(r[1] == col for r in cur.fetchall())
                except Exception:
                    return False

            mc_ts = None
            for col in ("scraped_at", "updated_at_utc", "last_seen_utc"):
                if col_exists("members_current", col):
                    cur.execute(f"SELECT MAX({col}) FROM members_current")
                    mc_ts = cur.fetchone()[0]
                    if mc_ts:
                        break

            mh_ts = None
            if col_exists("members_history", "snapshot_utc"):
                cur.execute("SELECT MAX(snapshot_utc) FROM members_history")
                mh_ts = cur.fetchone()[0]

            return mc_ts, mh_ts
        finally:
            con.close()

    def _ed_today_window(self) -> Tuple[datetime, datetime]:
        # Returns today's start and end in EDT translated to UTC
        now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        edt_now = now_utc + timedelta(hours=EDT_OFFSET)
        edt_start = edt_now.replace(hour=0, minute=0, second=0, microsecond=0)
        edt_end = edt_start + timedelta(days=1) - timedelta(seconds=1)
        # convert back to UTC
        start_utc = (edt_start - timedelta(hours=EDT_OFFSET)).astimezone(timezone.utc)
        end_utc = (edt_end - timedelta(hours=EDT_OFFSET)).astimezone(timezone.utc)
        return start_utc, end_utc

    def _is_roster_stale(self, scraped_ts: Optional[str], stale_window_minutes: int) -> bool:
        if not scraped_ts:
            return True
        dt = parse_iso(scraped_ts)
        if not dt:
            return True
        age = datetime.utcnow().replace(tzinfo=timezone.utc) - dt
        return age.total_seconds() > stale_window_minutes * 60

    def _select_member_sql(self) -> str:
        # handle schemas with mc_user_id or user_id
        return """
        SELECT
            name,
            COALESCE(mc_user_id, user_id) AS mcid,
            profile_href
        FROM members_current
        WHERE (LOWER(name)=LOWER(?) OR COALESCE(mc_user_id, user_id)=?)
        LIMIT 2
        """

    def _select_member_by_id_sql(self) -> str:
        return """
        SELECT
            name,
            COALESCE(mc_user_id, user_id) AS mcid,
            profile_href
        FROM members_current
        WHERE COALESCE(mc_user_id, user_id)=?
        LIMIT 2
        """

    def _profile_url(self, mcid: str) -> str:
        return f"https://www.missionchief.com/users/{mcid}"

    # ---------------- Queue machinery ----------------

    async def _queue_up(self, guild_id: int, discord_id: int, wanted_mc_id: Optional[str] = None):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO verify_queue(discord_id, guild_id, requested_at_utc, attempts, next_attempt_utc, wanted_mc_id)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(discord_id) DO UPDATE SET wanted_mc_id=excluded.wanted_mc_id
            """, (str(discord_id), str(guild_id), now_utc_str(), 0, now_utc_str(), str(wanted_mc_id) if wanted_mc_id else None))
            await db.commit()

    async def _queue_pop(self, discord_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM verify_queue WHERE discord_id=?", (str(discord_id),))
            await db.commit()

    async def _queue_list(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            rows = await db.execute_fetchall("SELECT * FROM verify_queue ORDER BY next_attempt_utc ASC LIMIT 50")
            return [dict(r) for r in rows]

    async def _queue_process_once(self) -> int:
        # process due items
        if aiosqlite is None:
            return 0
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        processed = 0
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row  # type: ignore
            cur = await db.execute("SELECT * FROM verify_queue")
            rows = await cur.fetchall()
        for r in rows:
            try:
                due = parse_iso(r["next_attempt_utc"]) or now
                if due > now:
                    continue
                member = self.bot.get_user(int(r["discord_id"])) or None
                guild = self.bot.guilds[0] if self.bot.guilds else None
                if not guild:
                    continue
                # try verify again
                ok, msg = await self._attempt_verify(guild, discord_id=int(r["discord_id"]), mc_id=r.get("wanted_mc_id"))
                if ok:
                    await self._queue_pop(int(r["discord_id"]))
                else:
                    # backoff
                    attempts = int(r["attempts"] or 0) + 1
                    idx = min(attempts - 1, len(BACKOFF_MINUTES) - 1)
                    next_dt = now + timedelta(minutes=BACKOFF_MINUTES[idx])
                    async with aiosqlite.connect(self.db_path) as db2:
                        await db2.execute("""
                            UPDATE verify_queue SET attempts=?, next_attempt_utc=? WHERE discord_id=?
                        """, (attempts, next_dt.isoformat(), str(r["discord_id"])))
                        await db2.commit()
                processed += 1
            except Exception:
                continue
        return processed

    async def _bg_loop(self):
        await self.bot.wait_until_ready()
        while True:
            try:
                # detect roster update and fast-path the queue
                mc_ts, mh_ts = await self._roster_latest_times()
                if mc_ts and mc_ts != self._last_scraped_at:
                    self._last_scraped_at = mc_ts
                    # new data -> try queue immediately
                    await self._queue_process_once()
                # otherwise normal cycle
                await self._queue_process_once()
            except Exception:
                pass
            await asyncio.sleep(60)

    # ---------------- Core verify logic ----------------

    async def _attempt_verify(self, guild: discord.Guild, discord_id: int, mc_id: Optional[str] = None) -> Tuple[bool, str]:
        user = guild.get_member(discord_id) or self.bot.get_user(discord_id)
        member: Optional[discord.Member] = guild.get_member(discord_id) if guild else None
        shown_name = member.nick if member and member.nick else (member.name if member else (user.name if user else str(discord_id)))

        adb = self._get_alliance_db_path()
        if not adb or not adb.exists():
            return False, "Alliance roster database not found."

        # get roster freshness
        stale_window = int(await self.config.stale_window_minutes())
        mc_ts, mh_ts = await self._roster_latest_times()
        roster_stale = self._is_roster_stale(mc_ts, stale_window)

        # actual match
        found: List[Tuple[str, str, Optional[str]]] = []
        try:
            con = sqlite3.connect(f"file:{adb}?mode=ro", uri=True)
            con.row_factory = sqlite3.Row
            cur = con.cursor()
            if mc_id:
                cur.execute(self._select_member_by_id_sql(), (str(mc_id),))
                rows = cur.fetchall()
            else:
                cur.execute(self._select_member_sql(), (shown_name, str(mc_id or "")))
                rows = cur.fetchall()
            for r in rows or []:
                name = r["name"]
                mcid = r["mcid"]
                href = r["profile_href"]
                if mcid:
                    found.append((name, mcid, href))
        finally:
            try:
                con.close()
            except Exception:
                pass

        if not found:
            if roster_stale and await self.config.queue_auto():
                # queue and tell user
                await self._queue_up(guild.id, discord_id, wanted_mc_id=str(mc_id) if mc_id else None)
                return False, "No match yet in roster, queued for automatic retry after next scrape."
            return False, "No matching roster entry found."

        # tie-break exact nickname match first
        if len(found) > 1 and (member and member.nick):
            f2 = [t for t in found if t[0].lower() == member.nick.lower()]
            if len(f2) == 1:
                found = f2
        if len(found) != 1:
            return False, "Ambiguous roster match. Please use `!verify <MC_ID>`."

        name, mcid, href = found[0]

        # write link
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO links(discord_id, mc_user_id, mc_name, status, created_at_utc)
                VALUES(?,?,?,?,?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    mc_user_id=excluded.mc_user_id,
                    mc_name=excluded.mc_name,
                    status='approved'
            """, (str(discord_id), str(mcid), name, "approved", now_utc_str()))
            await db.commit()

        # role + notify
        await self._grant_verified_role(guild, discord_id)
        await self._notify_success(guild, discord_id, mcid, name)

        return True, f"Linked to {name} ({mcid})."

    async def _grant_verified_role(self, guild: discord.Guild, discord_id: int):
        role_id = await self.config.verified_role_id()
        if not role_id:
            return
        role = guild.get_role(int(role_id))
        member = guild.get_member(discord_id)
        if role and member and role not in member.roles:
            try:
                await member.add_roles(role, reason="MemberSync verification")
            except Exception:
                pass

    async def _notify_success(self, guild: discord.Guild, discord_id: int, mcid: str, name: str):
        url = f"https://www.missionchief.com/users/{mcid}"
        user = guild.get_member(discord_id) or self.bot.get_user(discord_id)
        try:
            if user:
                await user.send(f"Verification successful. Linked to **{name}** (<{url}>).")
        except Exception:
            pass
        log_id = await self.config.log_channel_id()
        if log_id:
            ch = guild.get_channel(int(log_id))
            if isinstance(ch, discord.TextChannel):
                try:
                    await ch.send(f"✅ Linked <@{discord_id}> to **{name}** (<{url}>).")
                except Exception:
                    pass

    # ---------------- Commands ----------------

    @commands.group(name="membersync")
    @checks.admin_or_permissions(manage_guild=True)
    async def ms_group(self, ctx: commands.Context):
        """MemberSync administration and tools."""

    @ms_group.command(name="status")
    async def status(self, ctx: commands.Context):
        """Show config and latest roster timestamps."""
        cfg = await self.config.all()
        mc_ts, mh_ts = await self._roster_latest_times()
        embed = discord.Embed(title="MemberSync status", colour=discord.Colour.blurple())
        embed.add_field(name="Verified role", value=str(cfg["verified_role_id"]), inline=False)
        embed.add_field(name="Approver roles", value=", ".join(map(str, cfg["approver_role_ids"])) or "-", inline=False)
        embed.add_field(name="Admin channel", value=str(cfg["admin_channel_id"]), inline=False)
        embed.add_field(name="Log channel", value=str(cfg["log_channel_id"]), inline=False)
        embed.add_field(name="Queue auto", value=str(cfg["queue_auto"]), inline=True)
        embed.add_field(name="Stale window (min)", value=str(cfg["stale_window_minutes"]), inline=True)
        embed.add_field(name="members_current latest", value=mc_ts or "-", inline=False)
        embed.add_field(name="members_history latest", value=mh_ts or "-", inline=False)
        await ctx.send(embed=embed)

    @ms_group.group(name="config")
    async def config_group(self, ctx: commands.Context):
        """Configure roles and channels."""

    @config_group.command(name="setverifiedrole")
    async def set_verified_role(self, ctx: commands.Context, role: discord.Role):
        """Set the Verified role that is granted on successful link."""
        await self.config.verified_role_id.set(int(role.id))
        await ctx.send(f"Verified role set to {role.mention}")

    @config_group.command(name="addapproverrole")
    async def add_approver_role(self, ctx: commands.Context, role: discord.Role):
        """Add a role that is allowed to approve/retro actions."""
        ids = set(await self.config.approver_role_ids())
        ids.add(int(role.id))
        await self.config.approver_role_ids.set(list(ids))
        await ctx.send(f"Approver role added: {role.mention}")

    @config_group.command(name="delapproverrole")
    async def del_approver_role(self, ctx: commands.Context, role: discord.Role):
        """Remove an approver role."""
        ids = set(await self.config.approver_role_ids())
        ids.discard(int(role.id))
        await self.config.approver_role_ids.set(list(ids))
        await ctx.send(f"Approver role removed: {role.mention}")

    @config_group.command(name="setadminchannel")
    async def set_admin_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the admin channel for review messages (if used)."""
        await self.config.admin_channel_id.set(int(channel.id))
        await ctx.send(f"Admin channel set to {channel.mention}")

    @config_group.command(name="setlogchannel")
    async def set_log_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the log channel where actions are logged."""
        await self.config.log_channel_id.set(int(channel.id))
        await ctx.send(f"Log channel set to {channel.mention}")

    @config_group.command(name="queue")
    async def set_queue(self, ctx: commands.Context, auto: Optional[bool] = None, stale_window_minutes: Optional[int] = None):
        """Configure queue behavior. Example: membersync config queue true 60"""
        if auto is not None:
            await self.config.queue_auto.set(bool(auto))
        if stale_window_minutes is not None:
            await self.config.stale_window_minutes.set(max(5, int(stale_window_minutes)))
        cfg = await self.config.all()
        await ctx.send(f"Queue config → auto={cfg['queue_auto']} stale_window_minutes={cfg['stale_window_minutes']}")

    # Public verify command
    @commands.cooldown(1, 15, commands.BucketType.user)
    @commands.command(name="verify")
    async def verify(self, ctx: commands.Context, mc_id: Optional[str] = None):
        """Verify yourself by matching your server nickname to the roster, or provide MC_ID for disambiguation."""
        await ctx.send("Checking roster, please wait…")
        ok, msg = await self._attempt_verify(ctx.guild, ctx.author.id, mc_id)
        if ok:
            await ctx.send("Verification successful.")
        else:
            await ctx.send(msg)

    # Manual link by approver
    @ms_group.command(name="link")
    async def manual_link(self, ctx: commands.Context, member: discord.Member, mc_id: str, *, mc_name: Optional[str] = None):
        """Manually link a Discord member to a MC user id (approver role required)."""
        # permission check
        approver_ids = set(await self.config.approver_role_ids())
        if not any(r.id in approver_ids for r in member.guild.get_member(ctx.author.id).roles):
            await ctx.send("You are not allowed to use this command.")
            return

        if not mc_name:
            # try resolve name from roster
            adb = self._get_alliance_db_path()
            mc_name = f"MC:{mc_id}"
            if adb and adb.exists():
                con = sqlite3.connect(f"file:{adb}?mode=ro", uri=True)
                con.row_factory = sqlite3.Row
                cur = con.cursor()
                try:
                    cur.execute(self._select_member_by_id_sql(), (mc_id,))
                    r = cur.fetchone()
                    if r and r["name"]:
                        mc_name = r["name"]
                finally:
                    con.close()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO links(discord_id, mc_user_id, mc_name, status, created_at_utc)
                VALUES(?,?,?,?,?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    mc_user_id=excluded.mc_user_id,
                    mc_name=excluded.mc_name,
                    status='approved'
            """, (str(member.id), str(mc_id), mc_name, "approved", now_utc_str()))
            await db.commit()

        await self._grant_verified_role(ctx.guild, member.id)
        await ctx.send(f"Linked {member.mention} → {mc_name} ({mc_id})")

    # Retro tools
    @ms_group.group(name="retro")
    async def retro(self, ctx: commands.Context):
        """Retro scan/apply for members who already have the Verified role but no link yet."""

    @retro.command(name="scan")
    async def retro_scan(self, ctx: commands.Context):
        """Scan for members with Verified role but no approved link; show candidates resolvable by nickname."""
        role_id = await self.config.verified_role_id()
        if not role_id:
            await ctx.send("Verified role not configured.")
            return
        role = ctx.guild.get_role(int(role_id))
        if not role:
            await ctx.send("Verified role not found in this guild.")
            return

        # fetch linked discord ids
        linked_ids: set[int] = set()
        async with aiosqlite.connect(self.db_path) as db:
            rows = await db.execute_fetchall("SELECT discord_id FROM links WHERE status='approved'")
            linked_ids = {int(r["discord_id"]) for r in rows}

        adb = self._get_alliance_db_path()
        if not adb or not adb.exists():
            await ctx.send("Alliance roster database not found.")
            return

        # build name->(mcid,name) map
        con = sqlite3.connect(f"file:{adb}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("""
            SELECT LOWER(name) AS lname, COALESCE(mc_user_id, user_id) AS mcid, name
            FROM members_current
            WHERE name IS NOT NULL AND (mc_user_id IS NOT NULL OR user_id IS NOT NULL)
        """)
        m = {}
        for r in cur.fetchall():
            lname = r["lname"]
            mcid = r["mcid"]
            name = r["name"]
            if lname not in m:
                m[lname] = []
            m[lname].append((mcid, name))
        con.close()

        candidates = []
        for member in role.members:
            if member.id in linked_ids:
                continue
            nick = member.nick or member.name
            arr = m.get(nick.lower(), [])
            if len(arr) == 1:
                candidates.append((member, arr[0][0], arr[0][1]))

        if not candidates:
            await ctx.send("No retro candidates found.")
            return
        lines = [f"- {mem.mention} → {name} ({mcid})" for mem, mcid, name in candidates[:25]]
        await ctx.send("Retro candidates (first 25):\n" + "\n".join(lines))

    @retro.command(name="apply")
    async def retro_apply(self, ctx: commands.Context):
        """Apply links for all scan-candidates with exact unique nickname match."""
        role_id = await self.config.verified_role_id()
        if not role_id:
            await ctx.send("Verified role not configured.")
            return
        role = ctx.guild.get_role(int(role_id))
        if not role:
            await ctx.send("Verified role not found in this guild.")
            return

        # existing links
        linked_ids: set[int] = set()
        async with aiosqlite.connect(self.db_path) as db:
            rows = await db.execute_fetchall("SELECT discord_id FROM links WHERE status='approved'")
            linked_ids = {int(r["discord_id"]) for r in rows}

        adb = self._get_alliance_db_path()
        if not adb or not adb.exists():
            await ctx.send("Alliance roster database not found.")
            return

        # map
        con = sqlite3.connect(f"file:{adb}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("""
            SELECT LOWER(name) AS lname, COALESCE(mc_user_id, user_id) AS mcid, name
            FROM members_current
            WHERE name IS NOT NULL AND (mc_user_id IS NOT NULL OR user_id IS NOT NULL)
        """)
        m = {}
        for r in cur.fetchall():
            lname = r["lname"]
            mcid = r["mcid"]
            name = r["name"]
            if lname not in m:
                m[lname] = []
            m[lname].append((mcid, name))
        con.close()

        applied = 0
        async with aiosqlite.connect(self.db_path) as db:
            for member in role.members:
                if member.id in linked_ids:
                    continue
                nick = member.nick or member.name
                arr = m.get(nick.lower(), [])
                if len(arr) == 1:
                    mcid, name = arr[0]
                    await db.execute("""
                        INSERT INTO links(discord_id, mc_user_id, mc_name, status, created_at_utc)
                        VALUES(?,?,?,?,?)
                        ON CONFLICT(discord_id) DO UPDATE SET
                            mc_user_id=excluded.mc_user_id,
                            mc_name=excluded.mc_name,
                            status='approved'
                    """, (str(member.id), str(mcid), name, "approved", now_utc_str()))
                    applied += 1
            await db.commit()

        await ctx.send(f"Applied {applied} retro link(s).")

    # Queue admin
    @ms_group.group(name="queue")
    async def queue_group(self, ctx: commands.Context):
        """Inspect and manage the verification queue."""

    @queue_group.command(name="list")
    async def queue_list(self, ctx: commands.Context):
        """Show up to 20 pending queue entries ordered by due time."""
        rows = await self._queue_list()
        if not rows:
            await ctx.send("Queue is empty.")
            return
        lines = []
        for r in rows[:20]:
            due = r.get("next_attempt_utc") or "-"
            wanted = r.get("wanted_mc_id") or "-"
            lines.append(f"- <@{r['discord_id']}> next: `{due}` wanted: {wanted}")
        await ctx.send("\n".join(lines))

    @queue_group.command(name="retry")
    async def queue_retry(self, ctx: commands.Context, member: discord.Member):
        """Force an immediate retry for a queued user."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                UPDATE verify_queue SET next_attempt_utc=? WHERE discord_id=?
            """, (now_utc_str(), str(member.id)))
            await db.commit()
        await ctx.send(f"Retry scheduled now for {member.mention}")

    @queue_group.command(name="clear")
    async def queue_clear(self, ctx: commands.Context, member: discord.Member):
        """Remove a user from the queue."""
        await self._queue_pop(member.id)
        await ctx.send(f"Removed {member.mention} from queue.")
