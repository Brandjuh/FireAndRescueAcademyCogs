
from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord.ext import commands
from redbot.core import Config, checks
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.FARA.MemberSync")

# -------------------- Defaults provided by user --------------------
DEFAULT_REVIEWER_ROLE_ID = 544117282167586836
DEFAULT_VERIFIED_ROLE_ID = 565988933113085952
DEFAULT_LOG_CHANNEL_ID   = 668874513663918100
DEFAULT_ADMIN_CHANNEL_ID = 1421256548977606827

# -------------------- Config defaults --------------------
GUILD_DEFAULTS = {
    "review_channel_id": None,     # where review embeds go
    "log_channel_id": None,        # where audit/log embeds go
    "verified_role_id": None,      # role to add/remove
    "reviewer_roles": [],          # who can approve/deny & run admin commands
    "stale_window_minutes": 45,    # roster recency threshold
    "cooldown_seconds": 30,        # verify cooldown per user
    "queue_retry_seconds": [120]*30,   # 30 attempts, 2 minutes each (max ~60 min), plus event-trigger
    "queue_max_age_hours": 24,
}

GLOBAL_DEFAULTS = {
    "alliance_db_path": None,      # resolved path to alliance.db; discovered automatically if not set
}

NL = "\n"

# -------------- Dataclasses for clarity --------------
@dataclasses.dataclass
class RosterMatch:
    mc_user_id: Optional[str]
    mc_name: str

@dataclasses.dataclass
class PendingEntry:
    guild_id: int
    discord_id: int
    mode: str  # "name" | "id"
    nickname: Optional[str]
    mc_id: Optional[str]
    requested_at: str
    next_retry_at: str
    retries: int
    status: str
    last_error: Optional[str]

# -------------- Main Cog --------------
class MemberSync(commands.Cog):
    """MemberSync: verify Discord users as alliance members and keep the Verified role in sync.

    This cog reads the local AllianceScraper SQLite database (read-only) to match users by
    server nickname or MissionChief ID, and posts review embeds for approvers to approve/deny.
    Includes a snapshot-aware retry queue for stale roster situations, and an auto-prune loop.
    """

    def __init__(self, bot: Red) -> None:
        self.bot: Red = bot
        self.config: Config = Config.get_conf(self, identifier=0xFA11A97C, force_registration=True)
        self.config.register_guild(**GUILD_DEFAULTS)
        self.config.register_global(**GLOBAL_DEFAULTS)

        self.data_path = cog_data_path(self)
        self.ms_db = self.data_path / "membersync.db"
        self._bg_queue: Optional[asyncio.Task] = None
        self._bg_prune: Optional[asyncio.Task] = None
        self._last_seen_fresh_key: Optional[str] = None  # to detect fresh roster

    # ---------------- Cog lifecycle ----------------
    async def cog_load(self) -> None:
        await self._seed_defaults_once()
        await self._init_db()
        await self._resolve_alliance_db()
        await self._maybe_start_background_tasks()

    async def cog_unload(self) -> None:
        for t in (self._bg_queue, self._bg_prune):
            if t and not t.done():
                t.cancel()

    async def _seed_defaults_once(self) -> None:
        """Seed the configured IDs if they are not set yet."""
        # We only seed values if not set for at least one guild.
        for g in self.bot.guilds:
            cg = self.config.guild(g)
            current = await cg.all()
            changed = False
            if not current.get("review_channel_id"):
                await cg.review_channel_id.set(int(DEFAULT_ADMIN_CHANNEL_ID))
                changed = True
            if not current.get("log_channel_id"):
                await cg.log_channel_id.set(int(DEFAULT_LOG_CHANNEL_ID))
                changed = True
            if not current.get("verified_role_id"):
                await cg.verified_role_id.set(int(DEFAULT_VERIFIED_ROLE_ID))
                changed = True
            roles = current.get("reviewer_roles") or []
            if not roles:
                await cg.reviewer_roles.set([int(DEFAULT_REVIEWER_ROLE_ID)])
                changed = True
            if changed:
                log.info("Seeded default MemberSync config for guild %s", g.id)

    async def _init_db(self) -> None:
        self.data_path.mkdir(parents=True, exist_ok=True)
        async def run():
            con = sqlite3.connect(self.ms_db)
            cur = con.cursor()
            # links table: approved/pending/denied
            cur.execute("""
            CREATE TABLE IF NOT EXISTS links(
                discord_id   INTEGER PRIMARY KEY,
                mc_user_id   TEXT,
                mc_name      TEXT,
                status       TEXT,          -- pending|approved|denied
                created_at   TEXT,
                approved_at  TEXT,
                approved_by  INTEGER,
                denied_at    TEXT,
                denied_by    INTEGER,
                deny_reason  TEXT
            )
            """)
            # reviews table: outstanding review embeds
            cur.execute("""
            CREATE TABLE IF NOT EXISTS reviews(
                message_id   INTEGER PRIMARY KEY,
                channel_id   INTEGER,
                guild_id     INTEGER,
                discord_id   INTEGER,
                mc_user_id   TEXT,
                mc_name      TEXT,
                created_at   TEXT
            )
            """)
            # audit log
            cur.execute("""
            CREATE TABLE IF NOT EXISTS audit(
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ts           TEXT,
                guild_id     INTEGER,
                action       TEXT,
                discord_id   INTEGER,
                mc_user_id   TEXT,
                mc_name      TEXT,
                meta         TEXT
            )
            """)
            # retry queue
            cur.execute("""
            CREATE TABLE IF NOT EXISTS queue(
                guild_id      INTEGER,
                discord_id    INTEGER,
                mode          TEXT,     -- name|id
                nickname      TEXT,
                mc_id         TEXT,
                requested_at  TEXT,
                next_retry_at TEXT,
                retries       INTEGER,
                status        TEXT,     -- pending|done|failed
                last_error    TEXT,
                PRIMARY KEY(guild_id, discord_id)
            )
            """)
            # unique safety: one mc_user_id shouldn't be linked to multiple discord IDs
            cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_links_mc ON links(mc_user_id)
            WHERE mc_user_id IS NOT NULL AND status='approved'
            """)
            con.commit()
            con.close()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, run)

    async def _resolve_alliance_db(self) -> None:
        # Try stored global config first
        stored = await self.config.alliance_db_path()
        path = None
        if stored and Path(stored).exists():
            path = Path(stored)
        else:
            # Heuristic: scan under Red data root for AllianceScraper/alliance.db
            candidates: List[Path] = []
            try:
                # Typical root: ~/.local/share/Red-DiscordBot/data/
                red_data_root = Path.home() / ".local" / "share" / "Red-DiscordBot" / "data"
                if red_data_root.exists():
                    for p in red_data_root.rglob("AllianceScraper/alliance.db"):
                        candidates.append(p)
            except Exception:
                pass
            if candidates:
                # Pick the most recent file
                path = max(candidates, key=lambda p: p.stat().st_mtime)
        if path:
            await self.config.alliance_db_path.set(str(path))
            log.info("MemberSync resolved alliance.db at %s", path)
        else:
            log.warning("MemberSync could not locate alliance.db automatically. Set it via config.")

    async def _maybe_start_background_tasks(self) -> None:
        if self._bg_queue is None:
            self._bg_queue = asyncio.create_task(self._queue_loop())
        if self._bg_prune is None:
            self._bg_prune = asyncio.create_task(self._prune_loop())

    # ---------------- Helpers: Discord + Config ----------------
    async def _get_channels_roles(self, guild: discord.Guild) -> Tuple[Optional[discord.TextChannel], Optional[discord.TextChannel], Optional[discord.Role], List[int]]:
        cg = self.config.guild(guild)
        review_ch = guild.get_channel((await cg.review_channel_id()) or 0)
        log_ch    = guild.get_channel((await cg.log_channel_id()) or 0)
        ver_role  = guild.get_role((await cg.verified_role_id()) or 0)
        rr_ids    = await cg.reviewer_roles()
        return review_ch if isinstance(review_ch, discord.TextChannel) else None, \
               log_ch if isinstance(log_ch, discord.TextChannel) else None, \
               ver_role, rr_ids or []

    def _is_reviewer(self, member: discord.Member, reviewer_role_ids: List[int]) -> bool:
        if member.guild_permissions.administrator or member == member.guild.owner:
            return True
        return any((r.id in reviewer_role_ids) for r in member.roles)

    # ---------------- Helpers: DB access ----------------
    async def _open_alliance(self) -> sqlite3.Connection:
        path = await self.config.alliance_db_path()
        if not path:
            raise RuntimeError("Alliance DB path not set. Use config to set alliance_db_path.")
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        return con

    async def _latest_fresh_key(self) -> Optional[str]:
        """Return a string that changes when roster updates. Uses max(scraped_at) and snapshot if available."""
        try:
            con = await self._open_alliance()
            cur = con.cursor()
            cur.execute("SELECT MAX(scraped_at) FROM members_current")
            a = cur.fetchone()[0]
            with contextlib.suppress(Exception):
                cur.execute("SELECT MAX(snapshot_utc) FROM members_history")
                b = cur.fetchone()[0]
            con.close()
            return f"{a}|{b}"
        except Exception as e:
            log.debug("fresh_key error: %s", e)
            return None

    async def _is_stale(self, minutes: int) -> bool:
        try:
            con = await self._open_alliance()
            cur = con.cursor()
            cur.execute("SELECT MAX(scraped_at) FROM members_current")
            s = cur.fetchone()[0]
            con.close()
            if not s:
                return True
            last = datetime.fromisoformat(s)
            age = datetime.now(timezone.utc) - last.replace(tzinfo=timezone.utc)
            return age.total_seconds() > minutes * 60
        except Exception:
            return True

    async def _match_member(self, nickname: Optional[str], mc_id: Optional[str]) -> Optional[RosterMatch]:
        """Try to find a single roster match by mc_id first, then by exact nickname."""
        try:
            con = await self._open_alliance()
            cur = con.cursor()
            # By MC ID
            if mc_id and mc_id.isdigit():
                cur.execute("""
                    SELECT name, COALESCE(mc_user_id, user_id) as id
                    FROM members_current
                    WHERE COALESCE(mc_user_id, user_id) = ?
                    LIMIT 2
                """, (mc_id,))
                rows = cur.fetchall()
                if len(rows) == 1:
                    r = rows[0]
                    return RosterMatch(mc_user_id=str(r["id"]), mc_name=str(r["name"]))
            # By exact nickname (case-insensitive)
            if nickname:
                cur.execute("""
                    SELECT name, COALESCE(mc_user_id, user_id) as id
                    FROM members_current
                    WHERE lower(name) = lower(?)
                    LIMIT 2
                """, (nickname.strip(),))
                rows = cur.fetchall()
                if len(rows) == 1:
                    r = rows[0]
                    return RosterMatch(mc_user_id=str(r["id"]) if r["id"] is not None else None, mc_name=str(r["name"]))
            con.close()
        except Exception as e:
            log.warning("match_member error: %s", e)
        return None

    # ---------------- Queue engine ----------------
    async def _queue_loop(self) -> None:
        await self.bot.wait_until_red_ready()
        while True:
            try:
                await self._process_queue_tick()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("queue loop error: %s", e)
            await asyncio.sleep(60)  # 1-min loop

    async def _process_queue_tick(self) -> None:
        # If roster got fresher since last tick, trigger immediate retries
        fresh = await self._latest_fresh_key()
        freshness_changed = fresh and fresh != self._last_seen_fresh_key
        if fresh:
            self._last_seen_fresh_key = fresh

        con = sqlite3.connect(self.ms_db)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        now = datetime.utcnow()

        # Load due entries or all pending if freshness changed
        if freshness_changed:
            cur.execute("SELECT * FROM queue WHERE status='pending'")
        else:
            cur.execute("SELECT * FROM queue WHERE status='pending' AND next_retry_at <= ?", (now.isoformat(),))
        pendings = [dict(r) for r in cur.fetchall()]

        if not pendings:
            con.close()
            return

        # Load retry schedule per guild (we'll use guild config but the schedule is fixed per our defaults)
        for p in pendings:
            try:
                guild_id = int(p["guild_id"])
                discord_id = int(p["discord_id"])
                mc_id = p.get("mc_id")
                nickname = p.get("nickname")
                retries = int(p.get("retries") or 0)

                guild = self.bot.get_guild(guild_id)
                if not guild:
                    # guild gone; drop it
                    cur.execute("UPDATE queue SET status='failed', last_error=? WHERE guild_id=? AND discord_id=?",
                                ("guild unavailable", guild_id, discord_id))
                    continue

                match = await self._match_member(nickname, mc_id)
                if match:
                    # Create review embed automatically
                    reviewer_ch, log_ch, ver_role, rr_ids = await self._get_channels_roles(guild)
                    member = guild.get_member(discord_id)
                    # If no reviewer channel, mark failed gracefully
                    if not reviewer_ch or not isinstance(reviewer_ch, discord.TextChannel):
                        cur.execute("UPDATE queue SET status='failed', last_error=? WHERE guild_id=? AND discord_id=?",
                                    ("review channel missing", guild_id, discord_id))
                        continue

                    review_msg = await self._post_review_embed(reviewer_ch, guild_id, discord_id, match.mc_user_id, match.mc_name)
                    # Notify user
                    if member:
                        with contextlib.suppress(Exception):
                            await member.send(f"Your verification has been queued for review: **{match.mc_name}** (ID {match.mc_user_id}).")
                    # Save review + mark done
                    cur.execute("""
                        INSERT OR REPLACE INTO reviews(message_id, channel_id, guild_id, discord_id, mc_user_id, mc_name, created_at)
                        VALUES(?,?,?,?,?,?,?)
                    """, (review_msg.id, reviewer_ch.id, guild_id, discord_id, match.mc_user_id, match.mc_name, datetime.utcnow().isoformat()))
                    cur.execute("UPDATE queue SET status='done' WHERE guild_id=? AND discord_id=?", (guild_id, discord_id))
                    # audit
                    self._audit_db(cur, guild_id, "queue_promoted", discord_id, match.mc_user_id, match.mc_name, meta="auto-review")
                    continue

                # no match yet → schedule next retry
                guild_conf = self.config.guild(guild)
                retry_seq = (await guild_conf.queue_retry_seconds()) or GUILD_DEFAULTS["queue_retry_seconds"]
                max_age_h = (await guild_conf.queue_max_age_hours()) if hasattr(guild_conf, "queue_max_age_hours") else GUILD_DEFAULTS["queue_max_age_hours"]

                req_at = datetime.fromisoformat(p["requested_at"])
                age_h  = (now - req_at).total_seconds() / 3600
                if age_h >= max_age_h or retries >= len(retry_seq):
                    # give up
                    cur.execute("UPDATE queue SET status='failed', last_error=? WHERE guild_id=? AND discord_id=?",
                                ("max retries/age reached", guild_id, discord_id))
                    member = guild.get_member(discord_id)
                    if member:
                        with contextlib.suppress(Exception):
                            await member.send("Verification expired after multiple retries. Please try again later or provide your MissionChief ID.")
                    self._audit_db(cur, guild_id, "queue_failed", discord_id, None, None, meta="expired")
                else:
                    delay = retry_seq[retries] if retries < len(retry_seq) else retry_seq[-1]
                    next_at = now + timedelta(seconds=int(delay))
                    cur.execute("UPDATE queue SET retries=?, next_retry_at=? WHERE guild_id=? AND discord_id=?",
                                (retries + 1, next_at.isoformat(), guild_id, discord_id))
            except Exception as e:
                log.exception("queue process error: %s", e)
                cur.execute("UPDATE queue SET last_error=? WHERE guild_id=? AND discord_id=?",
                            (str(e), p["guild_id"], p["discord_id"]))

        con.commit()
        con.close()

    async def _prune_loop(self) -> None:
        await self.bot.wait_until_red_ready()
        while True:
            try:
                await self._run_prune_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("prune loop error: %s", e)
            await asyncio.sleep(3600)  # hourly

    async def _run_prune_once(self) -> None:
        # Iterate guilds
        con = sqlite3.connect(self.ms_db)
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        cur.execute("SELECT discord_id, mc_user_id FROM links WHERE status='approved'")
        rows = [dict(r) for r in cur.fetchall()]
        con.close()

        # Build a set of current roster IDs
        current_ids: set[str] = set()
        try:
            acon = await self._open_alliance()
            a = acon.cursor()
            a.execute("SELECT COALESCE(mc_user_id, user_id) AS id FROM members_current WHERE id IS NOT NULL")
            current_ids.update(str(x["id"]) for x in a.fetchall() if x["id"] is not None)
            acon.close()
        except Exception as e:
            log.warning("prune: cannot read alliance roster: %s", e)
            return

        for guild in self.bot.guilds:
            reviewer_ch, log_ch, ver_role, rr_ids = await self._get_channels_roles(guild)
            if not ver_role:
                continue
            for r in rows:
                did = int(r["discord_id"])
                mid = r.get("mc_user_id")
                if not mid:
                    continue
                if str(mid) not in current_ids:
                    member = guild.get_member(did)
                    if not member:
                        continue
                    # remove role
                    if ver_role in member.roles:
                        with contextlib.suppress(Exception):
                            await member.remove_roles(ver_role, reason="Auto-prune: no longer in alliance roster")
                    # log
                    if isinstance(log_ch, discord.TextChannel):
                        with contextlib.suppress(Exception):
                            await log_ch.send(
                                embed=discord.Embed(
                                    title="Auto-prune: removed Verified role",
                                    description=f"{member.mention} no longer found in alliance roster (MC ID {mid}).",
                                    color=discord.Color.orange(),
                                    timestamp=datetime.utcnow()
                                )
                            )

    # ---------------- UI: Review embeds ----------------
    async def _post_review_embed(self, ch: discord.TextChannel, guild_id: int, discord_id: int, mc_id: Optional[str], mc_name: str) -> discord.Message:
        member = ch.guild.get_member(discord_id)
        mention = member.mention if member else f"<@{discord_id}>"
        profile_url = f"https://www.missionchief.com/users/{mc_id}" if mc_id else None
        title = "Verification request"
        desc = f"{mention} requests verification as **{mc_name}**"
        if profile_url:
            desc += f"\nProfile: <{profile_url}>"
        e = discord.Embed(title=title, description=desc, color=discord.Color.blurple(), timestamp=datetime.utcnow())
        view = self._build_review_view(guild_id, discord_id, mc_id, mc_name)
        return await ch.send(embed=e, view=view)

    def _build_review_view(self, guild_id: int, discord_id: int, mc_id: Optional[str], mc_name: str) -> discord.ui.View:
        cog = self

        class ReviewView(discord.ui.View):
            def __init__(self) -> None:
                super().__init__(timeout=86400)

            async def interaction_check(self, interaction: discord.Interaction) -> bool:
                g = interaction.guild
                if not g or not interaction.user:
                    return False
                _, _, _, rr_ids = await cog._get_channels_roles(g)
                if not isinstance(interaction.user, discord.Member):
                    return False
                if not cog._is_reviewer(interaction.user, rr_ids):
                    await interaction.response.send_message("You are not allowed to review this request.", ephemeral=True)
                    return False
                return True

            @discord.ui.button(label="Approve", style=discord.ButtonStyle.success)
            async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
                await interaction.response.defer(thinking=False)
                await cog._approve_link(interaction, guild_id, discord_id, mc_id, mc_name, delete_message=True)

            @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger)
            async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
                modal = DenyModal()
                await interaction.response.send_modal(modal)

            async def on_timeout(self) -> None:
                # Best effort: leave message as-is
                pass

        class DenyModal(discord.ui.Modal, title="Deny verification"):
            reason = discord.ui.TextInput(
                label="Reason",
                style=discord.TextStyle.paragraph,
                required=True,
                max_length=300
            )
            async def on_submit(self, interaction: discord.Interaction) -> None:
                await interaction.response.defer(thinking=False)
                await cog._deny_link(interaction, guild_id, discord_id, str(self.reason.value), delete_message=True)

        return ReviewView()

    async def _approve_link(self, interaction: discord.Interaction, guild_id: int, discord_id: int, mc_id: Optional[str], mc_name: str, delete_message: bool) -> None:
        guild = interaction.guild
        if not guild:
            return
        reviewer_ch, log_ch, ver_role, rr_ids = await self._get_channels_roles(guild)
        member = guild.get_member(discord_id)

        # Persist approval
        con = sqlite3.connect(self.ms_db)
        cur = con.cursor()
        now = datetime.utcnow().isoformat()
        cur.execute("""
            INSERT INTO links(discord_id, mc_user_id, mc_name, status, created_at, approved_at, approved_by)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(discord_id) DO UPDATE SET
                mc_user_id=excluded.mc_user_id,
                mc_name=excluded.mc_name,
                status='approved',
                approved_at=excluded.approved_at,
                approved_by=excluded.approved_by
        """, (discord_id, mc_id, mc_name, "approved", now, now, interaction.user.id))
        # Remove any queue entry
        cur.execute("DELETE FROM queue WHERE guild_id=? AND discord_id=?", (guild_id, discord_id))
        self._audit_db(cur, guild_id, "approved", discord_id, mc_id, mc_name, meta=f"by {interaction.user.id}")
        con.commit()
        con.close()

        # Role assign and DM
        if member and ver_role:
            with contextlib.suppress(Exception):
                await member.add_roles(ver_role, reason="MemberSync: approved")
            with contextlib.suppress(Exception):
                await member.send(f"Your verification was approved. You now have the **{ver_role.name}** role.")

        # Log
        if isinstance(log_ch, discord.TextChannel):
            with contextlib.suppress(Exception):
                e = discord.Embed(
                    title="Verification approved",
                    description=f"<@{discord_id}> ↔ **{mc_name}** (ID {mc_id})",
                    color=discord.Color.green(),
                    timestamp=datetime.utcnow(),
                )
                await log_ch.send(embed=e)

        # Delete the review message if possible
        if delete_message:
            with contextlib.suppress(Exception):
                await interaction.message.delete()

    async def _deny_link(self, interaction: discord.Interaction, guild_id: int, discord_id: int, reason: str, delete_message: bool) -> None:
        guild = interaction.guild
        if not guild:
            return
        reviewer_ch, log_ch, ver_role, rr_ids = await self._get_channels_roles(guild)
        member = guild.get_member(discord_id)

        con = sqlite3.connect(self.ms_db)
        cur = con.cursor()
        now = datetime.utcnow().isoformat()

        cur.execute("""
            INSERT INTO links(discord_id, status, created_at, denied_at, denied_by, deny_reason)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(discord_id) DO UPDATE SET
                status='denied',
                denied_at=excluded.denied_at,
                denied_by=excluded.denied_by,
                deny_reason=excluded.deny_reason
        """, (discord_id, "denied", now, now, interaction.user.id, reason))
        # Remove any queue entry
        cur.execute("DELETE FROM queue WHERE guild_id=? AND discord_id=?", (guild_id, discord_id))
        self._audit_db(cur, guild_id, "denied", discord_id, None, None, meta=f"by {interaction.user.id}: {reason}")
        con.commit()
        con.close()

        if member:
            with contextlib.suppress(Exception):
                await member.send(f"Your verification was denied: {reason}")

        if isinstance(log_ch, discord.TextChannel):
            with contextlib.suppress(Exception):
                e = discord.Embed(
                    title="Verification denied",
                    description=f"<@{discord_id}> — reason: {reason}",
                    color=discord.Color.red(),
                    timestamp=datetime.utcnow(),
                )
                await log_ch.send(embed=e)

        if delete_message:
            with contextlib.suppress(Exception):
                await interaction.message.delete()

    def _audit_db(self, cur: sqlite3.Cursor, guild_id: int, action: str, discord_id: Optional[int], mc_id: Optional[str], mc_name: Optional[str], meta: Optional[str] = None) -> None:
        cur.execute("""
            INSERT INTO audit(ts, guild_id, action, discord_id, mc_user_id, mc_name, meta)
            VALUES(?,?,?,?,?,?,?)
        """, (datetime.utcnow().isoformat(), guild_id, action, discord_id, mc_id, mc_name, meta or ""))

    # ---------------- Commands ----------------
    @commands.group(name="membersync", invoke_without_command=True, help="Admin and configuration for MemberSync; verify flow with review buttons.")
    @checks.guildowner_or_permissions(manage_guild=True)
    async def membersync_group(self, ctx: commands.Context) -> None:
        await ctx.send_help()

    @membersync_group.command(name="status", help="Show MemberSync configuration, counts and queue stats.")
    @checks.guildowner_or_permissions(manage_guild=True)
    async def status(self, ctx: commands.Context) -> None:
        guild = ctx.guild
        if not guild:
            return
        cg = self.config.guild(guild)
        cfg = await cg.all()
        reviewer_ch, log_ch, ver_role, rr_ids = await self._get_channels_roles(guild)

        # quick counts
        con = sqlite3.connect(self.ms_db)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM links WHERE status='approved'")
        approved = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM queue WHERE status='pending'")
        pending = cur.fetchone()[0]
        con.close()

        e = discord.Embed(title="MemberSync Status", color=discord.Color.blurple(), timestamp=datetime.utcnow())
        e.add_field(name="Review channel", value=f"<#{cfg.get('review_channel_id') or 0}>", inline=False)
        e.add_field(name="Log channel", value=f"<#{cfg.get('log_channel_id') or 0}>", inline=False)
        e.add_field(name="Verified role", value=f"<@&{cfg.get('verified_role_id') or 0}>", inline=False)
        e.add_field(name="Reviewer roles", value=", ".join(f"<@&{x}>" for x in rr_ids) or "-", inline=False)
        e.add_field(name="Cooldown", value=f"{cfg.get('cooldown_seconds')} s", inline=True)
        e.add_field(name="Stale window", value=f"{cfg.get('stale_window_minutes')} min", inline=True)
        e.add_field(name="Approved links", value=str(approved), inline=True)
        e.add_field(name="Pending in queue", value=str(pending), inline=True)

        adb = await self.config.alliance_db_path()
        e.add_field(name="Alliance DB Path", value=adb or "Not set", inline=False)
        await ctx.send(embed=e)

    @membersync_group.group(name="config", help="Set reviewer channel/roles, log channel, verified role, and stale/queue settings.")
    @checks.guildowner_or_permissions(manage_guild=True)
    async def config_group(self, ctx: commands.Context) -> None:
        pass

    @config_group.command(name="setreviewchannel", help="Set the channel where review embeds are posted. Usage: membersync config setreviewchannel #channel")
    async def setreviewchannel(self, ctx: commands.Context, channel: discord.TextChannel) -> None:
        await self.config.guild(ctx.guild).review_channel_id.set(channel.id)
        await ctx.send(f"Review channel set to {channel.mention}")

    @config_group.command(name="setlogchannel", help="Set the log channel for approvals/denials/audits. Usage: membersync config setlogchannel #channel")
    async def setlogchannel(self, ctx: commands.Context, channel: discord.TextChannel) -> None:
        await self.config.guild(ctx.guild).log_channel_id.set(channel.id)
        await ctx.send(f"Log channel set to {channel.mention}")

    @config_group.command(name="setverifiedrole", help="Set the Verified role. Usage: membersync config setverifiedrole @Role")
    async def setverifiedrole(self, ctx: commands.Context, role: discord.Role) -> None:
        await self.config.guild(ctx.guild).verified_role_id.set(role.id)
        await ctx.send(f"Verified role set to {role.mention}")

    @config_group.command(name="addreviewerrole", help="Add a reviewer role allowed to approve/deny and use admin commands. Usage: membersync config addreviewerrole @Role")
    async def addreviewerrole(self, ctx: commands.Context, role: discord.Role) -> None:
        rr = await self.config.guild(ctx.guild).reviewer_roles()
        if role.id not in rr:
            rr.append(role.id)
            await self.config.guild(ctx.guild).reviewer_roles.set(rr)
        await ctx.send(f"Added reviewer role {role.mention}")

    @config_group.command(name="setalliancedb", help="Set the alliance.db path used for roster lookups. Usage: membersync config setalliancedb /full/path/to/alliance.db")
    async def setalliancedb(self, ctx: commands.Context, path: str) -> None:
        p = Path(path)
        if not p.exists():
            await ctx.send("That file does not exist.")
            return
        await self.config.alliance_db_path.set(str(p))
        await ctx.send(f"Alliance DB path set to `{p}`")

    # ---- Manual link ----
    @membersync_group.command(name="link", help="Manually link a Discord member to a MissionChief account as approved. Usage: membersync link @member <mc_id> [mc_name ...]")
    @checks.admin_or_permissions(manage_guild=True)
    async def link(self, ctx: commands.Context, member: discord.Member, mc_id: str, *, mc_name: Optional[str] = None) -> None:
        _, log_ch, ver_role, rr_ids = await self._get_channels_roles(ctx.guild)
        if not self._is_reviewer(ctx.author, rr_ids):
            await ctx.send("You are not allowed to use this command.")
            return
        if not mc_id.isdigit():
            await ctx.send("MC ID must be numeric.")
            return

        con = sqlite3.connect(self.ms_db)
        cur = con.cursor()
        now = datetime.utcnow().isoformat()

        # Check for collisions: same mc_id already approved elsewhere
        cur.execute("SELECT discord_id FROM links WHERE mc_user_id=? AND status='approved' AND discord_id<>?", (mc_id, member.id))
        row = cur.fetchone()
        if row:
            con.close()
            await ctx.send(f"MC ID {mc_id} is already linked to <@{row[0]}>.")
            return

        cur.execute("""
            INSERT INTO links(discord_id, mc_user_id, mc_name, status, created_at, approved_at, approved_by)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(discord_id) DO UPDATE SET
                mc_user_id=excluded.mc_user_id,
                mc_name=excluded.mc_name,
                status='approved',
                approved_at=excluded.approved_at,
                approved_by=excluded.approved_by
        """, (member.id, mc_id, mc_name or member.display_name, "approved", now, now, ctx.author.id))
        self._audit_db(cur, ctx.guild.id, "manual_link", member.id, mc_id, mc_name or member.display_name, meta=f"by {ctx.author.id}")
        con.commit()
        con.close()

        if ver_role:
            with contextlib.suppress(Exception):
                await member.add_roles(ver_role, reason="MemberSync: manual link")
        if isinstance(log_ch, discord.TextChannel):
            with contextlib.suppress(Exception):
                e = discord.Embed(
                    title="Manual link approved",
                    description=f"{member.mention} ↔ **{mc_name or member.display_name}** (ID {mc_id})",
                    color=discord.Color.green(),
                    timestamp=datetime.utcnow()
                )
                await log_ch.send(embed=e)
        await ctx.tick()

    # ---- Retro scan/apply ----
    def _retro_cache_key(self, guild_id: int) -> Path:
        return self.data_path / f"retro_{guild_id}.json"

    @membersync_group.group(name="retro", help="Retro-link tools for existing Verified-role holders without approved links.")
    @checks.guildowner_or_permissions(manage_guild=True)
    async def retro_group(self, ctx: commands.Context) -> None:
        pass

    @retro_group.command(name="scan", help="Scan verified-role holders without approved links and report safe matches. Usage: membersync retro scan")
    async def retro_scan(self, ctx: commands.Context) -> None:
        guild = ctx.guild
        reviewer_ch, log_ch, ver_role, rr_ids = await self._get_channels_roles(guild)
        if not ver_role:
            await ctx.send("Verified role is not set.")
            return

        # Build set of approved links
        con = sqlite3.connect(self.ms_db)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT discord_id FROM links WHERE status='approved'")
        linked = set(int(r["discord_id"]) for r in cur.fetchall())

        # Roster lookup map: name -> mc_user_id
        try:
            acon = await self._open_alliance()
            a = acon.cursor()
            a.execute("SELECT lower(name) as lname, COALESCE(mc_user_id, user_id) AS id, name FROM members_current")
            roster = [(str(r["name"]), str(r["id"]) if r["id"] is not None else None, str(r["lname"])) for r in a.fetchall()]
            acon.close()
        except Exception as e:
            await ctx.send(f"Alliance roster not available: {e}")
            return

        name_to_id: Dict[str, Optional[str]] = {ln: mid for _, mid, ln in roster}

        candidates: List[Tuple[int, str, Optional[str]]] = []  # (discord_id, nick, mc_id)
        for m in guild.members:
            if ver_role not in m.roles:
                continue
            if m.bot or m.id in linked:
                continue
            nick = m.display_name
            mcid = name_to_id.get(nick.lower())
            if mcid:  # only strict exact matches
                candidates.append((m.id, nick, mcid))

        # Save cache
        import json
        cache_path = self._retro_cache_key(guild.id)
        cache = [{"discord_id": did, "nickname": n, "mc_id": mid} for (did, n, mid) in candidates]
        cache_path.write_text(json.dumps(cache, indent=2))

        await ctx.send(f"Retro scan complete: {len(candidates)} safe matches found. Run `membersync retro apply` to link them.")

    @retro_group.command(name="apply", help="Apply last retro scan: link safe matches as approved and assign the Verified role.")
    async def retro_apply(self, ctx: commands.Context) -> None:
        guild = ctx.guild
        reviewer_ch, log_ch, ver_role, rr_ids = await self._get_channels_roles(guild)
        if not ver_role:
            await ctx.send("Verified role is not set.")
            return

        import json
        cache_path = self._retro_cache_key(guild.id)
        if not cache_path.exists():
            await ctx.send("No scan results found. Run `membersync retro scan` first.")
            return
        data = json.loads(cache_path.read_text())

        con = sqlite3.connect(self.ms_db)
        cur = con.cursor()
        now = datetime.utcnow().isoformat()

        applied = 0
        for row in data:
            did = int(row["discord_id"])
            mcid = str(row["mc_id"])
            member = guild.get_member(did)
            if not member:
                continue
            # skip if already approved (race-safety)
            cur.execute("SELECT status FROM links WHERE discord_id=?", (did,))
            s = cur.fetchone()
            if s and s[0] == "approved":
                continue
            # collision check
            cur.execute("SELECT discord_id FROM links WHERE mc_user_id=? AND status='approved' AND discord_id<>?", (mcid, did))
            if cur.fetchone():
                continue

            cur.execute("""
                INSERT INTO links(discord_id, mc_user_id, mc_name, status, created_at, approved_at, approved_by)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    mc_user_id=excluded.mc_user_id,
                    mc_name=excluded.mc_name,
                    status='approved',
                    approved_at=excluded.approved_at,
                    approved_by=excluded.approved_by
            """, (did, mcid, member.display_name, "approved", now, now, ctx.author.id))
            self._audit_db(cur, guild.id, "retro_link", did, mcid, member.display_name, meta=f"by {ctx.author.id}")
            applied += 1

            if ver_role:
                with contextlib.suppress(Exception):
                    await member.add_roles(ver_role, reason="MemberSync: retro link")

        con.commit()
        con.close()

        if applied and isinstance(log_ch, discord.TextChannel):
            with contextlib.suppress(Exception):
                await log_ch.send(embed=discord.Embed(
                    title="Retro link applied",
                    description=f"Linked {applied} member(s) as approved and assigned Verified role.",
                    color=discord.Color.green(),
                    timestamp=datetime.utcnow(),
                ))
        await ctx.send(f"Retro apply complete: {applied} linked.")

    # ---- Verify command (user) ----
    @commands.command(name="verify", help="Request verification. Uses your server nickname by default; optionally provide your MissionChief ID.")
    @commands.cooldown(1, 30, commands.BucketType.user)
    @commands.guild_only()
    async def verify(self, ctx: commands.Context, mc_id: Optional[str] = None) -> None:
        guild = ctx.guild
        member = ctx.author if isinstance(ctx.author, discord.Member) else guild.get_member(ctx.author.id)
        if not member:
            return
        reviewer_ch, log_ch, ver_role, rr_ids = await self._get_channels_roles(guild)

        # Already approved?
        con = sqlite3.connect(self.ms_db)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute("SELECT mc_user_id, mc_name, status FROM links WHERE discord_id=?", (member.id,))
        link = cur.fetchone()

        if link and link["status"] == "approved":
            # Fix role if missing
            if ver_role and ver_role not in member.roles:
                with contextlib.suppress(Exception):
                    await member.add_roles(ver_role, reason="MemberSync: role repair")
            await ctx.send("You are already verified.")
            con.close()
            return

        # Freshness check
        stale_window = await self.config.guild(guild).stale_window_minutes()
        is_stale = await self._is_stale(int(stale_window))

        nickname = member.display_name
        match = None if is_stale else await self._match_member(nickname, mc_id)

        if match:
            # Create review embed
            if not reviewer_ch or not isinstance(reviewer_ch, discord.TextChannel):
                await ctx.send("Verification queue is unavailable (review channel not configured). Please contact an admin.")
                con.close()
                return
            msg = await self._post_review_embed(reviewer_ch, guild.id, member.id, match.mc_user_id, match.mc_name)
            # persist review
            cur.execute("""
                INSERT OR REPLACE INTO reviews(message_id, channel_id, guild_id, discord_id, mc_user_id, mc_name, created_at)
                VALUES(?,?,?,?,?,?,?)
            """, (msg.id, reviewer_ch.id, guild.id, member.id, match.mc_user_id, match.mc_name, datetime.utcnow().isoformat()))
            self._audit_db(cur, guild.id, "review_created", member.id, match.mc_user_id, match.mc_name, meta="manual verify")
            con.commit()
            con.close()
            await ctx.send("Verification request sent for review.")
            return

        # Queue path
        now = datetime.utcnow()
        # Upsert queue entry
        try:
            cur.execute("""
                INSERT INTO queue(guild_id, discord_id, mode, nickname, mc_id, requested_at, next_retry_at, retries, status, last_error)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(guild_id, discord_id) DO UPDATE SET
                    mode=excluded.mode,
                    nickname=excluded.nickname,
                    mc_id=excluded.mc_id,
                    next_retry_at=excluded.next_retry_at,
                    last_error=NULL
            """, (guild.id, member.id, "id" if (mc_id and mc_id.isdigit()) else "name", nickname, mc_id if (mc_id and mc_id.isdigit()) else None,
                  now.isoformat(), (now + timedelta(seconds=120)).isoformat(), 0, "pending", None))
            self._audit_db(cur, guild.id, "queued", member.id, mc_id if (mc_id and mc_id.isdigit()) else None, nickname, meta="stale or not found")
            con.commit()
        finally:
            con.close()

        eta = (datetime.now() + timedelta(minutes=2)).strftime("%H:%M")
        await ctx.send(f"Roster not up-to-date yet or no unique match. I've queued your verification and will retry automatically. Next attempt around **{eta}**. You'll get a DM once this is resolved.")

def setup(bot: Red) -> None:
    bot.add_cog(MemberSync(bot))
