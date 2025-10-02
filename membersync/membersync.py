# MemberSync cog (full) v0.3.5
from __future__ import annotations

import asyncio
import logging
import sqlite3
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import discord
from pathlib import Path

from redbot.core import Config, checks, commands
from redbot.core.bot import Red
from redbot.core.commands import Context
from redbot.core.data_manager import cog_data_path

import aiosqlite  # type: ignore

log = logging.getLogger("red.FARA.MemberSync")
__version__ = "0.3.5"

UTC = timezone.utc

# -------- aiosqlite compat shim --------
async def _exec_fetchall(db: aiosqlite.Connection, sql: str, params: Tuple = ()):
    try:
        return await db.execute_fetchall(sql, params)  # type: ignore[attr-defined]
    except AttributeError:
        cur = await db.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return rows

async def _exec_fetchone(db: aiosqlite.Connection, sql: str, params: Tuple = ()):
    try:
        return await db.execute_fetchone(sql, params)  # type: ignore[attr-defined]
    except AttributeError:
        cur = await db.execute(sql, params)
        row = await cur.fetchone()
        await cur.close()
        return row

def _row_to_dict(row, keys: Optional[List[str]]=None):
    if hasattr(row, "keys"):
        try:
            return dict(row)
        except Exception:
            pass
    if keys:
        return dict(zip(keys, row))
    return row

def now_iso() -> str:
    return datetime.now(UTC).isoformat()

# Simple normalization for nickname -> roster name
def _norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    # strip common decorations: [TAG], (text), | text, emojis and extra symbols
    s = re.sub(r"\[[^\]]*\]", "", s)   # remove [ ... ]
    s = re.sub(r"\([^\)]*\)", "", s)   # remove ( ... )
    s = re.sub(r"\|.*$", "", s)        # cut after first pipe
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

@dataclass
class RetroCandidate:
    member: discord.Member
    display_name: str

class ReviewView(discord.ui.View):
    def __init__(self, cog: "MemberSync", discord_id: int, mc_user_id: str, timeout: int = 300):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.discord_id = discord_id
        self.mc_user_id = mc_user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # Only admins or reviewer roles
        if not interaction.guild:
            return False
        if interaction.user.guild_permissions.manage_guild:
            return True
        ids = await self.cog.config.review_role_ids()
        if any(r.id in ids for r in getattr(interaction.user, "roles", []) or []):
            return True
        await interaction.response.send_message("You are not allowed to do this.", ephemeral=True)
        return False

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success, emoji="✅")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=False)
        await self.cog._approve_link(interaction, self.discord_id, self.mc_user_id)

    @discord.ui.button(label="Deny", style=discord.ButtonStyle.danger, emoji="⛔")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(DenyModal(self.cog, self.discord_id, self.mc_user_id))

class DenyModal(discord.ui.Modal, title="Deny verification"):
    reason = discord.ui.TextInput(label="Reason", style=discord.TextStyle.long, required=True, max_length=500)

    def __init__(self, cog: "MemberSync", discord_id: int, mc_user_id: str):
        super().__init__()
        self.cog = cog
        self.discord_id = discord_id
        self.mc_user_id = mc_user_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await self.cog._deny_link(interaction, self.discord_id, self.mc_user_id, str(self.reason))

class MemberSync(commands.Cog):
    """Link Discord users to MissionChief accounts and manage verification flow."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA11BEEF, force_registration=True)
        self.data_path: Path = cog_data_path(self)
        self.db_path: Path = self.data_path / "membersync.db"
        defaults = {
            "verify_role_id": None,
            "review_channel_id": None,
            "log_channel_id": None,
            "review_role_ids": [],  # roles allowed to approve/deny
            "rate_limit_seconds": 15,
        }
        self.config.register_global(**defaults)
        self._last_attempt: Dict[int, datetime] = {}
        self._bg_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        await self._init_db()
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._queue_loop())

    async def cog_unload(self):
        if self._bg_task:
            self._bg_task.cancel()

    # ---------- DB init ----------
    async def _init_db(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """CREATE TABLE IF NOT EXISTS links(
                    discord_id      TEXT NOT NULL,
                    mc_user_id      TEXT NOT NULL,
                    status          TEXT NOT NULL,
                    created_at_utc  TEXT NOT NULL,
                    approved_by     TEXT,
                    approved_at_utc TEXT,
                    UNIQUE(discord_id),
                    UNIQUE(mc_user_id)
                )"""
            )
            await db.execute(
                """CREATE TABLE IF NOT EXISTS verify_queue(
                    discord_id        INTEGER NOT NULL,
                    guild_id          INTEGER NOT NULL,
                    requested_at_utc  TEXT NOT NULL,
                    attempts          INTEGER NOT NULL DEFAULT 0,
                    next_attempt_utc  TEXT NOT NULL,
                    wanted_mc_id      TEXT
                )"""
            )
            await db.commit()

    # ---------- Alliance DB helpers ----------
    def _get_alliance_db_path(self) -> Optional[Path]:
        sc = self.bot.get_cog("AllianceScraper")
        if sc and getattr(sc, "db_path", None):
            try:
                return Path(getattr(sc, "db_path"))
            except Exception:
                pass
        guess = Path.home() / ".local/share/Red-DiscordBot/data" / self.bot.instance_name / "cogs/AllianceScraper/alliance.db"  # type: ignore
        return guess if guess.exists() else None

    def _roster_lookup(self, name: Optional[str]=None, mcid: Optional[str]=None) -> Optional[Tuple[str,str]]:
        """Return (name, mcid) if found in members_current by either normalized name or mcid."""
        adb = self._get_alliance_db_path()
        if not adb or not adb.exists():
            return None
        con = sqlite3.connect(str(adb)); con.row_factory = sqlite3.Row
        cur = con.cursor()
        try:
            if mcid:
                try:
                    cur.execute("SELECT name, COALESCE(user_id, mc_user_id,'') mc FROM members_current WHERE COALESCE(user_id, mc_user_id,'')=?", (str(mcid),))
                except sqlite3.OperationalError:
                    cur.execute("SELECT name, COALESCE(user_id,'') mc FROM members_current WHERE COALESCE(user_id,'')=?", (str(mcid),))
                r = cur.fetchone()
                if r and (r["mc"] or "").strip():
                    return (r["name"], str(r["mc"]))
            if name:
                key_norm = _norm_name(name)
                cur.execute("SELECT name, COALESCE(user_id, mc_user_id,'') mc FROM members_current")
                for r in cur.fetchall():
                    nm = (r["name"] or "").strip()
                    if not nm:
                        continue
                    if _norm_name(nm) == key_norm:
                        mc = str((r["mc"] or "").strip())
                        if mc:
                            return (nm, mc)
        finally:
            con.close()
        return None

    # ---------- Public API used by other cogs ----------
    async def get_link_for_mc(self, mc_user_id: str) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            try:
                db.row_factory = aiosqlite.Row  # type: ignore[attr-defined]
            except Exception:
                pass
            row = await _exec_fetchone(db, "SELECT * FROM links WHERE mc_user_id=?", (str(mc_user_id),))
            if not row:
                return None
            row = _row_to_dict(row, ["discord_id","mc_user_id","status","created_at_utc","approved_by","approved_at_utc"])
            return row

    # ---------- Commands ----------
    @commands.group(name="membersync", invoke_without_command=True)
    async def ms_group(self, ctx: Context):
        """MemberSync: link Discord users to MissionChief accounts and manage verification."""
        await ctx.send_help()

    @ms_group.command(name="status")
    async def status(self, ctx: Context):
        """Show MemberSync status and current configuration."""
        cfg = await self.config.all()
        # best effort roster freshness
        ms = hs = "-"
        adb = self._get_alliance_db_path()
        if adb and adb.exists():
            con = sqlite3.connect(str(adb)); cur = con.cursor()
            try:
                cur.execute("SELECT MAX(scraped_at) FROM members_current"); ms = cur.fetchone()[0] or "-"
            except Exception:
                pass
            try:
                cur.execute("SELECT MAX(snapshot_utc) FROM members_history"); hs = cur.fetchone()[0] or "-"
            except Exception:
                pass
            con.close()
        lines = [
            f"MemberSync v{__version__}",
            f"Verify role ID: {cfg.get('verify_role_id')}",
            f"Review channel ID: {cfg.get('review_channel_id')}",
            f"Log channel ID: {cfg.get('log_channel_id')}",
            f"Reviewer roles: {cfg.get('review_role_ids')}",
            f"Rate limit: {cfg.get('rate_limit_seconds')}s",
            f"Roster members_current.scraped_at: {ms}",
            f"Roster members_history.snapshot_utc: {hs}",
        ]
        await ctx.send("```\n" + "\n".join(lines) + "\n```")

    # ----- VERIFY -----
    @commands.cooldown(1, 15, commands.BucketType.user)
    @ms_group.command(name="verify")
    async def verify(self, ctx: Context, mc_user_id: Optional[str] = None):
        """Start verification. Tries to match your server nickname to the alliance roster.
        You may optionally pass your MC user ID."""
        cfg = await self.config.all()
        vr_id = cfg.get("verify_role_id")
        rv_ch_id = cfg.get("review_channel_id")
        lg_ch_id = cfg.get("log_channel_id")

        if not rv_ch_id:
            await ctx.reply("Verification is not configured yet. Admins must set a review channel first.")
            return
        review_ch = ctx.guild.get_channel(int(rv_ch_id)) if ctx.guild else None
        if not isinstance(review_ch, discord.TextChannel):
            await ctx.reply("Configured review channel is invalid.")
            return

        # Already linked?
        async with aiosqlite.connect(self.db_path) as db:
            row = await _exec_fetchone(db, "SELECT status, mc_user_id FROM links WHERE discord_id=?", (str(ctx.author.id),))
            if row:
                r = _row_to_dict(row, ["status","mc_user_id"])
                if r.get("status") == "approved":
                    # ensure role present
                    if vr_id:
                        role = ctx.guild.get_role(int(vr_id))
                        if role and role not in ctx.author.roles:
                            try:
                                await ctx.author.add_roles(role, reason="MemberSync: ensure verified role")
                            except Exception:
                                pass
                    await ctx.reply("You are already verified.")
                    return

        # Search in roster
        display = (ctx.author.nick or ctx.author.display_name or ctx.author.name).strip()
        found = self._roster_lookup(display, mc_user_id)
        if not found:
            # queue it
            await self._queue_request(ctx.guild.id, ctx.author.id, mc_user_id)
            await ctx.reply("I couldn't find your account in the roster yet. I've queued your request and will retry automatically. You'll be notified.")
            return

        mc_name, mcid = found
        # create review embed
        e = discord.Embed(title="Verification request", color=discord.Color.blurple(), timestamp=datetime.now(UTC))
        e.add_field(name="Discord", value=f"{ctx.author.mention} (`{ctx.author.id}`)", inline=False)
        e.add_field(name="MissionChief", value=f"[{mc_name}](https://www.missionchief.com/users/{mcid}) (`{mcid}`)", inline=False)
        e.set_footer(text=f"Requested by {ctx.author}", icon_url=getattr(ctx.author.display_avatar, "url", discord.Embed.Empty))
        view = ReviewView(self, ctx.author.id, mcid)
        try:
            msg = await review_ch.send(embed=e, view=view)
        except Exception:
            await ctx.reply("Could not post to the review channel. Ask an admin to fix my permissions.")
            return

        await ctx.reply("Found your account. Your request is pending admin review.")

        # log
        if lg_ch_id:
            ch = ctx.guild.get_channel(int(lg_ch_id))
            if isinstance(ch, discord.TextChannel):
                try:
                    await ch.send(f"Queued review: {ctx.author.mention} ↔ https://www.missionchief.com/users/{mcid}")
                except Exception:
                    pass

    async def _approve_link(self, interaction: discord.Interaction, discord_id: int, mc_user_id: str):
        guild = interaction.guild
        if not guild:
            await interaction.followup.send("No guild context.", ephemeral=True)
            return
        cfg = await self.config.all()
        vr = guild.get_role(int(cfg.get("verify_role_id") or 0)) if cfg.get("verify_role_id") else None
        member = guild.get_member(discord_id)

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO links(discord_id, mc_user_id, status, created_at_utc, approved_by, approved_at_utc) "
                "VALUES(?,?,?,?,?,?)",
                (str(discord_id), str(mc_user_id), "approved", now_iso(), str(interaction.user.id), now_iso())
            )
            await db.commit()

        if member and vr:
            try:
                await member.add_roles(vr, reason="MemberSync verification approved")
            except Exception:
                pass

        # DM user
        try:
            if member:
                await member.send(f"✅ Your verification was approved. Linked to MC `{mc_user_id}`.")
        except Exception:
            pass

        # log channel
        lg_id = cfg.get("log_channel_id")
        if lg_id:
            ch = guild.get_channel(int(lg_id))
            if isinstance(ch, discord.TextChannel):
                try:
                    await ch.send(f"✅ Approved: <@{discord_id}> ↔ https://www.missionchief.com/users/{mc_user_id} by {interaction.user.mention}")
                except Exception:
                    pass

        # delete the review message if possible
        try:
            await interaction.message.delete()
        except Exception:
            pass

        await interaction.followup.send("Approved.", ephemeral=True)

    async def _deny_link(self, interaction: discord.Interaction, discord_id: int, mc_user_id: str, reason: str):
        guild = interaction.guild
        if not guild:
            await interaction.followup.send("No guild context.", ephemeral=True)
            return
        # mark as denied (keeps history simple)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO links(discord_id, mc_user_id, status, created_at_utc, approved_by, approved_at_utc) "
                "VALUES(?,?,?,?,?,?)",
                (str(discord_id), str(mc_user_id), "denied", now_iso(), str(interaction.user.id), now_iso())
            )
            await db.commit()

        member = guild.get_member(discord_id)
        try:
            if member:
                await member.send(f"❌ Your verification was denied.\nReason: {reason}")
        except Exception:
            pass

        cfg = await self.config.all()
        lg_id = cfg.get("log_channel_id")
        if lg_id:
            ch = guild.get_channel(int(lg_id))
            if isinstance(ch, discord.TextChannel):
                try:
                    await ch.send(f"⛔ Denied: <@{discord_id}> ↔ https://www.missionchief.com/users/{mc_user_id} by {interaction.user.mention}\nReason: {reason}")
                except Exception:
                    pass

        try:
            await interaction.message.delete()
        except Exception:
            pass

        await interaction.response.send_message("Denied.", ephemeral=True)

    # ----- QUEUE -----
    async def _queue_request(self, guild_id: int, discord_id: int, wanted_mcid: Optional[str]):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO verify_queue(discord_id, guild_id, requested_at_utc, attempts, next_attempt_utc, wanted_mc_id) "
                "VALUES(?,?,?,?,?,?)",
                (discord_id, guild_id, now_iso(), 0, now_iso(), (wanted_mcid or None))
            )
            await db.commit()

    async def _queue_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                await self._queue_tick()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("queue tick failed")
            await asyncio.sleep(120)  # every 2 minutes

    async def _queue_tick(self):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row  # type: ignore[attr-defined]
            now = datetime.now(UTC)
            rows = await _exec_fetchall(db, "SELECT * FROM verify_queue")
            to_delete = []
            for r in rows:
                data = _row_to_dict(r)
                try:
                    next_at = datetime.fromisoformat(data["next_attempt_utc"])
                except Exception:
                    next_at = now
                if next_at > now:
                    continue
                attempts = int(data.get("attempts", 0))
                requested = datetime.fromisoformat(data["requested_at_utc"]) if data.get("requested_at_utc") else now
                # expire after 24h or 30 attempts
                if attempts >= 30 or (now - requested) > timedelta(hours=24):
                    # notify and drop
                    guild = self.bot.get_guild(int(data["guild_id"]))
                    member = guild.get_member(int(data["discord_id"])) if guild else None
                    try:
                        if member:
                            await member.send("⏳ Verification expired after repeated retries. Please run the command again later.")
                    except Exception:
                        pass
                    to_delete.append((data["discord_id"], data["guild_id"]))
                    continue

                # Try match
                guild = self.bot.get_guild(int(data["guild_id"]))
                member = guild.get_member(int(data["discord_id"])) if guild else None
                disp = (member.nick or member.display_name or member.name) if member else None
                found = self._roster_lookup(disp, data.get("wanted_mc_id"))
                if found and guild:
                    mc_name, mcid = found
                    # post to review channel
                    rv_id = await self.config.review_channel_id()
                    review_ch = guild.get_channel(int(rv_id)) if rv_id else None
                    if isinstance(review_ch, discord.TextChannel):
                        e = discord.Embed(title="Verification request (auto-retry)", color=discord.Color.blurple(), timestamp=now)
                        md = f"<@{data['discord_id']}> (`{data['discord_id']}`)"
                        e.add_field(name="Discord", value=md, inline=False)
                        e.add_field(name="MissionChief", value=f"[{mc_name}](https://www.missionchief.com/users/{mcid}) (`{mcid}`)", inline=False)
                        view = ReviewView(self, int(data["discord_id"]), str(mcid))
                        try:
                            await review_ch.send(embed=e, view=view)
                            # DM notify
                            if member:
                                try:
                                    await member.send("✅ I found your MC account and queued your verification for admin review.")
                                except Exception:
                                    pass
                            to_delete.append((data["discord_id"], data["guild_id"]))
                            continue
                        except Exception:
                            pass

                # reschedule
                attempts += 1
                next_time = now + timedelta(minutes=10)
                await db.execute("UPDATE verify_queue SET attempts=?, next_attempt_utc=? WHERE discord_id=? AND guild_id=?",
                                 (attempts, next_time.isoformat(), data["discord_id"], data["guild_id"]))
            # delete processed
            for d_id, g_id in to_delete:
                await db.execute("DELETE FROM verify_queue WHERE discord_id=? AND guild_id=?", (d_id, g_id))
            await db.commit()

    # ----- Retro tools -----
    @ms_group.group(name="retro")
    @checks.admin_or_permissions(manage_guild=True)
    async def retro(self, ctx: Context):
        """Retroactive linking tools for already-verified members."""

    @retro.command(name="scan")
    @checks.admin_or_permissions(manage_guild=True)
    async def retro_scan(self, ctx: Context, role: Optional[discord.Role] = None):
        """Scan members with the verify role and report how many are not linked to a MC ID."""
        role = role or ctx.guild.get_role((await self.config.verify_role_id()) or 0)
        if not role:
            await ctx.send("Verify role is not configured. Use `membersync set verifyrole <role>` first.")
            return
        targets = [m for m in ctx.guild.members if role in m.roles]

        linked_ids: set[int] = set()
        async with aiosqlite.connect(self.db_path) as db:
            try:
                db.row_factory = aiosqlite.Row  # type: ignore[attr-defined]
            except Exception:
                pass
            rows = await _exec_fetchall(db, "SELECT discord_id FROM links WHERE status='approved'")
        for r in rows:
            try:
                linked_ids.add(int(r["discord_id"]))
            except Exception:
                linked_ids.add(int(r[0]))
        missing = [m for m in targets if m.id not in linked_ids]
        await ctx.send(f"Checked {len(targets)} members with {role.mention}. Missing links: **{len(missing)}**.")

    @retro.command(name="apply")
    @checks.admin_or_permissions(manage_guild=True)
    async def retro_apply(self, ctx: Context, role: Optional[discord.Role] = None):
        """Attempt to auto-link verified members by matching their server nickname to MC roster."""
        role = role or ctx.guild.get_role((await self.config.verify_role_id()) or 0)
        if not role:
            await ctx.send("Verify role is not configured. Use `membersync set verifyrole <role>` first.")
            return
        adb = self._get_alliance_db_path()
        if not adb or not adb.exists():
            await ctx.send("AllianceScraper database not found. Make sure that cog is loaded and has scraped at least once.")
            return

        con = sqlite3.connect(str(adb))
        con.row_factory = sqlite3.Row
        cur = con.cursor()

        # Build a robust name -> mcid map from current roster, with normalized variants
        roster_map: Dict[str, str] = {}
        try:
            cur.execute("SELECT name, COALESCE(user_id, mc_user_id, '') as mc_user_id FROM members_current")
        except sqlite3.OperationalError:
            cur.execute("SELECT name, COALESCE(user_id, '') as mc_user_id FROM members_current")
        for r in cur.fetchall():
            name = (r["name"] or "").strip()
            mcid = str(r["mc_user_id"] or "").strip()
            if not name or not mcid:
                continue
            roster_map[name.lower()] = mcid
            roster_map[_norm_name(name)] = mcid
        con.close()

        # Already linked
        linked_ids: set[int] = set()
        async with aiosqlite.connect(self.db_path) as db:
            try:
                db.row_factory = aiosqlite.Row  # type: ignore[attr-defined]
            except Exception:
                pass
            rows = await _exec_fetchall(db, "SELECT discord_id FROM links WHERE status='approved'")
        for r in rows:
            try:
                linked_ids.add(int(r["discord_id"]))
            except Exception:
                linked_ids.add(int(r[0]))

        targets = [m for m in ctx.guild.members if role in m.roles and m.id not in linked_ids]

        linked = 0
        tried = 0
        async with aiosqlite.connect(self.db_path) as db:
            for m in targets:
                tried += 1
                disp = (m.nick or m.display_name or m.name).strip()
                key_exact = disp.lower()
                key_norm = _norm_name(disp)
                mcid = roster_map.get(key_exact) or roster_map.get(key_norm)
                if not mcid:
                    continue
                try:
                    await db.execute(
                        "INSERT OR REPLACE INTO links(discord_id, mc_user_id, status, created_at_utc) VALUES(?,?,?,?)",
                        (str(m.id), str(mcid), "approved", now_iso())
                    )
                    linked += 1
                except Exception:
                    pass
            await db.commit()

        await ctx.send(f"Retro apply finished. Checked: **{tried}**. Newly linked: **{linked}**. Skipped: **{tried - linked}**.")

    # ----- Settings -----
    @ms_group.group(name="set")
    @checks.admin_or_permissions(manage_guild=True)
    async def set_group(self, ctx: Context):
        """Configure MemberSync settings (verify role, channels, reviewer roles, rate limit)."""

    @set_group.command(name="verifyrole")
    async def set_verify_role(self, ctx: Context, role: discord.Role):
        """Set the role to assign upon successful verification and to scan in retro tools."""
        await self.config.verify_role_id.set(role.id)
        await ctx.send(f"Verify role set to {role.mention}")

    @set_group.command(name="reviewchannel")
    async def set_review_channel(self, ctx: Context, channel: discord.TextChannel):
        """Set the admin review channel for verification requests."""
        await self.config.review_channel_id.set(channel.id)
        await ctx.send(f"Review channel set to {channel.mention}")

    @set_group.command(name="logchannel")
    async def set_log_channel(self, ctx: Context, channel: discord.TextChannel):
        """Set the log channel for MemberSync actions."""
        await self.config.log_channel_id.set(channel.id)
        await ctx.send(f"Log channel set to {channel.mention}")

    @set_group.command(name="ratelimit")
    async def set_rate_limit(self, ctx: Context, seconds: int):
        """Set per-user rate limit for verify attempts (seconds)."""
        seconds = max(1, int(seconds))
        await self.config.rate_limit_seconds.set(seconds)
        await ctx.send(f"Rate limit set to {seconds}s")

    # Reviewer role management
    @set_group.group(name="reviewer")
    async def reviewer_group(self, ctx: Context):
        """Manage reviewer roles that can approve/deny verifications."""

    @reviewer_group.command(name="add")
    async def reviewer_add(self, ctx: Context, role: discord.Role):
        """Add a role that can approve/deny verification requests."""
        roles = await self.config.review_role_ids()
        if role.id not in roles:
            roles.append(role.id)
            await self.config.review_role_ids.set(roles)
        await ctx.send(f"Added reviewer role: {role.mention}")

    @reviewer_group.command(name="remove")
    async def reviewer_remove(self, ctx: Context, role: discord.Role):
        """Remove a reviewer role."""
        roles = await self.config.review_role_ids()
        roles = [r for r in roles if r != role.id]
        await self.config.review_role_ids.set(roles)
        await ctx.send(f"Removed reviewer role: {role.mention}")

    @reviewer_group.command(name="list")
    async def reviewer_list(self, ctx: Context):
        """List all reviewer roles."""
        ids = await self.config.review_role_ids()
        if not ids:
            await ctx.send("No reviewer roles configured.")
            return
        names = []
        for i in ids:
            r = ctx.guild.get_role(i)
            names.append(r.mention if r else f"`{i}`")
        await ctx.send("Reviewer roles: " + ", ".join(names))

async def setup(bot: Red):
    await bot.add_cog(MemberSync(bot))
