# MemberSync cog (patched) v0.3.3
from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import discord
from pathlib import Path

# IMPORTANT: use Red's commands module so we inherit the correct Cog base class
from redbot.core import Config, checks, commands
from redbot.core.bot import Red
from redbot.core.commands import Context
from redbot.core.data_manager import cog_data_path

import aiosqlite  # type: ignore

log = logging.getLogger("red.FARA.MemberSync")
__version__ = "0.3.3"

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

# -------- Core Cog --------

@dataclass
class RetroCandidate:
    member: discord.Member
    display_name: str

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
            "review_role_ids": [],
            "rate_limit_seconds": 15,
        }
        self.config.register_global(**defaults)

    async def cog_load(self):
        await self._init_db()

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

    async def _roster_latest_times(self) -> Tuple[Optional[str], Optional[str]]:
        adb = self._get_alliance_db_path()
        if not adb or not adb.exists():
            return None, None
        con = sqlite3.connect(str(adb))
        cur = con.cursor()
        try:
            cur.execute("SELECT MAX(scraped_at) FROM members_current")
            ms = cur.fetchone()[0]
        except Exception:
            ms = None
        try:
            cur.execute("SELECT MAX(snapshot_utc) FROM members_history")
            hs = cur.fetchone()[0]
        except Exception:
            hs = None
        con.close()
        return ms, hs

    # ---------- Public API ----------
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
        """Show MemberSync status and latest roster times from AllianceScraper."""
        ms, hs = await self._roster_latest_times()
        cfg = await self.config.all()
        lines = [
            f"MemberSync v{__version__}",
            f"Verify role ID: {cfg.get('verify_role_id')}",
            f"Review channel ID: {cfg.get('review_channel_id')}",
            f"Log channel ID: {cfg.get('log_channel_id')}",
            f"Rate limit: {cfg.get('rate_limit_seconds')}s",
            f"Roster latest members_current.scraped_at: {ms or '-'}",
            f"Roster latest members_history.snapshot_utc: {hs or '-'}",
        ]
        await ctx.send("```\n" + "\n".join(lines) + "\n```")

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
        try:
            cur.execute("SELECT name, COALESCE(user_id, mc_user_id, '') as mc_user_id FROM members_current")
            roster = { (r["name"] or "").strip().lower(): str(r["mc_user_id"] or "") for r in cur.fetchall() }
        except Exception:
            cur.execute("SELECT name, COALESCE(user_id, '') as mc_user_id FROM members_current")
            roster = { (r["name"] or "").strip().lower(): str(r["mc_user_id"] or "") for r in cur.fetchall() }
        con.close()

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
        async with aiosqlite.connect(self.db_path) as db:
            for m in targets:
                name = (m.nick or m.display_name or m.name).strip().lower()
                mcid = roster.get(name, "")
                if not mcid:
                    continue
                try:
                    await db.execute(
                        "INSERT OR REPLACE INTO links(discord_id, mc_user_id, status, created_at_utc) VALUES(?,?,?,?)",
                        (str(m.id), str(mcid), "approved", datetime.utcnow().isoformat())
                    )
                    linked += 1
                except Exception:
                    pass
            await db.commit()

        await ctx.send(f"Retro apply finished. Newly linked: **{linked}**. Skipped: **{len(targets) - linked}**.")

    @ms_group.group(name="set")
    @checks.admin_or_permissions(manage_guild=True)
    async def set_group(self, ctx: Context):
        """Configure MemberSync settings (verify role, channels, rate limit)."""

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

async def setup(bot: Red):
    await bot.add_cog(MemberSync(bot))
