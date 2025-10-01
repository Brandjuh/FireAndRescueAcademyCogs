# membersync.py (v0.1.3) - enforce guild nickname
from __future__ import annotations

import asyncio
import aiosqlite
import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import ui, Interaction, ButtonStyle
from redbot.core import commands, checks, Config
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.FARA.MemberSync")

DEFAULTS = {
    "admin_channel_id": None,
    "log_channel_id": None,
    "verified_role_id": None,
    "admin_role_ids": [],
    "match_mode": "loose",  # strict|loose
    "fuzzy_threshold": 0.85,
    "autodelete_user_feedback_seconds": 10,
    "review_timeout_minutes": 60,
    "delete_admin_message_on_decision": True,
    "auto_prune_enabled": True,
    "auto_prune_interval_hours": 24,
    "profile_url_template": "https://www.missionchief.com/users/{id}",
    "require_nickname": True,  # NEW: strictly require guild nickname for matching
}

def now_utc() -> str:
    return datetime.utcnow().isoformat()

def norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    # collapse spaces; allow letters, digits and spaces
    import re
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = " ".join(s.split())
    return s

def fuzzy_ratio(a: str, b: str) -> float:
    from difflib import SequenceMatcher
    return SequenceMatcher(None, a, b).ratio()

class ReviewView(ui.View):
    def __init__(self, cog: "MemberSync", review_id: str, timeout: Optional[float] = None):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.review_id = review_id
        # persistent custom_ids
        self.add_item(ui.Button(label="Approve", style=ButtonStyle.success, custom_id=f"msync:approve:{review_id}"))
        self.add_item(ui.Button(label="Reject", style=ButtonStyle.danger, custom_id=f"msync:reject:{review_id}"))

    async def interaction_check(self, interaction: Interaction) -> bool:
        # Only configured approvers or owner can use
        if await self.cog._is_approver(interaction.user):
            return True
        await interaction.response.send_message("You are not allowed to approve or reject verification requests.", ephemeral=True)
        return False

class RejectModal(ui.Modal, title="Reject verification"):
    reason = ui.TextInput(label="Reason", style=discord.TextStyle.paragraph, required=True, max_length=400)

    def __init__(self, cog: "MemberSync", review_id: str):
        super().__init__()
        self.cog = cog
        self.review_id = review_id

    async def on_submit(self, interaction: Interaction) -> None:
        await self.cog._handle_reject_interaction(interaction, self.review_id, str(self.reason))

class MemberSync(commands.Cog):
    """Link Discord users to MissionChief members with admin review (buttons), auto-prune and API for other cogs."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFABA11C0DE, force_registration=True)
        self.config.register_global(**DEFAULTS)
        self.data_path = cog_data_path(self)
        self.db_path = self.data_path / "membersync.db"
        self._bg_task: Optional[asyncio.Task] = None
        self._loaded = False

    # ---------- Setup & DB ----------
    async def _init_db(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
            CREATE TABLE IF NOT EXISTS links(
                discord_id INTEGER PRIMARY KEY,
                mc_user_id TEXT,
                mc_name TEXT,
                status TEXT,
                requested_at TEXT,
                approved_at TEXT,
                approved_by INTEGER,
                rejected_reason TEXT,
                last_check_utc TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS reviews(
                id TEXT PRIMARY KEY,
                message_id INTEGER,
                channel_id INTEGER,
                discord_id INTEGER,
                mc_user_id TEXT,
                mc_name TEXT,
                match_type TEXT,
                confidence REAL,
                created_at TEXT,
                status TEXT
            )
            """)
            await db.execute("""
            CREATE TABLE IF NOT EXISTS audit(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT,
                actor_discord_id INTEGER,
                action TEXT,
                details_json TEXT
            )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_links_mc ON links(mc_user_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_reviews_status ON reviews(status)")
            await db.commit()
        self._loaded = True

    async def _reattach_pending_views(self):
        # Reattach persistent views for any pending reviews
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT id, message_id FROM reviews WHERE status='pending' AND message_id IS NOT NULL")
            rows = await cur.fetchall()
        for r in rows:
            rid = r["id"]
            mid = r["message_id"]
            try:
                self.bot.add_view(ReviewView(self, rid, timeout=None), message_id=mid)
            except Exception as e:
                log.warning("Failed to reattach view for %s: %s", rid, e)

    async def cog_load(self):
        await self._init_db()
        await self._reattach_pending_views()
        await self._maybe_start_background()

    # ---------- Permissions ----------
    async def _is_approver(self, user: discord.abc.User) -> bool:
        # Owner always allowed
        try:
            if await self.bot.is_owner(user):
                return True
        except Exception:
            pass
        if not isinstance(user, discord.Member):
            return False
        admin_roles: List[int] = await self.config.admin_role_ids()
        return any(r.id in admin_roles for r in user.roles)

    # ---------- Public API (for other cogs) ----------
    async def get_link_for_discord(self, discord_id: int) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM links WHERE discord_id=?", (int(discord_id),))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def get_link_for_mc(self, mc_user_id: str) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM links WHERE mc_user_id=?", (str(mc_user_id),))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def report_external(self, action: str, **kwargs) -> None:
        """
        External cogs can report events, e.g. action='unlink', 'removed_from_alliance', 'name_changed'.
        kwargs may include: discord_id, mc_user_id, note
        """
        await self._audit(None, f"external:{action}", kwargs)
        # Minimal reactions for common actions
        if action in {"unlink", "removed_from_alliance"}:
            discord_id = kwargs.get("discord_id")
            mc_user_id = kwargs.get("mc_user_id")
            if discord_id is not None:
                await self._unlink_discord(int(discord_id), reason=f"external:{action}")
            elif mc_user_id is not None:
                link = await self.get_link_for_mc(str(mc_user_id))
                if link:
                    await self._unlink_discord(int(link["discord_id"]), reason=f"external:{action}")

    # ---------- Helpers ----------
    async def _audit(self, actor: Optional[discord.abc.User], action: str, details: Dict[str, Any]):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO audit(ts, actor_discord_id, action, details_json) VALUES(?,?,?,?)",
                (now_utc(), int(getattr(actor, "id", 0) or 0), action, json.dumps(details, ensure_ascii=False)),
            )
            await db.commit()

    async def _send_log(self, guild: discord.Guild, embed: discord.Embed):
        ch_id = await self.config.log_channel_id()
        if ch_id:
            ch = guild.get_channel(int(ch_id))
            if ch:
                try:
                    await ch.send(embed=embed)
                except Exception as e:
                    log.warning("Failed to send log: %s", e)

    def _profile_url(self, mc_user_id: str) -> str:
        tpl = "https://www.missionchief.com/users/{id}"
        return tpl.format(id=mc_user_id)

    async def _get_alliance_members(self) -> List[Dict[str, Any]]:
        sc = self.bot.get_cog("AllianceScraper")
        if not sc:
            return []
        try:
            rows = await sc.get_members()
            return rows or []
        except Exception as e:
            log.warning("AllianceScraper get_members failed: %s", e)
            return []

    async def _find_candidate(self, guild: discord.Guild, member: discord.Member, mc_id_hint: Optional[str]) -> Optional[Dict[str, Any]]:
        members = await self._get_alliance_members()
        if not members:
            return None
        if mc_id_hint and str(mc_id_hint).isdigit():
            for m in members:
                if str(m.get("user_id")) == str(mc_id_hint):
                    return {"mc_user_id": str(m.get("user_id")), "mc_name": m.get("name"), "match_type": "id", "confidence": 1.0}
        # nickname-only policy
        require_nick = await self.config.require_nickname()
        nick = member.nick  # strictly guild nickname
        if not nick:
            if require_nick:
                return None
            # if not required, we could fallback, but default is True -> no fallback
        # name-based
        mmode = await self.config.match_mode()
        threshold = float(await self.config.fuzzy_threshold())
        def norm(s): return norm_name(s)
        nick_n = norm(nick or "")
        best = None
        best_score = 0.0
        for m in members:
            mcname = m.get("name") or ""
            score = 1.0 if norm(mcname) == nick_n else fuzzy_ratio(nick_n, norm(mcname))
            if score > best_score:
                best_score = score
                best = m
        if best is None:
            return None
        if mmode == "strict" and norm(best.get("name") or "") != nick_n:
            return None
        if mmode != "strict" and best_score < threshold:
            return None
        return {"mc_user_id": str(best.get("user_id")), "mc_name": best.get("name"), "match_type": "fuzzy" if mmode != "strict" else "strict", "confidence": float(best_score)}

    async def _create_review_embed(self, guild: discord.Guild, member: discord.Member, cand: Dict[str, Any]) -> discord.Embed:
        mc_id = cand["mc_user_id"]
        mc_name = cand["mc_name"]
        url = f"https://www.missionchief.com/users/{mc_id}"
        e = discord.Embed(title="Verification request", color=discord.Color.blurple(), timestamp=datetime.utcnow())
        e.add_field(name="MissionChief", value=f"[{mc_name}]({url})\nID: `{mc_id}`", inline=False)
        e.add_field(name="Discord", value=f"{member.mention}\nID: `{member.id}`\nGuild Nick: `{member.nick or 'None'}`", inline=False)
        e.add_field(name="Match", value=f"Type: `{cand.get('match_type')}`  •  Confidence: `{cand.get('confidence'):.2f}`", inline=False)
        e.set_thumbnail(url=getattr(member.display_avatar, "url", discord.Embed.Empty))
        e.set_footer(text="Click Approve to link and assign role, or Reject to decline.")
        return e

    async def _process_approve(self, guild: discord.Guild, review_id: str, approver: discord.Member) -> str:
        # Load review
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM reviews WHERE id=? AND status='pending'", (review_id,))
            rv = await cur.fetchone()
            if not rv:
                return "This review is no longer pending."
            discord_id = int(rv["discord_id"])
            mc_user_id = str(rv["mc_user_id"])
            mc_name = rv["mc_name"]
            message_id = int(rv["message_id"]) if rv["message_id"] else None
        # Link record
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
            INSERT INTO links(discord_id, mc_user_id, mc_name, status, requested_at, approved_at, approved_by, rejected_reason, last_check_utc)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(discord_id) DO UPDATE SET
                mc_user_id=excluded.mc_user_id,
                mc_name=excluded.mc_name,
                status='approved',
                approved_at=excluded.approved_at,
                approved_by=excluded.approved_by
            """, (discord_id, mc_user_id, mc_name, "approved", now_utc(), now_utc(), int(approver.id), None, now_utc()))
            await db.execute("UPDATE reviews SET status='finished' WHERE id=?", (review_id,))
            await db.commit()
        # Assign role
        mem = guild.get_member(discord_id)
        if mem:
            role_id = await self.config.verified_role_id()
            if role_id:
                role = guild.get_role(int(role_id))
                if role:
                    try:
                        await mem.add_roles(role, reason="MemberSync verification approved")
                    except Exception as e:
                        log.warning("Failed to add role: %s", e)
        # DM user
        try:
            if mem:
                await mem.send(f"You have been verified as **{mc_name}** (ID `{mc_user_id}`).")
        except Exception:
            pass
        # Delete admin message
        if message_id and (await self.config.delete_admin_message_on_decision()):
            ch = guild.get_channel(int(await self.config.admin_channel_id() or 0))
            if ch:
                try:
                    msg = await ch.fetch_message(message_id)
                    await msg.delete()
                except Exception:
                    pass
        # Log
        emb = discord.Embed(title="Member verified", color=discord.Color.green(), timestamp=datetime.utcnow())
        emb.add_field(name="MissionChief", value=f"{mc_name} (`{mc_user_id}`)", inline=False)
        emb.add_field(name="Discord", value=f"<@{discord_id}> (`{discord_id}`)", inline=False)
        emb.add_field(name="Approved by", value=f"<@{approver.id}>", inline=False)
        await self._send_log(guild, emb)
        # Audit
        await self._audit(approver, "approve", {"discord_id": discord_id, "mc_user_id": mc_user_id})
        return "Approved."

    async def _unlink_discord(self, discord_id: int, reason: str):
        # Update status and remove role if present
        guild = self.bot.guilds[0] if self.bot.guilds else None
        if guild:
            mem = guild.get_member(discord_id)
            role_id = await self.config.verified_role_id()
            if role_id and mem:
                role = guild.get_role(int(role_id))
                if role:
                    try:
                        await mem.remove_roles(role, reason=f"MemberSync unlink ({reason})")
                    except Exception:
                        pass
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE links SET status='unlinked', approved_at=NULL, approved_by=NULL WHERE discord_id=?", (discord_id,))
            await db.commit()

    async def _handle_reject_interaction(self, interaction: Interaction, review_id: str, reason: str) -> None:
        # Load review
        guild = interaction.guild
        if not guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM reviews WHERE id=? AND status='pending'", (review_id,))
            rv = await cur.fetchone()
            if not rv:
                await interaction.response.send_message("This review is no longer pending.", ephemeral=True)
                return
            discord_id = int(rv["discord_id"])
            mc_user_id = str(rv["mc_user_id"])
            mc_name = rv["mc_name"]
            message_id = int(rv["message_id"]) if rv["message_id"] else None
            await db.execute("UPDATE reviews SET status='finished' WHERE id=?", (review_id,))
            await db.execute("""
            INSERT INTO links(discord_id, mc_user_id, mc_name, status, requested_at, approved_at, approved_by, rejected_reason, last_check_utc)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(discord_id) DO UPDATE SET
                status='rejected',
                rejected_reason=excluded.rejected_reason
            """, (discord_id, mc_user_id, mc_name, "rejected", now_utc(), None, None, reason, now_utc()))
            await db.commit()
        # DM user
        mem = guild.get_member(discord_id)
        try:
            if mem:
                await mem.send(f"Your verification was rejected for **{mc_name}** (ID `{mc_user_id}`).\nReason: {reason}")
        except Exception:
            pass
        # Delete admin message
        if message_id and (await self.config.delete_admin_message_on_decision()):
            ch = guild.get_channel(int(await self.config.admin_channel_id() or 0))
            if ch:
                try:
                    msg = await ch.fetch_message(message_id)
                    await msg.delete()
                except Exception:
                    pass
        # Log
        emb = discord.Embed(title="Member verification rejected", color=discord.Color.red(), timestamp=datetime.utcnow())
        emb.add_field(name="MissionChief", value=f"{mc_name} (`{mc_user_id}`)", inline=False)
        emb.add_field(name="Discord", value=f"<@{discord_id}> (`{discord_id}`)", inline=False)
        emb.add_field(name="Rejected by", value=f"<@{interaction.user.id}>", inline=False)
        emb.add_field(name="Reason", value=reason[:400], inline=False)
        await self._send_log(guild, emb)
        await self._audit(interaction.user, "reject", {"discord_id": discord_id, "mc_user_id": mc_user_id, "reason": reason})
        await interaction.response.send_message("Rejected.", ephemeral=True)

    # ---------- Interaction handlers for persistent buttons ----------
    @commands.Cog.listener("on_interaction")
    async def handle_component(self, interaction: Interaction):
        if not interaction.type == discord.InteractionType.component:
            return
        cid = interaction.data.get("custom_id") if interaction.data else None  # type: ignore
        if not isinstance(cid, str) or not cid.startswith("msync:"):
            return
        parts = cid.split(":")
        if len(parts) != 3:
            return
        _, action, review_id = parts
        if not await self._is_approver(interaction.user):
            await interaction.response.send_message("You are not allowed to approve or reject verification requests.", ephemeral=True)
            return
        if action == "approve":
            guild = interaction.guild
            if not guild:
                await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
                return
            msg = await self._process_approve(guild, review_id, interaction.user)  # type: ignore
            try:
                await interaction.response.send_message(msg, ephemeral=True)
            except Exception:
                pass
        elif action == "reject":
            # Show modal to capture reason
            modal = RejectModal(self, review_id)
            try:
                await interaction.response.send_modal(modal)
            except Exception as e:
                # Fallback
                await interaction.response.send_message("Please use `!membersync reject <message_id> <reason>` as fallback.", ephemeral=True)

    # ---------- Background ----------
    async def _maybe_start_background(self):
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._bg_worker())

    async def _bg_worker(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                if await self.config.auto_prune_enabled():
                    await self._auto_prune_once()
            except Exception as e:
                log.warning("Auto-prune error: %s", e)
            hrs = max(1, int(await self.config.auto_prune_interval_hours()))
            await asyncio.sleep(hrs * 3600)

    async def _auto_prune_once(self):
        # Remove verified role from users no longer in alliance
        guild = self.bot.guilds[0] if self.bot.guilds else None
        if not guild:
            return
        members = await self._get_alliance_members()
        in_alliance_ids = {str(m.get("user_id")) for m in members if m.get("user_id")}
        role_id = await self.config.verified_role_id()
        if not role_id:
            return
        role = guild.get_role(int(role_id))
        if not role:
            return
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT * FROM links WHERE status='approved'")
            rows = await cur.fetchall()
        for row in rows:
            discord_id = int(row["discord_id"])
            mc_user_id = str(row["mc_user_id"])
            if not mc_user_id or mc_user_id not in in_alliance_ids:
                mem = guild.get_member(discord_id)
                if mem and role in mem.roles:
                    try:
                        await mem.remove_roles(role, reason="MemberSync auto-prune: not in alliance")
                    except Exception:
                        pass
                # Update status and log
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute("UPDATE links SET status='revoked' WHERE discord_id=?", (discord_id,))
                    await db.commit()
                emb = discord.Embed(title="Member auto-pruned", color=discord.Color.orange(), timestamp=datetime.utcnow())
                emb.add_field(name="Discord", value=f"<@{discord_id}> (`{discord_id}`)", inline=False)
                emb.add_field(name="Reason", value="Not found in alliance roster", inline=False)
                await self._send_log(guild, emb)

    # ---------- Commands ----------
    @commands.group(name="membersync")
    @checks.admin_or_permissions(manage_guild=True)
    async def membersync_group(self, ctx: commands.Context):
        """MemberSync admin and config commands."""

    @membersync_group.command(name="status")
    async def status(self, ctx: commands.Context):
        """Show counts and config summary."""
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM links WHERE status='approved'")
            approved = (await cur.fetchone())[0]
            cur = await db.execute("SELECT COUNT(*) FROM reviews WHERE status='pending'")
            pending = (await cur.fetchone())[0]
        cfg = await self.config.all()
        msg = "\n".join([
            f"Approved links: {approved}",
            f"Pending reviews: {pending}",
            f"Admin channel: {cfg['admin_channel_id']}",
            f"Log channel: {cfg['log_channel_id']}",
            f"Verified role: {cfg['verified_role_id']}",
            f"Admin roles: {cfg['admin_role_ids']}",
            f"Match mode: {cfg['match_mode']} threshold={cfg['fuzzy_threshold']}",
            f"Require nickname: {cfg['require_nickname']}",
            f"Auto prune: enabled={cfg['auto_prune_enabled']} interval_h={cfg['auto_prune_interval_hours']}",
        ])
        await ctx.send(f"```\n{msg}\n```")

    @membersync_group.command(name="diag")
    async def diag(self, ctx: commands.Context):
        """Diagnostic: count alliance members with/without numeric user_id and show match settings."""
        sc_members = await self._get_alliance_members()
        with_id = sum(1 for m in sc_members if m.get("user_id"))
        without_id = sum(1 for m in sc_members if not m.get("user_id"))
        cfg = await self.config.all()
        await ctx.send("```\n"
                       f"Alliance members total: {len(sc_members)}\n"
                       f"With user_id: {with_id}\n"
                       f"Without user_id: {without_id}\n"
                       f"Match mode: {cfg['match_mode']} (threshold={cfg['fuzzy_threshold']})\n"
                       f"Require nickname: {cfg['require_nickname']}\n"
                       "```")

    @membersync_group.command(name="debugfind")
    async def debugfind(self, ctx: commands.Context, *, name_or_mention: Optional[str] = None):
        """Show top name matches for a given guild nickname or @mention. Approvers/admins only."""
        if not await self._is_approver(ctx.author):
            await ctx.send("You are not allowed to run debugfind.")
            return
        target_name = None
        if ctx.message.mentions:
            m = ctx.message.mentions[0]
            target_name = m.nick  # strictly guild nick
        else:
            if name_or_mention:
                target_name = name_or_mention
            else:
                target_name = ctx.author.nick  # strictly guild nick
        if not target_name:
            await ctx.send("No guild nickname found. Please set a server nickname for this test.")
            return
        nick_n = norm_name(target_name)
        members = await self._get_alliance_members()
        scored = []
        for m in members:
            mcname = m.get("name") or ""
            score = 1.0 if norm_name(mcname) == nick_n else fuzzy_ratio(nick_n, norm_name(mcname))
            scored.append((score, mcname, str(m.get("user_id") or ""), m))
        scored.sort(key=lambda x: x[0], reverse=True)
        top = scored[:5]
        if not top:
            await ctx.send("No alliance members loaded.")
            return
        lines = [f"{i+1}. {t[1]} | user_id={t[2] or 'None'} | score={t[0]:.3f}" for i, t in enumerate(top)]
        await ctx.send("```\n" + "\n".join(lines) + "\n```")

    @membersync_group.group(name="config")
    @checks.is_owner()
    async def config_group(self, ctx: commands.Context):
        """Owner-only configuration for MemberSync."""

    @config_group.command(name="setadminchannel")
    async def set_admin_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.admin_channel_id.set(int(channel.id))
        await ctx.send(f"Admin channel set to {channel.mention}")

    @config_group.command(name="setlogchannel")
    async def set_log_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.log_channel_id.set(int(channel.id))
        await ctx.send(f"Log channel set to {channel.mention}")

    @config_group.command(name="setverifiedrole")
    async def set_verified_role(self, ctx: commands.Context, role: discord.Role):
        await self.config.verified_role_id.set(int(role.id))
        await ctx.send(f"Verified role set to {role.name}")

    @config_group.command(name="addadminrole")
    async def add_admin_role(self, ctx: commands.Context, role: discord.Role):
        roles = await self.config.admin_role_ids()
        if int(role.id) not in roles:
            roles.append(int(role.id))
            await self.config.admin_role_ids.set(roles)
        await ctx.send(f"Added admin role: {role.name}")

    @config_group.command(name="removeadminrole")
    async def remove_admin_role(self, ctx: commands.Context, role: discord.Role):
        roles = await self.config.admin_role_ids()
        roles = [r for r in roles if r != int(role.id)]
        await self.config.admin_role_ids.set(roles)
        await ctx.send(f"Removed admin role: {role.name}")

    @config_group.command(name="setmatch")
    async def set_match(self, ctx: commands.Context, mode: str, threshold: Optional[float] = None):
        mode = mode.lower().strip()
        if mode not in {"strict", "loose"}:
            await ctx.send("Mode must be 'strict' or 'loose'.")
            return
        await self.config.match_mode.set(mode)
        if threshold is not None:
            try:
                t = float(threshold)
                await self.config.fuzzy_threshold.set(t)
            except Exception:
                await ctx.send("Invalid threshold; must be a number 0..1")
                return
        await ctx.send("Match settings updated.")

    @config_group.command(name="setnickrequired")
    async def set_nick_required(self, ctx: commands.Context, required: bool):
        await self.config.require_nickname.set(bool(required))
        await ctx.send(f"Require guild nickname set to {bool(required)}")

    @config_group.command(name="setautoprune")
    async def set_auto_prune(self, ctx: commands.Context, enabled: bool, interval_hours: Optional[int] = None):
        await self.config.auto_prune_enabled.set(bool(enabled))
        if interval_hours is not None:
            try:
                ih = max(1, int(interval_hours))
                await self.config.auto_prune_interval_hours.set(ih)
            except Exception:
                await ctx.send("interval_hours must be an integer ≥ 1")
                return
        await ctx.send("Auto-prune settings updated.")

    @config_group.command(name="setautodelete")
    async def set_autodelete(self, ctx: commands.Context, seconds: int):
        await self.config.autodelete_user_feedback_seconds.set(int(seconds))
        await ctx.send("User feedback auto-delete updated.")

    # Fallback text approvals (rare use)
    @membersync_group.command(name="approve")
    async def approve_cmd(self, ctx: commands.Context, message_id: int):
        """Fallback: approve a pending review by message id."""
        if not await self._is_approver(ctx.author):
            await ctx.send("You are not allowed to approve reviews.")
            return
        # find review by message id
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT id FROM reviews WHERE message_id=? AND status='pending'", (int(message_id),))
            rv = await cur.fetchone()
        if not rv:
            await ctx.send("No pending review for that message id.")
            return
        msg = await self._process_approve(ctx.guild, rv["id"], ctx.author)  # type: ignore
        await ctx.send(msg)

    @membersync_group.command(name="reject")
    async def reject_cmd(self, ctx: commands.Context, message_id: int, *, reason: str):
        """Fallback: reject a pending review by message id with a reason."""
        if not await self._is_approver(ctx.author):
            await ctx.send("You are not allowed to reject reviews.")
            return
        # emulate modal path
        await self._handle_reject_interaction(type("X",(object,),{"guild":ctx.guild,"user":ctx.author,"response":type("Y",(object,),{"send_message":ctx.send})()})(), str(message_id), reason)  # type: ignore

    # User command
    @commands.command(name="verify")
    async def verify(self, ctx: commands.Context, mc_id: Optional[str] = None):
        """Verify yourself as a MissionChief member. Optionally provide MC user id: `!verify 123456`."""
        # Feedback message
        feedback = await ctx.send("Searching the alliance roster for your account... this can take a moment.")
        # If nickname required and missing, bail out clearly
        require_nick = await self.config.require_nickname()
        if require_nick and not ctx.author.nick:  # strictly guild nickname
            await feedback.edit(content="No server nickname set. Please set your Discord **server nickname** to your MissionChief name, then run `!verify` again. Or use `!verify <MC_ID>` as a fallback.")
            secs = int(await self.config.autodelete_user_feedback_seconds())
            if secs > 0:
                try:
                    await asyncio.sleep(secs)
                    await feedback.delete()
                except Exception:
                    pass
            return
        # Find candidate
        cand = await self._find_candidate(ctx.guild, ctx.author, mc_id)  # type: ignore
        if not cand:
            await feedback.edit(content="No matching MissionChief member was found. Ensure your **server nickname** matches your MissionChief name exactly, or run `!verify <MC_ID>` as a fallback.")
            # optional auto delete
            secs = int(await self.config.autodelete_user_feedback_seconds())
            if secs > 0:
                try:
                    await asyncio.sleep(secs)
                    await feedback.delete()
                except Exception:
                    pass
            return
        # Prepare admin embed
        admin_ch_id = await self.config.admin_channel_id()
        if not admin_ch_id:
            await feedback.edit(content="Verification is not configured yet. Please tell an admin to set the admin review channel.")
            return
        admin_ch = ctx.guild.get_channel(int(admin_ch_id))
        if not admin_ch:
            await feedback.edit(content="Admin review channel not found.")
            return
        embed = await self._create_review_embed(ctx.guild, ctx.author, cand)  # type: ignore
        review_id = str(uuid.uuid4())
        view = ReviewView(self, review_id, timeout=None)
        msg = await admin_ch.send(embed=embed, view=view)
        # Persist review
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
            INSERT INTO reviews(id, message_id, channel_id, discord_id, mc_user_id, mc_name, match_type, confidence, created_at, status)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """, (review_id, int(msg.id), int(msg.channel.id), int(ctx.author.id), str(cand["mc_user_id"]), str(cand["mc_name"]), cand["match_type"], float(cand["confidence"]), now_utc(), "pending"))
            await db.commit()
        # User feedback
        await feedback.edit(content=f"Found candidate **{cand['mc_name']}** (ID `{cand['mc_user_id']}`). Waiting for admin review.")
        secs = int(await self.config.autodelete_user_feedback_seconds())
        if secs > 0:
            try:
                await asyncio.sleep(secs)
                await feedback.delete()
            except Exception:
                pass

async def setup(bot):
    cog = MemberSync(bot)
    await bot.add_cog(cog)
