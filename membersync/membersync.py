from __future__ import annotations
import pathlib
import aiosqlite
import asyncio
import logging
import re
import sqlite3
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

    __version__ = "1.0.1"

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
            await db.commit()

    def _guess_alliance_db(self) -> Optional[pathlib.Path]:
        base = pathlib.Path.home() / ".local" / "share" / "Red-DiscordBot" / "data"
        for inst in base.iterdir():
            p = inst / "cogs" / "AllianceScraper" / "alliance.db"
            if p.exists():
                return p
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
        rows = await self._query_alliance("SELECT MAX(snapshot_utc) AS s FROM members_history")
        if rows and rows[0]["s"]:
            return rows[0]["s"]
        rows = await self._query_alliance("SELECT MAX(scraped_at) AS s FROM members_current")
        return rows[0]["s"] if rows and rows[0]["s"] else None

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
        name = _lower(candidate_name) if candidate_name else None
        mcid = str(candidate_mc_id) if candidate_mc_id else None

        if mcid:
            for col in ("user_id", "mc_user_id"):
                rows = await self._query_alliance(f"SELECT * FROM members_current WHERE {col}=?", (mcid,))
                if rows:
                    r = dict(rows[0])
                    r["mc_id"] = mcid
                    return r
            rows = await self._query_alliance("SELECT * FROM members_current WHERE profile_href LIKE ?", (f"%/users/{mcid}",))
            if rows:
                r = dict(rows[0])
                r["mc_id"] = mcid
                return r

        if name:
            rows = await self._query_alliance("SELECT * FROM members_current WHERE lower(name)=?", (name,))
            if rows:
                r = dict(rows[0])
                mc = r.get("user_id") or r.get("mc_user_id")
                if not mc:
                    href = r.get("profile_href") or ""
                    m = re.search(r"/users/(\d+)", href or "")
                    if m:
                        mc = m.group(1)
                r["mc_id"] = mc
                return r

        return None

    def _is_reviewer(self, member: discord.Member) -> bool:
        if member.guild_permissions.administrator:
            return True
        ids = set((self.bot.loop.create_task(self.config.reviewer_role_ids())) or [])
        return False

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
        review_ch_id = await self.config.review_channel_id()
        ch = guild.get_channel(int(review_ch_id)) if review_ch_id else None
        if not isinstance(ch, discord.TextChannel):
            return None

        view = discord.ui.View(timeout=3600)
        approve_btn = discord.ui.Button(style=discord.ButtonStyle.success, label="Approve", custom_id=f"ms.approve:{requester.id}:{mc_id}")
        deny_btn = discord.ui.Button(style=discord.ButtonStyle.danger, label="Deny", custom_id=f"ms.deny:{requester.id}:{mc_id}")

        view.add_item(approve_btn)
        view.add_item(deny_btn)

        async def __ms_approve_cb(interaction: discord.Interaction):
            try:
                if interaction.response and not interaction.response.is_done():
                    await interaction.response.defer(thinking=True, ephemeral=True)
            except Exception:
                pass
            
            data = getattr(interaction, "data", None) or {}
            cid = (data.get("custom_id") or "")
            parts = cid.split(":")
            requester_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
            mc_id = parts[2] if len(parts) > 2 else None
        
            guild = interaction.guild
            member = None
            if guild and requester_id:
                member = guild.get_member(requester_id)
                if member is None:
                    try:
                        member = await guild.fetch_member(requester_id)
                    except Exception:
                        member = None
        
            ok, msg = await self._approve_link(
                guild,
                member,
                str(mc_id) if mc_id else "",
                approver=interaction.user if isinstance(interaction.user, discord.Member) else None,
            )
        
            try:
                if interaction.message:
                    await interaction.message.delete()
            except discord.errors.NotFound:
                log.warning(f"Verification message already deleted for MC {mc_id}")
            except Exception as e:
                log.error(f"Failed to delete verification message after approval for MC {mc_id}: {e}")
        
            try:
                text = ("âœ… " if ok else "âš ï¸ ") + (msg or "")
                if interaction.response and interaction.response.is_done():
                    await interaction.followup.send(text, ephemeral=True)
                else:
                    await interaction.response.send_message(text, ephemeral=True)
            except Exception as e:
                log.error(f"Failed to send approval confirmation message: {e}")
        
        approve_btn.callback = __ms_approve_cb
        
        async def __ms_deny_cb(interaction: discord.Interaction):
            try:
                if interaction.response and not interaction.response.is_done():
                    await interaction.response.defer(ephemeral=True)
                
                # Delete the verification message
                try:
                    if interaction.message:
                        await interaction.message.delete()
                    await interaction.followup.send("❌ Verification request denied. Message deleted.", ephemeral=True)
                except discord.errors.NotFound:
                    await interaction.followup.send("❌ Denied (message already deleted).", ephemeral=True)
                except Exception as e:
                    log.error(f"Failed to delete verification message after denial: {e}")
                    await interaction.followup.send("❌ Denied, but failed to delete message.", ephemeral=True)
            except Exception as e:
                log.error(f"Error in deny callback: {e}")
        
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
            await ch.send(f"âœ… Linked {user.mention} to MC [{mc_id}]({url})")

        return True, "Approved, linked and role granted."

    async def _deny_link(self, guild: discord.Guild, user: discord.Member, mc_id: str, reviewer: Optional[discord.Member], reason: str) -> None:
        def _run():
            con = sqlite3.connect(self.links_db)
            try:
                con.execute("""
                INSERT INTO links(discord_id, mc_user_id, status, created_at, approved_by, updated_at)
                VALUES(?, ?, 'denied', ?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET
                  mc_user_id=excluded.mc_user_id,
                  status='denied',
                  approved_by=excluded.approved_by,
                  updated_at=excluded.updated_at
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
            await ch.send(f"âŒ Denied verification for {user.mention} (MC `{mc_id}`): {reason}")

    async def _queue_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                await self._process_queue_once()
            except Exception as e:
                log.exception("Queue loop error: %s", e)
            await asyncio.sleep(120)

    async def _process_queue_once(self):
        queue = await self.config.queue()
        if not queue:
            return
        guild = self.bot.guilds[0] if self.bot.guilds else None
        if not guild:
            return

        stale = False
        latest = await self._latest_snapshot()

        done = []
        for user_id, data in queue.items():
            attempts = int(data.get("attempts", 0))
            mc_id = data.get("mc_id")
            discord_user = guild.get_member(int(user_id))
            if not discord_user:
                done.append(user_id)
                continue

            cand = await self._find_member_in_db(discord_user.nick or discord_user.name, mc_id)
            if cand and cand.get("mc_id"):
                await self._send_review_embed(guild, discord_user, str(cand["mc_id"]), str(cand.get("name") or discord_user.display_name))
                try:
                    await discord_user.send("Your verification request has been found and queued for review.")
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
        lines = [
            f"Alliance DB: `{cfg['alliance_db_path']}`",
            f"Review channel: {cfg['review_channel_id']}",
            f"Log channel: {cfg['log_channel_id']}",
            f"Verified role: {cfg['verified_role_id']}",
            f"Reviewer roles: {cfg['reviewer_role_ids']}",
            f"Cooldown: {cfg['cooldown_seconds']} sec",
            f"Queue size: {len(cfg.get('queue', {}))}",
        ]
        await ctx.send("\n".join(lines))

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
        """Set the path to AllianceScraper's `alliance.db` file."""
        await self.config.alliance_db_path.set(path)
        await ctx.send(f"Alliance DB path set to `{path}`")

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
        rows = await self._query_alliance("SELECT name, user_id, mc_user_id, profile_href FROM members_current WHERE lower(name)=?", (_lower(name),))
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
        todo = 0
        for m in role.members:
            if await self.get_link_for_discord(m.id):
                continue
            hit = await self._find_by_exact_name(m.nick or m.name)
            if hit:
                todo += 1
        await ctx.send(f"Retro scan: {todo} member(s) can be auto-linked.")

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

    @membersync_group.command(name="link")
    async def link(self, ctx: commands.Context, member: discord.Member, mc_id: str, *, display_name: Optional[str] = None):
        """Manually link a Discord member to an MC-ID as approved."""
        if not await self._user_is_reviewer(ctx.author):
            await ctx.send("You are not allowed to do this.")
            return
        await self._approve_link(ctx.guild, member, mc_id, approver=ctx.author if isinstance(ctx.author, discord.Member) else None)
        await ctx.send(f"Linked {member.mention} to MC `{mc_id}`.")

    @membersync_group.command(name="approve")
    async def cmd_approve(self, ctx: commands.Context, target: str, mc_id: Optional[str] = None):
        """
        Manually approve a verification request.
        
        target: Discord user mention, ID, or MC-ID
        mc_id: MC-ID (optional if target is a Discord user)
        
        Examples:
        [p]membersync approve @User 318565
        [p]membersync approve 123456789012345678 318565
        [p]membersync approve 318565 (approves by MC-ID if link exists)
        """
        if not await self._user_is_reviewer(ctx.author):
            await ctx.send("❌ You are not allowed to approve verifications.")
            return
        
        # Try to parse target as Discord user
        member = None
        target_mc_id = mc_id
        
        # Check if target is a mention or ID
        if target.startswith("<@") and target.endswith(">"):
            # Mentioned user
            user_id = int(target.strip("<@!>"))
            member = ctx.guild.get_member(user_id)
        elif target.isdigit():
            # Could be Discord ID or MC-ID
            try:
                member = ctx.guild.get_member(int(target))
                if not member:
                    member = await ctx.guild.fetch_member(int(target))
            except:
                # Not a Discord ID, treat as MC-ID
                if not mc_id:
                    target_mc_id = target
        else:
            await ctx.send("❌ Invalid target. Use @mention, Discord ID, or MC-ID")
            return
        
        # If we have a member but no MC-ID, try to find it
        if member and not target_mc_id:
            await ctx.send("❌ Please provide the MC-ID for this user")
            return
        
        # If we don't have a member, try to find by MC-ID
        if not member and target_mc_id:
            link = await self.get_link_for_mc(target_mc_id)
            if link:
                discord_id = int(link['discord_id'])
                member = ctx.guild.get_member(discord_id)
                if not member:
                    await ctx.send(f"❌ MC-ID `{target_mc_id}` is linked to Discord ID `{discord_id}`, but they're not in the server")
                    return
        
        if not member or not target_mc_id:
            await ctx.send("❌ Could not identify member and MC-ID")
            return
        
        # Approve the link
        await self._approve_link(ctx.guild, member, target_mc_id, approver=ctx.author if isinstance(ctx.author, discord.Member) else None)
        await ctx.send(f"✅ Approved: {member.mention} linked to MC `{target_mc_id}`")

    @membersync_group.command(name="deny")
    async def cmd_deny(self, ctx: commands.Context, target: str, *, reason: str = "No reason provided"):
        """
        Manually deny a verification request.
        
        target: Discord user mention, ID, or MC-ID  
        reason: Reason for denial
        
        Examples:
        [p]membersync deny @User Not in alliance
        [p]membersync deny 123456789012345678 Account mismatch
        [p]membersync deny 318565 Duplicate account
        """
        if not await self._user_is_reviewer(ctx.author):
            await ctx.send("❌ You are not allowed to deny verifications.")
            return
        
        # Try to parse target
        member = None
        mc_id = None
        
        if target.startswith("<@") and target.endswith(">"):
            # Mentioned user
            user_id = int(target.strip("<@!>"))
            member = ctx.guild.get_member(user_id)
        elif target.isdigit():
            # Could be Discord ID or MC-ID
            try:
                member = ctx.guild.get_member(int(target))
                if not member:
                    member = await ctx.guild.fetch_member(int(target))
            except:
                # Not a Discord ID, treat as MC-ID
                mc_id = target
                # Try to find the Discord user
                link = await self.get_link_for_mc(mc_id)
                if link:
                    member = ctx.guild.get_member(int(link['discord_id']))
        
        if not member:
            await ctx.send(f"❌ Could not find member. Target: `{target}`")
            return
        
        # Get MC-ID if we don't have it
        if not mc_id:
            link = await self.get_link_for_discord(member.id)
            if link:
                mc_id = link['mc_user_id']
            else:
                # Try to find from pending verification
                await ctx.send(f"⚠️ No MC-ID found for {member.mention}. Denying without specific MC-ID.")
                mc_id = "unknown"
        
        # Deny the link
        await self._deny_link(ctx.guild, member, mc_id, reviewer=ctx.author if isinstance(ctx.author, discord.Member) else None, reason=reason)
        await ctx.send(f"❌ Denied: {member.mention} for MC `{mc_id}`. Reason: {reason}")


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

    async def _prune_once(self):
        guild = self.bot.guilds[0] if self.bot.guilds else None
        if not guild:
            return
        role_id = await self.config.verified_role_id()
        role = guild.get_role(int(role_id)) if role_id else None
        if not role:
            return

        rows = await self._query_alliance("SELECT user_id, mc_user_id, profile_href FROM members_current")
        current_ids: set[str] = set()
        for r in rows:
            mc = r["user_id"] if "user_id" in r.keys() else None
            if not mc and "mc_user_id" in r.keys():
                mc = r["mc_user_id"]
            if not mc and "profile_href" in r.keys() and r["profile_href"]:
                m = re.search(r"/users/(\d+)", r["profile_href"])
                if m:
                    mc = m.group(1)
            if mc:
                current_ids.add(str(mc))

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

        removed = 0
        for link in links:
            did = int(link["discord_id"])
            mcid = str(link["mc_user_id"])
            if mcid not in current_ids:
                member = guild.get_member(did)
                if not member or not role or role not in member.roles:
                    continue
                try:
                    await member.remove_roles(role, reason="MemberSync auto-prune: not in alliance anymore")
                    removed += 1
                except Exception:
                    pass
                if isinstance(ch, discord.TextChannel):
                    await ch.send(f"ðŸ”Ž Auto-prune removed Verified from <@{did}> (MC `{mcid}` no longer found).")

        if removed:
            log.info("Auto-prune removed %s roles", removed)
