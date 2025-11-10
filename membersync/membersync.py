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
    "debug_mode": False,
    "debug_channel_id": None,
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

    __version__ = "1.1.0"

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
                log.info(f"Auto-detected alliance DB: {guess}")
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._queue_loop())
            log.info("MemberSync queue loop started")

    async def cog_unload(self) -> None:
        if self._bg_task:
            self._bg_task.cancel()
            self._bg_task = None
            log.info("MemberSync queue loop stopped")

    async def _debug_log(self, message: str, level: str = "info") -> None:
        """Log debug messages to both console and Discord if debug mode is enabled"""
        # Always log to console
        if level == "error":
            log.error(message)
        elif level == "warning":
            log.warning(message)
        else:
            log.info(message)
        
        # Send to Discord if debug mode enabled
        if await self.config.debug_mode():
            debug_ch_id = await self.config.debug_channel_id()
            if debug_ch_id:
                ch = self.bot.get_channel(int(debug_ch_id))
                if isinstance(ch, discord.TextChannel):
                    try:
                        emoji = "🐛" if level == "info" else "⚠️" if level == "warning" else "❌"
                        await ch.send(f"{emoji} `[MemberSync]` {message}")
                    except Exception as e:
                        log.error(f"Failed to send debug message to Discord: {e}")

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
            log.info("MemberSync database initialized")

    def _guess_alliance_db(self) -> Optional[pathlib.Path]:
        """Try to find the alliance database automatically"""
        base = pathlib.Path.home() / ".local" / "share" / "Red-DiscordBot" / "data"
        
        # First try V2 database
        for inst in base.iterdir():
            if not inst.is_dir():
                continue
            p = inst / "cogs" / "scraper_databases" / "members_v2.db"
            if p.exists():
                log.info(f"Found V2 database: {p}")
                return p
        
        # Fall back to legacy database
        for inst in base.iterdir():
            if not inst.is_dir():
                continue
            p = inst / "cogs" / "AllianceScraper" / "alliance.db"
            if p.exists():
                log.info(f"Found legacy database: {p}")
                return p
        
        log.warning("Could not auto-detect alliance database")
        return None

    async def _query_alliance(self, sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
        """Query the alliance database safely"""
        path = await self.config.alliance_db_path()
        if not path:
            await self._debug_log("No alliance DB path configured", "error")
            return []
        
        def _run() -> List[sqlite3.Row]:
            try:
                con = sqlite3.connect(path)
                con.row_factory = sqlite3.Row
                try:
                    cur = con.execute(sql, params)
                    rows = cur.fetchall()
                    return rows
                finally:
                    con.close()
            except Exception as e:
                log.error(f"Alliance DB query error: {e}")
                return []
        
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _run)
    
    async def _latest_snapshot(self) -> Optional[str]:
        """Get timestamp of latest member data snapshot"""
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
        """Public API: returns approved link for given Discord ID or None."""
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
        """Search for member in alliance database by name or MC ID"""
        name = _lower(candidate_name) if candidate_name else None
        mcid = str(candidate_mc_id) if candidate_mc_id else None

        await self._debug_log(f"Searching for member: name='{candidate_name}', mc_id='{mcid}'")

        # Try by MC ID first (most reliable)
        if mcid:
            # Try user_id column
            rows = await self._query_alliance("SELECT * FROM members_current WHERE user_id=?", (mcid,))
            if rows:
                r = dict(rows[0])
                r["mc_id"] = mcid
                await self._debug_log(f"Found by user_id: {r.get('name', 'Unknown')}")
                return r
            
            # Try mc_user_id column
            rows = await self._query_alliance("SELECT * FROM members_current WHERE mc_user_id=?", (mcid,))
            if rows:
                r = dict(rows[0])
                r["mc_id"] = mcid
                await self._debug_log(f"Found by mc_user_id: {r.get('name', 'Unknown')}")
                return r
            
            # Try profile_href
            rows = await self._query_alliance("SELECT * FROM members_current WHERE profile_href LIKE ?", (f"%/users/{mcid}",))
            if rows:
                r = dict(rows[0])
                r["mc_id"] = mcid
                await self._debug_log(f"Found by profile_href: {r.get('name', 'Unknown')}")
                return r
            
            await self._debug_log(f"MC ID {mcid} not found in database", "warning")

        # Try by name (less reliable, but useful for auto-match)
        if name:
            rows = await self._query_alliance("SELECT * FROM members_current WHERE lower(name)=?", (name,))
            if rows:
                r = dict(rows[0])
                # Extract MC ID from row
                mc = r.get("user_id") or r.get("mc_user_id")
                if not mc:
                    href = r.get("profile_href") or ""
                    m = re.search(r"/users/(\d+)", href or "")
                    if m:
                        mc = m.group(1)
                r["mc_id"] = mc
                await self._debug_log(f"Found by name: {r.get('name', 'Unknown')} (MC: {mc})")
                return r
            
            await self._debug_log(f"Name '{candidate_name}' not found in database", "warning")

        await self._debug_log("Member not found in database", "warning")
        return None

    async def _get_reviewer_roles(self, guild: discord.Guild) -> List[discord.Role]:
        """Get list of reviewer roles"""
        ids = await self.config.reviewer_role_ids()
        roles: List[discord.Role] = []
        for rid in ids:
            r = guild.get_role(int(rid))
            if r:
                roles.append(r)
        return roles

    async def _user_is_reviewer(self, member: discord.Member) -> bool:
        """Check if user can approve/deny verifications"""
        if member.guild_permissions.administrator:
            return True
        reviewer_roles = await self._get_reviewer_roles(member.guild)
        return any(r in member.roles for r in reviewer_roles)

    async def _send_review_embed(self, guild: discord.Guild, requester: discord.Member, mc_id: str, mc_name: str) -> Optional[int]:
        """Send verification request embed to review channel"""
        try:
            review_ch_id = await self.config.review_channel_id()
            if not review_ch_id:
                await self._debug_log("No review channel configured", "error")
                return None
            
            ch = guild.get_channel(int(review_ch_id))
            if not isinstance(ch, discord.TextChannel):
                await self._debug_log(f"Review channel {review_ch_id} not found or not a text channel", "error")
                return None

            await self._debug_log(f"Creating review embed for {requester.name} (MC: {mc_id})")

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
                    text = ("✅ " if ok else "⚠️ ") + (msg or "")
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
            await self._debug_log(f"✅ Review embed sent (message ID: {msg.id})")
            return msg.id

        except Exception as e:
            await self._debug_log(f"Failed to send review embed: {e}", "error")
            log.exception("Error sending review embed")
            return None

    async def _approve_link(self, guild: discord.Guild, user: discord.Member, mc_id: str, approver: Optional[discord.Member]=None) -> Tuple[bool, str]:
        """Approve a verification link"""
        try:
            await self._debug_log(f"Approving link: Discord {user.id} -> MC {mc_id}")
            
            verified_role_id = await self.config.verified_role_id()
            role = guild.get_role(int(verified_role_id)) if verified_role_id else None

            def _run():
                con = sqlite3.connect(self.links_db)
                try:
                    con.execute("""
                    INSERT INTO links(discord_id, mc_user_id, status, created_at, reviewer_id, updated_at)
                    VALUES(?, ?, 'approved', ?, ?, ?)
                    ON CONFLICT(discord_id) DO UPDATE SET
                      mc_user_id=excluded.mc_user_id,
                      status='approved',
                      reviewer_id=excluded.reviewer_id,
                      updated_at=excluded.updated_at
                    """, (str(user.id), str(mc_id), utcnow_iso(), str(approver.id if approver else 0), utcnow_iso()))
                    con.commit()
                finally:
                    con.close()
            await asyncio.get_running_loop().run_in_executor(None, _run)

            # Grant role
            if role and role not in user.roles:
                try:
                    await user.add_roles(role, reason="MemberSync verified")
                    await self._debug_log(f"✅ Granted {role.name} to {user.name}")
                except Exception as e:
                    await self._debug_log(f"Failed to grant role: {e}", "error")

            # DM user
            try:
                await user.send(f"✅ Your Missionchief account `{mc_id}` has been approved and linked!")
            except Exception as e:
                await self._debug_log(f"Could not DM user: {e}", "warning")

            # Log to log channel
            log_ch_id = await self.config.log_channel_id()
            ch = guild.get_channel(int(log_ch_id)) if log_ch_id else None
            if isinstance(ch, discord.TextChannel):
                url = _mc_profile_url(mc_id)
                await ch.send(f"✅ Linked {user.mention} to MC [{mc_id}]({url})")

            await self._debug_log(f"✅ Link approved successfully")
            return True, "Approved, linked and role granted."
        
        except Exception as e:
            await self._debug_log(f"Error approving link: {e}", "error")
            log.exception("Error in _approve_link")
            return False, f"Error: {str(e)}"

    async def _deny_link(self, guild: discord.Guild, user: discord.Member, mc_id: str, reviewer: Optional[discord.Member], reason: str) -> None:
        """Deny a verification request"""
        try:
            await self._debug_log(f"Denying link: Discord {user.id} -> MC {mc_id}")
            
            def _run():
                con = sqlite3.connect(self.links_db)
                try:
                    con.execute("""
                    INSERT INTO links(discord_id, mc_user_id, status, created_at, reviewer_id, updated_at)
                    VALUES(?, ?, 'denied', ?, ?, ?)
                    ON CONFLICT(discord_id) DO UPDATE SET
                      mc_user_id=excluded.mc_user_id,
                      status='denied',
                      reviewer_id=excluded.reviewer_id,
                      updated_at=excluded.updated_at
                    """, (str(user.id), str(mc_id), utcnow_iso(), str(reviewer.id if reviewer else 0), utcnow_iso()))
                    con.commit()
                finally:
                    con.close()
            await asyncio.get_running_loop().run_in_executor(None, _run)

            # DM user
            try:
                await user.send(f"❌ Your verification for MC `{mc_id}` was denied. Reason: {reason}")
            except Exception:
                pass

            # Log
            log_ch_id = await self.config.log_channel_id()
            ch = guild.get_channel(int(log_ch_id)) if log_ch_id else None
            if isinstance(ch, discord.TextChannel):
                await ch.send(f"❌ Denied verification for {user.mention} (MC `{mc_id}`): {reason}")

            await self._debug_log(f"✅ Link denied")
        
        except Exception as e:
            await self._debug_log(f"Error denying link: {e}", "error")
            log.exception("Error in _deny_link")

    async def _queue_loop(self):
        """Background task that processes the verification queue"""
        await self.bot.wait_until_red_ready()
        log.info("MemberSync queue loop started")
        
        while True:
            try:
                await self._process_queue_once()
            except Exception as e:
                log.exception("Queue loop error: %s", e)
                await self._debug_log(f"Queue loop error: {e}", "error")
            await asyncio.sleep(120)  # Check every 2 minutes

    async def _process_queue_once(self):
        """Process verification queue once"""
        queue = await self.config.queue()
        if not queue:
            return
        
        await self._debug_log(f"Processing queue ({len(queue)} items)")
        
        guild = self.bot.guilds[0] if self.bot.guilds else None
        if not guild:
            await self._debug_log("No guild available", "warning")
            return

        done = []
        for user_id, data in queue.items():
            try:
                attempts = int(data.get("attempts", 0))
                mc_id = data.get("mc_id")
                discord_user = guild.get_member(int(user_id))
                
                if not discord_user:
                    await self._debug_log(f"User {user_id} no longer in guild, removing from queue")
                    done.append(user_id)
                    continue

                await self._debug_log(f"Processing queue item: {discord_user.name} (attempt {attempts + 1})")

                # Try to find member
                cand = await self._find_member_in_db(discord_user.nick or discord_user.name, mc_id)
                
                if cand and cand.get("mc_id"):
                    # Found! Send review embed
                    await self._debug_log(f"✅ Found {discord_user.name} in database")
                    
                    msg_id = await self._send_review_embed(
                        guild, 
                        discord_user, 
                        str(cand["mc_id"]), 
                        str(cand.get("name") or discord_user.display_name)
                    )
                    
                    if msg_id:
                        # Successfully sent review embed
                        try:
                            await discord_user.send(
                                "✅ Your verification request has been found and queued for review! "
                                "An administrator will approve or deny it shortly."
                            )
                        except Exception as e:
                            await self._debug_log(f"Could not DM user: {e}", "warning")
                        
                        done.append(user_id)
                        await self._debug_log(f"✅ Queued verification for {discord_user.name}")
                    else:
                        # Failed to send review embed - will retry
                        await self._debug_log(f"Failed to send review embed for {discord_user.name}, will retry", "warning")
                        attempts += 1
                        data["attempts"] = attempts
                        queue[user_id] = data
                    
                    continue

                # Not found yet, increment attempts
                attempts += 1
                data["attempts"] = attempts
                queue[user_id] = data
                
                await self._debug_log(f"Member not found yet, attempt {attempts}/30")

                # Give up after 30 attempts (1 hour)
                if attempts >= 30:
                    try:
                        await discord_user.send(
                            "❌ Verification queue expired. We couldn't find your account in the alliance roster. "
                            "Please make sure your Discord nickname matches your MissionChief name exactly, "
                            "or provide your MC User ID when verifying."
                        )
                    except Exception:
                        pass
                    done.append(user_id)
                    await self._debug_log(f"Queue expired for {discord_user.name}")
            
            except Exception as e:
                await self._debug_log(f"Error processing queue item {user_id}: {e}", "error")
                log.exception(f"Error processing queue item {user_id}")

        # Remove completed items from queue
        if done:
            for uid in done:
                queue.pop(uid, None)
            await self.config.queue.set(queue)
            await self._debug_log(f"Removed {len(done)} items from queue")

    @commands.group(name="membersync")
    @checks.admin_or_permissions(manage_guild=True)
    async def membersync_group(self, ctx: commands.Context):
        """MemberSync administration."""
        pass

    @membersync_group.command(name="status")
    async def status(self, ctx: commands.Context):
        """Show configuration and queue status."""
        cfg = await self.config.all()
        
        embed = discord.Embed(title="MemberSync Status", color=discord.Color.blue())
        
        # Database info
        db_path = cfg['alliance_db_path']
        embed.add_field(name="Alliance DB", value=f"`{db_path}`" if db_path else "❌ Not configured", inline=False)
        
        # Channels
        review_ch = ctx.guild.get_channel(int(cfg['review_channel_id'])) if cfg['review_channel_id'] else None
        log_ch = ctx.guild.get_channel(int(cfg['log_channel_id'])) if cfg['log_channel_id'] else None
        embed.add_field(name="Review Channel", value=review_ch.mention if review_ch else f"❌ {cfg['review_channel_id']}", inline=True)
        embed.add_field(name="Log Channel", value=log_ch.mention if log_ch else f"❌ {cfg['log_channel_id']}", inline=True)
        
        # Roles
        verified_role = ctx.guild.get_role(int(cfg['verified_role_id'])) if cfg['verified_role_id'] else None
        embed.add_field(name="Verified Role", value=verified_role.mention if verified_role else f"❌ {cfg['verified_role_id']}", inline=True)
        
        reviewer_roles = []
        for rid in cfg['reviewer_role_ids']:
            r = ctx.guild.get_role(int(rid))
            if r:
                reviewer_roles.append(r.mention)
        embed.add_field(name="Reviewer Roles", value=", ".join(reviewer_roles) if reviewer_roles else "None", inline=False)
        
        # Queue
        queue_size = len(cfg.get('queue', {}))
        embed.add_field(name="Queue Size", value=f"{queue_size} pending", inline=True)
        embed.add_field(name="Cooldown", value=f"{cfg['cooldown_seconds']}s", inline=True)
        
        # Debug mode
        debug_status = "✅ Enabled" if cfg.get('debug_mode', False) else "❌ Disabled"
        embed.add_field(name="Debug Mode", value=debug_status, inline=True)
        
        # Link stats
        def _get_stats():
            con = sqlite3.connect(self.links_db)
            try:
                cur = con.execute("SELECT COUNT(*) FROM links WHERE status='approved'")
                approved = cur.fetchone()[0]
                cur = con.execute("SELECT COUNT(*) FROM links WHERE status='pending'")
                pending = cur.fetchone()[0]
                cur = con.execute("SELECT COUNT(*) FROM links WHERE status='denied'")
                denied = cur.fetchone()[0]
                return approved, pending, denied
            finally:
                con.close()
        
        loop = asyncio.get_running_loop()
        approved, pending, denied = await loop.run_in_executor(None, _get_stats)
        
        embed.add_field(name="Links", value=f"✅ {approved} | ⏳ {pending} | ❌ {denied}", inline=False)
        
        embed.set_footer(text=f"MemberSync v{self.__version__}")
        
        await ctx.send(embed=embed)

    @membersync_group.group(name="queue")
    async def queue_group(self, ctx: commands.Context):
        """Queue management commands."""
        pass

    @queue_group.command(name="show")
    async def queue_show(self, ctx: commands.Context):
        """Show all items currently in the verification queue."""
        queue = await self.config.queue()
        
        if not queue:
            await ctx.send("✅ Queue is empty!")
            return
        
        embed = discord.Embed(
            title=f"Verification Queue ({len(queue)} items)",
            color=discord.Color.orange(),
            timestamp=datetime.utcnow()
        )
        
        for user_id, data in list(queue.items())[:25]:  # Max 25 fields
            member = ctx.guild.get_member(int(user_id))
            if member:
                name = f"{member.name} ({member.nick or 'No nickname'})"
            else:
                name = f"User ID: {user_id} (not in guild)"
            
            attempts = data.get("attempts", 0)
            enqueued = data.get("enqueued_at", "Unknown")
            mc_id = data.get("mc_id", "Not provided")
            
            value = f"Attempts: {attempts}/30\nEnqueued: {enqueued[:16]}\nMC ID: {mc_id}"
            embed.add_field(name=name, value=value, inline=False)
        
        if len(queue) > 25:
            embed.set_footer(text=f"Showing 25/{len(queue)} items")
        
        await ctx.send(embed=embed)

    @queue_group.command(name="clear")
    async def queue_clear(self, ctx: commands.Context):
        """Clear the entire verification queue."""
        queue = await self.config.queue()
        count = len(queue)
        
        if count == 0:
            await ctx.send("Queue is already empty.")
            return
        
        await self.config.queue.set({})
        await ctx.send(f"✅ Cleared {count} items from the queue.")
        await self._debug_log(f"Queue cleared by {ctx.author.name} ({count} items)")

    @queue_group.command(name="remove")
    async def queue_remove(self, ctx: commands.Context, member: discord.Member):
        """Remove a specific member from the queue."""
        queue = await self.config.queue()
        user_id = str(member.id)
        
        if user_id not in queue:
            await ctx.send(f"{member.mention} is not in the queue.")
            return
        
        queue.pop(user_id)
        await self.config.queue.set(queue)
        await ctx.send(f"✅ Removed {member.mention} from the queue.")
        await self._debug_log(f"Removed {member.name} from queue by {ctx.author.name}")

    @membersync_group.command(name="test")
    async def test_lookup(self, ctx: commands.Context, member: discord.Member, mc_id: Optional[str] = None):
        """Test if a member can be found in the alliance database."""
        await ctx.send(f"🔍 Testing lookup for {member.mention}...")
        await self._debug_log(f"Manual test lookup: {member.name} (MC ID: {mc_id})")
        
        result = await self._find_member_in_db(member.nick or member.name, mc_id)
        
        if result and result.get("mc_id"):
            embed = discord.Embed(
                title="✅ Member Found!",
                color=discord.Color.green(),
                description=f"This member can be verified"
            )
            embed.add_field(name="MC Name", value=result.get("name", "Unknown"), inline=True)
            embed.add_field(name="MC ID", value=result.get("mc_id", "Unknown"), inline=True)
            embed.add_field(name="Rank", value=result.get("role", "Unknown"), inline=True)
            embed.add_field(name="Credits", value=f"{result.get('earned_credits', 0):,}", inline=True)
            
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Member Not Found",
                color=discord.Color.red(),
                description="This member could not be found in the alliance database."
            )
            embed.add_field(name="Searched Name", value=member.nick or member.name, inline=True)
            embed.add_field(name="Searched MC ID", value=mc_id or "Not provided", inline=True)
            embed.add_field(name="Suggestion", value="Make sure the Discord nickname matches the MC name exactly, or provide the correct MC User ID.", inline=False)
            
            await ctx.send(embed=embed)

    @membersync_group.command(name="debug")
    async def debug_toggle(self, ctx: commands.Context, enabled: bool = True):
        """Enable or disable debug mode (sends detailed logs to this channel)."""
        await self.config.debug_mode.set(enabled)
        
        if enabled:
            await self.config.debug_channel_id.set(int(ctx.channel.id))
            await ctx.send(f"🐛 **Debug mode ENABLED**\nDetailed logs will be sent to {ctx.channel.mention}")
            await self._debug_log(f"Debug mode enabled by {ctx.author.name}")
        else:
            await self.config.debug_channel_id.set(None)
            await ctx.send("🐛 **Debug mode DISABLED**")

    @membersync_group.group(name="config")
    async def config_group(self, ctx: commands.Context):
        """Configure channels, roles and DB path."""
        pass

    @config_group.command(name="setreviewchannel")
    async def setreviewchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where review embeds are posted."""
        await self.config.review_channel_id.set(int(channel.id))
        await ctx.send(f"✅ Review channel set to {channel.mention}")
        await self._debug_log(f"Review channel set to {channel.name}")

    @config_group.command(name="setlogchannel")
    async def setlogchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the log channel for approvals/denials/prune notices."""
        await self.config.log_channel_id.set(int(channel.id))
        await ctx.send(f"✅ Log channel set to {channel.mention}")
        await self._debug_log(f"Log channel set to {channel.name}")

    @config_group.command(name="setverifiedrole")
    async def setverifiedrole(self, ctx: commands.Context, role: discord.Role):
        """Set the Verified role that is granted on approval."""
        await self.config.verified_role_id.set(int(role.id))
        await ctx.send(f"✅ Verified role set to {role.mention}")
        await self._debug_log(f"Verified role set to {role.name}")

    @config_group.command(name="addreviewerrole")
    async def addreviewerrole(self, ctx: commands.Context, role: discord.Role):
        """Add a role that is allowed to approve/deny verifications."""
        roles = await self.config.reviewer_role_ids()
        if int(role.id) not in roles:
            roles.append(int(role.id))
            await self.config.reviewer_role_ids.set(roles)
        await ctx.send(f"✅ Added reviewer role {role.mention}")
        await self._debug_log(f"Added reviewer role {role.name}")

    @config_group.command(name="setalliancedb")
    async def setalliancedb(self, ctx: commands.Context, path: str):
        """Set the path to the alliance database file."""
        await self.config.alliance_db_path.set(path)
        await ctx.send(f"✅ Alliance DB path set to `{path}`")
        await self._debug_log(f"Alliance DB path set to {path}")

    @commands.hybrid_command(name="verify")
    @commands.cooldown(1, 30, commands.BucketType.user)
    @commands.guild_only()
    @app_commands.describe(
        mc_id="Your Missionchief User ID (optional, helps if your nickname doesn't match exactly)"
    )
    async def verify(self, ctx: commands.Context, mc_id: Optional[str] = None):
        """Verify yourself as a member of the alliance. Match your server nickname to your MC name or provide your MC-ID."""
        if not isinstance(ctx.author, discord.Member) or not ctx.guild:
            await ctx.send("❌ This can only be used in a server.")
            return

        # Check if already verified
        link = await self.get_link_for_discord(ctx.author.id)
        if link:
            role_id = await self.config.verified_role_id()
            role = ctx.guild.get_role(int(role_id)) if role_id else None
            if role and role not in ctx.author.roles:
                try:
                    await ctx.author.add_roles(role, reason="MemberSync: ensure verified role")
                except Exception:
                    pass
            await ctx.send("✅ You are already verified.")
            return

        name = ctx.author.nick or ctx.author.name
        await ctx.send("🔍 Looking you up in the roster... this may take a moment.")
        await self._debug_log(f"Verification request from {ctx.author.name} (nick: {name}, MC ID: {mc_id})")

        # Try to find immediately
        cand = await self._find_member_in_db(name, mc_id)
        if cand and cand.get("mc_id"):
            rid = await self._send_review_embed(ctx.guild, ctx.author, str(cand["mc_id"]), str(cand.get("name") or name))
            if rid:
                await ctx.send("✅ Found you! A reviewer will approve or deny your verification shortly.")
                await self._debug_log(f"✅ Immediate match for {ctx.author.name}")
            else:
                await ctx.send("⚠️ Found you, but failed to send review request. Please contact an administrator.")
            return

        # Not found, add to queue
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
            await ctx.author.send(
                "⏳ We couldn't find you in the roster yet.\n\n"
                "I've queued your verification request and will automatically retry every 2 minutes for up to 1 hour. "
                "You'll receive another message once you're found!\n\n"
                "**Tips:**\n"
                "• Make sure your Discord nickname matches your MissionChief name exactly\n"
                "• If you just joined the alliance, wait a few minutes for the roster to update\n"
                "• You can provide your MC User ID by running `/verify <your_mc_id>`"
            )
        except Exception:
            pass
        
        await ctx.send(
            "⏳ I couldn't find you in the roster yet.\n"
            "I've queued your verification and will retry automatically every **2 minutes for up to 1 hour**. "
            "Check your DMs for more information!"
        )
        await self._debug_log(f"Added {ctx.author.name} to queue (will retry for ~1 hour)")

    @membersync_group.group(name="retro")
    async def retro_group(self, ctx: commands.Context):
        """Tools to link existing verified members based on exact nickname matches."""
        pass

    async def _find_by_exact_name(self, name: str) -> Optional[Tuple[str, str]]:
        """Find member by exact name match"""
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
            await ctx.send("❌ Verified role not configured.")
            return
        
        todo = 0
        for m in role.members:
            if await self.get_link_for_discord(m.id):
                continue
            hit = await self._find_by_exact_name(m.nick or m.name)
            if hit:
                todo += 1
        
        await ctx.send(f"📊 Retro scan: **{todo}** member(s) can be auto-linked.")

    @retro_group.command(name="apply")
    async def retro_apply(self, ctx: commands.Context):
        """Apply auto-link for existing Verified members with exact nickname matches."""
        role_id = await self.config.verified_role_id()
        role = ctx.guild.get_role(int(role_id)) if role_id else None
        if not role:
            await ctx.send("❌ Verified role not configured.")
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
        
        await ctx.send(f"✅ Retro applied: **{count}** link(s).")

    @membersync_group.command(name="link")
    async def link(self, ctx: commands.Context, member: discord.Member, mc_id: str, *, display_name: Optional[str] = None):
        """Manually link a Discord member to an MC-ID as approved."""
        if not await self._user_is_reviewer(ctx.author):
            await ctx.send("❌ You are not allowed to do this.")
            return
        await self._approve_link(ctx.guild, member, mc_id, approver=ctx.author if isinstance(ctx.author, discord.Member) else None)
        await ctx.send(f"✅ Linked {member.mention} to MC `{mc_id}`.")

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
            user_id = int(target.strip("<@!>"))
            member = ctx.guild.get_member(user_id)
        elif target.isdigit():
            try:
                member = ctx.guild.get_member(int(target))
                if not member:
                    member = await ctx.guild.fetch_member(int(target))
            except:
                if not mc_id:
                    target_mc_id = target
        else:
            await ctx.send("❌ Invalid target. Use @mention, Discord ID, or MC-ID")
            return
        
        if member and not target_mc_id:
            await ctx.send("❌ Please provide the MC-ID for this user")
            return
        
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
        
        member = None
        mc_id = None
        
        if target.startswith("<@") and target.endswith(">"):
            user_id = int(target.strip("<@!>"))
            member = ctx.guild.get_member(user_id)
        elif target.isdigit():
            try:
                member = ctx.guild.get_member(int(target))
                if not member:
                    member = await ctx.guild.fetch_member(int(target))
            except:
                mc_id = target
                link = await self.get_link_for_mc(mc_id)
                if link:
                    member = ctx.guild.get_member(int(link['discord_id']))
        
        if not member:
            await ctx.send(f"❌ Could not find member. Target: `{target}`")
            return
        
        if not mc_id:
            link = await self.get_link_for_discord(member.id)
            if link:
                mc_id = link['mc_user_id']
            else:
                await ctx.send(f"⚠️ No MC-ID found for {member.mention}. Denying without specific MC-ID.")
                mc_id = "unknown"
        
        await self._deny_link(ctx.guild, member, mc_id, reviewer=ctx.author if isinstance(ctx.author, discord.Member) else None, reason=reason)
        await ctx.send(f"❌ Denied: {member.mention} for MC `{mc_id}`. Reason: {reason}")

    @commands.Cog.listener()
    async def on_ready(self):
        """Start prune loop on bot ready"""
        async def _loop():
            await self.bot.wait_until_red_ready()
            while True:
                try:
                    await self._prune_once()
                except Exception as e:
                    log.exception("prune loop error: %s", e)
                    await self._debug_log(f"Prune loop error: {e}", "error")
                await asyncio.sleep(3600)
        asyncio.create_task(_loop())

    async def _prune_once(self):
        """Remove verified role from members who are no longer in the alliance"""
        guild = self.bot.guilds[0] if self.bot.guilds else None
        if not guild:
            return
        
        role_id = await self.config.verified_role_id()
        role = guild.get_role(int(role_id)) if role_id else None
        if not role:
            return

        # Get current alliance member IDs
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
                    await self._debug_log(f"Auto-pruned {member.name} (MC {mcid})")
                except Exception as e:
                    await self._debug_log(f"Failed to prune {member.name}: {e}", "error")
                
                if isinstance(ch, discord.TextChannel):
                    await ch.send(f"🔎 Auto-prune removed Verified from <@{did}> (MC `{mcid}` no longer found).")

        if removed:
            log.info("Auto-prune removed %s roles", removed)
            await self._debug_log(f"Auto-prune completed: {removed} roles removed")
