"""
MemberManager - Comprehensive member tracking and management
Fire & Rescue Academy Alliance

COMPLETE VERSION v2.0 - WITH SANCTIONS INTEGRATION
- Full SanctionManager integration
- Warning expiry system (30 days)
- Unified sanctions display
- Contribution monitoring
- Role drift detection
"""

from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta

import discord
from redbot.core import commands, Config, checks
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path
from redbot.core.utils.chat_formatting import box, pagify

from .database import MemberDatabase
from .models import MemberData, NoteData, InfractionData
from .views import MemberOverviewView
from .utils import fuzzy_search_member, format_contribution_trend
from .automation import ContributionMonitor
from .config_commands import ConfigCommands

log = logging.getLogger("red.FARA.MemberManager")

__version__ = "2.0.0"

DEFAULTS = {
    "contribution_threshold": 5.0,
    "contribution_trend_weeks": 3,
    "auto_contribution_alert": True,
    "auto_role_drift_check": True,
    "admin_alert_channel": None,
    "modlog_channel": None,
    "admin_role_ids": [],
    "moderator_role_ids": [],
    "note_expiry_days": 90,
    "dormancy_threshold_days": 30,
}


class MemberManager(ConfigCommands, commands.Cog):
    """
    Member Management System for Fire & Rescue Academy.
    
    Provides comprehensive tracking of:
    - Discord and MissionChief member data
    - Notes and sanctions
    - Contribution monitoring
    - Audit trails
    - Warning expiry (30 days automatic)
    """
    
    __version__ = __version__
    
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self,
            identifier=0xFA11A9E5,
            force_registration=True
        )
        self.config.register_global(**DEFAULTS)
        
        # Initialize database
        self.data_path = cog_data_path(self)
        self.db_path = self.data_path / "membermanager.db"
        self.db: Optional[MemberDatabase] = None
        
        # Integration references
        self.membersync: Optional[commands.Cog] = None
        self.alliance_scraper: Optional[commands.Cog] = None
        self.members_scraper: Optional[commands.Cog] = None
        self.sanction_manager: Optional[commands.Cog] = None
        
        # Automation
        self.contribution_monitor: Optional[ContributionMonitor] = None
        self._automation_task: Optional[asyncio.Task] = None
        
        # 🔧 NEW: Warning expiry task
        self._expiry_task: Optional[asyncio.Task] = None
        
        # Persistent views
        self._register_views()
    
    async def cog_load(self) -> None:
        """Initialize cog on load."""
        log.info(f"MemberManager v{__version__} loading...")
        
        # Initialize database
        self.db = MemberDatabase(str(self.db_path))
        await self.db.initialize()
        log.info("Database initialized")
        
        # Detect and connect to other cogs
        await self._connect_integrations()
        
        # Start automation after bot is ready
        asyncio.create_task(self._delayed_start())
        
        # 🔧 NEW: Start warning expiry task
        self._expiry_task = asyncio.create_task(self._run_warning_expiry_task())
        log.info("Warning expiry task started")
        
        log.info("MemberManager loaded successfully")
    
    async def cog_unload(self) -> None:
        """Cleanup on cog unload."""
        log.info("MemberManager unloading...")
        
        if self._automation_task:
            self._automation_task.cancel()
            try:
                await self._automation_task
            except asyncio.CancelledError:
                pass
        
        # 🔧 NEW: Stop warning expiry task
        if self._expiry_task:
            self._expiry_task.cancel()
            try:
                await self._expiry_task
            except asyncio.CancelledError:
                pass
        
        if self.db:
            await self.db.close()
        
        log.info("MemberManager unloaded")
    
    def _register_views(self):
        """Register persistent views for button interactions."""
        pass
    
    async def _delayed_start(self):
        """Start automation after bot is ready."""
        await self.bot.wait_until_red_ready()
        await asyncio.sleep(5)
        
        # Initialize contribution monitor
        if await self.config.auto_contribution_alert():
            self.contribution_monitor = ContributionMonitor(
                self.bot,
                self.db,
                self.config,
                self.alliance_scraper
            )
            self._automation_task = asyncio.create_task(
                self.contribution_monitor.run()
            )
            log.info("Contribution monitoring started")
    
    async def _connect_integrations(self):
        """Detect and connect to other cogs."""
        self.membersync = self.bot.get_cog("MemberSync")
        self.alliance_scraper = self.bot.get_cog("AllianceScraper")
        self.members_scraper = self.bot.get_cog("MembersScraper")
        self.sanction_manager = self.bot.get_cog("SanctionsManager")  # 🔧 CRITICAL
        
        integrations = []
        if self.membersync:
            integrations.append("MemberSync")
        if self.alliance_scraper:
            integrations.append("AllianceScraper")
        if self.members_scraper:
            integrations.append("MembersScraper")
        if self.sanction_manager:
            integrations.append("SanctionsManager")
        
        if integrations:
            log.info(f"Connected to: {', '.join(integrations)}")
        else:
            log.warning("No integrations found - some features may not work")
    
    # ==================== 🔧 NEW: WARNING EXPIRY SYSTEM ====================
    
    async def _run_warning_expiry_task(self):
        """
        Daily task to expire old warnings.
        
        Runs at 3 AM UTC daily.
        """
        await self.bot.wait_until_red_ready()
        
        while True:
            try:
                # Wait until 3 AM UTC
                now = datetime.now(timezone.utc)
                target_time = now.replace(hour=3, minute=0, second=0, microsecond=0)
                
                if now >= target_time:
                    # Already past 3 AM today, schedule for tomorrow
                    target_time += timedelta(days=1)
                
                wait_seconds = (target_time - now).total_seconds()
                log.info(f"Warning expiry task scheduled in {wait_seconds/3600:.1f} hours")
                
                await asyncio.sleep(wait_seconds)
                
                # Run expiry check
                await self._expire_old_warnings()
                
            except asyncio.CancelledError:
                log.info("Warning expiry task stopped")
                break
            except Exception as e:
                log.error(f"Error in warning expiry task: {e}", exc_info=True)
                # Wait 1 hour before retrying on error
                await asyncio.sleep(3600)
    
    async def _expire_old_warnings(self):
        """
        Expire warnings older than 30 days.
        
        Updates status to 'expired' in database.
        """
        if not self.sanction_manager:
            log.warning("SanctionManager not available for warning expiry")
            return
        
        log.info("Running warning expiry check...")
        
        try:
            total_expired = 0
            
            # Get all guilds
            for guild in self.bot.guilds:
                try:
                    expired_count = await self._expire_guild_warnings(guild.id)
                    total_expired += expired_count
                    
                    if expired_count > 0:
                        log.info(f"Expired {expired_count} warnings in guild {guild.name}")
                
                except Exception as e:
                    log.error(f"Error expiring warnings for guild {guild.id}: {e}")
            
            log.info(f"Warning expiry complete. Total expired: {total_expired}")
        
        except Exception as e:
            log.error(f"Error in warning expiry: {e}", exc_info=True)
    
    async def _expire_guild_warnings(self, guild_id: int) -> int:
        """
        Expire warnings for a specific guild.
        
        Returns number of warnings expired.
        """
        if not self.sanction_manager:
            return 0
        
        try:
            import sqlite3
            
            # Calculate 30 days ago
            now = int(datetime.now(timezone.utc).timestamp())
            thirty_days_ago = now - (30 * 86400)
            
            # Update warnings in database
            conn = sqlite3.connect(self.sanction_manager.db.db_path)
            cursor = conn.cursor()
            
            # Find warnings to expire
            cursor.execute("""
                UPDATE sanctions
                SET status = 'expired'
                WHERE guild_id = ?
                AND status = 'active'
                AND sanction_type LIKE '%Warning%'
                AND created_at < ?
            """, (guild_id, thirty_days_ago))
            
            expired_count = cursor.rowcount
            
            # Also log the expiry in sanction_history
            if expired_count > 0:
                cursor.execute("""
                    SELECT sanction_id FROM sanctions
                    WHERE guild_id = ?
                    AND status = 'expired'
                    AND sanction_type LIKE '%Warning%'
                    AND created_at < ?
                """, (guild_id, thirty_days_ago))
                
                expired_ids = [row[0] for row in cursor.fetchall()]
                
                # Log each expiry
                for sanction_id in expired_ids:
                    cursor.execute("""
                        INSERT INTO sanction_history
                        (sanction_id, action_type, action_by, action_at, notes)
                        VALUES (?, 'auto_expired', 0, ?, 'Auto-expired after 30 days')
                    """, (sanction_id, now))
            
            conn.commit()
            conn.close()
            
            return expired_count
        
        except Exception as e:
            log.error(f"Error expiring guild warnings: {e}", exc_info=True)
            return 0
    
    # ==================== PERMISSIONS ====================
    
    async def _is_admin(self, member: discord.Member) -> bool:
        """Check if member has admin permissions."""
        if member.guild_permissions.administrator:
            return True
        
        admin_role_ids = await self.config.admin_role_ids()
        return any(role.id in admin_role_ids for role in member.roles)
    
    async def _is_moderator(self, member: discord.Member) -> bool:
        """Check if member has moderator permissions."""
        if await self._is_admin(member):
            return True
        
        mod_role_ids = await self.config.moderator_role_ids()
        return any(role.id in mod_role_ids for role in member.roles)
    
    # ==================== MEMBER LOOKUP ====================
    
    async def _resolve_target(
        self,
        guild: discord.Guild,
        target: str
    ) -> Optional[MemberData]:
        """
        Resolve a target string to member data.
        
        Tries in order:
        1. Discord mention/ID
        2. MC ID (direct database lookup)
        3. Fuzzy search on names
        """
        # Try Discord mention/ID
        discord_member = None
        try:
            # Check if it's a mention
            if target.startswith("<@") and target.endswith(">"):
                user_id = int(target.strip("<@!>"))
                discord_member = guild.get_member(user_id)
            else:
                # Try as raw ID
                user_id = int(target)
                discord_member = guild.get_member(user_id)
        except ValueError:
            pass
        
        if discord_member:
            return await self._build_member_data(
                guild=guild,
                discord_id=discord_member.id
            )
        
        # Try MC ID (direct database lookup in members_v2.db)
        if target.isdigit():
            mc_data = await self._get_mc_data(target)
            
            if mc_data:
                # Found in members database! Now check if there's a link
                discord_id = None
                if self.membersync:
                    link = await self.membersync.get_link_for_mc(target)
                    if link:
                        discord_id = link.get("discord_id")
                
                # Build member data (with or without Discord link)
                return await self._build_member_data(
                    guild=guild,
                    discord_id=discord_id,
                    mc_user_id=target
                )
        
        # Fuzzy search (last resort)
        result = await fuzzy_search_member(
            target=target,
            guild=guild,
            membersync=self.membersync,
            alliance_scraper=self.alliance_scraper
        )
        
        if result:
            return await self._build_member_data(
                guild=guild,
                discord_id=result.get("discord_id"),
                mc_user_id=result.get("mc_user_id")
            )
        
        return None
    
    async def _build_member_data(
        self,
        guild: discord.Guild,
        discord_id: Optional[int] = None,
        mc_user_id: Optional[str] = None
    ) -> MemberData:
        """
        Build a complete MemberData object from available sources.
        
        🔧 UPDATED: Now includes sanctions from SanctionManager with expiry logic
        """
        data = MemberData(
            discord_id=discord_id,
            mc_user_id=mc_user_id
        )
        
        # Get Discord data
        if discord_id:
            member = guild.get_member(discord_id)
            if member:
                data.discord_username = str(member)
                data.discord_roles = [r.name for r in member.roles if r.name != "@everyone"]
                data.discord_joined = member.joined_at
        
        # Get MC data and link status from MemberSync
        if self.membersync:
            link = None
            
            # Get link data
            if discord_id and not mc_user_id:
                link = await self.membersync.get_link_for_discord(discord_id)
                if link:
                    data.mc_user_id = link.get("mc_user_id")
                    data.link_status = link.get("status", "none")
            elif mc_user_id and not discord_id:
                link = await self.membersync.get_link_for_mc(mc_user_id)
                if link:
                    data.discord_id = int(link.get("discord_id"))
                    data.link_status = link.get("status", "none")
            elif discord_id and mc_user_id:
                # Both provided, just get status
                link = await self.membersync.get_link_for_discord(discord_id)
                if link:
                    data.link_status = link.get("status", "none")
            
            # Set is_verified ONLY if link exists AND is approved
            if link and link.get("status") == "approved":
                data.is_verified = True
                
                # Also check if they have the verified role
                if discord_id:
                    member = guild.get_member(discord_id)
                    if member:
                        verified_role_id = await self.membersync.config.verified_role_id()
                        if verified_role_id:
                            verified_role = guild.get_role(verified_role_id)
                            if verified_role and verified_role not in member.roles:
                                log.warning(f"Member {discord_id} is linked but missing verified role")
            else:
                data.is_verified = False
                data.link_status = link.get("status", "none") if link else "none"
        
        # Get MC data from MembersScraper
        mc_in_alliance = False
        if data.mc_user_id and self.members_scraper:
            try:
                mc_data = await self._get_mc_data(data.mc_user_id)
                if mc_data:
                    data.mc_username = mc_data.get("name")
                    data.mc_role = mc_data.get("role")
                    data.contribution_rate = mc_data.get("contribution_rate")
                    mc_in_alliance = True
            except Exception as e:
                log.error(f"Failed to get MC data for {data.mc_user_id}: {e}")
        
        # If has MC ID but not in alliance, they're not active
        if data.mc_user_id and not mc_in_alliance:
            data.mc_username = f"Former member ({data.mc_user_id})"
            data.mc_role = "Left alliance"
        
        # Get notes count
        if self.db:
            try:
                notes = await self.db.get_notes(
                    discord_id=data.discord_id,
                    mc_user_id=data.mc_user_id
                )
                data.notes_count = len(notes)
            except Exception as e:
                log.error(f"Failed to get notes: {e}")
                data.notes_count = 0
        
        # 🔧 UPDATED: Get sanctions from SanctionManager with expiry logic
        if self.sanction_manager:
            try:
                sanctions = self.sanction_manager.db.get_user_sanctions(
                    guild_id=guild.id,
                    discord_user_id=data.discord_id,
                    mc_user_id=data.mc_user_id
                )
                
                # Calculate 30 days ago for expiry check
                now = int(datetime.now(timezone.utc).timestamp())
                thirty_days_ago = now - (30 * 86400)
                
                # Count only active sanctions (not expired)
                active_sanctions = []
                for sanction in sanctions:
                    status = sanction.get("status", "active")
                    is_warning = "Warning" in sanction.get("sanction_type", "")
                    created_at = sanction.get("created_at", 0)
                    
                    # Count as active if:
                    # 1. Status is active AND
                    # 2. Either not a warning OR warning is < 30 days old
                    if status == "active":
                        if not is_warning or created_at >= thirty_days_ago:
                            active_sanctions.append(sanction)
                
                data.infractions_count = len(active_sanctions)
                
                # Calculate severity score
                data.severity_score = 0
                for sanction in active_sanctions:
                    stype = sanction.get("sanction_type", "")
                    if "Warning" in stype:
                        if "1st" in stype:
                            data.severity_score += 2
                        elif "2nd" in stype:
                            data.severity_score += 4
                        elif "3rd" in stype:
                            data.severity_score += 6
                        else:
                            data.severity_score += 1
                    elif "Kick" in stype:
                        data.severity_score += 7
                    elif "Ban" in stype:
                        data.severity_score += 10
                    elif "Mute" in stype:
                        data.severity_score += 3
                    else:
                        data.severity_score += 1
            
            except Exception as e:
                log.error(f"Failed to get sanctions: {e}")
                data.infractions_count = 0
                data.severity_score = 0
        
        return data
    
    async def _get_mc_data(self, mc_user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get MC member data from MembersScraper (members_v2.db).
        """
        if not self.members_scraper:
            log.warning("MembersScraper not available")
            return None
        
        try:
            import sqlite3
            from pathlib import Path
            
            # Get database path from MembersScraper
            db_path = Path(self.members_scraper.db_path)
            
            if not db_path.exists():
                log.error(f"Database not found: {db_path}")
                return None
            
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Get most recent record INCLUDING contribution_rate
            cursor.execute("""
                SELECT member_id, username, rank, earned_credits, contribution_rate, online_status, timestamp
                FROM members
                WHERE member_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (mc_user_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                result = {
                    "user_id": row["member_id"],
                    "name": row["username"],
                    "role": row["rank"],
                    "earned_credits": row["earned_credits"],
                    "contribution_rate": row["contribution_rate"]
                }
                return result
            else:
                return None
                
        except Exception as e:
            log.error(f"Failed to get MC data for {mc_user_id}: {e}", exc_info=True)
            return None
    
    # ==================== COMMANDS ====================
    
    @commands.command(name="member")
    @commands.guild_only()
    async def member_info(self, ctx, *, target: str):
        """
        Look up comprehensive member information.
        
        Supports:
        - Discord @mention or ID
        - MissionChief ID
        - Name search (fuzzy)
        """
        if not await self._is_moderator(ctx.author):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        async with ctx.typing():
            # Resolve target
            member_data = await self._resolve_target(ctx.guild, target)
            
            if not member_data:
                await ctx.send(
                    f"❌ Could not find member matching `{target}`.\n"
                    f"Try using their Discord mention, MC ID, or exact name."
                )
                return
            
            # Create view with member data
            view = MemberOverviewView(
                bot=self.bot,
                db=self.db,
                config=self.config,
                member_data=member_data,
                integrations={
                    "membersync": self.membersync,
                    "alliance_scraper": self.alliance_scraper,
                    "sanction_manager": self.sanction_manager
                },
                invoker_id=ctx.author.id
            )
            
            # Get initial embed
            embed = await view.get_overview_embed()
            
            # Send message
            message = await ctx.send(embed=embed, view=view)
            view.message = message
    
    @commands.command(name="membersearch")
    @commands.guild_only()
    async def member_search(self, ctx, *, query: str):
        """Search for members by name (fuzzy search)."""
        if not await self._is_moderator(ctx.author):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        async with ctx.typing():
            # Perform fuzzy search
            results = await fuzzy_search_member(
                target=query,
                guild=ctx.guild,
                membersync=self.membersync,
                alliance_scraper=self.alliance_scraper,
                limit=10
            )
            
            if not results:
                await ctx.send(f"❌ No members found matching `{query}`")
                return
            
            # Format results
            embed = discord.Embed(
                title=f"🔍 Search Results: {query}",
                color=discord.Color.blue()
            )
            
            lines = []
            for i, result in enumerate(results, 1):
                discord_id = result.get("discord_id")
                mc_id = result.get("mc_user_id")
                name = result.get("name", "Unknown")
                
                line = f"{i}. **{name}**"
                if discord_id:
                    line += f" • <@{discord_id}>"
                if mc_id:
                    line += f" • MC: `{mc_id}`"
                
                lines.append(line)
            
            embed.description = "\n".join(lines)
            embed.set_footer(text=f"Found {len(results)} result(s)")
            
            await ctx.send(embed=embed)
    
    @commands.command(name="memberstats")
    @commands.guild_only()
    async def member_stats(self, ctx):
        """Show overall member management statistics."""
        if not await self._is_admin(ctx.author):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        async with ctx.typing():
            embed = discord.Embed(
                title="📊 Member Management Statistics",
                color=discord.Color.gold()
            )
            
            try:
                # Total notes
                notes = await self.db.get_notes(limit=9999)
                active_notes = len([n for n in notes if n["status"] == "active"])
                
                embed.add_field(
                    name="📝 Notes",
                    value=f"**Total:** {len(notes)}\n**Active:** {active_notes}",
                    inline=True
                )
                
                # Total sanctions (from SanctionManager)
                if self.sanction_manager:
                    # This would need a method to get all sanctions
                    embed.add_field(
                        name="🚨 Sanctions",
                        value="See `/sanction stats`",
                        inline=True
                    )
                
                # Events
                events = await self.db.get_events(limit=9999)
                
                embed.add_field(
                    name="📅 Events",
                    value=f"**Total:** {len(events)}",
                    inline=True
                )
                
                # Integration status
                integrations = []
                if self.membersync:
                    integrations.append("✅ MemberSync")
                else:
                    integrations.append("❌ MemberSync")
                
                if self.members_scraper:
                    integrations.append("✅ MembersScraper")
                else:
                    integrations.append("❌ MembersScraper")
                
                if self.alliance_scraper:
                    integrations.append("✅ AllianceScraper")
                else:
                    integrations.append("❌ AllianceScraper")
                
                if self.sanction_manager:
                    integrations.append("✅ SanctionManager")
                else:
                    integrations.append("❌ SanctionManager")
                
                embed.add_field(
                    name="🔌 Integrations",
                    value="\n".join(integrations),
                    inline=False
                )
                
            except Exception as e:
                log.error(f"Error getting stats: {e}")
                embed.description = "⚠️ Error retrieving statistics"
            
            await ctx.send(embed=embed)


async def setup(bot: Red):
    """Required setup function for Red-DiscordBot cog loading."""
    cog = MemberManager(bot)
    await bot.add_cog(cog)
