"""
MemberManager - Comprehensive member tracking and management
Fire & Rescue Academy Alliance

Integrates with:
- MemberSync: Discord â†” MC linking
- AllianceScraper: MC member data, contributions, logs
- Red's modlog: Discord infractions
"""

from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

import discord
from discord import app_commands
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

__version__ = "2.1.2"

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
    - Notes and infractions
    - Contribution monitoring
    - Audit trails
    """
    
    __version__ = __version__
    
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self,
            identifier=0xFA11A9E5,  # Unique identifier for this cog
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
        
        if self.db:
            await self.db.close()
        
        log.info("MemberManager unloaded")
    
    def _register_views(self):
        """Register persistent views for button interactions."""
        # Views will be added here when we create them
        pass
    
    async def _delayed_start(self):
        """Start automation after bot is ready."""
        await self.bot.wait_until_ready()
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
        self.sanction_manager = self.bot.get_cog("SanctionManager")
        
        integrations = []
        if self.membersync:
            integrations.append("MemberSync")
        if self.alliance_scraper:
            integrations.append("AllianceScraper")
        if self.members_scraper:
            integrations.append("MembersScraper")
        if self.sanction_manager:
            integrations.append("SanctionManager")
        
        if integrations:
            log.info(f"Connected to: {', '.join(integrations)}")
        else:
            log.warning("No integrations found - some features may not work")
    
    # ==================== PERMISSIONS ====================
    
    async def _is_admin(self, member: discord.Member) -> bool:
        """Check if member has admin permissions."""
        if member.guild_permissions.administrator:
            return True
        
        admin_roles = await self.config.admin_role_ids()
        return any(role.id in admin_roles for role in member.roles)
    
    async def _is_moderator(self, member: discord.Member) -> bool:
        """Check if member has moderator permissions (read-only + limited actions)."""
        if await self._is_admin(member):
            return True
        
        mod_roles = await self.config.moderator_role_ids()
        return any(role.id in mod_roles for role in member.roles)
    
    # ==================== LISTENERS ====================
    
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Track when members join Discord."""
        if member.bot:
            return
        
        # Check if they have an MC account linked
        if self.membersync:
            link = await self.membersync.get_link_for_discord(member.id)
            if link:
                await self.db.add_event(
                    guild_id=member.guild.id,
                    discord_id=member.id,
                    mc_user_id=link.get("mc_user_id"),
                    event_type="joined_discord",
                    event_data={"username": str(member)},
                    triggered_by="system"
                )
    
    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Track when members leave Discord."""
        if member.bot:
            return
        
        # Check if they have an MC account
        if self.membersync:
            link = await self.membersync.get_link_for_discord(member.id)
            if link:
                mc_id = link.get("mc_user_id")
                
                # Log the departure
                await self.db.add_event(
                    guild_id=member.guild.id,
                    discord_id=member.id,
                    mc_user_id=mc_id,
                    event_type="left_discord",
                    event_data={"username": str(member)},
                    triggered_by="system"
                )
                
                # Check for coordinated MC + Discord departure (within 72h)
                # This will be implemented in automation.py
    
    @commands.Cog.listener()
    async def on_modlog_case_create(self, case):
        """
        Auto-create infractions from Red's modlog events.
        Triggered when moderators use Red's built-in mod commands.
        """
        try:
            # Get case details
            user_id = case.user.id if hasattr(case, 'user') else None
            if not user_id:
                return
            
            # Map modlog action to our infraction type
            action_type = str(case.action_type).lower()
            
            infraction_map = {
                "ban": "ban",
                "tempban": "ban",
                "kick": "kick",
                "mute": "mute",
                "tempmute": "mute",
                "timeout": "timeout",
                "warning": "warning",
            }
            
            infraction_type = infraction_map.get(action_type)
            if not infraction_type:
                return  # Not a type we track
            
            # Get MC ID if linked
            mc_id = None
            if self.membersync:
                link = await self.membersync.get_link_for_discord(user_id)
                if link:
                    mc_id = link.get("mc_user_id")
            
            # Calculate duration for temp actions
            duration = None
            if hasattr(case, 'until') and case.until:
                duration = int((case.until - datetime.now(timezone.utc)).total_seconds())
            
            # Create infraction
            await self.db.add_infraction(
                guild_id=case.guild.id,
                discord_id=user_id,
                mc_user_id=mc_id,
                target_name=str(case.user) if hasattr(case, 'user') else "Unknown",
                platform="discord",
                infraction_type=infraction_type,
                reason=case.reason or "No reason provided",
                moderator_id=case.moderator.id if hasattr(case, 'moderator') else None,
                moderator_name=str(case.moderator) if hasattr(case, 'moderator') else "Unknown",
                duration=duration
            )
            
            log.info(
                f"Auto-created infraction for {user_id} "
                f"(type: {infraction_type}, case: {case.case_number})"
            )
            
        except Exception as e:
            log.error(f"Failed to create infraction from modlog: {e}", exc_info=True)
    
    # ==================== MAIN COMMAND GROUP ====================
    
    @commands.hybrid_group(name="member", fallback="help")
    @commands.guild_only()
    async def member(self, ctx: commands.Context):
        """
        Member management commands.
        
        View member information, add notes/infractions, and more.
        """
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    # ==================== WHOIS COMMAND ====================
    
    @member.command(name="whois", aliases=["lookup", "info"])
    @commands.guild_only()
    async def whois(
        self,
        ctx: commands.Context,
        *,
        target: str
    ):
        """
        Look up a member by Discord mention, MC ID, MC username, or Discord username.
        
        Uses fuzzy search to find members even with typos.
        
        **Examples:**
        - `[p]member whois @JohnDoe`
        - `[p]member whois 123456` (MC ID)
        - `[p]member whois JohnDoe` (fuzzy search)
        """
        # Check permissions
        if not await self._is_moderator(ctx.author):
            await ctx.send("âŒ You need moderator permissions to use this command.")
            return
        
        await ctx.typing()
        
        # Try to find the member
        member_data = await self._resolve_target(ctx.guild, target)
        
        if not member_data:
            await ctx.send(
                f"âŒ Could not find member matching `{target}`.\n"
                "Try using: @mention, MC ID, MC username, or Discord username."
            )
            return
        
        # Create the overview view with tabs
        view = MemberOverviewView(
            bot=self.bot,
            db=self.db,
            config=self.config,
            member_data=member_data,
            integrations={
                "membersync": self.membersync,
                "alliance_scraper": self.alliance_scraper,
                "sanction_manager": self.sanction_manager,
            },
            invoker_id=ctx.author.id,
            guild=ctx.guild
        )
        
        # Get initial embed
        embed = await view.get_overview_embed()
        
        await ctx.send(embed=embed, view=view)
    
    # ==================== SEARCH COMMAND ====================
    
    @member.command(name="search", aliases=["find"])
    @commands.guild_only()
    async def search(
        self,
        ctx: commands.Context,
        *,
        query: str
    ):
        """
        Search for members by name (MC or Discord).
        
        Shows multiple results with fuzzy matching.
        
        **Examples:**
        - `[p]member search John`
        - `[p]member search 1161` (partial MC ID)
        - `[p]member search brandjuh`
        """
        # Check permissions
        if not await self._is_moderator(ctx.author):
            await ctx.send("❌ You need moderator permissions to use this command.")
            return
        
        await ctx.typing()
        
        # Search with lower threshold for more results
        from .utils import fuzzy_match_score
        
        results = []
        query_lower = query.lower().strip()
        
        # Search Discord members
        for member in ctx.guild.members:
            if member.bot:
                continue
            
            # Check username
            score = fuzzy_match_score(query_lower, str(member))
            if score >= 0.5:  # Lower threshold
                results.append({
                    "score": score,
                    "discord_id": member.id,
                    "mc_user_id": None,
                    "name": str(member),
                    "display_name": member.display_name,
                    "source": "discord"
                })
            
            # Check display name
            if member.display_name != str(member):
                score = fuzzy_match_score(query_lower, member.display_name)
                if score >= 0.5:
                    results.append({
                        "score": score,
                        "discord_id": member.id,
                        "mc_user_id": None,
                        "name": str(member),
                        "display_name": member.display_name,
                        "source": "discord"
                    })
        
        # Search MC members by NAME (not just ID)
        if self.alliance_scraper:
            try:
                mc_members = await self.alliance_scraper.get_members()
                
                for mc_member in mc_members:
                    mc_name = mc_member.get("name", "")
                    mc_id = mc_member.get("user_id") or mc_member.get("mc_user_id")
                    
                    if not mc_id:
                        continue
                    
                    # Check MC username
                    score = fuzzy_match_score(query_lower, mc_name)
                    
                    # Also check if query is partial MC ID
                    if query_lower.isdigit() and query_lower in str(mc_id):
                        score = max(score, 0.8)
                    
                    if score >= 0.5:
                        # Try to find linked Discord account
                        discord_id = None
                        if self.membersync:
                            link = await self.membersync.get_link_for_mc(mc_id)
                            if link:
                                discord_id = link.get("discord_id")
                        
                        results.append({
                            "score": score,
                            "discord_id": discord_id,
                            "mc_user_id": mc_id,
                            "name": mc_name,
                            "source": "missionchief"
                        })
            except Exception as e:
                log.error(f"Error searching MC members: {e}")
        
        if not results:
            await ctx.send(f"❌ No members found matching `{query}`")
            return
        
        # Sort by score (highest first) and remove duplicates
        seen = set()
        unique_results = []
        for result in sorted(results, key=lambda x: x["score"], reverse=True):
            # Create unique key
            key = (result.get("discord_id"), result.get("mc_user_id"))
            if key not in seen:
                seen.add(key)
                unique_results.append(result)
        
        # Limit to top 15 results
        unique_results = unique_results[:15]
        
        # Build embed
        embed = discord.Embed(
            title=f"🔍 Search Results: {query}",
            description=f"Found {len(unique_results)} member(s)",
            color=discord.Color.blue()
        )
        
        lines = []
        for i, result in enumerate(unique_results, 1):
            name = result.get("name", "Unknown")
            display_name = result.get("display_name")
            discord_id = result.get("discord_id")
            mc_id = result.get("mc_user_id")
            source = result.get("source")
            score = result.get("score", 0)
            
            # Build line
            source_emoji = "🎮" if source == "discord" else "🚒"
            line = f"{i}. {source_emoji} **{name}**"
            
            # Add display name if different
            if display_name and display_name != name:
                line += f" *({display_name})*"
            
            # Add IDs
            if discord_id:
                line += f" • Discord: <@{discord_id}>"
            if mc_id:
                line += f" • MC: `{mc_id}`"
            
            # Add match score for debugging
            line += f" • Match: {score:.0%}"
            
            lines.append(line)
        
        embed.description = "\n".join(lines)
        embed.set_footer(text="Use [p]member whois <name/id> to view full details")
        
        await ctx.send(embed=embed)
    
    async def _resolve_target(
        self,
        guild: discord.Guild,
        target: str
    ) -> Optional[MemberData]:
        """
        Resolve a target string to a MemberData object.
        
        Tries in order:
        1. Discord mention/ID
        2. MC ID
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
        
        # Fuzzy search
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
                
                # Check for verified role from MemberSync config
                if self.membersync:
                    verified_role_id = await self.membersync.config.verified_role_id()
                    if verified_role_id:
                        verified_role = guild.get_role(verified_role_id)
                        if verified_role and verified_role in member.roles:
                            data.is_verified = True
        
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
        
        # Get sanctions from SanctionManager with expiry logic
        if self.sanction_manager:
            try:
                sanctions = self.sanction_manager.db.get_user_sanctions(
                    guild_id=guild.id,
                    discord_user_id=data.discord_id,
                    mc_user_id=data.mc_user_id
                )
                
                # Calculate 30 days ago for expiry check
                from datetime import datetime, timezone, timedelta
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


# This is the standard way to set up a cog for Red-DiscordBot
async def setup(bot: Red):
    """Load MemberManager cog."""
    await bot.add_cog(MemberManager(bot))
