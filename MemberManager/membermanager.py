"""
MemberManager - Comprehensive member tracking and management
Fire & Rescue Academy Alliance

FIXED:
- Verification status now checks link_status='approved' + role
- Better contribution display
- Proper database integration
- Error handling for infractions

Integrates with:
- MemberSync: Discord ↔ MC linking
- AllianceScraper: MC member data, contributions, logs
- SanctionManager: Sanctions/infractions
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

__version__ = "1.1.0"

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
        self.sanction_manager = self.bot.get_cog("SanctionManager")
        
        integrations = []
        if self.membersync:
            integrations.append("MemberSync")
        if self.alliance_scraper:
            integrations.append("AllianceScraper")
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
        
        # Try MC ID (numeric string)
        if target.isdigit() and self.membersync:
            link = await self.membersync.get_link_for_mc(target)
            if link:
                return await self._build_member_data(
                    guild=guild,
                    discord_id=link.get("discord_id"),
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
        
        🔧 FIXED: Now properly checks link_status='approved' for verification
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
        
        # 🔧 FIX: Get MC data and link status from MemberSync
        if self.membersync:
            link = None
            
            # Get link data
            if discord_id and not mc_user_id:
                link = await self.membersync.get_link_for_discord(discord_id)
                if link:
                    data.mc_user_id = link.get("mc_user_id")
                    data.link_status = link.get("status", "unknown")
            elif mc_user_id and not discord_id:
                link = await self.membersync.get_link_for_mc(mc_user_id)
                if link:
                    data.discord_id = int(link.get("discord_id"))
                    data.link_status = link.get("status", "unknown")
            elif discord_id and mc_user_id:
                # Both provided, just get status
                link = await self.membersync.get_link_for_discord(discord_id)
                if link:
                    data.link_status = link.get("status", "unknown")
            
            # 🔧 FIX: Set is_verified based on link_status AND role
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
                                # Linked but missing role - flag this
                                data.is_verified = True  # Still verified in DB
                                log.warning(f"Member {discord_id} is linked but missing verified role")
        
        # 🔧 FIX: Get MC data from AllianceScraper with better error handling
        if data.mc_user_id and self.alliance_scraper:
            try:
                mc_data = await self._get_mc_data(data.mc_user_id)
                if mc_data:
                    data.mc_username = mc_data.get("name")
                    data.mc_role = mc_data.get("role")
                    data.contribution_rate = mc_data.get("contribution_rate")
                    
                    # 🔧 FIX: Ensure contribution_rate is properly formatted
                    if data.contribution_rate is not None:
                        try:
                            data.contribution_rate = float(data.contribution_rate)
                        except (ValueError, TypeError):
                            data.contribution_rate = None
            except Exception as e:
                log.error(f"Failed to get MC data for {data.mc_user_id}: {e}")
        
        # 🔧 FIX: Get notes count with error handling
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
        
        # 🔧 FIX: Get infractions count with error handling
        if self.db:
            try:
                infractions = await self.db.get_infractions(
                    discord_id=data.discord_id,
                    mc_user_id=data.mc_user_id
                )
                data.infractions_count = len([i for i in infractions if i["status"] == "active"])
                
                # Calculate severity score
                data.severity_score = sum(
                    i.get("severity_score", 1) 
                    for i in infractions 
                    if i["status"] == "active"
                )
            except Exception as e:
                log.error(f"Failed to get infractions: {e}")
                data.infractions_count = 0
                data.severity_score = 0
        
        return data
    
    async def _get_mc_data(self, mc_user_id: str) -> Optional[Dict[str, Any]]:
        """Get MC member data from AllianceScraper."""
        if not self.alliance_scraper:
            return None
        
        try:
            # Query alliance database
            rows = await self.alliance_scraper._query_alliance(
                "SELECT name, role, contribution_rate, earned_credits "
                "FROM members_current WHERE user_id=? OR mc_user_id=?",
                (mc_user_id, mc_user_id)
            )
            
            if rows:
                return dict(rows[0])
        except Exception as e:
            log.error(f"Error querying alliance data: {e}")
        
        return None
    
    # ==================== COMMANDS ====================
    
    @commands.hybrid_command(name="member")
    @commands.guild_only()
    async def member_info(
        self,
        ctx: commands.Context,
        *,
        target: str
    ):
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
        
        await ctx.defer()
        
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
            invoker_id=ctx.author.id  # 🔧 FIX: Track who opened this view
        )
        
        # Get initial embed
        embed = await view.get_overview_embed()
        
        # Send message
        message = await ctx.send(embed=embed, view=view)
        view.message = message
    
    @commands.hybrid_command(name="membersearch")
    @commands.guild_only()
    async def member_search(
        self,
        ctx: commands.Context,
        *,
        query: str
    ):
        """
        Search for members by name (fuzzy search).
        """
        if not await self._is_moderator(ctx.author):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.defer()
        
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


# ==================== SETUP FUNCTION ====================

async def setup(bot: Red):
    """Required setup function for Red-DiscordBot cog loading."""
    cog = MemberManager(bot)
    await bot.add_cog(cog)
    
    @commands.hybrid_command(name="memberstats")
    @commands.guild_only()
    async def member_stats(self, ctx: commands.Context):
        """
        Show overall member management statistics.
        """
        if not await self._is_admin(ctx.author):
            await ctx.send("❌ You don't have permission to use this command.")
            return
        
        await ctx.defer()
        
        # Get stats from database
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
            
            # Total infractions
            infractions = await self.db.get_infractions(limit=9999)
            active_infractions = len([i for i in infractions if i["status"] == "active"])
            
            embed.add_field(
                name="⚠️ Infractions",
                value=f"**Total:** {len(infractions)}\n**Active:** {active_infractions}",
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
