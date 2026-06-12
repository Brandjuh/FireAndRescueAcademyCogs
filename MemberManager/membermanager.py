"""
MemberManager - Comprehensive member tracking and management
Fire & Rescue Academy Alliance

Integrates with:
- MemberSync: Discord ↔ MC linking
- AllianceScraper: MC member data, contributions, logs
- Red's modlog: Discord infractions

VERSION: 2.2.4 - FINAL WORKING VERSION
FIXED: All async/sync issues with asyncio.to_thread()
FIXED: Grace period fallback to first scrape date
FIXED: Timezone-aware datetime handling
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

__version__ = "2.2.4"

DEFAULTS = {
    "contribution_threshold": 5.0,
    "contribution_trend_weeks": 3,
    "auto_contribution_alert": True,
    "auto_role_drift_check": True,
    "admin_alert_channel": None,
    "modlog_channel": None,
    "panel_channel_id": 1426226521231589507,
    "panel_message_id": None,
    "admin_role_ids": [],
    "moderator_role_ids": [],
    "note_expiry_days": 90,
    "dormancy_threshold_days": 30,
}


class MemberPanelView(discord.ui.View):
    """Persistent entry point for the staff member management panel."""

    def __init__(self, cog: "MemberManager"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Open Member Management",
        style=discord.ButtonStyle.primary,
        custom_id="membermanager:open_panel",
    )
    async def open_panel(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button,
    ):
        del button
        if not await self.cog._interaction_is_moderator(interaction):
            await interaction.response.send_message(
                "You do not have permission to use MemberManager.",
                ephemeral=True,
            )
            return

        await interaction.response.send_modal(MemberSearchModal(self.cog))


class MemberSearchModal(discord.ui.Modal, title="Member Management Search"):
    """Search modal for opening a private member management profile."""

    query = discord.ui.TextInput(
        label="Member",
        placeholder="Discord mention, Discord ID, MC ID, MC name, or Discord name",
        max_length=100,
    )

    def __init__(self, cog: "MemberManager"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog._open_member_profile_from_interaction(
            interaction,
            str(self.query.value),
        )


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
        self.members_scraper: Optional[commands.Cog] = None
        self.logs_scraper: Optional[commands.Cog] = None
        self.sanction_manager: Optional[commands.Cog] = None
        
        # Automation
        self.contribution_monitor: Optional[ContributionMonitor] = None
        self._automation_task: Optional[asyncio.Task] = None
        self._member_context_menu = app_commands.ContextMenu(
            name="Member Management",
            callback=self._member_context_menu_callback,
        )
        self._context_menu_guild: Optional[discord.Object] = None
        
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

        self._unregister_context_menu()
        
        log.info("MemberManager unloaded")
    
    def _register_views(self):
        """Register persistent views for button interactions."""
        add_view = getattr(self.bot, "add_view", None)
        if add_view:
            add_view(MemberPanelView(self))

    async def _get_context_menu_guild(self) -> Optional[discord.Object]:
        """Resolve the guild where the context menu should be synced."""
        channel_id = await self.config.panel_channel_id()
        if channel_id:
            channel = self.bot.get_channel(int(channel_id))
            guild = getattr(channel, "guild", None) if channel else None
            if guild:
                return discord.Object(id=guild.id)

        guilds = getattr(self.bot, "guilds", [])
        if guilds:
            return discord.Object(id=guilds[0].id)

        return None

    async def _register_context_menu(self):
        """Register and sync the Discord user context menu for this guild."""
        tree = getattr(self.bot, "tree", None)
        add_command = getattr(tree, "add_command", None) if tree else None
        sync = getattr(tree, "sync", None) if tree else None
        if not add_command or not sync:
            log.warning("App command tree not available; context menu not registered")
            return

        guild = await self._get_context_menu_guild()
        if not guild:
            log.warning("No guild available; context menu not registered")
            return

        try:
            remove_command = getattr(tree, "remove_command", None)
            if remove_command:
                remove_command(
                    self._member_context_menu.name,
                    type=discord.AppCommandType.user,
                    guild=guild,
                )
            add_command(self._member_context_menu, guild=guild)
            await sync(guild=guild)
            self._context_menu_guild = guild
            log.info("MemberManager context menu synced for guild %s", guild.id)
        except Exception as exc:
            log.warning("Failed to register MemberManager context menu: %s", exc)

    def _unregister_context_menu(self):
        """Remove the Discord user context menu on unload."""
        tree = getattr(self.bot, "tree", None)
        remove_command = getattr(tree, "remove_command", None) if tree else None
        if not remove_command:
            return

        try:
            remove_command(
                self._member_context_menu.name,
                type=discord.AppCommandType.user,
                guild=self._context_menu_guild,
            )
        except Exception as exc:
            log.debug("Failed to unregister MemberManager context menu: %s", exc)
    
    async def _delayed_start(self):
        """Start automation after bot is ready."""
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)
        await self._register_context_menu()
        
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

        await self._ensure_panel_message()
    
    async def _connect_integrations(self):
        """Detect and connect to other cogs."""
        self.membersync = self.bot.get_cog("MemberSync")
        self.alliance_scraper = self.bot.get_cog("AllianceScraper")
        self.members_scraper = self.bot.get_cog("MembersScraper")
        self.logs_scraper = self.bot.get_cog("LogsScraper")
        self.sanction_manager = self.bot.get_cog("SanctionManager")
        
        integrations = []
        if self.membersync:
            integrations.append("MemberSync")
        if self.alliance_scraper:
            integrations.append("AllianceScraper")
        if self.members_scraper:
            integrations.append("MembersScraper")
        if self.logs_scraper:
            integrations.append("LogsScraper")
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
        """Check if member has moderator permissions."""
        if await self._is_admin(member):
            return True
        
        mod_roles = await self.config.moderator_role_ids()
        return any(role.id in mod_roles for role in member.roles)

    async def _interaction_is_moderator(self, interaction: discord.Interaction) -> bool:
        """Check whether an interaction user can access MemberManager."""
        user = getattr(interaction, "user", None)
        if not user:
            return False
        return await self._is_moderator(user)

    def _build_panel_embed(self) -> discord.Embed:
        """Build the persistent panel embed."""
        embed = discord.Embed(
            title="Member Management",
            description=(
                "Open a private staff panel to search Discord and MissionChief members, "
                "review linked data, and manage member information."
            ),
            color=discord.Color.blue(),
        )
        embed.set_footer(text="Only authorized staff can use this panel.")
        return embed

    async def _send_panel_message(self, channel: discord.TextChannel) -> discord.Message:
        """Post the MemberManager panel and store its message ID."""
        message = await channel.send(
            embed=self._build_panel_embed(),
            view=MemberPanelView(self),
        )
        await self.config.panel_message_id.set(message.id)
        return message

    async def _ensure_panel_message(self) -> None:
        """Post the default panel if the configured panel message is missing."""
        channel_id = await self.config.panel_channel_id()
        if not channel_id:
            return

        channel = self.bot.get_channel(int(channel_id))
        if not channel:
            log.warning("MemberManager panel channel not found: %s", channel_id)
            return

        message_id = await self.config.panel_message_id()
        if message_id:
            try:
                await channel.fetch_message(int(message_id))
                return
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                log.info("Stored MemberManager panel message is unavailable; reposting")

        try:
            await self._send_panel_message(channel)
            log.info("MemberManager panel posted in channel %s", channel_id)
        except Exception as exc:
            log.exception("Failed to post MemberManager panel: %s", exc)

    async def _send_member_profile(
        self,
        send,
        guild: discord.Guild,
        user_id: int,
        member_data: MemberData,
    ) -> None:
        """Send a private member overview through a context or interaction sender."""
        view = MemberOverviewView(
            bot=self.bot,
            db=self.db,
            config=self.config,
            member_data=member_data,
            integrations={
                "membersync": self.membersync,
                "alliance_scraper": self.alliance_scraper,
                "logs_scraper": self.logs_scraper,
                "sanction_manager": self.sanction_manager,
            },
            invoker_id=user_id,
            guild=guild,
        )
        embed = await view.get_overview_embed()
        await send(embed=embed, view=view)

    async def _open_member_profile_from_interaction(
        self,
        interaction: discord.Interaction,
        target: str,
    ) -> None:
        """Resolve and open a private member profile from an interaction."""
        if not await self._interaction_is_moderator(interaction):
            await interaction.response.send_message(
                "You do not have permission to use MemberManager.",
                ephemeral=True,
            )
            return

        member_data = await self._resolve_target(interaction.guild, target.strip())
        if not member_data:
            await interaction.response.send_message(
                f"No member found for `{target}`.",
                ephemeral=True,
            )
            return

        await self._send_member_profile(
            lambda **kwargs: interaction.response.send_message(
                **kwargs,
                ephemeral=True,
            ),
            interaction.guild,
            interaction.user.id,
            member_data,
        )

    async def _member_context_menu_callback(
        self,
        interaction: discord.Interaction,
        member: discord.Member,
    ) -> None:
        """Open MemberManager from Discord's right-click user context menu."""
        if not await self._interaction_is_moderator(interaction):
            await interaction.response.send_message(
                "You do not have permission to use MemberManager.",
                ephemeral=True,
            )
            return

        member_data = await self._build_member_data(
            guild=interaction.guild,
            discord_id=member.id,
        )
        await self._send_member_profile(
            lambda **kwargs: interaction.response.send_message(
                **kwargs,
                ephemeral=True,
            ),
            interaction.guild,
            interaction.user.id,
            member_data,
        )
    
    # ==================== LISTENERS ====================
    
    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Track when members join Discord."""
        if member.bot:
            return
        
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
        
        if self.membersync:
            link = await self.membersync.get_link_for_discord(member.id)
            if link:
                mc_id = link.get("mc_user_id")
                
                await self.db.add_event(
                    guild_id=member.guild.id,
                    discord_id=member.id,
                    mc_user_id=mc_id,
                    event_type="left_discord",
                    event_data={"username": str(member)},
                    triggered_by="system"
                )
    
    @commands.Cog.listener()
    async def on_modlog_case_create(self, case):
        """Auto-create infractions from Red's modlog events."""
        try:
            user_id = case.user.id if hasattr(case, 'user') else None
            if not user_id:
                return
            
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
                return
            
            mc_id = None
            if self.membersync:
                link = await self.membersync.get_link_for_discord(user_id)
                if link:
                    mc_id = link.get("mc_user_id")
            
            duration = None
            if hasattr(case, 'until') and case.until:
                duration = int((case.until - datetime.now(timezone.utc)).total_seconds())
            
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
            
            log.info(f"Auto-created infraction for {user_id} (type: {infraction_type})")
            
        except Exception as e:
            log.error(f"Failed to create infraction from modlog: {e}", exc_info=True)
    
    # ==================== MAIN COMMAND GROUP ====================
    
    @commands.hybrid_group(name="member", fallback="help")
    @commands.guild_only()
    async def member(self, ctx: commands.Context):
        """Member management commands."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @commands.group(name="memberpanel")
    @commands.guild_only()
    async def memberpanel(self, ctx: commands.Context):
        """Manage the persistent MemberManager panel."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @memberpanel.command(name="post")
    async def memberpanel_post(self, ctx: commands.Context):
        """Post the MemberManager panel if it is missing."""
        if not await self._is_moderator(ctx.author):
            await ctx.send("You need MemberManager permissions to use this command.")
            return

        channel_id = await self.config.panel_channel_id()
        channel = self.bot.get_channel(int(channel_id)) if channel_id else ctx.channel
        if not channel:
            await ctx.send("MemberManager panel channel not found.")
            return

        message_id = await self.config.panel_message_id()
        if message_id:
            try:
                await channel.fetch_message(int(message_id))
                await ctx.send("MemberManager panel already exists.")
                return
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

        await self._send_panel_message(channel)
        await ctx.send(f"MemberManager panel posted in {channel.mention}.")

    @memberpanel.command(name="repost")
    async def memberpanel_repost(self, ctx: commands.Context):
        """Force-post a new MemberManager panel."""
        if not await self._is_moderator(ctx.author):
            await ctx.send("You need MemberManager permissions to use this command.")
            return

        channel_id = await self.config.panel_channel_id()
        channel = self.bot.get_channel(int(channel_id)) if channel_id else ctx.channel
        if not channel:
            await ctx.send("MemberManager panel channel not found.")
            return

        await self.config.panel_message_id.set(None)
        await self._send_panel_message(channel)
        await ctx.send(f"MemberManager panel reposted in {channel.mention}.")
    
    # ==================== WHOIS COMMAND ====================
    
    @member.command(name="whois", aliases=["lookup", "info"])
    @commands.guild_only()
    async def whois(self, ctx: commands.Context, *, target: str):
        """
        Look up a member by Discord mention, MC ID, MC username, or Discord username.
        
        **Examples:**
        - `[p]member whois @JohnDoe`
        - `[p]member whois 123456`
        - `[p]member whois JohnDoe`
        """
        if not await self._is_moderator(ctx.author):
            await ctx.send("❌ You need moderator permissions to use this command.")
            return
        
        await ctx.typing()
        
        member_data = await self._resolve_target(ctx.guild, target)
        
        if not member_data:
            await ctx.send(
                f"❌ Could not find member matching `{target}`.\n"
                "Try using: @mention, MC ID, MC username, or Discord username."
            )
            return
        
        view = MemberOverviewView(
            bot=self.bot,
            db=self.db,
            config=self.config,
            member_data=member_data,
            integrations={
                "membersync": self.membersync,
                "alliance_scraper": self.alliance_scraper,
                "logs_scraper": self.logs_scraper,
                "sanction_manager": self.sanction_manager,
            },
            invoker_id=ctx.author.id,
            guild=ctx.guild
        )
        
        embed = await view.get_overview_embed()
        await ctx.send(embed=embed, view=view)
    
    # ==================== SEARCH COMMAND ====================
    
    @member.command(name="search", aliases=["find"])
    @commands.guild_only()
    async def search(self, ctx: commands.Context, *, query: str):
        """
        Search for members by name (MC or Discord).
        
        **Examples:**
        - `[p]member search John`
        - `[p]member search 1161`
        """
        if not await self._is_moderator(ctx.author):
            await ctx.send("❌ You need moderator permissions to use this command.")
            return
        
        await ctx.typing()
        
        from .utils import fuzzy_match_score
        
        results = []
        query_lower = query.lower().strip()
        
        # Search Discord members
        for member in ctx.guild.members:
            if member.bot:
                continue
            
            score = fuzzy_match_score(query_lower, str(member))
            if score >= 0.5:
                results.append({
                    "score": score,
                    "discord_id": member.id,
                    "mc_user_id": None,
                    "name": str(member),
                    "display_name": member.display_name,
                    "source": "discord"
                })
            
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
        
        # Search MC members
        if self.alliance_scraper:
            try:
                mc_members = await self.alliance_scraper.get_members()
                
                for mc_member in mc_members:
                    mc_name = mc_member.get("name", "")
                    mc_id = mc_member.get("user_id") or mc_member.get("mc_user_id")
                    
                    if not mc_id:
                        continue
                    
                    score = fuzzy_match_score(query_lower, mc_name)
                    
                    if query_lower.isdigit() and query_lower in str(mc_id):
                        score = max(score, 0.8)
                    
                    if score >= 0.5:
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
        
        # Remove duplicates
        seen = set()
        unique_results = []
        for result in sorted(results, key=lambda x: x["score"], reverse=True):
            key = (result.get("discord_id"), result.get("mc_user_id"))
            if key not in seen:
                seen.add(key)
                unique_results.append(result)
        
        unique_results = unique_results[:15]
        
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
            
            source_emoji = "🎮" if source == "discord" else "🚒"
            line = f"{i}. {source_emoji} **{name}**"
            
            if display_name and display_name != name:
                line += f" *({display_name})*"
            
            if discord_id:
                line += f" • Discord: <@{discord_id}>"
            if mc_id:
                line += f" • MC: `{mc_id}`"
            
            line += f" • Match: {score:.0%}"
            
            lines.append(line)
        
        embed.description = "\n".join(lines)
        embed.set_footer(text="Use [p]member whois <name/id> to view full details")
        
        await ctx.send(embed=embed)

    @member.command(name="mcsearch", aliases=["mcs"])
    @commands.guild_only()
    async def mcsearch(self, ctx: commands.Context, *, query: str):
        """Search MissionChief members by name or ID."""
        if not await self._is_moderator(ctx.author):
            await ctx.send("You need MemberManager permissions to use this command.")
            return

        await ctx.typing()
        results = await self._search_missionchief_members(query)
        if not results:
            await ctx.send(f"No MissionChief members found for `{query}`.")
            return

        embed = discord.Embed(
            title=f"MissionChief Search: {query}",
            description=f"Found {len(results)} result(s)",
            color=discord.Color.blue(),
        )

        lines = []
        for result in results[:10]:
            mc_id = result.get("mc_user_id")
            name = result.get("name", "Unknown")
            link = None
            if self.membersync and mc_id:
                link = await self.membersync.get_link_for_mc(str(mc_id))

            line = f"- **{name}** | MC ID: `{mc_id}`"
            if link and link.get("discord_id"):
                line += f" | Discord: <@{link['discord_id']}>"
            elif link:
                line += f" | Link: {link.get('status', 'unknown')}"
            else:
                line += " | Link: none"
            lines.append(line)

        embed.add_field(
            name="Results",
            value="\n".join(lines),
            inline=False,
        )
        embed.set_footer(text="Use member whois <MC ID> to open the full profile.")
        await ctx.send(embed=embed)
    
    # ==================== CONTRIBUTION CHECK COMMAND ====================
    
    @member.command(name="checkcontributions", aliases=["checkcontrib", "contribcheck"])
    @commands.guild_only()
    async def check_contributions(
        self,
        ctx: commands.Context,
        target: Optional[str] = None,
        *,
        flags: str = ""
    ):
        """
        🔍 Debug tool: Check who qualifies for low contribution alerts.
        
        **DOES NOT SEND ALERTS** - Dry-run mode for testing.
        
        **Usage:**
        - `[p]member checkcontrib` - Show summary
        - `[p]member checkcontrib --verbose` - Show all members
        - `[p]member checkcontrib @user` - Check specific member
        - `[p]member checkcontrib 123456` - Check by MC ID
        - `[p]member checkcontrib @user --force-alert` - Send test alert
        """
        if not await self._is_admin(ctx.author):
            await ctx.send("❌ You need admin permissions to use this command.")
            return
        
        verbose = "--verbose" in flags or "-v" in flags
        force_alert = "--force-alert" in flags or "--force" in flags
        
        await ctx.typing()
        
        if not self.members_scraper:
            await ctx.send(
                "❌ **MembersScraper not available**\n"
                "Make sure MembersScraper is loaded: `[p]load membersscraper`"
            )
            return
        
        logs_scraper = self.logs_scraper
        threshold = await self.config.contribution_threshold()
        
        if target and not target.startswith("--"):
            await self._check_single_member(
                ctx, 
                target, 
                threshold, 
                logs_scraper,
                force_alert
            )
            return
        
        await self._check_all_members(
            ctx,
            threshold,
            logs_scraper,
            verbose,
            force_alert
        )
    
    async def _check_single_member(
        self,
        ctx: commands.Context,
        target: str,
        threshold: float,
        logs_scraper,
        force_alert: bool
    ):
        """Check contribution status for a single member."""
        member_data = await self._resolve_target(ctx.guild, target)
        
        if not member_data or not member_data.mc_user_id:
            await ctx.send(
                f"❌ Could not find member with MC account: `{target}`\n"
                "Make sure they have a linked MC account."
            )
            return
        
        mc_id = member_data.mc_user_id
        mc_name = member_data.mc_username or "Unknown"
        
        mc_data = await self._get_mc_data(mc_id)
        
        if not mc_data:
            await ctx.send(
                f"❌ Member `{mc_name}` ({mc_id}) not found in alliance.\n"
                "They may have left the alliance."
            )
            return
        
        current_rate = mc_data.get("contribution_rate", 0.0)
        
        embed = discord.Embed(
            title=f"🔍 Contribution Check: {mc_name}",
            color=discord.Color.blue()
        )
        
        # Basic info
        info_lines = [
            f"**MC ID:** `{mc_id}`",
            f"**Current Rate:** {current_rate:.1f}%",
            f"**Threshold:** {threshold}%",
            f"**Status:** {'🔴 Below' if current_rate < threshold else '🟢 Above'} threshold"
        ]
        
        if member_data.discord_id:
            info_lines.insert(0, f"**Discord:** <@{member_data.discord_id}>")
        
        embed.add_field(
            name="📊 Basic Info",
            value="\n".join(info_lines),
            inline=False
        )
        
        # Check 1: Below threshold?
        checks = []
        check1_pass = current_rate < threshold
        checks.append(f"{'✅' if check1_pass else '❌'} **Below threshold** ({current_rate:.1f}% < {threshold}%)")
        
        # Check 2: Grace period (🔧 FIXED: Timezone-aware datetimes)
        join_date, grace_source = await self._get_join_date_for_member(mc_id, mc_name, logs_scraper)
        check2_pass = True
        
        if join_date:
            days_in_alliance = (datetime.now(timezone.utc) - join_date).days
            check2_pass = days_in_alliance >= 7
            
            checks.append(
                f"{'✅' if check2_pass else '❌'} **Grace period** "
                f"({days_in_alliance} days in alliance, need 7+)\n"
                f"  *Source: {grace_source}*"
            )
        else:
            checks.append("⚠️ **Grace period** (no data found, FAILING check)")
            check2_pass = False
        
        # Check 3: Cooldown
        last_alert_time = self.contribution_monitor._last_alerts.get(mc_id, 0) if self.contribution_monitor else 0
        now = int(datetime.now(timezone.utc).timestamp())
        days_since_alert = (now - last_alert_time) / 86400
        check3_pass = (now - last_alert_time) >= (7 * 86400)
        
        if last_alert_time == 0:
            checks.append("✅ **Cooldown** (no previous alerts)")
        else:
            checks.append(
                f"{'✅' if check3_pass else '❌'} **Cooldown** "
                f"({days_since_alert:.1f} days since last alert, need 7+)"
            )
        
        # Check 4: Historical consistency (🔧 FIXED: Works perfectly)
        historical_rates = await self._get_historical_rates_for_member(mc_id)
        check4_pass = False
        
        if len(historical_rates) >= 4:
            recent_4 = historical_rates[:4]
            check4_pass = all(rate < threshold for rate in recent_4)
            checks.append(
                f"{'✅' if check4_pass else '❌'} **Consistency** "
                f"(last 4 checks: {', '.join(f'{r:.1f}%' for r in recent_4)})"
            )
        else:
            checks.append(
                f"❌ **Consistency** "
                f"(only {len(historical_rates)} checks, need 4+)"
            )
        
        embed.add_field(
            name="🔍 Qualification Checks",
            value="\n".join(checks),
            inline=False
        )
        
        # Final verdict
        all_pass = check1_pass and check2_pass and check3_pass and check4_pass
        
        if all_pass:
            verdict = "✅ **QUALIFIES FOR ALERT**"
            embed.color = discord.Color.red()
        else:
            verdict = "❌ **DOES NOT QUALIFY**"
            embed.color = discord.Color.green()
        
        embed.add_field(
            name="📋 Result",
            value=verdict,
            inline=False
        )
        
        # Historical trend
        if historical_rates:
            trend_str = " → ".join(f"{r:.1f}%" for r in historical_rates[:8])
            embed.add_field(
                name=f"📈 Historical Trend (last {len(historical_rates[:8])} checks)",
                value=trend_str,
                inline=False
            )
        
        # Debug info
        debug_info = [
            f"**MembersScraper DB:** `{self.members_scraper.db_path}`",
            f"**Historical checks found:** {len(historical_rates)}"
        ]
        
        if logs_scraper:
            debug_info.append(f"**LogsScraper DB:** `{logs_scraper.db_path}`")
        else:
            debug_info.append("**LogsScraper:** ❌ Not available")
        
        embed.add_field(
            name="🔧 Debug Info",
            value="\n".join(debug_info),
            inline=False
        )
        
        if force_alert and not all_pass:
            embed.set_footer(text="⚠️ Cannot send alert - member does not qualify")
        elif force_alert and all_pass:
            embed.set_footer(text="⚠️ Ready to send test alert")
        
        await ctx.send(embed=embed)
        
        # Send test alert if requested
        if force_alert and all_pass:
            confirm_msg = await ctx.send(
                f"⚠️ **Confirm Test Alert**\n"
                f"This will send a REAL alert for {mc_name} to the admin channel.\n"
                f"React with ✅ to confirm or ❌ to cancel."
            )
            
            await confirm_msg.add_reaction("✅")
            await confirm_msg.add_reaction("❌")
            
            def check(reaction, user):
                return (
                    user == ctx.author 
                    and str(reaction.emoji) in ["✅", "❌"]
                    and reaction.message.id == confirm_msg.id
                )
            
            try:
                reaction, user = await self.bot.wait_for("reaction_add", timeout=30.0, check=check)
                
                if str(reaction.emoji) == "✅":
                    if self.contribution_monitor:
                        success = await self.contribution_monitor._send_contribution_alert(
                            mc_id=mc_id,
                            mc_member=mc_data,
                            current_rate=current_rate,
                            historical_rates=historical_rates
                        )
                        
                        if success:
                            await ctx.send("✅ Test alert sent successfully!")
                        else:
                            await ctx.send("❌ Failed to send test alert. Check logs for details.")
                    else:
                        await ctx.send("❌ Contribution monitor not initialized.")
                else:
                    await ctx.send("❌ Test alert cancelled.")
            
            except asyncio.TimeoutError:
                await ctx.send("❌ Test alert cancelled (timeout).")
    
    async def _check_all_members(
        self,
        ctx: commands.Context,
        threshold: float,
        logs_scraper,
        verbose: bool,
        force_alert: bool
    ):
        """Check contribution status for all members."""
        try:
            mc_members = await self.alliance_scraper.get_members()
        except Exception as e:
            await ctx.send(f"❌ Failed to get alliance members: {str(e)}")
            return
        
        if not mc_members:
            await ctx.send("❌ No alliance members found.")
            return
        
        would_alert = []
        skipped_grace = []
        skipped_cooldown = []
        skipped_insufficient = []
        skipped_inconsistent = []
        above_threshold = []
        
        status_msg = await ctx.send(f"🔍 Checking {len(mc_members)} alliance members...")
        
        for i, mc_member in enumerate(mc_members):
            mc_id = mc_member.get("user_id") or mc_member.get("mc_user_id")
            if not mc_id:
                continue
            
            mc_name = mc_member.get("name", "Unknown")
            current_rate = mc_member.get("contribution_rate", 0.0)
            
            if i % 20 == 0:
                try:
                    await status_msg.edit(
                        content=f"🔍 Checking members... ({i}/{len(mc_members)})"
                    )
                except:
                    pass
            
            # Check 1: Below threshold?
            if current_rate >= threshold:
                above_threshold.append({
                    "mc_id": mc_id,
                    "mc_name": mc_name,
                    "rate": current_rate
                })
                continue
            
            # Check 2: Grace period
            join_date, _ = await self._get_join_date_for_member(mc_id, mc_name, logs_scraper)
            if join_date:
                days_in_alliance = (datetime.now(timezone.utc) - join_date).days
                if days_in_alliance < 7:
                    skipped_grace.append({
                        "mc_id": mc_id,
                        "mc_name": mc_name,
                        "rate": current_rate,
                        "days": days_in_alliance
                    })
                    continue
            else:
                skipped_grace.append({
                    "mc_id": mc_id,
                    "mc_name": mc_name,
                    "rate": current_rate,
                    "days": 0,
                    "no_data": True
                })
                continue
            
            # Check 3: Cooldown
            last_alert_time = self.contribution_monitor._last_alerts.get(mc_id, 0) if self.contribution_monitor else 0
            now = int(datetime.now(timezone.utc).timestamp())
            
            if now - last_alert_time < (7 * 86400) and last_alert_time > 0:
                days_since = (now - last_alert_time) / 86400
                skipped_cooldown.append({
                    "mc_id": mc_id,
                    "mc_name": mc_name,
                    "rate": current_rate,
                    "days_since": days_since
                })
                continue
            
            # Check 4: Historical consistency
            historical_rates = await self._get_historical_rates_for_member(mc_id)
            
            if len(historical_rates) < 4:
                skipped_insufficient.append({
                    "mc_id": mc_id,
                    "mc_name": mc_name,
                    "rate": current_rate,
                    "checks": len(historical_rates)
                })
                continue
            
            recent_4 = historical_rates[:4]
            if not all(rate < threshold for rate in recent_4):
                skipped_inconsistent.append({
                    "mc_id": mc_id,
                    "mc_name": mc_name,
                    "rate": current_rate,
                    "history": recent_4
                })
                continue
            
            # ALL CHECKS PASSED
            would_alert.append({
                "mc_id": mc_id,
                "mc_name": mc_name,
                "rate": current_rate,
                "history": historical_rates[:4],
                "days_in_alliance": (datetime.now(timezone.utc) - join_date).days if join_date else None
            })
        
        try:
            await status_msg.delete()
        except:
            pass
        
        # Build summary embed
        embed = discord.Embed(
            title="🔍 Contribution Check Results (Dry Run)",
            description=f"**Threshold:** {threshold}% | **Total members:** {len(mc_members)}",
            color=discord.Color.blue()
        )
        
        stats = [
            f"✅ **Would trigger alert:** {len(would_alert)}",
            f"❌ **Skipped (total):** {len(skipped_grace) + len(skipped_cooldown) + len(skipped_insufficient) + len(skipped_inconsistent)}",
            f"   └─ Grace period: {len(skipped_grace)}",
            f"   └─ Cooldown: {len(skipped_cooldown)}",
            f"   └─ Insufficient data: {len(skipped_insufficient)}",
            f"   └─ Inconsistent: {len(skipped_inconsistent)}",
            f"🟢 **Above threshold:** {len(above_threshold)}"
        ]
        
        embed.add_field(
            name="📊 Summary",
            value="\n".join(stats),
            inline=False
        )
        
        if would_alert:
            alert_lines = []
            for member in would_alert[:10]:
                history_str = " → ".join(f"{r:.1f}%" for r in member["history"])
                alert_lines.append(
                    f"• **{member['mc_name']}** (`{member['mc_id']}`)\n"
                    f"  Current: {member['rate']:.1f}% | History: {history_str}"
                )
            
            if len(would_alert) > 10:
                alert_lines.append(f"*...and {len(would_alert) - 10} more*")
            
            embed.add_field(
                name="🚨 Would Trigger Alert",
                value="\n".join(alert_lines) if alert_lines else "*None*",
                inline=False
            )
        
        if verbose:
            if skipped_grace:
                grace_lines = []
                for member in skipped_grace[:5]:
                    if member.get("no_data"):
                        grace_lines.append(
                            f"• {member['mc_name']} (`{member['mc_id']}`) - "
                            f"{member['rate']:.1f}% | ⚠️ No join date found"
                        )
                    else:
                        grace_lines.append(
                            f"• {member['mc_name']} (`{member['mc_id']}`) - "
                            f"{member['rate']:.1f}% | {member['days']} days"
                        )
                if len(skipped_grace) > 5:
                    grace_lines.append(f"*...and {len(skipped_grace) - 5} more*")
                
                embed.add_field(
                    name="⏳ Skipped: Grace Period",
                    value="\n".join(grace_lines),
                    inline=False
                )
            
            if skipped_cooldown:
                cooldown_lines = []
                for member in skipped_cooldown[:5]:
                    cooldown_lines.append(
                        f"• {member['mc_name']} (`{member['mc_id']}`) - "
                        f"{member['rate']:.1f}% | Alert {member['days_since']:.1f} days ago"
                    )
                if len(skipped_cooldown) > 5:
                    cooldown_lines.append(f"*...and {len(skipped_cooldown) - 5} more*")
                
                embed.add_field(
                    name="🔕 Skipped: Cooldown",
                    value="\n".join(cooldown_lines),
                    inline=False
                )
            
            if skipped_insufficient:
                insuf_lines = []
                for member in skipped_insufficient[:5]:
                    insuf_lines.append(
                        f"• {member['mc_name']} (`{member['mc_id']}`) - "
                        f"{member['rate']:.1f}% | Only {member['checks']} checks"
                    )
                if len(skipped_insufficient) > 5:
                    insuf_lines.append(f"*...and {len(skipped_insufficient) - 5} more*")
                
                embed.add_field(
                    name="📊 Skipped: Insufficient Data",
                    value="\n".join(insuf_lines),
                    inline=False
                )
            
            if skipped_inconsistent:
                incon_lines = []
                for member in skipped_inconsistent[:5]:
                    history_str = ", ".join(f"{r:.1f}%" for r in member["history"])
                    incon_lines.append(
                        f"• {member['mc_name']} (`{member['mc_id']}`) - "
                        f"History: {history_str}"
                    )
                if len(skipped_inconsistent) > 5:
                    incon_lines.append(f"*...and {len(skipped_inconsistent) - 5} more*")
                
                embed.add_field(
                    name="📈 Skipped: Inconsistent",
                    value="\n".join(incon_lines),
                    inline=False
                )
        
        embed.set_footer(
            text=(
                "This is a DRY RUN - no alerts were sent. "
                "Use --verbose for detailed breakdown."
            )
        )
        
        await ctx.send(embed=embed)
    
    # ==================== SYNC DATABASE FUNCTIONS ====================
    
    def _query_join_date_sync(self, db_path: Path, mc_id: str, mc_name: Optional[str]) -> Optional[str]:
        """Sync query for join date from LogsScraper."""
        import sqlite3
        
        try:
            if not db_path.exists():
                return None
            
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            cursor.execute(
                """
                SELECT MIN(ts) as join_date 
                FROM logs 
                WHERE (affected_mc_id = ? OR affected_name = ?)
                AND action_key = 'added_to_alliance'
                """,
                (mc_id, mc_name)
            )
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0]:
                return result[0]
        
        except Exception as e:
            log.error(f"Query join date error: {e}")
        
        return None
    
    def _query_first_scrape_sync(self, db_path: Path, mc_id: str) -> Optional[str]:
        """Sync query for first scrape date from MembersScraper."""
        import sqlite3
        
        try:
            if not db_path.exists():
                return None
            
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT MIN(timestamp) as first_seen
                FROM members
                WHERE member_id = ?
            """, (mc_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result and result["first_seen"]:
                return result["first_seen"]
        
        except Exception as e:
            log.error(f"Query first scrape error: {e}")
        
        return None
    
    def _query_historical_rates_sync(self, db_path: Path, mc_id: str) -> List[float]:
        """Sync query for historical contribution rates."""
        import sqlite3
        
        try:
            if not db_path.exists():
                log.error(f"Database not found: {db_path}")
                return []
            
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT contribution_rate, timestamp
                FROM members
                WHERE member_id = ?
                ORDER BY timestamp DESC
                LIMIT 12
            """, (mc_id,))
            
            rows = cursor.fetchall()
            conn.close()
            
            rates = [row["contribution_rate"] for row in rows if row["contribution_rate"] is not None]
            
            log.debug(f"Found {len(rates)} historical rates for {mc_id}")
            return rates
        
        except Exception as e:
            log.error(f"Query historical rates error: {e}", exc_info=True)
            return []
    
    def _query_mc_data_sync(self, db_path: Path, mc_user_id: str) -> Optional[Dict[str, Any]]:
        """Sync query for MC member data."""
        import sqlite3
        
        try:
            if not db_path.exists():
                return None
            
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("PRAGMA table_info(members)")
            columns = {row["name"] for row in cursor.fetchall()}
            source_select = "snapshot_source" if "snapshot_source" in columns else "'unknown' AS snapshot_source"
            
            cursor.execute(f"""
                SELECT member_id, username, rank, earned_credits, contribution_rate,
                       timestamp, {source_select}
                FROM members
                WHERE member_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (mc_user_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    "user_id": row["member_id"],
                    "name": row["username"],
                    "role": row["rank"],
                    "earned_credits": row["earned_credits"],
                    "contribution_rate": row["contribution_rate"],
                    "snapshot_at": row["timestamp"],
                    "snapshot_source": row["snapshot_source"],
                }
        
        except Exception as e:
            log.error(f"Query MC data error: {e}")
        
        return None
    
    # ==================== ASYNC WRAPPERS ====================
    
    async def _get_join_date_for_member(
        self,
        mc_id: str,
        mc_name: Optional[str],
        logs_scraper
    ) -> tuple[Optional[datetime], str]:
        """
        Get when member joined the alliance.
        
        🔧 FIXED: Timezone-aware datetime handling
        """
        # Try LogsScraper first
        if logs_scraper:
            try:
                db_path = logs_scraper.db_path
                
                join_date_str = await asyncio.to_thread(
                    self._query_join_date_sync,
                    db_path,
                    mc_id,
                    mc_name
                )
                
                if join_date_str:
                    if join_date_str.endswith('Z'):
                        join_date_str = join_date_str.replace('Z', '+00:00')
                    
                    # 🔧 FIX: Make timezone-aware
                    join_date = datetime.fromisoformat(join_date_str)
                    if join_date.tzinfo is None:
                        join_date = join_date.replace(tzinfo=timezone.utc)
                    
                    log.debug(f"Found join date for {mc_name} ({mc_id}): {join_date}")
                    return join_date, "LogsScraper"
            
            except Exception as e:
                log.error(f"Failed to get join date from LogsScraper: {e}")
        
        # Fallback: First scrape date
        if self.members_scraper:
            try:
                db_path = self.members_scraper.db_path
                
                first_seen_str = await asyncio.to_thread(
                    self._query_first_scrape_sync,
                    Path(db_path),
                    mc_id
                )
                
                if first_seen_str:
                    if first_seen_str.endswith('Z'):
                        first_seen_str = first_seen_str.replace('Z', '+00:00')
                    
                    # 🔧 FIX: Make timezone-aware
                    first_seen = datetime.fromisoformat(first_seen_str)
                    if first_seen.tzinfo is None:
                        first_seen = first_seen.replace(tzinfo=timezone.utc)
                    
                    log.debug(f"Using first scrape for {mc_name} ({mc_id}): {first_seen}")
                    return first_seen, "first scrape (fallback)"
            
            except Exception as e:
                log.error(f"Failed to get first scrape date: {e}")
        
        return None, "unknown"
    
    async def _get_historical_rates_for_member(self, mc_id: str) -> List[float]:
        """
        Get historical contribution rates for a member.
        
        🔧 FIXED: Uses asyncio.to_thread() for blocking sqlite3 calls
        """
        if not self.members_scraper:
            log.warning("MembersScraper not available")
            return []
        
        try:
            db_path = Path(self.members_scraper.db_path)
            
            rates = await asyncio.to_thread(
                self._query_historical_rates_sync,
                db_path,
                mc_id
            )
            
            return rates
        
        except Exception as e:
            log.error(f"Failed to get historical rates for {mc_id}: {e}", exc_info=True)
            return []
    
    async def _get_mc_data(self, mc_user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get MC member data from MembersScraper.
        
        🔧 FIXED: Uses asyncio.to_thread() for blocking sqlite3 calls
        """
        if not self.members_scraper:
            log.warning("MembersScraper not available")
            return None
        
        try:
            db_path = Path(self.members_scraper.db_path)
            
            result = await asyncio.to_thread(
                self._query_mc_data_sync,
                db_path,
                mc_user_id
            )
            
            return result
                
        except Exception as e:
            log.error(f"Failed to get MC data for {mc_user_id}: {e}", exc_info=True)
            return None

    async def _populate_contribution_data(self, data: MemberData) -> None:
        """Populate contribution status fields from existing scraper reads."""
        if not data.mc_user_id:
            data.contribution_data_status = "missing"
            return

        if not self.members_scraper:
            data.contribution_data_status = "unavailable"
            return

        if data.contribution_rate is None:
            data.contribution_data_status = "missing"
            return

        data.contribution_data_status = "available"
        data.contribution_history = await self._get_historical_rates_for_member(data.mc_user_id)

        if len(data.contribution_history) >= 2:
            current = data.contribution_history[0]
            previous = data.contribution_history[1]
            if abs(current - previous) < 0.5:
                data.contribution_trend = "stable"
            elif current > previous:
                data.contribution_trend = "up"
            else:
                data.contribution_trend = "down"

        join_date, join_source = await self._get_join_date_for_member(
            data.mc_user_id,
            data.mc_username,
            self.logs_scraper,
        )
        data.contribution_join_source = join_source
        if join_date:
            data.mc_joined = join_date
            days_in_alliance = (datetime.now(timezone.utc) - join_date).days
            data.contribution_grace_status = (
                f"{days_in_alliance} days in alliance"
                if days_in_alliance >= 7
                else f"grace period ({days_in_alliance}/7 days)"
            )
        else:
            data.contribution_grace_status = "join date unavailable"
    
    # ==================== DEBUG COMMAND ====================
    
    @member.command(name="debug", aliases=["status"])
    @commands.guild_only()
    async def debug(self, ctx: commands.Context):
        """Show MemberManager integration status."""
        if not await self._is_moderator(ctx.author):
            await ctx.send("❌ You need moderator permissions to use this command.")
            return
        
        embed = discord.Embed(
            title="🔧 MemberManager Debug Info",
            color=discord.Color.blue()
        )
        
        integrations = []
        integrations.append(f"**MemberSync:** {'✅ Connected' if self.membersync else '❌ Not found'}")
        integrations.append(f"**AllianceScraper:** {'✅ Connected' if self.alliance_scraper else '❌ Not found'}")
        integrations.append(f"**MembersScraper:** {'✅ Connected' if self.members_scraper else '❌ Not found'}")
        integrations.append(f"**LogsScraper:** {'✅ Connected' if self.logs_scraper else '❌ Not found'}")
        integrations.append(f"**SanctionManager:** {'✅ Connected' if self.sanction_manager else '❌ Not found'}")
        
        embed.add_field(
            name="🔌 Integrations",
            value="\n".join(integrations),
            inline=False
        )
        
        db_info = []
        if self.db:
            db_info.append(f"✅ Database connected")
            db_info.append(f"Path: `{self.db_path}`")
        else:
            db_info.append("❌ Database not connected")
        
        if self.members_scraper:
            db_info.append(f"MembersScraper DB: `{self.members_scraper.db_path}`")
        
        embed.add_field(
            name="💾 Database",
            value="\n".join(db_info),
            inline=False
        )
        
        monitor_info = []
        if self.contribution_monitor:
            monitor_info.append("✅ Contribution monitor active")
            monitor_info.append(f"Threshold: {await self.config.contribution_threshold()}%")
            monitor_info.append(f"Tracked alerts: {len(self.contribution_monitor._last_alerts)}")
        else:
            monitor_info.append("❌ Contribution monitor not active")
        
        embed.add_field(
            name="🔍 Monitoring",
            value="\n".join(monitor_info),
            inline=False
        )
        
        all_cogs = [c.qualified_name for c in self.bot.cogs.values()]
        cog_list = ", ".join(sorted(all_cogs))
        
        embed.add_field(
            name="📦 All Loaded Cogs",
            value=f"```{cog_list}```",
            inline=False
        )
        
        embed.set_footer(text=f"MemberManager v{__version__}")
        
        await ctx.send(embed=embed)

    async def _search_missionchief_members(self, query: str) -> List[Dict[str, Any]]:
        """Search MissionChief members by name or MC user ID."""
        from .utils import fuzzy_match_score

        query_clean = query.strip()
        query_lower = query_clean.lower()
        results: List[Dict[str, Any]] = []

        if query_clean.isdigit():
            mc_data = await self._get_mc_data(query_clean)
            if mc_data:
                return [{
                    "score": 1.0,
                    "mc_user_id": mc_data.get("user_id") or mc_data.get("mc_user_id") or query_clean,
                    "name": mc_data.get("name", "Unknown"),
                }]

        if not self.alliance_scraper:
            return []

        try:
            mc_members = await self.alliance_scraper.get_members()
        except Exception as exc:
            log.error("Error searching MissionChief members: %s", exc)
            return []

        for mc_member in mc_members:
            mc_name = mc_member.get("name", "")
            mc_id = mc_member.get("user_id") or mc_member.get("mc_user_id")
            if not mc_id:
                continue

            score = fuzzy_match_score(query_lower, mc_name)
            if query_clean.isdigit() and query_clean in str(mc_id):
                score = max(score, 0.9)

            if score >= 0.5:
                results.append({
                    "score": score,
                    "mc_user_id": str(mc_id),
                    "name": mc_name,
                })

        results.sort(key=lambda result: result["score"], reverse=True)
        return results[:15]
    
    async def _resolve_target(
        self,
        guild: discord.Guild,
        target: str
    ) -> Optional[MemberData]:
        """Resolve a target string to a MemberData object."""
        # Try Discord mention/ID
        discord_member = None
        try:
            if target.startswith("<@") and target.endswith(">"):
                user_id = int(target.strip("<@!>"))
                discord_member = guild.get_member(user_id)
            else:
                user_id = int(target)
                discord_member = guild.get_member(user_id)
        except ValueError:
            pass
        
        if discord_member:
            return await self._build_member_data(
                guild=guild,
                discord_id=discord_member.id
            )
        
        # Try MC ID
        if target.isdigit():
            mc_data = await self._get_mc_data(target)
            
            if mc_data:
                discord_id = None
                if self.membersync:
                    link = await self.membersync.get_link_for_mc(target)
                    if link:
                        discord_id = link.get("discord_id")
                
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
        """Build a complete MemberData object from available sources."""
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
                
                if self.membersync:
                    verified_role_id = await self.membersync.config.verified_role_id()
                    if verified_role_id:
                        verified_role = guild.get_role(verified_role_id)
                        data.verified_role_present = bool(verified_role and verified_role in member.roles)
        
        # Get MC data and link status
        if self.membersync:
            link = None
            get_status = getattr(self.membersync, "get_verification_status", None)
            if get_status:
                link = await get_status(discord_id=discord_id, mc_user_id=mc_user_id)
            
            if not link:
                if discord_id and not mc_user_id:
                    link = await self.membersync.get_link_for_discord(discord_id)
                elif mc_user_id and not discord_id:
                    link = await self.membersync.get_link_for_mc(mc_user_id)
                elif discord_id and mc_user_id:
                    link = await self.membersync.get_link_for_discord(discord_id)

            if link:
                if link.get("mc_user_id") and not data.mc_user_id:
                    data.mc_user_id = str(link.get("mc_user_id"))
                if link.get("discord_id") and not data.discord_id:
                    data.discord_id = int(link.get("discord_id"))

                data.link_status = link.get("status", "none")
                data.link_updated = link.get("updated_at")
                reviewer_id = link.get("reviewer_id")
                if reviewer_id is not None:
                    data.link_reviewer_id = int(reviewer_id)
            
            if data.link_status == "approved":
                data.is_verified = True
                
                if discord_id:
                    member = guild.get_member(discord_id)
                    if member:
                        verified_role_id = await self.membersync.config.verified_role_id()
                        if verified_role_id:
                            verified_role = guild.get_role(verified_role_id)
                            if verified_role and verified_role not in member.roles:
                                data.member_sync_conflict = "Approved link but missing verified role"
                                log.warning(f"Member {discord_id} is linked but missing verified role")
            else:
                data.is_verified = False
                data.link_status = link.get("status", "none") if link else "none"

            if data.verified_role_present and data.link_status != "approved":
                data.member_sync_conflict = "Verified role present without approved MemberSync link"

        if data.discord_id and not data.discord_username:
            member = guild.get_member(data.discord_id)
            if member:
                data.discord_username = str(member)
                data.discord_roles = [r.name for r in member.roles if r.name != "@everyone"]
                data.discord_joined = member.joined_at
        
        # Get MC data from MembersScraper
        mc_in_alliance = False
        if data.mc_user_id and self.members_scraper:
            try:
                mc_data = await self._get_mc_data(data.mc_user_id)
                if mc_data:
                    data.mc_username = mc_data.get("name")
                    data.mc_role = mc_data.get("role")
                    data.contribution_rate = mc_data.get("contribution_rate")
                    data.contribution_snapshot_at = mc_data.get("snapshot_at")
                    data.contribution_snapshot_source = mc_data.get("snapshot_source")
                    mc_in_alliance = True
            except Exception as e:
                log.error(f"Failed to get MC data for {data.mc_user_id}: {e}")
        
        if data.mc_user_id and not mc_in_alliance:
            data.mc_username = f"Former member ({data.mc_user_id})"
            data.mc_role = "Left alliance"

        await self._populate_contribution_data(data)
        
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
        
        # Get sanctions from SanctionManager
        if self.sanction_manager:
            try:
                sanctions = self.sanction_manager.db.get_user_sanctions(
                    guild_id=guild.id,
                    discord_user_id=data.discord_id,
                    mc_user_id=data.mc_user_id
                )
                
                now = int(datetime.now(timezone.utc).timestamp())
                thirty_days_ago = now - (30 * 86400)
                
                active_sanctions = []
                for sanction in sanctions:
                    status = sanction.get("status", "active")
                    is_warning = "Warning" in sanction.get("sanction_type", "")
                    created_at = sanction.get("created_at", 0)
                    
                    if status == "active":
                        if not is_warning or created_at >= thirty_days_ago:
                            active_sanctions.append(sanction)
                
                data.infractions_count = len(active_sanctions)
                
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


async def setup(bot: Red):
    """Load MemberManager cog."""
    await bot.add_cog(MemberManager(bot))
