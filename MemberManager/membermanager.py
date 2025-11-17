"""
MemberManager - Comprehensive member tracking and management
Fire & Rescue Academy Alliance

Integrates with:
- MemberSync: Discord ↔ MC linking
- AllianceScraper: MC member data, contributions, logs
- Red's modlog: Discord infractions

VERSION: 2.2.1
FIXED: Grace period fallback to first scrape date
FIXED: Historical rates now correctly query members_v2.db
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

__version__ = "2.2.1"

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
        self.logs_scraper: Optional[commands.Cog] = None
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
            await ctx.send("❌ You need moderator permissions to use this command.")
            return
        
        await ctx.typing()
        
        # Try to find the member
        member_data = await self._resolve_target(ctx.guild, target)
        
        if not member_data:
            await ctx.send(
                f"❌ Could not find member matching `{target}`.\n"
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
                "logs_scraper": self.logs_scraper,
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
    
    # ==================== 🆕 CONTRIBUTION CHECK COMMAND ====================
    
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
        
        **DOES NOT SEND ALERTS** - This is a dry-run to test the monitoring system.
        
        **Usage:**
        - `[p]member checkcontrib` - Show summary of who qualifies
        - `[p]member checkcontrib --verbose` - Show ALL members with details
        - `[p]member checkcontrib @user` - Check specific member
        - `[p]member checkcontrib 123456` - Check by MC ID
        - `[p]member checkcontrib @user --force-alert` - Send real test alert
        
        **Examples:**
        ```
        [p]member checkcontrib
        [p]member checkcontrib --verbose
        [p]member checkcontrib @JohnDoe
        [p]member checkcontrib 384563 --force-alert
        ```
        """
        # Check permissions
        if not await self._is_admin(ctx.author):
            await ctx.send("❌ You need admin permissions to use this command.")
            return
        
        # Parse flags
        verbose = "--verbose" in flags or "-v" in flags
        force_alert = "--force-alert" in flags or "--force" in flags
        
        await ctx.typing()
        
        # Check if MembersScraper is available
        if not self.members_scraper:
            await ctx.send(
                "❌ **MembersScraper not available**\n"
                "Make sure MembersScraper is loaded: `[p]load membersscraper`"
            )
            return
        
        # Get LogsScraper for join date checking
        logs_scraper = self.logs_scraper
        
        # Get configuration
        threshold = await self.config.contribution_threshold()
        
        # If target specified, check single member
        if target and not target.startswith("--"):
            await self._check_single_member(
                ctx, 
                target, 
                threshold, 
                logs_scraper,
                force_alert
            )
            return
        
        # Otherwise, check all members
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
        # Try to resolve target
        member_data = await self._resolve_target(ctx.guild, target)
        
        if not member_data or not member_data.mc_user_id:
            await ctx.send(
                f"❌ Could not find member with MC account: `{target}`\n"
                "Make sure they have a linked MC account."
            )
            return
        
        mc_id = member_data.mc_user_id
        mc_name = member_data.mc_username or "Unknown"
        
        # Get current MC data
        mc_data = await self._get_mc_data(mc_id)
        
        if not mc_data:
            await ctx.send(
                f"❌ Member `{mc_name}` ({mc_id}) not found in alliance.\n"
                "They may have left the alliance."
            )
            return
        
        current_rate = mc_data.get("contribution_rate", 0.0)
        
        # Build detailed embed
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
        
        # Check 2: Grace period (🔧 FIXED: Now with fallback)
        join_date = await self._get_join_date_for_member(mc_id, mc_name, logs_scraper)
        check2_pass = True
        grace_source = "unknown"
        
        if join_date:
            days_in_alliance = (datetime.now(timezone.utc) - join_date).days
            check2_pass = days_in_alliance >= 7
            
            # Determine source
            if logs_scraper:
                # Try to verify it came from LogsScraper
                try:
                    import aiosqlite
                    db_path = logs_scraper.db_path
                    if db_path.exists():
                        async with aiosqlite.connect(db_path) as db:
                            cursor = await db.execute(
                                "SELECT COUNT(*) FROM logs WHERE (affected_mc_id = ? OR affected_name = ?) AND action_key = 'added_to_alliance'",
                                (mc_id, mc_name)
                            )
                            count = (await cursor.fetchone())[0]
                            grace_source = "LogsScraper" if count > 0 else "first scrape (fallback)"
                except:
                    grace_source = "first scrape (fallback)"
            else:
                grace_source = "first scrape (fallback)"
            
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
        
        # Check 4: Historical consistency (🔧 FIXED: Now queries members_v2.db)
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
        
        # Force alert option
        if force_alert and all_pass:
            embed.set_footer(text="⚠️ Use --force-alert to send a real test alert")
        
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
                    # Send real alert
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
        
        # Initialize tracking
        would_alert = []
        skipped_grace = []
        skipped_cooldown = []
        skipped_insufficient = []
        skipped_inconsistent = []
        above_threshold = []
        
        status_msg = await ctx.send(f"🔍 Checking {len(mc_members)} alliance members...")
        
        # Check each member
        for i, mc_member in enumerate(mc_members):
            mc_id = mc_member.get("user_id") or mc_member.get("mc_user_id")
            if not mc_id:
                continue
            
            mc_name = mc_member.get("name", "Unknown")
            current_rate = mc_member.get("contribution_rate", 0.0)
            
            # Update status every 20 members
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
            
            # Check 2: Grace period (🔧 FIXED)
            join_date = await self._get_join_date_for_member(mc_id, mc_name, logs_scraper)
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
                # No join date found - skip this member
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
            
            # Check 4: Historical consistency (🔧 FIXED)
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
        
        # Update status
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
        
        # Summary stats
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
        
        # Show members who would get alerts
        if would_alert:
            alert_lines = []
            for member in would_alert[:10]:  # Show max 10
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
        
        # Verbose mode: show skipped members
        if verbose:
            # Grace period
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
            
            # Cooldown
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
            
            # Insufficient data
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
            
            # Inconsistent
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
                "Use --verbose for detailed breakdown. "
                "Use [p]member checkcontrib @user --force-alert to test."
            )
        )
        
        await ctx.send(embed=embed)
    
    async def _get_join_date_for_member(
        self,
        mc_id: str,
        mc_name: Optional[str],
        logs_scraper
    ) -> Optional[datetime]:
        """
        Get when member joined the alliance.
        
        🔧 FIXED: Now with fallback to first scrape date!
        
        Priority:
        1. LogsScraper: 'added_to_alliance' event
        2. Fallback: First scrape date from members_v2.db
        
        Returns None only if both methods fail.
        """
        # Try LogsScraper first
        if logs_scraper:
            try:
                import aiosqlite
                
                db_path = logs_scraper.db_path
                
                if db_path.exists():
                    async with aiosqlite.connect(db_path) as db:
                        cursor = await db.execute(
                            """
                            SELECT MIN(ts) as join_date 
                            FROM logs 
                            WHERE (affected_mc_id = ? OR affected_name = ?)
                            AND action_key = 'added_to_alliance'
                            """,
                            (mc_id, mc_name)
                        )
                        result = await cursor.fetchone()
                        
                        if result and result[0]:
                            join_date_str = result[0]
                            if join_date_str.endswith('Z'):
                                join_date_str = join_date_str.replace('Z', '+00:00')
                            
                            join_date = datetime.fromisoformat(join_date_str)
                            log.debug(f"Found join date for {mc_name} ({mc_id}) in LogsScraper: {join_date}")
                            return join_date
            
            except Exception as e:
                log.error(f"Failed to get join date from LogsScraper for {mc_id}: {e}")
        
        # 🔧 FALLBACK: Use first scrape date from members_v2.db
        if self.members_scraper:
            try:
                import sqlite3
                
                db_path = self.members_scraper.db_path
                
                if db_path.exists():
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
                        first_seen_str = result["first_seen"]
                        # Parse ISO timestamp
                        if first_seen_str.endswith('Z'):
                            first_seen_str = first_seen_str.replace('Z', '+00:00')
                        
                        first_seen = datetime.fromisoformat(first_seen_str)
                        log.debug(f"Using first scrape date for {mc_name} ({mc_id}): {first_seen} (fallback)")
                        return first_seen
            
            except Exception as e:
                log.error(f"Failed to get first scrape date for {mc_id}: {e}")
        
        return None
    
    async def _get_historical_rates_for_member(
        self,
        mc_id: str
    ) -> List[float]:
        """
        Get historical contribution rates for a member.
        
        🔧 FIXED: Now correctly queries members_v2.db instead of AllianceScraper!
        
        Returns list of rates (most recent first).
        """
        if not self.members_scraper:
            log.warning("MembersScraper not available for historical rates")
            return []
        
        try:
            import sqlite3
            
            db_path = self.members_scraper.db_path
            
            if not db_path.exists():
                log.error(f"Database not found: {db_path}")
                return []
            
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Query members table for historical rates
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
            log.error(f"Failed to get historical rates for {mc_id}: {e}", exc_info=True)
            return []
    
    # ==================== DEBUG COMMAND ====================
    
    @member.command(name="debug", aliases=["status"])
    @commands.guild_only()
    async def debug(self, ctx: commands.Context):
        """
        Show MemberManager integration status (for debugging).
        """
        # Check permissions
        if not await self._is_moderator(ctx.author):
            await ctx.send("❌ You need moderator permissions to use this command.")
            return
        
        embed = discord.Embed(
            title="🔧 MemberManager Debug Info",
            color=discord.Color.blue()
        )
        
        # Integration status
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
        
        # Database info
        db_info = []
        if self.db:
            db_info.append(f"✅ Database connected")
            db_info.append(f"Path: `{self.db_path}`")
        else:
            db_info.append("❌ Database not connected")
        
        # MembersScraper database
        if self.members_scraper:
            db_info.append(f"MembersScraper DB: `{self.members_scraper.db_path}`")
        
        embed.add_field(
            name="💾 Database",
            value="\n".join(db_info),
            inline=False
        )
        
        # Monitoring status
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
        
        # Available cogs (for debugging)
        all_cogs = [c.qualified_name for c in self.bot.cogs.values()]
        cog_list = ", ".join(sorted(all_cogs))
        
        embed.add_field(
            name="📦 All Loaded Cogs",
            value=f"```{cog_list}```",
            inline=False
        )
        
        # Version
        embed.set_footer(text=f"MemberManager v{__version__}")
        
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
