"""
Configuration commands for MemberManager
Extension of the main cog for settings management
"""

import discord
from redbot.core import commands
from redbot.core.utils.chat_formatting import box


class ConfigCommands:
    """Configuration commands mixin for MemberManager."""
    
    @commands.group(name="memberset", aliases=["mmset"])
    @commands.admin()
    @commands.guild_only()
    async def memberset(self, ctx: commands.Context):
        """
        Configure MemberManager settings.
        """
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @memberset.command(name="view")
    async def memberset_view(self, ctx: commands.Context):
        """View current MemberManager configuration."""
        config = await self.config.all()
        
        # Get channel names
        admin_channel = ctx.guild.get_channel(config.get("admin_alert_channel"))
        modlog_channel = ctx.guild.get_channel(config.get("modlog_channel"))
        
        # Get role names
        admin_roles = [ctx.guild.get_role(r) for r in config.get("admin_role_ids", [])]
        admin_roles = [r.name for r in admin_roles if r]
        
        mod_roles = [ctx.guild.get_role(r) for r in config.get("moderator_role_ids", [])]
        mod_roles = [r.name for r in mod_roles if r]
        
        settings = f"""
MemberManager Configuration
═══════════════════════════

Contribution Monitoring:
  Threshold: {config.get('contribution_threshold', 5.0)}%
  Trend weeks: {config.get('contribution_trend_weeks', 3)}
  Auto alerts: {'✅ Enabled' if config.get('auto_contribution_alert') else '❌ Disabled'}

Automation:
  Role drift check: {'✅ Enabled' if config.get('auto_role_drift_check') else '❌ Disabled'}

Channels:
  Admin alerts: {admin_channel.mention if admin_channel else '❌ Not set'}
  Modlog: {modlog_channel.mention if modlog_channel else '❌ Not set'}

Permissions:
  Admin roles: {', '.join(admin_roles) if admin_roles else '❌ None'}
  Moderator roles: {', '.join(mod_roles) if mod_roles else '❌ None'}

Notes:
  Default expiry: {config.get('note_expiry_days', 90)} days
  
Integrations:
  MemberSync: {'✅ Connected' if self.membersync else '❌ Not found'}
  AllianceScraper: {'✅ Connected' if self.alliance_scraper else '❌ Not found'}
  SanctionManager: {'✅ Connected' if self.sanction_manager else '❌ Not found'}
        """
        
        await ctx.send(box(settings, lang="yaml"))
    
    @memberset.command(name="alertchannel")
    async def memberset_alertchannel(
        self,
        ctx: commands.Context,
        channel: discord.TextChannel
    ):
        """
        Set the channel for admin alerts.
        
        This is where contribution alerts and other notifications will be sent.
        """
        await self.config.admin_alert_channel.set(channel.id)
        await ctx.send(f"✅ Admin alert channel set to {channel.mention}")
    
    @memberset.command(name="modlogchannel")
    async def memberset_modlogchannel(
        self,
        ctx: commands.Context,
        channel: discord.TextChannel
    ):
        """Set the channel for modlog events."""
        await self.config.modlog_channel.set(channel.id)
        await ctx.send(f"✅ Modlog channel set to {channel.mention}")
    
    @memberset.command(name="adminroles")
    async def memberset_adminroles(
        self,
        ctx: commands.Context,
        *roles: discord.Role
    ):
        """
        Set admin roles for MemberManager.
        
        Admins have full access to all commands.
        """
        role_ids = [r.id for r in roles]
        await self.config.admin_role_ids.set(role_ids)
        
        role_mentions = ", ".join(r.mention for r in roles)
        await ctx.send(f"✅ Admin roles set to: {role_mentions}")
    
    @memberset.command(name="modroles")
    async def memberset_modroles(
        self,
        ctx: commands.Context,
        *roles: discord.Role
    ):
        """
        Set moderator roles for MemberManager.
        
        Moderators have read-only access and can add notes.
        """
        role_ids = [r.id for r in roles]
        await self.config.moderator_role_ids.set(role_ids)
        
        role_mentions = ", ".join(r.mention for r in roles)
        await ctx.send(f"✅ Moderator roles set to: {role_mentions}")
    
    @memberset.command(name="threshold")
    async def memberset_threshold(
        self,
        ctx: commands.Context,
        threshold: float
    ):
        """
        Set contribution rate threshold for alerts.
        
        Members with contribution below this percentage will trigger alerts.
        Default: 5.0%
        """
        if threshold < 0 or threshold > 100:
            await ctx.send("❌ Threshold must be between 0 and 100.")
            return
        
        await self.config.contribution_threshold.set(threshold)
        await ctx.send(f"✅ Contribution threshold set to {threshold}%")
    
    @memberset.command(name="trendweeks")
    async def memberset_trendweeks(
        self,
        ctx: commands.Context,
        weeks: int
    ):
        """
        Set number of weeks to analyze for contribution trends.
        
        Default: 3 weeks
        """
        if weeks < 1 or weeks > 12:
            await ctx.send("❌ Trend weeks must be between 1 and 12.")
            return
        
        await self.config.contribution_trend_weeks.set(weeks)
        await ctx.send(f"✅ Contribution trend period set to {weeks} weeks")
    
    @memberset.command(name="autocontribution")
    async def memberset_autocontribution(
        self,
        ctx: commands.Context,
        enabled: bool
    ):
        """
        Enable or disable automatic contribution monitoring.
        
        When enabled, the bot will check contributions twice per day
        and send alerts for members below the threshold.
        """
        await self.config.auto_contribution_alert.set(enabled)
        
        if enabled:
            # Start monitoring if not already running
            if not self._automation_task or self._automation_task.done():
                if await self.config.auto_contribution_alert():
                    from .automation import ContributionMonitor
                    self.contribution_monitor = ContributionMonitor(
                        self.bot,
                        self.db,
                        self.config,
                        self.alliance_scraper
                    )
                    self._automation_task = self.bot.loop.create_task(
                        self.contribution_monitor.run()
                    )
            
            await ctx.send("✅ Automatic contribution monitoring **enabled**")
        else:
            # Stop monitoring
            if self._automation_task and not self._automation_task.done():
                self._automation_task.cancel()
                self._automation_task = None
            
            await ctx.send("❌ Automatic contribution monitoring **disabled**")
    
    @memberset.command(name="autoroledrift")
    async def memberset_autoroledrift(
        self,
        ctx: commands.Context,
        enabled: bool
    ):
        """
        Enable or disable automatic role drift detection.
        
        When enabled, the bot will check for missing verified roles.
        """
        await self.config.auto_role_drift_check.set(enabled)
        
        status = "enabled" if enabled else "disabled"
        await ctx.send(f"✅ Automatic role drift detection **{status}**")
    
    @memberset.command(name="noteexpiry")
    async def memberset_noteexpiry(
        self,
        ctx: commands.Context,
        days: int
    ):
        """
        Set default expiry time for notes (in days).
        
        Default: 90 days
        Set to 0 for no expiry.
        """
        if days < 0:
            await ctx.send("❌ Days must be 0 or positive.")
            return
        
        await self.config.note_expiry_days.set(days)
        
        if days == 0:
            await ctx.send("✅ Notes will no longer expire by default")
        else:
            await ctx.send(f"✅ Default note expiry set to {days} days")
    
    @memberset.command(name="reset")
    async def memberset_reset(self, ctx: commands.Context):
        """
        Reset all MemberManager settings to defaults.
        
        **Warning:** This cannot be undone!
        """
        # Ask for confirmation
        msg = await ctx.send(
            "⚠️ **Warning**: This will reset ALL MemberManager settings to defaults.\n"
            "React with ✅ to confirm or ❌ to cancel."
        )
        
        await msg.add_reaction("✅")
        await msg.add_reaction("❌")
        
        def check(reaction, user):
            return (
                user == ctx.author 
                and str(reaction.emoji) in ["✅", "❌"]
                and reaction.message.id == msg.id
            )
        
        try:
            reaction, user = await ctx.bot.wait_for("reaction_add", timeout=30.0, check=check)
            
            if str(reaction.emoji) == "✅":
                await self.config.clear_all()
                await ctx.send("✅ All settings have been reset to defaults.")
            else:
                await ctx.send("❌ Reset cancelled.")
        
        except asyncio.TimeoutError:
            await ctx.send("❌ Reset cancelled (timeout).")
