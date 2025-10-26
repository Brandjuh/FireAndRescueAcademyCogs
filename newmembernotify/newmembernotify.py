from __future__ import annotations

import asyncio
import logging
from typing import Optional, List, Dict, Any, Set
from datetime import datetime

import discord
from discord import Embed, ButtonStyle
from discord.ui import Button, View
from bs4 import BeautifulSoup

from redbot.core import commands, Config, checks
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import pagify

log = logging.getLogger("red.FARA.NewMemberNotify")

DEFAULTS = {
    "notification_channel_id": 1421625293130567690,
    "auto_accept_log_channel_id": 550375997258596356,
    "admin_role_id": None,
    "auto_accept_enabled": False,
    "check_interval_minutes": 15,
    "bewerbungen_url": "https://www.missionchief.com/verband/bewerbungen",
}


class AcceptDenyView(View):
    """Discord UI View with Accept and Deny buttons."""
    
    def __init__(self, cog, application_id: str, username: str, profile_url: str, admin_role_id: Optional[int]):
        super().__init__(timeout=None)  # Persistent view
        self.cog = cog
        self.application_id = application_id
        self.username = username
        self.profile_url = profile_url
        self.admin_role_id = admin_role_id
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Check if user has admin role."""
        if self.admin_role_id is None:
            # No role set, allow all
            return True
        
        if not interaction.user.guild:
            await interaction.response.send_message("❌ This can only be used in a server.", ephemeral=True)
            return False
        
        member = interaction.guild.get_member(interaction.user.id)
        if not member:
            await interaction.response.send_message("❌ Could not verify your permissions.", ephemeral=True)
            return False
        
        # Check if user has the required role
        has_role = any(role.id == self.admin_role_id for role in member.roles)
        if not has_role:
            await interaction.response.send_message("❌ You don't have permission to use this button.", ephemeral=True)
            return False
        
        return True
    
    @discord.ui.button(label="Accept", style=ButtonStyle.success, custom_id="accept_member")
    async def accept_button(self, interaction: discord.Interaction, button: Button):
        """Accept button handler."""
        await interaction.response.defer(ephemeral=True)
        
        success, message = await self.cog._process_application(self.application_id, action="accept")
        
        if success:
            # Update embed to show accepted
            if interaction.message:
                embed = interaction.message.embeds[0] if interaction.message.embeds else None
                if embed:
                    embed.color = discord.Color.green()
                    embed.set_footer(text=f"✅ Accepted by {interaction.user.display_name} at {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
                    await interaction.message.edit(embed=embed, view=None)
            
            await interaction.followup.send(f"✅ Successfully accepted **{self.username}**!", ephemeral=True)
            
            # Log to auto-accept channel
            await self.cog._log_action("manual_accept", self.username, self.profile_url, interaction.user.display_name)
        else:
            await interaction.followup.send(f"❌ Failed to accept: {message}", ephemeral=True)
    
    @discord.ui.button(label="Deny", style=ButtonStyle.danger, custom_id="deny_member")
    async def deny_button(self, interaction: discord.Interaction, button: Button):
        """Deny button handler."""
        await interaction.response.defer(ephemeral=True)
        
        success, message = await self.cog._process_application(self.application_id, action="deny")
        
        if success:
            # Update embed to show denied
            if interaction.message:
                embed = interaction.message.embeds[0] if interaction.message.embeds else None
                if embed:
                    embed.color = discord.Color.red()
                    embed.set_footer(text=f"❌ Denied by {interaction.user.display_name} at {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
                    await interaction.message.edit(embed=embed, view=None)
            
            await interaction.followup.send(f"❌ Successfully denied **{self.username}**!", ephemeral=True)
            
            # Log to auto-accept channel
            await self.cog._log_action("manual_deny", self.username, self.profile_url, interaction.user.display_name)
        else:
            await interaction.followup.send(f"❌ Failed to deny: {message}", ephemeral=True)


class NewMemberNotify(commands.Cog):
    """Automatically notify admins of new alliance member applications."""
    
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA4A1234, force_registration=True)
        self.config.register_global(**DEFAULTS)
        
        self._check_task: Optional[asyncio.Task] = None
        self._seen_applications: Set[str] = set()
        self._first_run = True
        
        # Start background task
        self.bot.loop.create_task(self._start_background_task())
    
    def cog_unload(self):
        """Cleanup on unload."""
        if self._check_task:
            self._check_task.cancel()
    
    async def _start_background_task(self):
        """Start the background check task after bot is ready."""
        try:
            await self.bot.wait_until_red_ready()
        except Exception:
            pass
        
        if self._check_task is None:
            self._check_task = asyncio.create_task(self._background_checker())
            log.info("NewMemberNotify background task started")
    
    async def _background_checker(self):
        """Background task that checks for new applications."""
        while True:
            try:
                await self._check_for_new_applications()
            except Exception as e:
                log.error(f"Error in background checker: {e}", exc_info=True)
            
            # Wait for next check
            interval_minutes = await self.config.check_interval_minutes()
            await asyncio.sleep(interval_minutes * 60)
    
    async def _get_cookie_session(self):
        """Get authenticated session from CookieManager cog."""
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager:
            log.error("CookieManager cog not loaded!")
            return None
        
        try:
            session = await cookie_manager.get_session()
            return session
        except Exception as e:
            log.error(f"Failed to get session from CookieManager: {e}")
            return None
    
    async def _fetch_applications(self) -> List[Dict[str, str]]:
        """
        Fetch pending applications from MissionChief.
        Returns list of dicts with keys: id, username, profile_url
        """
        session = await self._get_cookie_session()
        if not session:
            log.warning("No session available, skipping check")
            return []
        
        url = await self.config.bewerbungen_url()
        
        try:
            async with session.get(url, allow_redirects=True) as resp:
                if resp.status != 200:
                    log.warning(f"Non-200 status when fetching applications: {resp.status}")
                    return []
                
                html = await resp.text()
                
                # Check if we're still logged in
                if "/users/sign_in" in str(resp.url) or "sign_in" in html.lower():
                    log.warning("Session appears to be logged out. CookieManager should refresh soon.")
                    return []
                
                # Parse HTML
                soup = BeautifulSoup(html, "lxml")
                applications = []
                
                # Find the table with applications
                # Look for table rows with accept/deny links
                for row in soup.find_all("tr"):
                    # Find accept link (annehmen)
                    accept_link = row.find("a", href=lambda x: x and "/verband/bewerbungen/annehmen/" in x)
                    if not accept_link:
                        continue
                    
                    # Extract application ID from accept link
                    href = accept_link.get("href", "")
                    if "/annehmen/" not in href:
                        continue
                    
                    app_id = href.split("/annehmen/")[-1].strip()
                    if not app_id:
                        continue
                    
                    # Find username (should be a link in the same row)
                    username_link = row.find("a", href=lambda x: x and "/profile/" in x)
                    if username_link:
                        username = username_link.get_text(strip=True)
                        profile_url = username_link.get("href", "")
                        
                        # Make profile URL absolute if needed
                        if profile_url and not profile_url.startswith("http"):
                            profile_url = f"https://www.missionchief.com{profile_url}"
                    else:
                        # Fallback: try to find any text in the "Name" column
                        name_cell = row.find("td")
                        if name_cell:
                            username = name_cell.get_text(strip=True)
                            # Remove button text if present
                            for btn_text in ["Accept", "Deny", "Annehmen", "Ablehnen"]:
                                username = username.replace(btn_text, "").strip()
                            profile_url = f"https://www.missionchief.com/profile/{app_id}"
                        else:
                            username = f"User_{app_id}"
                            profile_url = f"https://www.missionchief.com/profile/{app_id}"
                    
                    applications.append({
                        "id": app_id,
                        "username": username,
                        "profile_url": profile_url
                    })
                
                log.debug(f"Found {len(applications)} pending applications")
                return applications
                
        except Exception as e:
            log.error(f"Error fetching applications: {e}", exc_info=True)
            return []
    
    async def _process_application(self, app_id: str, action: str = "accept") -> tuple[bool, str]:
        """
        Process an application (accept or deny).
        Returns (success: bool, message: str)
        """
        session = await self._get_cookie_session()
        if not session:
            return False, "No authenticated session available"
        
        base_url = "https://www.missionchief.com/verband/bewerbungen"
        if action == "accept":
            url = f"{base_url}/annehmen/{app_id}"
        elif action == "deny":
            url = f"{base_url}/ablehnen/{app_id}"
        else:
            return False, f"Invalid action: {action}"
        
        try:
            async with session.get(url, allow_redirects=True) as resp:
                if resp.status == 200:
                    return True, f"Application {action}ed successfully"
                else:
                    return False, f"HTTP {resp.status}"
        except Exception as e:
            log.error(f"Error processing application {app_id} ({action}): {e}")
            return False, str(e)
    
    async def _check_for_new_applications(self, skip_first_run_check: bool = False):
        """Check for new applications and handle them.
        
        Args:
            skip_first_run_check: If True, process applications even on first run (for manual checks)
        """
        applications = await self._fetch_applications()
        
        if not applications:
            log.debug("No applications found or error fetching")
            return
        
        # Get current config
        auto_accept = await self.config.auto_accept_enabled()
        
        # Check if this is first run and we should skip processing
        should_skip_processing = self._first_run and not skip_first_run_check
        
        for app in applications:
            app_id = app["id"]
            
            # Skip if we've already seen this application
            if app_id in self._seen_applications:
                continue
            
            # Mark as seen
            self._seen_applications.add(app_id)
            
            # Skip processing on first run to avoid spam (unless explicitly told not to)
            if should_skip_processing:
                log.debug(f"First run: skipping notification for {app['username']} ({app_id})")
                continue
            
            # Auto-accept if enabled
            if auto_accept:
                success, message = await self._process_application(app_id, "accept")
                if success:
                    log.info(f"Auto-accepted application: {app['username']} ({app_id})")
                    await self._log_action("auto_accept", app["username"], app["profile_url"])
                else:
                    log.error(f"Failed to auto-accept {app['username']}: {message}")
                    await self._log_action("auto_accept_failed", app["username"], app["profile_url"], error=message)
            else:
                # Send notification to admins
                await self._send_notification(app)
        
        # Mark first run as complete
        if self._first_run:
            self._first_run = False
            log.info("First run complete, will now process new applications")
    
    async def _send_notification(self, app: Dict[str, str]):
        """Send notification embed with accept/deny buttons to admin channel."""
        channel_id = await self.config.notification_channel_id()
        if not channel_id:
            log.warning("No notification channel configured")
            return
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            log.warning(f"Could not find notification channel {channel_id}")
            return
        
        # Create embed
        embed = Embed(
            title="🆕 New Alliance Application",
            description=f"**{app['username']}** has requested to join the alliance!",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        embed.add_field(name="Username", value=app['username'], inline=True)
        embed.add_field(name="Profile", value=f"[View Profile]({app['profile_url']})", inline=True)
        embed.set_footer(text=f"Application ID: {app['id']}")
        
        # Create view with buttons
        admin_role_id = await self.config.admin_role_id()
        view = AcceptDenyView(
            cog=self,
            application_id=app["id"],
            username=app["username"],
            profile_url=app["profile_url"],
            admin_role_id=admin_role_id
        )
        
        try:
            await channel.send(embed=embed, view=view)
            log.info(f"Sent notification for new application: {app['username']} ({app['id']})")
        except Exception as e:
            log.error(f"Failed to send notification: {e}")
    
    async def _log_action(self, action_type: str, username: str, profile_url: str, actor: str = "System", error: str = None):
        """Log an action to the auto-accept log channel."""
        channel_id = await self.config.auto_accept_log_channel_id()
        if not channel_id:
            return
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return
        
        # Create log embed
        if action_type == "auto_accept":
            embed = Embed(
                title="✅ Auto-Accepted Application",
                description=f"**{username}** was automatically accepted",
                color=discord.Color.green(),
                timestamp=datetime.utcnow()
            )
        elif action_type == "auto_accept_failed":
            embed = Embed(
                title="⚠️ Auto-Accept Failed",
                description=f"Failed to auto-accept **{username}**",
                color=discord.Color.orange(),
                timestamp=datetime.utcnow()
            )
            if error:
                embed.add_field(name="Error", value=error, inline=False)
        elif action_type == "manual_accept":
            embed = Embed(
                title="✅ Manually Accepted",
                description=f"**{username}** was accepted by {actor}",
                color=discord.Color.green(),
                timestamp=datetime.utcnow()
            )
        elif action_type == "manual_deny":
            embed = Embed(
                title="❌ Manually Denied",
                description=f"**{username}** was denied by {actor}",
                color=discord.Color.red(),
                timestamp=datetime.utcnow()
            )
        else:
            return
        
        embed.add_field(name="Profile", value=f"[View Profile]({profile_url})", inline=False)
        
        try:
            await channel.send(embed=embed)
        except Exception as e:
            log.error(f"Failed to send log message: {e}")
    
    # ================== COMMANDS ==================
    
    @commands.group(name="newmember")
    @checks.is_owner()
    async def newmember(self, ctx: commands.Context):
        """New member notification commands (owner only)."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @newmember.command(name="status")
    async def status(self, ctx: commands.Context):
        """Show current configuration and status."""
        cfg = await self.config.all()
        
        notif_channel = self.bot.get_channel(cfg["notification_channel_id"])
        log_channel = self.bot.get_channel(cfg["auto_accept_log_channel_id"])
        
        lines = [
            f"**Notification Channel:** {notif_channel.mention if notif_channel else 'Not set'}",
            f"**Log Channel:** {log_channel.mention if log_channel else 'Not set'}",
            f"**Admin Role ID:** {cfg['admin_role_id'] or 'Not set (anyone can use buttons)'}",
            f"**Auto-Accept:** {'✅ Enabled' if cfg['auto_accept_enabled'] else '❌ Disabled'}",
            f"**Check Interval:** {cfg['check_interval_minutes']} minutes",
            f"**Applications Seen:** {len(self._seen_applications)}",
            f"**Background Task:** {'Running' if self._check_task and not self._check_task.done() else 'Not running'}",
        ]
        
        embed = Embed(
            title="New Member Notify Status",
            description="\n".join(lines),
            color=discord.Color.blue()
        )
        
        await ctx.send(embed=embed)
    
    @newmember.command(name="setchannel")
    async def setchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the notification channel for new applications."""
        await self.config.notification_channel_id.set(channel.id)
        await ctx.send(f"✅ Notification channel set to {channel.mention}")
    
    @newmember.command(name="setlogchannel")
    async def setlogchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the log channel for auto-accept notifications."""
        await self.config.auto_accept_log_channel_id.set(channel.id)
        await ctx.send(f"✅ Log channel set to {channel.mention}")
    
    @newmember.command(name="setadminrole")
    async def setadminrole(self, ctx: commands.Context, role: discord.Role):
        """Set the admin role required to use Accept/Deny buttons."""
        await self.config.admin_role_id.set(role.id)
        await ctx.send(f"✅ Admin role set to {role.mention}")
    
    @newmember.command(name="clearadminrole")
    async def clearadminrole(self, ctx: commands.Context):
        """Clear the admin role requirement (anyone can use buttons)."""
        await self.config.admin_role_id.set(None)
        await ctx.send("✅ Admin role requirement cleared. Anyone can now use the buttons.")
    
    @newmember.command(name="autoaccept")
    async def autoaccept(self, ctx: commands.Context, enabled: bool):
        """Enable or disable auto-accept mode."""
        await self.config.auto_accept_enabled.set(enabled)
        if enabled:
            await ctx.send("✅ Auto-accept **enabled**. New applications will be automatically accepted.")
        else:
            await ctx.send("✅ Auto-accept **disabled**. Admins will be notified of new applications.")
    
    @newmember.command(name="setinterval")
    async def setinterval(self, ctx: commands.Context, minutes: int):
        """Set check interval in minutes (minimum 1)."""
        if minutes < 1:
            await ctx.send("❌ Interval must be at least 1 minute.")
            return
        
        await self.config.check_interval_minutes.set(minutes)
        await ctx.send(f"✅ Check interval set to {minutes} minutes. This will take effect on the next check.")
    
    @newmember.command(name="checknow")
    async def checknow(self, ctx: commands.Context):
        """Manually trigger a check for new applications (processes immediately, ignores first-run protection)."""
        await ctx.send("🔍 Checking for new applications...")
        
        try:
            # Manual checks should always process, even on first run
            await self._check_for_new_applications(skip_first_run_check=True)
            await ctx.send("✅ Check complete!")
        except Exception as e:
            await ctx.send(f"❌ Error during check: {e}")
            log.error(f"Manual check failed: {e}", exc_info=True)
    
    @newmember.command(name="reset")
    async def reset(self, ctx: commands.Context):
        """Reset the list of seen applications (will re-process on next check)."""
        count = len(self._seen_applications)
        self._seen_applications.clear()
        await ctx.send(f"✅ Reset complete. Cleared {count} seen applications. Next check will process all pending applications.")
    
    @newmember.command(name="debug")
    async def debug(self, ctx: commands.Context):
        """Show debug information."""
        lines = [
            f"**First Run Mode:** {self._first_run}",
            f"**Seen Applications:** {len(self._seen_applications)}",
            f"**Seen IDs:** {', '.join(self._seen_applications) if self._seen_applications else 'None'}",
        ]
        await ctx.send("\n".join(lines))
    
    @newmember.command(name="forceaccept")
    async def forceaccept(self, ctx: commands.Context, application_id: str):
        """Force accept a specific application by ID (bypasses seen check)."""
        await ctx.send(f"🔄 Attempting to accept application {application_id}...")
        
        success, message = await self._process_application(application_id, action="accept")
        
        if success:
            await ctx.send(f"✅ Successfully accepted application {application_id}!")
            self._seen_applications.add(application_id)
        else:
            await ctx.send(f"❌ Failed to accept: {message}")
    
    @newmember.command(name="forcedeny")
    async def forcedeny(self, ctx: commands.Context, application_id: str):
        """Force deny a specific application by ID (bypasses seen check)."""
        await ctx.send(f"🔄 Attempting to deny application {application_id}...")
        
        success, message = await self._process_application(application_id, action="deny")
        
        if success:
            await ctx.send(f"✅ Successfully denied application {application_id}!")
            self._seen_applications.add(application_id)
        else:
            await ctx.send(f"❌ Failed to deny: {message}")
    
    @newmember.command(name="list")
    async def list_applications(self, ctx: commands.Context):
        """List all current pending applications."""
        await ctx.send("🔍 Fetching applications...")
        
        applications = await self._fetch_applications()
        
        if not applications:
            await ctx.send("No pending applications found.")
            return
        
        embed = Embed(
            title="Pending Alliance Applications",
            description=f"Found {len(applications)} pending application(s)",
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        
        for i, app in enumerate(applications[:10], 1):  # Limit to 10 to avoid embed limits
            seen_marker = "✅ Seen" if app["id"] in self._seen_applications else "🆕 New"
            embed.add_field(
                name=f"{i}. {app['username']} ({seen_marker})",
                value=f"[Profile]({app['profile_url']}) • ID: `{app['id']}`",
                inline=False
            )
        
        if len(applications) > 10:
            embed.set_footer(text=f"Showing 10 of {len(applications)} applications")
        
        await ctx.send(embed=embed)


async def setup(bot: Red):
    """Setup function for Red-DiscordBot."""
    await bot.add_cog(NewMemberNotify(bot))
