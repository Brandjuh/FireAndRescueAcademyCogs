"""
Discord UI Views for MemberManager
Tab-based interface with buttons for navigation
"""

import discord
from typing import Optional, Dict, Any, List
import logging

from .models import MemberData
from .utils import (
    format_timestamp,
    format_contribution_trend,
    format_role_list,
    truncate_text,
    get_severity_emoji,
    build_mc_profile_url
)

log = logging.getLogger("red.FARA.MemberManager.views")


class MemberOverviewView(discord.ui.View):
    """
    Main view for member information with tabs.
    
    Tabs: Overview | Notes | Infractions | Events
    """
    
    def __init__(
        self,
        bot,
        db,
        config,
        member_data: MemberData,
        integrations: Dict[str, Any]
    ):
        super().__init__(timeout=300)  # 5 minute timeout
        
        self.bot = bot
        self.db = db
        self.config = config
        self.member_data = member_data
        self.integrations = integrations
        
        self.current_tab = "overview"
        self.message: Optional[discord.Message] = None
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only allow the command invoker to use buttons."""
        # For now, allow anyone with permissions
        return True
    
    async def on_timeout(self):
        """Disable all buttons when view times out."""
        for item in self.children:
            item.disabled = True
        
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass
    
    # ==================== TAB BUTTONS ====================
    
    @discord.ui.button(
        label="Overview",
        style=discord.ButtonStyle.primary,
        custom_id="mm:overview",
        row=0
    )
    async def btn_overview(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        """Show overview tab."""
        self.current_tab = "overview"
        await self._update_view(interaction)
    
    @discord.ui.button(
        label="Notes",
        style=discord.ButtonStyle.secondary,
        custom_id="mm:notes",
        row=0
    )
    async def btn_notes(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        """Show notes tab."""
        self.current_tab = "notes"
        await self._update_view(interaction)
    
    @discord.ui.button(
        label="Infractions",
        style=discord.ButtonStyle.secondary,
        custom_id="mm:infractions",
        row=0
    )
    async def btn_infractions(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        """Show infractions tab."""
        self.current_tab = "infractions"
        await self._update_view(interaction)
    
    @discord.ui.button(
        label="Events",
        style=discord.ButtonStyle.secondary,
        custom_id="mm:events",
        row=0
    )
    async def btn_events(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        """Show events tab."""
        self.current_tab = "events"
        await self._update_view(interaction)
    
    # ==================== ACTION BUTTONS ====================
    
    @discord.ui.button(
        label="Add Note",
        style=discord.ButtonStyle.success,
        emoji="📝",
        custom_id="mm:add_note",
        row=1
    )
    async def btn_add_note(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        """Open modal to add a note."""
        modal = AddNoteModal(self)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(
        label="Edit Note",
        style=discord.ButtonStyle.secondary,
        emoji="✏️",
        custom_id="mm:edit_note",
        row=1
    )
    async def btn_edit_note(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        """Open modal to edit a note."""
        # Only show in notes tab
        if self.current_tab != "notes":
            await interaction.response.send_message(
                "⚠️ Switch to the Notes tab first to edit notes.",
                ephemeral=True
            )
            return
        
        modal = EditNoteModal(self)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(
        label="Delete Note",
        style=discord.ButtonStyle.danger,
        emoji="🗑️",
        custom_id="mm:delete_note",
        row=1
    )
    async def btn_delete_note(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        """Open modal to delete a note."""
        # Only show in notes tab
        if self.current_tab != "notes":
            await interaction.response.send_message(
                "⚠️ Switch to the Notes tab first to delete notes.",
                ephemeral=True
            )
            return
        
        modal = DeleteNoteModal(self)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(
        label="Export Data",
        style=discord.ButtonStyle.secondary,
        emoji="💾",
        custom_id="mm:export",
        row=1
    )
    async def btn_export(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        """Export member data."""
        await interaction.response.send_message(
            "📦 Export feature coming soon!",
            ephemeral=True
        )
    
    @discord.ui.button(
        label="Close",
        style=discord.ButtonStyle.danger,
        emoji="❌",
        custom_id="mm:close",
        row=1
    )
    async def btn_close(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        """Close the view and delete the message."""
        # Disable all buttons
        for item in self.children:
            item.disabled = True
        
        try:
            await interaction.message.delete()
        except:
            # If we can't delete, just disable the view
            await interaction.response.edit_message(
                content="*View closed*",
                embed=None,
                view=self
            )
    
    # ==================== EMBED BUILDERS ====================
    
    async def get_overview_embed(self) -> discord.Embed:
        """Build the overview embed."""
        data = self.member_data
        
        embed = discord.Embed(
            title=f"👤 Member Overview - {data.get_display_name()}",
            color=discord.Color.blue()
        )
        
        # Discord Information
        discord_lines = []
        if data.has_discord():
            discord_lines.append(f"**Username:** {data.discord_username or 'Unknown'}")
            discord_lines.append(f"**ID:** `{data.discord_id}`")
            discord_lines.append(f"**Roles:** {format_role_list(data.discord_roles)}")
            
            if data.discord_joined:
                discord_lines.append(f"**Joined:** {format_timestamp(int(data.discord_joined.timestamp()), 'D')}")
            
            status_emoji = "✅" if data.is_verified else "⚠️"
            discord_lines.append(f"**Status:** {status_emoji} {'Verified' if data.is_verified else 'Not Verified'}")
        else:
            discord_lines.append("*No Discord information available*")
        
        embed.add_field(
            name="🎮 Discord Information",
            value="\n".join(discord_lines),
            inline=False
        )
        
        # MissionChief Information
        mc_lines = []
        if data.has_mc():
            mc_lines.append(f"**Username:** {data.mc_username or 'Unknown'}")
            mc_lines.append(f"**ID:** `{data.mc_user_id}`")
            
            if data.mc_user_id:
                profile_url = build_mc_profile_url(data.mc_user_id)
                mc_lines.append(f"**Profile:** [View Profile]({profile_url})")
            
            mc_lines.append(f"**Role:** {data.mc_role or 'None'}")
            
            if data.contribution_rate is not None:
                contrib_display = format_contribution_trend(
                    data.contribution_rate,
                    use_emoji=True
                )
                mc_lines.append(f"**Contribution:** {contrib_display}")
            
            mc_lines.append(f"**Status:** ✅ Active")
        else:
            mc_lines.append("*No MissionChief information available*")
        
        embed.add_field(
            name="🚒 MissionChief Information",
            value="\n".join(mc_lines),
            inline=False
        )
        
        # Quick Stats
        stats_lines = [
            f"**Infractions:** {data.infractions_count} active",
            f"**Notes:** {data.notes_count} total",
            f"**Severity:** {get_severity_emoji(data.severity_score)} {data.severity_score} points"
        ]
        
        if data.on_watchlist:
            stats_lines.append(f"**Watchlist:** ⚠️ {data.watchlist_reason or 'Active'}")
        
        embed.add_field(
            name="📊 Quick Stats",
            value="\n".join(stats_lines),
            inline=False
        )
        
        # Link status
        if data.is_linked():
            embed.set_footer(text="✅ Discord and MC accounts are linked")
        elif data.has_discord() and data.has_mc():
            embed.set_footer(text="⚠️ Accounts not linked or pending verification")
        
        return embed
    
    async def get_notes_embed(self) -> discord.Embed:
        """Build the notes embed."""
        data = self.member_data
        
        embed = discord.Embed(
            title=f"📝 Notes - {data.get_display_name()}",
            color=discord.Color.gold()
        )
        
        # Fetch notes
        notes = await self.db.get_notes(
            discord_id=data.discord_id,
            mc_user_id=data.mc_user_id,
            status="active",
            limit=10
        )
        
        if not notes:
            embed.description = "*No notes found for this member.*"
            return embed
        
        # Separate pinned and regular notes
        pinned_notes = [n for n in notes if n.get("is_pinned")]
        regular_notes = [n for n in notes if not n.get("is_pinned")]
        
        # Pinned notes
        if pinned_notes:
            pinned_lines = []
            for note in pinned_notes[:3]:
                ref = note.get("ref_code", "???")
                text = truncate_text(note.get("note_text", ""), 100)
                created = format_timestamp(note.get("created_at", 0), "R")
                author = note.get("author_name", "Unknown")
                
                pinned_lines.append(f"📌 **`{ref}`** | {created} | {author}")
                pinned_lines.append(f"   {text}")
                
                if note.get("infraction_ref"):
                    pinned_lines.append(f"   🔗 Linked: `{note['infraction_ref']}`")
                
                pinned_lines.append("")  # Blank line
            
            embed.add_field(
                name="📌 Pinned Notes",
                value="\n".join(pinned_lines),
                inline=False
            )
        
        # Regular notes
        if regular_notes:
            regular_lines = []
            for note in regular_notes[:5]:
                ref = note.get("ref_code", "???")
                text = truncate_text(note.get("note_text", ""), 80)
                created = format_timestamp(note.get("created_at", 0), "R")
                author = note.get("author_name", "Unknown")
                
                regular_lines.append(f"• **`{ref}`** | {created} | {author}")
                regular_lines.append(f"  {text}\n")
            
            if len(notes) > 8:
                remaining = len(notes) - 8
                regular_lines.append(f"*...and {remaining} more notes*")
            
            embed.add_field(
                name="📄 Recent Notes",
                value="\n".join(regular_lines),
                inline=False
            )
        
        embed.set_footer(text=f"Total notes: {len(notes)}")
        
        return embed
    
    async def get_infractions_embed(self) -> discord.Embed:
        """Build the infractions embed."""
        data = self.member_data
        
        embed = discord.Embed(
            title=f"⚠️ Infractions - {data.get_display_name()}",
            color=discord.Color.red()
        )
        
        # Fetch infractions
        infractions = await self.db.get_infractions(
            discord_id=data.discord_id,
            mc_user_id=data.mc_user_id,
            limit=15
        )
        
        if not infractions:
            embed.description = "*No infractions found for this member.*"
            return embed
        
        # Group by platform
        discord_inf = [i for i in infractions if i.get("platform") == "discord"]
        mc_inf = [i for i in infractions if i.get("platform") == "missionchief"]
        
        # Discord infractions
        if discord_inf:
            discord_lines = []
            for inf in discord_inf[:5]:
                ref = inf.get("ref_code", "???")
                inf_type = inf.get("infraction_type", "unknown")
                reason = truncate_text(inf.get("reason", "No reason"), 60)
                created = format_timestamp(inf.get("created_at", 0), "R")
                status = inf.get("status", "unknown")
                
                status_emoji = "🔴" if status == "active" else "⚪"
                
                discord_lines.append(
                    f"{status_emoji} **`{ref}`** - {inf_type.title()} | {created}\n"
                    f"   {reason}"
                )
            
            embed.add_field(
                name=f"💬 Discord Infractions ({len(discord_inf)})",
                value="\n\n".join(discord_lines) if discord_lines else "*None*",
                inline=False
            )
        
        # MC infractions
        if mc_inf:
            mc_lines = []
            for inf in mc_inf[:5]:
                ref = inf.get("ref_code", "???")
                inf_type = inf.get("infraction_type", "unknown")
                reason = truncate_text(inf.get("reason", "No reason"), 60)
                created = format_timestamp(inf.get("created_at", 0), "R")
                status = inf.get("status", "unknown")
                
                status_emoji = "🔴" if status == "active" else "⚪"
                
                mc_lines.append(
                    f"{status_emoji} **`{ref}`** - {inf_type.title()} | {created}\n"
                    f"   {reason}"
                )
            
            embed.add_field(
                name=f"🚒 MissionChief Infractions ({len(mc_inf)})",
                value="\n\n".join(mc_lines) if mc_lines else "*None*",
                inline=False
            )
        
        # Summary
        active_count = len([i for i in infractions if i.get("status") == "active"])
        embed.set_footer(text=f"Total: {len(infractions)} | Active: {active_count}")
        
        return embed
    
    async def get_events_embed(self) -> discord.Embed:
        """Build the events embed."""
        data = self.member_data
        
        embed = discord.Embed(
            title=f"📌 Events - {data.get_display_name()}",
            color=discord.Color.purple()
        )
        
        # Fetch events
        events = await self.db.get_events(
            discord_id=data.discord_id,
            mc_user_id=data.mc_user_id,
            limit=10
        )
        
        if not events:
            embed.description = "*No events found for this member.*"
            return embed
        
        event_lines = []
        for event in events:
            event_type = event.get("event_type", "unknown")
            timestamp = format_timestamp(event.get("timestamp", 0), "R")
            triggered = event.get("triggered_by", "unknown")
            
            # Get emoji for event type
            emoji_map = {
                "joined_discord": "📥",
                "left_discord": "📤",
                "joined_mc": "🚒",
                "left_mc": "🚪",
                "link_created": "🔗",
                "role_changed": "👔",
                "contribution_drop": "📉"
            }
            emoji = emoji_map.get(event_type, "📌")
            
            event_lines.append(
                f"{emoji} **{event_type.replace('_', ' ').title()}** | {timestamp}\n"
                f"   Triggered by: {triggered}"
            )
        
        embed.description = "\n\n".join(event_lines)
        embed.set_footer(text=f"Showing {len(events)} most recent events")
        
        return embed
    
    async def _update_view(self, interaction: discord.Interaction):
        """Update the view when tabs change."""
        # Update button styles
        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.row == 0:
                # Tab buttons
                tab_name = item.custom_id.split(":")[-1]
                if tab_name == self.current_tab:
                    item.style = discord.ButtonStyle.primary
                else:
                    item.style = discord.ButtonStyle.secondary
        
        # Get appropriate embed
        if self.current_tab == "overview":
            embed = await self.get_overview_embed()
        elif self.current_tab == "notes":
            embed = await self.get_notes_embed()
        elif self.current_tab == "infractions":
            embed = await self.get_infractions_embed()
        elif self.current_tab == "events":
            embed = await self.get_events_embed()
        else:
            embed = await self.get_overview_embed()
        
        await interaction.response.edit_message(embed=embed, view=self)


class AddNoteModal(discord.ui.Modal, title="Add Note"):
    """Modal for adding a note to a member."""
    
    note_text = discord.ui.TextInput(
        label="Note Text",
        style=discord.TextStyle.paragraph,
        placeholder="Enter your note here...",
        required=True,
        max_length=2000
    )
    
    infraction_ref = discord.ui.TextInput(
        label="Link to Infraction (optional)",
        style=discord.TextStyle.short,
        placeholder="e.g., INF-DC-2025-000123",
        required=False,
        max_length=50
    )
    
    expires_days = discord.ui.TextInput(
        label="Expires after (days, optional)",
        style=discord.TextStyle.short,
        placeholder="Leave empty for no expiry",
        required=False,
        max_length=3
    )
    
    def __init__(self, parent_view: MemberOverviewView):
        super().__init__()
        self.parent_view = parent_view
    
    async def on_submit(self, interaction: discord.Interaction):
        """Handle note submission."""
        try:
            # Parse expiry days
            expires_days = None
            if self.expires_days.value:
                try:
                    expires_days = int(self.expires_days.value)
                except ValueError:
                    await interaction.response.send_message(
                        "❌ Invalid expiry days. Must be a number.",
                        ephemeral=True
                    )
                    return
            
            # Add note to database
            ref_code = await self.parent_view.db.add_note(
                guild_id=interaction.guild.id,
                discord_id=self.parent_view.member_data.discord_id,
                mc_user_id=self.parent_view.member_data.mc_user_id,
                note_text=self.note_text.value,
                author_id=interaction.user.id,
                author_name=str(interaction.user),
                infraction_ref=self.infraction_ref.value or None,
                expires_days=expires_days
            )
            
            # Update member data
            self.parent_view.member_data.notes_count += 1
            
            # Switch to notes tab
            self.parent_view.current_tab = "notes"
            
            await interaction.response.send_message(
                f"✅ Note added successfully! Reference: `{ref_code}`",
                ephemeral=True
            )
            
            # Refresh the view
            embed = await self.parent_view.get_notes_embed()
            
            # Update button styles
            for item in self.parent_view.children:
                if isinstance(item, discord.ui.Button) and item.row == 0:
                    tab_name = item.custom_id.split(":")[-1]
                    if tab_name == "notes":
                        item.style = discord.ButtonStyle.primary
                    else:
                        item.style = discord.ButtonStyle.secondary
            
            if self.parent_view.message:
                await self.parent_view.message.edit(embed=embed, view=self.parent_view)
        
        except Exception as e:
            log.error(f"Failed to add note: {e}", exc_info=True)
            await interaction.response.send_message(
                f"❌ Failed to add note: {str(e)}",
                ephemeral=True
            )


class EditNoteModal(discord.ui.Modal, title="Edit Note"):
    """Modal for editing an existing note."""
    
    ref_code = discord.ui.TextInput(
        label="Note Reference Code",
        style=discord.TextStyle.short,
        placeholder="e.g., N2025-000123",
        required=True,
        max_length=50
    )
    
    new_text = discord.ui.TextInput(
        label="New Note Text",
        style=discord.TextStyle.paragraph,
        placeholder="Enter the updated note text...",
        required=True,
        max_length=2000
    )
    
    def __init__(self, parent_view: MemberOverviewView):
        super().__init__()
        self.parent_view = parent_view
    
    async def on_submit(self, interaction: discord.Interaction):
        """Handle note edit submission."""
        try:
            # Update note in database
            success = await self.parent_view.db.update_note(
                ref_code=self.ref_code.value,
                new_text=self.new_text.value,
                updated_by=interaction.user.id
            )
            
            if not success:
                await interaction.response.send_message(
                    f"❌ Note `{self.ref_code.value}` not found.",
                    ephemeral=True
                )
                return
            
            await interaction.response.send_message(
                f"✅ Note `{self.ref_code.value}` updated successfully!",
                ephemeral=True
            )
            
            # Refresh the notes view
            self.parent_view.current_tab = "notes"
            embed = await self.parent_view.get_notes_embed()
            
            if self.parent_view.message:
                await self.parent_view.message.edit(embed=embed, view=self.parent_view)
        
        except Exception as e:
            log.error(f"Failed to edit note: {e}", exc_info=True)
            await interaction.response.send_message(
                f"❌ Failed to edit note: {str(e)}",
                ephemeral=True
            )


class DeleteNoteModal(discord.ui.Modal, title="Delete Note"):
    """Modal for deleting a note."""
    
    ref_code = discord.ui.TextInput(
        label="Note Reference Code",
        style=discord.TextStyle.short,
        placeholder="e.g., N2025-000123",
        required=True,
        max_length=50
    )
    
    reason = discord.ui.TextInput(
        label="Reason for Deletion",
        style=discord.TextStyle.paragraph,
        placeholder="Why are you deleting this note?",
        required=True,
        max_length=500
    )
    
    def __init__(self, parent_view: MemberOverviewView):
        super().__init__()
        self.parent_view = parent_view
    
    async def on_submit(self, interaction: discord.Interaction):
        """Handle note deletion."""
        try:
            # Check if note exists first
            notes = await self.parent_view.db.get_notes(ref_code=self.ref_code.value)
            
            if not notes:
                await interaction.response.send_message(
                    f"❌ Note `{self.ref_code.value}` not found.",
                    ephemeral=True
                )
                return
            
            # Delete note
            success = await self.parent_view.db.delete_note(self.ref_code.value)
            
            if success:
                # Log the deletion as an event
                await self.parent_view.db.add_event(
                    guild_id=interaction.guild.id,
                    discord_id=self.parent_view.member_data.discord_id,
                    mc_user_id=self.parent_view.member_data.mc_user_id,
                    event_type="note_deleted",
                    event_data={
                        "ref_code": self.ref_code.value,
                        "reason": self.reason.value
                    },
                    triggered_by="admin",
                    actor_id=interaction.user.id
                )
                
                # Update count
                self.parent_view.member_data.notes_count -= 1
                
                await interaction.response.send_message(
                    f"✅ Note `{self.ref_code.value}` deleted successfully!",
                    ephemeral=True
                )
                
                # Refresh the notes view
                self.parent_view.current_tab = "notes"
                embed = await self.parent_view.get_notes_embed()
                
                if self.parent_view.message:
                    await self.parent_view.message.edit(embed=embed, view=self.parent_view)
            else:
                await interaction.response.send_message(
                    f"❌ Failed to delete note `{self.ref_code.value}`.",
                    ephemeral=True
                )
        
        except Exception as e:
            log.error(f"Failed to delete note: {e}", exc_info=True)
            await interaction.response.send_message(
                f"❌ Failed to delete note: {str(e)}",
                ephemeral=True
            )
