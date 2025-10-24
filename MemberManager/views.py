"""
Discord UI Views for MemberManager
Tab-based interface with buttons for navigation

🔧 FIXED:
- Multi-user access prevention (interaction_check)
- Audit log tab showing all edits
- Note editing shows editor name
- Better error handling
- updated_by_name parameter included
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
    
    Tabs: Overview | Notes | Infractions | Events | Audit
    """
    
    def __init__(
        self,
        bot,
        db,
        config,
        member_data: MemberData,
        integrations: Dict[str, Any],
        invoker_id: int  # 🔧 NEW: Track who can use this view
    ):
        super().__init__(timeout=300)  # 5 minute timeout
        
        self.bot = bot
        self.db = db
        self.config = config
        self.member_data = member_data
        self.integrations = integrations
        self.invoker_id = invoker_id  # 🔧 NEW
        
        self.current_tab = "overview"
        self.message: Optional[discord.Message] = None
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """🔧 FIX: Only allow the command invoker to use buttons."""
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message(
                "❌ This is not your member info panel. Use `[p]member` to create your own.",
                ephemeral=True
            )
            return False
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
    
    async def _update_view(self, interaction: discord.Interaction):
        """Update the view based on current tab."""
        # Get appropriate embed
        if self.current_tab == "overview":
            embed = await self.get_overview_embed()
        elif self.current_tab == "notes":
            embed = await self.get_notes_embed()
        elif self.current_tab == "infractions":
            embed = await self.get_infractions_embed()
        elif self.current_tab == "events":
            embed = await self.get_events_embed()
        elif self.current_tab == "audit":  # 🔧 NEW
            embed = await self.get_audit_embed()
        else:
            embed = await self.get_overview_embed()
        
        # Update button styles
        self._update_button_styles()
        
        try:
            await interaction.response.edit_message(embed=embed, view=self)
        except discord.errors.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=self)
    
    def _update_button_styles(self):
        """Update button styles based on current tab."""
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                if item.custom_id == f"mm:{self.current_tab}":
                    item.style = discord.ButtonStyle.primary
                elif item.custom_id and item.custom_id.startswith("mm:") and ":" in item.custom_id:
                    # Tab button
                    if item.custom_id.split(":")[1] in ["overview", "notes", "infractions", "events", "audit"]:
                        item.style = discord.ButtonStyle.secondary
    
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
    
    @discord.ui.button(
        label="Audit",
        style=discord.ButtonStyle.secondary,
        custom_id="mm:audit",
        emoji="📋",
        row=0
    )
    async def btn_audit(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        """🔧 NEW: Show audit log tab."""
        self.current_tab = "audit"
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
        if self.current_tab != "notes":
            await interaction.response.send_message(
                "⚠️ Switch to the Notes tab first to delete notes.",
                ephemeral=True
            )
            return
        
        modal = DeleteNoteModal(self)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(
        label="Refresh",
        style=discord.ButtonStyle.secondary,
        emoji="🔄",
        custom_id="mm:refresh",
        row=1
    )
    async def btn_refresh(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        """Refresh member data."""
        await interaction.response.defer()
        
        # Rebuild member data
        guild = interaction.guild
        if guild:
            # Get parent cog
            cog = self.bot.get_cog("MemberManager")
            if cog:
                self.member_data = await cog._build_member_data(
                    guild=guild,
                    discord_id=self.member_data.discord_id,
                    mc_user_id=self.member_data.mc_user_id
                )
        
        await self._update_view(interaction)
    
    # ==================== EMBED BUILDERS ====================
    
    async def get_overview_embed(self) -> discord.Embed:
        """Build the overview embed."""
        data = self.member_data
        
        # Base embed
        embed = discord.Embed(
            title=f"👤 Member Overview: {data.get_display_name()}",
            color=discord.Color.blue() if data.is_verified else discord.Color.orange()
        )
        
        # Discord Information
        discord_lines = []
        if data.has_discord():
            discord_lines.append(f"**User:** {data.discord_username}")
            discord_lines.append(f"**ID:** `{data.discord_id}`")
            
            if data.discord_joined:
                discord_lines.append(f"**Joined:** {format_timestamp(int(data.discord_joined.timestamp()), 'D')}")
            
            status_emoji = "✅" if data.is_verified else "⚠️"
            discord_lines.append(f"**Status:** {status_emoji} {'Verified' if data.is_verified else 'Not Verified'}")
            
            # 🔧 FIX: Show link status if available
            if data.link_status:
                discord_lines.append(f"**Link Status:** {data.link_status}")
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
            
            # 🔧 FIX: Better contribution display with error handling
            if data.contribution_rate is not None:
                try:
                    contrib_display = format_contribution_trend(
                        data.contribution_rate,
                        use_emoji=True
                    )
                    mc_lines.append(f"**Contribution:** {contrib_display}")
                except Exception as e:
                    log.error(f"Error formatting contribution: {e}")
                    mc_lines.append(f"**Contribution:** {data.contribution_rate}%")
            else:
                mc_lines.append("**Contribution:** *No data*")
            
            # 🔧 FIX: Status based on alliance membership
            if "Left alliance" in (data.mc_role or ""):
                mc_lines.append(f"**Status:** ❌ Not in alliance")
            elif data.mc_username and "Former member" not in data.mc_username:
                mc_lines.append(f"**Status:** ✅ Active in alliance")
            else:
                mc_lines.append(f"**Status:** ⚠️ Unknown")
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
        
        # Link status footer
        if data.is_linked():
            embed.set_footer(text="✅ Discord and MC accounts are linked • Member active in alliance")
        elif data.has_discord() and data.has_mc() and data.link_status == "approved":
            embed.set_footer(text="⚠️ Linked but not active in alliance")
        elif data.has_discord() and data.has_mc():
            embed.set_footer(text="⚠️ Accounts not linked or pending verification")
        else:
            embed.set_footer(text="❌ Incomplete information")
        
        return embed
    
    async def get_notes_embed(self) -> discord.Embed:
        """Build the notes embed."""
        data = self.member_data
        
        embed = discord.Embed(
            title=f"📝 Notes - {data.get_display_name()}",
            color=discord.Color.gold()
        )
        
        # Fetch notes
        try:
            notes = await self.db.get_notes(
                discord_id=data.discord_id,
                mc_user_id=data.mc_user_id,
                status="active",
                limit=10
            )
        except Exception as e:
            log.error(f"Error fetching notes: {e}")
            embed.description = "⚠️ Error loading notes"
            return embed
        
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
                
                # 🔧 NEW: Show if edited
                if note.get("updated_by"):
                    updated_by_name = note.get("updated_by_name", "Unknown")
                    updated_at = format_timestamp(note.get("updated_at", 0), "R")
                    pinned_lines.append(f"   ✏️ *Edited by {updated_by_name} {updated_at}*")
                
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
                regular_lines.append(f"  {text}")
                
                # 🔧 NEW: Show if edited
                if note.get("updated_by"):
                    updated_by_name = note.get("updated_by_name", "Unknown")
                    regular_lines.append(f"  ✏️ *Edited by {updated_by_name}*")
                
                regular_lines.append("")
            
            if len(notes) > 8:
                remaining = len(notes) - 8
                regular_lines.append(f"*...and {remaining} more notes*")
            
            embed.add_field(
                name="📄 Recent Notes",
                value="\n".join(regular_lines),
                inline=False
            )
        
        embed.set_footer(text=f"Total notes: {len(notes)} | Use Edit/Delete buttons to manage")
        
        return embed
    
    async def get_infractions_embed(self) -> discord.Embed:
        """Build the infractions embed."""
        data = self.member_data
        
        embed = discord.Embed(
            title=f"⚠️ Infractions - {data.get_display_name()}",
            color=discord.Color.red()
        )
        
        all_infractions = []
        
        # 🔧 FIX: Get infractions from MemberManager DB
        try:
            infractions = await self.db.get_infractions(
                discord_id=data.discord_id,
                mc_user_id=data.mc_user_id,
                status="active",
                limit=10
            )
            all_infractions.extend([(i, "mm") for i in infractions])
        except Exception as e:
            log.error(f"Error fetching infractions: {e}")
        
        # 🆕 NEW: Get sanctions from SanctionManager
        sanction_manager = self.integrations.get("sanction_manager")
        if sanction_manager:
            try:
                guild_id = data.discord_id  # Assuming we have guild context
                sanctions = sanction_manager.db.get_user_sanctions(
                    guild_id=guild_id,
                    discord_user_id=data.discord_id,
                    mc_user_id=data.mc_user_id
                )
                active_sanctions = [s for s in sanctions if s.get("status") == "active"]
                all_infractions.extend([(s, "sm") for s in active_sanctions])
            except Exception as e:
                log.error(f"Error fetching sanctions: {e}")
        
        if not all_infractions:
            embed.description = "*No active infractions for this member.*"
            embed.color = discord.Color.green()
            return embed
        
        # Sort by date (newest first)
        all_infractions.sort(key=lambda x: x[0].get("created_at", 0), reverse=True)
        
        # Group by source
        mm_infractions = [i for i, src in all_infractions if src == "mm"]
        sm_infractions = [i for i, src in all_infractions if src == "sm"]
        
        # MemberManager infractions
        if mm_infractions:
            lines = []
            for inf in mm_infractions[:5]:
                ref = inf.get("ref_code", "???")
                inf_type = inf.get("infraction_type", "Unknown")
                reason = truncate_text(inf.get("reason", ""), 80)
                created = format_timestamp(inf.get("created_at", 0), "R")
                moderator = inf.get("moderator_name", "Unknown")
                severity = inf.get("severity_score", 1)
                
                lines.append(f"• **`{ref}`** | {inf_type} | Severity: {severity}")
                lines.append(f"  {reason}")
                lines.append(f"  *By {moderator} • {created}*\n")
            
            embed.add_field(
                name="📋 MemberManager Infractions",
                value="\n".join(lines),
                inline=False
            )
        
        # SanctionManager sanctions
        if sm_infractions:
            lines = []
            for sanction in sm_infractions[:5]:
                sanction_id = sanction.get("sanction_id", "???")
                sanction_type = sanction.get("sanction_type", "Unknown")
                reason = truncate_text(sanction.get("reason_detail", ""), 80)
                created = format_timestamp(sanction.get("created_at", 0), "R")
                admin = sanction.get("admin_username", "Unknown")
                
                lines.append(f"• **Sanction #{sanction_id}** | {sanction_type}")
                lines.append(f"  {reason}")
                lines.append(f"  *By {admin} • {created}*\n")
            
            embed.add_field(
                name="🚨 Sanctions",
                value="\n".join(lines),
                inline=False
            )
        
        # Summary
        total_severity = sum(
            i[0].get("severity_score", 1) if i[1] == "mm" else 2 
            for i in all_infractions
        )
        embed.set_footer(
            text=f"Total active: {len(all_infractions)} | "
                 f"Total severity: {total_severity}"
        )
        
        return embed
    
    async def get_events_embed(self) -> discord.Embed:
        """Build the events embed."""
        data = self.member_data
        
        embed = discord.Embed(
            title=f"📅 Events - {data.get_display_name()}",
            color=discord.Color.purple()
        )
        
        try:
            events = await self.db.get_events(
                discord_id=data.discord_id,
                mc_user_id=data.mc_user_id,
                limit=10
            )
        except Exception as e:
            log.error(f"Error fetching events: {e}")
            embed.description = "⚠️ Error loading events"
            return embed
        
        if not events:
            embed.description = "*No events recorded for this member.*"
            return embed
        
        lines = []
        for event in events:
            event_type = event.get("event_type", "unknown")
            timestamp = format_timestamp(event.get("timestamp", 0), "R")
            triggered_by = event.get("triggered_by", "system")
            
            # Format event type
            event_display = event_type.replace("_", " ").title()
            
            lines.append(f"• **{event_display}** | {triggered_by} | {timestamp}")
            
            # Add notes if present
            if event.get("notes"):
                lines.append(f"  *{truncate_text(event['notes'], 100)}*")
            
            lines.append("")
        
        embed.description = "\n".join(lines)
        embed.set_footer(text=f"Total events: {len(events)}")
        
        return embed
    
    async def get_audit_embed(self) -> discord.Embed:
        """🔧 NEW: Build the audit log embed."""
        data = self.member_data
        
        embed = discord.Embed(
            title=f"📋 Audit Log - {data.get_display_name()}",
            color=discord.Color.dark_gray()
        )
        
        # 🔧 NOTE: Using member_events as audit log
        try:
            events = await self.db.get_events(
                discord_id=data.discord_id,
                mc_user_id=data.mc_user_id,
                limit=20
            )
        except Exception as e:
            log.error(f"Error fetching audit log: {e}")
            embed.description = "⚠️ Error loading audit log"
            return embed
        
        if not events:
            embed.description = "*No audit entries for this member.*"
            return embed
        
        # Filter for edit/admin actions only
        admin_actions = [
            "note_created", "note_edited", "note_deleted",
            "infraction_added", "infraction_revoked",
            "link_created", "link_approved", "link_denied",
            "role_changed"
        ]
        
        audit_events = [e for e in events if e.get("event_type") in admin_actions]
        
        if not audit_events:
            embed.description = "*No administrative actions recorded.*"
            return embed
        
        lines = []
        for entry in audit_events[:15]:
            event_type = entry.get("event_type", "unknown")
            timestamp = format_timestamp(entry.get("timestamp", 0), "R")
            triggered_by = entry.get("triggered_by", "system")
            
            # Action emoji mapping
            action_emoji = {
                "note_created": "📝",
                "note_edited": "✏️",
                "note_deleted": "🗑️",
                "infraction_added": "⚠️",
                "infraction_revoked": "✅",
                "link_created": "🔗",
                "link_approved": "✅",
                "link_denied": "❌",
                "role_changed": "👔"
            }.get(event_type, "•")
            
            action_display = event_type.replace("_", " ").title()
            
            lines.append(f"{action_emoji} **{action_display}**")
            lines.append(f"  *By {triggered_by} • {timestamp}*")
            
            # Show event data if present
            event_data = entry.get("event_data", {})
            if isinstance(event_data, dict):
                if "ref_code" in event_data:
                    lines.append(f"  📄 Ref: `{event_data['ref_code']}`")
                if "reason" in event_data:
                    lines.append(f"  💬 {truncate_text(event_data['reason'], 60)}")
            
            lines.append("")
        
        embed.description = "\n".join(lines)
        embed.set_footer(text=f"Showing last {len(audit_events)} administrative actions")
        
        return embed


# ==================== MODALS ====================

class AddNoteModal(discord.ui.Modal, title="Add Note"):
    """Modal for adding a new note."""
    
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
            
            # Log event
            await self.parent_view.db.add_event(
                guild_id=interaction.guild.id,
                discord_id=self.parent_view.member_data.discord_id,
                mc_user_id=self.parent_view.member_data.mc_user_id,
                event_type="note_created",
                event_data={"ref_code": ref_code},
                triggered_by="admin",
                actor_id=interaction.user.id
            )
            
            # Update member data
            self.parent_view.member_data.notes_count += 1
            
            # Switch to notes tab
            self.parent_view.current_tab = "notes"
            
            await interaction.response.send_message(
                f"✅ Note added successfully! Reference: `{ref_code}`",
                ephemeral=True
            )
            
            # Update view
            await self.parent_view._update_view(interaction)
            
        except Exception as e:
            log.error(f"Error adding note: {e}")
            await interaction.response.send_message(
                f"❌ Error adding note: {str(e)}",
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
        """Handle note edit."""
        try:
            # 🔧 FIXED: Update note with editor tracking
            success = await self.parent_view.db.update_note(
                ref_code=self.ref_code.value,
                new_text=self.new_text.value,
                updated_by=interaction.user.id,
                updated_by_name=str(interaction.user)
            )
            
            if success:
                # Log event
                await self.parent_view.db.add_event(
                    guild_id=interaction.guild.id,
                    discord_id=self.parent_view.member_data.discord_id,
                    mc_user_id=self.parent_view.member_data.mc_user_id,
                    event_type="note_edited",
                    event_data={"ref_code": self.ref_code.value},
                    triggered_by="admin",
                    actor_id=interaction.user.id
                )
                
                await interaction.response.send_message(
                    f"✅ Note `{self.ref_code.value}` updated successfully!",
                    ephemeral=True
                )
                
                # Update view
                self.parent_view.current_tab = "notes"
                await self.parent_view._update_view(interaction)
            else:
                await interaction.response.send_message(
                    f"❌ Note `{self.ref_code.value}` not found.",
                    ephemeral=True
                )
                
        except Exception as e:
            log.error(f"Error editing note: {e}")
            await interaction.response.send_message(
                f"❌ Error editing note: {str(e)}",
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
    
    confirm = discord.ui.TextInput(
        label="Type DELETE to confirm",
        style=discord.TextStyle.short,
        placeholder="DELETE",
        required=True,
        max_length=10
    )
    
    def __init__(self, parent_view: MemberOverviewView):
        super().__init__()
        self.parent_view = parent_view
    
    async def on_submit(self, interaction: discord.Interaction):
        """Handle note deletion."""
        if self.confirm.value.upper() != "DELETE":
            await interaction.response.send_message(
                "❌ You must type DELETE to confirm.",
                ephemeral=True
            )
            return
        
        try:
            # Delete note
            success = await self.parent_view.db.delete_note(self.ref_code.value)
            
            if success:
                # Log event
                await self.parent_view.db.add_event(
                    guild_id=interaction.guild.id,
                    discord_id=self.parent_view.member_data.discord_id,
                    mc_user_id=self.parent_view.member_data.mc_user_id,
                    event_type="note_deleted",
                    event_data={"ref_code": self.ref_code.value},
                    triggered_by="admin",
                    actor_id=interaction.user.id
                )
                
                # Update member data
                self.parent_view.member_data.notes_count -= 1
                
                await interaction.response.send_message(
                    f"✅ Note `{self.ref_code.value}` deleted successfully!",
                    ephemeral=True
                )
                
                # Update view
                self.parent_view.current_tab = "notes"
                await self.parent_view._update_view(interaction)
            else:
                await interaction.response.send_message(
                    f"❌ Note `{self.ref_code.value}` not found.",
                    ephemeral=True
                )
                
        except Exception as e:
            log.error(f"Error deleting note: {e}")
            await interaction.response.send_message(
                f"❌ Error deleting note: {str(e)}",
                ephemeral=True
            )
