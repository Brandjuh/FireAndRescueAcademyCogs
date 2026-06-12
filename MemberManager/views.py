"""
Discord UI Views for MemberManager
Tab-based interface with context-aware buttons

VERSION v2.1 - IMPROVEMENTS:
- Note text limited to 1000 characters
- Audit shows server nicknames
- Audit has search and full pagination
- Events shows alliance logs
- Search works by name and MC ID
"""

import discord
import aiosqlite
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime, timezone

from .audit import (
    BUILDING_ACTIVITY_ACTION_KEYS,
    EVENT_EMOJI,
    OPERATIONS_ACTION_KEYS,
    build_identity_filters,
    fetch_missionchief_events,
    merge_timeline_events,
)
from .models import MemberData
from .utils import (
    format_timestamp,
    format_contribution_trend,
    truncate_text,
    get_severity_emoji,
    build_mc_profile_url
)

log = logging.getLogger("red.FARA.MemberManager.views")


class MemberOverviewView(discord.ui.View):
    """
    Main view for member information with tabs.
    
    Tabs: Overview | Notes | Sanctions | Events | Audit
    """
    
    def __init__(
        self,
        bot,
        db,
        config,
        member_data: MemberData,
        integrations: Dict[str, Any],
        invoker_id: int,
        guild: discord.Guild
    ):
        super().__init__(timeout=300)
        
        self.bot = bot
        self.db = db
        self.config = config
        self.member_data = member_data
        self.integrations = integrations
        self.invoker_id = invoker_id
        self.guild = guild
        
        self.current_tab = "overview"
        self.message: Optional[discord.Message] = None
        
        # Pagination
        self.infraction_page = 0
        self.infractions_per_page = 5
        
        # 🔧 NEW: Audit pagination and search
        self.audit_page = 0
        self.audit_per_page = 10
        self.audit_search_query: Optional[str] = None
        self.overview_mode = "simple"
        
        # 🔧 NEW: Events pagination
        self.events_page = 0
        self.events_per_page = 10
        self.buildings_page = 0
        self.buildings_per_page = 10
        
        # Initialize with correct buttons
        self._rebuild_buttons()
    
    def _rebuild_buttons(self):
        """Rebuild buttons based on current tab."""
        self.clear_items()

        self.add_item(TabButton("Overview", "overview", self, row=0))
        self.add_item(TabButton("Notes", "notes", self, row=0))
        self.add_item(TabButton("Sanctions", "infractions", self, row=0))
        self.add_item(TabButton("Events", "events", self, row=0))
        self.add_item(TabButton("Audit", "audit", self, row=0))
        self.add_item(TabButton("Buildings", "buildings", self, row=1))

        if self.current_tab == "overview":
            self.add_item(ToggleOverviewModeButton(self, row=2))

        elif self.current_tab == "notes":
            self.add_item(AddNoteButton(self, row=2))
            self.add_item(ViewNoteButton(self, row=2))
            self.add_item(TogglePinNoteButton(self, row=2))
            self.add_item(EditNoteButton(self, row=2))
            self.add_item(DeleteNoteButton(self, row=2))

        elif self.current_tab == "infractions":
            self.add_item(AddSanctionButton(self, row=2))

            if self._has_sanctions():
                self.add_item(EditSanctionButton(self, row=2))
                self.add_item(RemoveSanctionButton(self, row=2))

            if self._has_multiple_sanctions():
                self.add_item(SearchSanctionButton(self, row=3))
                self.add_item(PreviousPageButton(self, "infraction", row=3))
                self.add_item(NextPageButton(self, "infraction", row=3))

        elif self.current_tab == "audit":
            self.add_item(SearchAuditButton(self, row=2))
            if self.audit_search_query:
                self.add_item(ClearAuditSearchButton(self, row=2))
            self.add_item(PreviousPageButton(self, "audit", row=3))
            self.add_item(NextPageButton(self, "audit", row=3))

        elif self.current_tab == "events":
            self.add_item(PreviousPageButton(self, "events", row=3))
            self.add_item(NextPageButton(self, "events", row=3))

        elif self.current_tab == "buildings":
            self.add_item(PreviousPageButton(self, "buildings", row=3))
            self.add_item(NextPageButton(self, "buildings", row=3))

        self.add_item(RefreshButton(self, row=4))
        self.add_item(CloseButton(self, row=4))

    def _has_sanctions(self) -> bool:
        """Check if member has any sanctions."""
        return self.member_data.infractions_count > 0
    
    def _has_multiple_sanctions(self) -> bool:
        """Check if pagination/search is needed."""
        return self.member_data.infractions_count > 1

    def _note_belongs_to_member(self, note: Dict[str, Any]) -> bool:
        """Return whether a note is linked to the member currently open in the panel."""
        data = self.member_data
        note_discord_id = note.get("discord_id")
        note_mc_user_id = note.get("mc_user_id")

        discord_matches = (
            data.discord_id is not None
            and note_discord_id is not None
            and str(note_discord_id) == str(data.discord_id)
        )
        mc_matches = (
            data.mc_user_id is not None
            and note_mc_user_id is not None
            and str(note_mc_user_id) == str(data.mc_user_id)
        )
        return discord_matches or mc_matches

    async def _get_member_note(self, ref_code: str) -> Optional[Dict[str, Any]]:
        """Fetch a note by reference code only if it belongs to this member."""
        notes = await self.db.get_notes(ref_code=ref_code)
        if not notes:
            return None

        note = notes[0]
        if not self._note_belongs_to_member(note):
            return None

        return note
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only allow the command invoker to use buttons."""
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
        self._rebuild_buttons()
        
        # Get appropriate embed
        if self.current_tab == "overview":
            embed = await self.get_overview_embed()
        elif self.current_tab == "notes":
            embed = await self.get_notes_embed()
        elif self.current_tab == "infractions":
            embed = await self.get_infractions_embed()
        elif self.current_tab == "events":
            embed = await self.get_events_embed()
        elif self.current_tab == "buildings":
            embed = await self.get_buildings_embed()
        elif self.current_tab == "audit":
            embed = await self.get_audit_embed()
        else:
            embed = await self.get_overview_embed()
        
        try:
            await interaction.response.edit_message(embed=embed, view=self)
        except discord.errors.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=self)
    
    async def close_view(self, interaction: discord.Interaction):
        """Close the view and cleanup."""
        for item in self.children:
            item.disabled = True
        
        try:
            await interaction.response.edit_message(
                content="✅ Member panel closed.",
                embed=None,
                view=self
            )
        except:
            try:
                await interaction.edit_original_response(
                    content="✅ Member panel closed.",
                    embed=None,
                    view=self
                )
            except:
                pass
        
        self.stop()
    
    # ==================== EMBED BUILDERS ====================
    
    async def get_overview_embed(self) -> discord.Embed:
        """Build the overview embed."""
        if getattr(self, "overview_mode", "simple") == "simple":
            return self._build_simple_overview_embed()

        data = self.member_data
        
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
            
            if data.link_status:
                discord_lines.append(f"**Link Status:** {data.link_status}")
        else:
            discord_lines.append("*No Discord information available*")
        
        embed.add_field(
            name="🎮 Discord Information",
            value="\n".join(discord_lines),
            inline=False
        )
        
        sync_lines = [
            f"**Link Status:** {data.link_status or 'none'}",
        ]
        if data.mc_user_id:
            sync_lines.append(f"**MC ID:** `{data.mc_user_id}`")
        if data.discord_id:
            sync_lines.append(f"**Discord ID:** `{data.discord_id}`")
        if data.verified_role_present is True:
            sync_lines.append("**Verified Role:** Present")
        elif data.verified_role_present is False:
            sync_lines.append("**Verified Role:** Missing")
        else:
            sync_lines.append("**Verified Role:** Unknown")
        if data.link_updated:
            sync_lines.append(f"**Updated:** {data.link_updated}")
        if data.link_reviewer_id:
            sync_lines.append(f"**Reviewer:** <@{data.link_reviewer_id}>")
        if data.member_sync_conflict:
            sync_lines.append(f"**Conflict:** {data.member_sync_conflict}")

        embed.add_field(
            name="MemberSync",
            value="\n".join(sync_lines),
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
        
        contribution_lines = [
            f"**Status:** {data.contribution_data_status}",
        ]
        if data.contribution_rate is not None:
            contribution_lines.append(f"**Current Rate:** {data.contribution_rate:.1f}%")
        else:
            contribution_lines.append("**Current Rate:** No data")
        if data.contribution_trend:
            contribution_lines.append(f"**Trend:** {data.contribution_trend}")
        if data.contribution_history:
            history = " -> ".join(f"{rate:.1f}%" for rate in data.contribution_history[:4])
            contribution_lines.append(f"**Recent History:** {history}")
        if data.contribution_snapshot_at:
            contribution_lines.append(f"**Snapshot:** {data.contribution_snapshot_at}")
        if data.contribution_snapshot_source:
            contribution_lines.append(f"**Source:** {data.contribution_snapshot_source}")
        if data.contribution_grace_status:
            contribution_lines.append(f"**Grace:** {data.contribution_grace_status}")
        if data.contribution_join_source:
            contribution_lines.append(f"**Join Source:** {data.contribution_join_source}")

        embed.add_field(
            name="Contribution",
            value="\n".join(contribution_lines),
            inline=False
        )

        # Quick Stats
        stats_lines = [
            f"**Sanctions:** {data.infractions_count} active",
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

    def _build_simple_overview_embed(self) -> discord.Embed:
        """Build a compact overview for quick member triage."""
        data = self.member_data

        embed = discord.Embed(
            title=f"👤 Member Overview: {data.get_display_name()}",
            color=discord.Color.blue() if data.is_verified else discord.Color.orange()
        )

        identity_lines = []
        if data.discord_username:
            identity_lines.append(f"**Discord:** {data.discord_username}")
        if data.discord_id:
            identity_lines.append(f"**Discord ID:** `{data.discord_id}`")
        if data.mc_username:
            identity_lines.append(f"**MissionChief:** {data.mc_username}")
        if data.mc_user_id:
            identity_lines.append(f"**MC ID:** `{data.mc_user_id}`")
        if data.mc_user_id:
            identity_lines.append(f"**Profile:** [View Profile]({build_mc_profile_url(data.mc_user_id)})")
        if not identity_lines:
            identity_lines.append("*No identity data available*")

        status_lines = [
            f"**Link:** {data.link_status or 'none'}",
            f"**Verified:** {'yes' if data.is_verified else 'no'}",
            f"**MC Role:** {data.mc_role or 'unknown'}",
        ]
        if data.member_sync_conflict:
            status_lines.append(f"**Conflict:** {data.member_sync_conflict}")

        risk_lines = [
            f"**Active sanctions:** {data.infractions_count}",
            f"**Notes:** {data.notes_count}",
            f"**Severity:** {data.severity_score}",
        ]
        if data.contribution_rate is not None:
            risk_lines.append(f"**Contribution:** {data.contribution_rate:.1f}%")
        else:
            risk_lines.append("**Contribution:** no data")

        embed.add_field(name="Identity", value="\n".join(identity_lines), inline=False)
        embed.add_field(name="Status", value="\n".join(status_lines), inline=False)
        embed.add_field(name="Risk Snapshot", value="\n".join(risk_lines), inline=False)
        embed.set_footer(text="Simple overview • Use Advanced Overview for full details")
        return embed
    
    async def get_notes_embed(self) -> discord.Embed:
        """Build the notes embed."""
        data = self.member_data
        
        embed = discord.Embed(
            title=f"📝 Notes - {data.get_display_name()}",
            color=discord.Color.gold()
        )
        
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
                
                if note.get("updated_by"):
                    updated_by_name = note.get("updated_by_name", "Unknown")
                    updated_at = format_timestamp(note.get("updated_at", 0), "R")
                    pinned_lines.append(f"   ✏️ *Edited by {updated_by_name} {updated_at}*")
                
                if note.get("infraction_ref"):
                    pinned_lines.append(f"   🔗 Linked: `{note['infraction_ref']}`")
                
                pinned_lines.append("")
            
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
        
        embed.set_footer(text=f"Total notes: {len(notes)} | Use 'View Note' to see full text • Edit/Delete to manage")
        
        return embed
    
    async def get_infractions_embed(self) -> discord.Embed:
        """Build sanctions embed with smart view."""
        data = self.member_data
        
        embed = discord.Embed(
            title=f"🚨 Sanctions - {data.get_display_name()}",
            color=discord.Color.red()
        )
        
        # Get all sanctions from SanctionManager
        all_sanctions = []
        sanction_manager = self.integrations.get("sanction_manager")
        
        if sanction_manager and self.guild:
            try:
                sanctions = sanction_manager.db.get_user_sanctions(
                    guild_id=self.guild.id,
                    discord_user_id=data.discord_id,
                    mc_user_id=data.mc_user_id
                )
                
                # Check expiry status
                now = int(datetime.now(timezone.utc).timestamp())
                thirty_days_ago = now - (30 * 86400)
                
                for sanction in sanctions:
                    is_warning = "Warning" in sanction.get("sanction_type", "")
                    created_at = sanction.get("created_at", 0)
                    status = sanction.get("status", "active")
                    
                    # Mark warnings as expired if older than 30 days
                    if is_warning and status == "active" and created_at < thirty_days_ago:
                        sanction["_display_expired"] = True
                    else:
                        sanction["_display_expired"] = False
                    
                    all_sanctions.append(sanction)
            
            except Exception as e:
                log.error(f"Error fetching sanctions: {e}")
                embed.description = "⚠️ Error loading sanctions"
                return embed
        else:
            embed.description = "*No sanctions are currently shown for this member.*"
            embed.set_footer(text="Sanction backend is not loaded")
            embed.color = discord.Color.green()
            return embed
        
        if not all_sanctions:
            embed.description = (
                "*No sanctions found for this member.*\n\n"
                "✅ Clean record!"
            )
            embed.color = discord.Color.green()
            return embed
        
        # Sort by date (newest first)
        all_sanctions.sort(key=lambda x: x.get("created_at", 0), reverse=True)
        
        # SMART VIEW LOGIC
        active_sanctions = [s for s in all_sanctions if s.get("status") == "active" and not s.get("_display_expired")]
        
        # MODE 1: Single sanction - show full details
        if len(active_sanctions) == 1:
            return self._build_single_sanction_embed(active_sanctions[0], data, all_sanctions)
        
        # MODE 2: Multiple sanctions - show list with pagination
        elif len(active_sanctions) > 1:
            return self._build_sanctions_list_embed(all_sanctions, data)
        
        # MODE 3: No active sanctions but historical ones exist
        else:
            embed.description = "✅ No active sanctions"
            embed.color = discord.Color.green()
            
            expired_count = len([s for s in all_sanctions if s.get("_display_expired")])
            removed_count = len([s for s in all_sanctions if s.get("status") != "active"])
            
            if expired_count or removed_count:
                embed.add_field(
                    name="📊 Historical Record",
                    value=(
                        f"Expired warnings: {expired_count}\n"
                        f"Removed sanctions: {removed_count}\n"
                        f"Total historical: {len(all_sanctions)}"
                    ),
                    inline=False
                )
            
            return embed
    
    def _build_single_sanction_embed(
        self,
        sanction: Dict[str, Any],
        member_data: MemberData,
        all_sanctions: List[Dict[str, Any]]
    ) -> discord.Embed:
        """Detailed view for single active sanction."""
        embed = discord.Embed(
            title=f"🚨 Active Sanction - {member_data.get_display_name()}",
            color=discord.Color.red()
        )
        
        sanction_id = sanction.get("sanction_id")
        sanction_type = sanction.get("sanction_type", "Unknown")
        reason_category = sanction.get("reason_category", "N/A")
        reason_detail = sanction.get("reason_detail", "No details")
        admin_name = sanction.get("admin_username", "Unknown")
        created_at = sanction.get("created_at", 0)
        additional_notes = sanction.get("additional_notes")
        
        # Header
        embed.description = f"**ID:** `{sanction_id}` | **Type:** {sanction_type}"
        
        # Reason
        embed.add_field(
            name="📋 Reason",
            value=f"**Category:** {reason_category}\n**Detail:** {reason_detail}",
            inline=False
        )
        
        # Admin notes (if any)
        if additional_notes:
            embed.add_field(
                name="📝 Admin Notes",
                value=truncate_text(additional_notes, 1024),
                inline=False
            )
        
        # Metadata
        metadata = (
            f"**Admin:** {admin_name}\n"
            f"**Issued:** {format_timestamp(created_at, 'F')}\n"
            f"**Age:** {format_timestamp(created_at, 'R')}"
        )
        embed.add_field(name="ℹ️ Details", value=metadata, inline=False)
        
        # Historical summary
        total = len(all_sanctions)
        active_count = len([s for s in all_sanctions if s.get("status") == "active" and not s.get("_display_expired")])
        
        embed.set_footer(text=f"Active: {active_count} • Total historical: {total} • Use buttons to manage")
        
        return embed
    
    def _build_sanctions_list_embed(
        self,
        all_sanctions: List[Dict[str, Any]],
        member_data: MemberData
    ) -> discord.Embed:
        """List view for multiple sanctions with pagination."""
        embed = discord.Embed(
            title=f"🚨 Sanctions List - {member_data.get_display_name()}",
            color=discord.Color.red()
        )
        
        # Pagination
        start_idx = self.infraction_page * self.infractions_per_page
        end_idx = start_idx + self.infractions_per_page
        page_sanctions = all_sanctions[start_idx:end_idx]
        
        lines = []
        active_count = 0
        expired_count = 0
        removed_count = 0
        
        for sanction in page_sanctions:
            sanction_id = sanction.get("sanction_id")
            sanction_type = sanction.get("sanction_type", "Unknown")
            reason = truncate_text(sanction.get("reason_detail", ""), 50)
            created = format_timestamp(sanction.get("created_at", 0), "R")
            status = sanction.get("status", "active")
            
            # Status emoji
            if status != "active":
                emoji = "⚫"
                removed_count += 1
            elif sanction.get("_display_expired"):
                emoji = "⏱️"
                expired_count += 1
            else:
                emoji = "🔴"
                active_count += 1
            
            lines.append(f"{emoji} **#{sanction_id}** | {sanction_type}")
            lines.append(f"  {reason} • {created}")
            lines.append("")
        
        embed.description = "\n".join(lines) if lines else "*No sanctions on this page*"
        
        # Footer with stats
        total_count = len(all_sanctions)
        total_pages = (total_count + self.infractions_per_page - 1) // self.infractions_per_page
        current_page = self.infraction_page + 1
        
        embed.set_footer(
            text=(
                f"Page {current_page}/{total_pages} • "
                f"🔴 Active: {active_count} • "
                f"⏱️ Expired: {expired_count} • "
                f"⚫ Removed: {removed_count}"
            )
        )
        
        return embed
    
    async def get_events_embed(self) -> discord.Embed:
        """Build the operations embed for alliance storms and large alliance missions."""
        return await self._build_filtered_logs_embed(
            title=f"?? Alliance Operations - {self.member_data.get_display_name()}",
            empty_text="No alliance storm or large alliance mission logs found for this member.",
            action_keys=OPERATIONS_ACTION_KEYS,
            page=self.events_page,
            per_page=self.events_per_page,
            footer_label="operations",
            color=discord.Color.purple(),
        )

    async def get_buildings_embed(self) -> discord.Embed:
        """Build the building and extension activity embed."""
        return await self._build_filtered_logs_embed(
            title=f"??? Buildings & Extensions - {self.member_data.get_display_name()}",
            empty_text="No building or extension logs found for this member.",
            action_keys=BUILDING_ACTIVITY_ACTION_KEYS,
            page=self.buildings_page,
            per_page=self.buildings_per_page,
            footer_label="building logs",
            color=discord.Color.dark_gold(),
        )

    async def _build_filtered_logs_embed(
        self,
        *,
        title: str,
        empty_text: str,
        action_keys: set[str],
        page: int,
        per_page: int,
        footer_label: str,
        color,
    ) -> discord.Embed:
        """Build a filtered LogsScraper embed for a specific action category."""
        embed = discord.Embed(title=title, color=color)

        try:
            logs, total_count, error = await self._fetch_member_log_rows(
                action_keys,
                page=page,
                per_page=per_page,
            )
        except Exception as e:
            log.error(f"Error fetching filtered member logs: {e}", exc_info=True)
            embed.description = f"?? Error loading logs: {str(e)}"
            return embed

        if error:
            embed.description = empty_text
            embed.set_footer(text=error)
            return embed

        if not logs:
            embed.description = f"*{empty_text}*"
            return embed

        lines = []
        for log_entry in logs:
            ts = log_entry.get("event_timestamp") or log_entry.get("ts")
            action_text = log_entry.get("action_text") or log_entry.get("action_key")
            description = log_entry.get("description")
            executed_name = log_entry.get("executed_name")
            affected_name = log_entry.get("affected_name")

            try:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                timestamp_formatted = format_timestamp(int(dt.timestamp()), "R")
            except Exception:
                timestamp_formatted = ts or "Unknown time"

            actor_info = ""
            if executed_name:
                actor_info = f" by {executed_name}"
            if affected_name and affected_name != executed_name:
                actor_info += f" -> {affected_name}"

            emoji = self._get_action_emoji(log_entry.get("action_key"))
            lines.append(f"{emoji} **{action_text}**{actor_info} | {timestamp_formatted}")
            if description:
                lines.append(f"  *{truncate_text(description, 100)}*")
            lines.append("")

        embed.description = "\n".join(lines)
        total_pages = max(1, (total_count + per_page - 1) // per_page)
        current_page = min(page + 1, total_pages)
        embed.set_footer(text=f"Page {current_page}/{total_pages} - Total {footer_label}: {total_count}")
        return embed

    def _get_action_emoji(self, action_key: str) -> str:
        """Get emoji for alliance action."""
        emoji_map = {
            "member_join": "➕",
            "member_leave": "➖",
            "member_kicked": "🚫",
            "member_banned": "⛔",
            "role_change": "👔",
            "building_constructed": "🏗️",
            "building_destroyed": "💥",
            "mission_started": "🚨",
            "contribution": "💰",
            "course_created": "📚",
            "course_completed": "✅",
        }
        return emoji_map.get(action_key, "•")
    
    async def _fetch_member_log_rows(
        self,
        action_keys: set[str],
        *,
        page: int,
        per_page: int,
    ) -> tuple[List[Dict[str, Any]], int, Optional[str]]:
        """Fetch stored LogsScraper rows for this member and action category."""
        data = self.member_data
        logs_scraper = self.integrations.get("logs_scraper")
        if not logs_scraper:
            return [], 0, "LogsScraper is not loaded."

        db_path = logs_scraper.db_path
        if not db_path.exists():
            return [], 0, f"Database not found: {db_path}"

        where_clause, params = build_identity_filters(
            mc_user_id=data.mc_user_id,
            mc_username=data.mc_username,
        )
        if not where_clause:
            return [], 0, "Cannot determine member identity for log search."

        action_key_list = sorted(action_keys)
        placeholders = ", ".join("?" for _ in action_key_list)
        offset = page * per_page
        query_params = [*params, *action_key_list]

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                f"""
                SELECT id, ts, event_timestamp, action_key, action_text,
                       executed_name, executed_mc_id, affected_name, affected_mc_id,
                       description, contribution_amount
                FROM logs
                WHERE ({where_clause}) AND action_key IN ({placeholders})
                ORDER BY COALESCE(event_timestamp, ts) DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                [*query_params, per_page, offset],
            )
            rows = [dict(row) for row in await cursor.fetchall()]

            count_cursor = await db.execute(
                f"""
                SELECT COUNT(*)
                FROM logs
                WHERE ({where_clause}) AND action_key IN ({placeholders})
                """,
                query_params,
            )
            total_count = (await count_cursor.fetchone())[0]

        return rows, total_count, None

    async def get_audit_embed(self) -> discord.Embed:
        """
        Build the audit log embed with search and pagination.
        
        🔧 NEW: Full audit log with search, pagination, and server nicknames
        """
        data = self.member_data
        
        embed = discord.Embed(
            title=f"📋 Audit Log - {data.get_display_name()}",
            color=discord.Color.dark_gray()
        )
        return await self._build_audit_timeline_embed(embed)

    async def _build_audit_timeline_embed(self, embed: discord.Embed) -> discord.Embed:
        """Build the combined audit timeline from existing stored data."""
        data = self.member_data

        try:
            member_events = await self.db.get_events(
                discord_id=data.discord_id,
                mc_user_id=data.mc_user_id,
                limit=10000,
            )

            missionchief_events = []
            logs_scraper = self.integrations.get("logs_scraper")
            if logs_scraper:
                missionchief_events = await fetch_missionchief_events(
                    logs_scraper.db_path,
                    mc_user_id=data.mc_user_id,
                    mc_username=data.mc_username,
                    limit=250,
                )

            audit_events = merge_timeline_events(
                member_events,
                missionchief_events,
                query=self.audit_search_query,
            )
        except Exception as e:
            log.error(f"Error fetching audit log: {e}", exc_info=True)
            embed.description = "⚠️ Error loading audit log"
            return embed

        if not audit_events:
            if self.audit_search_query:
                embed.description = f"*No audit entries matching '{self.audit_search_query}'*"
            else:
                embed.description = "*No audit entries for this member.*"
            return embed

        total_count = len(audit_events)
        total_pages = (total_count + self.audit_per_page - 1) // self.audit_per_page
        current_page = min(self.audit_page + 1, total_pages)

        start_idx = self.audit_page * self.audit_per_page
        end_idx = start_idx + self.audit_per_page
        page_events = audit_events[start_idx:end_idx]

        lines = []
        for entry in page_events:
            timestamp = format_timestamp(entry.timestamp, "R") if entry.timestamp else "Unknown time"
            actor_display = entry.actor_name or "system"
            if entry.actor_id and self.guild:
                member = self.guild.get_member(entry.actor_id)
                if member:
                    actor_display = member.display_name

            action_emoji = EVENT_EMOJI.get(
                entry.event_type,
                EVENT_EMOJI.get(entry.source.lower(), "•"),
            )

            lines.append(f"{action_emoji} **{entry.title}**")
            lines.append(f"  *{entry.source} • {actor_display} • {timestamp}*")

            if entry.reference:
                lines.append(f"  Ref: `{entry.reference}`")
            if entry.details:
                lines.append(f"  {truncate_text(entry.details, 120)}")

            lines.append("")

        embed.description = "\n".join(lines)

        footer_text = f"Page {current_page}/{total_pages} • Total: {total_count} entries"
        if self.audit_search_query:
            footer_text += f" • Filtered by: '{self.audit_search_query}'"

        embed.set_footer(text=footer_text)

        return embed


# ==================== BUTTON COMPONENTS ====================

class TabButton(discord.ui.Button):
    """Generic tab button."""
    
    def __init__(self, label: str, tab_name: str, parent_view: MemberOverviewView, row: int):
        style = discord.ButtonStyle.primary if parent_view.current_tab == tab_name else discord.ButtonStyle.secondary
        super().__init__(
            label=label,
            style=style,
            custom_id=f"mm:tab:{tab_name}",
            row=row
        )
        self.tab_name = tab_name
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        self.parent_view.current_tab = self.tab_name
        self.parent_view.infraction_page = 0
        self.parent_view.audit_page = 0  # Reset audit page
        self.parent_view.events_page = 0  # Reset events page
        self.parent_view.buildings_page = 0
        await self.parent_view._update_view(interaction)


class CloseButton(discord.ui.Button):
    """Close button."""
    
    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(
            label="Close",
            style=discord.ButtonStyle.danger,
            emoji="❌",
            custom_id="mm:close",
            row=row
        )
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        await self.parent_view.close_view(interaction)


class RefreshButton(discord.ui.Button):
    """Refresh button."""
    
    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(
            label="Refresh",
            style=discord.ButtonStyle.secondary,
            emoji="🔄",
            custom_id="mm:refresh",
            row=row
        )
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        guild = interaction.guild
        if guild:
            cog = self.parent_view.bot.get_cog("MemberManager")
            if cog:
                self.parent_view.member_data = await cog._build_member_data(
                    guild=guild,
                    discord_id=self.parent_view.member_data.discord_id,
                    mc_user_id=self.parent_view.member_data.mc_user_id
                )
        
        await self.parent_view._update_view(interaction)


class ToggleOverviewModeButton(discord.ui.Button):
    """Toggle between simple and advanced overview modes."""

    def __init__(self, parent_view: MemberOverviewView, row: int):
        next_mode = "Advanced" if parent_view.overview_mode == "simple" else "Simple"
        super().__init__(
            label=f"{next_mode} Overview",
            style=discord.ButtonStyle.secondary,
            custom_id="mm:toggle_overview_mode",
            row=row
        )
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        self.parent_view.overview_mode = (
            "advanced" if self.parent_view.overview_mode == "simple" else "simple"
        )
        await self.parent_view._update_view(interaction)


# ==================== NOTES BUTTONS ====================

class AddNoteButton(discord.ui.Button):
    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(label="Add Note", style=discord.ButtonStyle.success, emoji="📝", row=row)
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        modal = AddNoteModal(self.parent_view)
        await interaction.response.send_modal(modal)


class EditNoteButton(discord.ui.Button):
    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(label="Edit Note", style=discord.ButtonStyle.secondary, emoji="✏️", row=row)
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        modal = EditNoteModal(self.parent_view)
        await interaction.response.send_modal(modal)


class DeleteNoteButton(discord.ui.Button):
    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(label="Delete Note", style=discord.ButtonStyle.danger, emoji="🗑️", row=row)
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        modal = DeleteNoteModal(self.parent_view)
        await interaction.response.send_modal(modal)


class TogglePinNoteButton(discord.ui.Button):
    """Pin or unpin a note button."""

    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(
            label="Pin/Unpin",
            style=discord.ButtonStyle.secondary,
            emoji="📌",
            row=row
        )
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        modal = TogglePinNoteModal(self.parent_view)
        await interaction.response.send_modal(modal)


class ViewNoteButton(discord.ui.Button):
    """View full note button."""
    
    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(
            label="View Note",
            style=discord.ButtonStyle.primary,
            emoji="👁️",
            custom_id="mm:view_note",
            row=row
        )
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        modal = ViewNoteModal(self.parent_view)
        await interaction.response.send_modal(modal)


# ==================== SANCTION BUTTONS ====================

class AddSanctionButton(discord.ui.Button):
    """Add sanction button."""
    
    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(
            label="Add Sanction",
            style=discord.ButtonStyle.danger,
            emoji="🚨",
            custom_id="mm:add_sanction",
            row=row
        )
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        modal = CreateSanctionModal(self.parent_view)
        await interaction.response.send_modal(modal)


class EditSanctionButton(discord.ui.Button):
    """Edit sanction button."""
    
    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(
            label="Edit Sanction",
            style=discord.ButtonStyle.secondary,
            emoji="✏️",
            custom_id="mm:edit_sanction",
            row=row
        )
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        modal = EditSanctionModal(self.parent_view)
        await interaction.response.send_modal(modal)


class RemoveSanctionButton(discord.ui.Button):
    """Remove sanction button."""
    
    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(
            label="Remove Sanction",
            style=discord.ButtonStyle.danger,
            emoji="🗑️",
            custom_id="mm:remove_sanction",
            row=row
        )
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        modal = RemoveSanctionModal(self.parent_view)
        await interaction.response.send_modal(modal)


class SearchSanctionButton(discord.ui.Button):
    """Search sanction by ID button."""
    
    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(
            label="Search ID",
            style=discord.ButtonStyle.primary,
            emoji="🔍",
            custom_id="mm:search_sanction",
            row=row
        )
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        modal = SearchSanctionModal(self.parent_view)
        await interaction.response.send_modal(modal)


# 🔧 NEW: Audit buttons
class SearchAuditButton(discord.ui.Button):
    """Search audit log button."""
    
    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(
            label="Search Audit",
            style=discord.ButtonStyle.primary,
            emoji="🔍",
            custom_id="mm:search_audit",
            row=row
        )
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        modal = SearchAuditModal(self.parent_view)
        await interaction.response.send_modal(modal)


class ClearAuditSearchButton(discord.ui.Button):
    """Clear audit search button."""
    
    def __init__(self, parent_view: MemberOverviewView, row: int):
        super().__init__(
            label="Clear Search",
            style=discord.ButtonStyle.secondary,
            emoji="🗑️",
            custom_id="mm:clear_audit_search",
            row=row
        )
        self.parent_view = parent_view
    
    async def callback(self, interaction: discord.Interaction):
        self.parent_view.audit_search_query = None
        self.parent_view.audit_page = 0
        await self.parent_view._update_view(interaction)


# ==================== PAGINATION BUTTONS ====================

class PreviousPageButton(discord.ui.Button):
    """Previous page button."""
    
    def __init__(self, parent_view: MemberOverviewView, page_type: str, row: int):
        # Determine if disabled based on page type
        if page_type == "infraction":
            disabled = parent_view.infraction_page == 0
        elif page_type == "audit":
            disabled = parent_view.audit_page == 0
        elif page_type == "buildings":
            disabled = parent_view.buildings_page == 0
        else:  # events
            disabled = parent_view.events_page == 0
        
        super().__init__(
            label="◀ Previous",
            style=discord.ButtonStyle.secondary,
            custom_id=f"mm:prev_page:{page_type}",
            row=row,
            disabled=disabled
        )
        self.parent_view = parent_view
        self.page_type = page_type
    
    async def callback(self, interaction: discord.Interaction):
        if self.page_type == "infraction":
            if self.parent_view.infraction_page > 0:
                self.parent_view.infraction_page -= 1
        elif self.page_type == "audit":
            if self.parent_view.audit_page > 0:
                self.parent_view.audit_page -= 1
        elif self.page_type == "buildings":
            if self.parent_view.buildings_page > 0:
                self.parent_view.buildings_page -= 1
        else:  # events
            if self.parent_view.events_page > 0:
                self.parent_view.events_page -= 1
        
        await self.parent_view._update_view(interaction)


class NextPageButton(discord.ui.Button):
    """Next page button."""
    
    def __init__(self, parent_view: MemberOverviewView, page_type: str, row: int):
        super().__init__(
            label="Next ▶",
            style=discord.ButtonStyle.secondary,
            custom_id=f"mm:next_page:{page_type}",
            row=row
        )
        self.parent_view = parent_view
        self.page_type = page_type
    
    async def callback(self, interaction: discord.Interaction):
        if self.page_type == "infraction":
            self.parent_view.infraction_page += 1
        elif self.page_type == "audit":
            self.parent_view.audit_page += 1
        elif self.page_type == "buildings":
            self.parent_view.buildings_page += 1
        else:  # events
            self.parent_view.events_page += 1
        
        await self.parent_view._update_view(interaction)


# ==================== MODALS ====================

class AddNoteModal(discord.ui.Modal, title="Add Note"):
    """Modal for adding a new note."""
    
    # 🔧 CHANGED: max_length to 1000 instead of 2000
    note_text = discord.ui.TextInput(
        label="Note Text",
        style=discord.TextStyle.paragraph,
        placeholder="Enter your note here (max 1000 characters)...",
        required=True,
        max_length=1000  # 🔧 LIMITEER TOT 1000 TEKENS
    )
    
    infraction_ref = discord.ui.TextInput(
        label="Link to Sanction (optional)",
        style=discord.TextStyle.short,
        placeholder="e.g., 123 (sanction ID)",
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
            
            await self.parent_view.db.add_event(
                guild_id=interaction.guild.id,
                discord_id=self.parent_view.member_data.discord_id,
                mc_user_id=self.parent_view.member_data.mc_user_id,
                event_type="note_created",
                event_data={"ref_code": ref_code},
                triggered_by="admin",
                actor_id=interaction.user.id
            )
            
            self.parent_view.member_data.notes_count += 1
            self.parent_view.current_tab = "notes"
            
            await interaction.response.send_message(
                f"✅ Note added successfully! Reference: `{ref_code}`",
                ephemeral=True
            )
            
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
    
    # 🔧 CHANGED: max_length to 1000
    new_text = discord.ui.TextInput(
        label="New Note Text",
        style=discord.TextStyle.paragraph,
        placeholder="Enter the updated note text (max 1000 chars)...",
        required=True,
        max_length=1000  # 🔧 LIMITEER TOT 1000 TEKENS
    )
    
    def __init__(self, parent_view: MemberOverviewView):
        super().__init__()
        self.parent_view = parent_view
    
    async def on_submit(self, interaction: discord.Interaction):
        """Handle note edit."""
        try:
            note = await self.parent_view._get_member_note(self.ref_code.value)
            if not note:
                await interaction.response.send_message(
                    f"❌ Note `{self.ref_code.value}` not found for this member.",
                    ephemeral=True
                )
                return

            success = await self.parent_view.db.update_note(
                ref_code=self.ref_code.value,
                new_text=self.new_text.value,
                updated_by=interaction.user.id,
                updated_by_name=str(interaction.user)
            )
            
            if success:
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
            note = await self.parent_view._get_member_note(self.ref_code.value)
            if not note:
                await interaction.response.send_message(
                    f"❌ Note `{self.ref_code.value}` not found for this member.",
                    ephemeral=True
                )
                return

            success = await self.parent_view.db.delete_note(self.ref_code.value)
            
            if success:
                await self.parent_view.db.add_event(
                    guild_id=interaction.guild.id,
                    discord_id=self.parent_view.member_data.discord_id,
                    mc_user_id=self.parent_view.member_data.mc_user_id,
                    event_type="note_deleted",
                    event_data={"ref_code": self.ref_code.value},
                    triggered_by="admin",
                    actor_id=interaction.user.id
                )
                
                self.parent_view.member_data.notes_count -= 1
                
                await interaction.response.send_message(
                    f"✅ Note `{self.ref_code.value}` deleted successfully!",
                    ephemeral=True
                )
                
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


class TogglePinNoteModal(discord.ui.Modal, title="Pin or Unpin Note"):
    """Modal for pinning or unpinning a note."""

    ref_code = discord.ui.TextInput(
        label="Note Reference Code",
        style=discord.TextStyle.short,
        placeholder="e.g., N2025-000123",
        required=True,
        max_length=50
    )

    action = discord.ui.TextInput(
        label="Action",
        style=discord.TextStyle.short,
        placeholder="PIN or UNPIN",
        required=True,
        max_length=10
    )

    def __init__(self, parent_view: MemberOverviewView):
        super().__init__()
        self.parent_view = parent_view

    async def on_submit(self, interaction: discord.Interaction):
        """Handle note pin or unpin."""
        action = self.action.value.strip().upper()
        if action not in {"PIN", "UNPIN"}:
            await interaction.response.send_message(
                "Action must be PIN or UNPIN.",
                ephemeral=True
            )
            return

        try:
            note = await self.parent_view._get_member_note(self.ref_code.value)
            if not note:
                await interaction.response.send_message(
                    f"Note `{self.ref_code.value}` not found for this member.",
                    ephemeral=True
                )
                return

            pinned = action == "PIN"
            success = await self.parent_view.db.pin_note(self.ref_code.value, pinned=pinned)

            if success:
                event_type = "note_pinned" if pinned else "note_unpinned"
                await self.parent_view.db.add_event(
                    guild_id=interaction.guild.id,
                    discord_id=self.parent_view.member_data.discord_id,
                    mc_user_id=self.parent_view.member_data.mc_user_id,
                    event_type=event_type,
                    event_data={"ref_code": self.ref_code.value},
                    triggered_by="admin",
                    actor_id=interaction.user.id
                )

                await interaction.response.send_message(
                    f"Note `{self.ref_code.value}` {'pinned' if pinned else 'unpinned'} successfully!",
                    ephemeral=True
                )

                self.parent_view.current_tab = "notes"
                await self.parent_view._update_view(interaction)
            else:
                await interaction.response.send_message(
                    f"Note `{self.ref_code.value}` not found.",
                    ephemeral=True
                )

        except Exception as e:
            log.error(f"Error pinning note: {e}", exc_info=True)
            await interaction.response.send_message(
                f"Error pinning note: {str(e)}",
                ephemeral=True
            )

class ViewNoteModal(discord.ui.Modal, title="View Full Note"):
    """Modal to view the full text of a note."""
    
    ref_code = discord.ui.TextInput(
        label="Note Reference Code",
        style=discord.TextStyle.short,
        placeholder="e.g., N2025-000123",
        required=True,
        max_length=50
    )
    
    def __init__(self, parent_view: MemberOverviewView):
        super().__init__()
        self.parent_view = parent_view
    
    async def on_submit(self, interaction: discord.Interaction):
        """Display the full note."""
        try:
            note = await self.parent_view._get_member_note(self.ref_code.value)

            if not note:
                await interaction.response.send_message(
                    f"Note `{self.ref_code.value}` not found for this member.",
                    ephemeral=True
                )
                return
            
            # Build detailed embed
            embed = discord.Embed(
                title=f"📝 Full Note: {self.ref_code.value}",
                color=discord.Color.gold()
            )
            
            # Note text (full, no truncation)
            note_text = note.get("note_text", "")
            char_count = len(note_text)
            
            embed.description = f"{note_text}\n\n*({char_count} characters)*"
            
            # Author info
            author_name = note.get("author_name", "Unknown")
            created_at = note.get("created_at", 0)
            
            author_info = (
                f"**Author:** {author_name}\n"
                f"**Created:** {format_timestamp(created_at, 'F')}\n"
                f"**Age:** {format_timestamp(created_at, 'R')}"
            )
            
            # Check if edited
            if note.get("updated_by"):
                updated_by_name = note.get("updated_by_name", "Unknown")
                updated_at = note.get("updated_at", 0)
                author_info += f"\n**Last edited by:** {updated_by_name}\n"
                author_info += f"**Edited:** {format_timestamp(updated_at, 'R')}"
            
            embed.add_field(
                name="ℹ️ Note Information",
                value=author_info,
                inline=False
            )
            
            # Status
            status = note.get("status", "active")
            is_pinned = note.get("is_pinned", False)
            
            status_info = []
            if status == "active":
                status_info.append("**Status:** 🟢 Active")
            else:
                status_info.append(f"**Status:** ⚫ {status.title()}")
            
            if is_pinned:
                status_info.append("**Pinned:** 📌 Yes")
            
            if note.get("expires_at"):
                expires_at = note["expires_at"]
                status_info.append(f"**Expires:** {format_timestamp(expires_at, 'R')}")
            
            if note.get("infraction_ref"):
                status_info.append(f"**Linked to:** `{note['infraction_ref']}`")
            
            if status_info:
                embed.add_field(
                    name="📊 Status",
                    value="\n".join(status_info),
                    inline=False
                )
            
            # Tags
            if note.get("tags"):
                try:
                    import json
                    tags = json.loads(note["tags"]) if isinstance(note["tags"], str) else note["tags"]
                    if tags:
                        tag_str = ", ".join(f"`{tag}`" for tag in tags)
                        embed.add_field(
                            name="🏷️ Tags",
                            value=tag_str,
                            inline=False
                        )
                except:
                    pass
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            log.error(f"Error viewing note: {e}", exc_info=True)
            await interaction.response.send_message(
                f"❌ Error viewing note: {str(e)}",
                ephemeral=True
            )


# ==================== SANCTION MODALS ====================

class CreateSanctionModal(discord.ui.Modal, title="Create Sanction"):
    """Modal to create a new sanction."""
    
    sanction_type = discord.ui.TextInput(
        label="Sanction Type",
        style=discord.TextStyle.short,
        placeholder="Warning - Official 1st warning",
        required=True,
        max_length=100
    )
    
    reason_category = discord.ui.TextInput(
        label="Reason Category",
        style=discord.TextStyle.short,
        placeholder="Member Conduct",
        required=True,
        max_length=100
    )
    
    reason_detail = discord.ui.TextInput(
        label="Reason Detail",
        style=discord.TextStyle.paragraph,
        placeholder="1.3. No respect - ...",
        required=True,
        max_length=500
    )
    
    admin_notes = discord.ui.TextInput(
        label="Admin Notes (Optional)",
        style=discord.TextStyle.paragraph,
        placeholder="Internal notes (not visible to member)",
        required=False,
        max_length=1000
    )
    
    def __init__(self, parent_view: MemberOverviewView):
        super().__init__()
        self.parent_view = parent_view
    
    async def on_submit(self, interaction: discord.Interaction):
        """Handle sanction creation."""
        try:
            sanction_manager = self.parent_view.integrations.get("sanction_manager")
            if not sanction_manager:
                await interaction.response.send_message(
                    "❌ SanctionManager not available",
                    ephemeral=True
                )
                return
            
            data = self.parent_view.member_data
            
            sanction_id = sanction_manager.db.add_sanction(
                guild_id=interaction.guild.id,
                discord_user_id=data.discord_id,
                mc_user_id=data.mc_user_id,
                mc_username=data.mc_username or data.discord_username or "Unknown",
                admin_user_id=interaction.user.id,
                admin_username=str(interaction.user),
                sanction_type=self.sanction_type.value,
                reason_category=self.reason_category.value,
                reason_detail=self.reason_detail.value,
                additional_notes=self.admin_notes.value or None
            )
            
            await self.parent_view.db.add_event(
                guild_id=interaction.guild.id,
                discord_id=data.discord_id,
                mc_user_id=data.mc_user_id,
                event_type="sanction_added",
                event_data={
                    "sanction_id": sanction_id,
                    "sanction_type": self.sanction_type.value
                },
                triggered_by="admin",
                actor_id=interaction.user.id
            )
            
            await interaction.response.send_message(
                f"✅ Sanction created successfully!\n"
                f"**ID:** `{sanction_id}`\n"
                f"**Type:** {self.sanction_type.value}",
                ephemeral=True
            )
            
            self.parent_view.member_data.infractions_count += 1
            await self.parent_view._update_view(interaction)
            
        except Exception as e:
            log.error(f"Error creating sanction: {e}", exc_info=True)
            await interaction.response.send_message(
                f"❌ Error creating sanction: {str(e)}",
                ephemeral=True
            )


class EditSanctionModal(discord.ui.Modal, title="Edit Sanction"):
    """Modal to edit an existing sanction."""
    
    sanction_id = discord.ui.TextInput(
        label="Sanction ID",
        style=discord.TextStyle.short,
        placeholder="123",
        required=True,
        max_length=10
    )
    
    new_reason = discord.ui.TextInput(
        label="New Reason Detail",
        style=discord.TextStyle.paragraph,
        placeholder="Enter updated reason...",
        required=False,
        max_length=500
    )
    
    new_notes = discord.ui.TextInput(
        label="New Admin Notes",
        style=discord.TextStyle.paragraph,
        placeholder="Updated internal notes...",
        required=False,
        max_length=1000
    )
    
    def __init__(self, parent_view: MemberOverviewView):
        super().__init__()
        self.parent_view = parent_view
    
    async def on_submit(self, interaction: discord.Interaction):
        """Handle sanction edit."""
        try:
            sanction_manager = self.parent_view.integrations.get("sanction_manager")
            if not sanction_manager:
                await interaction.response.send_message(
                    "❌ SanctionManager not available",
                    ephemeral=True
                )
                return
            
            try:
                sid = int(self.sanction_id.value)
            except ValueError:
                await interaction.response.send_message(
                    "❌ Invalid sanction ID",
                    ephemeral=True
                )
                return
            
            sanction = sanction_manager.db.get_sanction(sid)
            if not sanction or sanction['guild_id'] != interaction.guild.id:
                await interaction.response.send_message(
                    f"❌ Sanction #{sid} not found",
                    ephemeral=True
                )
                return
            
            updates = {}
            if self.new_reason.value:
                updates['reason_detail'] = self.new_reason.value
            if self.new_notes.value:
                updates['additional_notes'] = self.new_notes.value
            
            if not updates:
                await interaction.response.send_message(
                    "❌ No changes specified",
                    ephemeral=True
                )
                return
            
            sanction_manager.db.edit_sanction(sid, interaction.user.id, **updates)
            
            await self.parent_view.db.add_event(
                guild_id=interaction.guild.id,
                discord_id=self.parent_view.member_data.discord_id,
                mc_user_id=self.parent_view.member_data.mc_user_id,
                event_type="sanction_edited",
                event_data={"sanction_id": sid},
                triggered_by="admin",
                actor_id=interaction.user.id
            )
            
            await interaction.response.send_message(
                f"✅ Sanction #{sid} updated successfully!",
                ephemeral=True
            )
            
            await self.parent_view._update_view(interaction)
            
        except Exception as e:
            log.error(f"Error editing sanction: {e}", exc_info=True)
            await interaction.response.send_message(
                f"❌ Error editing sanction: {str(e)}",
                ephemeral=True
            )


class RemoveSanctionModal(discord.ui.Modal, title="Remove Sanction"):
    """Modal to remove a sanction."""
    
    sanction_id = discord.ui.TextInput(
        label="Sanction ID",
        style=discord.TextStyle.short,
        placeholder="123",
        required=True,
        max_length=10
    )
    
    reason = discord.ui.TextInput(
        label="Removal Reason",
        style=discord.TextStyle.paragraph,
        placeholder="Why is this sanction being removed?",
        required=True,
        max_length=500
    )
    
    confirm = discord.ui.TextInput(
        label="Type REMOVE to confirm",
        style=discord.TextStyle.short,
        placeholder="REMOVE",
        required=True,
        max_length=10
    )
    
    def __init__(self, parent_view: MemberOverviewView):
        super().__init__()
        self.parent_view = parent_view
    
    async def on_submit(self, interaction: discord.Interaction):
        """Handle sanction removal."""
        if self.confirm.value.upper() != "REMOVE":
            await interaction.response.send_message(
                "❌ You must type REMOVE to confirm",
                ephemeral=True
            )
            return
        
        try:
            sanction_manager = self.parent_view.integrations.get("sanction_manager")
            if not sanction_manager:
                await interaction.response.send_message(
                    "❌ SanctionManager not available",
                    ephemeral=True
                )
                return
            
            try:
                sid = int(self.sanction_id.value)
            except ValueError:
                await interaction.response.send_message(
                    "❌ Invalid sanction ID",
                    ephemeral=True
                )
                return
            
            sanction = sanction_manager.db.get_sanction(sid)
            if not sanction or sanction['guild_id'] != interaction.guild.id:
                await interaction.response.send_message(
                    f"❌ Sanction #{sid} not found",
                    ephemeral=True
                )
                return
            
            sanction_manager.db.update_sanction_status(
                sid,
                'removed',
                interaction.user.id,
                f"Removed by {interaction.user}: {self.reason.value}"
            )
            
            await self.parent_view.db.add_event(
                guild_id=interaction.guild.id,
                discord_id=self.parent_view.member_data.discord_id,
                mc_user_id=self.parent_view.member_data.mc_user_id,
                event_type="sanction_removed",
                event_data={
                    "sanction_id": sid,
                    "reason": self.reason.value
                },
                triggered_by="admin",
                actor_id=interaction.user.id
            )
            
            await interaction.response.send_message(
                f"✅ Sanction #{sid} removed successfully!",
                ephemeral=True
            )
            
            self.parent_view.member_data.infractions_count -= 1
            await self.parent_view._update_view(interaction)
            
        except Exception as e:
            log.error(f"Error removing sanction: {e}", exc_info=True)
            await interaction.response.send_message(
                f"❌ Error removing sanction: {str(e)}",
                ephemeral=True
            )


class SearchSanctionModal(discord.ui.Modal, title="Search Sanction by ID"):
    """Modal to search and view a specific sanction."""
    
    sanction_id = discord.ui.TextInput(
        label="Sanction ID",
        style=discord.TextStyle.short,
        placeholder="123",
        required=True,
        max_length=10
    )
    
    def __init__(self, parent_view: MemberOverviewView):
        super().__init__()
        self.parent_view = parent_view
    
    async def on_submit(self, interaction: discord.Interaction):
        """Handle sanction search."""
        try:
            sanction_manager = self.parent_view.integrations.get("sanction_manager")
            if not sanction_manager:
                await interaction.response.send_message(
                    "❌ SanctionManager not available",
                    ephemeral=True
                )
                return
            
            try:
                sid = int(self.sanction_id.value)
            except ValueError:
                await interaction.response.send_message(
                    "❌ Invalid sanction ID",
                    ephemeral=True
                )
                return
            
            sanction = sanction_manager.db.get_sanction(sid)
            if not sanction or sanction['guild_id'] != interaction.guild.id:
                await interaction.response.send_message(
                    f"❌ Sanction #{sid} not found",
                    ephemeral=True
                )
                return
            
            # Build detailed embed
            embed = discord.Embed(
                title=f"🔍 Sanction Details - #{sid}",
                color=discord.Color.blue()
            )
            
            status = sanction.get("status", "active")
            sanction_type = sanction.get("sanction_type", "Unknown")
            reason_category = sanction.get("reason_category", "N/A")
            reason_detail = sanction.get("reason_detail", "No details")
            admin_name = sanction.get("admin_username", "Unknown")
            created_at = sanction.get("created_at", 0)
            
            # Status indicator
            if status == "active":
                status_emoji = "🔴"
            elif status == "removed":
                status_emoji = "⚫"
            else:
                status_emoji = "⏱️"
            
            embed.description = f"{status_emoji} **Status:** {status.title()}\n**Type:** {sanction_type}"
            
            # Member info
            mc_username = sanction.get("mc_username", "Unknown")
            discord_id = sanction.get("discord_user_id")
            
            member_info = f"**MC Name:** {mc_username}"
            if discord_id:
                member_info += f"\n**Discord:** <@{discord_id}>"
            
            embed.add_field(name="👤 Member", value=member_info, inline=False)
            
            # Reason
            embed.add_field(
                name="📋 Reason",
                value=f"**Category:** {reason_category}\n**Detail:** {reason_detail}",
                inline=False
            )
            
            # Admin notes
            if sanction.get("additional_notes"):
                embed.add_field(
                    name="📝 Admin Notes",
                    value=truncate_text(sanction["additional_notes"], 1024),
                    inline=False
                )
            
            # Metadata
            metadata = (
                f"**Admin:** {admin_name}\n"
                f"**Issued:** {format_timestamp(created_at, 'F')}"
            )
            
            if sanction.get("edited_at"):
                edited_at = sanction["edited_at"]
                metadata += f"\n**Last edited:** {format_timestamp(edited_at, 'R')}"
            
            if status == "removed":
                revoke_reason = sanction.get("revoke_reason", "No reason")
                metadata += f"\n**Removal reason:** {truncate_text(revoke_reason, 100)}"
            
            embed.add_field(name="ℹ️ Details", value=metadata, inline=False)
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            
        except Exception as e:
            log.error(f"Error searching sanction: {e}", exc_info=True)
            await interaction.response.send_message(
                f"❌ Error searching sanction: {str(e)}",
                ephemeral=True
            )


# 🔧 NEW: Audit search modal
class SearchAuditModal(discord.ui.Modal, title="Search Audit Log"):
    """Modal to search audit log."""
    
    query = discord.ui.TextInput(
        label="Search Query",
        style=discord.TextStyle.short,
        placeholder="Enter search term (event type, admin name, etc.)",
        required=True,
        max_length=100
    )
    
    def __init__(self, parent_view: MemberOverviewView):
        super().__init__()
        self.parent_view = parent_view
    
    async def on_submit(self, interaction: discord.Interaction):
        """Handle audit search."""
        self.parent_view.audit_search_query = self.query.value
        self.parent_view.audit_page = 0
        
        await interaction.response.defer()
        await self.parent_view._update_view(interaction)
