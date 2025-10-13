import asyncio
import aiohttp
import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import quote

import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box, pagify

log = logging.getLogger("red.cog.sanctions_manager")

# ---------- Utilities ----------

def ts() -> int:
    """Get current unix timestamp."""
    return int(datetime.now(timezone.utc).timestamp())

def fmt_dt(timestamp: int) -> str:
    """Format unix timestamp to Discord timestamp."""
    return f"<t:{timestamp}:F>"

def _mc_profile_url(mc_id: str) -> str:
    """Generate Missionchief profile URL."""
    return f"https://www.missionchief.com/profile/{mc_id}"

async def safe_update(interaction: discord.Interaction, *, content=None, embed=None, view=None):
    """Robust message updater for component/modal callbacks."""
    try:
        if not interaction.response.is_done():
            await interaction.response.edit_message(content=content, embed=embed, view=view)
            return
    except Exception as e:
        log.debug("safe_update: response.edit_message failed: %r", e)
    try:
        if getattr(interaction, "message", None) is not None:
            await interaction.message.edit(content=content, embed=embed, view=view)
            return
    except Exception as e:
        log.debug("safe_update: message.edit failed: %r", e)
    try:
        await interaction.followup.send(content or "Updated.", embed=embed, view=view, ephemeral=True)
    except Exception as e:
        log.exception("safe_update completely failed: %r", e)
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(content or "Updated.", embed=embed, view=view, ephemeral=True)
        except Exception:
            pass

# ---------- Database ----------

class SanctionsDatabase:
    """SQLite database for sanctions."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize database tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Sanctions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sanctions (
                sanction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                discord_user_id INTEGER,
                mc_user_id TEXT,
                mc_username TEXT,
                admin_user_id INTEGER NOT NULL,
                admin_username TEXT NOT NULL,
                sanction_type TEXT NOT NULL,
                reason_category TEXT NOT NULL,
                reason_detail TEXT,
                additional_notes TEXT,
                created_at INTEGER NOT NULL,
                expires_at INTEGER,
                status TEXT NOT NULL DEFAULT 'active',
                edited_at INTEGER,
                edited_by INTEGER
            )
        ''')
        
        # Sanction history table (for edits/removals)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sanction_history (
                history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sanction_id INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                action_by INTEGER NOT NULL,
                action_at INTEGER NOT NULL,
                old_values TEXT,
                new_values TEXT,
                notes TEXT,
                FOREIGN KEY (sanction_id) REFERENCES sanctions(sanction_id)
            )
        ''')
        
        # Custom rules table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS custom_rules (
                rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                category TEXT NOT NULL,
                rule_code TEXT NOT NULL,
                rule_text TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                created_at INTEGER NOT NULL,
                UNIQUE(guild_id, category, rule_code)
            )
        ''')
        
        # Auto-action settings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS auto_actions (
                guild_id INTEGER PRIMARY KEY,
                third_warning_action TEXT,
                enabled INTEGER DEFAULT 0
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def add_sanction(self, guild_id: int, discord_user_id: Optional[int], mc_user_id: Optional[str],
                    mc_username: Optional[str], admin_user_id: int, admin_username: str,
                    sanction_type: str, reason_category: str, reason_detail: Optional[str],
                    additional_notes: Optional[str], expires_at: Optional[int] = None) -> int:
        """Add a new sanction."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = ts()
        cursor.execute('''
            INSERT INTO sanctions 
            (guild_id, discord_user_id, mc_user_id, mc_username, admin_user_id, admin_username,
             sanction_type, reason_category, reason_detail, additional_notes, created_at, expires_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
        ''', (guild_id, discord_user_id, mc_user_id, mc_username, admin_user_id, admin_username,
              sanction_type, reason_category, reason_detail, additional_notes, now, expires_at))
        
        sanction_id = cursor.lastrowid
        
        # Log creation in history
        cursor.execute('''
            INSERT INTO sanction_history
            (sanction_id, action_type, action_by, action_at, notes)
            VALUES (?, 'created', ?, ?, 'Sanction created')
        ''', (sanction_id, admin_user_id, now))
        
        conn.commit()
        conn.close()
        
        return sanction_id
    
    def get_sanction(self, sanction_id: int) -> Optional[dict]:
        """Get a sanction by ID."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM sanctions WHERE sanction_id = ?', (sanction_id,))
        result = cursor.fetchone()
        conn.close()
        
        return dict(result) if result else None
    
    def get_user_sanctions(self, guild_id: int, discord_user_id: Optional[int] = None, 
                          mc_user_id: Optional[str] = None) -> List[dict]:
        """Get all sanctions for a user."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if discord_user_id:
            cursor.execute('''
                SELECT * FROM sanctions 
                WHERE guild_id = ? AND discord_user_id = ?
                ORDER BY created_at DESC
            ''', (guild_id, discord_user_id))
        elif mc_user_id:
            cursor.execute('''
                SELECT * FROM sanctions 
                WHERE guild_id = ? AND mc_user_id = ?
                ORDER BY created_at DESC
            ''', (guild_id, mc_user_id))
        else:
            return []
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return results
    
    def get_active_warnings(self, guild_id: int, discord_user_id: Optional[int] = None,
                           mc_user_id: Optional[str] = None) -> List[dict]:
        """Get active warnings for a user."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = '''
            SELECT * FROM sanctions 
            WHERE guild_id = ? AND status = 'active' 
            AND sanction_type LIKE 'Warning - Official%'
        '''
        
        if discord_user_id:
            query += ' AND discord_user_id = ?'
            params = (guild_id, discord_user_id)
        elif mc_user_id:
            query += ' AND mc_user_id = ?'
            params = (guild_id, mc_user_id)
        else:
            conn.close()
            return []
        
        query += ' ORDER BY created_at DESC'
        cursor.execute(query, params)
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return results
    
    def update_sanction_status(self, sanction_id: int, status: str, admin_user_id: int, notes: Optional[str] = None):
        """Update sanction status."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = ts()
        cursor.execute('''
            UPDATE sanctions 
            SET status = ?, edited_at = ?, edited_by = ?
            WHERE sanction_id = ?
        ''', (status, now, admin_user_id, sanction_id))
        
        # Log in history
        cursor.execute('''
            INSERT INTO sanction_history
            (sanction_id, action_type, action_by, action_at, notes)
            VALUES (?, ?, ?, ?, ?)
        ''', (sanction_id, f'status_changed_to_{status}', admin_user_id, now, notes))
        
        conn.commit()
        conn.close()
    
    def edit_sanction(self, sanction_id: int, admin_user_id: int, **updates):
        """Edit sanction fields."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get current values
        cursor.execute('SELECT * FROM sanctions WHERE sanction_id = ?', (sanction_id,))
        old_values = dict(cursor.fetchone())
        
        # Build update query
        fields = []
        values = []
        for key, value in updates.items():
            if key in ['sanction_type', 'reason_category', 'reason_detail', 'additional_notes']:
                fields.append(f"{key} = ?")
                values.append(value)
        
        if not fields:
            conn.close()
            return
        
        now = ts()
        fields.append("edited_at = ?")
        fields.append("edited_by = ?")
        values.extend([now, admin_user_id, sanction_id])
        
        query = f"UPDATE sanctions SET {', '.join(fields)} WHERE sanction_id = ?"
        cursor.execute(query, values)
        
        # Log in history
        cursor.execute('''
            INSERT INTO sanction_history
            (sanction_id, action_type, action_by, action_at, old_values, new_values)
            VALUES (?, 'edited', ?, ?, ?, ?)
        ''', (sanction_id, admin_user_id, now, json.dumps(old_values), json.dumps(updates)))
        
        conn.commit()
        conn.close()
    
    def get_sanction_history(self, sanction_id: int) -> List[dict]:
        """Get history for a sanction."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM sanction_history 
            WHERE sanction_id = ?
            ORDER BY action_at DESC
        ''', (sanction_id,))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return results
    
    def get_stats_overall(self, guild_id: int) -> dict:
        """Get overall statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total by type
        cursor.execute('''
            SELECT sanction_type, COUNT(*) 
            FROM sanctions 
            WHERE guild_id = ?
            GROUP BY sanction_type
        ''', (guild_id,))
        type_counts = dict(cursor.fetchall())
        
        # Total by reason category
        cursor.execute('''
            SELECT reason_category, COUNT(*)
            FROM sanctions
            WHERE guild_id = ?
            GROUP BY reason_category
        ''', (guild_id,))
        reason_counts = dict(cursor.fetchall())
        
        # Top admins
        cursor.execute('''
            SELECT admin_username, COUNT(*) as count
            FROM sanctions
            WHERE guild_id = ?
            GROUP BY admin_user_id
            ORDER BY count DESC
            LIMIT 5
        ''', (guild_id,))
        top_admins = cursor.fetchall()
        
        # Most sanctioned users
        cursor.execute('''
            SELECT mc_username, COUNT(*) as count
            FROM sanctions
            WHERE guild_id = ? AND mc_username IS NOT NULL
            GROUP BY mc_user_id
            ORDER BY count DESC
            LIMIT 5
        ''', (guild_id,))
        most_sanctioned = cursor.fetchall()
        
        conn.close()
        
        return {
            "type_counts": type_counts,
            "reason_counts": reason_counts,
            "top_admins": top_admins,
            "most_sanctioned": most_sanctioned
        }
    
    def get_stats_admin(self, guild_id: int, admin_user_id: int) -> dict:
        """Get admin statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total by type
        cursor.execute('''
            SELECT sanction_type, COUNT(*)
            FROM sanctions
            WHERE guild_id = ? AND admin_user_id = ?
            GROUP BY sanction_type
        ''', (guild_id, admin_user_id))
        type_counts = dict(cursor.fetchall())
        
        # Total by reason
        cursor.execute('''
            SELECT reason_category, COUNT(*)
            FROM sanctions
            WHERE guild_id = ? AND admin_user_id = ?
            GROUP BY reason_category
        ''', (guild_id, admin_user_id))
        reason_counts = dict(cursor.fetchall())
        
        # Recent sanctions
        cursor.execute('''
            SELECT sanction_type, mc_username, created_at
            FROM sanctions
            WHERE guild_id = ? AND admin_user_id = ?
            ORDER BY created_at DESC
            LIMIT 10
        ''', (guild_id, admin_user_id))
        recent = cursor.fetchall()
        
        conn.close()
        
        return {
            "type_counts": type_counts,
            "reason_counts": reason_counts,
            "recent": recent
        }
    
    def add_custom_rule(self, guild_id: int, category: str, rule_code: str, rule_text: str) -> bool:
        """Add a custom rule."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO custom_rules (guild_id, category, rule_code, rule_text, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (guild_id, category, rule_code, rule_text, ts()))
            conn.commit()
            success = True
        except sqlite3.IntegrityError:
            success = False
        
        conn.close()
        return success
    
    def get_custom_rules(self, guild_id: int, category: Optional[str] = None) -> List[dict]:
        """Get custom rules."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        if category:
            cursor.execute('''
                SELECT * FROM custom_rules 
                WHERE guild_id = ? AND category = ? AND enabled = 1
                ORDER BY rule_code
            ''', (guild_id, category))
        else:
            cursor.execute('''
                SELECT * FROM custom_rules 
                WHERE guild_id = ? AND enabled = 1
                ORDER BY category, rule_code
            ''', (guild_id,))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return results
    
    def remove_custom_rule(self, guild_id: int, rule_id: int) -> bool:
        """Remove a custom rule."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            DELETE FROM custom_rules 
            WHERE guild_id = ? AND rule_id = ?
        ''', (guild_id, rule_id))
        
        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        
        return success

# ---------- Default Rules ----------

DEFAULT_RULES = {
    "Member Conduct": {
        "1.3": "No respect - The Alliance is barrier free which requires all members to be aware and respectful of our diversity.",
        "1.4": "Drama - Don't cause it and don't make yourself the subject of it.",
        "1.5": "Religion/Politics - We forbid discussions pertaining to politics and religion.",
        "1.6": "Racism/Bullying - Derogatory racist remarks and any type of bullying will NOT be tolerated.",
        "1.7": "Non-Active Community Members - Members will be removed after 60 days of inactivity.",
        "1.8": "Offensive Nicknames - All the rules in the COC are also applied to nicknames.",
        "1.9": "Advertisement - Advertising/alliance poaching is not allowed.",
    },
    "General Etiquette": {
        "2.1": "Foul language - Gratuitous and excessive use of vulgar foul language is not acceptable.",
        "2.2": "Personal Privacy - Zero tolerance for requests for personal financial information.",
        "2.4": "Common sense - Use common sense, don't spam/flood or yell.",
    },
    "Buildings and Vehicles": {
        "3.1": "Placement - Buildings should be placed at realistic locations. Buildings do not float on water.",
        "3.2": "Naming - Name buildings and vehicles appropriately according to the Code of Conduct.",
    },
    "Other": {
        "4.1": "5% donation to alliance - Minimum 5% donation required.",
    }
}

SANCTION_TYPES = [
    "Warning - In direct message",
    "Warning - In chat",
    "Warning - Official 1st warning",
    "Warning - Official 2nd warning",
    "Warning - Official 3rd and last warning",
    "Kick",
    "Ban",
    "Mute 5m",
    "Mute 15m",
    "Mute 30m",
    "Mute 1h",
    "Mute 6h",
    "Mute 12h",
    "Mute 1d",
    "Mute 7d",
    "Mute 14d",
]

# ---------- Models ----------

class SanctionRequest:
    def __init__(
        self,
        admin_user_id: int,
        admin_username: str,
        target_discord_id: Optional[int],
        target_mc_id: Optional[str],
        target_mc_username: Optional[str],
        target_discord_user: Optional[discord.Member],
        sanction_type: str,
        reason_category: str,
        reason_detail: Optional[str] = None,
        additional_notes: Optional[str] = None,
    ):
        self.admin_user_id = admin_user_id
        self.admin_username = admin_username
        self.target_discord_id = target_discord_id
        self.target_mc_id = target_mc_id
        self.target_mc_username = target_mc_username
        self.target_discord_user = target_discord_user
        self.sanction_type = sanction_type
        self.reason_category = reason_category
        self.reason_detail = reason_detail
        self.additional_notes = additional_notes

# ---------- Views ----------

class StartView(discord.ui.View):
    def __init__(self, cog: "SanctionsManager"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Create Sanction", style=discord.ButtonStyle.danger, custom_id="sm:start")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check admin permissions
        if not await self.cog._is_admin(interaction):
            await interaction.response.send_message("You don't have permission to create sanctions.", ephemeral=True)
            return
        
        await interaction.response.send_message(
            "Let's create a sanction. First, provide the member's information.",
            view=MemberInputView(self.cog),
            ephemeral=True,
        )

class MemberInputView(discord.ui.View):
    def __init__(self, cog: "SanctionsManager"):
        super().__init__(timeout=600)
        self.cog = cog

    @discord.ui.button(label="Discord Member", style=discord.ButtonStyle.primary, custom_id="sm:discord_member")
    async def discord_member(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(DiscordMemberModal(self.cog))

    @discord.ui.button(label="MC ID Only", style=discord.ButtonStyle.secondary, custom_id="sm:mc_only")
    async def mc_only(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(MCOnlyModal(self.cog))

class DiscordMemberModal(discord.ui.Modal, title="Discord Member Lookup"):
    member_input = discord.ui.TextInput(
        label="Discord Member (ID, mention, or username)",
        style=discord.TextStyle.short,
        max_length=100,
        required=True,
        placeholder="@username or 123456789",
    )

    def __init__(self, cog: "SanctionsManager"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            
            guild = interaction.guild
            if not guild:
                await interaction.followup.send("This command must be used in a server.", ephemeral=True)
                return
            
            # Try to find member
            member_str = str(self.member_input.value).strip()
            log.info(f"Looking up member: {member_str}")
            member = None
            
            # Try mention format <@!123> or <@123>
            mention_match = re.match(r'<@!?(\d+)>', member_str)
            if mention_match:
                try:
                    member_id = int(mention_match.group(1))
                    member = guild.get_member(member_id)
                    if not member:
                        log.info(f"Member {member_id} not in cache, fetching...")
                        member = await guild.fetch_member(member_id)
                    log.info(f"Found member via mention: {member}")
                except Exception as e:
                    log.error(f"Error finding member via mention: {e}")
            
            # Try direct ID
            if not member and member_str.isdigit():
                try:
                    member_id = int(member_str)
                    member = guild.get_member(member_id)
                    if not member:
                        log.info(f"Member {member_id} not in cache, fetching...")
                        member = await guild.fetch_member(member_id)
                    log.info(f"Found member via ID: {member}")
                except Exception as e:
                    log.error(f"Error finding member via ID: {e}")
            
            # Try username (case insensitive)
            if not member:
                member_str_lower = member_str.lower()
                for m in guild.members:
                    if m.name.lower() == member_str_lower:
                        member = m
                        log.info(f"Found member via username: {member}")
                        break
                    if m.nick and m.nick.lower() == member_str_lower:
                        member = m
                        log.info(f"Found member via nickname: {member}")
                        break
                    if m.display_name.lower() == member_str_lower:
                        member = m
                        log.info(f"Found member via display name: {member}")
                        break
            
            if not member:
                log.warning(f"Could not find member: {member_str}")
                await interaction.followup.send(
                    f"❌ Could not find Discord member: `{member_str}`\n"
                    f"Try using their exact Discord ID (right-click → Copy ID) or @mention them.",
                    ephemeral=True
                )
                return
            
            log.info(f"Successfully found member: {member.name} ({member.id})")
            
            # Look up MC info via MemberSync
            membersync = self.cog.bot.get_cog("MemberSync")
            mc_data = None
            mc_id = None
            mc_username = None
            
            log.info(f"MemberSync cog found: {membersync is not None}")
            
            if membersync:
                try:
                    log.info(f"Looking up MemberSync data for Discord ID: {member.id}")
                    mc_data = await membersync.get_link_for_discord(member.id)
                    log.info(f"MemberSync raw data: {mc_data}")
                    log.info(f"MemberSync data type: {type(mc_data)}")
                    
                    if mc_data:
                        # MemberSync uses 'mc_user_id' key
                        mc_id = mc_data.get("mc_user_id")
                        log.info(f"Extracted MC ID: {mc_id}")
                        
                        # MemberSync doesn't store username, so we need to look it up
                        # Try to get it from the alliance database via MemberSync's query method
                        if mc_id:
                            try:
                                log.info(f"Querying alliance DB for MC ID: {mc_id}")
                                # Use MemberSync's internal method to query alliance DB
                                rows = await membersync._query_alliance(
                                    "SELECT name FROM members_current WHERE user_id=? OR mc_user_id=?",
                                    (str(mc_id), str(mc_id))
                                )
                                log.info(f"Alliance DB query returned {len(rows) if rows else 0} rows")
                                if rows and len(rows) > 0:
                                    mc_username = rows[0]['name']
                                    log.info(f"Found MC username from alliance DB: {mc_username}")
                                else:
                                    log.warning("No rows returned from alliance DB")
                            except Exception as e:
                                log.error(f"Could not fetch MC username from alliance DB: {e}", exc_info=True)
                        
                        # Fallback to Discord display name if we couldn't get MC username
                        if not mc_username:
                            mc_username = member.display_name
                            log.info(f"Using Discord display name as fallback: {mc_username}")
                    else:
                        log.warning("MemberSync returned None - member not verified")
                        mc_username = member.display_name
                except Exception as e:
                    log.error(f"MemberSync lookup failed: {e}", exc_info=True)
                    mc_username = member.display_name
            else:
                log.warning("MemberSync cog not loaded")
                mc_username = member.display_name
            
            if not mc_username:
                mc_username = member.display_name
            
            log.info(f"Final MC data - ID: {mc_id}, Username: {mc_username}")
            
            log.info(f"Creating SanctionTypeView for {mc_username}")
            
            # Move to sanction type selection
            view = SanctionTypeView(
                self.cog,
                admin_user_id=interaction.user.id,
                admin_username=str(interaction.user),
                target_discord_id=member.id,
                target_mc_id=mc_id,
                target_mc_username=mc_username,
                target_discord_user=member,
            )
            
            # Send new message instead of trying to update
            embed = view._create_target_embed()
            await interaction.followup.send(
                content="Select the type of sanction:",
                embed=embed,
                view=view,
                ephemeral=True
            )
            log.info("Successfully sent sanction type selection")
            
        except Exception as e:
            log.exception(f"Error in DiscordMemberModal.on_submit: {e}")
            try:
                await interaction.followup.send(
                    f"❌ An error occurred: {str(e)}\nPlease check the logs.",
                    ephemeral=True
                )
            except:
                pass

class MCOnlyModal(discord.ui.Modal, title="MC Member Info"):
    mc_id = discord.ui.TextInput(
        label="Missionchief User ID (Optional)",
        style=discord.TextStyle.short,
        max_length=50,
        required=False,
        placeholder="12345",
    )
    
    mc_username = discord.ui.TextInput(
        label="Missionchief Username (Optional)",
        style=discord.TextStyle.short,
        max_length=100,
        required=False,
        placeholder="PlayerName",
    )

    def __init__(self, cog: "SanctionsManager"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            
            mc_id_val = str(self.mc_id.value).strip() if self.mc_id.value else None
            mc_username_val = str(self.mc_username.value).strip() if self.mc_username.value else None
            
            log.info(f"MC Only Modal - ID: {mc_id_val}, Username: {mc_username_val}")
            
            # At least one must be provided
            if not mc_id_val and not mc_username_val:
                await interaction.followup.send("❌ You must provide at least MC ID or MC Username.", ephemeral=True)
                return
            
            # Use username as fallback if no username provided
            display_name = mc_username_val if mc_username_val else f"MC User {mc_id_val}"
            
            # Try to find Discord account via MemberSync (reverse lookup)
            discord_member = None
            discord_id = None
            final_mc_id = mc_id_val
            
            membersync = self.cog.bot.get_cog("MemberSync")
            guild = interaction.guild
            
            if membersync and guild:
                try:
                    log.info("Attempting reverse lookup via MemberSync...")
                    
                    # If we have MC ID, try direct lookup
                    if mc_id_val:
                        log.info(f"Looking up Discord account for MC ID: {mc_id_val}")
                        link_data = await membersync.get_link_for_mc(mc_id_val)
                        log.info(f"MemberSync reverse lookup result: {link_data}")
                        
                        if link_data:
                            discord_id = int(link_data.get("discord_id"))
                            discord_member = guild.get_member(discord_id)
                            if not discord_member:
                                try:
                                    discord_member = await guild.fetch_member(discord_id)
                                except:
                                    pass
                            
                            if discord_member:
                                log.info(f"Found Discord member: {discord_member.name} ({discord_id})")
                            else:
                                log.warning(f"Discord ID {discord_id} found but member not in guild")
                    
                    # If no MC ID provided but we have username, try to find MC ID from alliance DB
                    if not mc_id_val and mc_username_val and membersync:
                        try:
                            log.info(f"Searching alliance DB for MC username: {mc_username_val}")
                            rows = await membersync._query_alliance(
                                "SELECT user_id, mc_user_id FROM members_current WHERE LOWER(name)=?",
                                (mc_username_val.lower(),)
                            )
                            if rows and len(rows) > 0:
                                found_mc_id = rows[0].get('user_id') or rows[0].get('mc_user_id')
                                if found_mc_id:
                                    final_mc_id = str(found_mc_id)
                                    log.info(f"Found MC ID from alliance DB: {final_mc_id}")
                                    
                                    # Now try reverse lookup with this MC ID
                                    link_data = await membersync.get_link_for_mc(final_mc_id)
                                    if link_data:
                                        discord_id = int(link_data.get("discord_id"))
                                        discord_member = guild.get_member(discord_id)
                                        if not discord_member:
                                            try:
                                                discord_member = await guild.fetch_member(discord_id)
                                            except:
                                                pass
                                        
                                        if discord_member:
                                            log.info(f"Found Discord member via username lookup: {discord_member.name}")
                        except Exception as e:
                            log.error(f"Error searching alliance DB: {e}", exc_info=True)
                
                except Exception as e:
                    log.error(f"MemberSync reverse lookup failed: {e}", exc_info=True)
            else:
                if not membersync:
                    log.warning("MemberSync cog not loaded")
                if not guild:
                    log.warning("No guild context")
            
            log.info(f"Final data - MC ID: {final_mc_id}, Username: {display_name}, Discord: {discord_member}")
            log.info(f"Creating SanctionTypeView for MC user: {display_name}")
            
            # Move to sanction type selection
            view = SanctionTypeView(
                self.cog,
                admin_user_id=interaction.user.id,
                admin_username=str(interaction.user),
                target_discord_id=discord_id,
                target_mc_id=final_mc_id,
                target_mc_username=display_name,
                target_discord_user=discord_member,
            )
            
            # Send new message instead of trying to update
            embed = view._create_target_embed()
            await interaction.followup.send(
                content="Select the type of sanction:",
                embed=embed,
                view=view,
                ephemeral=True
            )
            log.info("Successfully sent sanction type selection")
            
        except Exception as e:
            log.exception(f"Error in MCOnlyModal.on_submit: {e}")
            try:
                await interaction.followup.send(
                    f"❌ An error occurred: {str(e)}\nPlease check the logs.",
                    ephemeral=True
                )
            except:
                pass

class SanctionTypeView(discord.ui.View):
    def __init__(self, cog: "SanctionsManager", admin_user_id: int, admin_username: str,
                 target_discord_id: Optional[int], target_mc_id: Optional[str],
                 target_mc_username: Optional[str], target_discord_user: Optional[discord.Member]):
        super().__init__(timeout=600)
        self.cog = cog
        self.admin_user_id = admin_user_id
        self.admin_username = admin_username
        self.target_discord_id = target_discord_id
        self.target_mc_id = target_mc_id
        self.target_mc_username = target_mc_username
        self.target_discord_user = target_discord_user
        self.add_item(SanctionTypeSelect(self))

    def _create_target_embed(self) -> discord.Embed:
        """Create embed showing target info."""
        embed = discord.Embed(
            title="🎯 Target Member",
            color=discord.Color.blue(),
        )
        
        member_info = f"**MC Username**: {self.target_mc_username}\n"
        if self.target_mc_id:
            member_info += f"**MC ID**: {self.target_mc_id}\n"
            member_info += f"**MC Profile**: [Link]({_mc_profile_url(self.target_mc_id)})\n"
        if self.target_discord_user:
            member_info += f"**Discord**: {self.target_discord_user.mention}\n"
        else:
            member_info += f"**Discord**: No Discord found\n"
        
        embed.description = member_info
        return embed

    async def send_selection(self, interaction: discord.Interaction):
        """Deprecated - use followup.send with embed instead."""
        embed = self._create_target_embed()
        await interaction.followup.send(
            content="Select the type of sanction:",
            embed=embed,
            view=self,
            ephemeral=True
        )

class SanctionTypeSelect(discord.ui.Select):
    def __init__(self, parent_view: "SanctionTypeView"):
        self.parent_view = parent_view
        options = [discord.SelectOption(label=t) for t in SANCTION_TYPES]
        super().__init__(placeholder="Choose sanction type", min_values=1, max_values=1, options=options, custom_id="sm:type")

    async def callback(self, interaction: discord.Interaction):
        try:
            sanction_type = self.values[0]
            log.info(f"Selected sanction type: {sanction_type}")
            
            # Move to reason selection
            view = ReasonCategoryView(
                self.parent_view.cog,
                self.parent_view.admin_user_id,
                self.parent_view.admin_username,
                self.parent_view.target_discord_id,
                self.parent_view.target_mc_id,
                self.parent_view.target_mc_username,
                self.parent_view.target_discord_user,
                sanction_type,
            )
            
            await safe_update(
                interaction,
                content=f"Sanction type: **{sanction_type}**\n\nSelect the reason category:",
                view=view
            )
            log.info("Successfully moved to reason category selection")
            
        except Exception as e:
            log.exception(f"Error in SanctionTypeSelect callback: {e}")
            try:
                await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            except:
                pass

class ReasonCategoryView(discord.ui.View):
    def __init__(self, cog: "SanctionsManager", admin_user_id: int, admin_username: str,
                 target_discord_id: Optional[int], target_mc_id: Optional[str],
                 target_mc_username: Optional[str], target_discord_user: Optional[discord.Member],
                 sanction_type: str):
        super().__init__(timeout=600)
        self.cog = cog
        self.admin_user_id = admin_user_id
        self.admin_username = admin_username
        self.target_discord_id = target_discord_id
        self.target_mc_id = target_mc_id
        self.target_mc_username = target_mc_username
        self.target_discord_user = target_discord_user
        self.sanction_type = sanction_type
        self.add_item(ReasonCategorySelect(self))

    async def send_selection(self, interaction: discord.Interaction):
        await safe_update(
            interaction,
            content=f"Sanction type: **{self.sanction_type}**\n\nSelect the reason category:",
            view=self
        )

class ReasonCategorySelect(discord.ui.Select):
    def __init__(self, parent_view: "ReasonCategoryView"):
        self.parent_view = parent_view
        options = [discord.SelectOption(label=cat) for cat in DEFAULT_RULES.keys()]
        options.append(discord.SelectOption(label="Other reason"))
        super().__init__(placeholder="Choose reason category", min_values=1, max_values=1, options=options, custom_id="sm:category")

    async def callback(self, interaction: discord.Interaction):
        try:
            category = self.values[0]
            log.info(f"Selected reason category: {category}")
            
            if category == "Other reason":
                # Show modal for custom reason
                modal = CustomReasonModal(
                    self.parent_view.cog,
                    self.parent_view.admin_user_id,
                    self.parent_view.admin_username,
                    self.parent_view.target_discord_id,
                    self.parent_view.target_mc_id,
                    self.parent_view.target_mc_username,
                    self.parent_view.target_discord_user,
                    self.parent_view.sanction_type,
                )
                await interaction.response.send_modal(modal)
                log.info("Sent custom reason modal")
            else:
                # Move to specific rule selection
                view = ReasonDetailView(
                    self.parent_view.cog,
                    self.parent_view.admin_user_id,
                    self.parent_view.admin_username,
                    self.parent_view.target_discord_id,
                    self.parent_view.target_mc_id,
                    self.parent_view.target_mc_username,
                    self.parent_view.target_discord_user,
                    self.parent_view.sanction_type,
                    category,
                )
                
                await safe_update(
                    interaction,
                    content=f"Category: **{category}**\n\nSelect the specific rule:",
                    view=view
                )
                log.info("Successfully moved to reason detail selection")
                
        except Exception as e:
            log.exception(f"Error in ReasonCategorySelect callback: {e}")
            try:
                await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            except:
                pass

class ReasonDetailView(discord.ui.View):
    def __init__(self, cog: "SanctionsManager", admin_user_id: int, admin_username: str,
                 target_discord_id: Optional[int], target_mc_id: Optional[str],
                 target_mc_username: Optional[str], target_discord_user: Optional[discord.Member],
                 sanction_type: str, reason_category: str):
        super().__init__(timeout=600)
        self.cog = cog
        self.admin_user_id = admin_user_id
        self.admin_username = admin_username
        self.target_discord_id = target_discord_id
        self.target_mc_id = target_mc_id
        self.target_mc_username = target_mc_username
        self.target_discord_user = target_discord_user
        self.sanction_type = sanction_type
        self.reason_category = reason_category
        self.add_item(ReasonDetailSelect(self))

    async def send_selection(self, interaction: discord.Interaction):
        await safe_update(
            interaction,
            content=f"Category: **{self.reason_category}**\n\nSelect the specific rule:",
            view=self
        )

class ReasonDetailSelect(discord.ui.Select):
    def __init__(self, parent_view: "ReasonDetailView"):
        self.parent_view = parent_view
        rules = DEFAULT_RULES.get(parent_view.reason_category, {})
        
        # Get custom rules for this category
        guild_id = parent_view.target_discord_user.guild.id if parent_view.target_discord_user else 0
        custom_rules = parent_view.cog.db.get_custom_rules(guild_id, parent_view.reason_category) if guild_id else []
        
        options = []
        for code, text in rules.items():
            label = f"{code}. {text[:50]}"
            options.append(discord.SelectOption(label=label, value=f"default:{code}", description=text[:100]))
        
        for rule in custom_rules:
            label = f"{rule['rule_code']}. {rule['rule_text'][:50]}"
            options.append(discord.SelectOption(label=label, value=f"custom:{rule['rule_id']}", description=rule['rule_text'][:100]))
        
        super().__init__(placeholder="Choose specific rule", min_values=1, max_values=1, options=options[:25], custom_id="sm:detail")

    async def callback(self, interaction: discord.Interaction):
        try:
            selection = self.values[0]
            log.info(f"Selected reason detail: {selection}")
            
            if selection.startswith("default:"):
                code = selection.split(":", 1)[1]
                rules = DEFAULT_RULES.get(self.parent_view.reason_category, {})
                reason_detail = f"{code}. {rules.get(code, '')}"
            else:
                rule_id = int(selection.split(":", 1)[1])
                guild_id = self.parent_view.target_discord_user.guild.id if self.parent_view.target_discord_user else 0
                custom_rules = self.parent_view.cog.db.get_custom_rules(guild_id) if guild_id else []
                rule = next((r for r in custom_rules if r['rule_id'] == rule_id), None)
                reason_detail = f"{rule['rule_code']}. {rule['rule_text']}" if rule else ""
            
            log.info(f"Reason detail: {reason_detail}")
            
            # Move to summary
            view = SummarySanctionView(
                self.parent_view.cog,
                self.parent_view.admin_user_id,
                self.parent_view.admin_username,
                self.parent_view.target_discord_id,
                self.parent_view.target_mc_id,
                self.parent_view.target_mc_username,
                self.parent_view.target_discord_user,
                self.parent_view.sanction_type,
                self.parent_view.reason_category,
                reason_detail,
            )
            await view.send_summary(interaction)
            log.info("Successfully sent summary")
            
        except Exception as e:
            log.exception(f"Error in ReasonDetailSelect callback: {e}")
            try:
                await interaction.followup.send(f"❌ Error: {str(e)}", ephemeral=True)
            except:
                pass

class CustomReasonModal(discord.ui.Modal, title="Custom Reason"):
    reason = discord.ui.TextInput(
        label="Reason",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=True,
        placeholder="Explain the reason for this sanction...",
    )

    def __init__(self, cog: "SanctionsManager", admin_user_id: int, admin_username: str,
                 target_discord_id: Optional[int], target_mc_id: Optional[str],
                 target_mc_username: Optional[str], target_discord_user: Optional[discord.Member],
                 sanction_type: str):
        super().__init__()
        self.cog = cog
        self.admin_user_id = admin_user_id
        self.admin_username = admin_username
        self.target_discord_id = target_discord_id
        self.target_mc_id = target_mc_id
        self.target_mc_username = target_mc_username
        self.target_discord_user = target_discord_user
        self.sanction_type = sanction_type

    async def on_submit(self, interaction: discord.Interaction):
        view = SummarySanctionView(
            self.cog,
            self.admin_user_id,
            self.admin_username,
            self.target_discord_id,
            self.target_mc_id,
            self.target_mc_username,
            self.target_discord_user,
            self.sanction_type,
            "Other reason",
            str(self.reason),
        )
        await view.send_summary(interaction)

class SummarySanctionView(discord.ui.View):
    def __init__(self, cog: "SanctionsManager", admin_user_id: int, admin_username: str,
                 target_discord_id: Optional[int], target_mc_id: Optional[str],
                 target_mc_username: Optional[str], target_discord_user: Optional[discord.Member],
                 sanction_type: str, reason_category: str, reason_detail: str,
                 additional_notes: Optional[str] = None):
        super().__init__(timeout=600)
        self.cog = cog
        self.admin_user_id = admin_user_id
        self.admin_username = admin_username
        self.target_discord_id = target_discord_id
        self.target_mc_id = target_mc_id
        self.target_mc_username = target_mc_username
        self.target_discord_user = target_discord_user
        self.sanction_type = sanction_type
        self.reason_category = reason_category
        self.reason_detail = reason_detail
        self.additional_notes = additional_notes

    async def send_summary(self, interaction: discord.Interaction):
        embed = self._create_embed()
        await safe_update(
            interaction,
            content="⚠️ Review the sanction before submitting:",
            embed=embed,
            view=self
        )

    def _create_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title="🚨 Sanction Summary",
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc),
        )
        
        # Member info
        member_info = f"**MC Username**: {self.target_mc_username}\n"
        if self.target_mc_id:
            member_info += f"**MC Profile**: [Link]({_mc_profile_url(self.target_mc_id)})\n"
        if self.target_discord_user:
            member_info += f"**Discord**: {self.target_discord_user.mention}\n"
        else:
            member_info += f"**Discord**: No Discord found\n"
        
        embed.add_field(name="Member", value=member_info, inline=False)
        embed.add_field(name="Admin", value=self.admin_username, inline=True)
        embed.add_field(name="Sanction", value=self.sanction_type, inline=True)
        embed.add_field(name="Reason Category", value=self.reason_category, inline=False)
        embed.add_field(name="Reason", value=self.reason_detail[:1024], inline=False)
        
        if self.additional_notes:
            embed.add_field(name="Additional Information", value=self.additional_notes[:1024], inline=False)
        
        return embed

    @discord.ui.button(label="Add Admin Notes", style=discord.ButtonStyle.secondary, custom_id="sm:add_notes")
    async def add_notes(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = AdditionalNotesModal(self)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Submit Sanction", style=discord.ButtonStyle.danger, custom_id="sm:submit")
    async def submit(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        guild = interaction.guild
        if not guild:
            await interaction.followup.send("This must be used in a server.", ephemeral=True)
            return

        conf = await self.cog.config.guild(guild).all()
        sanction_channel_id = conf.get("sanction_channel_id")
        log_channel_id = conf.get("log_channel_id")

        if not sanction_channel_id or not log_channel_id:
            await interaction.followup.send(
                "Sanction/Log channels are not configured. Ask an admin to use [p]sanctionset.",
                ephemeral=True,
            )
            return

        sanction_channel = guild.get_channel(sanction_channel_id)
        log_channel = guild.get_channel(log_channel_id)

        if not sanction_channel or not log_channel:
            await interaction.followup.send("Configured channels not found.", ephemeral=True)
            return

        # Check for auto-actions on 3rd warning
        auto_action_enabled = await self.cog.config.guild(guild).auto_action_enabled()
        if auto_action_enabled and self.sanction_type == "Warning - Official 3rd and last warning":
            active_warnings = self.cog.db.get_active_warnings(
                guild.id,
                self.target_discord_id,
                self.target_mc_id
            )
            
            official_warnings = [w for w in active_warnings if "Official" in w['sanction_type']]
            if len(official_warnings) >= 2:  # This will be the 3rd
                auto_action = await self.cog.config.guild(guild).third_warning_action()
                if auto_action:
                    # Log that auto-action should be taken
                    await log_channel.send(
                        f"⚠️ **Auto-Action Triggered**: {self.target_mc_username} has received a 3rd official warning. "
                        f"Recommended action: {auto_action}"
                    )

        # Save to database
        sanction_id = self.cog.db.add_sanction(
            guild_id=guild.id,
            discord_user_id=self.target_discord_id,
            mc_user_id=self.target_mc_id,
            mc_username=self.target_mc_username,
            admin_user_id=self.admin_user_id,
            admin_username=self.admin_username,
            sanction_type=self.sanction_type,
            reason_category=self.reason_category,
            reason_detail=self.reason_detail,
            additional_notes=self.additional_notes
        )

        # Post to sanction channel (public)
        public_embed = discord.Embed(
            title="🚨 Sanction Issued",
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc),
        )
        
        member_info = f"**MC Username**: {self.target_mc_username}\n"
        if self.target_mc_id:
            member_info += f"**MC Profile**: [Link]({_mc_profile_url(self.target_mc_id)})\n"
        if self.target_discord_user:
            member_info += f"**Discord**: {self.target_discord_user.mention}\n"
        else:
            member_info += f"**Discord**: No Discord found\n"
        
        public_embed.add_field(name="Member", value=member_info, inline=False)
        public_embed.add_field(name="Admin", value=self.admin_username, inline=True)
        public_embed.add_field(name="Sanction", value=self.sanction_type, inline=True)
        public_embed.add_field(name="Reason", value=self.reason_detail[:1024], inline=False)
        public_embed.set_footer(text=f"Sanction ID: {sanction_id}")
        
        await sanction_channel.send(embed=public_embed)

        # Post to log channel (with admin notes)
        log_embed = discord.Embed(
            title="📋 Sanction Logged",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        
        log_embed.add_field(name="Member", value=member_info, inline=False)
        log_embed.add_field(name="Admin", value=f"{self.admin_username} ({self.admin_user_id})", inline=True)
        log_embed.add_field(name="Sanction", value=self.sanction_type, inline=True)
        log_embed.add_field(name="Reason Category", value=self.reason_category, inline=False)
        log_embed.add_field(name="Reason", value=self.reason_detail[:1024], inline=False)
        
        if self.additional_notes:
            log_embed.add_field(name="Admin Notes", value=self.additional_notes[:1024], inline=False)
        
        log_embed.set_footer(text=f"Sanction ID: {sanction_id}")
        
        await log_channel.send(embed=log_embed)

        # Send DM to member if Discord user exists
        if self.target_discord_user:
            dm_text = (
                f"🚨 **You have received a sanction from Fire & Rescue Academy**\n\n"
                f"**Sanction Type**: {self.sanction_type}\n"
                f"**Reason**: {self.reason_detail}\n\n"
                f"If you have questions, please contact an administrator."
            )
            try:
                await self.target_discord_user.send(dm_text)
            except discord.Forbidden:
                await log_channel.send(f"⚠️ Could not DM {self.target_discord_user.mention} about their sanction.")

        # Disable all buttons
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

        await safe_update(
            interaction,
            content="✅ Sanction submitted and logged.",
            embed=None,
            view=self
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, custom_id="sm:cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True
        
        await safe_update(interaction, content="❌ Sanction cancelled.", embed=None, view=self)

class AdditionalNotesModal(discord.ui.Modal, title="Additional Admin Notes"):
    notes = discord.ui.TextInput(
        label="Admin-only notes",
        style=discord.TextStyle.paragraph,
        max_length=1000,
        required=True,
        placeholder="These notes are only visible to other admins...",
    )

    def __init__(self, parent: SummarySanctionView):
        super().__init__()
        self.parent = parent
        if parent.additional_notes:
            self.notes.default = parent.additional_notes

    async def on_submit(self, interaction: discord.Interaction):
        self.parent.additional_notes = str(self.notes)
        embed = self.parent._create_embed()
        await safe_update(
            interaction,
            content="⚠️ Review the sanction before submitting:",
            embed=embed,
            view=self.parent
        )

# ---------- Cog ----------

class SanctionsManager(commands.Cog):
    """Manage sanctions for alliance members with full tracking and statistics."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xDEADBEEF2, force_registration=True)
        default_guild = {
            "admin_channel_id": None,
            "sanction_channel_id": None,
            "log_channel_id": None,
            "admin_role_id": None,
            "auto_action_enabled": False,
            "third_warning_action": "Kick",
        }
        self.config.register_guild(**default_guild)

        # Initialize database
        from redbot.core import data_manager
        db_path = str(data_manager.cog_data_path(self) / "sanctions.db")
        self.db = SanctionsDatabase(db_path)

        self.bot.add_view(StartView(self))

    def cog_unload(self):
        pass

    async def _is_admin(self, interaction: discord.Interaction) -> bool:
        """Check if user has admin permissions."""
        guild = interaction.guild
        if guild is None or not isinstance(interaction.user, discord.Member):
            return False
        
        if interaction.user.guild_permissions.administrator:
            return True
        
        role_id = await self.config.guild(guild).admin_role_id()
        if role_id is None:
            return False
        
        role = guild.get_role(role_id)
        return role in interaction.user.roles if role else False

    @commands.group(name="sanctionset", invoke_without_command=True)
    @commands.admin()
    @commands.guild_only()
    async def sanctionset(self, ctx: commands.Context):
        """Configure Sanctions Manager."""
        conf = await self.config.guild(ctx.guild).all()
        
        admin_ch = ctx.guild.get_channel(conf['admin_channel_id']) if conf.get('admin_channel_id') else None
        sanction_ch = ctx.guild.get_channel(conf['sanction_channel_id']) if conf.get('sanction_channel_id') else None
        log_ch = ctx.guild.get_channel(conf['log_channel_id']) if conf.get('log_channel_id') else None
        admin_role = ctx.guild.get_role(conf['admin_role_id']) if conf.get('admin_role_id') else None
        
        txt = (
            f"Admin channel: {admin_ch.mention if admin_ch else '—'}\n"
            f"Sanction channel (public): {sanction_ch.mention if sanction_ch else '—'}\n"
            f"Log channel: {log_ch.mention if log_ch else '—'}\n"
            f"Admin role: {admin_role.mention if admin_role else '—'}\n"
            f"Auto-action on 3rd warning: {'Enabled' if conf.get('auto_action_enabled') else 'Disabled'}\n"
            f"3rd warning action: {conf.get('third_warning_action', 'Kick')}\n"
        )
        await ctx.send(box(txt, lang="ini"))

    @sanctionset.command(name="adminchannel")
    @commands.admin()
    @commands.guild_only()
    async def adminchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where admins create sanctions."""
        await self.config.guild(ctx.guild).admin_channel_id.set(channel.id)
        await ctx.tick()

    @sanctionset.command(name="sanctionchannel")
    @commands.admin()
    @commands.guild_only()
    async def sanctionchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the public channel where sanctions are posted."""
        await self.config.guild(ctx.guild).sanction_channel_id.set(channel.id)
        await ctx.tick()

    @sanctionset.command(name="logchannel")
    @commands.admin()
    @commands.guild_only()
    async def logchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where all sanctions are logged (with admin notes)."""
        await self.config.guild(ctx.guild).log_channel_id.set(channel.id)
        await ctx.tick()

    @sanctionset.command(name="adminrole")
    @commands.admin()
    @commands.guild_only()
    async def adminrole(self, ctx: commands.Context, role: discord.Role):
        """Set the role that can create/manage sanctions."""
        await self.config.guild(ctx.guild).admin_role_id.set(role.id)
        await ctx.tick()

    @sanctionset.command(name="autoaction")
    @commands.admin()
    @commands.guild_only()
    async def autoaction(self, ctx: commands.Context, enabled: bool):
        """Enable/disable automatic action recommendations on 3rd warning."""
        await self.config.guild(ctx.guild).auto_action_enabled.set(enabled)
        await ctx.send(f"Auto-action on 3rd warning: {'Enabled' if enabled else 'Disabled'}")

    @sanctionset.command(name="thirdwarningaction")
    @commands.admin()
    @commands.guild_only()
    async def thirdwarningaction(self, ctx: commands.Context, action: str):
        """Set the recommended action for 3rd official warning (e.g., Kick, Ban)."""
        await self.config.guild(ctx.guild).third_warning_action.set(action)
        await ctx.send(f"3rd warning action set to: {action}")

    @sanctionset.command(name="post")
    @commands.admin()
    @commands.guild_only()
    async def post(self, ctx: commands.Context):
        """Post the Create Sanction button in the admin channel."""
        admin_channel_id = await self.config.guild(ctx.guild).admin_channel_id()
        if not admin_channel_id:
            await ctx.send("Set the admin channel first with `[p]sanctionset adminchannel #channel`.")
            return
        
        ch = ctx.guild.get_channel(admin_channel_id)
        if not ch:
            await ctx.send("The configured admin channel was not found.")
            return
        
        emb = discord.Embed(
            title="🚨 Sanctions Manager",
            description=(
                "Use this system to issue sanctions to alliance members.\n\n"
                "**Available Sanctions:**\n"
                "• Verbal warnings\n"
                "• Official warnings (1st, 2nd, 3rd)\n"
                "• Kicks\n"
                "• Bans\n"
                "• Mutes (various durations)\n\n"
                "Click the button below to start."
            ),
            color=discord.Color.red(),
        )
        await ch.send(embed=emb, view=StartView(self))
        await ctx.tick()

    @commands.hybrid_group(name="sanction", invoke_without_command=True)
    @commands.guild_only()
    async def sanction_group(self, ctx: commands.Context):
        """Sanction management commands."""
        await ctx.send_help(ctx.command)

    @sanction_group.command(name="history")
    @commands.guild_only()
    async def history(self, ctx: commands.Context, member: discord.Member = None, mc_id: str = None):
        """View sanction history for a member."""
        if not member and not mc_id:
            await ctx.send("Provide either a Discord member or MC ID.")
            return
        
        sanctions = self.db.get_user_sanctions(
            ctx.guild.id,
            member.id if member else None,
            mc_id
        )
        
        if not sanctions:
            await ctx.send("No sanctions found for this member.")
            return
        
        embed = discord.Embed(
            title=f"📋 Sanction History",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        
        if member:
            embed.set_author(name=member.display_name, icon_url=member.display_avatar.url)
        
        for s in sanctions[:10]:  # Show last 10
            status_emoji = "🔴" if s['status'] == 'active' else "⚫"
            value = (
                f"**Type**: {s['sanction_type']}\n"
                f"**Reason**: {s['reason_detail'][:100]}\n"
                f"**Admin**: {s['admin_username']}\n"
                f"**Date**: {fmt_dt(s['created_at'])}"
            )
            embed.add_field(
                name=f"{status_emoji} ID: {s['sanction_id']} ({s['status']})",
                value=value,
                inline=False
            )
        
        if len(sanctions) > 10:
            embed.set_footer(text=f"Showing 10 of {len(sanctions)} sanctions")
        
        await ctx.send(embed=embed)

    @sanction_group.command(name="view")
    @commands.guild_only()
    async def view(self, ctx: commands.Context, sanction_id: int):
        """View details of a specific sanction."""
        sanction = self.db.get_sanction(sanction_id)
        
        if not sanction or sanction['guild_id'] != ctx.guild.id:
            await ctx.send("Sanction not found.")
            return
        
        embed = discord.Embed(
            title=f"🚨 Sanction #{sanction_id}",
            color=discord.Color.red() if sanction['status'] == 'active' else discord.Color.gray(),
            timestamp=datetime.fromtimestamp(sanction['created_at'], tz=timezone.utc)
        )
        
        member_info = f"**MC Username**: {sanction['mc_username']}\n"
        if sanction['mc_user_id']:
            member_info += f"**MC Profile**: [Link]({_mc_profile_url(sanction['mc_user_id'])})\n"
        if sanction['discord_user_id']:
            member_info += f"**Discord**: <@{sanction['discord_user_id']}>\n"
        else:
            member_info += "**Discord**: No Discord found\n"
        
        embed.add_field(name="Member", value=member_info, inline=False)
        embed.add_field(name="Admin", value=sanction['admin_username'], inline=True)
        embed.add_field(name="Status", value=sanction['status'], inline=True)
        embed.add_field(name="Sanction Type", value=sanction['sanction_type'], inline=False)
        embed.add_field(name="Reason Category", value=sanction['reason_category'], inline=False)
        embed.add_field(name="Reason", value=sanction['reason_detail'][:1024], inline=False)
        
        if sanction['additional_notes']:
            embed.add_field(name="Admin Notes", value=sanction['additional_notes'][:1024], inline=False)
        
        if sanction['edited_at']:
            embed.add_field(name="Last Edited", value=fmt_dt(sanction['edited_at']), inline=True)
        
        await ctx.send(embed=embed)

    @sanction_group.command(name="remove")
    @commands.guild_only()
    async def remove(self, ctx: commands.Context, sanction_id: int, *, reason: str):
        """Remove/archive a sanction (admin only)."""
        if not await self._is_admin(discord.Interaction(data={}, state=ctx.bot._connection)):
            await ctx.send("You don't have permission to do this.")
            return
        
        sanction = self.db.get_sanction(sanction_id)
        
        if not sanction or sanction['guild_id'] != ctx.guild.id:
            await ctx.send("Sanction not found.")
            return
        
        self.db.update_sanction_status(
            sanction_id,
            'removed',
            ctx.author.id,
            f"Removed by {ctx.author}: {reason}"
        )
        
        await ctx.send(f"✅ Sanction #{sanction_id} has been removed.")

    @sanction_group.command(name="edit")
    @commands.guild_only()
    async def edit(self, ctx: commands.Context, sanction_id: int):
        """Edit a sanction (admin only)."""
        if not isinstance(ctx.author, discord.Member):
            return
        
        # Check admin permission
        if not ctx.author.guild_permissions.administrator:
            role_id = await self.config.guild(ctx.guild).admin_role_id()
            if not role_id:
                await ctx.send("You don't have permission to edit sanctions.")
                return
            role = ctx.guild.get_role(role_id)
            if not role or role not in ctx.author.roles:
                await ctx.send("You don't have permission to edit sanctions.")
                return
        
        sanction = self.db.get_sanction(sanction_id)
        
        if not sanction or sanction['guild_id'] != ctx.guild.id:
            await ctx.send("Sanction not found.")
            return
        
        # Send edit modal/view
        await ctx.send(
            f"Editing sanction #{sanction_id}. Use the buttons below:",
            view=EditSanctionView(self, sanction, ctx.author),
            ephemeral=True
        )

class EditSanctionView(discord.ui.View):
    def __init__(self, cog: "SanctionsManager", sanction: dict, editor: discord.Member):
        super().__init__(timeout=600)
        self.cog = cog
        self.sanction = sanction
        self.editor = editor

    @discord.ui.button(label="Edit MC Info", style=discord.ButtonStyle.primary)
    async def edit_mc_info(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = EditMCInfoModal(self.cog, self.sanction, self.editor)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Edit Sanction Type", style=discord.ButtonStyle.primary)
    async def edit_type(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Select new sanction type:",
            view=EditSanctionTypeView(self.cog, self.sanction, self.editor),
            ephemeral=True
        )

    @discord.ui.button(label="Edit Reason", style=discord.ButtonStyle.primary)
    async def edit_reason(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = EditReasonModal(self.cog, self.sanction, self.editor)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Edit Admin Notes", style=discord.ButtonStyle.secondary)
    async def edit_notes(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = EditNotesModal(self.cog, self.sanction, self.editor)
        await interaction.response.send_modal(modal)

class EditMCInfoModal(discord.ui.Modal, title="Edit MC Info"):
    mc_id = discord.ui.TextInput(
        label="MC User ID (Optional)",
        style=discord.TextStyle.short,
        max_length=50,
        required=False,
    )
    
    mc_username = discord.ui.TextInput(
        label="MC Username (Optional)",
        style=discord.TextStyle.short,
        max_length=100,
        required=False,
    )

    def __init__(self, cog: "SanctionsManager", sanction: dict, editor: discord.Member):
        super().__init__()
        self.cog = cog
        self.sanction = sanction
        self.editor = editor
        
        if sanction.get('mc_user_id'):
            self.mc_id.default = str(sanction['mc_user_id'])
        if sanction.get('mc_username'):
            self.mc_username.default = str(sanction['mc_username'])

    async def on_submit(self, interaction: discord.Interaction):
        updates = {}
        
        mc_id_val = str(self.mc_id).strip() if self.mc_id.value else None
        mc_username_val = str(self.mc_username).strip() if self.mc_username.value else None
        
        if mc_id_val:
            updates['mc_user_id'] = mc_id_val
        if mc_username_val:
            updates['mc_username'] = mc_username_val
        
        if not updates:
            await interaction.response.send_message("No changes made.", ephemeral=True)
            return
        
        self.cog.db.edit_sanction(
            self.sanction['sanction_id'],
            self.editor.id,
            **updates
        )
        
        await interaction.response.send_message(
            f"✅ Updated MC info for sanction #{self.sanction['sanction_id']}",
            ephemeral=True
        )

class EditSanctionTypeView(discord.ui.View):
    def __init__(self, cog: "SanctionsManager", sanction: dict, editor: discord.Member):
        super().__init__(timeout=600)
        self.cog = cog
        self.sanction = sanction
        self.editor = editor
        self.add_item(EditSanctionTypeSelect(self))

class EditSanctionTypeSelect(discord.ui.Select):
    def __init__(self, parent: EditSanctionTypeView):
        self.parent = parent
        options = [discord.SelectOption(label=t) for t in SANCTION_TYPES]
        super().__init__(placeholder="Choose new sanction type", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        new_type = self.values[0]
        
        self.parent.cog.db.edit_sanction(
            self.parent.sanction['sanction_id'],
            self.parent.editor.id,
            sanction_type=new_type
        )
        
        await interaction.response.send_message(
            f"✅ Updated sanction type to: {new_type}",
            ephemeral=True
        )

class EditReasonModal(discord.ui.Modal, title="Edit Reason"):
    reason_category = discord.ui.TextInput(
        label="Reason Category",
        style=discord.TextStyle.short,
        max_length=100,
        required=True,
    )
    
    reason_detail = discord.ui.TextInput(
        label="Reason Detail",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=True,
    )

    def __init__(self, cog: "SanctionsManager", sanction: dict, editor: discord.Member):
        super().__init__()
        self.cog = cog
        self.sanction = sanction
        self.editor = editor
        
        if sanction.get('reason_category'):
            self.reason_category.default = str(sanction['reason_category'])
        if sanction.get('reason_detail'):
            self.reason_detail.default = str(sanction['reason_detail'])

    async def on_submit(self, interaction: discord.Interaction):
        self.cog.db.edit_sanction(
            self.sanction['sanction_id'],
            self.editor.id,
            reason_category=str(self.reason_category),
            reason_detail=str(self.reason_detail)
        )
        
        await interaction.response.send_message(
            f"✅ Updated reason for sanction #{self.sanction['sanction_id']}",
            ephemeral=True
        )

class EditNotesModal(discord.ui.Modal, title="Edit Admin Notes"):
    notes = discord.ui.TextInput(
        label="Admin Notes",
        style=discord.TextStyle.paragraph,
        max_length=1000,
        required=False,
    )

    def __init__(self, cog: "SanctionsManager", sanction: dict, editor: discord.Member):
        super().__init__()
        self.cog = cog
        self.sanction = sanction
        self.editor = editor
        
        if sanction.get('additional_notes'):
            self.notes.default = str(sanction['additional_notes'])

    async def on_submit(self, interaction: discord.Interaction):
        self.cog.db.edit_sanction(
            self.sanction['sanction_id'],
            self.editor.id,
            additional_notes=str(self.notes) if self.notes.value else None
        )
        
        await interaction.response.send_message(
            f"✅ Updated admin notes for sanction #{self.sanction['sanction_id']}",
            ephemeral=True
        )

    @commands.hybrid_group(name="sanctionstats", invoke_without_command=True)
    @commands.guild_only()
    async def sanctionstats(self, ctx: commands.Context):
        """View sanction statistics."""
        stats = self.db.get_stats_overall(ctx.guild.id)
        
        embed = discord.Embed(
            title="📊 Sanction Statistics",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        
        # By type
        type_text = ""
        for stype, count in stats['type_counts'].items():
            type_text += f"• {stype}: {count}\n"
        
        if type_text:
            embed.add_field(name="By Sanction Type", value=type_text, inline=False)
        
        # By reason
        reason_text = ""
        for reason, count in list(stats['reason_counts'].items())[:5]:
            reason_text += f"• {reason}: {count}\n"
        
        if reason_text:
            embed.add_field(name="Top Reasons", value=reason_text, inline=False)
        
        # Top admins
        admin_text = ""
        for admin, count in stats['top_admins']:
            admin_text += f"• {admin}: {count}\n"
        
        if admin_text:
            embed.add_field(name="Most Active Admins", value=admin_text, inline=True)
        
        # Most sanctioned
        sanctioned_text = ""
        for username, count in stats['most_sanctioned']:
            sanctioned_text += f"• {username}: {count}\n"
        
        if sanctioned_text:
            embed.add_field(name="Most Sanctioned Members", value=sanctioned_text, inline=True)
        
        await ctx.send(embed=embed)

    @sanctionstats.command(name="admin")
    @commands.guild_only()
    async def sanctionstats_admin(self, ctx: commands.Context, admin: discord.Member = None):
        """View statistics for a specific admin."""
        if admin is None:
            admin = ctx.author
        
        stats = self.db.get_stats_admin(ctx.guild.id, admin.id)
        
        if not stats['type_counts']:
            await ctx.send(f"{admin.mention} has not issued any sanctions yet.")
            return
        
        embed = discord.Embed(
            title=f"📊 Admin Statistics for {admin.display_name}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_thumbnail(url=admin.display_avatar.url)
        
        # By type
        type_text = ""
        total = sum(stats['type_counts'].values())
        for stype, count in stats['type_counts'].items():
            percentage = (count * 100 // total) if total else 0
            type_text += f"• {stype}: {count} ({percentage}%)\n"
        
        embed.add_field(name=f"Total Sanctions: {total}", value=type_text, inline=False)
        
        # By reason
        reason_text = ""
        for reason, count in list(stats['reason_counts'].items())[:5]:
            reason_text += f"• {reason}: {count}\n"
        
        if reason_text:
            embed.add_field(name="Top Reasons Used", value=reason_text, inline=False)
        
        # Recent
        recent_text = ""
        for stype, username, timestamp in stats['recent'][:5]:
            recent_text += f"• {stype} to {username} ({fmt_dt(timestamp)})\n"
        
        if recent_text:
            embed.add_field(name="Recent Sanctions (last 5)", value=recent_text, inline=False)
        
        await ctx.send(embed=embed)

    @commands.group(name="sanctionrules", invoke_without_command=True)
    @commands.admin()
    @commands.guild_only()
    async def sanctionrules(self, ctx: commands.Context):
        """Manage custom sanction rules."""
        await ctx.send_help(ctx.command)

    @sanctionrules.command(name="add")
    @commands.admin()
    @commands.guild_only()
    async def add_rule(self, ctx: commands.Context, category: str, rule_code: str, *, rule_text: str):
        """Add a custom rule.
        
        Example: [p]sanctionrules add "Member Conduct" "1.10" "No excessive caps lock in chat"
        """
        success = self.db.add_custom_rule(ctx.guild.id, category, rule_code, rule_text)
        
        if success:
            await ctx.send(f"✅ Added custom rule: {category} - {rule_code}")
        else:
            await ctx.send("❌ This rule already exists or there was an error.")

    @sanctionrules.command(name="list")
    @commands.admin()
    @commands.guild_only()
    async def list_rules(self, ctx: commands.Context, category: str = None):
        """List custom rules."""
        rules = self.db.get_custom_rules(ctx.guild.id, category)
        
        if not rules:
            await ctx.send("No custom rules found.")
            return
        
        embed = discord.Embed(
            title=f"📜 Custom Rules" + (f" - {category}" if category else ""),
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc)
        )
        
        current_category = None
        rule_text = ""
        
        for rule in rules:
            if rule['category'] != current_category:
                if rule_text:
                    embed.add_field(name=current_category, value=rule_text, inline=False)
                current_category = rule['category']
                rule_text = ""
            
            rule_text += f"**{rule['rule_code']}** (ID: {rule['rule_id']}): {rule['rule_text']}\n"
        
        if rule_text:
            embed.add_field(name=current_category, value=rule_text, inline=False)
        
        await ctx.send(embed=embed)

    @sanctionrules.command(name="remove")
    @commands.admin()
    @commands.guild_only()
    async def remove_rule(self, ctx: commands.Context, rule_id: int):
        """Remove a custom rule by ID."""
        success = self.db.remove_custom_rule(ctx.guild.id, rule_id)
        
        if success:
            await ctx.send(f"✅ Removed custom rule ID {rule_id}")
        else:
            await ctx.send("❌ Rule not found or could not be removed.")

    @sanctionrules.command(name="default")
    @commands.admin()
    @commands.guild_only()
    async def default_rules(self, ctx: commands.Context):
        """Show all default rules."""
        embed = discord.Embed(
            title="📜 Default Sanction Rules",
            description="These are the built-in rules available for sanctions.",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        
        for category, rules in DEFAULT_RULES.items():
            rule_text = ""
            for code, text in rules.items():
                rule_text += f"**{code}**: {text}\n"
            embed.add_field(name=category, value=rule_text, inline=False)
        
        await ctx.send(embed=embed)

    @commands.command(name="testmembersync")
    @commands.admin()
    async def testmembersync(self, ctx: commands.Context, member: discord.Member):
        """Test MemberSync integration for debugging."""
        membersync = self.bot.get_cog("MemberSync")
        
        if not membersync:
            await ctx.send("❌ MemberSync cog not loaded!")
            return
        
        await ctx.send(f"Testing MemberSync for {member.mention}...")
        
        try:
            mc_data = await membersync.get_link_for_discord(member.id)
            
            if mc_data:
                output = "✅ **MemberSync Data Found:**\n"
                output += f"```json\n{json.dumps(mc_data, indent=2)}\n```"
                
                mc_id = mc_data.get("mc_user_id")
                if mc_id:
                    output += f"\n**MC Profile:** {_mc_profile_url(mc_id)}"
            else:
                output = "⚠️ No MemberSync link found for this member."
            
            await ctx.send(output)
            
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")
            log.exception(f"testmembersync error: {e}")
    @commands.guild_only()
    async def warnings(self, ctx: commands.Context, member: discord.Member = None):
        """Check active official warnings for a member."""
        if member is None:
            member = ctx.author
        
        warnings = self.db.get_active_warnings(ctx.guild.id, member.id)
        
        if not warnings:
            await ctx.send(f"{member.mention} has no active official warnings.")
            return
        
        embed = discord.Embed(
            title=f"⚠️ Active Official Warnings for {member.display_name}",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        
        for w in warnings:
            value = (
                f"**Type**: {w['sanction_type']}\n"
                f"**Reason**: {w['reason_detail'][:100]}\n"
                f"**Date**: {fmt_dt(w['created_at'])}\n"
                f"**Admin**: {w['admin_username']}"
            )
            embed.add_field(
                name=f"ID: {w['sanction_id']}",
                value=value,
                inline=False
            )
        
        embed.set_footer(text=f"Total active warnings: {len(warnings)}")
        
        await ctx.send(embed=embed)

    @commands.hybrid_command(name="mysanctions")
    @commands.guild_only()
    async def mysanctions(self, ctx: commands.Context):
        """View your own sanction history."""
        sanctions = self.db.get_user_sanctions(ctx.guild.id, ctx.author.id)
        
        if not sanctions:
            await ctx.send("You have no sanctions on record. Keep up the good work! ✅")
            return
        
        embed = discord.Embed(
            title="📋 Your Sanction History",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_author(name=ctx.author.display_name, icon_url=ctx.author.display_avatar.url)
        
        active_count = 0
        for s in sanctions[:10]:
            if s['status'] == 'active':
                active_count += 1
            
            status_emoji = "🔴" if s['status'] == 'active' else "⚫"
            value = (
                f"**Type**: {s['sanction_type']}\n"
                f"**Reason**: {s['reason_detail'][:100]}\n"
                f"**Date**: {fmt_dt(s['created_at'])}"
            )
            embed.add_field(
                name=f"{status_emoji} {s['sanction_type']}",
                value=value,
                inline=False
            )
        
        if len(sanctions) > 10:
            embed.set_footer(text=f"Showing 10 of {len(sanctions)} sanctions • {active_count} active")
        else:
            embed.set_footer(text=f"{active_count} active sanction(s)")
        
        await ctx.send(embed=embed, ephemeral=True)


async def setup(bot: Red):
    await bot.add_cog(SanctionsManager(bot))
