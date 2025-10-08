import asyncio
import aiohttp
import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote, unquote

import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box, pagify

log = logging.getLogger("red.cog.building_manager")

# ---------- Utilities ----------

def ts() -> int:
    """Get current unix timestamp."""
    return int(datetime.now(timezone.utc).timestamp())

def fmt_dt(timestamp: int) -> str:
    """Format unix timestamp to Discord timestamp."""
    return f"<t:{timestamp}:F>"

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

# ---------- Location Parser ----------

class LocationParser:
    """Parse and geocode location inputs."""
    
    # Rate limiting for Nominatim (1 req/sec)
    _last_nominatim_call = 0
    _nominatim_delay = 1.0
    
    @staticmethod
    def extract_coordinates(text: str) -> Optional[Tuple[float, float]]:
        """Extract coordinates from various formats."""
        # Pattern 1: Google Maps ?q=lat,lon
        pattern1 = r'[?&]q=(-?\d+\.?\d*),\s*(-?\d+\.?\d*)'
        match = re.search(pattern1, text)
        if match:
            return (float(match.group(1)), float(match.group(2)))
        
        # Pattern 2: Google Maps /@lat,lon
        pattern2 = r'/@(-?\d+\.?\d*),\s*(-?\d+\.?\d*)'
        match = re.search(pattern2, text)
        if match:
            return (float(match.group(1)), float(match.group(2)))
        
        # Pattern 3: Direct coordinates "lat, lon" or "lat,lon"
        pattern3 = r'^(-?\d+\.?\d*)[,\s]+(-?\d+\.?\d*)$'
        match = re.search(pattern3, text.strip())
        if match:
            return (float(match.group(1)), float(match.group(2)))
        
        # Pattern 4: X: lat, Y: lon format
        pattern4 = r'X:\s*(-?\d+\.?\d*)[,\s]+Y:\s*(-?\d+\.?\d*)'
        match = re.search(pattern4, text, re.IGNORECASE)
        if match:
            return (float(match.group(1)), float(match.group(2)))
        
        return None
    
    @classmethod
    async def geocode_nominatim(cls, lat: float, lon: float) -> Optional[str]:
        """Reverse geocode using Nominatim."""
        # Rate limiting
        now = time.time()
        elapsed = now - cls._last_nominatim_call
        if elapsed < cls._nominatim_delay:
            await asyncio.sleep(cls._nominatim_delay - elapsed)
        
        cls._last_nominatim_call = time.time()
        
        url = f"https://nominatim.openstreetmap.org/reverse"
        params = {
            "lat": lat,
            "lon": lon,
            "format": "json",
            "addressdetails": 1
        }
        headers = {
            "User-Agent": "DiscordBot-BuildingManager/1.0"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        display_name = data.get("display_name")
                        return display_name
        except Exception as e:
            log.warning("Nominatim geocoding failed: %r", e)
        
        return None
    
    @staticmethod
    async def geocode_google(lat: float, lon: float, api_key: str) -> Optional[str]:
        """Reverse geocode using Google Geocoding API."""
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "latlng": f"{lat},{lon}",
            "key": api_key
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "OK" and data.get("results"):
                            return data["results"][0].get("formatted_address")
        except Exception as e:
            log.warning("Google geocoding failed: %r", e)
        
        return None

# ---------- Database ----------

class BuildingDatabase:
    """SQLite database for building requests."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize database tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Building requests table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_requests (
                request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                building_type TEXT NOT NULL,
                building_name TEXT NOT NULL,
                location_input TEXT NOT NULL,
                coordinates TEXT,
                address TEXT,
                notes TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        ''')
        
        # Building actions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_actions (
                action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                guild_id INTEGER NOT NULL,
                admin_user_id INTEGER,
                admin_username TEXT,
                action_type TEXT NOT NULL,
                denial_reason TEXT,
                previous_values TEXT,
                timestamp INTEGER NOT NULL,
                FOREIGN KEY (request_id) REFERENCES building_requests(request_id)
            )
        ''')
        
        # Geocoding cache table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS geocoding_cache (
                location_input TEXT PRIMARY KEY,
                coordinates TEXT,
                address TEXT,
                provider TEXT,
                cached_at INTEGER
            )
        ''')
        
        # Building types table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_types (
                type_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                type_name TEXT NOT NULL,
                emoji TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                created_at INTEGER NOT NULL,
                UNIQUE(guild_id, type_name)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def add_request(self, guild_id: int, user_id: int, username: str, building_type: str,
                   building_name: str, location_input: str, coordinates: Optional[str],
                   address: Optional[str], notes: Optional[str]) -> int:
        """Add a new building request."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = ts()
        cursor.execute('''
            INSERT INTO building_requests 
            (guild_id, user_id, username, building_type, building_name, location_input,
             coordinates, address, notes, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
        ''', (guild_id, user_id, username, building_type, building_name, location_input,
              coordinates, address, notes, now, now))
        
        request_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return request_id
    
    def update_request_status(self, request_id: int, status: str):
        """Update request status."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE building_requests 
            SET status = ?, updated_at = ?
            WHERE request_id = ?
        ''', (status, ts(), request_id))
        
        conn.commit()
        conn.close()
    
    def add_action(self, request_id: int, guild_id: int, admin_user_id: Optional[int],
                  admin_username: Optional[str], action_type: str, denial_reason: Optional[str] = None,
                  previous_values: Optional[str] = None):
        """Log an action on a request."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO building_actions
            (request_id, guild_id, admin_user_id, admin_username, action_type, 
             denial_reason, previous_values, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (request_id, guild_id, admin_user_id, admin_username, action_type,
              denial_reason, previous_values, ts()))
        
        conn.commit()
        conn.close()
    
    def get_cached_geocode(self, location_input: str) -> Optional[Tuple[str, str, str]]:
        """Get cached geocoding result."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT coordinates, address, provider 
            FROM geocoding_cache 
            WHERE location_input = ?
        ''', (location_input,))
        
        result = cursor.fetchone()
        conn.close()
        
        return result if result else None
    
    def cache_geocode(self, location_input: str, coordinates: str, address: str, provider: str):
        """Cache geocoding result."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO geocoding_cache
            (location_input, coordinates, address, provider, cached_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (location_input, coordinates, address, provider, ts()))
        
        conn.commit()
        conn.close()
    
def get_stats_overall(self, guild_id: int) -> dict:
        """Get overall statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total counts by status
        cursor.execute('''
            SELECT status, COUNT(*) 
            FROM building_requests 
            WHERE guild_id = ?
            GROUP BY status
        ''', (guild_id,))
        status_counts = dict(cursor.fetchall())
        
        # By building type
        cursor.execute('''
            SELECT building_type, status, COUNT(*)
            FROM building_requests
            WHERE guild_id = ?
            GROUP BY building_type, status
        ''', (guild_id,))
        type_stats = cursor.fetchall()
        
        # Top requesters
        cursor.execute('''
            SELECT username, COUNT(*) as count
            FROM building_requests
            WHERE guild_id = ?
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 5
        ''', (guild_id,))
        top_requesters = cursor.fetchall()
        
        # Top admins
        cursor.execute('''
            SELECT admin_username, COUNT(*) as count
            FROM building_actions
            WHERE guild_id = ? AND admin_user_id IS NOT NULL
            GROUP BY admin_user_id
            ORDER BY count DESC
            LIMIT 5
        ''', (guild_id,))
        top_admins = cursor.fetchall()
        
        # Average response time
        cursor.execute('''
            SELECT AVG(ba.timestamp - br.created_at) as avg_time
            FROM building_requests br
            JOIN building_actions ba ON br.request_id = ba.request_id
            WHERE br.guild_id = ? AND ba.action_type IN ('approved', 'denied')
        ''', (guild_id,))
        avg_response_result = cursor.fetchone()
        avg_response_time = avg_response_result[0] if avg_response_result[0] else 0
        
        conn.close()
        
        return {
            "status_counts": status_counts,
            "type_stats": type_stats,
            "top_requesters": top_requesters,
            "top_admins": top_admins,
            "avg_response_time": avg_response_time
        }
    
    def get_stats_user(self, guild_id: int, user_id: int) -> dict:
        """Get user statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total counts by status
        cursor.execute('''
            SELECT status, COUNT(*) 
            FROM building_requests 
            WHERE guild_id = ? AND user_id = ?
            GROUP BY status
        ''', (guild_id, user_id))
        status_counts = dict(cursor.fetchall())
        
        # By building type
        cursor.execute('''
            SELECT building_type, status, COUNT(*)
            FROM building_requests
            WHERE guild_id = ? AND user_id = ?
            GROUP BY building_type, status
        ''', (guild_id, user_id))
        type_stats = cursor.fetchall()
        
        # Denial reasons
        cursor.execute('''
            SELECT ba.denial_reason, COUNT(*) as count
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE br.guild_id = ? AND br.user_id = ? AND ba.action_type = 'denied'
            GROUP BY ba.denial_reason
            ORDER BY count DESC
        ''', (guild_id, user_id))
        denial_reasons = cursor.fetchall()
        
        # Recent requests
        cursor.execute('''
            SELECT building_type, building_name, status, created_at
            FROM building_requests
            WHERE guild_id = ? AND user_id = ?
            ORDER BY created_at DESC
            LIMIT 5
        ''', (guild_id, user_id))
        recent_requests = cursor.fetchall()
        
        conn.close()
        
        return {
            "status_counts": status_counts,
            "type_stats": type_stats,
            "denial_reasons": denial_reasons,
            "recent_requests": recent_requests
        }
    
    def get_stats_admin(self, guild_id: int, admin_user_id: int) -> dict:
        """Get admin statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total actions by type
        cursor.execute('''
            SELECT action_type, COUNT(*)
            FROM building_actions
            WHERE guild_id = ? AND admin_user_id = ?
            GROUP BY action_type
        ''', (guild_id, admin_user_id))
        action_counts = dict(cursor.fetchall())
        
        # By building type
        cursor.execute('''
            SELECT br.building_type, ba.action_type, COUNT(*)
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE ba.guild_id = ? AND ba.admin_user_id = ?
            GROUP BY br.building_type, ba.action_type
        ''', (guild_id, admin_user_id))
        type_stats = cursor.fetchall()
        
        # Denial reasons breakdown
        cursor.execute('''
            SELECT denial_reason, COUNT(*) as count
            FROM building_actions
            WHERE guild_id = ? AND admin_user_id = ? AND action_type = 'denied'
            GROUP BY denial_reason
            ORDER BY count DESC
        ''', (guild_id, admin_user_id))
        denial_breakdown = cursor.fetchall()
        
        # Response times
        cursor.execute('''
            SELECT 
                AVG(ba.timestamp - br.created_at) as avg_time,
                MIN(ba.timestamp - br.created_at) as min_time,
                MAX(ba.timestamp - br.created_at) as max_time
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE ba.guild_id = ? AND ba.admin_user_id = ? AND ba.action_type IN ('approved', 'denied')
        ''', (guild_id, admin_user_id))
        response_times = cursor.fetchone()
        
        # Recent actions
        cursor.execute('''
            SELECT ba.action_type, br.building_type, br.username, ba.timestamp
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE ba.guild_id = ? AND ba.admin_user_id = ?
            ORDER BY ba.timestamp DESC
            LIMIT 5
        ''', (guild_id, admin_user_id))
        recent_actions = cursor.fetchall()
        
        conn.close()
        
        return {
            "action_counts": action_counts,
            "type_stats": type_stats,
            "denial_breakdown": denial_breakdown,
            "response_times": response_times or (0, 0, 0),
            "recent_actions": recent_actions
        }
    
    def get_stats_type(self, guild_id: int, building_type: str) -> dict:
        """Get building type statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total counts by status
        cursor.execute('''
            SELECT status, COUNT(*)
            FROM building_requests
            WHERE guild_id = ? AND building_type = ?
            GROUP BY status
        ''', (guild_id, building_type))
        status_counts = dict(cursor.fetchall())
        
        # Top requesters for this type
        cursor.execute('''
            SELECT username, COUNT(*) as count
            FROM building_requests
            WHERE guild_id = ? AND building_type = ?
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 5
        ''', (guild_id, building_type))
        top_requesters = cursor.fetchall()
        
        # Most common denial reason
        cursor.execute('''
            SELECT ba.denial_reason, COUNT(*) as count
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE br.guild_id = ? AND br.building_type = ? AND ba.action_type = 'denied'
            GROUP BY ba.denial_reason
            ORDER BY count DESC
            LIMIT 1
        ''', (guild_id, building_type))
        common_denial = cursor.fetchone()
        
        # Approval rate by admin
        cursor.execute('''
            SELECT 
                ba.admin_username,
                SUM(CASE WHEN ba.action_type = 'approved' THEN 1 ELSE 0 END) as approved,
                COUNT(*) as total
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE br.guild_id = ? AND br.building_type = ? 
              AND ba.action_type IN ('approved', 'denied')
              AND ba.admin_user_id IS NOT NULL
            GROUP BY ba.admin_user_id
        ''', (guild_id, building_type))
        admin_rates = cursor.fetchall()
        
        conn.close()
        
        return {
            "status_counts": status_counts,
            "top_requesters": top_requesters,
            "common_denial": common_denial,
            "admin_rates": admin_rates
        }

# ---------- Models ----------

class BuildingRequest:
    def __init__(
        self,
        user_id: int,
        username: str,
        building_type: str,
        building_name: str,
        location_input: str,
        coordinates: Optional[str] = None,
        address: Optional[str] = None,
        notes: Optional[str] = None,
        request_id: Optional[int] = None,
    ):
        self.user_id = user_id
        self.username = username
        self.building_type = building_type
        self.building_name = building_name
        self.location_input = location_input
        self.coordinates = coordinates
        self.address = address
        self.notes = notes
        self.request_id = request_id

# ---------- Views ----------

class StartView(discord.ui.View):
    def __init__(self, cog: "BuildingManager"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Request Building", style=discord.ButtonStyle.primary, custom_id="bm:start")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Select the type of building you want to request.",
            view=BuildingTypeView(self.cog),
            ephemeral=True,
        )

class BuildingTypeView(discord.ui.View):
    def __init__(self, cog: "BuildingManager"):
        super().__init__(timeout=600)
        self.cog = cog
        self.add_item(BuildingTypeSelect(self.cog))

class BuildingTypeSelect(discord.ui.Select):
    def __init__(self, cog: "BuildingManager"):
        self.cog = cog
        # Default types
        options = [
            discord.SelectOption(label="Hospital", emoji="🏥", description="Medical facility"),
            discord.SelectOption(label="Prison", emoji="🔒", description="Correctional facility"),
        ]
        super().__init__(placeholder="Choose a building type", min_values=1, max_values=1, options=options, custom_id="bm:type")

    async def callback(self, interaction: discord.Interaction):
        building_type = self.values[0]
        modal = BuildingRequestModal(self.cog, building_type)
        await interaction.response.send_modal(modal)

class BuildingRequestModal(discord.ui.Modal, title="Building Request"):
    building_name = discord.ui.TextInput(
        label="Building Name",
        style=discord.TextStyle.short,
        max_length=100,
        required=True,
        placeholder="e.g., Central Medical Center",
    )
    
    location = discord.ui.TextInput(
        label="Location (Google Maps link or coordinates)",
        style=discord.TextStyle.short,
        max_length=500,
        required=True,
        placeholder="Paste Google Maps link, coordinates, or description",
    )
    
    notes = discord.ui.TextInput(
        label="Additional Notes (Optional)",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=False,
        placeholder="Any additional information...",
    )

    def __init__(self, cog: "BuildingManager", building_type: str):
        super().__init__()
        self.cog = cog
        self.building_type = building_type

    async def on_submit(self, interaction: discord.Interaction):
        # Parse location
        await interaction.response.defer(ephemeral=True)
        
        location_input = str(self.location)
        coords = LocationParser.extract_coordinates(location_input)
        
        coordinates_str = None
        address = None
        
        if coords:
            lat, lon = coords
            coordinates_str = f"{lat}, {lon}"
            
            # Check cache first
            cached = self.cog.db.get_cached_geocode(location_input)
            if cached:
                _, address, _ = cached
            else:
                # Try Nominatim first
                address = await LocationParser.geocode_nominatim(lat, lon)
                provider = "nominatim"
                
                # Fallback to Google if available and Nominatim failed
                if not address:
                    google_key = await self.cog.config.google_api_key()
                    if google_key:
                        address = await LocationParser.geocode_google(lat, lon, google_key)
                        provider = "google"
                
                # Cache result
                if address:
                    self.cog.db.cache_geocode(location_input, coordinates_str, address, provider)
        
        # Create request object
        req = BuildingRequest(
            user_id=interaction.user.id,
            username=str(interaction.user),
            building_type=self.building_type,
            building_name=str(self.building_name),
            location_input=location_input,
            coordinates=coordinates_str,
            address=address,
            notes=str(self.notes) if self.notes.value else None,
        )
        
        # Show summary
        view = SummaryView(self.cog, req)
        await view.send_summary(interaction)

class SummaryView(discord.ui.View):
    def __init__(self, cog: "BuildingManager", req: BuildingRequest):
        super().__init__(timeout=600)
        self.cog = cog
        self.req = req

    async def send_summary(self, interaction: discord.Interaction):
        """Display the summary embed."""
        embed = self._create_embed(interaction.user)
        await safe_update(
            interaction,
            content="⚠️ **Warning**: Once submitted, you cannot edit this request!\n\nReview your request:",
            embed=embed,
            view=self
        )

    def _create_embed(self, user: discord.User) -> discord.Embed:
        """Create summary embed."""
        emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
        emoji = emoji_map.get(self.req.building_type, "🏢")
        
        embed = discord.Embed(
            title=f"{emoji} Building Request Summary",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        
        embed.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Building Type", value=self.req.building_type, inline=True)
        embed.add_field(name="Building Name", value=self.req.building_name, inline=True)
        embed.add_field(name="Location Input", value=self.req.location_input[:100], inline=False)
        
        if self.req.coordinates:
            embed.add_field(name="📍 Coordinates", value=self.req.coordinates, inline=True)
        else:
            embed.add_field(name="📍 Coordinates", value="Not detected", inline=True)
        
        if self.req.address:
            embed.add_field(name="📫 Address", value=self.req.address[:200], inline=False)
        
        if self.req.notes:
            embed.add_field(name="Notes", value=self.req.notes[:200], inline=False)
        
        return embed

    @discord.ui.button(label="✏️ Edit", style=discord.ButtonStyle.secondary, custom_id="bm:edit")
    async def edit(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = BuildingRequestModal(self.cog, self.req.building_type)
        modal.building_name.default = self.req.building_name
        modal.location.default = self.req.location_input
        if self.req.notes:
            modal.notes.default = self.req.notes
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="📤 Submit to Admin", style=discord.ButtonStyle.success, custom_id="bm:submit")
    async def submit(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This only works inside a server.", ephemeral=True)
            return

        conf = await self.cog.config.guild(guild).all()
        admin_channel_id = conf.get("admin_channel_id")
        log_channel_id = conf.get("log_channel_id")

        if not admin_channel_id or not log_channel_id:
            await interaction.response.send_message(
                "Admin/Log channels are not configured yet. Ask an admin to use [p]buildset.",
                ephemeral=True,
            )
            return

        admin_channel = guild.get_channel(admin_channel_id)
        log_channel = guild.get_channel(log_channel_id)

        if not admin_channel or not log_channel:
            await interaction.response.send_message("One or more configured channels could not be found.", ephemeral=True)
            return

        # Save to database
        request_id = self.cog.db.add_request(
            guild_id=guild.id,
            user_id=self.req.user_id,
            username=self.req.username,
            building_type=self.req.building_type,
            building_name=self.req.building_name,
            location_input=self.req.location_input,
            coordinates=self.req.coordinates,
            address=self.req.address,
            notes=self.req.notes
        )
        
        self.req.request_id = request_id

        # Send to admin channel
        emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
        emoji = emoji_map.get(self.req.building_type, "🏢")
        
        emb = discord.Embed(
            title=f"{emoji} New Building Request",
            color=discord.Color.yellow(),
            timestamp=datetime.now(timezone.utc),
        )
        
        user = interaction.user
        emb.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        emb.add_field(name="Building Type", value=self.req.building_type, inline=True)
        emb.add_field(name="Building Name", value=self.req.building_name, inline=True)
        emb.add_field(name="Location Input", value=self.req.location_input[:100], inline=False)
