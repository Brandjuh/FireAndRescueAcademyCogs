"""
Data models for MemberManager
Type-safe dataclasses for member information
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any


@dataclass
class MemberData:
    """Complete member information from all sources."""
    
    # Identifiers
    discord_id: Optional[int] = None
    mc_user_id: Optional[str] = None
    
    # Discord information
    discord_username: Optional[str] = None
    discord_roles: List[str] = field(default_factory=list)
    discord_joined: Optional[datetime] = None
    discord_left: Optional[datetime] = None
    
    # MissionChief information
    mc_username: Optional[str] = None
    mc_role: Optional[str] = None
    contribution_rate: Optional[float] = None
    contribution_trend: Optional[str] = None  # "up", "down", "stable"
    mc_joined: Optional[datetime] = None
    mc_left: Optional[datetime] = None
    
    # Link status
    link_status: Optional[str] = None  # "approved", "pending", "none"
    link_created: Optional[datetime] = None
    
    # Stats
    notes_count: int = 0
    infractions_count: int = 0
    severity_score: int = 0
    events_count: int = 0
    
    # Flags
    on_watchlist: bool = False
    watchlist_reason: Optional[str] = None
    is_verified: bool = False
    
    def has_discord(self) -> bool:
        """Check if member has Discord data."""
        return self.discord_id is not None
    
    def has_mc(self) -> bool:
        """Check if member has MC data."""
        return self.mc_user_id is not None
    
    def is_linked(self) -> bool:
        """Check if Discord and MC accounts are linked."""
        return self.has_discord() and self.has_mc() and self.link_status == "approved"
    
    def get_display_name(self) -> str:
        """Get best available display name."""
        if self.discord_username:
            return self.discord_username
        if self.mc_username:
            return self.mc_username
        if self.discord_id:
            return f"Discord User {self.discord_id}"
        if self.mc_user_id:
            return f"MC User {self.mc_user_id}"
        return "Unknown User"
    
    def get_profile_url(self) -> Optional[str]:
        """Get MC profile URL if available."""
        if self.mc_user_id:
            return f"https://www.missionchief.com/users/{self.mc_user_id}"
        return None


@dataclass
class NoteData:
    """Note information."""
    
    note_id: int
    ref_code: str
    guild_id: int
    
    # Target
    discord_id: Optional[int]
    mc_user_id: Optional[str]
    
    # Content
    note_text: str
    author_id: int
    author_name: str
    
    # Links
    infraction_ref: Optional[str] = None
    sanction_ref: Optional[int] = None
    
    # Timestamps
    created_at: int = 0
    updated_at: Optional[int] = None
    expires_at: Optional[int] = None
    
    # Metadata
    updated_by: Optional[int] = None
    content_hash: str = ""
    status: str = "active"
    is_pinned: bool = False
    tags: Optional[List[str]] = None
    
    def is_expired(self) -> bool:
        """Check if note has expired."""
        if not self.expires_at:
            return False
        return self.expires_at < int(datetime.now().timestamp())
    
    def is_active(self) -> bool:
        """Check if note is active (not deleted or expired)."""
        return self.status == "active" and not self.is_expired()


@dataclass
class InfractionData:
    """Infraction information."""
    
    infraction_id: int
    ref_code: str
    guild_id: int
    
    # Target
    discord_id: Optional[int]
    mc_user_id: Optional[str]
    target_name: str
    
    # Infraction details
    platform: str  # "discord" or "missionchief"
    infraction_type: str  # "mute", "kick", "ban", "warning", "timeout"
    reason: str
    duration: Optional[int] = None  # seconds
    
    # Moderator
    moderator_id: int = 0
    moderator_name: str = ""
    
    # Timestamps
    created_at: int = 0
    expires_at: Optional[int] = None
    revoked_at: Optional[int] = None
    
    # Revocation
    revoked_by: Optional[int] = None
    revoke_reason: Optional[str] = None
    
    # Severity
    severity_score: int = 1
    status: str = "active"
    
    def is_expired(self) -> bool:
        """Check if infraction has expired."""
        if not self.expires_at:
            return False
        return self.expires_at < int(datetime.now().timestamp())
    
    def is_active(self) -> bool:
        """Check if infraction is active."""
        return self.status == "active" and not self.is_expired()
    
    def is_temporary(self) -> bool:
        """Check if this is a temporary punishment."""
        return self.duration is not None and self.expires_at is not None
    
    def get_platform_emoji(self) -> str:
        """Get emoji for platform."""
        return "💬" if self.platform == "discord" else "🚒"
    
    def get_type_emoji(self) -> str:
        """Get emoji for infraction type."""
        emoji_map = {
            "warning": "⚠️",
            "mute": "🔇",
            "kick": "👢",
            "ban": "🔨",
            "timeout": "⏰"
        }
        return emoji_map.get(self.infraction_type, "❓")


@dataclass
class EventData:
    """Member event information."""
    
    event_id: int
    guild_id: int
    
    # Target
    discord_id: Optional[int]
    mc_user_id: Optional[str]
    
    # Event details
    event_type: str
    event_data: Dict[str, Any]
    triggered_by: str  # "automation", "admin", "system"
    
    # Context
    actor_id: Optional[int] = None
    timestamp: int = 0
    notes: Optional[str] = None
    
    def get_type_emoji(self) -> str:
        """Get emoji for event type."""
        emoji_map = {
            "joined_discord": "📥",
            "left_discord": "📤",
            "joined_mc": "🚒",
            "left_mc": "🚪",
            "link_created": "🔗",
            "link_approved": "✅",
            "role_changed": "👔",
            "contribution_drop": "📉",
            "contribution_rise": "📈",
            "watchlist_added": "👁️",
            "watchlist_removed": "✔️"
        }
        return emoji_map.get(self.event_type, "📌")


@dataclass
class WatchlistEntry:
    """Watchlist entry information."""
    
    watchlist_id: int
    guild_id: int
    
    # Target
    discord_id: Optional[int]
    mc_user_id: Optional[str]
    
    # Details
    reason: str
    added_by: int
    added_at: int
    watch_type: str  # "contribution", "behavior", "probation", "general"
    
    # Alert configuration
    alert_threshold: Optional[Dict[str, Any]] = None
    
    # Status
    status: str = "active"
    resolved_at: Optional[int] = None
    resolved_by: Optional[int] = None
    resolution_notes: Optional[str] = None
    
    def is_active(self) -> bool:
        """Check if watchlist entry is active."""
        return self.status == "active"
    
    def get_type_emoji(self) -> str:
        """Get emoji for watch type."""
        emoji_map = {
            "contribution": "📊",
            "behavior": "⚠️",
            "probation": "🚨",
            "general": "👁️"
        }
        return emoji_map.get(self.watch_type, "👁️")


@dataclass
class ContributionTrend:
    """Contribution rate trend analysis."""
    
    mc_user_id: str
    current_rate: float
    previous_rate: Optional[float] = None
    
    # Trend calculation
    trend: str = "unknown"  # "rising", "falling", "stable", "unknown"
    change_percent: float = 0.0
    
    # Weekly breakdown
    weekly_rates: List[float] = field(default_factory=list)
    
    def is_concerning(self, threshold: float = 5.0) -> bool:
        """Check if contribution rate is below threshold."""
        return self.current_rate < threshold
    
    def is_dropping(self, drop_threshold: float = 1.0) -> bool:
        """Check if contribution is dropping significantly."""
        if not self.previous_rate:
            return False
        return self.change_percent < -drop_threshold
    
    def get_emoji(self) -> str:
        """Get emoji representing trend."""
        if self.trend == "rising":
            return "📈"
        elif self.trend == "falling":
            return "📉"
        elif self.trend == "stable":
            return "➡️"
        return "❓"
