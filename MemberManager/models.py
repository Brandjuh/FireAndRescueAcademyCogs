"""
Data models for MemberManager
Type-safe dataclasses for member information

🔧 UPDATED: Added audit-related fields
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
    link_status: Optional[str] = None  # "approved", "pending", "denied", "none"
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
        """Check if Discord and MC accounts are properly linked."""
        return (
            self.has_discord() and 
            self.has_mc() and 
            self.link_status == "approved" and
            self.is_verified
        )
    
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
    discord_id: Optional[int]
    mc_user_id: Optional[str]
    note_text: str
    author_id: int
    author_name: str
    created_at: int
    
    # Optional fields
    infraction_ref: Optional[str] = None
    sanction_ref: Optional[int] = None
    updated_at: Optional[int] = None
    updated_by: Optional[int] = None
    updated_by_name: Optional[str] = None  # 🔧 NEW
    expires_at: Optional[int] = None
    status: str = "active"
    is_pinned: bool = False
    tags: Optional[List[str]] = None
    
    def is_active(self) -> bool:
        """Check if note is active."""
        return self.status == "active"
    
    def is_expired(self) -> bool:
        """Check if note has expired."""
        if not self.expires_at:
            return False
        return int(datetime.now().timestamp()) > self.expires_at


@dataclass
class InfractionData:
    """Infraction information."""
    
    infraction_id: int
    ref_code: str
    guild_id: int
    discord_id: Optional[int]
    mc_user_id: Optional[str]
    target_name: str
    platform: str  # "discord" or "missionchief"
    infraction_type: str
    reason: str
    moderator_id: int
    moderator_name: str
    created_at: int
    
    # Optional fields
    duration: Optional[int] = None
    expires_at: Optional[int] = None
    revoked_at: Optional[int] = None
    revoked_by: Optional[int] = None
    revoke_reason: Optional[str] = None
    severity_score: int = 1
    status: str = "active"
    
    def is_active(self) -> bool:
        """Check if infraction is active."""
        return self.status == "active"
    
    def is_expired(self) -> bool:
        """Check if infraction has expired."""
        if not self.expires_at:
            return False
        return int(datetime.now().timestamp()) > self.expires_at
    
    def is_revoked(self) -> bool:
        """Check if infraction was revoked."""
        return self.status == "revoked"


@dataclass
class AuditEntry:
    """🔧 NEW: Audit log entry."""
    
    audit_id: int
    guild_id: int
    action_type: str
    action_target: str
    actor_id: int
    actor_name: str
    timestamp: int
    
    # Optional fields
    discord_id: Optional[int] = None
    mc_user_id: Optional[str] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def get_action_display(self) -> str:
        """Get human-readable action type."""
        return self.action_type.replace("_", " ").title()


@dataclass
class EventData:
    """Member event information."""
    
    event_id: int
    guild_id: int
    event_type: str
    event_data: Dict[str, Any]
    triggered_by: str
    timestamp: int
    
    # Optional fields
    discord_id: Optional[int] = None
    mc_user_id: Optional[str] = None
    actor_id: Optional[int] = None
    notes: Optional[str] = None
    
    def get_event_display(self) -> str:
        """Get human-readable event type."""
        return self.event_type.replace("_", " ").title()


@dataclass
class WatchlistEntry:
    """Watchlist entry information."""
    
    watchlist_id: int
    guild_id: int
    reason: str
    added_by: int
    added_at: int
    watch_type: str
    status: str = "active"
    
    # Optional fields
    discord_id: Optional[int] = None
    mc_user_id: Optional[str] = None
    alert_threshold: Optional[str] = None
    resolved_at: Optional[int] = None
    resolved_by: Optional[int] = None
    resolution_notes: Optional[str] = None
    
    def is_active(self) -> bool:
        """Check if watchlist entry is active."""
        return self.status == "active"
    
    def is_resolved(self) -> bool:
        """Check if watchlist entry is resolved."""
        return self.status == "resolved"
