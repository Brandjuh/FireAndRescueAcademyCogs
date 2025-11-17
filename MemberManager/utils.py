"""
Utility functions for MemberManager
Fuzzy search, formatting, and helper functions

COMPLETE VERSION - No sanctions-specific changes needed
All sanction utilities are in SanctionManager

🔧 FIXED: Added format_historical_trend() for list of rates
"""

import re
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from difflib import SequenceMatcher

import discord


def fuzzy_match_score(a: str, b: str) -> float:
    """
    Calculate fuzzy match score between two strings.
    Returns 0.0 to 1.0 (higher is better match).
    """
    if not a or not b:
        return 0.0
    
    a_lower = a.lower().strip()
    b_lower = b.lower().strip()
    
    # Exact match
    if a_lower == b_lower:
        return 1.0
    
    # Contains match
    if a_lower in b_lower or b_lower in a_lower:
        return 0.9
    
    # Sequence matcher
    return SequenceMatcher(None, a_lower, b_lower).ratio()


async def fuzzy_search_member(
    target: str,
    guild: discord.Guild,
    membersync,
    alliance_scraper,
    threshold: float = 0.6,
    limit: int = 1
) -> Optional[Dict[str, Any]]:
    """
    Fuzzy search for a member across Discord and MC databases.
    
    Args:
        target: Search query
        guild: Discord guild
        membersync: MemberSync cog instance
        alliance_scraper: AllianceScraper cog instance
        threshold: Minimum match score (0.0-1.0)
        limit: Max results to return (default 1 for single best match)
    
    Returns:
        Dict with discord_id and/or mc_user_id, or None
        If limit > 1, returns list of dicts
    """
    target_clean = target.lower().strip()
    results = []
    
    # Search Discord members
    for member in guild.members:
        if member.bot:
            continue
        
        # Check username
        score = fuzzy_match_score(target_clean, str(member))
        if score >= threshold:
            results.append({
                "score": score,
                "discord_id": member.id,
                "mc_user_id": None,
                "name": str(member),
                "source": "discord"
            })
        
        # Check display name
        score = fuzzy_match_score(target_clean, member.display_name)
        if score >= threshold:
            results.append({
                "score": score,
                "discord_id": member.id,
                "mc_user_id": None,
                "name": member.display_name,
                "source": "discord"
            })
    
    # Search MC members via AllianceScraper
    if alliance_scraper:
        try:
            mc_members = await alliance_scraper.get_members()
            
            for mc_member in mc_members:
                mc_name = mc_member.get("name", "")
                mc_id = mc_member.get("user_id") or mc_member.get("mc_user_id")
                
                if not mc_id:
                    continue
                
                # Check MC username
                score = fuzzy_match_score(target_clean, mc_name)
                if score >= threshold:
                    # Try to find linked Discord account
                    discord_id = None
                    if membersync:
                        link = await membersync.get_link_for_mc(mc_id)
                        if link:
                            discord_id = link.get("discord_id")
                    
                    results.append({
                        "score": score,
                        "discord_id": discord_id,
                        "mc_user_id": mc_id,
                        "name": mc_name,
                        "source": "missionchief"
                    })
        except Exception:
            # Silently fail - AllianceScraper might not be available
            pass
    
    if not results:
        return None if limit == 1 else []
    
    # Sort by score (highest first)
    results.sort(key=lambda x: x["score"], reverse=True)
    
    if limit == 1:
        return results[0]
    else:
        return results[:limit]


def format_contribution_trend(
    current: float,
    previous: Optional[float] = None,
    use_emoji: bool = True
) -> str:
    """
    Format contribution rate with trend indicator.
    
    Examples:
        - "8.5% ⬇️ (-1.2%)"
        - "5.0% ➡️"
        - "12.3% ⬆️ (+2.5%)"
    """
    if current is None:
        return "*No data*"
    
    current_str = f"{current:.1f}%"
    
    if previous is None or previous == 0:
        return current_str
    
    change = current - previous
    change_str = f"{change:+.1f}%"
    
    # Determine emoji
    if use_emoji:
        if abs(change) < 0.5:
            emoji = "➡️"
        elif change > 0:
            emoji = "⬆️"
        else:
            emoji = "⬇️"
        
        return f"{current_str} {emoji} ({change_str})"
    else:
        trend_word = "stable" if abs(change) < 0.5 else ("rising" if change > 0 else "falling")
        return f"{current_str} ({trend_word}, {change_str})"


def format_historical_trend(historical_rates: List[float], max_display: int = 4) -> str:
    """
    Format historical contribution rates as a trend string.
    
    🔧 NEW FUNCTION: For displaying lists of historical rates.
    
    Args:
        historical_rates: List of rates (most recent first)
        max_display: Maximum number of rates to display
    
    Returns:
        Formatted string like "8.5% → 7.2% → 6.8% → 5.5%"
    
    Examples:
        >>> format_historical_trend([8.5, 7.2, 6.8, 5.5])
        "8.5% → 7.2% → 6.8% → 5.5%"
        
        >>> format_historical_trend([])
        "No data"
    """
    if not historical_rates:
        return "No data"
    
    rates_to_show = historical_rates[:max_display]
    return " → ".join(f"{rate:.1f}%" for rate in rates_to_show)


def format_timestamp(timestamp: int, style: str = "F") -> str:
    """
    Format Unix timestamp as Discord timestamp.
    
    Styles:
        - "F": Long date/time (default)
        - "R": Relative time (e.g., "2 hours ago")
        - "D": Short date
        - "T": Short time
        - "f": Short date/time
        - "d": Long date
        - "t": Long time
    """
    return f"<t:{timestamp}:{style}>"


def format_duration(seconds: int) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Examples:
        - 3600 -> "1 hour"
        - 86400 -> "1 day"
        - 604800 -> "1 week"
    """
    if seconds < 60:
        return f"{seconds} second{'s' if seconds != 1 else ''}"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    elif seconds < 86400:
        hours = seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''}"
    elif seconds < 604800:
        days = seconds // 86400
        return f"{days} day{'s' if days != 1 else ''}"
    else:
        weeks = seconds // 604800
        return f"{weeks} week{'s' if weeks != 1 else ''}"


def truncate_text(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate text to max length with suffix.
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def parse_duration_string(duration_str: str) -> Optional[int]:
    """
    Parse duration string to seconds.
    
    Examples:
        - "1h" -> 3600
        - "2d" -> 172800
        - "30m" -> 1800
        - "1w" -> 604800
    """
    duration_str = duration_str.lower().strip()
    
    # Pattern: number + unit
    match = re.match(r'^(\d+)([smhdw])$', duration_str)
    if not match:
        return None
    
    value = int(match.group(1))
    unit = match.group(2)
    
    multipliers = {
        's': 1,
        'm': 60,
        'h': 3600,
        'd': 86400,
        'w': 604800
    }
    
    return value * multipliers.get(unit, 0)


def sanitize_username(username: str) -> str:
    """
    Sanitize username for safe display (remove @everyone, @here, etc).
    """
    return discord.utils.escape_mentions(username)


def get_severity_emoji(score: int) -> str:
    """
    Get emoji based on severity score.
    
    Score ranges:
        - 0-2: 🟢 Low
        - 3-5: 🟡 Medium
        - 6-9: 🟠 High
        - 10+: 🔴 Critical
    """
    if score <= 2:
        return "🟢"
    elif score <= 5:
        return "🟡"
    elif score <= 9:
        return "🟠"
    else:
        return "🔴"


def format_role_list(roles: List[str], max_display: int = 5) -> str:
    """
    Format role list for display.
    
    Examples:
        - ["Admin", "Moderator"] -> "Admin, Moderator"
        - [] -> "None"
        - [10 roles] -> "Admin, Moderator, Member, +7 more"
    """
    if not roles:
        return "None"
    
    if len(roles) <= max_display:
        return ", ".join(roles)
    
    displayed = roles[:max_display]
    remaining = len(roles) - max_display
    return f"{', '.join(displayed)}, +{remaining} more"


def calculate_contribution_trend(
    rates: List[float],
    weeks: int = 3
) -> Dict[str, Any]:
    """
    Calculate contribution trend from historical rates.
    
    Args:
        rates: List of contribution rates (most recent first)
        weeks: Number of weeks to analyze
    
    Returns:
        Dict with trend, change_percent, and analysis
    """
    if not rates or len(rates) < 2:
        return {
            "trend": "unknown",
            "change_percent": 0.0,
            "analysis": "Insufficient data"
        }
    
    # Take only the requested number of weeks
    rates = rates[:weeks]
    
    current = rates[0]
    previous = rates[-1]
    
    change = current - previous
    change_percent = (change / previous * 100) if previous > 0 else 0
    
    # Determine trend
    if abs(change_percent) < 5:
        trend = "stable"
        analysis = "Contribution rate is stable"
    elif change_percent > 0:
        trend = "rising"
        analysis = f"Contribution rate increased by {change_percent:.1f}%"
    else:
        trend = "falling"
        analysis = f"Contribution rate decreased by {abs(change_percent):.1f}%"
    
    return {
        "trend": trend,
        "change_percent": change_percent,
        "analysis": analysis,
        "current": current,
        "previous": previous
    }


def format_member_identifier(
    discord_id: Optional[int] = None,
    mc_user_id: Optional[str] = None,
    discord_username: Optional[str] = None,
    mc_username: Optional[str] = None
) -> str:
    """
    Format member identifier for display.
    
    Prioritizes usernames over IDs.
    """
    if discord_username:
        return discord_username
    if mc_username:
        return mc_username
    if discord_id:
        return f"<@{discord_id}>"
    if mc_user_id:
        return f"MC User {mc_user_id}"
    return "Unknown User"


def paginate_list(
    items: List[Any],
    page: int = 1,
    per_page: int = 10
) -> Dict[str, Any]:
    """
    Paginate a list of items.
    
    Returns:
        Dict with items, page, total_pages, has_next, has_prev
    """
    total = len(items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    
    # Clamp page to valid range
    page = max(1, min(page, total_pages))
    
    start = (page - 1) * per_page
    end = start + per_page
    
    return {
        "items": items[start:end],
        "page": page,
        "per_page": per_page,
        "total": total,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1
    }


def is_valid_mc_id(mc_id: str) -> bool:
    """
    Validate MC user ID format.
    
    MC IDs are typically numeric strings.
    """
    if not mc_id:
        return False
    
    # Check if it's a reasonable numeric string
    return mc_id.isdigit() and len(mc_id) <= 15


def is_valid_discord_id(discord_id: int) -> bool:
    """
    Validate Discord user ID.
    
    Discord IDs are snowflakes (64-bit integers).
    """
    return 1000000000000000000 <= discord_id <= 9999999999999999999


def format_notes_preview(notes: List[Dict[str, Any]], max_notes: int = 3) -> str:
    """
    Format a preview of recent notes.
    
    Returns formatted string with note summaries.
    """
    if not notes:
        return "No notes"
    
    preview_notes = notes[:max_notes]
    lines = []
    
    for note in preview_notes:
        ref = note.get("ref_code", "???")
        text = note.get("note_text", "")
        preview = truncate_text(text, 50)
        lines.append(f"• `{ref}`: {preview}")
    
    if len(notes) > max_notes:
        remaining = len(notes) - max_notes
        lines.append(f"*...and {remaining} more*")
    
    return "\n".join(lines)


def format_infractions_summary(infractions: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Summarize infractions by type and platform.
    
    Returns:
        Dict with counts by type, platform, and time periods
    """
    summary = {
        "total": len(infractions),
        "active": 0,
        "discord": 0,
        "missionchief": 0,
        "by_type": {},
        "last_30_days": 0,
        "last_90_days": 0
    }
    
    now = int(datetime.now(timezone.utc).timestamp())
    thirty_days = 30 * 86400
    ninety_days = 90 * 86400
    
    for infraction in infractions:
        # Status
        if infraction.get("status") == "active":
            summary["active"] += 1
        
        # Platform
        platform = infraction.get("platform", "unknown")
        if platform in ["discord", "missionchief"]:
            summary[platform] += 1
        
        # Type
        inf_type = infraction.get("infraction_type", "unknown")
        summary["by_type"][inf_type] = summary["by_type"].get(inf_type, 0) + 1
        
        # Time periods
        created = infraction.get("created_at", 0)
        if now - created <= thirty_days:
            summary["last_30_days"] += 1
        if now - created <= ninety_days:
            summary["last_90_days"] += 1
    
    return summary


def build_mc_profile_url(mc_user_id: str) -> str:
    """Build MissionChief profile URL."""
    return f"https://www.missionchief.com/users/{mc_user_id}"


def extract_discord_id_from_mention(mention: str) -> Optional[int]:
    """
    Extract Discord ID from mention string.
    
    Examples:
        - "<@123456789>" -> 123456789
        - "<@!123456789>" -> 123456789
    """
    match = re.match(r'^<@!?(\d+)>$', mention)
    if match:
        return int(match.group(1))
    return None


def format_export_filename(
    export_type: str,
    guild_name: str,
    target_name: Optional[str] = None
) -> str:
    """
    Generate filename for exports.
    
    Example:
        - "notes_FireRescueAcademy_JohnDoe_2025-03-15.json"
    """
    timestamp = datetime.now().strftime("%Y-%m-%d")
    
    # Sanitize guild name
    guild_clean = re.sub(r'[^\w\s-]', '', guild_name).strip().replace(' ', '')
    
    parts = [export_type, guild_clean]
    
    if target_name:
        target_clean = re.sub(r'[^\w\s-]', '', target_name).strip().replace(' ', '')
        parts.append(target_clean)
    
    parts.append(timestamp)
    
    return f"{'_'.join(parts)}.json"


def is_concerning_contribution(
    current_rate: float,
    threshold: float = 5.0,
    previous_rate: Optional[float] = None,
    drop_threshold: float = 2.0
) -> tuple[bool, Optional[str]]:
    """
    Check if contribution rate is concerning.
    
    Returns:
        (is_concerning, reason)
    """
    # Below absolute threshold
    if current_rate < threshold:
        return True, f"Below {threshold}% threshold"
    
    # Significant drop
    if previous_rate and current_rate < previous_rate - drop_threshold:
        drop = previous_rate - current_rate
        return True, f"Dropped {drop:.1f}% from {previous_rate:.1f}%"
    
    return False, None
