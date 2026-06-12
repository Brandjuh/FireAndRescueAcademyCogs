"""
Audit timeline helpers for MemberManager.

This module only normalizes data that is already stored by other cogs. It does
not scrape MissionChief and it does not deduplicate LogsScraper rows, because
repeated in-game actions can be legitimate separate events.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import aiosqlite


ADMIN_EVENT_TYPES = {
    "note_created",
    "note_edited",
    "note_deleted",
    "note_pinned",
    "note_unpinned",
    "infraction_added",
    "infraction_revoked",
    "sanction_added",
    "sanction_edited",
    "sanction_removed",
    "link_created",
    "link_approved",
    "link_denied",
    "role_changed",
    "contribution_drop",
    "role_restored",
}

EXCLUDED_PERSON_AUDIT_ACTION_KEYS = {
    # Course completions are course-level alliance logs, not member-specific
    # records. Keeping them in a member audit creates false personal activity.
    "course_completed",
}

PERSON_AUDIT_ACTION_KEYS = {
    "added_to_alliance",
    "left_alliance",
    "kicked_from_alliance",
    "chat_ban_removed",
    "chat_ban_set",
    "set_admin",
    "removed_admin",
    "set_co_admin",
    "removed_co_admin",
    "set_mod_action_admin",
    "removed_mod_action_admin",
    "set_as_staff",
    "removed_as_staff",
    "promoted_to_event_manager",
    "removed_event_manager",
}

OPERATIONS_ACTION_KEYS = {
    "large_mission_started",
    "alliance_event_started",
}

BUILDING_ACTIVITY_ACTION_KEYS = {
    "building_constructed",
    "building_destroyed",
    "extension_started",
    "expansion_finished",
}

EVENT_EMOJI = {
    "membermanager": "🛠️",
    "missionchief": "🎮",
    "note_created": "📝",
    "note_edited": "✏️",
    "note_deleted": "🗑️",
    "note_pinned": "📌",
    "note_unpinned": "📍",
    "infraction_added": "⚠️",
    "infraction_revoked": "✅",
    "sanction_added": "🚨",
    "sanction_edited": "✏️",
    "sanction_removed": "✅",
    "link_created": "🔗",
    "link_approved": "✅",
    "link_denied": "❌",
    "role_changed": "👔",
    "contribution_drop": "📉",
    "role_restored": "🔄",
}


@dataclass(frozen=True)
class AuditTimelineEvent:
    """A normalized audit entry shown in MemberManager."""

    source: str
    event_type: str
    timestamp: Optional[int]
    title: str
    actor_name: Optional[str] = None
    actor_id: Optional[int] = None
    details: str = ""
    reference: Optional[str] = None

    @property
    def sort_key(self) -> int:
        """Sort unknown timestamps last without hiding the entry."""
        return self.timestamp or 0

    def matches(self, query: str) -> bool:
        """Return whether this event matches a case-insensitive search query."""
        normalized_query = query.lower().strip()
        if not normalized_query:
            return True

        searchable = [
            self.source,
            self.event_type,
            self.title,
            self.actor_name or "",
            self.details,
            self.reference or "",
        ]
        return any(normalized_query in value.lower() for value in searchable)


def parse_timestamp(value: Any) -> Optional[int]:
    """Parse common stored timestamp formats to a Unix timestamp."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.isdigit():
            return int(text)
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def normalize_member_event(event: dict[str, Any]) -> Optional[AuditTimelineEvent]:
    """Convert a MemberManager event row to an audit timeline entry."""
    event_type = event.get("event_type") or "unknown"
    if event_type not in ADMIN_EVENT_TYPES:
        return None

    event_data = event.get("event_data")
    if not isinstance(event_data, dict):
        event_data = {}

    details = []
    for key in ("reason", "note", "status", "old_value", "new_value"):
        value = event_data.get(key)
        if value:
            details.append(str(value))

    reference = None
    if event_data.get("ref_code"):
        reference = str(event_data["ref_code"])
    elif event_data.get("sanction_id"):
        reference = f"Sanction #{event_data['sanction_id']}"

    return AuditTimelineEvent(
        source="MemberManager",
        event_type=event_type,
        timestamp=parse_timestamp(event.get("timestamp")),
        title=event_type.replace("_", " ").title(),
        actor_name=event.get("triggered_by") or "system",
        actor_id=event.get("actor_id"),
        details=" | ".join(details),
        reference=reference,
    )


def normalize_log_row(row: dict[str, Any]) -> AuditTimelineEvent:
    """Convert a LogsScraper row to an audit timeline entry."""
    action_key = row.get("action_key") or "missionchief_log"
    title = row.get("action_text") or action_key.replace("_", " ").title()
    timestamp = parse_timestamp(row.get("event_timestamp")) or parse_timestamp(row.get("ts"))

    parts = []
    executed_name = row.get("executed_name")
    affected_name = row.get("affected_name")
    description = row.get("description")
    if executed_name:
        parts.append(f"Executed by {executed_name}")
    if affected_name and affected_name != executed_name:
        parts.append(f"Affected {affected_name}")
    if description:
        parts.append(str(description))

    return AuditTimelineEvent(
        source="MissionChief",
        event_type=action_key,
        timestamp=timestamp,
        title=title,
        actor_name=executed_name,
        details=" | ".join(parts),
        reference=str(row["id"]) if row.get("id") is not None else None,
    )


def should_include_log_row(row: dict[str, Any]) -> bool:
    """Return whether a LogsScraper row belongs in a member audit timeline."""
    action_key = row.get("action_key") or ""
    return action_key in PERSON_AUDIT_ACTION_KEYS and action_key not in EXCLUDED_PERSON_AUDIT_ACTION_KEYS


def build_identity_filters(
    *,
    mc_user_id: Optional[str],
    mc_username: Optional[str],
) -> tuple[str, list[str]]:
    """Build a LogsScraper WHERE clause for a MissionChief member identity."""
    where_parts = []
    params = []

    if mc_user_id:
        where_parts.append("(executed_mc_id = ? OR affected_mc_id = ?)")
        params.extend([str(mc_user_id), str(mc_user_id)])

    clean_username = mc_username
    if clean_username and "Former member" in clean_username:
        clean_username = None

    if clean_username:
        where_parts.append("(executed_name = ? OR affected_name = ?)")
        params.extend([clean_username, clean_username])

    if not where_parts:
        return "", []

    return " OR ".join(where_parts), params


async def fetch_missionchief_events(
    db_path: Path,
    *,
    mc_user_id: Optional[str],
    mc_username: Optional[str],
    limit: int = 250,
) -> list[AuditTimelineEvent]:
    """Read stored LogsScraper entries for a member and normalize them."""
    where_clause, params = build_identity_filters(
        mc_user_id=mc_user_id,
        mc_username=mc_username,
    )
    if not where_clause or not db_path.exists():
        return []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            f"""
            SELECT id, ts, event_timestamp, action_key, action_text,
                   executed_name, executed_mc_id, affected_name, affected_mc_id,
                   description, occurrence_index
            FROM logs
            WHERE {where_clause}
            ORDER BY COALESCE(event_timestamp, ts) DESC, id DESC
            LIMIT ?
            """,
            [*params, limit],
        )
        rows = await cursor.fetchall()

    events = []
    for row in rows:
        log_row = dict(row)
        if should_include_log_row(log_row):
            events.append(normalize_log_row(log_row))

    return events


def merge_timeline_events(
    member_events: Iterable[dict[str, Any]],
    missionchief_events: Iterable[AuditTimelineEvent],
    *,
    query: Optional[str] = None,
) -> list[AuditTimelineEvent]:
    """Normalize, merge, filter, and sort audit timeline events."""
    timeline = [
        normalized
        for event in member_events
        if (normalized := normalize_member_event(event)) is not None
    ]
    timeline.extend(missionchief_events)

    if query:
        timeline = [event for event in timeline if event.matches(query)]

    return sorted(timeline, key=lambda event: event.sort_key, reverse=True)
