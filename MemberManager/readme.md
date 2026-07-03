# MemberManager

MemberManager is the private staff console for member information. It combines Discord data, MissionChief member data, MemberSync links, notes, sanctions, audit history, event requests, building activity, and watchlist status into one admin-facing profile.

Current version: `2.2.4`

## Purpose

MemberManager is the visible front end for staff. It should not own every workflow itself. Other cogs keep their own responsibilities and expose public contracts:

- `MemberSync` owns Discord to MissionChief verification.
- `MembersScraper` owns current MissionChief alliance member and contribution data.
- `LogsScraper` owns stored MissionChief alliance logs.
- `SanctionManager` owns sanctions and TAX warning records.
- `EventManager`, `BuildingManager`, and other managers may write member events for visibility.

MemberManager reads those contracts where available and keeps fallback database reads only for older cog versions.

## Staff Entry Points

- Right-click a Discord user and use `Apps -> Member Management`.
- Use the persistent MemberManager panel button in the configured panel channel.
- Use `[p]member whois <member>` for direct lookup.
- Use `[p]member search <name or id>` to search Discord and MissionChief members.
- Use `[p]member mcsearch <name or id>` for MissionChief-only lookup.

All interactive output is private to the staff member who opened it.

## Profile Tabs

- `Overview`: compact triage view with simple and advanced modes.
- `Notes`: create, view, edit, pin, and delete staff notes.
- `Sanctions`: view and start sanction workflows through SanctionManager when loaded.
- `Events`: member-related alliance event requests and operations.
- `Audit`: combined staff actions and relevant MissionChief member logs.
- `Buildings`: building and extension activity related to the member.

## Watchlist

The overview includes a watchlist action:

- `Add Watchlist`: add a member with a staff reason.
- `Resolve Watchlist`: resolve active watchlist entries for that member.

Watchlist changes are stored in MemberManager and written to the member audit timeline.

## Contribution / TAX Handling

MemberManager shows contribution data, but it no longer sends automatic TAX warning messages.

Automatic TAX warnings are owned by `MessageManager` and logged through `SanctionManager`. This avoids duplicate warning systems and keeps official sanction history in one place.

The legacy MemberManager contribution monitor remains disabled. Diagnostic contribution commands may still be used for investigation.

## Public Contract

Other cogs should use `get_stats()` instead of reading MemberManager tables directly.

```python
stats = await member_manager.get_stats(
    guild_id=guild.id,
    period_start=start_timestamp,
    period_end=end_timestamp,
)
```

The returned dictionary contains:

- note counts
- member event counts by type
- sanction-related event counts
- event request counts
- active and resolved watchlist counts

## Database Ownership

MemberManager owns these local SQLite tables:

- `notes`
- `infractions` for legacy local infractions
- `member_events`
- `watchlist`
- `role_history`
- local config/migration tables

Other cogs should not query these tables directly unless a public contract is not yet available.

## Setup

Recommended setup:

```text
[p]load MemberManager
[p]memberset panelchannel #channel
[p]memberpanel post
```

Optional:

```text
[p]memberset adminroles @Role
[p]memberset modroles @Role
```

The default panel channel is `1426226521231589507`.

## Validation

Focused tests live under:

- `tests/test_membermanager_entrypoints.py`
- `tests/test_membermanager_notes.py`
- `tests/test_membermanager_audit.py`
- `tests/test_membermanager_sanctions.py`
- `tests/test_membermanager_contribution.py`
- `tests/test_membermanager_stats_watchlist.py`
