# MemberManager

Comprehensive member management system for Fire & Rescue Academy alliance on MissionChief USA.

## Features

### 🔍 Member Lookup
- **Fuzzy search** across Discord and MissionChief databases
- Search by: Discord mention, MC ID, MC username, Discord username
- Single unified view of all member data

### 📝 Notes System
- Add, edit, and delete notes about members
- Link notes to infractions
- Optional expiry dates
- Pinned notes for important information
- Tamper-proof with content hashing
- Full audit trail

### ⚠️ Infractions Tracking
- Automatic tracking from Discord modlog events
- Manual MC infractions
- Severity scoring
- Temporary vs permanent punishments
- Revocation support

### 📊 Contribution Monitoring
- Automatic alerts for low contribution rates
- Trend analysis over configurable weeks
- Admin notifications
- Optional member DMs
- Runs twice per day (configurable)

### 🔗 Integration
- **MemberSync**: Discord ↔ MC account linking
- **AllianceScraper**: MC member data, roles, contribution rates
- **Red Modlog**: Automatic infraction creation from Discord mod actions
- **SanctionManager**: Cross-reference with existing sanctions (optional)

### 🎯 Automation
- ✅ Contribution rate monitoring
- ✅ Role drift detection (missing verified roles)
- ✅ Coordinated departure detection (leaving Discord + MC within 72h)
- 🔄 Dormancy tracking (planned)
- 🔄 Auto-escalation system (planned)

---

## Installation

### 1. Prerequisites
Ensure you have these cogs installed:
- **MemberSync** (required for linking)
- **AllianceScraper** (required for MC data)

### 2. Install MemberManager

```bash
[p]repo add fara-cogs https://github.com/YourRepo/FireAndRescueAcademyCogs
[p]cog install fara-cogs MemberManager
[p]load MemberManager
```

### 3. Initial Configuration

```bash
# Set admin roles (full access)
[p]memberset adminroles @Admin @Owner

# Set moderator roles (read-only + add notes)
[p]memberset modroles @Moderator @Officer

# Set alert channel for automation notifications
[p]memberset alertchannel #admin-alerts

# Configure contribution monitoring
[p]memberset threshold 5.0
[p]memberset trendweeks 3
[p]memberset autocontribution on
```

---

## Commands

### Member Lookup

#### `[p]member whois <target>`
Look up complete member information.

**Examples:**
```
[p]member whois @JohnDoe
[p]member whois 123456
[p]member whois JohnDoe
```

**What it shows:**
- 🎮 Discord: Username, ID, roles, join date, verification status
- 🚒 MissionChief: Username, ID, role, contribution rate, profile link
- 📊 Stats: Infractions, notes, severity score, watchlist status
- ⚡ Tabs: Overview | Notes | Infractions | Events

---

### Notes Management

#### `[p]member note add <target> <text> [infraction_ref] [expires_days]`
Add a note to a member.

**Examples:**
```
[p]member note add @JohnDoe "Warned about spam in chat"
[p]member note add 123456 "Low contribution - reached out via DM" INF-MC-2025-000123 30
```

#### `[p]member note view <ref_code>`
View a specific note.

```
[p]member note view N2025-000123
```

#### `[p]member note list <target> [limit]`
List all notes for a member.

```
[p]member note list @JohnDoe
[p]member note list 123456 20
```

#### `[p]member note edit <ref_code> <new_text>`
Edit an existing note (admin only).

```
[p]member note edit N2025-000123 "Updated: Issue resolved"
```

#### `[p]member note delete <ref_code> [reason]`
Delete a note (admin only).

```
[p]member note delete N2025-000123 "Outdated information"
```

#### `[p]member note search <query>`
Search notes by text content.

```
[p]member note search "contribution"
```

---

### Infractions Management

#### `[p]member infraction add <target> <platform> <type> <reason> [duration]`
Manually add an infraction.

**Platforms:** `discord` or `missionchief`  
**Types:** `warning`, `mute`, `kick`, `ban`, `timeout`

**Examples:**
```
[p]member infraction add @JohnDoe discord mute "Spam in chat" 1h
[p]member infraction add 123456 missionchief kick "Inactive for 30 days"
```

#### `[p]member infraction view <ref_code>`
View infraction details.

```
[p]member infraction view INF-DC-2025-000123
```

#### `[p]member infraction list <target> [platform]`
List infractions for a member.

```
[p]member infraction list @JohnDoe
[p]member infraction list 123456 discord
```

#### `[p]member infraction revoke <ref_code> <reason>`
Revoke an infraction early.

```
[p]member infraction revoke INF-DC-2025-000123 "Appeal approved"
```

---

### Watchlist

#### `[p]member watchlist add <target> <reason> <type>`
Add member to watchlist.

**Types:** `contribution`, `behavior`, `probation`, `general`

```
[p]member watchlist add @JohnDoe "Low contribution for 3 weeks" contribution
```

#### `[p]member watchlist remove <target> [notes]`
Remove from watchlist.

```
[p]member watchlist remove @JohnDoe "Contribution improved"
```

#### `[p]member watchlist list [type]`
View all watchlist entries.

```
[p]member watchlist list
[p]member watchlist list contribution
```

---

### Statistics & Export

#### `[p]member stats <target>`
Quick stats summary for a member.

```
[p]member stats @JohnDoe
```

#### `[p]member export <data_type> [target] [format]`
Export member data.

**Data types:** `notes`, `infractions`, `events`, `full`  
**Formats:** `json`, `csv`

```
[p]member export full @JohnDoe json
[p]member export notes 123456
```

---

### Audit Commands

#### `[p]member audit [role] [check_type]`
Bulk audit members for issues.

**Check types:** `role_drift`, `contribution`, `infractions`

```
[p]member audit @Member role_drift
[p]member audit contribution
```

---

## Configuration Commands

All configuration commands require admin permissions.

### `[p]memberset view`
View current configuration.

### `[p]memberset alertchannel <channel>`
Set admin alert channel.

### `[p]memberset modlogchannel <channel>`
Set modlog channel.

### `[p]memberset adminroles <roles...>`
Set admin roles (full access).

### `[p]memberset modroles <roles...>`
Set moderator roles (read-only + limited actions).

### `[p]memberset threshold <percentage>`
Set contribution rate threshold for alerts (default: 5.0%).

### `[p]memberset trendweeks <weeks>`
Set trend analysis period (default: 3 weeks).

### `[p]memberset autocontribution <on|off>`
Enable/disable automatic contribution monitoring.

### `[p]memberset autoroledrift <on|off>`
Enable/disable role drift detection.

### `[p]memberset noteexpiry <days>`
Set default note expiry (0 = never expire).

### `[p]memberset reset`
Reset all settings to defaults (requires confirmation).

---

## Automation

### Contribution Monitoring

**How it works:**
1. Runs every 12 hours (twice per day)
2. Checks all MC members' contribution rates
3. Analyzes trend over configured weeks
4. Sends alerts if:
   - Rate drops below threshold (default 5%)
   - Rate drops >2% from previous period
5. Cooldown: 1 week between alerts per member

**Alert includes:**
- Current rate
- Trend (rising/falling/stable)
- Analysis of change
- Linked Discord account
- Historical context

### Role Drift Detection

**Detects:**
- Members with linked MC accounts but missing Discord verified role
- Can auto-restore or alert admins

### Coordinated Departure Detection

**Detects:**
- Members who leave both Discord and MC within 72 hours
- Logs as potential rage quit or coordinated action

---

## Database Schema

### Notes Table
- Reference codes (e.g., `N2025-000123`)
- Content with tamper-proof hashing
- Author tracking
- Optional infraction linking
- Expiry dates
- Pin support

### Infractions Table
- Platform-specific reference codes
- `INF-DC-2025-000123` (Discord)
- `INF-MC-2025-000123` (MissionChief)
- Severity scoring
- Temporary vs permanent tracking
- Revocation support

### Events Table
- Complete audit trail
- Tracks: joins, leaves, role changes, link status, contribution changes
- Triggered by: automation, admins, system

### Watchlist Table
- Configurable watch types
- Alert thresholds
- Resolution tracking

---

## Permissions

### Admin
- All commands
- Add/edit/delete notes and infractions
- Configure settings
- Manage watchlist
- Export data

### Moderator
- View member information (`whois`)
- View notes and infractions
- Add notes (cannot delete)
- View watchlist
- View stats

### Public
- None (all commands require permissions)

---

## Integration Details

### With MemberSync
- Reads link status (approved/pending/none)
- Gets Discord ↔ MC ID mappings
- Checks verification status
- **Does not write** to MemberSync database

### With AllianceScraper
- Reads MC member data (name, role, contribution)
- Reads contribution history for trend analysis
- Queries members_current and members_history tables
- **Does not write** to AllianceScraper database

### With Red Modlog
- Listens to `on_modlog_case_create` event
- Auto-creates infractions from:
  - Bans, kicks, mutes, timeouts, warnings
- Links Discord user to MC account if linked
- Stores moderator, reason, duration

---

## Troubleshooting

### "No integrations found" warning
**Problem:** MemberSync or AllianceScraper not loaded.

**Solution:**
```bash
[p]load MemberSync
[p]load AllianceScraper
[p]reload MemberManager
```

### Contribution monitoring not working
**Check:**
1. Is automation enabled? `[p]memberset view`
2. Is AllianceScraper running? `[p]scraperinfo`
3. Is alert channel set? `[p]memberset alertchannel #channel`
4. Check logs: `[p]debug`

### Fuzzy search not finding members
**Common causes:**
- Member not in AllianceScraper database yet (wait for next scrape)
- Typo too severe (try exact MC ID or Discord mention)
- Member left alliance

---

## Changelog

### v1.0.0 (Initial Release)
- ✅ Complete member lookup system
- ✅ Notes and infractions tracking
- ✅ Contribution monitoring automation
- ✅ Tab-based Discord UI
- ✅ Integration with MemberSync, AllianceScraper, Modlog
- ✅ Full configuration system
- ✅ Export functionality
- ✅ Watchlist system

### Planned Features
- 🔄 Web dashboard API
- 🔄 Appeal system for infractions
- 🔄 Advanced analytics
- 🔄 Dormancy auto-pings
- 🔄 Auto-escalation thresholds

---

## Support

For issues, feature requests, or questions:
- Open an issue on GitHub
- Contact Fire & Rescue Academy leadership
- Check Red-DiscordBot support server

---

## Credits

**Developer:** FireAndRescueAcademy  
**For:** Fire & Rescue Academy Alliance - MissionChief USA  
**Framework:** Red-DiscordBot v3.5+  

---

## License

This cog is provided as-is for Fire & Rescue Academy alliance use.
