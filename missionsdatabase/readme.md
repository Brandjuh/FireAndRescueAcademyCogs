# MissionChief Missions Database

A Red-DiscordBot cog that automatically fetches MissionChief missions and posts them to a Discord forum channel.

## Features

- 🔄 Automatic daily synchronization with MissionChief
- 📝 Creates formatted forum posts for each mission
- 🔍 Detects mission changes and updates posts accordingly
- ⚙️ Configurable forum channel and admin alerts
- 📊 Statistics and manual sync commands
- 🗄️ SQLite database for tracking posted missions

## Installation

### Prerequisites

- Red-DiscordBot V3
- Python 3.8+
- A Discord server with a forum channel

### Installing the Cog

1. Add this repository to your Red instance:
```
[p]repo add faracogs https://github.com/Brandjuh/FireAndRescueAcademyCogs
```

2. Install the cog:
```
[p]cog install faracogs missionsdatabase
```

3. Load the cog:
```
[p]load missionsdatabase
```

## Setup

### 1. Create a Forum Channel

Create a forum channel in your Discord server where missions will be posted.

### 2. Create a "Missions" Tag

In the forum channel settings, create a tag named "Missions" (case-insensitive). This tag will be applied to all mission posts.

### 3. Configure the Cog

Run the setup command:
```
[p]missions setup #forum-channel [#admin-channel]
```

- `#forum-channel`: The forum channel where missions will be posted (required)
- `#admin-channel`: Optional channel where admin alerts will be sent

Example:
```
[p]missions setup #missions-database #admin-alerts
```

## Commands

All commands require Administrator permissions.

### `[p]missions setup <forum_channel> [admin_channel]`
Configure the forum channel and optional admin alerts channel.

### `[p]missions sync`
Manually trigger a mission sync. This will:
- Fetch all missions from MissionChief
- Post new missions
- Update changed missions
- Report statistics

### `[p]missions check`
Display statistics about the missions database:
- Total missions tracked
- Number of updated missions
- Auto-sync status
- Last sync time

### `[p]missions toggle`
Enable or disable automatic daily syncing.

### `[p]missions update <mission_id>`
Force update a specific mission. Useful if a particular mission needs to be refreshed.

Example:
```
[p]missions update 88/a
```

### `[p]missions view <mission_id>`
Preview how a mission will be formatted without posting it.

Example:
```
[p]missions view 88
```

### `[p]missions fullreset`
⚠️ **WARNING: Destructive Operation** ⚠️

Completely reset the missions database. This will:
1. Clear all tracked missions from the database
2. Optionally delete all forum posts

This command requires confirmation and should only be used when starting fresh.

## How It Works

### Automatic Syncing

The cog runs an automatic sync every day at 3:00 AM (server time). During sync:

1. Fetches all missions from `https://www.missionchief.com/einsaetze.json`
2. For each mission:
   - Calculates a hash of the mission data
   - Compares with stored hash to detect changes
   - Creates new forum posts for new missions
   - Updates existing posts if mission data changed

### Mission Post Format

Each mission is posted with the following information:

- **Mission Name** (as thread title)
- **Average Credits**
- **Mission ID** (including overlay variants like 88/a, 88/b)
- **Categories** (e.g., Fire, Urban, Water Damage and Flood)
- **Locations** (if specified)
- **Requirements** (vehicles and equipment needed)
- **Patients** (if applicable)
  - Number of possible patients
  - Required hospital specializations
  - US medical codes
- **Chances** (probabilities for events like patient transport, hazmat, etc.)
- **Unlock Requirements**
  - Main building type
  - Number of required stations
  - Required extensions

### Data Source

Mission data is sourced from the official MissionChief API:
- URL: `https://www.missionchief.com/einsaetze.json`
- Format: JSON
- Contains all mission definitions for MissionChief USA

## Database

The cog uses SQLite to store:

### Mission Posts Table
- `mission_id`: Mission ID (e.g., "88/a")
- `thread_id`: Discord thread ID
- `mission_data_hash`: Hash of mission data for change detection
- `posted_at`: When the post was created
- `last_updated`: When the post was last updated
- `last_check`: When the mission was last checked

### Configuration Table
- `guild_id`: Discord guild ID
- `forum_channel_id`: Forum channel for posts
- `admin_alert_channel_id`: Channel for admin alerts
- `auto_sync_enabled`: Whether auto-sync is enabled
- `last_full_sync`: Timestamp of last sync
- `missions_tag_name`: Name of the forum tag to use

## Troubleshooting

### Posts Not Updating

If posts aren't updating when missions change:

1. Check that auto-sync is enabled: `[p]missions check`
2. Try a manual sync: `[p]missions sync`
3. Check the bot has permissions to edit posts in the forum channel

### Missing "Missions" Tag

If forum posts don't have the "Missions" tag:

1. Create a tag named "Missions" in your forum channel settings
2. The tag name is case-insensitive
3. Existing posts won't automatically get the tag - only new posts

### Permission Issues

Ensure the bot has these permissions in the forum channel:
- View Channel
- Send Messages in Threads
- Create Public Threads
- Manage Threads (for editing posts)

## Technical Details

### Architecture

The cog is organized into several modules:

- `missions_database.py`: Main cog with commands and background tasks
- `database.py`: Database operations (SQLite)
- `mission_fetcher.py`: Fetches and parses mission JSON
- `mission_formatter.py`: Formats missions for forum posts
- `mappings.py`: Data mappings (buildings, vehicles, equipment, trainings)

### Data Mappings

The cog includes comprehensive mappings for:
- 28 building types
- 114 vehicle types
- 19 equipment types
- 40+ training types
- 9 hospital specializations

These mappings convert game IDs and codes into human-readable names.

### Background Task

The auto-sync background task:
- Runs in a loop
- Calculates time until next 3 AM
- Sleeps until then
- Processes all configured guilds
- Handles errors gracefully

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## Support

For support, join the Fire & Rescue Academy Alliance Discord or create an issue on GitHub.

## Credits

- **Author**: Brandjuh
- **Data Source**: MissionChief / Leitstellenspiel
- **Mappings**: Based on LSSM (Leitstellenspiel Manager) data

## License

This project is licensed under the MIT License.
