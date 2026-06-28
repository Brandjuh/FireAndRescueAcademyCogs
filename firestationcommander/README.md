# FireStationCommander

FireStationCommander is a playable Red Discord Bot MVP where each Discord user manages a fictional fire station.

## Load

Install the cog through the repository in Red, then load it with:

```text
[p]load firestationcommander
```

The cog requires Python 3.11+ and `aiosqlite`. Game progress is stored in a local SQLite database under the cog data path. Red Config is reserved for guild settings only.

## Commands

- `[p]fsc start` creates your player, starter station, first TS, six personnel members, starter equipment, and starter trainings.
- `[p]fsc status` opens the station dashboard.
- `[p]fsc vehicles` shows owned vehicles.
- `[p]fsc staff` shows personnel and trainings.
- `[p]fsc incident` generates or reopens an active incident.
- `[p]fsc maintenance` shows worn vehicles and equipment with repair actions.
- `[p]fsc report` shows the latest incident report.

## Data

All MVP vehicles, equipment, trainings, and incidents are fictional project data stored in `data/`. Do not copy MissionChief text, artwork, screenshots, or protected datasets into this cog.
