# FireStationCommander

FireStationCommander is a playable Red Discord Bot MVP where each Discord user manages a fictional fire station.

The current development priority is Phase 3 hardening. Read `ROADMAP.md`, `AGENTS.md`, `QUESTIONS.md`, and `TEST_PLAN.md` before adding new gameplay systems.

## Load

Install the cog through the repository in Red, then load it with:

```text
[p]load firestationcommander
```

The cog requires Python 3.11+ and `aiosqlite`. Game progress is stored in a local SQLite database under the cog data path. Red Config is reserved for guild settings only.

## Commands

- `[p]fsc start` creates your player, Station 1, Engine 1, six personnel members, starter equipment, and starter trainings.
- `[p]fsc status` opens the station dashboard.
- `[p]fsc vehicles` shows owned vehicles.
- `[p]fsc personnel` shows personnel and trainings.
- `[p]fsc incident` generates or reopens an active incident.
- `[p]fsc maintenance` shows worn vehicles and equipment with repair actions.
- `[p]fsc report` shows the latest incident report.

## Data

All MVP vehicles, equipment, trainings, and incidents are fictional project data stored in `data/`. Do not copy MissionChief text, artwork, screenshots, or protected datasets into this cog.

## Project Docs

- `ROADMAP.md` tracks current phase, completed phases, next work, blocked items, future ideas, and work that should not be built yet.
- `CHANGELOG.md` tracks date, phase, added, changed, fixed, removed, and known issues.
- `AGENTS.md` stores durable development rules.
- `QUESTIONS.md` stores owner decisions needed later.
- `DESIGN_DECISIONS.md` stores architecture and gameplay decisions.
- `BALANCE_NOTES.md` stores economy, XP, reward, maintenance, and risk assumptions.
- `TEST_PLAN.md` stores manual and automated test coverage expectations.
