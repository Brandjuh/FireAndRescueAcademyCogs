# FireStationCommander Agent Rules

These rules apply to the `firestationcommander` cog.

## Core Rules

- Keep all player-facing and server-admin-facing text in English.
- Use fictional game data only. Do not copy MissionChief text, data, screenshots, icons, or protected assets.
- Keep the game primarily about fire station management.
- Prefer small complete milestones over broad unfinished systems.
- Do not add UI controls, commands, or select options that do nothing.
- Do not skip roadmap phases unless a bug blocks the current playable foundation.

## Technical Rules

- Follow Red Discord Bot cog conventions.
- Use `redbot.core.commands` and hybrid commands where practical.
- Store game state in SQLite through `aiosqlite`.
- Use Red Config only for guild/server settings.
- Keep database initialization idempotent.
- Keep the cog multi-guild and multi-user safe.
- Avoid blocking calls in command and interaction paths.
- Keep business logic in services where practical.
- Use data files for vehicles, equipment, trainings, and incidents instead of hardcoding large content lists in command handlers.
- Never expose internal tracebacks to players.

## Phase Rules

- Read `ROADMAP.md`, `CHANGELOG.md`, `QUESTIONS.md`, and this file before starting a new FireStationCommander task.
- At the end of a phase milestone, update the roadmap, changelog, design decisions, balance notes, questions, and test plan if affected.
- Work on Phase 3 hardening before adding major Phase 4+ gameplay.
- Do not start multiplayer, tactical choices, or large content expansion until the core loop is stable and documented.
