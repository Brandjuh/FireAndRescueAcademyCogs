# FireStationCommander Design Decisions

## English-Only Player Experience

- Decision: All player-facing commands, embeds, buttons, menus, reports, errors, item names, incident titles, and admin documentation use English.
- Reason: The project owner explicitly set English as the game language.
- Alternatives considered: Keep Dutch aliases or mixed-language data during early MVP.
- Future risk: Existing SQLite rows may contain earlier Dutch keys.
- Reversal cost: Low for display text, moderate for persisted keys.

## Legacy Key Normalization

- Decision: Earlier Dutch MVP incident and training keys are normalized to canonical English keys when scoring or rendering.
- Reason: This keeps early test/player data usable after the English conversion.
- Alternatives considered: Hard migration of existing SQLite rows.
- Future risk: Alias maps must remain until a database migration is added.
- Reversal cost: Low if a future migration rewrites old rows.

## SQLite for Game State

- Decision: Persistent game state remains in SQLite through `aiosqlite`.
- Reason: SQLite matches the Red cog deployment model and keeps per-bot state local and durable.
- Alternatives considered: Red Config for all state or external database.
- Future risk: Large multiplayer systems may need more careful indexing and migrations.
- Reversal cost: High once player data grows.

## Red Config Only for Guild Settings

- Decision: Red Config is reserved for guild settings and is not used for full game state.
- Reason: Game state has relational data and incident/report history that fits SQLite better.
- Alternatives considered: Red Config user storage.
- Future risk: Some settings may be split between SQLite and Red Config if not documented.
- Reversal cost: Moderate.

## Data-Driven Content

- Decision: Vehicles, equipment, trainings, and incidents are defined in JSON data files.
- Reason: Content should expand without rewriting command handlers.
- Alternatives considered: Hardcoded Python lists.
- Future risk: JSON schema validation is still informal.
- Reversal cost: Low.

## Phase-Gated Development

- Decision: Hardening comes before major new systems such as shops, timed training, tactical choices, or multiplayer.
- Reason: The core loop should be reliable before deeper mechanics depend on it.
- Alternatives considered: Add larger content immediately.
- Future risk: Users may want more content before reliability work is complete.
- Reversal cost: Low.
