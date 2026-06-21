# MissionsDatabase

Publishes MissionChief possible missions from `https://www.missionchief.com/einsaetze.json`
to Discord.

## Safety model

This cog does not run a full publish by default. The normal `sync` command is a limited
test sync, capped at 25 missions. A full publish requires an explicit `CONFIRM` argument.

The default target channel is:

`1518038840152031262`

## Setup

```text
[p]load missionsdatabase
[p]missions setup
```

To use another text channel:

```text
[p]missions setup #channel-name
```

## Commands

```text
[p]missions sync [limit] [search]
```

Safely publishes or updates a small number of missions. Default limit is 5, maximum is 25.
Optional `search` filters by mission ID, name, category, requirement, or URL.

```text
[p]missions view <search>
```

Previews one mission without posting it.

```text
[p]missions update <search>
```

Publishes or updates one mission.

```text
[p]missions syncall CONFIRM
```

Publishes or updates every possible mission.

```text
[p]missions check
```

Shows configuration and tracked post counts.

```text
[p]missions auto on
[p]missions auto off
```

Controls the daily full auto-sync. It is disabled by default.

## Duplicate prevention

Every mission is tracked by a stable MissionChief mission key:

- normal missions use their `id`
- additive overlays use `base_mission_id/additive_overlays`, for example `2/a`
- MissionChief hyphen variants such as `438-0` stay separate posts, but link to the
  official detail URL with `overlay_index`, for example `/einsaetze/438?overlay_index=0`

The cog stores the Discord message or forum thread ID in SQLite. On later syncs it edits the
existing post if the mission changed, skips it if unchanged, and only creates a new post when
no tracked or recoverable post exists.
