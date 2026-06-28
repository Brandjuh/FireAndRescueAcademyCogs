# FireStationCommander Roadmap

## Current Phase

Phase 3 - Hardening and quality pass.

The playable foundation and core incident loop exist. The next milestone should focus on reliability before adding larger gameplay systems.

## Completed Phases

### Phase 0 - Repository Audit and Project Control

- Current cog structure inspected.
- Required project documentation created.
- Existing commands, services, database tables, views, data files, tests, and assets identified.
- Missing documentation files documented and added.
- Current risks and owner questions recorded.

### Phase 1 - Stable Playable Foundation

- `/fsc start` creates a player, Station 1, Engine 1, starter crew, starter equipment, and starter trainings.
- `/fsc status` shows player and station data.
- `/fsc vehicles` lists owned vehicles.
- `/fsc personnel` lists personnel and trainings.
- Starter data is persistent SQLite data and is created only once for a new player.

### Phase 2 - Core Incident Loop

- `/fsc incident` creates or reopens an active incident.
- Players can dispatch vehicles through a select menu.
- Incident results are scored and persisted.
- `/fsc report` shows the latest incident report.
- Rewards, XP, reputation, vehicle wear, fuel use, and personnel stress persist.

## Next Phase

### Phase 3 - Hardening and Quality Pass

- Enforce active incident expiry.
- Add duplicate interaction protection for dispatch completion.
- Add clearer validation for missing station, missing vehicles, unavailable vehicles, and stale incidents.
- Review whether more business logic should move out of `firestationcommander.py`.
- Add cooldowns if incident generation can be spammed.
- Add restart resilience checks for active incidents and stale views.
- Improve logging around database and interaction failures.

## Blocked Items

- Salary/upkeep cadence needs an owner decision before recurring costs are added.
- Training duration and downtime rules need an owner decision before Phase 5.
- Multiplayer assistance rules need anti-farming decisions before Phase 8.

## Future Ideas

- Phase 4: maintenance risk, vehicle unavailability, and fuel/condition failure thresholds.
- Phase 5: personnel training with costs and downtime.
- Phase 6: shop, station upgrades, salary/upkeep, and capacity strategy.
- Phase 7: tactical incident choices.
- Phase 8: regional assistance and multiplayer.
- Phase 9: larger content packs, achievements, daily tasks, weekly inspections, admin tools, and server configuration.

## Do Not Build Yet

- Do not add bulk vehicles, incidents, or equipment until Phase 3 hardening is complete.
- Do not add multiplayer assistance until solo incident rewards and anti-farming rules are stable.
- Do not add recurring salary/upkeep until economy assumptions are approved.
- Do not add tactical incident branches until the current incident lifecycle has expiry and duplicate-action protection.
