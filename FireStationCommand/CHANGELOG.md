# Changelog

All notable changes to FireStationCommand will be documented in this file.

## [Unreleased]

### Added

- Added FireStationCommand-specific cog metadata.
- Added YAML-backed loading for missions and vehicle shop data.
- Added fallback data so the cog can still load if YAML data is unavailable.
- Added balance-backed reward multiplier support.
- Added tests for FireStationCommand data loading helpers.
- Added mission state helpers with explicit stage constants and schema versioning.
- Added mission timestamp fields for future scheduler/resume support.
- Added an interactive `[p]fsc` dashboard with station overview and action buttons.
- Added mission control buttons for active incidents from the dashboard and `[p]fsc mission`.
- Added richer story narrative fields for mission dispatches, scene updates, and outcomes.
- Added narrative alert, re-alert, turnout, and en-route messages with dispatch and responder perspectives.
- Added generated mission images for the initial incident set.
- Added generated vehicle images for the initial vehicle catalog.
- Added dashboard recruitment buttons for hiring station staff without typing the recruit command.
- Added dashboard buttons for station upgrades and career conversion.
- Added generated station images for station levels 1 through 10.
- Added command XP and command levels as the first progression layer.
- Added `progression.yaml` with level XP thresholds and planned unlock data.
- Added mission readiness scoring based on capabilities, staffing, vehicle requirements, and command level.
- Added capability values to the initial mission, vehicle, and equipment config data.

### Changed

- Bumped the cog version to `1.1.3`.
- Vehicle shop options now come from `data/config/vehicles.yaml` when available.
- Mission definitions now come from `data/config/missions.yaml` when available.
- Mission embeds now show configured mission images when available.
- Vehicle purchase embeds now show configured vehicle images when available.
- Mission images now use the same flat illustrated fire-station style as vehicle assets.
- Mission embeds now show configured required equipment names when available.
- Mission embeds now show configured required vehicle names when available.
- Mission embeds now warn when the station is missing required vehicle types for the incident.
- Mission rewards now use each mission's `base_credits` value when available.
- Career conversion cost and career turnout time now use matching `balance.yaml` values when available.
- New missions now store a mission state schema version.
- New missions now retain configured required vehicle and equipment IDs in mission state.
- Mission state now tracks `created_at`, `updated_at`, `next_action`, and `next_action_at`.
- `[p]fsc` now opens the station dashboard instead of showing command help.
- `[p]fsc mission` now opens mission controls when an incident is already active.
- Mission embeds now use narrative text from mission config when available.
- On-scene updates now pause briefly before sending the incident result.
- Crew alert, turnout result, re-alert, vehicle selection, and en-route updates now use richer embeds instead of short plain status messages.
- Early manual gameplay timers are shorter until automatic dispatch is implemented.
- Short positive wait times now display as at least `in 1 minute` instead of `now`.
- Station overview images now use the station level instead of vehicle capacity.
- Default maximum station level is now 10.
- Mission selection now prefers incidents around the player's command level and readiness, with occasional challenge calls.
- Mission results now award XP, update command level automatically, and show XP progress in the result embed.
- Dashboard, status, and station embeds now show command XP progress.
- Station upgrades now require the matching command level in addition to credits.
- Vehicle shop entries now show locked vehicles, and purchases are blocked until the required command level is reached.
- Rescue Call is now level 2 content instead of a regular level 1 incident.

### Fixed

- Prevented Red help from appearing after the `[p]fsc` dashboard.
- Dashboard buttons now update the existing dashboard message instead of opening private responses.
- Vehicle shop now has a back button to return to the station dashboard.
- Vehicle purchase confirmation now edits the shop message instead of opening a private interaction flow.

### Planned

- Add automatic dispatch later, after the manual mission flow is more complete.

### Known Issues

- Mission timers still rely on in-memory `asyncio.sleep` calls.
- Training, equipment purchasing, expansions, maintenance, and reputation config files are present but not fully wired into gameplay yet.

## [1.1.1]

### Added

- Added the initial playable FireStationCommand loop with station creation, staff recruitment, station upgrades, career conversion, vehicle purchases, incident turnout, dispatch, travel, and incident resolution.

