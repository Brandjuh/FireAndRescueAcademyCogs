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

### Changed

- Bumped the cog version to `1.1.2`.
- Vehicle shop options now come from `data/config/vehicles.yaml` when available.
- Mission definitions now come from `data/config/missions.yaml` when available.
- Mission embeds now show configured mission images when available.
- Vehicle purchase embeds now show configured vehicle images when available.
- Mission images now use the same flat illustrated fire-station style as vehicle assets.
- Mission rewards now use each mission's `base_credits` value when available.
- Career conversion cost and career turnout time now use matching `balance.yaml` values when available.
- New missions now store a mission state schema version.
- Mission state now tracks `created_at`, `updated_at`, `next_action`, and `next_action_at`.
- `[p]fsc` now opens the station dashboard instead of showing command help.
- `[p]fsc mission` now opens mission controls when an incident is already active.
- Mission embeds now use narrative text from mission config when available.
- On-scene updates now pause briefly before sending the incident result.
- Crew alert, turnout result, re-alert, vehicle selection, and en-route updates now use richer embeds instead of short plain status messages.
- Early manual gameplay timers are shorter until automatic dispatch is implemented.
- Short positive wait times now display as at least `in 1 minute` instead of `now`.

### Fixed

- Prevented Red help from appearing after the `[p]fsc` dashboard.
- Dashboard buttons now update the existing dashboard message instead of opening private responses.
- Vehicle shop now has a back button to return to the station dashboard.

### Planned

- Add automatic dispatch later, after the manual mission flow is more complete.

### Known Issues

- Mission timers still rely on in-memory `asyncio.sleep` calls.
- Training, equipment, expansions, maintenance, XP, and reputation config files are present but not fully wired into gameplay yet.
- Active mission data still uses a single dict without schema versioning.

## [1.1.1]

### Added

- Added the initial playable FireStationCommand loop with station creation, staff recruitment, station upgrades, career conversion, vehicle purchases, incident turnout, dispatch, travel, and incident resolution.

