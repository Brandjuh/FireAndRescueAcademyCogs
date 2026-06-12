# Changelog

All notable changes to FireStationCommand will be documented in this file.

## [Unreleased]

### Added

- Added FireStationCommand-specific cog metadata.
- Added YAML-backed loading for missions and vehicle shop data.
- Added fallback data so the cog can still load if YAML data is unavailable.
- Added balance-backed reward multiplier support.
- Added tests for FireStationCommand data loading helpers.

### Changed

- Bumped the cog version to `1.1.2`.
- Vehicle shop options now come from `data/config/vehicles.yaml` when available.
- Mission definitions now come from `data/config/missions.yaml` when available.
- Mission rewards now use each mission's `base_credits` value when available.
- Career conversion cost and career turnout time now use matching `balance.yaml` values when available.

### Known Issues

- Mission timers still rely on in-memory `asyncio.sleep` calls.
- Training, equipment, expansions, maintenance, XP, and reputation config files are present but not fully wired into gameplay yet.
- Active mission data still uses a single dict without schema versioning.

## [1.1.1]

### Added

- Added the initial playable FireStationCommand loop with station creation, staff recruitment, station upgrades, career conversion, vehicle purchases, incident turnout, dispatch, travel, and incident resolution.

