# FireStationCommander Changelog

## 2026-06-28 - Phase 0 / Phase 1 Alignment

### Added

- Added project control documentation: roadmap, agent rules, design decisions, owner questions, balance notes, and test plan.
- Added compatibility aliases for earlier Dutch MVP incident and training keys.

### Changed

- Aligned starter setup with the Phase 1 specification:
  - starting cash is now 50,000
  - safety starts at 100
  - starter station name is `Station 1`
  - storage slots start at 10
  - starter vehicle callsign is `Engine 1`
  - starter fire engine reliability is 95
  - starter personnel roles are Crew Commander, Driver/Operator, Pump Operator, and three Firefighters
- Made `/fsc personnel` the primary personnel command. `/fsc staff` remains an English alias.
- Renamed Phase 2 incident templates to `Grass Fire`, `House Fire`, and `Vehicle Crash with Entrapment`.
- Renamed training keys and display names to Basic Firefighting, Breathing Apparatus, Driver Operator, Pump Operator, Technical Rescue, and Crew Command.

### Fixed

- Preserved scoring and rendering for existing early MVP records that still use Dutch training or incident keys.

### Removed

- Removed Dutch player-facing command names, catalog names, incident titles, vehicle names, equipment names, and training names from the active MVP data.

### Known Issues

- Active incident expiry is stored but not yet enforced.
- Duplicate interaction protection is limited to Discord owner checks and status validation.
- Vehicle failure thresholds for low condition and low fuel are documented for Phase 4 but not fully enforced yet.

## 2026-06-28 - Phase 0 / Phase 2 MVP Polish

### Added

- Added clearer incident embeds with time limits and complete requirement summaries.
- Added shared interaction error handling for dashboard, incident, dispatch, and maintenance views.
- Added a cog README with Red load instructions, command overview, storage notes, and data guidelines.
- Added timeout handling that disables stale UI controls and edits the original menu message.
- Added worn equipment visibility by name, condition, and repair cost in the maintenance dashboard.

### Changed

- Changed FireStationCommander player-facing commands, button labels, catalog names, and incident text to English.
- Added training and incident key normalization for earlier Dutch MVP data.

### Fixed

- Reduced stale Discord UI interaction failures by porting the proven timeout/error handling pattern from FireStationCommand.

### Removed

- None.

### Known Issues

- Phase 3 hardening still needs a broader edge-case pass.

## 2026-06-28 - 0.1.0 MVP

### Added

- Added the initial SQLite-backed FireStationCommander Red cog.
- Added starter station creation with one fire engine, six personnel members, starter equipment, and starter trainings.
- Added `/fsc start`, `/fsc status`, `/fsc vehicles`, `/fsc personnel`, `/fsc incident`, `/fsc maintenance`, and `/fsc report`.
- Added dashboard buttons, incident controls, vehicle dispatch selection, and maintenance controls.
- Added fictional vehicle, equipment, training, and incident catalogs.
- Added incident scoring based on vehicle match, staffing, trainings, equipment coverage, vehicle health, fuel, personnel wellness, and a small random modifier.
- Added persistent incident reports with cash, XP, reputation, safety, vehicle wear, personnel stress, and maintenance repair flow.
- Added automated MVP tests for startup assets, incident gating, scoring, dispatch resolution, and maintenance.

### Changed

- None.

### Fixed

- None.

### Removed

- None.

### Known Issues

- Economy and operational risk need Phase 3/4 tuning before adding more content.
