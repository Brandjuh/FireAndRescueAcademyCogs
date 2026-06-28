# FireStationCommander Changelog

## Unreleased

- Added clearer incident embeds with time limits and complete requirement summaries.
- Added shared interaction error handling for dashboard, incident, dispatch, and maintenance views.
- Added a cog README with Red load instructions, command overview, storage notes, and data guidelines.
- Listed worn equipment by name, condition, and repair cost in the maintenance dashboard.

## 0.1.0 - MVP

- Added the initial SQLite-backed FireStationCommander Red cog.
- Added starter station creation with one TS vehicle, six personnel members, starter equipment, and starter trainings.
- Added `/fsc start`, `/fsc status`, `/fsc voertuigen`, `/fsc personeel`, `/fsc melding`, `/fsc onderhoud`, and `/fsc rapport`.
- Added dashboard buttons, incident controls, vehicle dispatch selection, and maintenance controls.
- Added fictional vehicle, equipment, training, and incident catalogs.
- Added incident scoring based on vehicle match, staffing, trainings, equipment coverage, vehicle health, fuel, personnel wellness, and a small random modifier.
- Added persistent incident reports with cash, XP, reputation, safety, vehicle wear, personnel stress, and maintenance repair flow.
- Added automated MVP tests for startup assets, incident gating, scoring, dispatch resolution, and maintenance.
