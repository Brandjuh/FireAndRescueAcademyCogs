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
- Added starter equipment inventory for new stations.
- Added an equipment shop command and dashboard button.
- Added equipment purchase confirmation flow with command-level locks.
- Added station training certifications with a training desk command and dashboard button.
- Added training purchase confirmation flow with command-level locks.
- Added station expansion inventory with an expansion desk command and dashboard button.
- Added expansion build confirmation flow with command-level locks.
- Added vehicle condition tracking with a maintenance bay command and dashboard button.
- Added fleet repair actions that restore damaged vehicles for balance-backed maintenance costs.
- Added mission reputation gains and losses from balance config.
- Added manual recovery for due mission timer actions through `[p]fsc mission` and mission control refresh.
- Added the full imported MissionChief possible-missions catalog as FireStationCommand mission content.
- Added expanded vehicle and equipment catalogs derived from MissionChief mission requirements.
- Added generated flat-style mission, vehicle, and equipment images for the expanded catalogs.
- Added catalog quality tests for imported mission XP, narrative coverage, and referenced vehicle/equipment IDs.
- Added specialist equipment kits for traffic control, EMS, law enforcement, HazMat, water rescue, command, rescue, and aviation progression.
- Added equipment image support to equipment purchase confirmation and purchase result embeds.
- Added mutual aid and extension unlock roadmap notes.
- Added station extensions for command, ambulance, police, HazMat, water rescue, wildland, foam, aviation, and rescue specialization.
- Added extension requirements for specialized vehicles, equipment, and mission dispatch families.
- Added tests to keep ambulance and police content behind their station extensions while preserving fire service as the primary core loop.
- Added balance tests that verify mission extension locks include all required vehicle and equipment expansion gates.
- Added a config format regression test so every FireStationCommand config file parses as YAML.
- Added economy-aware purchase scaling for upgrades, vehicles, equipment, training, expansions, and career conversion based on the player's current Red economy balance.
- Added station-wide training scope messaging so players know certifications apply permanently to current and future staff.

### Changed

- Bumped the cog version to `1.2.6`.
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
- Mission readiness now only counts equipment that the station actually owns.
- Mission embeds now warn when required equipment is missing.
- Station and dashboard embeds now show owned equipment counts.
- Mission readiness now factors in missing required training from vehicles and equipment.
- Vehicle and equipment purchases now enforce configured training requirements.
- Imported missions now have broader equipment requirements while preserving repeated MissionChief equipment quantities.
- Imported vehicles now expose equipment slots so owned equipment contributes to mission readiness across the full catalog.
- Vehicle and equipment shop locked lists are now compacted to avoid oversized Discord embed fields.
- Mission selection now skips incidents that require station extensions the player has not built.
- Vehicle and equipment shops now show and enforce extension locks in addition to command-level locks.
- Mission extension locks now inherit requirements from required vehicles and equipment, preventing hidden locked loadouts in core dispatches.
- Early station extension costs are lower so ambulance, police, rescue, command, and extra-bay progression is reachable from early core missions.
- Balance, progression, and training config files now use the same JSON-compatible YAML format as missions, vehicles, equipment, and expansions.
- New stations now start with Basic Firefighting certification.
- Technical Rescue now unlocks at command level 2 so level 2 rescue progression is playable.
- Built expansions now appear on dashboard and station overview embeds.
- Extra Vehicle Bay now increases vehicle capacity when built.
- Vehicle condition now affects station capability and mission readiness calculations.
- Mission results now apply vehicle wear to dispatched vehicles and show repair estimates.
- Dashboard, status, and station embeds now show reputation and maintenance information.
- Mission control refresh now advances overdue turnout, travel, and result steps instead of only showing stale timers.
- Vehicle and equipment shops now paginate catalog options so large catalogs stay within Discord select-menu limits.
- Rebalanced imported mission unlock levels and XP using mission credits, requirement complexity, and capability weight.
- Rewrote every imported mission narrative field with category-aware dispatch, scene, success, partial, and failure text.
- Upgrade and expansion locks now explain that the option is not available yet and show the required command level more clearly.
- Config validation now accepts real YAML instead of requiring JSON-compatible YAML syntax.

### Fixed

- Prevented Red help from appearing after the `[p]fsc` dashboard.
- Dashboard buttons now update the existing dashboard message instead of opening private responses.
- Vehicle shop now has a back button to return to the station dashboard.
- Vehicle purchase confirmation now edits the shop message instead of opening a private interaction flow.
- Fixed the station overview command using expansion, equipment, and training counts before calculating them.

### Planned

- Add automatic dispatch later, after the manual mission flow is more complete.

### Known Issues

- Mission timers still use in-memory `asyncio.sleep` for automatic follow-up while the bot stays online.

## [1.1.1]

### Added

- Added the initial playable FireStationCommand loop with station creation, staff recruitment, station upgrades, career conversion, vehicle purchases, incident turnout, dispatch, travel, and incident resolution.

