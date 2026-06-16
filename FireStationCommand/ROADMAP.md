# FireStationCommand Roadmap Notes

This document tracks larger gameplay systems that should be implemented after the current core loop is stable.

## Requested Gameplay TODOs

- Show the visible station image directly on the FSC start panel so the first dashboard already feels like the player's station.
- Improve purchase-blocked feedback for vehicles, equipment, expansions, and upgrades with clear reasons such as missing credits, missing level, missing extension, full capacity, or already owned.
- Add vehicle selling so players can recover part of the vehicle value, free station capacity, and cleanly remove equipment or unavailable state attached to that vehicle.
- Add a developer menu toggle for user ID `132620654087241729` that enables a testing mode with all unlocks, unrestricted actions, and fast access to credits, XP, station levels, vehicles, equipment, missions, and outcome controls.
- Add a daily command that grants a once-per-day credit and XP bonus, with clear cooldown feedback and balance fields for reward amounts.
- Add level-up messaging that clearly lists newly unlocked missions, vehicles, equipment, trainings, expansions, station upgrades, and feature buttons when a player reaches a new command level.
- Add a repeatable parking lot upgrade that adds extra vehicle parking capacity. It should be purchasable indefinitely, with each additional parking lot upgrade becoming more expensive than the previous one.

## Mutual Aid

Mutual aid should let another player contribute vehicles, staff, and equipment to an active mission without taking ownership of that mission.

### Data Model

- Extend `active_mission` with `mutual_aid_requests`.
- Each request stores `request_id`, `requester_id`, `guild_id`, `channel_id`, `mission_id`, `status`, `created_at`, `expires_at`, and `needed_capabilities`.
- Add `assisting_units` entries with `helper_id`, `vehicle_instance_ids`, `equipment_ids`, `staff_committed`, `capability_snapshot`, `travel_minutes`, `return_at`, and `reward_share`.
- Vehicles committed to another player should be marked unavailable until their return timer finishes.

### Flow

- Mission Control gets a `Request mutual aid` button when readiness is low or required vehicles/equipment are missing.
- Other players see a compact public embed with the request, missing capabilities, and a `Send units` button.
- The helper selects available vehicles from their own station.
- The requester mission readiness is recalculated with local and assisting capability snapshots.
- Mission results split rewards by contribution:
  - Requester receives mission completion credit and reputation impact.
  - Helpers receive a smaller credit and XP share based on contributed capabilities.
  - Helper vehicles take condition loss and become unavailable until return.

### Safeguards

- Prevent a helper from sending the same vehicle to multiple missions.
- Expire unanswered requests automatically.
- Limit one active assistance package per helper per mission.
- Keep mission resolution deterministic by storing helper capability snapshots when the vehicles are dispatched.
- Add cooldowns using the existing `mutual_aid_cooldown_minutes` and `mutual_aid_unavailable_minutes` balance fields.

## Extensions And Unlocks

Extensions should become the second unlock layer after command level. Command level proves player progress; extensions prove station specialization.

### Proposed Extension Effects

- `extra_vehicle_slots`: adds station vehicle capacity.
- `unlock_equipment`: unlocks specific equipment types in the shop.
- `unlock_vehicle_categories`: unlocks vehicle categories or specific vehicle IDs.
- `unlock_mission_types`: adds mission families to the regular dispatch pool.
- `capability_bonus`: adds station-wide bonuses for specific capabilities.
- `maintenance_bonus`: reduces repair time or repair cost.
- `training_bonus`: reduces training time or training cost.
- `mutual_aid_bonus`: improves helper reward share or response time.

### Suggested Extensions

- `workshop`: improves repairs and unlocks advanced maintenance actions.
- `training_facility`: improves training and unlocks specialist certifications.
- `hazmat_unit`: unlocks HazMat equipment, HazMat vehicle depth, and chemical incident frequency.
- `water_rescue_bay`: unlocks water rescue equipment, boats, and water rescue missions.
- `command_room`: unlocks mutual aid coordination, command tablets, and larger incident management.
- `ems_bay`: unlocks EMS equipment and medical mission depth.
- `police_liaison`: unlocks law enforcement equipment and police support missions.
- `aviation_pad`: unlocks aviation rescue equipment and aircraft-related missions.
- `wildland_cache`: unlocks wildland gear, rural fire mission depth, and wildfire support vehicles.
- `foam_storage`: unlocks large foam operations and airport/industrial fire readiness.

### Implementation Order

1. Add extension effects to the catalog parser and tests.
2. Gate selected equipment and vehicle shop entries by command level plus extension effects.
3. Gate regular mission selection by extension effects while still allowing rare challenge incidents.
4. Show missing extension requirements in mission and shop embeds.
5. Add extension build timers after the basic unlock logic is proven.
