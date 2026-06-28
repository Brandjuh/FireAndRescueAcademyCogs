# FireStationCommander Balance Notes

## Economy Assumptions

- Starter cash is 50,000.
- Early incidents should pay enough to cover light maintenance without forcing immediate grinding.
- Maintenance is currently manual and paid only when the player chooses to repair.
- Salary, upkeep, subsidies, and recurring costs are intentionally deferred until Phase 6.

## XP Assumptions

- Players start at command level 1 with 0 XP.
- Command level currently uses a simple XP threshold model.
- XP rewards scale with incident score, so poor outcomes slow progression.
- Future level gates should unlock larger incidents, vehicles, training, and upgrades gradually.

## Incident Reward Assumptions

- Cash reward is `base_reward * score_percentage`.
- XP reward is `base_xp * score_percentage`.
- Reputation changes:
  - score >= 90: +3
  - score >= 70: +1
  - score >= 50: 0
  - score < 50: -2
- Safety changes:
  - score >= 70: 0
  - score < 70: -1
  - score < 40: -3

## Maintenance Cost Assumptions

- Vehicle condition repair costs 25 cash per missing condition point.
- Vehicle refuel costs 5 cash per missing fuel point.
- Vehicle damage repair costs 10 cash per damage point.
- Equipment repair costs 10 cash per missing condition point.
- Phase 4 should add warning thresholds and unavailable states.

## Risk Penalties

- Vehicle condition and fuel already affect incident score.
- Personnel stress and condition already affect incident score.
- Current vehicle wear after incidents scales with incident risk.
- Phase 4 should enforce:
  - condition below 60 affects results more visibly
  - condition below 35 adds failure chance
  - condition below 20 makes vehicle unavailable
  - fuel below 15 prevents dispatch or adds severe penalty

## Known Balance Risks

- Starting cash may be too generous once shops and upgrades exist.
- Early incident rewards may need tuning after maintenance risk becomes stricter.
- No salary/upkeep system exists yet, so long-term money pressure is incomplete.
- Incident selection is simple and may feel repetitive until Phase 7 tactical choices or more balanced content exists.
