# FireStationCommander Questions

## Resolved Questions

### Existing Player Data During Early Development

- Decision: Existing FireStationCommander players can be reset when starter values or core systems change.
- Reason: There are not many active players yet, and full resets are acceptable during early development.
- Follow-up trigger: Revisit this before the cog has a larger active player base.
- Affected files/systems: `database.py`, admin reset command, future migrations.

## Open Questions

### Salary and Upkeep Cadence

- Question: Should salaries and vehicle upkeep run daily, weekly, or only through manual actions?
- Why it matters: Recurring costs strongly affect economy pressure and player retention.
- Safe default: Do not add recurring costs until Phase 6; keep maintenance as an explicit paid action.
- Affected files/systems: `services/economy.py`, `database.py`, future scheduler/tasks.

### Incident Expiry Behavior

- Question: When an incident expires, should it be ignored automatically, penalize safety, or stay visible as stale?
- Why it matters: Expiry affects player pressure and fairness when Discord interactions time out.
- Safe default: In Phase 3, mark expired incidents as ignored without rewards and show a clear message.
- Affected files/systems: `services/incidents.py`, `database.py`, `views/incident_view.py`.

### Vehicle Failure Thresholds

- Question: Should low-condition vehicles be blocked, risky, or allowed with heavy score penalties?
- Why it matters: Maintenance has to matter without trapping new players.
- Safe default: In Phase 4, block dispatch below 20 condition, add failure chance below 35, and apply score penalties below 60.
- Affected files/systems: `services/vehicles.py`, `services/incidents.py`, `views/incident_view.py`.

### Training Duration

- Question: Should training complete instantly, after a real-time delay, or after a command/check-in?
- Why it matters: Training duration controls personnel availability and progression pacing.
- Safe default: Use instant paid training for the first Phase 5 version, then add timed training later.
- Affected files/systems: future `training` commands, `database.py`, `services/training.py`.

### Regional Assistance Rewards

- Question: How should rewards split when another player assists?
- Why it matters: Multiplayer assistance needs fair rewards and anti-farming rules.
- Safe default: Do not build until Phase 8. Start with capped helper reputation and a small cash share.
- Affected files/systems: future multiplayer services, incident reports, economy.
