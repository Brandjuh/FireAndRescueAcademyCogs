# FireStationCommander Test Plan

## Automated Checks

- Run `python -m pytest tests/test_firestationcommander_mvp.py -q`.
- Run `python -m ruff check firestationcommander tests/test_firestationcommander_mvp.py`.
- Run the full repository test suite before merging changes that touch shared stubs or command behavior.

## Manual Test Checklist

- Load the cog with `[p]load firestationcommander`.
- Run `[p]fsc start` as a new user.
- Run `[p]fsc start` again and confirm starter data is not duplicated.
- Restart the bot and confirm the player data still exists.
- Confirm all visible text is English.

## Command Test Checklist

- `[p]fsc start` creates Station 1, Engine 1, six personnel, starter equipment, and starter trainings.
- `[p]fsc status` shows cash, reputation, command level, XP, safety, morale, station level, vehicle count, personnel count, and active incident count.
- `[p]fsc vehicles` shows callsign, type, condition, reliability, fuel, and status.
- `[p]fsc personnel` shows name, rank, contract, condition, stress, morale, and trainings.
- `[p]fsc incident` creates or reopens an active incident.
- `[p]fsc maintenance` shows vehicle and equipment repair needs.
- `[p]fsc report` shows the latest incident report or a clear message when none exists.
- `[p]fsc reset CONFIRM` removes FireStationCommander progress for the current server and requires admin permissions.

## Interaction Test Checklist

- Dashboard buttons are owner-only.
- Incident dispatch, requirements, and ignore buttons are owner-only.
- Vehicle select only allows the owner to dispatch vehicles.
- Wrong users receive ephemeral errors.
- Stale views disable controls when timed out.
- View callback errors return a clean ephemeral message and do not expose tracebacks.

## Persistence Test Checklist

- Player cash, XP, reputation, safety, and morale persist after restart.
- Station slots and level persist after restart.
- Vehicles, condition, fuel, status, and mileage persist after restart.
- Personnel stress, condition, morale, and trainings persist after restart.
- Active and completed incidents persist after restart.
- Incident reports persist and remain queryable through `[p]fsc report`.

## Edge Cases

- Running commands in DMs returns a clear server-only message.
- Running status/vehicles/personnel/incident/maintenance/report before start returns a clear start-first message.
- Dispatching with no owned available vehicles returns an ephemeral error.
- Dispatching an inactive or completed incident returns an ephemeral error.
- Maintenance with no repair needs returns an ephemeral message.
- Maintenance with insufficient cash returns an ephemeral error.
- Existing early MVP rows with legacy Dutch keys still render/scoring through alias normalization.
- Reset without `CONFIRM` returns a warning and leaves player data untouched.
- Reset deletes only the current server's FireStationCommander players.
