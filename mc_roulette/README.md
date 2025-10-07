# mc_roulette — Incident Roulette (Red Cog)

**Datum:** 2025-10-07

Economy-enabled, seed-based mini-game. Daglimiet, wekelijkse payout cap, perfect-bonus, server-side timers, mobile-vriendelijke UI.

## Installatie
1. Kopieer `mc_roulette` naar je Red cogs path, bv:
```
~/.local/share/Red-Discord-Bot/data/<instance>/cogs/CogManager/cogs/mc_roulette
```
2. Laad de cog:
```
[p]load mc_roulette
```
3. Config (optioneel):
```
[p]roulset cost 50
[p]roulset reward 2 10
[p]roulset limit 1 10000
[p]roulset flags false false
```

## Gebruik
- `roulette start` — start een run (3 calls) en toont een interactieve view (ephemeral).
- `roulette claim` — berekent score en betaalt uit via Red bank.
- `roulette cancel` — annuleert de run (geen refund).

## Scoring
- +3 per vereiste rol die je invult (tot het vereiste aantal)
- −2 per overbodige/te hoge inzet
- +1 speed bonus als < 30s bevestigd
- +4 perfect per call (exact match);
- Perfecte run bonus via config (`ir_bonus_perfect`) komt bij totaalpunten.

## Anti-cheat
- Seed in start-embed (footer), alleen starter kan klikken, TTL 15m, server-side timestamps.
