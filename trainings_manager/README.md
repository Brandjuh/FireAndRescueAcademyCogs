# Training Manager (Red Cog)

Laat gebruikers trainingsklassen aanvragen via een menu, laat admins goed- of afkeuren, en stuur eventueel een herinnering zodra de klas klaar is.

## Features
- Start-knop in een instelbaar request-kanaal.
- Stapsgewijs menu: Discipline → Training (gefilterd) → Vergoeding per dag → Optionele referentie → Overzicht + keuze voor herinnering.
- Admin-overzicht in een apart kanaal met **Start Education** en **Afwijzen** knoppen.
- Alleen admins met een ingestelde Discord-rol mogen keuren.
- Bij goedkeuring: aanvrager krijgt bericht en, indien gekozen, automatische herinnering aan het einde.
- Bij afwijzing: admin geeft reden op; aanvrager krijgt deze te zien.
- Admin-queue bericht wordt opgeruimd; logging gaat naar een log-kanaal.
- Herinneringen zijn persistent en overleven restarts (worden elke 30s gecheckt).

## Installatie
1. Plaats deze map in je Red cogs directory of installeer via de zip:
2. Laad de cog:
   ```
   [p]load trainings_manager
   ```

## Config
Stel kanalen en admin-rol in:
```
[p]tmset requestchannel #verzoeken
[p]tmset adminchannel #admin-trainingen
[p]tmset logchannel #training-log
[p]tmset adminrole @TrainingAdmin
[p]tmset post
```

## Notities
- Tijden worden weergegeven in Europe/Amsterdam en als Discord timestamps.
- Vergoedingen: Free, 100, 200, 300, 400, 500 credits per dag per trainee.
- Het aantal trainees is expres niet nodig.
- Trainingsduur is hardcoded op basis van je lijst. Pas `DISCIPLINES` aan indien nodig.