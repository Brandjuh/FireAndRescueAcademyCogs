# 🔧 Troubleshooting Guide - Missies worden niet aangemaakt

## Snelle Diagnose

Voer deze commands uit in Discord om te zien wat het probleem is:

```
[p]rr admin debug @jezelf
[p]rr admin stats
```

---

## Veel Voorkomende Problemen & Oplossingen

### ❌ Problem 1: Mission Channel niet geconfigureerd

**Symptoom:** Debug toont "Mission channel not configured!"

**Oplossing:**
```
[p]rr admin setchannel #missions
```
Vervang `#missions` met je gewenste kanaal.

---

### ❌ Problem 2: Geen missions in cache

**Symptoom:** Debug toont "No missions in cache!" of Stats toont "0 missions"

**Oplossing:**
```
[p]rr admin refreshmissions
```

Wacht 30 seconden en check:
```
[p]rr admin stats
```

Als het nog steeds 0 is:
- Check internet connectie
- Check of `https://www.missionchief.com/einsaetze.json` bereikbaar is
- Check Red logs: `[p]logs`

---

### ❌ Problem 3: Scheduler draait niet

**Symptoom:** Debug toont "Scheduler is NOT running!"

**Oplossing:**
```
[p]reload rapidresponse
```

Of herstart bot:
```
[p]restart
```

Check na restart:
```
[p]rr admin debug
```

---

### ❌ Problem 4: Player is not on duty

**Symptoom:** Debug toont "Player is not on duty"

**Oplossing:**
```
[p]rr status on
```

---

### ❌ Problem 5: Thread ID stored maar thread niet gevonden

**Symptoom:** Debug toont "Thread ID stored but not found"

**Oplossing:**
```
[p]rr admin fixthread @user
```

Of handmatig:
```sql
-- Reset thread ID in database
UPDATE players SET thread_id = NULL WHERE user_id = USER_ID;
```

Dan:
```
[p]rr admin forcemission @user
```

---

### ❌ Problem 6: Player heeft cooldown

**Symptoom:** Debug toont "Cooldown: X minutes left"

**Oplossing:** Dit is normaal! Wacht de cooldown af of:
```
[p]rr admin forcemission @user
```

---

### ❌ Problem 7: Player heeft al active mission

**Symptoom:** Debug toont "Has active mission"

**Oplossing:** Voltooi de huidige missie eerst of wacht tot timeout.

Om te resetten (alleen admin):
```sql
-- In database
UPDATE active_missions SET status = 'cancelled' WHERE user_id = USER_ID;
```

---

## Debug Command Gebruik

### Basis Debug
```
[p]rr admin debug
```
Toont jouw status

### Debug andere speler
```
[p]rr admin debug @user
```

### Debug Output Uitleg

**✅ Groene checks:** Alles OK
**⚠️ Gele warnings:** Mogelijk probleem
**❌ Rode errors:** Moet gefixed worden

Voorbeeld goede output:
```
Status: 🟢 Active
Level: 1
Total Missions: 0
Eligible for Mission? ✅ YES

Diagnostics:
✅ Thread exists: #dispatch-username
✅ Scheduler is running
✅ 1500 missions cached
✅ Mission channel: #missions
```

Voorbeeld probleem:
```
Status: 🔴 Inactive
Level: 1
Total Missions: 0
Eligible for Mission? ❌ NO

Diagnostics:
❌ Player is not on duty
❌ No missions in cache!
⚠️ No thread ID stored
✅ Scheduler is running
✅ Mission channel: #missions
```

---

## Test Commands

### Test 1: Manual Scheduler Run
```
[p]rr admin testscheduler
```
Dit forceert de scheduler om direct te checken voor missions.

### Test 2: Force Mission
```
[p]rr admin forcemission @user
```
Wijst direct een missie toe (ignoreert cooldown).

### Test 3: Fix Thread
```
[p]rr admin fixthread @user
```
Probeert thread te repareren/recreëren.

---

## Log Checking

### Red Logs
```
[p]logs
```
Kijk naar regels met "rapidresponse" of "RapidResponse"

### Specifieke Errors

**"No module named 'aiohttp'"**
```bash
pip3 install aiohttp aiosqlite --break-system-packages
```

**"Database is locked"**
- Stop bot
- Check of er geen andere processen database gebruiken
- Start bot opnieuw

**"Permission denied"**
- Check file permissions op cog directory
- Check bot permissions in Discord (threads maken, berichten sturen)

---

## Stap-voor-Stap Complete Check

Voer deze stappen uit in volgorde:

### Stap 1: Check Cog Status
```
[p]cogs
```
Zie je `rapidresponse` in de lijst?

### Stap 2: Check Configuration
```
[p]rr admin stats
```
- Cached missions > 0?
- Active players > 0?

### Stap 3: Set Channel (als nog niet gedaan)
```
[p]rr admin setchannel #missions
```

### Stap 4: Refresh Missions (als 0 cached)
```
[p]rr admin refreshmissions
```

### Stap 5: Go On Duty
```
[p]rr status on
```

### Stap 6: Debug Yourself
```
[p]rr admin debug
```
Check of "Eligible for Mission?" YES is.

### Stap 7: Manual Test (als eligible maar geen missie)
```
[p]rr admin testscheduler
```

Wacht 5 seconden, dan:
```
[p]rr admin forcemission @jezelf
```

### Stap 8: Check Logs
```
[p]logs
```
Zoek naar errors of "rapidresponse"

---

## Direct Database Checks

Als admin commands niet werken, check database direct:

```bash
# Connect to database
sqlite3 /path/to/rapidresponse.db

# Check players
SELECT user_id, is_active, station_level, thread_id FROM players;

# Check mission cache
SELECT COUNT(*) FROM mission_cache;

# Check config
SELECT * FROM config;

# Exit
.exit
```

---

## Common Log Messages

### Goede Berichten (alles werkt):
```
INFO: RapidResponse starting up...
INFO: ✅ Database initialized
INFO: ✅ Loaded 1500 missions from MissionChief
INFO: ✅ Mission channel configured: 123456789
INFO: ✅ Scheduler started (checks every 30 seconds)
INFO: 🚀 RapidResponse startup complete!
INFO: Scheduler check: Found 1 active players
INFO: Player 123456789 is eligible for mission, assigning...
INFO: Starting mission assignment for player 123456789
INFO: Selected mission 123: Structure Fire
INFO: ✅ Successfully assigned mission...
```

### Probleem Berichten:
```
ERROR: ⚠️ WARNING: No missions loaded!
WARNING: ⚠️ Mission channel not configured!
ERROR: Could not get thread for player
ERROR: No mission available for player
```

---

## Emergency Reset

Als niets werkt, complete reset:

### Optie 1: Soft Reset (speler data behouden)
```
[p]unload rapidresponse
[p]rr admin refreshmissions
[p]load rapidresponse
[p]rr admin setchannel #missions
```

### Optie 2: Hard Reset (alles verwijderen)
```bash
# Stop bot
[p]shutdown

# On server
cd /path/to/cogs/rapidresponse/
rm rapidresponse.db

# Start bot
# Reload cog
[p]load rapidresponse
[p]rr admin setchannel #missions
[p]rr admin refreshmissions
```

---

## Still Not Working?

Als missies nog steeds niet spawnen na alle stappen:

1. **Post je debug output:**
```
[p]rr admin debug
[p]rr admin stats
```

2. **Post laatste logs:**
```
[p]logs
```

3. **Check Red info:**
```
[p]debuginfo
```

4. **Manual database inspection:**
```bash
sqlite3 rapidresponse.db
SELECT * FROM players WHERE user_id = YOUR_ID;
SELECT * FROM config;
SELECT COUNT(*) FROM mission_cache;
```

Met deze info kan ik je precies helpen waar het fout gaat!

---

## Quick Fixes Samenvatting

| Probleem | Command |
|----------|---------|
| Geen missions in cache | `[p]rr admin refreshmissions` |
| Channel niet configured | `[p]rr admin setchannel #channel` |
| Scheduler loopt niet | `[p]reload rapidresponse` |
| Thread kapot | `[p]rr admin fixthread @user` |
| Test mission spawn | `[p]rr admin forcemission @user` |
| Check status | `[p]rr admin debug` |
| Manual scheduler run | `[p]rr admin testscheduler` |

---

**Voer eerst `[p]rr admin debug` uit en stuur me de output!** 🔍
