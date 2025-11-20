# TIMING FIX - Changelog

## ⚡ Wat is er veranderd?

### Probleem
- Eerste missie duurde 5-45 minuten voordat deze spawned
- Scheduler checkte maar elke 5 minuten

### Oplossing
✅ **Eerste missie nu binnen 30 seconden!**

---

## 📝 Wijzigingen

### 1. Config.py
```python
# OUD
MISSION_CHECK_INTERVAL = 5  # 5 minuten

# NIEUW
MISSION_CHECK_INTERVAL = 0.5  # 30 seconden
FIRST_MISSION_DELAY = 0.5     # Delay voor eerste missie
```

**Effect:** 
- Scheduler checkt nu elke 30 seconden in plaats van 5 minuten
- Nieuwe constant voor eerste missie delay

---

### 2. Scheduler.py - Eligibility Check

**Toegevoegd:** Speciale behandeling voor eerste missie
```python
else:
    # First mission ever - just needs to wait FIRST_MISSION_DELAY
    if player.get('updated_at'):
        went_active = datetime.fromisoformat(player['updated_at'])
        wait_time = timedelta(minutes=config.FIRST_MISSION_DELAY)
        if datetime.utcnow() < went_active + wait_time:
            return False
```

**Effect:**
- Spelers zonder missie geschiedenis (eerste keer) wachten maar 30 seconden
- Na eerste missie: normale cooldown van 30-45 minuten

---

### 3. Rapidresponse.py - Status On Command

**Toegevoegd:** Directe eerste missie assignment
```python
# Check if this is their first time going on duty
is_first_time = player['total_missions'] == 0 and not player['last_mission_time']

if is_first_time:
    embed.add_field(
        name="📻 First Mission",
        value="Your first mission will arrive within **30 seconds**!",
        inline=False
    )
    
# Assign first mission in background
if is_first_time:
    asyncio.create_task(self._assign_first_mission_soon(ctx.author.id))
```

**Effect:**
- Wanneer nieuwe speler `/rr status on` doet, krijgen ze melding
- Na 30 seconden krijgen ze automatisch hun eerste missie
- Geen wachttijd van 30-45 minuten meer voor eerste keer

---

### 4. Rapidresponse.py - Helper Method

**Nieuw:** `_assign_first_mission_soon()` method
```python
async def _assign_first_mission_soon(self, user_id: int):
    """Assign first mission to a new player after a short delay"""
    await asyncio.sleep(30)  # Wait 30 seconds
    # Then assign mission
```

**Effect:**
- Dedicated method voor eerste missie assignment
- Safety checks (nog steeds active? nog geen missie?)

---

## 🎮 Gebruikerservaring

### Voor Nieuwe Spelers:
```
[Speler] /rr status on
[Bot]    🟢 On Duty!
         Your first mission will arrive within 30 seconds!

[30 seconden later]
[Bot]    🚨 [Mission naam]
         [Mission details]
         [Buttons: Minimal/Standard/Full/Overwhelming]
```

### Voor Bestaande Spelers:
```
[Speler] /rr status on
[Bot]    🟢 On Duty!
         Good luck out there! 🚒🚑🚓

[30-45 minuten later, na cooldown]
[Bot]    🚨 [Mission naam]
```

---

## ⚙️ Technische Details

### Scheduler Frequency
- **Was:** Elke 5 minuten (300 seconden)
- **Nu:** Elke 30 seconden
- **Impact:** Hogere responsiviteit, minimale extra load

### Mission Assignment Logic
1. ✅ Scheduler draait elke 30 seconden
2. ✅ Check alle actieve spelers
3. ✅ Voor nieuwe spelers: wacht 30 sec sinds on duty
4. ✅ Voor ervaren spelers: wacht cooldown (30-45 min)
5. ✅ Assign mission als eligible

### Performance Impact
- Scheduler: ~10x vaker (elke 30s vs 5min)
- Database queries: ~10x meer
- Load: Minimaal - enkele DB reads per check
- Voor 10 actieve spelers: ~200 queries/uur (verwaarloosbaar)

---

## 🔄 Migratie

### Als je de oude versie draait:
```bash
# 1. Unload oude cog
[p]unload rapidresponse

# 2. Vervang bestanden met nieuwe ZIP
rm -rf /path/to/cogs/rapidresponse
unzip rapidresponse.zip -d /path/to/cogs/

# 3. Reload cog
[p]load rapidresponse
```

### Database compatibiliteit
✅ **Geen database changes nodig!**
- Alle changes zijn alleen in code
- Bestaande database werkt perfect
- Bestaande spelers blijven hun data behouden

---

## 🧪 Testing

### Test Scenario 1: Nieuwe Speler
```
[p]rr status on
[Wacht 30 seconden]
✅ Missie verschijnt in thread
```

### Test Scenario 2: Admin Force
```
[p]rr admin forcemission @user
✅ Missie verschijnt onmiddellijk
```

### Test Scenario 3: Scheduler Check
```
[Kijk logs na 30 seconden]
✅ "Mission scheduler check..." logs verschijnen elke 30s
```

---

## 📊 Timings Overzicht

| Event | Oude Timing | Nieuwe Timing |
|-------|-------------|---------------|
| Scheduler check | 5 min | 30 sec |
| Eerste missie | 5-45 min | 30 sec |
| Volgende missies | 30-45 min | 30-45 min |
| Mission timeout | 60-120 sec | 60-120 sec |
| Training | 1 uur | 1 uur |

**✅ Alleen eerste missie is versneld!**
**✅ Game balance blijft hetzelfde voor ervaren spelers**

---

## 🎯 Resultaat

**VOOR:**
- Nieuwe speler: `/rr status on` → wacht 5-45 minuten 😴
- Frustratie: "Werkt het wel?"

**NA:**
- Nieuwe speler: `/rr status on` → 30 seconden → 🚨 Missie! 🎉
- Directe feedback en engagement

---

Download nieuwe versie: **[rapidresponse.zip](computer:///mnt/user-data/outputs/rapidresponse.zip)**
