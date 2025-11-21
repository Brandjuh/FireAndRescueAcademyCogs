# Rapid Response Cog - Installation & Testing Guide

## 📦 Installation

### 1. Upload to your server

Copy the entire `rapidresponse` folder to your Red-DiscordBot cogs directory:

```bash
# If using the standard Red installation:
cp -r rapidresponse ~/.local/share/Red-DiscordBot/data/<your-bot-name>/cogs/

# Or your custom path
cp -r rapidresponse /path/to/your/bot/cogs/
```

### 2. Load the cog

```
[p]load rapidresponse
```

You should see: "RapidResponse cog loaded"

### 3. Verify it works

```
[p]rr help
```

## 🧪 Testing Checklist

### Basic Functionality

- [ ] **Start a game**
  ```
  [p]rr start
  ```
  - Lobby embed appears
  - Join/Leave/Start/Cancel buttons work
  
- [ ] **Join mechanics**
  - Click "Join Game" button
  - Credits are deducted
  - Player appears in lobby
  - Pot increases
  
- [ ] **Leave mechanics**
  - Click "Leave Game" button
  - Credits are refunded
  - Player removed from lobby
  - Pot decreases

- [ ] **Round start**
  - Either wait for lobby timer or click "Start Now"
  - Mission name and ID appear
  - Round countdown starts

- [ ] **Answer submission**
  - Type various answer formats:
    - `FT2 BC1`
    - `2 fire trucks, 1 chief`
    - `FT2, battalion chief 1`
  - Message gets ✅ reaction
  - Multiple messages accumulate

- [ ] **Round end**
  - After 60 seconds (or configured time)
  - Shows correct answer
  - Shows all player scores
  - Shows score breakdown
  - Winner gets credits

### Edge Cases

- [ ] **Solo mode**
  - Start game, join alone
  - No payout but stats recorded
  
- [ ] **Tie situation**
  - Get two players with same score
  - Pot splits evenly
  
- [ ] **No players**
  - Start game, don't join
  - Game cancels automatically
  
- [ ] **Rate limiting**
  - Send multiple messages quickly
  - Should see "wait 2 seconds" message

- [ ] **Bot restart**
  - Start a game
  - Stop the bot (`[p]shutdown`)
  - Start the bot
  - Check that credits were refunded
  - Check channel for refund message

### Statistics

- [ ] **View stats**
  ```
  [p]rr stats
  [p]rr stats @othermember
  ```
  - Shows games played, wins, winnings
  - Shows average score and perfect rounds

### Configuration

- [ ] **View config**
  ```
  [p]rr config view
  ```
  
- [ ] **Change entry fee**
  ```
  [p]rr config entryfee 500
  ```
  - Start new game
  - Verify new fee is charged

- [ ] **Change timers**
  ```
  [p]rr config lobbytime 30
  [p]rr config roundtime 45
  ```
  - Start new game
  - Verify timers match

- [ ] **Toggle enable/disable**
  ```
  [p]rr config toggle
  [p]rr start
  ```
  - Should get disabled message

## 🐛 Common Issues & Solutions

### Issue: "Unable to fetch missions"
**Solution:** Check internet connection, try again in a few minutes

### Issue: Commands not working
**Solution:** 
```
[p]unload rapidresponse
[p]load rapidresponse
```

### Issue: Database errors
**Solution:** Check that `RapidResponseGame.db` was created in the cog folder

### Issue: Credits not refunding
**Solution:** Check Red's economy is set up:
```
[p]bank balance
[p]bank set @user 10000
```

### Issue: Mission has no vehicle requirements
**Solution:** The cog filters these out automatically and selects another mission

## 📊 Example Test Game Flow

1. **Setup** (as admin):
   ```
   [p]rr config entryfee 100
   [p]rr config lobbytime 20
   [p]rr config roundtime 30
   ```

2. **Start game**:
   ```
   [p]rr start
   ```

3. **Join** (as player):
   - Click "Join Game" button

4. **Wait or start**:
   - Either wait 20 seconds or click "Start Now"

5. **Play**:
   - See mission: "Container fire"
   - Type: `FT1` (1 fire truck)
   - Wait for round to end

6. **Results**:
   - See if answer was correct
   - Check score breakdown
   - Verify credits paid out

7. **Check stats**:
   ```
   [p]rr stats
   ```

## 🎯 Scoring Test Cases

### Test Case 1: Perfect Match
- **Mission requires:** 2 Fire Trucks, 1 Battalion Chief
- **Player answers:** `FT2 BC1`
- **Expected score:** 2 + 2 + 2 + 1 + 4 (bonus) = **11 points**

### Test Case 2: Over-deployment
- **Mission requires:** 1 Fire Truck
- **Player answers:** `FT3`
- **Expected score:** 2 + 1 - 1.0 (2 extra) = **2 points**

### Test Case 3: Missing vehicles
- **Mission requires:** 2 Fire Trucks, 1 Battalion Chief
- **Player answers:** `FT2`
- **Expected score:** 2 + 2 = **4 points** (no perfect bonus)

### Test Case 4: Extra types
- **Mission requires:** 1 Fire Truck
- **Player answers:** `FT1 BC1`
- **Expected score:** 2 + 1 - 1 (extra type) = **2 points**

### Test Case 5: Complete wrong
- **Mission requires:** 2 Fire Trucks
- **Player answers:** `BC1 HR1`
- **Expected score:** -2 (2 extra types) = **0 points** (minimum)

## 🔍 Debugging

Enable detailed logging:

```python
import logging
logging.getLogger("red.rapidresponse").setLevel(logging.DEBUG)
```

Check database:
```bash
cd ~/.local/share/Red-DiscordBot/data/<bot-name>/cogs/rapidresponse/
sqlite3 RapidResponseGame.db

# View tables
.tables

# Check recent games
SELECT * FROM games ORDER BY started_at DESC LIMIT 5;

# Check player stats
SELECT * FROM game_players WHERE user_id = YOUR_USER_ID;
```

## ✅ Post-Installation Checklist

- [ ] Cog loads without errors
- [ ] Help command works
- [ ] Can start a game
- [ ] Lobby buttons work
- [ ] Can submit answers
- [ ] Round ends properly
- [ ] Credits are handled correctly
- [ ] Stats are tracked
- [ ] Config commands work
- [ ] Bot restart recovery works

## 🎉 Ready to Go!

Once all tests pass, your Rapid Response cog is ready for production use!

Recommended next steps:
1. Set appropriate entry fee for your server
2. Announce to your alliance members
3. Run a few test games to familiarize everyone
4. Monitor the first few real games for any issues

## 📞 Support

If you encounter any issues not covered here:
1. Check the console/logs for errors
2. Verify all dependencies are installed
3. Try reloading the cog
4. Check the README.md for additional info

---

**Happy gaming! 🚒🎮**
