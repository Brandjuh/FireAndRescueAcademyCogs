# Rapid Response - MissionChief Game Cog

**Author:** BrandjuhNL

A competitive guessing game for Red-DiscordBot where players identify the correct vehicle requirements for random MissionChief USA missions.

## Features

✅ **Solo and Multiplayer Support**
- Play alone for practice or compete against other alliance members
- Automatic pot splitting for tied winners

✅ **Smart Vehicle Parsing**
- Accept short codes (`FT2 BC1`)
- Accept full names (`2 fire trucks, 1 chief`)
- Accept mixed formats
- Multiple messages per player (accumulative)

✅ **Comprehensive Scoring System**
- Points for using correct vehicle types
- Bonuses for correct counts
- Penalties for over-deployment
- Perfect match bonus

✅ **Full Stats Tracking**
- Games played, wins, win rate
- Total credits won
- Average scores
- Perfect rounds

✅ **Red Economy Integration**
- Uses Red's built-in bank system
- Configurable entry fees
- Automatic payouts to winners

✅ **Bot Restart Recovery**
- Automatically refunds players if bot restarts during a game
- No credits lost!

## Installation

```bash
[p]repo add brandcogs <your-repo-url>
[p]cog install brandcogs rapidresponse
[p]load rapidresponse
```

## Commands

### Player Commands

| Command | Description |
|---------|-------------|
| `[p]rapidresponse start` (or `[p]rr start`) | Start a new game in the current channel |
| `[p]rapidresponse stats [@user]` | View game statistics |
| `[p]rapidresponse help` | Show detailed help and vehicle codes |

### Admin Commands

| Command | Description |
|---------|-------------|
| `[p]rr config view` | View current configuration |
| `[p]rr config entryfee <amount>` | Set entry fee (default: 1000) |
| `[p]rr config lobbytime <seconds>` | Set lobby duration (10-300) |
| `[p]rr config roundtime <seconds>` | Set round duration (15-300) |
| `[p]rr config toggle` | Enable/disable the game |

## How to Play

### 1. Starting a Game
```
[p]rr start
```
This creates a lobby where players can join. The lobby closes after the configured duration (default: 60 seconds).

### 2. Joining
Click the **Join Game** button. You need enough credits for the entry fee.

### 3. Playing
When the round starts, you'll see a mission name and ID. Type your guess for the vehicle requirements!

**Answer formats:**
- Short codes: `FT2 BC1 HR1`
- Full names: `2 fire trucks, 1 battalion chief, 1 heavy rescue`
- Mixed: `FT2, chief 1, heavy rescue 1`

**Tips:**
- You can send multiple messages - they accumulate!
- Wait 2 seconds between messages (rate limited)
- All messages after "ROUND START" count

### 4. Scoring

Your score is calculated based on:

| Action | Points |
|--------|--------|
| Using a required vehicle type | +2 |
| Each correctly matched vehicle | +1 |
| Each over-deployed vehicle | -0.5 |
| Each unnecessary vehicle type | -1 |
| Perfect match (all correct, no extras) | +4 bonus |

**Example:**

Mission requires: `2 Fire Trucks, 1 Battalion Chief`

You answer: `3 Fire Trucks, 1 Battalion Chief`

Score calculation:
- Fire Trucks used: +2
- Correct FT count: +2 (matched 2 out of 3)
- Over-deployed FT: -0.5 (1 extra)
- Battalion Chief used: +2
- Correct BC count: +1 (matched 1)
- **Total: 6.5 points**

### 5. Winning

- **Multiplayer:** Highest score wins the entire pot
- **Tie:** Pot split evenly among winners
- **Solo:** No payout, just stats and practice!

## Vehicle Codes Reference

| Code | Full Name | Aliases |
|------|-----------|---------|
| FT | Fire Truck | Engine, Pumper |
| BC | Battalion Chief | Chief, Command |
| PT | Platform Truck | Ladder, Aerial, Tower Ladder |
| HR | Heavy Rescue | Rescue Truck, Rescue |
| MCV | Mobile Command Vehicle | Command Vehicle |
| MAV | Mobile Air Vehicle | Air Unit, Air Vehicle |
| WT | Water Tanker | Tanker, Tender |
| HM | Hazmat | Hazmat Truck, Haz Mat |
| FI | Fire Investigation | Investigator, Fire Inv |
| LS | Light Supply | Lighting, Light Unit |
| TR | Technical Rescue | Tech Rescue |
| PC | Police Car | Police, Patrol, Cop Car |
| PH | Police Helicopter | Heli, Chopper |
| K9 | K9 Unit | Police Dog, Canine |
| AMB | Ambulance | Medic, EMS |
| FWK | Fire Crane | Crane |

## Configuration

### Default Settings

```json
{
  "entry_fee": 1000,
  "lobby_duration": 60,
  "round_duration": 60,
  "enabled": true
}
```

### Recommended Settings

**For Quick Games:**
```
[p]rr config lobbytime 30
[p]rr config roundtime 45
[p]rr config entryfee 500
```

**For Competitive Play:**
```
[p]rr config lobbytime 90
[p]rr config roundtime 90
[p]rr config entryfee 2000
```

**For Training:**
```
[p]rr config entryfee 0
[p]rr config roundtime 120
```

## Database

The cog creates `RapidResponseGame.db` with the following tables:

- **games**: Game metadata and status
- **game_players**: Player participation and scores
- **rounds**: Round information and mission data
- **round_answers**: Player answers and perfect match tracking

This data powers the statistics system and can be used for:
- Leaderboards
- Historical analysis
- Achievement systems
- Custom reporting

## Technical Details

### Mission Source
Missions are fetched from: `https://www.missionchief.com/einsaetze.json`

The mission cache refreshes every hour to ensure up-to-date data.

### Rate Limiting
- Players can submit answers every 2 seconds
- Prevents spam while allowing corrections

### Error Handling
- Comprehensive error logging
- Automatic refunds on errors
- Graceful recovery from bot restarts

### Performance
- Async database operations
- Efficient message parsing
- Minimal Discord API calls

## Troubleshooting

**"Unable to fetch missions"**
- Check internet connectivity
- MissionChief API may be temporarily down
- Wait a few minutes and try again

**"You need X credits to join"**
- Check your balance: `[p]bank balance`
- Earn more credits or ask admin to reduce entry fee

**"There's already a game running"**
- Only one game per channel at a time
- Wait for current game to finish or use another channel

**Players not refunded after bot restart**
- Check the `recover_from_restart` function
- Manual refund command: `[p]bank deposit @user <amount>`

## Support

For issues, feature requests, or questions:
1. Check this README
2. Use `[p]rr help` for in-game help
3. Contact BrandjuhNL

## Changelog

### Version 1.0.0 (Initial Release)
- Complete game implementation
- Solo and multiplayer support
- Full stats tracking
- Bot restart recovery
- Comprehensive vehicle parsing
- Red economy integration

## License

This cog is part of the Fire & Rescue Academy Alliance bot ecosystem.

## Credits

- **Author:** BrandjuhNL
- **Game Concept:** Based on MissionChief USA vehicle requirements
- **Framework:** Red-DiscordBot
- **API:** MissionChief.com

---

**Made with ❤️ for the Fire & Rescue Academy Alliance**
