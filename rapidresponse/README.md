# Rapid Response Dispatch

🚒 **A MissionChief-inspired Emergency Response Simulation Game for Discord**

A fully automated Discord game where players manage their own emergency station, respond to real missions from MissionChief USA, level up their stats, and compete on leaderboards.

## Features

- ✅ **Fully Automated**: No admin intervention needed after setup
- 🎯 **Real MissionChief Missions**: Uses actual mission data from the MissionChief API
- 🧵 **Personal Threads**: Each player gets their own dispatch thread
- 🔘 **Button-Based Gameplay**: Intuitive UI with Discord buttons
- 📈 **Progression System**: Level up, improve stats, earn credits
- 🎓 **Training System**: Train stats to improve success rates
- 🏆 **Leaderboards**: Compete with other players
- 💰 **Red Bank Integration**: Seamless integration with Red's economy system
- 🎲 **Multi-Stage Missions**: Complex missions can escalate requiring multiple responses
- ⏱️ **Dynamic Difficulty**: Mission frequency and difficulty scale with player level

## Installation

### Requirements
- Red-DiscordBot v3.5.0 or higher
- Python 3.8+
- Red bank system configured

### Steps

1. **Add the cog folder to your Red instance**:
   ```bash
   # Copy the rapidresponse folder to your cogs directory
   cp -r rapidresponse /path/to/red/instance/cogs/
   ```

2. **Load the cog**:
   ```
   [p]load rapidresponse
   ```

3. **Set up the mission channel** (Admin only):
   ```
   [p]rr admin setchannel #missions
   ```
   This is where player threads will be created.

4. **Verify setup**:
   ```
   [p]rr admin stats
   ```

The cog will automatically:
- Create the database
- Fetch MissionChief missions
- Start the mission scheduler

## Configuration

### Server Restriction
The game only works on the configured server (ID: `543935264234536960`). To change this, edit `config.py`:

```python
GAME_SERVER_ID = YOUR_SERVER_ID
```

### Game Balance
All game balance values can be adjusted in `config.py`:
- Mission cooldowns and timeouts
- XP and credit rewards
- Training costs and duration
- Success chances and difficulty scaling

## Player Commands

### Status Management
```
[p]rr status          - View your current status
[p]rr status on       - Go on duty (start receiving missions)
[p]rr status off      - Go off duty (stop receiving missions)
```

### Profile & Stats
```
[p]rr profile         - View your station profile and stats
[p]rr profile @user   - View another player's profile
```

### Training
```
[p]rr train          - Start a training session to improve stats
```
Training takes 1 hour and costs credits based on current stat level.

**Stats:**
- **Response**: Affects timeouts and overall performance
- **Tactics**: Improves success on fire/tactical missions
- **Logistics**: Reduces penalties and improves efficiency
- **Medical**: Improves medical mission outcomes
- **Command**: Better performance on complex missions

### Leaderboards
```
[p]rr leaderboard level        - Top players by station level
[p]rr leaderboard missions     - Most missions completed
[p]rr leaderboard streak       - Best success streaks
[p]rr leaderboard credits      - Most credits earned
[p]rr leaderboard success_rate - Highest success rates
```

## Admin Commands

### Setup
```
[p]rr admin setchannel #channel    - Set the mission thread channel
[p]rr admin refreshmissions        - Manually refresh mission cache
[p]rr admin stats                  - View game statistics
```

### Player Management
```
[p]rr admin givexp @user <amount>          - Give XP to a player
[p]rr admin givecredits @user <amount>     - Give credits to a player
[p]rr admin setstat @user <stat> <value>   - Set a player's stat
[p]rr admin forcemission @user             - Force assign a mission
```

## Gameplay

### How It Works

1. **Go On Duty**: Use `[p]rr status on` to start receiving missions

2. **Receive Missions**: The bot automatically assigns missions based on:
   - Your station level
   - Time since last mission
   - Current availability
   - Mission difficulty tiers

3. **Respond to Missions**: Each mission presents 4 response options:
   - **Minimal Response** (-15% success, 50% cost)
   - **Standard Response** (base success, 100% cost)
   - **Full Response** (+10% success, 150% cost)
   - **Overwhelming Force** (+20% success, 250% cost)

4. **Mission Outcomes**:
   - **Full Success**: Maximum rewards, morale boost, streak continues
   - **Partial Success**: Reduced rewards, small morale loss
   - **Failure**: Minimal rewards, morale penalty, streak reset
   - **Escalation**: Multi-stage missions may escalate requiring another response

5. **Progress**: Earn XP and credits, level up, improve stats, climb leaderboards

### Mission Tiers
- **Tier 1 (Routine)**: Easy missions for beginners
- **Tier 2 (Standard)**: Moderate difficulty
- **Tier 3 (Complex)**: Challenging missions
- **Tier 4 (Critical)**: High-risk, high-reward

### Success Factors
Your success chance is calculated based on:
- Base success chance (60%)
- Relevant stat levels (primary + secondary stats)
- Mission tier and difficulty
- Response type chosen
- Current morale level
- Mission streak bonus

### Morale System
- Morale affects success chances
- Below 30 morale: -15% success penalty
- Above 80 morale: +5% success bonus
- Gained from successes, lost from failures

### Cooldowns
Mission cooldowns scale with level:
- **Beginners (Lvl 1-5)**: 30-45 minutes between missions
- **Advanced (Lvl 6+)**: 15-25 minutes between missions

### Timeouts
Players have limited time to respond:
- **Beginners**: 2 minutes + tier bonus
- **Advanced (Lvl 6-20)**: 1.5 minutes + tier bonus
- **Expert (Lvl 21+)**: 1 minute + tier bonus

Missing 3 missions automatically sets you **Off Duty**.

## Economy Integration

The cog integrates with Red's bank system:
- Mission rewards are deposited to your bank account
- Training costs are withdrawn from your bank
- Use standard Red economy commands to manage your credits

## Technical Details

### Database Structure
- **players**: User profiles, stats, levels, mission records
- **active_missions**: Current pending missions
- **mission_history**: Completed mission records
- **training**: Active training sessions
- **mission_cache**: Cached MissionChief data
- **config**: Bot configuration

### Mission Data Source
Missions are fetched from: `https://www.missionchief.com/einsaetze.json`
- Cache refreshes every 6 hours
- ~1,500+ real missions available
- Includes fire, medical, police, rescue, and technical missions

### Background Tasks
The scheduler runs every 5 minutes and:
- Assigns missions to eligible players
- Cleans up expired missions
- Completes training sessions
- Handles timeouts

## Troubleshooting

### "Mission channel not configured"
Run `[p]rr admin setchannel #channel` to set the channel.

### "No missions available"
Try `[p]rr admin refreshmissions` to manually refresh the cache.

### Players not receiving missions
Check that:
- They used `[p]rr status on`
- They don't have an active mission already
- They're not in training
- Enough time has passed since their last mission

### Thread not found errors
Threads auto-archive after 1 week. They'll be recreated automatically when the next mission is assigned.

## Support & Development

### Logs
Check Red's logs for detailed information:
```
[p]debug rapidresponse
```

### Database Access
Database located at: `<cog_path>/rapidresponse.db`

Can be accessed directly with SQLite for advanced queries.

## Credits

- **MissionChief USA**: Mission data source
- **Red-DiscordBot**: Bot framework
- **Discord.py**: Discord API wrapper

---

**Version**: 1.0.0  
**Author**: Roel  
**License**: Custom - For use with Fire & Rescue Academy Alliance

🚒 Happy dispatching! 🚑
