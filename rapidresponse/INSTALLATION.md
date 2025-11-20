# Rapid Response Dispatch - Quick Installation Guide

## Installation Steps

### 1. Upload to Red
```bash
# Copy the rapidresponse folder to your Red cogs directory
# Example on your Raspberry Pi:
scp -r rapidresponse/ pi@your-pi:/home/pi/.local/share/Red-DiscordBot/data/instance_name/cogs/
```

### 2. Load the Cog
In Discord:
```
[p]load rapidresponse
```

### 3. Configure Mission Channel
```
[p]rr admin setchannel #missions
```
Replace `#missions` with your desired channel where player threads will be created.

### 4. Test It
```
[p]rr admin stats          # View game stats
[p]rr admin refreshmissions # Fetch missions
[p]rr status on            # Start playing!
```

## Important Notes

### Server Configuration
The game is currently configured to ONLY work on server ID: `543935264234536960`

To change this, edit `rapidresponse/config.py`:
```python
GAME_SERVER_ID = 543935264234536960  # Change to your server ID
```

### Red Bank Requirement
This cog integrates with Red's economy system. Make sure the bank is set up:
```
[p]bankset registeramount 1000  # Starting credits
[p]bank balance                 # Test bank
```

### Dependencies
Required Python packages (should auto-install with Red):
- `aiohttp` - For fetching MissionChief missions
- `aiosqlite` - For database operations

If you get import errors:
```bash
pip install aiohttp aiosqlite
```

## File Structure
```
rapidresponse/
├── __init__.py          # Package initializer
├── info.json            # Cog metadata
├── config.py            # Game configuration
├── models.py            # Database models
├── mission_manager.py   # MissionChief API handler
├── game_logic.py        # Game mechanics
├── scheduler.py         # Automatic mission assignment
├── views.py             # Discord UI components
├── rapidresponse.py     # Main cog file
└── README.md            # Full documentation
```

## First Time Setup Checklist

- [ ] Upload cog to Red cogs directory
- [ ] Load cog: `[p]load rapidresponse`
- [ ] Check server ID in `config.py` matches your server
- [ ] Set mission channel: `[p]rr admin setchannel #channel`
- [ ] Verify missions loaded: `[p]rr admin stats`
- [ ] Test with your account: `[p]rr status on`
- [ ] Wait for first mission (5-45 minutes depending on cooldown)

## Troubleshooting

### "This command is not available in this server"
- Check that you're on the correct server (ID in config.py)
- Make sure the cog is loaded: `[p]cogs`

### "Mission channel not configured"
- Run: `[p]rr admin setchannel #missions`

### No missions appearing
- Check logs: `[p]debug rapidresponse`
- Manually refresh: `[p]rr admin refreshmissions`
- Force a mission: `[p]rr admin forcemission @user`

### Database errors
- Check file permissions on the cog directory
- Database will be created automatically at first run
- Location: `<cogs_dir>/rapidresponse/rapidresponse.db`

## Support

For issues or questions:
1. Check the full README.md
2. Check Red logs: `[p]debuginfo`
3. Check cog logs: `[p]debug rapidresponse`

## Quick Commands Reference

### Player Commands
- `[p]rr status [on/off]` - Manage duty status
- `[p]rr profile [@user]` - View profile
- `[p]rr train` - Start training
- `[p]rr leaderboard <type>` - View leaderboards

### Admin Commands
- `[p]rr admin setchannel #channel` - Configure mission channel
- `[p]rr admin refreshmissions` - Refresh mission cache
- `[p]rr admin stats` - Game statistics
- `[p]rr admin forcemission @user` - Force mission assignment
- `[p]rr admin givexp @user <amount>` - Give XP
- `[p]rr admin givecredits @user <amount>` - Give credits

---

🚒 Ready to dispatch! 🚑
