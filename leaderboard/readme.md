# Leaderboard Cog

Alliance leaderboard system for Missionchief USA Fire and Rescue Academy. Automatically posts daily and monthly top 10 rankings for earned credits and treasury contributions.

## Features

- 🏆 **Daily Rankings** - Earned credits & treasury contributions
- 📅 **Monthly Rankings** - End-of-month summaries
- 📊 **Position Tracking** - Shows rank changes (▲ +2, ▼ -3, 🆕 NEW)
- 🥇 **Top 3 Medals** - Visual medals for podium positions
- ⏰ **Automated Posting** - Daily at 06:00 Amsterdam time
- 🎯 **Separate Channels** - Configure different channels for each leaderboard type

## Requirements

- **AllianceScraper cog** - This cog reads data from the AllianceScraper database
- **pytz** - For timezone handling (auto-installed)
- Red-DiscordBot 3.5.0+

## Installation

```
[p]repo add FRAcogs https://github.com/Brandjuh/FireAndRescueAcademyCogs
[p]cog install FRAcogs leaderboard
[p]load leaderboard
```

## Configuration

### Set Channels

Configure where each leaderboard should be posted:

```
[p]leaderboard dailyearnedchannel #daily-earned-credits
[p]leaderboard dailycontribchannel #daily-contributions
[p]leaderboard monthlyearnedchannel #monthly-earned-credits
[p]leaderboard monthlycontribchannel #monthly-contributions
```

### View Settings

```
[p]leaderboard settings
```

Shows current channel configuration and posting schedule.

## Commands

| Command | Permission | Description |
|---------|-----------|-------------|
| `[p]leaderboard dailyearnedchannel <channel>` | Admin | Set channel for daily earned credits leaderboard |
| `[p]leaderboard dailycontribchannel <channel>` | Admin | Set channel for daily treasury contributions leaderboard |
| `[p]leaderboard monthlyearnedchannel <channel>` | Admin | Set channel for monthly earned credits leaderboard |
| `[p]leaderboard monthlycontribchannel <channel>` | Admin | Set channel for monthly treasury contributions leaderboard |
| `[p]leaderboard settings` | Admin | Show current configuration |
| `[p]leaderboard testnow <type>` | Owner | Manually trigger a leaderboard for testing |

### Test Command Types

```
[p]leaderboard testnow daily_earned
[p]leaderboard testnow daily_contrib
[p]leaderboard testnow monthly_earned
[p]leaderboard testnow monthly_contrib
```

## How It Works

### Data Sources

1. **Earned Credits Rankings**
   - Source: `members_history` table in AllianceScraper database
   - Compares most recent scrape with previous period (24h for daily, 30 days for monthly)
   - Shows total earned credits per member

2. **Treasury Contribution Rankings**
   - Source: `treasury_income` table in AllianceScraper database
   - Uses `period='daily'` for daily rankings
   - Uses `period='monthly'` for monthly rankings
   - Shows credits contributed to alliance treasury

### Posting Schedule

- **Daily Leaderboards**: Every day at 06:00 Amsterdam time (CET/CEST)
- **Monthly Leaderboards**: Last day of each month at 06:00 Amsterdam time

### Position Indicators

| Indicator | Meaning |
|-----------|---------|
| 🥇 🥈 🥉 | Top 3 positions |
| `▲ +2` | Moved up 2 positions |
| `▼ -3` | Moved down 3 positions |
| `━` | Same position as previous period |
| `🆕` | New entry in top 10 |

## Example Output

```
🏆 Daily Top 10 - Earned Credits

🥇 PlayerOne - 1,234,567 credits ▲ +1
🥈 PlayerTwo - 987,654 credits ━
🥉 PlayerThree - 876,543 credits ▼ -1
#04 PlayerFour - 765,432 credits 🆕
#05 PlayerFive - 654,321 credits ━
...
```

## Troubleshooting

### Leaderboards not posting

1. Check that AllianceScraper cog is loaded and scraping data:
   ```
   [p]alliance scrape_members
   ```

2. Verify channels are configured:
   ```
   [p]leaderboard settings
   ```

3. Check bot has permission to post in configured channels

4. Test manually:
   ```
   [p]leaderboard testnow daily_earned
   ```

### No data available

- Ensure AllianceScraper has run at least twice (for comparison)
- Check database exists at AllianceScraper data path
- Verify treasury scraping is enabled in AllianceScraper

### Wrong timezone

The cog is hardcoded to Amsterdam time (CET/CEST). If you need a different timezone, edit `leaderboard.py` line 41:
```python
tz = pytz.timezone('Europe/Amsterdam')  # Change to your timezone
```

## Support

For issues or questions:
- Open an issue on [GitHub](https://github.com/Brandjuh/FireAndRescueAcademyCogs/issues)
- Contact Fire and Rescue Academy alliance

## Credits

Created by Fire and Rescue Academy for Missionchief USA alliance management.

## License

MIT License - See repository for details
