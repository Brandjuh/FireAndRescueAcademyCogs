# AllianceReports

Comprehensive reporting system for Fire & Rescue Academy alliance data.

## Version
**1.0.0** - Phase 1: Foundation Complete

## Features

### Current (Phase 1)
- ✅ Configuration system with extensive customization options
- ✅ Time-based scheduler with timezone support
- ✅ Automatic database path detection
- ✅ Permission system (bot owner + admin role)
- ✅ Manual report triggers for testing
- ✅ Error notification system

### Upcoming
- ⏳ Phase 2: Daily Member Reports
- ⏳ Phase 3: Daily Admin Reports
- ⏳ Phase 4: Monthly Member Reports
- ⏳ Phase 5: Monthly Admin Reports
- ⏳ Phase 6: Polish & Advanced Features

## Installation

1. Place the `alliancereports` folder in your Red-DiscordBot cogs directory
2. Load the cog: `[p]load alliancereports`
3. Configure channels: `[p]reportset channel ...`
4. Set timing: `[p]reportset time ...`
5. Enable reports: `[p]reportset enable ...`

## Requirements

### Required Cogs
This cog pulls data from:
- **AllianceScraper** (alliance.db) - Member data, logs, treasury
- **MemberSync** (membersync.db) - Verification data
- **BuildingManager** (building_manager.db) - Building requests
- **SanctionsManager** (sanctions.db) - Discipline data

### Database Auto-Detection
The cog automatically detects databases at:
```
~/.local/share/Red-DiscordBot/data/{instance}/cogs/
├── AllianceScraper/alliance.db
├── MemberSync/membersync.db
├── BuildingManager/building_manager.db
└── SanctionsManager/sanctions.db
```

## Configuration

### Quick Setup
```
# Set channels
[p]reportset channel dailymember #daily-report
[p]reportset channel dailyadmin #admin-daily
[p]reportset channel monthlymember #monthly-report
[p]reportset channel monthlyadmin #admin-monthly
[p]reportset channel error #bot-errors

# Set timing (Amsterdam timezone)
[p]reportset time daily 06:00
[p]reportset time monthly 1 06:00

# Set admin role (bot owner only)
[p]reportset adminrole @ReportAdmin

# Enable reports
[p]reportset enable dailymember
[p]reportset enable dailyadmin
[p]reportset enable monthlymember
[p]reportset enable monthlyadmin

# Check status
[p]reportset status
```

### All Commands

#### Configuration (`[p]reportset`)
- `status` - Show current configuration
- `channel dailymember <channel>` - Set daily member report channel
- `channel dailyadmin <channel>` - Set daily admin report channel
- `channel monthlymember <channel>` - Set monthly member report channel
- `channel monthlyadmin <channel>` - Set monthly admin report channel
- `channel error <channel>` - Set error notification channel
- `time daily <HH:MM>` - Set daily report time (24-hour format)
- `time monthly <day> <HH:MM>` - Set monthly report day and time
- `time timezone <timezone>` - Set timezone (e.g., Europe/Amsterdam)
- `enable dailymember` - Enable daily member reports
- `enable dailyadmin` - Enable daily admin reports
- `enable monthlymember` - Enable monthly member reports
- `enable monthlyadmin` - Enable monthly admin reports
- `disable dailymember` - Disable daily member reports
- `disable dailyadmin` - Disable daily admin reports
- `disable monthlymember` - Disable monthly member reports
- `disable monthlyadmin` - Disable monthly admin reports
- `adminrole <role>` - Set admin role (bot owner only)
- `testmode <true/false>` - Enable/disable test mode
- `database detect` - Re-detect database paths
- `reset` - Reset all settings to defaults (bot owner only)
- `version` - Show cog version

#### Manual Triggers (`[p]report`)
- `daily member` - Generate daily member report now
- `daily admin` - Generate daily admin report now
- `monthly member` - Generate monthly member report now
- `monthly admin` - Generate monthly admin report now
- `test` - Generate all reports in test mode

## Permissions

### Bot Owner
- Full access to all commands
- Can set admin role
- Can reset settings

### Admin Role
- Can configure all settings
- Can manually trigger reports
- Cannot set admin role or reset settings

### Regular Users
- No access to reportset commands
- Can view public reports in configured channels

## Report Types

### Daily Reports
Generated daily at configured time (default: 06:00 Amsterdam time)

#### Member Report (Public)
- New members & verifications
- Training activity
- Building approvals & extensions
- Alliance operations
- Treasury snapshot
- Activity score

#### Admin Report (Admin-Only)
- Detailed member statistics
- Training processing times
- Building approval metrics
- Treasury analysis
- Sanctions summary
- Admin activity tracking
- Action items & alerts

### Monthly Reports
Generated monthly on configured day (default: 1st at 06:00)

#### Member Report (Public)
- Membership growth trends
- Training achievements
- Infrastructure expansion
- Financial health
- Activity comparison (MoM, YoY)
- Fun facts
- Predictions for next month

#### Admin Report (Admin-Only)
- Comprehensive 10-section analysis
- Executive summary
- Deep-dive statistics
- Performance metrics
- Risk analysis
- Recommendations

## Configuration Options

### Channels
- `daily_member_channel` - Where daily member reports post
- `daily_admin_channel` - Where daily admin reports post
- `monthly_member_channel` - Where monthly member reports post
- `monthly_admin_channel` - Where monthly admin reports post
- `error_channel` - Where error notifications go (default: 1422729594103926804)

### Timing
- `daily_time` - Time for daily reports (default: "06:00")
- `monthly_day` - Day of month for monthly reports (default: 1)
- `monthly_time` - Time for monthly reports (default: "06:00")
- `timezone` - Timezone for scheduling (default: "Europe/Amsterdam")

### Report Toggles
Each report type can be enabled/disabled:
- `daily_member_enabled`
- `daily_admin_enabled`
- `monthly_member_enabled`
- `monthly_admin_enabled`

Each report has configurable sections (see config_manager.py for details)

### Thresholds
Configurable alert thresholds:
- `inactive_warning_days` - Days before inactive warning (default: 30)
- `inactive_critical_days` - Days before inactive critical (default: 60)
- `low_contributor_rate` - % below which is concerning (default: 40.0)
- `response_time_target_hours` - SLA target (default: 2)
- `treasury_runway_months` - Minimum safe runway (default: 3)
- `sanction_rate_concern` - Per 100 members/month (default: 8.0)

### Activity Score Weights
Weights for activity score calculation (must sum to 100):
- `membership` (default: 20)
- `training` (default: 20)
- `buildings` (default: 20)
- `treasury` (default: 20)
- `operations` (default: 20)

### Features
- `fun_facts_enabled` - Include fun facts (default: true)
- `fun_facts_count` - Number of fun facts (default: 5)
- `predictions_enabled` - Include predictions (default: true)
- `prediction_confidence` - low/medium/high (default: "medium")
- `previous_report_links` - Link to previous reports (default: true)
- `milestone_alerts` - Send milestone notifications (default: true)

### Comparisons
Enable/disable comparison types:
- `enable_day_over_day` (default: true)
- `enable_week_over_week` (default: true)
- `enable_month_over_month` (default: true)
- `enable_year_over_year` (default: true)

### Advanced
- `test_mode` - Generate but don't post (default: false)
- `verbose_logging` - Extra debug logging (default: false)
- `admin_role_id` - Role ID for admin permissions (default: null)

## Database Structure

The cog reads from these tables:

### AllianceScraper (alliance.db)
- `members_current` - Current member roster
- `members_history` - Historical snapshots
- `logs` - Alliance activity logs
- `treasury_balance` - Treasury balance history
- `treasury_income` - Income tracking (daily/monthly)
- `treasury_expenses` - Expense tracking

### MemberSync (membersync.db)
- `links` - Discord ↔ MC account links
- `queue` - Pending verification requests

### BuildingManager (building_manager.db)
- `building_requests` - All building requests
- `building_actions` - Admin actions on requests
- `geocoding_cache` - Location data cache

### SanctionsManager (sanctions.db)
- `sanctions` - All sanctions issued
- `sanction_history` - Edit/removal history
- `custom_rules` - Custom rule definitions

## Test Mode

Enable test mode to generate reports without posting:

```bash
[p]reportset testmode true
[p]report test
```

This will:
1. Generate all reports
2. Log the output
3. NOT post to channels
4. Show you what would have been posted

Useful for:
- Testing configuration changes
- Previewing report formats
- Debugging issues

## Troubleshooting

### Reports Not Generating

1. **Check scheduler status:**
   ```
   [p]reportset status
   ```
   Look for "Scheduler: 🟢 Running"

2. **Check database paths:**
   ```
   [p]reportset database detect
   ```
   All databases should show "✅ Found"

3. **Check channel configuration:**
   ```
   [p]reportset status
   ```
   All channels should be set

4. **Check if reports are enabled:**
   ```
   [p]reportset status
   ```
   Look for "✅ Enabled" status

### Permission Errors

- Ensure bot has permission to post in configured channels
- Ensure bot has permission to read from channels
- Check that admin role is properly set

### Database Not Found

If databases aren't auto-detected:
1. Verify cogs are loaded: `[p]cogs`
2. Check database files exist in expected locations
3. Try manual detection: `[p]reportset database detect`
4. Check bot logs for detection errors

### Time Issues

If reports generate at wrong time:
1. Check timezone: `[p]reportset status`
2. Verify time format (24-hour): `[p]reportset time daily HH:MM`
3. Check system time on bot server
4. Remember: Time is in configured timezone, not UTC

### Error Notifications

All errors are sent to the configured error channel. Monitor this channel for:
- Database connection errors
- Report generation failures
- Permission issues
- Scheduling problems

## Examples

### Basic Daily Setup
```bash
# Set channel
[p]reportset channel dailymember #daily-stats

# Enable report
[p]reportset enable dailymember

# Test it
[p]report daily member
```

### Complete Setup
```bash
# Channels
[p]reportset channel dailymember #daily-report
[p]reportset channel dailyadmin #admin-daily
[p]reportset channel monthlymember #monthly-report
[p]reportset channel monthlyadmin #admin-monthly

# Timing
[p]reportset time daily 06:00
[p]reportset time monthly 1 06:00

# Enable all
[p]reportset enable dailymember
[p]reportset enable dailyadmin
[p]reportset enable monthlymember
[p]reportset enable monthlyadmin

# Verify
[p]reportset status
```

### Testing Configuration
```bash
# Enable test mode
[p]reportset testmode true

# Generate test reports
[p]report test

# Review output in logs

# Disable test mode
[p]reportset testmode false
```

## Development Roadmap

### ✅ Phase 1: Foundation (COMPLETE)
- Configuration system
- Scheduler with timezone support
- Database detection
- Permission system
- Command structure

### 🔄 Phase 2: Daily Member Reports (NEXT)
- Member activity tracking
- Training statistics
- Building approvals
- Operations summary
- Treasury snapshot
- Activity score calculation

### ⏳ Phase 3: Daily Admin Reports
- Detailed breakdowns
- Processing time metrics
- Action items & alerts
- Admin performance tracking

### ⏳ Phase 4: Monthly Member Reports
- Growth trends & analysis
- Fun facts generator
- Predictions engine
- Historical comparisons

### ⏳ Phase 5: Monthly Admin Reports
- Comprehensive 10-section analysis
- Risk assessment
- Performance reviews
- Recommendations

### ⏳ Phase 6: Polish & Features
- Comparison tools
- Previous report links
- Milestone alerts
- Advanced customization

## Support

### Logs
Check Red-DiscordBot logs for detailed error messages:
```bash
tail -f ~/.local/share/Red-DiscordBot/logs/red.log | grep AllianceReports
```

### Debug Mode
Enable verbose logging for extra details:
```bash
[p]reportset verbose true
```

### Issues
If you encounter issues:
1. Check `[p]reportset status`
2. Check bot logs
3. Check error channel
4. Try `[p]reportset database detect`
5. Contact bot administrator

## Credits

**Author:** FireAndRescueAcademy  
**Version:** 1.0.0  
**License:** MIT  

Built for Fire & Rescue Academy alliance on Missionchief USA.

## Changelog

### v1.0.0 (2025-10-13)
- ✅ Initial release - Phase 1 complete
- ✅ Configuration system with extensive customization
- ✅ Time-based scheduler with timezone support
- ✅ Automatic database path detection
- ✅ Permission system (bot owner + admin role)
- ✅ Manual report triggers
- ✅ Error notification system
- ⏳ Report generation (Phase 2-5 upcoming)

---

**Status:** Phase 1 Complete - Foundation Ready  
**Next:** Phase 2 - Daily Member Reports