"""
Automation features for MemberManager
- Contribution rate monitoring with grace period and consistency checks
- Role drift detection
- Dormancy tracking

UPDATED: Enhanced contribution monitoring with:
- 7-day grace period for new members
- 4 consecutive checks requirement
- Automatic note creation
- Alert channel notifications
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

import discord
import aiosqlite

from .utils import calculate_contribution_trend, format_contribution_trend

log = logging.getLogger("red.FARA.MemberManager.automation")


class ContributionMonitor:
    """
    Monitor member contribution rates and alert when concerning.
    
    Enhanced monitoring includes:
    - Grace period for new members (7 days)
    - Consistency check (4 consecutive checks below threshold)
    - Automatic note creation
    - Alert cooldown (7 days between alerts)
    
    Runs every 12 hours (2x per day).
    """
    
    def __init__(self, bot, db, config, alliance_scraper):
        self.bot = bot
        self.db = db
        self.config = config
        self.alliance_scraper = alliance_scraper
        
        # Track last alert per member to avoid spam
        self._last_alerts: Dict[str, int] = {}
    
    async def run(self):
        """Main monitoring loop."""
        log.info("Contribution monitor started")
        
        while True:
            try:
                # Wait 12 hours between checks
                await asyncio.sleep(12 * 3600)
                
                log.info("Running contribution check...")
                await self._check_all_contributions()
                
            except asyncio.CancelledError:
                log.info("Contribution monitor stopped")
                break
            except Exception as e:
                log.error(f"Error in contribution monitor: {e}", exc_info=True)
                # Wait 1 hour before retrying on error
                await asyncio.sleep(3600)
    
    async def _check_all_contributions(self):
        """
        Check contributions for all linked members.
        
        New logic:
        1. Check if contribution < threshold
        2. Check grace period (> 7 days in alliance)
        3. Check cooldown (> 7 days since last alert)
        4. Check consistency (4+ checks below threshold)
        5. Send alert + create note if all checks pass
        """
        if not self.alliance_scraper:
            log.warning("AllianceScraper not available, skipping contribution check")
            return
        
        # Get LogsScraper for join date checking
        logs_scraper = self.bot.get_cog("LogsScraper")
        
        # Get all MC members
        try:
            mc_members = await self.alliance_scraper.get_members()
        except Exception as e:
            log.error(f"Failed to get MC members: {e}")
            return
        
        threshold = await self.config.contribution_threshold()
        alerts_sent = 0
        
        for mc_member in mc_members:
            mc_id = mc_member.get("user_id") or mc_member.get("mc_user_id")
            if not mc_id:
                continue
            
            mc_name = mc_member.get("name", "Unknown")
            current_rate = mc_member.get("contribution_rate", 0.0)
            
            # 1. CHECK: Is contribution below threshold?
            if current_rate >= threshold:
                continue
            
            # 2. CHECK: Grace period (7 days in alliance)
            join_date = await self._get_join_date(mc_id, mc_name, logs_scraper)
            if join_date:
                days_in_alliance = (datetime.now(timezone.utc) - join_date).days
                if days_in_alliance < 7:
                    log.debug(f"Skipping {mc_name} ({mc_id}): in grace period ({days_in_alliance} days)")
                    continue
            else:
                # No join date found - might be old member, allow alert
                log.debug(f"No join date found for {mc_name} ({mc_id}), proceeding with check")
            
            # 3. CHECK: Cooldown (7 days since last alert)
            last_alert = self._last_alerts.get(mc_id, 0)
            now = int(datetime.now(timezone.utc).timestamp())
            
            if now - last_alert < (7 * 86400):
                log.debug(f"Skipping {mc_name} ({mc_id}): cooldown active")
                continue
            
            # 4. CHECK: Consistency (4 consecutive checks below threshold)
            historical_rates = await self._get_historical_rates(mc_id, weeks=4)
            
            # Need at least 4 checks
            if len(historical_rates) < 4:
                log.debug(f"Skipping {mc_name} ({mc_id}): insufficient data ({len(historical_rates)} checks)")
                continue
            
            # Check if last 4 checks are ALL below threshold
            recent_4 = historical_rates[:4]
            if not all(rate < threshold for rate in recent_4):
                log.debug(f"Skipping {mc_name} ({mc_id}): not consistently low")
                continue
            
            # 🚨 ALL CHECKS PASSED - SEND ALERT
            log.info(f"Sending low contribution alert for {mc_name} ({mc_id}): {current_rate:.1f}%")
            
            success = await self._send_contribution_alert(
                mc_id=mc_id,
                mc_member=mc_member,
                current_rate=current_rate,
                historical_rates=historical_rates
            )
            
            if success:
                self._last_alerts[mc_id] = now
                alerts_sent += 1
        
        log.info(f"Contribution check complete. Sent {alerts_sent} alerts.")
    
    async def _get_join_date(
        self,
        mc_id: str,
        mc_name: Optional[str],
        logs_scraper
    ) -> Optional[datetime]:
        """
        Get when member joined the alliance.
        
        Queries LogsScraper for 'added_to_alliance' event.
        Returns None if not found or if LogsScraper unavailable.
        """
        if not logs_scraper:
            log.debug("LogsScraper not available for join date lookup")
            return None
        
        try:
            db_path = logs_scraper.db_path
            
            if not db_path.exists():
                log.warning(f"LogsScraper database not found: {db_path}")
                return None
            
            async with aiosqlite.connect(db_path) as db:
                cursor = await db.execute(
                    """
                    SELECT MIN(ts) as join_date 
                    FROM logs 
                    WHERE (affected_mc_id = ? OR affected_name = ?)
                    AND action_key = 'added_to_alliance'
                    """,
                    (mc_id, mc_name)
                )
                result = await cursor.fetchone()
                
                if result and result[0]:
                    # Parse ISO timestamp
                    join_date_str = result[0]
                    # Handle both 'Z' and '+00:00' timezone formats
                    if join_date_str.endswith('Z'):
                        join_date_str = join_date_str.replace('Z', '+00:00')
                    
                    join_date = datetime.fromisoformat(join_date_str)
                    log.debug(f"Found join date for {mc_name} ({mc_id}): {join_date}")
                    return join_date
        
        except Exception as e:
            log.error(f"Failed to get join date for {mc_id}: {e}", exc_info=True)
        
        return None
    
    async def _get_historical_rates(
        self,
        mc_id: str,
        weeks: int
    ) -> List[float]:
        """
        Get historical contribution rates for trend analysis.
        
        Returns list of rates (most recent first).
        """
        if not self.alliance_scraper:
            return []
        
        try:
            # Query members_history table for past rates
            rows = await self.alliance_scraper._query_alliance(
                """
                SELECT contribution_rate, scraped_at 
                FROM members_history 
                WHERE user_id=? OR mc_user_id=? 
                ORDER BY scraped_at DESC 
                LIMIT ?
                """,
                (mc_id, mc_id, weeks * 2)  # Get more data for better trend
            )
            
            rates = [row["contribution_rate"] for row in rows if row["contribution_rate"] is not None]
            return rates
        
        except Exception as e:
            log.error(f"Failed to get historical rates for {mc_id}: {e}")
            return []
    
    async def _create_contribution_note(
        self,
        mc_id: str,
        discord_id: Optional[int],
        current_rate: float,
        trend_info: str,
        guild_id: int
    ) -> Optional[str]:
        """
        Create system note for low contribution alert.
        
        Returns note ref_code if successful, None otherwise.
        """
        note_text = (
            f"🔴 Low Contribution Alert\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Current contribution: {current_rate:.1f}%\n"
            f"Trend: {trend_info}\n"
            f"Alert reason: Contribution below 5% threshold for 4+ consecutive checks\n\n"
            f"⚙️ Auto-generated by contribution monitoring system"
        )
        
        try:
            # Use bot's own ID and name for system notes
            bot_id = self.bot.user.id
            bot_name = self.bot.user.name
            
            ref_code = await self.db.add_note(
                guild_id=guild_id,
                discord_id=discord_id,
                mc_user_id=mc_id,
                note_text=note_text,
                author_id=bot_id,
                author_name=bot_name,
                tags=["system", "low_contribution", "automated"]
            )
            
            log.info(f"Created system note {ref_code} for mc_id={mc_id}")
            return ref_code
        
        except Exception as e:
            log.error(f"Failed to create note for {mc_id}: {e}", exc_info=True)
            return None
    
    async def _send_contribution_alert(
        self,
        mc_id: str,
        mc_member: Dict[str, Any],
        current_rate: float,
        historical_rates: List[float]
    ) -> bool:
        """
        Send alert about low contribution rate.
        
        Creates:
        1. Alert embed to admin channel
        2. Automatic note in database
        
        Returns True if alert was sent successfully.
        """
        mc_name = mc_member.get("name", "Unknown")
        
        # Get linked Discord user
        discord_id = None
        discord_user = None
        membersync = self.bot.get_cog("MemberSync")
        
        if membersync:
            link = await membersync.get_link_for_mc(mc_id)
            if link:
                discord_id = link.get("discord_id")
                if discord_id:
                    try:
                        discord_user = await self.bot.fetch_user(discord_id)
                    except Exception as e:
                        log.debug(f"Could not fetch user {discord_id}: {e}")
        
        # Get alert channel
        alert_channel_id = await self.config.admin_alert_channel()
        
        if not alert_channel_id:
            log.warning("No admin alert channel configured - cannot send alert")
            return False
        
        channel = self.bot.get_channel(alert_channel_id)
        
        if not channel:
            log.error(f"Alert channel {alert_channel_id} not found")
            return False
        
        # Calculate trend
        trend_info = format_contribution_trend(historical_rates) if historical_rates else "Unknown"
        
        # Build alert embed
        embed = discord.Embed(
            title="🔴 Low Contribution Alert",
            description=f"**{mc_name}** has a contribution rate below 5%",
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc)
        )
        
        embed.add_field(
            name="Current Rate",
            value=f"{current_rate:.1f}%",
            inline=True
        )
        
        embed.add_field(
            name="Trend",
            value=trend_info,
            inline=True
        )
        
        embed.add_field(
            name="Consecutive Low Checks",
            value="4+ checks below threshold",
            inline=True
        )
        
        if discord_user:
            embed.add_field(
                name="Discord User",
                value=discord_user.mention,
                inline=False
            )
        
        embed.add_field(
            name="MC Profile",
            value=f"[View Profile](https://www.missionchief.com/users/{mc_id})",
            inline=False
        )
        
        # Create note
        guild = channel.guild
        note_ref = await self._create_contribution_note(
            mc_id=mc_id,
            discord_id=discord_id,
            current_rate=current_rate,
            trend_info=trend_info,
            guild_id=guild.id
        )
        
        if note_ref:
            embed.set_footer(text=f"Auto-generated note: {note_ref}")
        else:
            embed.set_footer(text="Note creation failed")
        
        # Send alert
        try:
            await channel.send(embed=embed)
            log.info(f"Sent contribution alert for {mc_name} (note: {note_ref})")
            
            # Log event in database
            try:
                await self.db.add_event(
                    guild_id=guild.id,
                    discord_id=discord_id,
                    mc_user_id=mc_id,
                    event_type="low_contribution_alert",
                    event_data={
                        "current_rate": current_rate,
                        "trend": trend_info,
                        "note_ref": note_ref
                    },
                    triggered_by="automation"
                )
            except Exception as e:
                log.debug(f"Failed to log event: {e}")
            
            return True
        
        except Exception as e:
            log.error(f"Failed to send alert: {e}", exc_info=True)
            return False


class RoleDriftDetector:
    """
    Detect when member roles are out of sync between Discord and MC.
    """
    
    def __init__(self, bot, db, config, membersync, alliance_scraper):
        self.bot = bot
        self.db = db
        self.config = config
        self.membersync = membersync
        self.alliance_scraper = alliance_scraper
    
    async def check_role_drift(self, guild: discord.Guild) -> List[Dict[str, Any]]:
        """
        Check for members with role drift.
        
        Returns list of members with issues.
        """
        issues = []
        
        if not self.membersync or not self.alliance_scraper:
            return issues
        
        # Get all MC members
        try:
            mc_members = await self.alliance_scraper.get_members()
        except Exception as e:
            log.error(f"Failed to get MC members: {e}")
            return issues
        
        verified_role_id = await self.membersync.config.verified_role_id()
        verified_role = guild.get_role(verified_role_id) if verified_role_id else None
        
        for mc_member in mc_members:
            mc_id = mc_member.get("user_id") or mc_member.get("mc_user_id")
            if not mc_id:
                continue
            
            # Get linked Discord account
            link = await self.membersync.get_link_for_mc(mc_id)
            if not link or link.get("status") != "approved":
                continue
            
            discord_id = link.get("discord_id")
            member = guild.get_member(discord_id)
            
            if not member:
                continue
            
            # Check if verified role is missing
            if verified_role and verified_role not in member.roles:
                issues.append({
                    "type": "missing_verified_role",
                    "discord_id": discord_id,
                    "mc_id": mc_id,
                    "member": member,
                    "mc_name": mc_member.get("name")
                })
        
        return issues
    
    async def auto_fix_verified_role(
        self,
        guild: discord.Guild,
        member: discord.Member,
        verified_role: discord.Role
    ) -> bool:
        """
        Automatically restore verified role to a member.
        
        Returns True if successful.
        """
        try:
            await member.add_roles(
                verified_role,
                reason="MemberManager: Auto-restore verified role (linked MC account found)"
            )
            
            # Log event
            await self.db.add_event(
                guild_id=guild.id,
                discord_id=member.id,
                mc_user_id=None,  # Could fetch from link
                event_type="role_restored",
                event_data={
                    "role": verified_role.name,
                    "role_id": verified_role.id
                },
                triggered_by="automation"
            )
            
            log.info(f"Auto-restored verified role to {member} ({member.id})")
            return True
        
        except Exception as e:
            log.error(f"Failed to auto-restore role to {member.id}: {e}")
            return False


class DormancyTracker:
    """
    Track inactive members and send reminders.
    """
    
    def __init__(self, bot, db, config):
        self.bot = bot
        self.db = db
        self.config = config
    
    async def check_dormant_members(
        self,
        guild: discord.Guild,
        threshold_days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Find members who haven't been active in X days.
        
        Note: This requires activity tracking to be implemented.
        For now, returns empty list.
        """
        # TODO: Implement activity tracking
        # This would require:
        # 1. Tracking last message/voice activity
        # 2. Storing in database
        # 3. Comparing against threshold
        
        return []


class CoordinatedDepartureDetector:
    """
    Detect when members leave both Discord and MC within a short timeframe.
    """
    
    def __init__(self, bot, db, config):
        self.bot = bot
        self.db = db
        self.config = config
    
    async def check_coordinated_departure(
        self,
        guild_id: int,
        discord_id: int,
        mc_user_id: str
    ) -> bool:
        """
        Check if member left both platforms within 72 hours.
        
        Returns True if coordinated departure detected.
        """
        # Get recent events
        events = await self.db.get_events(
            discord_id=discord_id,
            mc_user_id=mc_user_id,
            limit=10
        )
        
        discord_leave = None
        mc_leave = None
        
        for event in events:
            event_type = event.get("event_type")
            timestamp = event.get("timestamp", 0)
            
            if event_type == "left_discord" and not discord_leave:
                discord_leave = timestamp
            elif event_type == "left_mc" and not mc_leave:
                mc_leave = timestamp
        
        if discord_leave and mc_leave:
            # Check if within 72 hours (259200 seconds)
            time_diff = abs(discord_leave - mc_leave)
            
            if time_diff <= 259200:
                log.info(
                    f"Coordinated departure detected: Discord {discord_id}, "
                    f"MC {mc_user_id}, time diff: {time_diff/3600:.1f}h"
                )
                return True
        
        return False
