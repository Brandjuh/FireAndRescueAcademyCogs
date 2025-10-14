"""
Automation features for MemberManager
- Contribution rate monitoring
- Role drift detection
- Dormancy tracking
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

import discord

from .utils import calculate_contribution_trend, is_concerning_contribution

log = logging.getLogger("red.FARA.MemberManager.automation")


class ContributionMonitor:
    """
    Monitor member contribution rates and alert when concerning.
    
    Runs every 12 hours (2x per day as requested).
    """
    
    def __init__(self, bot, db, config, alliance_scraper):
        self.bot = bot
        self.db = db
        self.config = config
        self.alliance_scraper = alliance_scraper
        
        # Track last check per member to avoid spam
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
        """Check contributions for all linked members."""
        if not self.alliance_scraper:
            log.warning("AllianceScraper not available, skipping contribution check")
            return
        
        # Get all MC members
        try:
            mc_members = await self.alliance_scraper.get_members()
        except Exception as e:
            log.error(f"Failed to get MC members: {e}")
            return
        
        threshold = await self.config.contribution_threshold()
        trend_weeks = await self.config.contribution_trend_weeks()
        
        alerts_sent = 0
        
        for mc_member in mc_members:
            mc_id = mc_member.get("user_id") or mc_member.get("mc_user_id")
            if not mc_id:
                continue
            
            current_rate = mc_member.get("contribution_rate", 0.0)
            
            # Get historical rates for trend analysis
            historical_rates = await self._get_historical_rates(mc_id, trend_weeks)
            
            # Check if concerning
            is_concerning, reason = is_concerning_contribution(
                current_rate=current_rate,
                threshold=threshold,
                previous_rate=historical_rates[0] if historical_rates else None,
                drop_threshold=2.0
            )
            
            if is_concerning:
                # Check if we've alerted recently (avoid spam)
                last_alert = self._last_alerts.get(mc_id, 0)
                now = int(datetime.now(timezone.utc).timestamp())
                
                # Only alert once per week
                if now - last_alert < (7 * 86400):
                    continue
                
                # Send alert
                success = await self._send_contribution_alert(
                    mc_id=mc_id,
                    mc_member=mc_member,
                    current_rate=current_rate,
                    reason=reason,
                    historical_rates=historical_rates
                )
                
                if success:
                    self._last_alerts[mc_id] = now
                    alerts_sent += 1
        
        log.info(f"Contribution check complete. Sent {alerts_sent} alerts.")
    
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
    
    async def _send_contribution_alert(
        self,
        mc_id: str,
        mc_member: Dict[str, Any],
        current_rate: float,
        reason: str,
        historical_rates: List[float]
    ) -> bool:
        """
        Send alert about concerning contribution rate.
        
        Returns True if alert was sent successfully.
        """
        mc_name = mc_member.get("name", "Unknown")
        
        # Get linked Discord user
        discord_id = None
        membersync = self.bot.get_cog("MemberSync")
        
        if membersync:
            link = await membersync.get_link_for_mc(mc_id)
            if link:
                discord_id = link.get("discord_id")
        
        # Calculate trend
        trend_data = calculate_contribution_trend(historical_rates) if historical_rates else None
        
        # Build alert embed
        embed = discord.Embed(
            title="📉 Low Contribution Rate Alert",
            description=f"**{mc_name}** (MC ID: `{mc_id}`)",
            color=discord.Color.orange()
        )
        
        embed.add_field(
            name="Current Rate",
            value=f"{current_rate:.1f}%",
            inline=True
        )
        
        if trend_data:
            embed.add_field(
                name="Trend",
                value=f"{trend_data['trend'].title()} {trend_data.get('emoji', '')}",
                inline=True
            )
            
            if trend_data.get("analysis"):
                embed.add_field(
                    name="Analysis",
                    value=trend_data["analysis"],
                    inline=False
                )
        
        embed.add_field(
            name="Reason",
            value=reason,
            inline=False
        )
        
        if discord_id:
            embed.add_field(
                name="Discord Account",
                value=f"<@{discord_id}>",
                inline=False
            )
        
        embed.set_footer(text="MemberManager Automation")
        embed.timestamp = datetime.now(timezone.utc)
        
        # Send to admin alert channel
        alert_channel_id = await self.config.admin_alert_channel()
        
        if alert_channel_id:
            channel = self.bot.get_channel(alert_channel_id)
            if channel:
                try:
                    await channel.send(embed=embed)
                    
                    # Log event
                    await self.db.add_event(
                        guild_id=channel.guild.id,
                        discord_id=discord_id,
                        mc_user_id=mc_id,
                        event_type="contribution_drop",
                        event_data={
                            "current_rate": current_rate,
                            "reason": reason,
                            "trend": trend_data["trend"] if trend_data else "unknown"
                        },
                        triggered_by="automation"
                    )
                    
                    return True
                
                except Exception as e:
                    log.error(f"Failed to send alert to channel {alert_channel_id}: {e}")
        
        # Send DM to member if enabled
        if discord_id and await self.config.get_raw("dm_members_on_alert", default=False):
            try:
                user = await self.bot.fetch_user(discord_id)
                if user:
                    dm_embed = discord.Embed(
                        title="📊 Contribution Rate Notice",
                        description=(
                            f"Hello {mc_name},\n\n"
                            f"We've noticed your contribution rate has dropped to {current_rate:.1f}%. "
                            f"If you need help or have questions, please reach out to an admin.\n\n"
                            f"- Fire & Rescue Academy Leadership"
                        ),
                        color=discord.Color.blue()
                    )
                    
                    await user.send(embed=dm_embed)
            
            except Exception as e:
                log.debug(f"Failed to DM user {discord_id}: {e}")
        
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
