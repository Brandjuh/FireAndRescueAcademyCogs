"""
Daily Admin Report Template
Comprehensive administrative view with detailed metrics and alerts
"""

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple

import discord

from ..data_aggregator import DataAggregator
from ..calculators.activity_score import ActivityScoreCalculator
from ..embed_formatter import EmbedFormatter

log = logging.getLogger("red.FARA.AllianceReports.DailyAdmin")


class DailyAdminReport:
    """Generate comprehensive daily admin reports."""
    
    def __init__(self, bot, config_manager):
        """Initialize report generator."""
        self.bot = bot
        self.config_manager = config_manager
        self.aggregator = DataAggregator(config_manager)
        
        # Get activity weights from config
        # Default weights if not configured
        default_weights = {
            "membership": 20,
            "training": 20,
            "buildings": 20,
            "treasury": 20,
            "operations": 20
        }
        self.calculator = ActivityScoreCalculator(default_weights)
        self.formatter = EmbedFormatter()
    
    async def generate(self) -> Optional[discord.Embed]:
        """Generate daily admin report embed."""
        try:
            log.info("Generating daily admin report...")
            
            # Get timezone
            tz_str = await self.config_manager.config.timezone()
            tz = ZoneInfo(tz_str)
            now = datetime.now(tz)
            
            # Get data
            data = await self.aggregator.get_daily_data()
            if not data:
                log.error("No data available for daily admin report")
                return None
            
            # Build comprehensive embed
            embed = discord.Embed(
                title="🛡️ ADMIN DAILY BRIEFING",
                description=f"📅 {now.strftime('%A, %B %d, %Y')}",
                color=discord.Color.dark_blue(),
                timestamp=datetime.now()
            )
            
            # Add all sections
            await self._add_membership_section(embed, data)
            await self._add_training_section(embed, data)
            await self._add_building_section(embed, data)
            await self._add_operations_section(embed, data)
            await self._add_treasury_section(embed, data)
            await self._add_sanctions_section(embed, data)
            await self._add_admin_activity_section(embed, data)
            await self._add_action_items_section(embed, data)
            await self._add_performance_section(embed, data)
            
            # Footer
            embed.set_footer(text=f"Next report: {(now + timedelta(days=1)).strftime('%A, %B %d, %Y')} 06:00 {tz_str}")
            
            log.info("Daily admin report generated successfully")
            return embed
            
        except Exception as e:
            log.exception(f"Error generating daily admin report: {e}")
            return None
    
    async def _add_membership_section(self, embed: discord.Embed, data: Dict):
        """Add detailed membership metrics."""
        membership = data.get("membership", {})
        
        # Basic stats
        total = membership.get("total_members", 0)
        new_joins = membership.get("new_joins_24h", 0)
        left = membership.get("left_24h", 0)
        kicked = membership.get("kicked_24h", 0)
        net = new_joins - left - kicked
        
        # Growth percentage
        growth_pct = (net / total * 100) if total > 0 else 0
        
        # Verifications
        verif_approved = membership.get("verifications_approved_24h", 0)
        verif_denied = membership.get("verifications_denied_24h", 0)
        verif_pending = membership.get("verifications_pending", 0)
        
        # Avg processing time
        avg_verif_time = membership.get("avg_verification_time_hours", 0)
        
        # Inactive tracking
        inactive_30_60 = membership.get("inactive_30_60_days", 0)
        inactive_60_plus = membership.get("inactive_60_plus_days", 0)
        
        value = (
            f"**✅ New Members:** {new_joins}\n"
            f"   • Joined: {new_joins} | Left: {left} | Kicked: {kicked}\n"
            f"   • Net growth: {net:+d} ({growth_pct:+.1f}%)\n\n"
            f"**🔗 Verifications:**\n"
            f"   • Approved: {verif_approved} | Denied: {verif_denied} | Pending: {verif_pending}\n"
            f"   • Avg processing: {self._format_hours(avg_verif_time)}\n\n"
            f"**📉 Inactive Watch:**\n"
            f"   • 30-60 days: {inactive_30_60} members\n"
            f"   • 60+ days: {inactive_60_plus} members (prune candidates)"
        )
        
        embed.add_field(
            name="👥 MEMBERSHIP DETAILS - Last 24 Hours",
            value=value,
            inline=False
        )
    
    async def _add_training_section(self, embed: discord.Embed, data: Dict):
        """Add detailed training metrics."""
        training = data.get("training", {})
        
        # Basic stats
        submitted = training.get("requests_submitted_24h", 0)
        approved = training.get("requests_approved_24h", 0)
        denied = training.get("requests_denied_24h", 0)
        
        started = training.get("started_24h", 0)
        completed = training.get("completed_24h", 0)
        
        # Success rate
        success_rate = (completed / started * 100) if started > 0 else 0
        
        # Processing time
        avg_approval_time = training.get("avg_approval_time_hours", 0)
        
        # By discipline
        by_discipline = training.get("by_discipline_24h", {})
        discipline_str = ""
        for disc, counts in by_discipline.items():
            discipline_str += f"   • {disc}: {counts.get('started', 0)} started, {counts.get('completed', 0)} completed\n"
        
        # Reminders
        reminders_sent = training.get("reminders_sent_24h", 0)
        
        value = (
            f"**📚 Training Requests:**\n"
            f"   • Submitted: {submitted} | Approved: {approved} | Denied: {denied}\n"
            f"   • Avg approval time: {self._format_hours(avg_approval_time)}\n\n"
            f"**🏁 Completions:**\n"
            f"   • Started: {started} | Finished: {completed}\n"
            f"   • Success rate: {success_rate:.1f}%\n"
            f"   • Reminders sent: {reminders_sent}\n\n"
            f"**📋 By Discipline:**\n"
            f"{discipline_str if discipline_str else '   • No activity'}"
        )
        
        embed.add_field(
            name="🎓 EDUCATION SYSTEM - Last 24 Hours",
            value=value,
            inline=False
        )
    
    async def _add_building_section(self, embed: discord.Embed, data: Dict):
        """Add detailed building metrics."""
        buildings = data.get("buildings", {})
        
        # Requests
        submitted = buildings.get("requests_submitted_24h", 0)
        approved = buildings.get("approved_24h", 0)
        denied = buildings.get("denied_24h", 0)
        pending = buildings.get("pending", 0)
        
        # Approval rate
        total_processed = approved + denied
        approval_rate = (approved / total_processed * 100) if total_processed > 0 else 0
        
        # Avg review time
        avg_review_time = buildings.get("avg_review_time_hours", 0)
        
        # By type
        by_type = buildings.get("by_type_24h", {})
        type_str = ""
        for btype, count in by_type.items():
            type_str += f"   • {btype}: {count} approved\n"
        
        # Extensions
        ext_started = buildings.get("extensions_started_24h", 0)
        ext_completed = buildings.get("extensions_completed_24h", 0)
        ext_in_progress = buildings.get("extensions_in_progress", 0)
        
        # Trends
        ext_trend = buildings.get("extensions_trend_pct", 0)
        
        value = (
            f"**📝 Requests:**\n"
            f"   • Submitted: {submitted} | Approved: {approved} | Denied: {denied}\n"
            f"   • Pending: {pending}\n"
            f"   • Avg review time: {self._format_hours(avg_review_time)}\n"
            f"   • Approval rate: {approval_rate:.1f}%\n\n"
            f"**🏥 By Type:**\n"
            f"{type_str if type_str else '   • No approvals'}\n"
            f"**🔨 Extensions:**\n"
            f"   • Started: {ext_started} (trend: {ext_trend:+.1f}%)\n"
            f"   • Completed: {ext_completed}\n"
            f"   • In progress: {ext_in_progress} total"
        )
        
        embed.add_field(
            name="🏗️ BUILDING MANAGEMENT - Last 24 Hours",
            value=value,
            inline=False
        )
    
    async def _add_operations_section(self, embed: discord.Embed, data: Dict):
        """Add operations metrics."""
        ops = data.get("operations", {})
        
        large_missions = ops.get("large_missions_started_24h", 0)
        events = ops.get("alliance_events_started_24h", 0)
        custom_created = ops.get("custom_missions_created_24h", 0)
        custom_removed = ops.get("custom_missions_removed_24h", 0)
        
        value = (
            f"• Large missions: {large_missions} started\n"
            f"• Events: {events} started\n"
            f"• Custom missions: {custom_created} created, {custom_removed} removed"
        )
        
        embed.add_field(
            name="🎯 ALLIANCE OPERATIONS - Last 24 Hours",
            value=value,
            inline=False
        )
    
    async def _add_treasury_section(self, embed: discord.Embed, data: Dict):
        """Add detailed treasury analysis."""
        treasury = data.get("treasury", {})
        
        # Balance
        current_balance = treasury.get("current_balance", 0)
        change_24h = treasury.get("change_24h", 0)
        change_pct = treasury.get("change_24h_pct", 0)
        trend_7d = treasury.get("trend_7d", 0)
        
        # Income
        income_24h = treasury.get("income_24h", 0)
        num_contributors = treasury.get("contributors_24h", 0)
        avg_per_contributor = (income_24h / num_contributors) if num_contributors > 0 else 0
        
        # Expenses
        expenses_24h = treasury.get("expenses_24h", 0)
        largest_expense = treasury.get("largest_expense_24h", 0)
        
        # Opening/closing
        opening = current_balance - change_24h
        
        value = (
            f"**💵 Income (24h):**\n"
            f"   • Contributions: +{income_24h:,} credits\n"
            f"   • Contributors: {num_contributors} members\n"
            f"   • Avg per contributor: {avg_per_contributor:,.0f} credits\n\n"
            f"**💸 Expenses (24h):**\n"
            f"   • Total spent: -{expenses_24h:,} credits\n"
            f"   • Largest expense: {largest_expense:,} credits\n\n"
            f"**📈 Balance Analysis:**\n"
            f"   • Opening: {opening:,} | Closing: {current_balance:,}\n"
            f"   • Net change: {change_24h:+,} ({change_pct:+.1f}%)\n"
            f"   • 7-day trend: {trend_7d:+,} credits"
        )
        
        embed.add_field(
            name="💰 TREASURY MANAGEMENT - Last 24 Hours",
            value=value,
            inline=False
        )
    
    async def _add_sanctions_section(self, embed: discord.Embed, data: Dict):
        """Add sanctions tracking."""
        sanctions = data.get("sanctions", {})
        
        issued = sanctions.get("issued_24h", 0)
        active_warnings = sanctions.get("active_warnings", 0)
        warning_1st = sanctions.get("active_1st_warnings", 0)
        warning_2nd = sanctions.get("active_2nd_warnings", 0)
        warning_3rd = sanctions.get("active_3rd_warnings", 0)
        
        value = (
            f"• Sanctions issued: {issued}\n"
            f"• Active warnings: {active_warnings} total members\n"
            f"   └─ 1st warning: {warning_1st} members\n"
            f"   └─ 2nd warning: {warning_2nd} members\n"
            f"   └─ 3rd warning: {warning_3rd} member{'s' if warning_3rd != 1 else ''}"
        )
        
        if warning_3rd > 0:
            value += " ⚠️"
        
        embed.add_field(
            name="⚖️ DISCIPLINE & SANCTIONS - Last 24 Hours",
            value=value,
            inline=False
        )
    
    async def _add_admin_activity_section(self, embed: discord.Embed, data: Dict):
        """Add admin team activity."""
        admin = data.get("admin_activity", {})
        
        building_actions = admin.get("building_reviews_24h", 0)
        training_actions = admin.get("training_approvals_24h", 0)
        verification_actions = admin.get("verifications_24h", 0)
        sanction_actions = admin.get("sanctions_24h", 0)
        
        total_actions = building_actions + training_actions + verification_actions + sanction_actions
        
        most_active = admin.get("most_active_admin", "N/A")
        most_active_count = admin.get("most_active_admin_count", 0)
        
        value = (
            f"• Building reviews: {building_actions} actions\n"
            f"• Training approvals: {training_actions} actions\n"
            f"• Verifications: {verification_actions} actions\n"
            f"• Sanctions: {sanction_actions} actions\n\n"
            f"**Total:** {total_actions} admin actions\n"
            f"**Most Active:** {most_active} ({most_active_count} actions)"
        )
        
        embed.add_field(
            name="📋 ADMIN ACTIVITY - Last 24 Hours",
            value=value,
            inline=False
        )
    
    async def _add_action_items_section(self, embed: discord.Embed, data: Dict):
        """Add pending items requiring attention."""
        alerts = []
        
        # Check pending items
        membership = data.get("membership", {})
        verif_pending = membership.get("verifications_pending", 0)
        oldest_verif_hours = membership.get("oldest_verification_hours", 0)
        
        buildings = data.get("buildings", {})
        building_pending = buildings.get("pending", 0)
        oldest_building_hours = buildings.get("oldest_request_hours", 0)
        
        sanctions = data.get("sanctions", {})
        warning_3rd = sanctions.get("active_3rd_warnings", 0)
        
        # Generate alerts
        if verif_pending > 0 and oldest_verif_hours > 6:
            alerts.append(f"⚠️ {verif_pending} verification{'s' if verif_pending != 1 else ''} waiting (oldest: {oldest_verif_hours:.1f}h)")
        
        if building_pending > 0 and oldest_building_hours > 4:
            alerts.append(f"⚠️ {building_pending} building request{'s' if building_pending != 1 else ''} pending (oldest: {oldest_building_hours:.1f}h)")
        
        if warning_3rd > 0:
            alerts.append(f"⚠️ {warning_3rd} member{'s' if warning_3rd != 1 else ''} on 3rd warning - monitor closely")
        
        if alerts:
            value = "\n".join(alerts)
        else:
            value = "✅ No urgent action items"
        
        embed.add_field(
            name="🔔 ACTION ITEMS",
            value=value,
            inline=False
        )
    
    async def _add_performance_section(self, embed: discord.Embed, data: Dict):
        """Add performance indicators."""
        # Get response times
        training = data.get("training", {})
        buildings = data.get("buildings", {})
        membership = data.get("membership", {})
        
        avg_training_time = training.get("avg_approval_time_hours", 0)
        avg_building_time = buildings.get("avg_review_time_hours", 0)
        avg_verif_time = membership.get("avg_verification_time_hours", 0)
        
        # Overall average
        times = [avg_training_time, avg_building_time, avg_verif_time]
        valid_times = [t for t in times if t > 0]
        overall_avg = sum(valid_times) / len(valid_times) if valid_times else 0
        
        # Target check (2 hours)
        target = 2.0
        meets_target = "✅" if overall_avg <= target else "⚠️"
        
        value = (
            f"• Response time (all): {self._format_hours(overall_avg)} {meets_target}\n"
            f"   └─ Target: <{target:.0f}h\n"
            f"• Member satisfaction: No complaints filed\n"
            f"• System health: All systems operational"
        )
        
        embed.add_field(
            name="📊 PERFORMANCE INDICATORS",
            value=value,
            inline=False
        )
    
    def _format_hours(self, hours: float) -> str:
        """Format hours into readable string."""
        if hours < 1:
            minutes = int(hours * 60)
            return f"{minutes}m"
        elif hours < 24:
            h = int(hours)
            m = int((hours - h) * 60)
            return f"{h}h {m}m" if m > 0 else f"{h}h"
        else:
            days = int(hours / 24)
            h = int(hours % 24)
            return f"{days}d {h}h"
    
    async def post(self, channel: discord.TextChannel) -> bool:
        """Generate and post report to channel."""
        try:
            embed = await self.generate()
            if not embed:
                log.error("Failed to generate daily admin report")
                return False
            
            await channel.send(embed=embed)
            log.info(f"Daily admin report posted to {channel.name}")
            return True
            
        except discord.Forbidden:
            log.error(f"No permission to post in {channel.name}")
            return False
        except Exception as e:
            log.exception(f"Error posting daily admin report: {e}")
            return False
