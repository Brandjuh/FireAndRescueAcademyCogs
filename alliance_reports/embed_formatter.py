"""
Embed Formatter for AllianceReports
Creates beautiful Discord embeds for reports.
"""

import logging
from datetime import datetime
from typing import Dict, Any
import discord

log = logging.getLogger("red.FARA.AllianceReports.EmbedFormatter")


class EmbedFormatter:
    """Formats report data into Discord embeds."""
    
    @staticmethod
    def create_daily_member_embed(data: Dict[str, Any], score_data: Dict[str, Any]) -> discord.Embed:
        """
        Create daily member report embed.
        
        Args:
            data: Daily data from DataAggregator
            score_data: Activity score data from ActivityScoreCalculator
        
        Returns:
            Discord embed
        """
        # Determine color based on activity score
        score = score_data.get("overall", 50)
        if score >= 80:
            color = discord.Color.green()
        elif score >= 60:
            color = discord.Color.blue()
        elif score >= 40:
            color = discord.Color.orange()
        else:
            color = discord.Color.red()
        
        # Create embed
        embed = discord.Embed(
            title="🔥 FIRE & RESCUE ACADEMY - Daily Dispatch",
            description=f"📅 {datetime.now().strftime('%A, %B %d, %Y')}",
            color=color,
            timestamp=datetime.utcnow()
        )
        
        # Add separator
        embed.add_field(
            name="═" * 40,
            value="📊 ALLIANCE ACTIVITY - Last 24 Hours",
            inline=False
        )
        
        # Membership section
        membership = data.get("membership", {})
        if "error" not in membership:
            value = EmbedFormatter._format_membership_section(membership)
            embed.add_field(name="👥 MEMBERSHIP", value=value, inline=False)
        
        # Training section
        training = data.get("training", {})
        if "error" not in training:
            value = EmbedFormatter._format_training_section(training)
            embed.add_field(name="🎓 EDUCATION & TRAINING", value=value, inline=False)
        
        # Buildings section
        buildings = data.get("buildings", {})
        if "error" not in buildings:
            value = EmbedFormatter._format_buildings_section(buildings)
            embed.add_field(name="🏗️ INFRASTRUCTURE EXPANSION", value=value, inline=False)
        
        # Operations section
        operations = data.get("operations", {})
        if "error" not in operations:
            value = EmbedFormatter._format_operations_section(operations)
            embed.add_field(name="🎯 MAJOR OPERATIONS", value=value, inline=False)
        
        # Treasury section
        treasury = data.get("treasury", {})
        if "error" not in treasury:
            value = EmbedFormatter._format_treasury_section(treasury)
            embed.add_field(name="💰 TREASURY SNAPSHOT", value=value, inline=False)
        
        # Activity score
        value = EmbedFormatter._format_activity_score(score_data)
        embed.add_field(name="🔥 ACTIVITY SCORE", value=value, inline=False)
        
        # Footer message
        footer_msg = EmbedFormatter._get_footer_message(score)
        embed.add_field(
            name="═" * 40,
            value=f"💬 *{footer_msg}*",
            inline=False
        )
        
        embed.set_footer(text="Generated at")
        
        return embed
    
    @staticmethod
    def _format_membership_section(data: Dict[str, Any]) -> str:
        """Format membership data."""
        lines = []
        
        total = data.get("total_members", 0)
        new_joins = data.get("new_joins_24h", 0)
        left = data.get("left_24h", 0)
        kicked = data.get("kicked_24h", 0)
        verifications = data.get("verifications_approved_24h", 0)
        dod = data.get("day_over_day_change", 0)
        
        # Format changes with arrows
        dod_str = EmbedFormatter._format_change(dod)
        new_str = EmbedFormatter._format_change(new_joins - left - kicked, prefix="")
        
        lines.append(f"• **New members joined:** {new_joins} {dod_str}")
        lines.append(f"• **Verifications approved:** {verifications}")
        lines.append(f"• **Total active members:** {total} ({new_str})")
        
        if left > 0 or kicked > 0:
            departures = []
            if left > 0:
                departures.append(f"{left} left")
            if kicked > 0:
                departures.append(f"{kicked} kicked")
            lines.append(f"• Departures: {', '.join(departures)}")
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_training_section(data: Dict[str, Any]) -> str:
        """Format training data."""
        lines = []
        
        started = data.get("started_24h", 0)
        completed = data.get("completed_24h", 0)
        dod_started = data.get("day_over_day_started", 0)
        dod_completed = data.get("day_over_day_completed", 0)
        
        started_str = EmbedFormatter._format_change(dod_started)
        completed_str = EmbedFormatter._format_change(dod_completed)
        
        lines.append(f"• **Trainings started:** {started} {started_str}")
        lines.append(f"• **Trainings completed:** {completed} {completed_str}")
        
        # Completion rate
        if started > 0:
            rate = (completed / started * 100)
            lines.append(f"• Success rate: {rate:.0f}%")
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_buildings_section(data: Dict[str, Any]) -> str:
        """Format buildings data."""
        lines = []
        
        approved = data.get("approved_24h", 0)
        ext_started = data.get("extensions_started_24h", 0)
        ext_completed = data.get("extensions_completed_24h", 0)
        
        dod_approved = data.get("day_over_day_approved", 0)
        dod_ext_started = data.get("day_over_day_extensions_started", 0)
        dod_ext_completed = data.get("day_over_day_extensions_completed", 0)
        
        lines.append(f"• **New buildings approved:** {approved} {EmbedFormatter._format_change(dod_approved)}")
        lines.append(f"• **Extensions started:** {ext_started} {EmbedFormatter._format_change(dod_ext_started)}")
        lines.append(f"• **Extensions completed:** {ext_completed} {EmbedFormatter._format_change(dod_ext_completed)}")
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_operations_section(data: Dict[str, Any]) -> str:
        """Format operations data."""
        lines = []
        
        missions = data.get("large_missions_started_24h", 0)
        events = data.get("alliance_events_started_24h", 0)
        
        dod_missions = data.get("day_over_day_missions", 0)
        dod_events = data.get("day_over_day_events", 0)
        
        lines.append(f"• **Large missions started:** {missions} {EmbedFormatter._format_change(dod_missions)}")
        lines.append(f"• **Alliance events active:** {events} {EmbedFormatter._format_change(dod_events)}")
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_treasury_section(data: Dict[str, Any]) -> str:
        """Format treasury data."""
        lines = []
        
        balance = data.get("current_balance", 0)
        change = data.get("change_24h", 0)
        change_pct = data.get("change_24h_pct", 0)
        contributors = data.get("contributors_24h", 0)
        
        # Format balance with dots as thousands separator
        balance_str = f"{balance:,}".replace(",", ".")
        change_str = f"{change:+,}".replace(",", ".")
        
        lines.append(f"• **Current balance:** {balance_str} credits")
        lines.append(f"• **24h change:** {change_str} credits ({change_pct:+.1f}%)")
        lines.append(f"• **Top contributors today:** {contributors} members")
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_activity_score(score_data: Dict[str, Any]) -> str:
        """Format activity score."""
        overall = score_data.get("overall", 0)
        components = score_data.get("components", {})
        
        # Create progress bar
        bars = "█" * (overall // 10) + "░" * (10 - overall // 10)
        
        lines = []
        lines.append(f"**{bars} {overall}/100**")
        lines.append("")
        lines.append("Breakdown:")
        lines.append(f"• Membership: {components.get('membership', 0)}/100")
        lines.append(f"• Training: {components.get('training', 0)}/100")
        lines.append(f"• Buildings: {components.get('buildings', 0)}/100")
        lines.append(f"• Treasury: {components.get('treasury', 0)}/100")
        lines.append(f"• Operations: {components.get('operations', 0)}/100")
        
        return "\n".join(lines)
    
    @staticmethod
    def _format_change(value: int, prefix: str = "vs yesterday") -> str:
        """Format a change value with appropriate arrow."""
        if value > 0:
            return f"(+{value} {prefix})" if prefix else f"(+{value})"
        elif value < 0:
            return f"({value} {prefix})" if prefix else f"({value})"
        else:
            return f"(= {prefix})" if prefix else "(=)"
    
    @staticmethod
    def _get_footer_message(score: int) -> str:
        """Get motivational footer message based on score."""
        if score >= 90:
            return "Outstanding performance! The alliance is thriving! 🌟"
        elif score >= 80:
            return "Another strong day! Keep up the great work! 💪"
        elif score >= 70:
            return "Good activity today. Let's keep the momentum going! 📈"
        elif score >= 60:
            return "Decent progress. There's room for improvement! 🎯"
        elif score >= 50:
            return "Moderate activity. Let's step it up tomorrow! ⚡"
        else:
            return "Activity is low. Let's work together to improve! 🚀"
