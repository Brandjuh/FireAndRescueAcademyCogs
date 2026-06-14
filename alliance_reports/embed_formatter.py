"""Formatting for database-backed AllianceReports member output."""

from datetime import datetime
from typing import Any, Dict, Optional

import discord


class EmbedFormatter:
    """Format recorded daily alliance metrics."""

    @staticmethod
    def create_daily_member_embed(
        data: Dict[str, Any],
        now: Optional[datetime] = None,
        timezone_label: Optional[str] = None,
    ) -> discord.Embed:
        now = now or datetime.now()
        period_label = now.strftime("%A, %B %d, %Y")
        timezone_suffix = f" ({timezone_label})" if timezone_label else ""
        embed = discord.Embed(
            title="🔥 FIRE & RESCUE ACADEMY - Daily Dispatch",
            description=f"Reporting date: **{period_label}**{timezone_suffix}",
            color=discord.Color.blue(),
            timestamp=now,
        )
        embed.add_field(
            name="═" * 40,
            value="📊 ALLIANCE ACTIVITY - Current Reporting Window",
            inline=False,
        )

        sections = (
            ("membership", "👥 MEMBERSHIP", EmbedFormatter._format_membership),
            ("training", "🎓 EDUCATION & TRAINING", EmbedFormatter._format_training),
            ("buildings", "🏗️ INFRASTRUCTURE", EmbedFormatter._format_buildings),
            ("operations", "🎯 MAJOR OPERATIONS", EmbedFormatter._format_operations),
        )
        for key, title, formatter in sections:
            section = data.get(key, {})
            if "error" not in section:
                embed.add_field(name=title, value=formatter(section), inline=False)

        embed.set_footer(text="Generated at")
        return embed

    @staticmethod
    def _format_membership(data: Dict[str, Any]) -> str:
        joined = data.get("new_joins_24h", 0)
        left = data.get("left_24h", 0)
        kicked = data.get("kicked_24h", 0)
        lines = [
            f"• **Join logs recorded:** {joined}",
            f"• **Verifications approved:** {data.get('verifications_approved_24h', 0)}",
            f"• **Total current members:** {data.get('total_members', 0)}",
        ]
        if left or kicked:
            lines.append(f"• **Recorded departures:** {left} leave logs, {kicked} kicks")
        return "\n".join(lines)

    @staticmethod
    def _format_training(data: Dict[str, Any]) -> str:
        return (
            f"• **Courses started:** {data.get('started_24h', 0)}\n"
            f"• **Courses completed:** {data.get('completed_24h', 0)}"
        )

    @staticmethod
    def _format_buildings(data: Dict[str, Any]) -> str:
        return (
            f"• **Requests approved:** {data.get('approved_24h', 0)}\n"
            f"• **Extensions started:** {data.get('extensions_started_24h', 0)}\n"
            f"• **Extensions completed:** {data.get('extensions_completed_24h', 0)}"
        )

    @staticmethod
    def _format_operations(data: Dict[str, Any]) -> str:
        return (
            f"• **Large missions started:** {data.get('large_missions_started_24h', 0)}\n"
            f"• **Alliance events started:** {data.get('alliance_events_started_24h', 0)}"
        )
