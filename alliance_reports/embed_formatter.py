"""Formatting for database-backed AllianceReports member output."""

from datetime import datetime
from typing import Any, Dict, Optional

import discord

from .report_formatting import add_section, count_line, report_title


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
            title=report_title("PUBLIC", f"{period_label}{timezone_suffix}"),
            color=discord.Color.blue(),
            timestamp=now,
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
                add_section(embed, title, formatter(section))

        embed.set_footer(text="Generated at")
        return embed

    @staticmethod
    def _format_membership(data: Dict[str, Any]) -> list:
        left = int(data.get("left_24h", 0) or 0)
        kicked = int(data.get("kicked_24h", 0) or 0)
        lines = [
            count_line("Join logs recorded", data.get("new_joins_24h", 0)),
            count_line("Verifications approved", data.get("verifications_approved_24h", 0)),
            count_line("Total current members", data.get("total_members", 0)),
        ]
        if left or kicked:
            departure_parts = []
            if left:
                departure_parts.append(f"{left} leave logs")
            if kicked:
                departure_parts.append(f"{kicked} kicks")
            lines.append(f"• Recorded departures: {', '.join(departure_parts)}")
        return lines

    @staticmethod
    def _format_training(data: Dict[str, Any]) -> tuple:
        return (
            count_line("Courses started", data.get("started_24h", 0)),
            count_line("Courses completed", data.get("completed_24h", 0)),
        )

    @staticmethod
    def _format_buildings(data: Dict[str, Any]) -> tuple:
        return (
            count_line("Requests approved", data.get("approved_24h", 0)),
            count_line("Extensions started", data.get("extensions_started_24h", 0)),
            count_line("Extensions completed", data.get("extensions_completed_24h", 0)),
        )

    @staticmethod
    def _format_operations(data: Dict[str, Any]) -> tuple:
        return (
            count_line("Large missions started", data.get("large_missions_started_24h", 0)),
            count_line("Alliance events started", data.get("alliance_events_started_24h", 0)),
        )
