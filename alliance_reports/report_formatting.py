"""Shared AllianceReports formatting helpers."""

from typing import Iterable, Optional

import discord


def report_title(audience: str, period_label: str) -> str:
    """Return the standard report title."""
    return f"{audience.upper()} REPORT: {period_label}"


def count_line(label: str, value: int, *, always: bool = False, signed: bool = False) -> Optional[str]:
    """Return a bullet line unless the numeric value is zero."""
    value = int(value or 0)
    if value == 0 and not always:
        return None
    rendered = f"{value:+d}" if signed else f"{value:,}"
    return f"• {label}: {rendered}"


def text_line(label: str, text: str, *, show: bool = True) -> Optional[str]:
    """Return a bullet text line when it should be shown."""
    if not show:
        return None
    return f"• {label}: {text}"


def add_section(embed: discord.Embed, name: str, lines: Iterable[Optional[str]]) -> None:
    """Add an embed section only when it contains visible lines."""
    visible = [line for line in lines if line]
    if visible:
        embed.add_field(name=name, value="\n".join(visible), inline=False)
