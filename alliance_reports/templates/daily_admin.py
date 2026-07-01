"""Daily admin report containing only database-backed metrics."""

import logging
from datetime import datetime
from typing import Dict, Optional
from zoneinfo import ZoneInfo

import discord

from ..data_aggregator import DataAggregator
from ..report_formatting import add_section, count_line, report_title, text_line

log = logging.getLogger("red.FARA.AllianceReports.DailyAdmin")


class DailyAdminReport:
    """Generate a database-backed daily admin report."""

    def __init__(self, bot, config_manager):
        self.bot = bot
        self.config_manager = config_manager
        self.aggregator = DataAggregator(config_manager, bot)

    async def generate(self) -> Optional[discord.Embed]:
        try:
            tz_str = await self.config_manager.config.timezone()
            now = datetime.now(ZoneInfo(tz_str))
            data = await self.aggregator.get_daily_data()
            if not data:
                return None

            embed = discord.Embed(
                title=report_title("ADMIN", f"{now.strftime('%A, %B %d, %Y')} ({tz_str})"),
                color=discord.Color.dark_blue(),
                timestamp=now,
            )
            sections = (
                ("membership", self._add_membership),
                ("training", self._add_training),
                ("buildings", self._add_buildings),
                ("operations", self._add_operations),
                ("sanctions", self._add_sanctions),
                ("admin_activity", self._add_admin_activity),
            )
            for section_name, add_report_section in sections:
                if "error" not in data.get(section_name, {}):
                    add_report_section(embed, data[section_name])

            embed.set_footer(text=f"Report generated: {now.strftime('%H:%M')} {tz_str}")
            return embed
        except Exception as exc:
            log.exception("Error generating daily admin report: %s", exc)
            return None

    @staticmethod
    def _add_membership(embed: discord.Embed, data: Dict) -> None:
        add_section(
            embed,
            name="👥 Membership - Current Reporting Window",
            lines=(
                count_line("Current members", data.get("total_members", 0)),
                count_line("Join logs recorded", data.get("new_joins_24h", 0)),
                count_line("Leave logs recorded", data.get("left_24h", 0)),
                count_line("Kicked", data.get("kicked_24h", 0)),
                count_line("Verifications approved", data.get("verifications_approved_24h", 0)),
                count_line("Verifications pending", data.get("verifications_pending", 0)),
            ),
        )

    @staticmethod
    def _add_training(embed: discord.Embed, data: Dict) -> None:
        add_section(
            embed,
            name="🎓 Training - Current Reporting Window",
            lines=(
                count_line("Courses started", data.get("started_24h", 0)),
                count_line("Courses completed", data.get("completed_24h", 0)),
            ),
        )

    @staticmethod
    def _add_buildings(embed: discord.Embed, data: Dict) -> None:
        add_section(
            embed,
            name="🏗️ Buildings - Current Reporting Window",
            lines=(
                count_line("Requests processed", data.get("processed_24h", 0)),
                count_line("Requests approved", data.get("approved_24h", 0)),
                count_line("Requests denied", data.get("denied_24h", 0)),
                count_line("Requests pending", data.get("pending", 0)),
                count_line("Extensions started", data.get("extensions_started_24h", 0)),
                count_line("Extensions completed", data.get("extensions_completed_24h", 0)),
            ),
        )

    @staticmethod
    def _add_operations(embed: discord.Embed, data: Dict) -> None:
        add_section(
            embed,
            name="🎯 Operations - Current Reporting Window",
            lines=(
                count_line("Large missions started", data.get("large_missions_started_24h", 0)),
                count_line("Alliance events started", data.get("alliance_events_started_24h", 0)),
            ),
        )

    @staticmethod
    def _add_sanctions(embed: discord.Embed, data: Dict) -> None:
        add_section(
            embed,
            name="⚖️ Sanctions - Current Reporting Window",
            lines=(
                count_line("Sanctions issued", data.get("issued_24h", 0)),
                count_line("Active warnings", data.get("active_warnings", 0)),
                count_line("TAX warnings sent", data.get("tax_warnings_total_24h", 0)),
                count_line("TAX 1st warnings sent", data.get("tax_warning_1_24h", 0)),
                count_line("TAX 2nd warnings sent", data.get("tax_warning_2_24h", 0)),
                count_line("TAX 3rd warnings sent", data.get("tax_warning_3_24h", 0)),
                count_line("TAX auto-kicks", data.get("tax_auto_kicks_24h", 0)),
            ),
        )

    @staticmethod
    def _add_admin_activity(embed: discord.Embed, data: Dict) -> None:
        reviewer_count = data.get("most_active_admin_count", 0)
        add_section(
            embed,
            name="📋 Recorded Admin Activity - Current Reporting Window",
            lines=(
                count_line("Building reviews", data.get("building_reviews_24h", 0)),
                count_line("Sanctions issued", data.get("sanctions_24h", 0)),
                text_line(
                    "Most active building reviewer",
                    f"{data.get('most_active_admin', 'N/A')} ({reviewer_count} reviews)",
                    show=bool(reviewer_count),
                ),
            ),
        )

    async def post(self, channel: discord.TextChannel) -> bool:
        try:
            embed = await self.generate()
            if not embed:
                return False
            await channel.send(embed=embed)
            return True
        except discord.Forbidden:
            log.error("No permission to post in %s", channel.name)
            return False
        except Exception as exc:
            log.exception("Error posting daily admin report: %s", exc)
            return False
