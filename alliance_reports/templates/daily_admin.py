"""Daily admin report containing only database-backed metrics."""

import logging
from datetime import datetime
from typing import Dict, Optional
from zoneinfo import ZoneInfo

import discord

from ..data_aggregator import DataAggregator

log = logging.getLogger("red.FARA.AllianceReports.DailyAdmin")


class DailyAdminReport:
    """Generate a database-backed daily admin report."""

    def __init__(self, bot, config_manager):
        self.bot = bot
        self.config_manager = config_manager
        self.aggregator = DataAggregator(config_manager)

    async def generate(self) -> Optional[discord.Embed]:
        try:
            tz_str = await self.config_manager.config.timezone()
            now = datetime.now(ZoneInfo(tz_str))
            data = await self.aggregator.get_daily_data()
            if not data:
                return None

            embed = discord.Embed(
                title="🛡️ ADMIN DAILY REPORT",
                description=f"📅 {now.strftime('%A, %B %d, %Y')}",
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
            for section_name, add_section in sections:
                if "error" not in data.get(section_name, {}):
                    add_section(embed, data[section_name])

            embed.set_footer(text=f"Report generated: {now.strftime('%H:%M')} {tz_str}")
            return embed
        except Exception as exc:
            log.exception("Error generating daily admin report: %s", exc)
            return None

    @staticmethod
    def _add_membership(embed: discord.Embed, data: Dict) -> None:
        joined = data.get("new_joins_24h", 0)
        left = data.get("left_24h", 0)
        kicked = data.get("kicked_24h", 0)
        embed.add_field(
            name="👥 Membership - Current Reporting Window",
            value=(
                f"• Current members: {data.get('total_members', 0)}\n"
                f"• Joined: {joined}\n"
                f"• Left: {left}\n"
                f"• Kicked: {kicked}\n"
                f"• Net change: {joined - left - kicked:+d}\n"
                f"• Verifications approved: {data.get('verifications_approved_24h', 0)}\n"
                f"• Verifications pending: {data.get('verifications_pending', 0)}"
            ),
            inline=False,
        )

    @staticmethod
    def _add_training(embed: discord.Embed, data: Dict) -> None:
        embed.add_field(
            name="🎓 Training - Current Reporting Window",
            value=(
                f"• Courses started: {data.get('started_24h', 0)}\n"
                f"• Courses completed: {data.get('completed_24h', 0)}"
            ),
            inline=False,
        )

    @staticmethod
    def _add_buildings(embed: discord.Embed, data: Dict) -> None:
        embed.add_field(
            name="🏗️ Buildings - Current Reporting Window",
            value=(
                f"• Requests processed: {data.get('processed_24h', 0)}\n"
                f"• Requests approved: {data.get('approved_24h', 0)}\n"
                f"• Requests denied: {data.get('denied_24h', 0)}\n"
                f"• Requests pending: {data.get('pending', 0)}\n"
                f"• Extensions started: {data.get('extensions_started_24h', 0)}\n"
                f"• Extensions completed: {data.get('extensions_completed_24h', 0)}"
            ),
            inline=False,
        )

    @staticmethod
    def _add_operations(embed: discord.Embed, data: Dict) -> None:
        embed.add_field(
            name="🎯 Operations - Current Reporting Window",
            value=(
                f"• Large missions started: {data.get('large_missions_started_24h', 0)}\n"
                f"• Alliance events started: {data.get('alliance_events_started_24h', 0)}"
            ),
            inline=False,
        )

    @staticmethod
    def _add_sanctions(embed: discord.Embed, data: Dict) -> None:
        embed.add_field(
            name="⚖️ Sanctions - Current Reporting Window",
            value=(
                f"• Sanctions issued: {data.get('issued_24h', 0)}\n"
                f"• Active warnings: {data.get('active_warnings', 0)}"
            ),
            inline=False,
        )

    @staticmethod
    def _add_admin_activity(embed: discord.Embed, data: Dict) -> None:
        embed.add_field(
            name="📋 Recorded Admin Activity - Current Reporting Window",
            value=(
                f"• Building reviews: {data.get('building_reviews_24h', 0)}\n"
                f"• Sanctions issued: {data.get('sanctions_24h', 0)}\n"
                f"• Most active building reviewer: {data.get('most_active_admin', 'N/A')} "
                f"({data.get('most_active_admin_count', 0)} reviews)"
            ),
            inline=False,
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
