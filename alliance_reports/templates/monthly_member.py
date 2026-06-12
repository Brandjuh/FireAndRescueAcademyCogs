"""Monthly member report containing only database-backed metrics."""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import discord

from ..data_aggregator import DataAggregator

log = logging.getLogger("red.FARA.AllianceReports.MonthlyMember")


class MonthlyMemberReport:
    """Generate the database-backed monthly member overview."""

    def __init__(self, bot, config_manager):
        self.bot = bot
        self.config_manager = config_manager
        self.aggregator = DataAggregator(config_manager)

    async def generate(self) -> Optional[List[discord.Embed]]:
        try:
            tz_str = await self.config_manager.config.timezone()
            now = datetime.now(ZoneInfo(tz_str))
            last_month = now.replace(day=1) - timedelta(days=1)
            data = await self.aggregator.get_monthly_data(last_month)
            if not data:
                log.error("No data available for monthly member report")
                return None

            embed = self._create_overview(
                data,
                last_month.strftime("%B %Y"),
                now,
                tz_str,
            )
            return [embed]
        except Exception as exc:
            log.exception("Error generating monthly member report: %s", exc)
            return None

    @staticmethod
    def _create_overview(data: Dict, month_name: str, now: datetime, tz_str: str) -> discord.Embed:
        membership = data.get("membership", {})
        training = data.get("training", {})
        buildings = data.get("buildings", {})
        operations = data.get("operations", {})

        embed = discord.Embed(
            title="🔥 FIRE & RESCUE ACADEMY",
            description=f"📊 **Monthly Briefing - {month_name}**",
            color=discord.Color.blue(),
            timestamp=now,
        )
        if "error" not in membership:
            embed.add_field(
                name="👥 Membership",
                value=(
                f"• Starting members: {membership.get('starting_members', 0)}\n"
                f"• Ending members: {membership.get('ending_members', 0)}\n"
                f"• Join logs recorded: {membership.get('new_joins_period', 0)}\n"
                f"• Recorded departures: {membership.get('left_period', 0)} leave logs, "
                f"{membership.get('kicked_period', 0)} kicks\n"
                f"• Net growth: {membership.get('net_growth', 0):+d}"
                ),
                inline=False,
            )
        if "error" not in training:
            embed.add_field(
                name="🎓 Training",
                value=(
                f"• Courses started: {training.get('started_period', 0)}\n"
                f"• Courses completed: {training.get('completed_period', 0)}"
                ),
                inline=False,
            )
        if "error" not in buildings:
            embed.add_field(
                name="🏗️ Buildings",
                value=(
                f"• Requests approved: {buildings.get('approved_period', 0)}\n"
                f"• Extensions started: {buildings.get('extensions_started_period', 0)}\n"
                f"• Extensions completed: {buildings.get('extensions_completed_period', 0)}"
                ),
                inline=False,
            )
        if "error" not in operations:
            embed.add_field(
                name="🎯 Operations",
                value=(
                f"• Large missions started: {operations.get('large_missions_period', 0)}\n"
                f"• Alliance events started: {operations.get('alliance_events_period', 0)}"
                ),
                inline=False,
            )
        embed.set_footer(text=f"Report generated: {now.strftime('%B %d, %Y %H:%M')} {tz_str}")
        return embed

    async def post(self, channel: discord.TextChannel) -> bool:
        try:
            embeds = await self.generate()
            if not embeds:
                return False
            for embed in embeds:
                await channel.send(embed=embed)
            return True
        except discord.Forbidden:
            log.error("No permission to post in %s", channel.name)
            return False
        except Exception as exc:
            log.exception("Error posting monthly member report: %s", exc)
            return False
