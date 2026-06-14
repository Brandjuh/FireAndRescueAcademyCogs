"""Monthly member report containing only database-backed metrics."""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import discord

from ..data_aggregator import DataAggregator
from ..report_formatting import add_section, count_line, report_title

log = logging.getLogger("red.FARA.AllianceReports.MonthlyMember")


class MonthlyMemberReport:
    """Generate the database-backed monthly member overview."""

    def __init__(self, bot, config_manager):
        self.bot = bot
        self.config_manager = config_manager
        self.aggregator = DataAggregator(config_manager, bot)

    async def generate(
        self,
        report_month: Optional[datetime] = None,
    ) -> Optional[List[discord.Embed]]:
        try:
            tz_str = await self.config_manager.config.timezone()
            now = datetime.now(ZoneInfo(tz_str))
            selected_month = report_month or (now.replace(day=1) - timedelta(days=1))
            data = await self.aggregator.get_monthly_data(selected_month)
            if not data:
                log.error("No data available for monthly member report")
                return None

            embed = self._create_overview(
                data,
                selected_month.strftime("%B %Y"),
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
            title=report_title("PUBLIC", month_name),
            color=discord.Color.blue(),
            timestamp=now,
        )
        if "error" not in membership:
            membership_lines = [
                count_line("Starting members", membership.get("starting_members", 0)),
                count_line("Ending members", membership.get("ending_members", 0)),
            ]
            if membership.get("log_activity_available", True):
                membership_lines.append(count_line("Join logs recorded", membership.get("new_joins_period", 0)))
                departures = []
                left = int(membership.get("left_period", 0) or 0)
                kicked = int(membership.get("kicked_period", 0) or 0)
                if left:
                    departures.append(f"{left} leave logs")
                if kicked:
                    departures.append(f"{kicked} kicks")
                if departures:
                    membership_lines.append(f"• Recorded departures: {', '.join(departures)}")
            else:
                membership_lines.append(count_line("Kicks recorded", membership.get("kicked_period", 0)))
            membership_lines.append(count_line("Net growth", membership.get("net_growth", 0), signed=True))
            add_section(embed, "👥 Membership", membership_lines)
        if "error" not in training:
            add_section(
                embed,
                "🎓 Training",
                (
                    count_line("Courses started", training.get("started_period", 0)),
                    count_line("Courses completed", training.get("completed_period", 0)),
                ),
            )
        if "error" not in buildings:
            building_lines = [
                count_line("Requests approved", buildings.get("approved_period", 0)),
            ]
            if buildings.get("extension_activity_available", True):
                building_lines.extend(
                    [
                        count_line("Extensions started", buildings.get("extensions_started_period", 0)),
                        count_line("Extensions completed", buildings.get("extensions_completed_period", 0)),
                    ]
                )
            add_section(embed, "🏗️ Buildings", building_lines)
        if "error" not in operations:
            add_section(
                embed,
                "🎯 Operations",
                (
                    count_line("Large missions started", operations.get("large_missions_period", 0)),
                    count_line("Alliance events started", operations.get("alliance_events_period", 0)),
                ),
            )
        embed.set_footer(text=f"Report generated: {now.strftime('%B %d, %Y %H:%M')} {tz_str}")
        return embed

    async def post(
        self,
        channel: discord.TextChannel,
        report_month: Optional[datetime] = None,
    ) -> bool:
        try:
            embeds = await self.generate(report_month=report_month)
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
