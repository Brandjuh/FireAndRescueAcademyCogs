"""Monthly admin report containing only database-backed metrics."""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import discord

from ..data_aggregator import DataAggregator

log = logging.getLogger("red.FARA.AllianceReports.MonthlyAdmin")


class MonthlyAdminReport:
    """Generate the database-backed monthly admin overview."""

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
                log.error("No data available for monthly admin report")
                return None

            embed = self._create_overview(
                data,
                selected_month.strftime("%B %Y"),
                now,
                tz_str,
            )
            return [embed]
        except Exception as exc:
            log.exception("Error generating monthly admin report: %s", exc)
            return None

    @staticmethod
    def _create_overview(data: Dict, month_name: str, now: datetime, tz_str: str) -> discord.Embed:
        membership = data.get("membership", {})
        training = data.get("training", {})
        buildings = data.get("buildings", {})
        operations = data.get("operations", {})
        sanctions = data.get("sanctions", {})
        admin = data.get("admin_activity", {})

        embed = discord.Embed(
            title="🛡️ ADMIN MONTHLY REPORT",
            description=f"📊 **{month_name}**",
            color=discord.Color.dark_gold(),
            timestamp=now,
        )
        if "error" not in membership:
            membership_lines = [
                f"• Starting members: {membership.get('starting_members', 0)}",
                f"• Ending members: {membership.get('ending_members', 0)}",
            ]
            if membership.get("log_activity_available", True):
                membership_lines.extend(
                    [
                        f"• Join logs recorded: {membership.get('new_joins_period', 0)}",
                        f"• Leave logs recorded: {membership.get('left_period', 0)}",
                    ]
                )
            membership_lines.extend(
                [
                    f"• Kicked: {membership.get('kicked_period', 0)}",
                    f"• Net growth: {membership.get('net_growth', 0):+d}",
                ]
            )
            embed.add_field(
                name="👥 Membership",
                value="\n".join(membership_lines),
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
            building_lines = [
                f"• Requests approved: {buildings.get('approved_period', 0)}",
                f"• Requests denied: {buildings.get('denied_period', 0)}",
            ]
            if buildings.get("extension_activity_available", True):
                building_lines.extend(
                    [
                        f"• Extensions started: {buildings.get('extensions_started_period', 0)}",
                        f"• Extensions completed: {buildings.get('extensions_completed_period', 0)}",
                    ]
                )
            embed.add_field(
                name="🏗️ Buildings",
                value="\n".join(building_lines),
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
        if "error" not in sanctions:
            embed.add_field(
                name="⚖️ Sanctions",
                value=f"• Issued: {sanctions.get('issued_period', 0)}",
                inline=False,
            )
        if "error" not in admin:
            embed.add_field(
                name="📋 Recorded Admin Activity",
                value=(
                f"• Recorded actions: {admin.get('total_actions_period', 0)}\n"
                f"• Most active building reviewer: "
                f"{admin.get('most_active_admin_name', 'N/A')} "
                f"({admin.get('most_active_admin_count', 0)} reviews)"
                ),
                inline=False,
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
            await channel.send(embeds=embeds)
            return True
        except discord.Forbidden:
            log.error("No permission to post in %s", channel.name)
            return False
        except Exception as exc:
            log.exception("Error posting monthly admin report: %s", exc)
            return False
