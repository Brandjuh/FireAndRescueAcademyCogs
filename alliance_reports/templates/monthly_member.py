"""
Monthly Member Report Template
Comprehensive monthly overview with fun facts, predictions, and historical comparisons
"""

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional

import discord

from ..data_aggregator import DataAggregator
from ..calculators.activity_score import ActivityScoreCalculator
from ..calculators.trends import TrendsCalculator
from ..calculators.fun_facts import FunFactsGenerator
from ..calculators.predictions import PredictionsCalculator
from ..embed_formatter import EmbedFormatter

log = logging.getLogger("red.FARA.AllianceReports.MonthlyMember")


class MonthlyMemberReport:
    """Generate comprehensive monthly member reports."""
    
    def __init__(self, bot, config_manager):
        """Initialize report generator."""
        self.bot = bot
        self.config_manager = config_manager
        self.aggregator = DataAggregator(config_manager)
        
        # Get activity weights from config
        default_weights = {
            "membership": 20,
            "training": 20,
            "buildings": 20,
            "treasury": 20,
            "operations": 20
        }
        self.calculator = ActivityScoreCalculator(default_weights)
        self.trends_calc = TrendsCalculator(self.aggregator)
        self.fun_facts = FunFactsGenerator()
        self.predictions = PredictionsCalculator()
        self.formatter = EmbedFormatter()
    
    async def generate(self) -> Optional[List[discord.Embed]]:
        """Generate monthly member report embeds."""
        try:
            log.info("Generating monthly member report...")
            
            # Get timezone
            tz_str = await self.config_manager.config.timezone()
            tz = ZoneInfo(tz_str)
            now = datetime.now(tz)
            
            # Get last month's date range
            first_day = now.replace(day=1)
            last_month = first_day - timedelta(days=1)
            month_name = last_month.strftime("%B %Y")
            
            # Get monthly data
            data = await self.aggregator.get_monthly_data(last_month)
            if not data:
                log.error("No data available for monthly member report")
                return None
            
            # Calculate trends
            trends = await self.trends_calc.calculate_monthly_trends(data, tz)
            
            # Generate fun facts
            fun_facts = await self.fun_facts.generate_facts(data, count=5)
            
            # Generate predictions
            preds = await self.predictions.generate_predictions(data, trends)
            
            # Build embeds (multiple for member report)
            embeds = []
            
            # Main embed - Overview
            main_embed = await self._create_main_embed(data, month_name, now, tz_str)
            if main_embed:
                embeds.append(main_embed)
            
            # Details embed - Breakdown
            details_embed = await self._create_details_embed(data, trends)
            if details_embed:
                embeds.append(details_embed)
            
            # Fun & Predictions embed
            fun_embed = await self._create_fun_predictions_embed(data, fun_facts, preds, month_name)
            if fun_embed:
                embeds.append(fun_embed)
            
            log.info(f"Monthly member report generated with {len(embeds)} embeds")
            return embeds if embeds else None
            
        except Exception as e:
            log.exception(f"Error generating monthly member report: {e}")
            return None
    
    async def _create_main_embed(
        self,
        data: Dict,
        month_name: str,
        now: datetime,
        tz_str: str
    ) -> Optional[discord.Embed]:
        """Create main overview embed."""
        try:
            membership = data.get("membership", {})
            training = data.get("training", {})
            buildings = data.get("buildings", {})
            treasury = data.get("treasury", {})
            
            embed = discord.Embed(
                title="🔥 FIRE & RESCUE ACADEMY",
                description=f"📊 **Monthly Briefing - {month_name}**",
                color=discord.Color.blue(),
                timestamp=now
            )
            
            # Highlights summary
            highlights = (
                f"🎉 **{month_name.upper()} HIGHLIGHTS**\n\n"
                f"We've had an incredible month! Here's what we achieved together:"
            )
            embed.add_field(name="\u200b", value=highlights, inline=False)
            
            # Membership
            start_members = membership.get("starting_members", 0)
            end_members = membership.get("ending_members", 0)
            new_joins = membership.get("new_joins_period", 0)
            net_growth = membership.get("net_growth", 0)
            net_pct = membership.get("net_growth_pct", 0)
            retention = membership.get("retention_rate", 0)
            
            membership_value = (
                f"• Started: {start_members} members\n"
                f"• Ended: {end_members} members\n"
                f"• New joins: {new_joins} ({net_growth:+d} net growth, {net_pct:+.1f}%)\n"
                f"• Retention rate: {retention:.1f}%"
            )
            
            # Add trend indicator
            if net_pct > 0:
                membership_value += " (↑ vs last month)"
            elif net_pct < 0:
                membership_value += " (↓ vs last month)"
            
            embed.add_field(
                name="👥 MEMBERSHIP GROWTH",
                value=membership_value,
                inline=False
            )
            
            # Training achievements
            started = training.get("started_period", 0)
            completed = training.get("completed_period", 0)
            success_rate = training.get("success_rate", 0)
            
            # Top trainings
            top_trainings = training.get("top_5_trainings", [])
            top_str = ""
            for i, (name, count) in enumerate(top_trainings[:5], 1):
                emoji = ["🥇", "🥈", "🥉", "4.", "5."][i-1]
                top_str += f"      {emoji} {name}: {count} trainings\n"
            
            training_value = (
                f"• {started} trainings started\n"
                f"• {completed} trainings completed ({success_rate:.1f}% success rate!)\n\n"
                f"**📚 Most Popular Trainings:**\n{top_str if top_str else '      • No data'}"
            )
            
            embed.add_field(
                name="🎓 EDUCATION ACHIEVEMENTS",
                value=training_value,
                inline=False
            )
            
            # Infrastructure
            approved = buildings.get("approved_period", 0)
            ext_started = buildings.get("extensions_started_period", 0)
            ext_completed = buildings.get("extensions_completed_period", 0)
            
            buildings_value = (
                f"• {approved} new buildings approved!\n"
                f"  └─ Breakdown by type in details\n\n"
                f"**🔨 Expansion Activity:**\n"
                f"   • {ext_started} extensions started\n"
                f"   • {ext_completed} extensions completed"
            )
            
            embed.add_field(
                name="🏗️ INFRASTRUCTURE BOOM",
                value=buildings_value,
                inline=False
            )
            
            # Treasury
            opening = treasury.get("opening_balance", 0)
            closing = treasury.get("closing_balance", 0)
            growth = treasury.get("growth_amount", 0)
            growth_pct = treasury.get("growth_percentage", 0)
            
            treasury_value = (
                f"• Opening Balance: {opening:,} credits\n"
                f"• Closing Balance: {closing:,} credits\n"
                f"• Growth: {growth:+,} credits ({growth_pct:+.1f}%!)\n\n"
                f"**📈 30-day trend:** Strong growth ↗️"
            )
            
            embed.add_field(
                name="💰 FINANCIAL HEALTH",
                value=treasury_value,
                inline=False
            )
            
            # Activity score
            score = data.get("activity_score", 0)
            embed.add_field(
                name="🔥 Overall Activity Score",
                value=f"**{score}/100**",
                inline=False
            )
            
            embed.set_footer(text=f"Report generated: {now.strftime('%B %d, %Y %H:%M')} {tz_str}")
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating main embed: {e}")
            return None
    
    async def _create_details_embed(
        self,
        data: Dict,
        trends: Dict
    ) -> Optional[discord.Embed]:
        """Create detailed breakdown embed."""
        try:
            embed = discord.Embed(
                title="📊 DETAILED ANALYSIS",
                color=discord.Color.dark_blue()
            )
            
            # Training by discipline
            training = data.get("training", {})
            by_discipline = training.get("by_discipline_counts", {})
            
            if by_discipline:
                total_trainings = sum(by_discipline.values())
                disc_str = ""
                for disc, count in sorted(by_discipline.items(), key=lambda x: x[1], reverse=True):
                    pct = (count / total_trainings * 100) if total_trainings > 0 else 0
                    disc_str += f"   • {disc}: {count} trainings ({pct:.1f}%)\n"
                
                embed.add_field(
                    name="🎓 Training by Discipline",
                    value=disc_str if disc_str else "No data",
                    inline=False
                )
            
            # Buildings by type
            buildings = data.get("buildings", {})
            by_type = buildings.get("by_type_counts", {})
            
            if by_type:
                total_buildings = sum(by_type.values())
                type_str = ""
                for btype, count in sorted(by_type.items(), key=lambda x: x[1], reverse=True):
                    pct = (count / total_buildings * 100) if total_buildings > 0 else 0
                    type_str += f"   • {btype}: {count} ({pct:.0f}%)\n"
                
                embed.add_field(
                    name="🏗️ Buildings by Type",
                    value=type_str if type_str else "No data",
                    inline=False
                )
            
            # Comparison table
            mom = trends.get("mom", {})
            
            comparison = "```\n"
            comparison += f"{'Metric':<15} | {'This Month':<12} | {'Last Month':<12} | {'Change':<10}\n"
            comparison += "-" * 60 + "\n"
            
            # Members
            members_curr = mom.get("members", {}).get("current", 0)
            members_prev = mom.get("members", {}).get("previous", 0)
            members_change = mom.get("members", {}).get("percentage", 0)
            comparison += f"{'Members':<15} | {members_curr:<12} | {members_prev:<12} | {members_change:>+9.1f}%\n"
            
            # Trainings
            train_curr = mom.get("trainings", {}).get("current", 0)
            train_prev = mom.get("trainings", {}).get("previous", 0)
            train_change = mom.get("trainings", {}).get("percentage", 0)
            comparison += f"{'Trainings':<15} | {train_curr:<12} | {train_prev:<12} | {train_change:>+9.1f}%\n"
            
            # Buildings
            build_curr = mom.get("buildings", {}).get("current", 0)
            build_prev = mom.get("buildings", {}).get("previous", 0)
            build_change = mom.get("buildings", {}).get("percentage", 0)
            comparison += f"{'Buildings':<15} | {build_curr:<12} | {build_prev:<12} | {build_change:>+9.1f}%\n"
            
            # Extensions
            ext_curr = mom.get("extensions", {}).get("current", 0)
            ext_prev = mom.get("extensions", {}).get("previous", 0)
            ext_change = mom.get("extensions", {}).get("percentage", 0)
            comparison += f"{'Extensions':<15} | {ext_curr:<12} | {ext_prev:<12} | {ext_change:>+9.1f}%\n"
            
            comparison += "```"
            
            embed.add_field(
                name="📊 Month-over-Month Comparison",
                value=comparison,
                inline=False
            )
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating details embed: {e}")
            return None
    
    async def _create_fun_predictions_embed(
        self,
        data: Dict,
        fun_facts: List[str],
        predictions: Dict,
        month_name: str
    ) -> Optional[discord.Embed]:
        """Create fun facts and predictions embed."""
        try:
            embed = discord.Embed(
                title="🎲 FUN FACTS & PREDICTIONS",
                color=discord.Color.green()
            )
            
            # Fun facts
            if fun_facts:
                facts_str = "\n".join([f"• {fact}" for fact in fun_facts])
                embed.add_field(
                    name="🎉 This Month's Fun Facts",
                    value=facts_str,
                    inline=False
                )
            
            # Predictions
            preds_str = "Based on current trends, we predict:\n\n"
            
            members_pred = predictions.get("members", {})
            preds_str += f"• **Members:** {members_pred.get('predicted', 0)} by month end ({members_pred.get('change', 0):+d})\n"
            
            trainings_pred = predictions.get("trainings", {})
            preds_str += f"• **Trainings:** {trainings_pred.get('predicted', 0)} started\n"
            
            buildings_pred = predictions.get("buildings", {})
            preds_str += f"• **Buildings:** {buildings_pred.get('predicted', 0)} approved\n"
            
            treasury_pred = predictions.get("treasury", {})
            treas_val = treasury_pred.get('predicted', 0)
            preds_str += f"• **Treasury:** {treas_val:,} credits\n\n"
            
            # Challenge
            challenge = self.predictions.generate_challenge(predictions)
            preds_str += f"**Can we beat these predictions? {challenge}** 🚀"
            
            embed.add_field(
                name="🔮 Next Month Forecast",
                value=preds_str,
                inline=False
            )
            
            # Closing message
            embed.add_field(
                name="\u200b",
                value=(
                    f"💬 \"{month_name} was exceptional! Our alliance is thriving\n"
                    f"    thanks to everyone's dedication. Let's keep this\n"
                    f"    momentum going!\" 🚀"
                ),
                inline=False
            )
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating fun/predictions embed: {e}")
            return None
    
    async def post(self, channel: discord.TextChannel) -> bool:
        """Generate and post report to channel."""
        try:
            embeds = await self.generate()
            if not embeds:
                log.error("Failed to generate monthly member report")
                return False
            
            # Post all embeds
            for embed in embeds:
                await channel.send(embed=embed)
            
            log.info(f"Monthly member report posted to {channel.name}")
            return True
            
        except discord.Forbidden:
            log.error(f"No permission to post in {channel.name}")
            return False
        except Exception as e:
            log.exception(f"Error posting monthly member report: {e}")
            return False
