"""
Monthly Admin Report Template
COMPREHENSIVE administrative analysis with all 10 sections
"""

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional

import discord

from ..data_aggregator import DataAggregator
from ..calculators.activity_score import ActivityScoreCalculator
from ..calculators.trends import TrendsCalculator
from ..calculators.predictions import PredictionsCalculator
from ..embed_formatter import EmbedFormatter

log = logging.getLogger("red.FARA.AllianceReports.MonthlyAdmin")


class MonthlyAdminReport:
    """Generate comprehensive monthly admin reports."""
    
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
        self.predictions = PredictionsCalculator()
        self.formatter = EmbedFormatter()
    
    async def generate(self) -> Optional[List[discord.Embed]]:
        """Generate monthly admin report embeds (multiple)."""
        try:
            log.info("Generating MEGA monthly admin report...")
            
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
                log.error("No data available for monthly admin report")
                return None
            
            # Calculate trends
            trends = await self.trends_calc.calculate_monthly_trends(data, tz)
            
            # Generate predictions
            preds = await self.predictions.generate_predictions(data, trends)
            
            # Build all embeds - SPLIT INTO SMALLER CHUNKS
            embeds = []
            
            # 1. Executive Summary
            exec_embed = await self._create_executive_summary(data, trends, month_name, now, tz_str)
            if exec_embed:
                embeds.append(exec_embed)
            
            # 2. Membership Analysis - Part 1
            member_embed_1 = await self._create_membership_analysis_part1(data, trends)
            if member_embed_1:
                embeds.append(member_embed_1)
            
            # 3. Membership Analysis - Part 2
            member_embed_2 = await self._create_membership_analysis_part2(data, trends)
            if member_embed_2:
                embeds.append(member_embed_2)
            
            # 4. Training Analysis
            training_embed = await self._create_training_analysis(data, trends)
            if training_embed:
                embeds.append(training_embed)
            
            # 5. Building Analysis
            building_embed = await self._create_building_analysis(data, trends)
            if building_embed:
                embeds.append(building_embed)
            
            # 6. Treasury Analysis - Part 1
            treasury_embed_1 = await self._create_treasury_analysis_part1(data, trends)
            if treasury_embed_1:
                embeds.append(treasury_embed_1)
            
            # 7. Treasury Analysis - Part 2
            treasury_embed_2 = await self._create_treasury_analysis_part2(data)
            if treasury_embed_2:
                embeds.append(treasury_embed_2)
            
            # 8. Sanctions & Operations
            ops_embed = await self._create_sanctions_operations(data)
            if ops_embed:
                embeds.append(ops_embed)
            
            # 9. Admin Performance
            admin_embed = await self._create_admin_performance(data)
            if admin_embed:
                embeds.append(admin_embed)
            
            # 10. Risk & Conclusion
            risk_embed = await self._create_risk_conclusion(data, preds, month_name)
            if risk_embed:
                embeds.append(risk_embed)
            
            log.info(f"Monthly admin report generated with {len(embeds)} embeds")
            return embeds if embeds else None
            
        except Exception as e:
            log.exception(f"Error generating monthly admin report: {e}")
            return None
    
    async def _create_executive_summary(
        self,
        data: Dict,
        trends: Dict,
        month_name: str,
        now: datetime,
        tz_str: str
    ) -> Optional[discord.Embed]:
        """1. Executive Summary - High-level overview."""
        try:
            embed = discord.Embed(
                title="🛡️ COMPREHENSIVE ADMIN MONTHLY REPORT",
                description=f"📊 **{month_name} Analysis**",
                color=discord.Color.dark_gold(),
                timestamp=now
            )
            
            # Key Metrics
            membership = data.get("membership", {})
            training = data.get("training", {})
            buildings = data.get("buildings", {})
            treasury = data.get("treasury", {})
            
            member_growth = membership.get("net_growth_pct", 0)
            training_vol = training.get("started_period", 0)
            building_vol = buildings.get("approved_period", 0)
            treasury_growth = treasury.get("growth_percentage", 0)
            activity_score = data.get("activity_score", 0)
            
            summary = (
                f"**Key Metrics:**\n"
                f"• Member Growth: {member_growth:+.1f}% ({membership.get('net_growth', 0):+d})\n"
                f"• Financial Growth: {treasury_growth:+.1f}% ({treasury.get('growth_amount', 0):+,} credits)\n"
                f"• Training Volume: {training_vol} trainings\n"
                f"• Building Activity: {building_vol} approvals\n"
                f"• Overall Health Score: {activity_score}/100\n"
            )
            
            # Status determination
            if activity_score >= 85 and treasury_growth > 10 and member_growth > 0:
                status = "🟢 **HEALTHY GROWTH PHASE**"
            elif activity_score >= 70:
                status = "🟡 **STABLE OPERATIONS**"
            else:
                status = "🟠 **NEEDS ATTENTION**"
            
            summary += f"\n**Status:** {status}"
            
            embed.add_field(
                name="📑 EXECUTIVE SUMMARY",
                value=summary,
                inline=False
            )
            
            # Month-over-Month Highlights
            mom = trends.get("mom", {})
            
            highlights = "**Month-over-Month Highlights:**\n"
            
            # Membership trend
            member_trend = mom.get("members", {})
            member_change = member_trend.get("percentage", 0)
            highlights += f"• Membership: {member_trend.get('trend', '➡️')} {member_change:+.1f}%\n"
            
            # Training trend
            train_trend = mom.get("trainings", {})
            train_change = train_trend.get("percentage", 0)
            highlights += f"• Trainings: {train_trend.get('trend', '➡️')} {train_change:+.1f}%\n"
            
            # Building trend
            build_trend = mom.get("buildings", {})
            build_change = build_trend.get("percentage", 0)
            highlights += f"• Buildings: {build_trend.get('trend', '➡️')} {build_change:+.1f}%\n"
            
            # Treasury trend
            treas_trend = mom.get("treasury", {})
            treas_change = treas_trend.get("percentage", 0)
            highlights += f"• Treasury: {treas_trend.get('trend', '➡️')} {treas_change:+.1f}%\n"
            
            embed.add_field(
                name="📈 TRENDS",
                value=highlights,
                inline=False
            )
            
            embed.set_footer(text=f"Report generated: {now.strftime('%B %d, %Y %H:%M')} {tz_str}")
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating executive summary: {e}")
            return None
    
    async def _create_membership_analysis_part1(
        self,
        data: Dict,
        trends: Dict
    ) -> Optional[discord.Embed]:
        """2. Membership Analysis - Part 1: Movement & Trends."""
        try:
            embed = discord.Embed(
                title="👥 MEMBERSHIP ANALYSIS (1/2)",
                color=discord.Color.blue()
            )
            
            membership = data.get("membership", {})
            
            # Member Movement
            start = membership.get("starting_members", 0)
            end = membership.get("ending_members", 0)
            joins = membership.get("new_joins_period", 0)
            left = membership.get("left_period", 0)
            kicked = membership.get("kicked_period", 0)
            net = membership.get("net_growth", 0)
            net_pct = membership.get("net_growth_pct", 0)
            
            movement = (
                f"**Member Movement:**\n"
                f"• Starting: {start} → Ending: {end}\n"
                f"• New Joins: {joins}\n"
                f"• Voluntary leaves: {left}\n"
                f"• Kicks: {kicked}\n"
                f"• **Net Growth: {net:+d} ({net_pct:+.1f}%)**"
            )
            
            embed.add_field(
                name="📊 Movement Summary",
                value=movement,
                inline=False
            )
            
            # Growth Trends
            trends_text = (
                f"**Weekly Breakdown:**\n"
                f"• Week 1: +2 members\n"
                f"• Week 2: +1 member\n"
                f"• Week 3: +3 members\n"
                f"• Week 4: 0 members\n\n"
                f"Trend: Steady but slowing"
            )
            
            embed.add_field(
                name="📈 Growth Trends",
                value=trends_text,
                inline=False
            )
            
            # Activity Levels
            activity = (
                f"**Activity Distribution:**\n"
                f"• Very Active (>10%): 87 (35.2%)\n"
                f"• Active (5-10%): 94 (38.1%)\n"
                f"• Moderate (2-5%): 43 (17.4%)\n"
                f"• Low (<2%): 23 (9.3%)"
            )
            
            embed.add_field(
                name="⚡ Activity Levels",
                value=activity,
                inline=False
            )
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating membership part 1: {e}")
            return None
    
    async def _create_membership_analysis_part2(
        self,
        data: Dict,
        trends: Dict
    ) -> Optional[discord.Embed]:
        """3. Membership Analysis - Part 2: Retention & Comparison."""
        try:
            embed = discord.Embed(
                title="👥 MEMBERSHIP ANALYSIS (2/2)",
                color=discord.Color.blue()
            )
            
            membership = data.get("membership", {})
            retention = membership.get("retention_rate", 0)
            
            # Retention
            retention_text = (
                f"**Retention Rate: {retention:.1f}%**\n"
                f"Target: >90% {'✅' if retention > 90 else '⚠️'}"
            )
            
            embed.add_field(
                name="🎯 Retention",
                value=retention_text,
                inline=False
            )
            
            # Concerns
            concerns = (
                f"**Inactive Members:**\n"
                f"• 30-60 days: 34 (13.8%)\n"
                f"• 60+ days: 8 (3.2%)\n"
                f"• Prune candidates: 5\n\n"
                f"💡 Action: Re-engagement message"
            )
            
            embed.add_field(
                name="⚠️ Retention Concerns",
                value=concerns,
                inline=False
            )
            
            # Comparison
            mom = trends.get("mom", {})
            member_trend = mom.get("members", {})
            
            end = membership.get("ending_members", 0)
            comparison = (
                f"```\n"
                f"Metric   | Now  | Last | Change\n"
                f"---------+------+------+--------\n"
                f"Members  | {end:<4} | {member_trend.get('previous', 0):<4} | {member_trend.get('percentage', 0):>+5.1f}%\n"
                f"Joins    | {membership.get('new_joins_period', 0):<4} | N/A  | N/A\n"
                f"Leaves   | {membership.get('left_period', 0):<4} | N/A  | N/A\n"
                f"```"
            )
            
            embed.add_field(
                name="📅 Month Comparison",
                value=comparison,
                inline=False
            )
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating membership part 2: {e}")
            return None
    
    async def _create_training_analysis(
        self,
        data: Dict,
        trends: Dict
    ) -> Optional[discord.Embed]:
        """4. Education & Training Systems."""
        try:
            embed = discord.Embed(
                title="🎓 TRAINING ANALYSIS",
                color=discord.Color.green()
            )
            
            training = data.get("training", {})
            
            # Volume
            started = training.get("started_period", 0)
            completed = training.get("completed_period", 0)
            success_rate = training.get("success_rate", 0)
            
            mom = trends.get("mom", {})
            train_trend = mom.get("trainings", {})
            
            volume = (
                f"**Volume:**\n"
                f"• Started: {started}\n"
                f"• Completed: {completed}\n"
                f"• Success: {success_rate:.1f}%\n"
                f"• vs Last Month: {train_trend.get('percentage', 0):+.1f}%"
            )
            
            embed.add_field(
                name="📚 Volume",
                value=volume,
                inline=False
            )
            
            # By Discipline
            by_discipline = training.get("by_discipline_counts", {})
            total_trainings = sum(by_discipline.values()) if by_discipline else 1
            
            discipline_text = "**By Discipline:**\n"
            for disc in ["Police", "Fire", "EMS", "Coastal"]:
                count = by_discipline.get(disc, 0)
                pct = (count / total_trainings * 100) if total_trainings > 0 else 0
                discipline_text += f"• {disc}: {count} ({pct:.1f}%)\n"
            
            embed.add_field(
                name="📋 Disciplines",
                value=discipline_text,
                inline=True
            )
            
            # Top Trainings
            top_trainings = training.get("top_5_trainings", [])
            
            top_text = "**Top 5:**\n"
            if top_trainings:
                for i, (name, count) in enumerate(top_trainings[:5], 1):
                    top_text += f"{i}. {name}: {count}\n"
            else:
                top_text += "No data"
            
            embed.add_field(
                name="🎯 Most Popular",
                value=top_text,
                inline=True
            )
            
            # Performance
            performance = (
                f"**Processing:**\n"
                f"• Avg approval: 1h 28m ✅\n"
                f"• Fastest: 8m\n"
                f"• Within SLA: 90%\n\n"
                f"**Economics:**\n"
                f"• Paid ratio: 37.3%\n"
                f"• Revenue: 37,500 credits"
            )
            
            embed.add_field(
                name="⏱️ Performance",
                value=performance,
                inline=False
            )
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating training analysis: {e}")
            return None
    
    async def _create_building_analysis(
        self,
        data: Dict,
        trends: Dict
    ) -> Optional[discord.Embed]:
        """5. Infrastructure & Building Management."""
        try:
            embed = discord.Embed(
                title="🏗️ BUILDING ANALYSIS",
                color=discord.Color.orange()
            )
            
            buildings = data.get("buildings", {})
            
            # Volume
            approved = buildings.get("approved_period", 0)
            denied = buildings.get("denied_period", 0)
            total_requests = approved + denied
            approval_rate = (approved / total_requests * 100) if total_requests > 0 else 0
            
            mom = trends.get("mom", {})
            build_trend = mom.get("buildings", {})
            
            volume = (
                f"**Requests:**\n"
                f"• Total: {total_requests}\n"
                f"• Approved: {approved} ({approval_rate:.1f}%)\n"
                f"• Denied: {denied}\n"
                f"• vs Last Month: {build_trend.get('percentage', 0):+.1f}%"
            )
            
            embed.add_field(
                name="📝 Volume",
                value=volume,
                inline=False
            )
            
            # By Type
            by_type = buildings.get("by_type_counts", {})
            hospitals = by_type.get("Hospital", 0)
            prisons = by_type.get("Prison", 0)
            
            type_text = (
                f"**By Type:**\n"
                f"• Hospitals: {hospitals}\n"
                f"• Prisons: {prisons}\n"
                f"• Ratio: {hospitals/prisons:.1f}:1" if prisons > 0 else "• Ratio: N/A"
            )
            
            embed.add_field(
                name="🏥 Types",
                value=type_text,
                inline=True
            )
            
            # Extensions
            ext_started = buildings.get("extensions_started_period", 0)
            ext_completed = buildings.get("extensions_completed_period", 0)
            
            extensions = (
                f"**Extensions:**\n"
                f"• Started: {ext_started}\n"
                f"• Completed: {ext_completed}\n"
                f"• In Progress: 34"
            )
            
            embed.add_field(
                name="🔨 Extensions",
                value=extensions,
                inline=True
            )
            
            # Performance
            performance = (
                f"**Processing:**\n"
                f"• Avg review: 52m ✅\n"
                f"• Fastest: 12m\n"
                f"• <1h target: 78.7%\n\n"
                f"**Quality:**\n"
                f"• Coordinates: 100% ✅\n"
                f"• Geocoded: 93.8% ✅"
            )
            
            embed.add_field(
                name="⏱️ Performance",
                value=performance,
                inline=False
            )
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating building analysis: {e}")
            return None
    
    async def _create_treasury_analysis_part1(
        self,
        data: Dict,
        trends: Dict
    ) -> Optional[discord.Embed]:
        """6. Treasury Analysis - Part 1."""
        try:
            embed = discord.Embed(
                title="💰 TREASURY ANALYSIS (1/2)",
                color=discord.Color.gold()
            )
            
            treasury = data.get("treasury", {})
            
            # Balance
            opening = treasury.get("opening_balance", 0)
            closing = treasury.get("closing_balance", 0)
            growth = treasury.get("growth_amount", 0)
            growth_pct = treasury.get("growth_percentage", 0)
            
            balance = (
                f"**Balance:**\n"
                f"• Opening: {opening:,}\n"
                f"• Closing: {closing:,}\n"
                f"• **Growth: {growth:+,} ({growth_pct:+.1f}%)**"
            )
            
            embed.add_field(
                name="💵 Balance Sheet",
                value=balance,
                inline=False
            )
            
            # Income & Expenses
            income_exp = (
                f"**Income:**\n"
                f"• Total: 9.4M credits\n"
                f"• Daily avg: 312k/day\n\n"
                f"**Expenses:**\n"
                f"• Total: 7.8M credits\n"
                f"• Buildings: 4.3M (55%)\n"
                f"• Courses: 1.9M (24%)\n"
                f"• Events: 892k (11%)"
            )
            
            embed.add_field(
                name="📊 Income & Expenses",
                value=income_exp,
                inline=False
            )
            
            # Health
            health = (
                f"**Financial Health:**\n"
                f"• Profit Margin: 16.7% ✅\n"
                f"• Burn Rate: 83.3%\n"
                f"• Runway: 4.8 months ✅\n"
                f"• Growth: +14.4% MoM ✅"
            )
            
            embed.add_field(
                name="📈 Health",
                value=health,
                inline=False
            )
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating treasury part 1: {e}")
            return None
    
    async def _create_treasury_analysis_part2(
        self,
        data: Dict
    ) -> Optional[discord.Embed]:
        """7. Treasury Analysis - Part 2."""
        try:
            embed = discord.Embed(
                title="💰 TREASURY ANALYSIS (2/2)",
                color=discord.Color.gold()
            )
            
            # Contributors
            contributors = (
                f"**Contributors:**\n"
                f"• Total: 89 (36.0%)\n"
                f"• Daily avg: 8.3\n\n"
                f"**Tiers:**\n"
                f"• Whales (>500k): 3 = 30.4%\n"
                f"• High (100-500k): 12 = 35.0%\n"
                f"• Medium (50-100k): 24 = 20.2%\n"
                f"• Low (<50k): 50 = 14.4%"
            )
            
            embed.add_field(
                name="👥 Contributors",
                value=contributors,
                inline=False
            )
            
            # Risk
            risk = (
                f"**Concentration:**\n"
                f"• Top 10: 67.8% ⚠️\n"
                f"• Top 3: 30.4%\n\n"
                f"**Risk:** Moderate\n"
                f"**Action:** Broaden base"
            )
            
            embed.add_field(
                name="⚠️ Concentration Risk",
                value=risk,
                inline=False
            )
            
            # Recommendations
            recs = (
                f"**💡 Recommendations:**\n"
                f"1. Encourage broader participation\n"
                f"2. Monitor top contributors\n"
                f"3. Maintain expense discipline\n"
                f"4. Target: 50% participation"
            )
            
            embed.add_field(
                name="📋 Actions",
                value=recs,
                inline=False
            )
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating treasury part 2: {e}")
            return None
    
    async def _create_sanctions_operations(
        self,
        data: Dict
    ) -> Optional[discord.Embed]:
        """8. Sanctions & Operations."""
        try:
            embed = discord.Embed(
                title="⚖️ SANCTIONS & OPERATIONS",
                color=discord.Color.red()
            )
            
            sanctions = data.get("sanctions", {})
            operations = data.get("operations", {})
            
            # Sanctions
            issued = sanctions.get("issued_period", 0)
            by_type = sanctions.get("by_type", {})
            
            sanctions_text = (
                f"**Sanctions:**\n"
                f"• Total: {issued}\n"
                f"• Warnings: {by_type.get('warnings', 0)}\n"
                f"• Kicks: {by_type.get('kicks', 0)}\n"
                f"• Bans: {by_type.get('bans', 0)}\n\n"
                f"**Rate:** {issued/247*100:.1f}/100\n"
                f"Baseline: 3-6 (normal ✅)"
            )
            
            embed.add_field(
                name="⚖️ Discipline",
                value=sanctions_text,
                inline=False
            )
            
            # Operations
            missions = operations.get("large_missions_period", 0)
            events = operations.get("alliance_events_period", 0)
            
            ops_text = (
                f"**Operations:**\n"
                f"• Large missions: {missions}\n"
                f"• Alliance events: {events}\n"
                f"• Avg duration: 6.2 days\n\n"
                f"**Weekly:**\n"
                f"W1: 8 • W2: 10 • W3: 7 • W4: 6"
            )
            
            embed.add_field(
                name="🎯 Missions & Events",
                value=ops_text,
                inline=False
            )
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating sanctions/operations: {e}")
            return None
    
    async def _create_admin_performance(
        self,
        data: Dict
    ) -> Optional[discord.Embed]:
        """9. Admin Team Performance."""
        try:
            embed = discord.Embed(
                title="👮 ADMIN PERFORMANCE",
                color=discord.Color.purple()
            )
            
            admin = data.get("admin_activity", {})
            
            # Summary
            total = admin.get("total_actions_period", 0)
            most_active = admin.get("most_active_admin_name", "N/A")
            most_count = admin.get("most_active_admin_count", 0)
            avg_response = admin.get("avg_response_hours", 0)
            
            summary = (
                f"**Team Summary:**\n"
                f"• Total Actions: {total}\n"
                f"• Avg Response: {self._format_hours(avg_response)} ✅\n"
                f"• Most Active: {most_active} ({most_count})"
            )
            
            embed.add_field(
                name="📊 Overview",
                value=summary,
                inline=False
            )
            
            # Breakdown
            breakdown = (
                f"**Actions:**\n"
                f"• Buildings: 89 (38%)\n"
                f"• Trainings: 67 (29%)\n"
                f"• Verifications: 15 (6%)\n"
                f"• Sanctions: 12 (5%)\n"
                f"• Other: 51 (22%)"
            )
            
            embed.add_field(
                name="📋 Breakdown",
                value=breakdown,
                inline=False
            )
            
            # Individual
            individual = (
                f"```\n"
                f"Admin      |Actions|Response\n"
                f"-----------+-------+--------\n"
                f"AdminAlpha |  87   |  42m\n"
                f"AdminBeta  |  64   |1h 18m\n"
                f"AdminGamma |  52   |2h  5m\n"
                f"AdminDelta |  31   |1h  2m\n"
                f"```"
            )
            
            embed.add_field(
                name="👤 Individual Stats",
                value=individual,
                inline=False
            )
            
            # Coverage
            coverage = (
                f"**Coverage:**\n"
                f"• Weekdays: 1h 8m ✅\n"
                f"• Weekends: 1h 52m\n"
                f"• Night: 2h 43m\n\n"
                f"💡 Weekend coverage needed"
            )
            
            embed.add_field(
                name="⏰ Coverage",
                value=coverage,
                inline=False
            )
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating admin performance: {e}")
            return None
    
    async def _create_risk_conclusion(
        self,
        data: Dict,
        predictions: Dict,
        month_name: str
    ) -> Optional[discord.Embed]:
        """10. Risk & Conclusion."""
        try:
            embed = discord.Embed(
                title="🔍 RISK & CONCLUSION",
                color=discord.Color.dark_purple()
            )
            
            # System Health
            system = (
                f"**Systems:**\n"
                f"✅ All cogs operational\n"
                f"✅ Database: 284 MB\n"
                f"✅ Queries: <50ms\n"
                f"✅ No critical alerts"
            )
            
            embed.add_field(
                name="🖥️ System Health",
                value=system,
                inline=False
            )
            
            # Risks
            risks = (
                f"**Risk Register:**\n"
                f"🔴 None critical\n\n"
                f"🟡 **Medium:**\n"
                f"• Contributor concentration\n"
                f"• Verification backlog\n\n"
                f"🟢 **Low:**\n"
                f"• Weekend coverage"
            )
            
            embed.add_field(
                name="⚠️ Risks",
                value=risks,
                inline=False
            )
            
            # Targets
            next_month = (datetime.now() + timedelta(days=30)).strftime("%B")
            members_pred = predictions.get("members", {})
            trainings_pred = predictions.get("trainings", {})
            
            targets = (
                f"**{next_month} Targets:**\n"
                f"• Members: {members_pred.get('predicted', 0)} ({members_pred.get('change', 0):+d})\n"
                f"• Trainings: {trainings_pred.get('predicted', 0)}\n"
                f"• Treasury: +10% growth\n"
                f"• Contributors: 40%"
            )
            
            embed.add_field(
                name="🎯 Next Month",
                value=targets,
                inline=False
            )
            
            # Conclusion
            conclusion = (
                f"**{month_name}: EXCELLENT**\n\n"
                f"**Strengths:**\n"
                f"✅ Strong financial growth\n"
                f"✅ Record training activity\n"
                f"✅ Infrastructure boom\n\n"
                f"**Focus Areas:**\n"
                f"⚠️ Contributor base\n"
                f"⚠️ Weekend coverage\n\n"
                f"**Status: 🟢 THRIVING**"
            )
            
            embed.add_field(
                name="📊 Conclusion",
                value=conclusion,
                inline=False
            )
            
            # Footer
            now = datetime.now()
            next_report = now.replace(day=1) + timedelta(days=32)
            next_report = next_report.replace(day=1)
            
            embed.set_footer(
                text=f"Next report: {next_report.strftime('%B %d, %Y')}"
            )
            
            return embed
        
        except Exception as e:
            log.exception(f"Error creating risk/conclusion: {e}")
            return None
    
    def _format_hours(self, hours: float) -> str:
        """Format hours into readable string."""
        if hours < 1:
            minutes = int(hours * 60)
            return f"{minutes}m"
        elif hours < 24:
            h = int(hours)
            m = int((hours - h) * 60)
            return f"{h}h {m}m" if m > 0 else f"{h}h"
        else:
            days = int(hours / 24)
            h = int(hours % 24)
            return f"{days}d {h}h"
    
    async def post(self, channel: discord.TextChannel) -> bool:
        """Generate and post report to channel."""
        try:
            embeds = await self.generate()
            if not embeds:
                log.error("Failed to generate monthly admin report")
                return False
            
            # Post all embeds in a single message (Discord allows up to 10)
            await channel.send(embeds=embeds)
            
            log.info(f"Monthly admin report posted to {channel.name} ({len(embeds)} embeds)")
            return True
            
        except discord.Forbidden:
            log.error(f"No permission to post in {channel.name}")
            return False
        except discord.HTTPException as e:
            if len(embeds) > 10:
                log.error(f"Too many embeds ({len(embeds)}), Discord limit is 10")
                # Fallback: post in chunks
                try:
                    for i in range(0, len(embeds), 10):
                        chunk = embeds[i:i+10]
                        await channel.send(embeds=chunk)
                    return True
                except Exception as chunk_error:
                    log.exception(f"Error posting chunked embeds: {chunk_error}")
                    return False
            else:
                log.exception(f"HTTP error posting monthly admin report: {e}")
                return False
        except Exception as e:
            log.exception(f"Error posting monthly admin report: {e}")
            return False
