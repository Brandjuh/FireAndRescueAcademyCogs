"""
Fun Facts Generator
Creates interesting and engaging statistics from alliance data
"""

import logging
import random
from datetime import datetime
from typing import Dict, List, Optional
from collections import Counter

log = logging.getLogger("red.FARA.AllianceReports.FunFacts")


class FunFactsGenerator:
    """Generate fun and interesting facts from alliance data."""
    
    def __init__(self):
        """Initialize fun facts generator."""
        self.facts_generated = []
    
    async def generate_facts(
        self,
        monthly_data: Dict,
        count: int = 5
    ) -> List[str]:
        """
        Generate fun facts from monthly data.
        
        Args:
            monthly_data: Complete monthly data dictionary
            count: Number of facts to generate (default: 5)
        
        Returns:
            List of fun fact strings
        """
        self.facts_generated = []
        
        try:
            # Generate all possible facts
            all_facts = []
            
            # Training facts
            all_facts.extend(await self._generate_training_facts(monthly_data))
            
            # Building facts
            all_facts.extend(await self._generate_building_facts(monthly_data))
            
            # Activity facts
            all_facts.extend(await self._generate_activity_facts(monthly_data))
            
            # Treasury facts
            all_facts.extend(await self._generate_treasury_facts(monthly_data))
            
            # Member facts
            all_facts.extend(await self._generate_member_facts(monthly_data))
            
            # Operations facts
            all_facts.extend(await self._generate_operations_facts(monthly_data))
            
            # Admin facts
            all_facts.extend(await self._generate_admin_facts(monthly_data))
            
            # Filter out None values
            all_facts = [f for f in all_facts if f]
            
            # Shuffle and select requested count
            random.shuffle(all_facts)
            selected = all_facts[:count] if len(all_facts) >= count else all_facts
            
            self.facts_generated = selected
            log.info(f"Generated {len(selected)} fun facts")
            
            return selected
        
        except Exception as e:
            log.exception(f"Error generating fun facts: {e}")
            return ["Error generating fun facts"]
    
    async def _generate_training_facts(self, data: Dict) -> List[str]:
        """Generate training-related fun facts."""
        facts = []
        training = data.get("training", {})
        
        try:
            # Most popular training
            by_type = training.get("by_type_counts", {})
            if by_type:
                most_popular = max(by_type.items(), key=lambda x: x[1])
                facts.append(f"Most popular training: {most_popular[0]} ({most_popular[1]} requests)")
            
            # Most popular day for training
            by_day = training.get("by_day_of_week", {})
            if by_day:
                popular_day = max(by_day.items(), key=lambda x: x[1])
                day_name = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][popular_day[0]]
                facts.append(f"Most popular training day: {day_name} ({popular_day[1]} starts)")
            
            # Success rate
            completed = training.get("completed_period", 0)
            started = training.get("started_period", 0)
            if started > 0:
                success_rate = (completed / started * 100)
                if success_rate >= 95:
                    facts.append(f"Impressive {success_rate:.1f}% training completion rate! 🎯")
                elif success_rate >= 90:
                    facts.append(f"Strong {success_rate:.1f}% training completion rate 💪")
            
            # Fastest approval
            fastest = training.get("fastest_approval_minutes", 0)
            if fastest > 0 and fastest < 30:
                facts.append(f"Fastest training approval: {fastest} minutes ⚡")
        
        except Exception as e:
            log.exception(f"Error generating training facts: {e}")
        
        return facts
    
    async def _generate_building_facts(self, data: Dict) -> List[str]:
        """Generate building-related fun facts."""
        facts = []
        buildings = data.get("buildings", {})
        
        try:
            # Most active builder
            top_builder = buildings.get("top_requester_count", 0)
            if top_builder > 5:
                facts.append(f"Most prolific builder: {top_builder} building requests this month! 🏗️")
            
            # Hospital vs Prison ratio
            by_type = buildings.get("by_type_counts", {})
            hospitals = by_type.get("Hospital", 0)
            prisons = by_type.get("Prison", 0)
            if hospitals > 0 and prisons > 0:
                ratio = hospitals / prisons
                if ratio > 1.5:
                    facts.append(f"Hospital boom: {ratio:.1f}x more hospitals than prisons built 🏥")
                elif ratio < 0.7:
                    facts.append(f"Prison expansion: {1/ratio:.1f}x more prisons than hospitals 🔒")
            
            # Extension milestone
            extensions = buildings.get("extensions_started_period", 0)
            if extensions > 150:
                facts.append(f"Massive expansion month: {extensions} extensions started! 📈")
            
            # Approval speed
            avg_hours = buildings.get("avg_review_time_hours", 0)
            if avg_hours > 0 and avg_hours < 1:
                facts.append(f"Lightning-fast building approvals: {int(avg_hours * 60)}min average! ⚡")
        
        except Exception as e:
            log.exception(f"Error generating building facts: {e}")
        
        return facts
    
    async def _generate_activity_facts(self, data: Dict) -> List[str]:
        """Generate activity-related fun facts."""
        facts = []
        
        try:
            # Busiest week
            weekly_data = data.get("weekly_breakdown", [])
            if weekly_data:
                busiest = max(weekly_data, key=lambda w: w.get("total_activity", 0))
                week_num = weekly_data.index(busiest) + 1
                facts.append(f"Week {week_num} was on fire! 🔥 Highest activity of the month")
            
            # Activity streak
            consecutive_days = data.get("consecutive_active_days", 0)
            if consecutive_days >= 30:
                facts.append(f"Perfect month! Active every single day 🌟")
            elif consecutive_days >= 20:
                facts.append(f"Incredible {consecutive_days}-day activity streak! 🎯")
        
        except Exception as e:
            log.exception(f"Error generating activity facts: {e}")
        
        return facts
    
    async def _generate_treasury_facts(self, data: Dict) -> List[str]:
        """Generate treasury-related fun facts."""
        facts = []
        treasury = data.get("treasury", {})
        
        try:
            # Growth milestone
            growth = treasury.get("growth_amount", 0)
            growth_pct = treasury.get("growth_percentage", 0)
            if growth_pct > 15:
                facts.append(f"Treasury boom: +{growth_pct:.1f}% growth this month! 💰")
            
            # Biggest single contribution
            biggest = treasury.get("largest_contribution", 0)
            if biggest > 500000:
                facts.append(f"Biggest contribution: {biggest:,} credits! 🏆")
            
            # Balance milestone
            balance = treasury.get("closing_balance", 0)
            if balance > 12000000 and balance % 1000000 < 500000:
                millions = balance // 1000000
                facts.append(f"Treasury milestone: Over {millions}M credits! 💎")
        
        except Exception as e:
            log.exception(f"Error generating treasury facts: {e}")
        
        return facts
    
    async def _generate_member_facts(self, data: Dict) -> List[str]:
        """Generate member-related fun facts."""
        facts = []
        membership = data.get("membership", {})
        
        try:
            # Growth milestone
            total = membership.get("ending_members", 0)
            if total >= 250 and total < 260:
                facts.append(f"We're almost at 250 members! Currently at {total} 🎉")
            
            # Retention
            retention = membership.get("retention_rate", 0)
            if retention > 95:
                facts.append(f"Outstanding {retention:.1f}% member retention! 🌟")
            
            # New member wave
            new_joins = membership.get("new_joins_period", 0)
            if new_joins > 15:
                facts.append(f"Welcome wave: {new_joins} new members joined this month! 👋")
        
        except Exception as e:
            log.exception(f"Error generating member facts: {e}")
        
        return facts
    
    async def _generate_operations_facts(self, data: Dict) -> List[str]:
        """Generate operations-related fun facts."""
        facts = []
        ops = data.get("operations", {})
        
        try:
            # Mission volume
            missions = ops.get("large_missions_period", 0)
            if missions > 25:
                facts.append(f"Mission masters: {missions} large missions completed! 🎯")
            
            # Event participation
            events = ops.get("alliance_events_period", 0)
            if events > 10:
                facts.append(f"Event champions: {events} alliance events this month! 🏆")
        
        except Exception as e:
            log.exception(f"Error generating operations facts: {e}")
        
        return facts
    
    async def _generate_admin_facts(self, data: Dict) -> List[str]:
        """Generate admin-related fun facts."""
        facts = []
        admin = data.get("admin_activity", {})
        
        try:
            # Most active admin
            top_admin = admin.get("most_active_admin_name", None)
            top_count = admin.get("most_active_admin_count", 0)
            if top_admin and top_count > 100:
                facts.append(f"Admin MVP: {top_count} actions this month! 🌟")
            
            # Total admin actions
            total_actions = admin.get("total_actions_period", 0)
            if total_actions > 300:
                facts.append(f"Admin team crushed it: {total_actions} total actions! 💪")
            
            # Response time
            avg_response = admin.get("avg_response_hours", 0)
            if avg_response > 0 and avg_response < 1:
                facts.append(f"Lightning-fast admin team: {int(avg_response * 60)}min avg response! ⚡")
        
        except Exception as e:
            log.exception(f"Error generating admin facts: {e}")
        
        return facts
