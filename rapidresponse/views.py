"""
Discord UI views for mission interactions
"""
import discord
import logging
from typing import Optional, Dict, Any
from . import config

log = logging.getLogger("red.rapidresponse.views")


class MissionView(discord.ui.View):
    """View for mission response buttons"""
    
    def __init__(
        self,
        cog,
        mission_instance_id: int,
        user_id: int,
        timeout: int = 180
    ):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.mission_instance_id = mission_instance_id
        self.user_id = user_id
        self.response_given = False
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only allow the mission owner to interact"""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "This is not your mission!", ephemeral=True
            )
            return False
        return True
    
    async def on_timeout(self):
        """Handle view timeout"""
        if not self.response_given:
            # Mission timed out - handle in background
            log.info(f"Mission {self.mission_instance_id} timed out")
    
    @discord.ui.button(
        label="Minimal Response",
        style=discord.ButtonStyle.secondary,
        custom_id="response_minimal",
        emoji="🚗"
    )
    async def minimal_response(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        """Handle minimal response"""
        await self._handle_response(interaction, "minimal")
    
    @discord.ui.button(
        label="Standard Response",
        style=discord.ButtonStyle.primary,
        custom_id="response_standard",
        emoji="🚒"
    )
    async def standard_response(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        """Handle standard response"""
        await self._handle_response(interaction, "standard")
    
    @discord.ui.button(
        label="Full Response",
        style=discord.ButtonStyle.success,
        custom_id="response_full",
        emoji="🚁"
    )
    async def full_response(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        """Handle full response"""
        await self._handle_response(interaction, "full")
    
    @discord.ui.button(
        label="Overwhelming Force",
        style=discord.ButtonStyle.danger,
        custom_id="response_overwhelming",
        emoji="⚡"
    )
    async def overwhelming_response(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        """Handle overwhelming response"""
        await self._handle_response(interaction, "overwhelming")
    
    async def _handle_response(self, interaction: discord.Interaction, response_type: str):
        """Process mission response"""
        if self.response_given:
            await interaction.response.send_message(
                "You've already responded to this mission!", ephemeral=True
            )
            return
        
        self.response_given = True
        
        # Defer response as this might take a moment
        await interaction.response.defer()
        
        # Disable all buttons
        for item in self.children:
            item.disabled = True
        
        # Update original message
        try:
            await interaction.message.edit(view=self)
        except:
            pass
        
        # Process the response
        await self.cog.process_mission_response(
            interaction,
            self.mission_instance_id,
            response_type
        )
        
        # Stop the view
        self.stop()


class TrainView(discord.ui.View):
    """View for training stat selection"""
    
    def __init__(self, cog, user_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.user_id = user_id
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only allow the command user to interact"""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "This is not your training menu!", ephemeral=True
            )
            return False
        return True
    
    @discord.ui.button(
        label="Response",
        style=discord.ButtonStyle.primary,
        custom_id="train_response",
        emoji="⚡"
    )
    async def train_response(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await self._handle_training(interaction, "response")
    
    @discord.ui.button(
        label="Tactics",
        style=discord.ButtonStyle.primary,
        custom_id="train_tactics",
        emoji="🎯"
    )
    async def train_tactics(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await self._handle_training(interaction, "tactics")
    
    @discord.ui.button(
        label="Logistics",
        style=discord.ButtonStyle.primary,
        custom_id="train_logistics",
        emoji="📦"
    )
    async def train_logistics(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await self._handle_training(interaction, "logistics")
    
    @discord.ui.button(
        label="Medical",
        style=discord.ButtonStyle.primary,
        custom_id="train_medical",
        emoji="🏥"
    )
    async def train_medical(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await self._handle_training(interaction, "medical")
    
    @discord.ui.button(
        label="Command",
        style=discord.ButtonStyle.primary,
        custom_id="train_command",
        emoji="⭐"
    )
    async def train_command(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ):
        await self._handle_training(interaction, "command")
    
    async def _handle_training(self, interaction: discord.Interaction, stat_type: str):
        """Process training request"""
        await interaction.response.defer()
        
        # Disable all buttons
        for item in self.children:
            item.disabled = True
        
        try:
            await interaction.message.edit(view=self)
        except:
            pass
        
        # Start training
        await self.cog.start_training(interaction, stat_type)
        
        self.stop()


class ConfirmView(discord.ui.View):
    """Simple confirmation view"""
    
    def __init__(self, user_id: int, timeout: int = 30):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.value = None
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "This is not your confirmation!", ephemeral=True
            )
            return False
        return True
    
    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success, emoji="✅")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = True
        await interaction.response.defer()
        self.stop()
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, emoji="❌")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.value = False
        await interaction.response.defer()
        self.stop()


def create_mission_embed(
    mission_data: Dict[str, Any],
    mission_name: str,
    tier: int,
    difficulty: int,
    description: str,
    requirements: str,
    timeout_seconds: int,
    stage: int = 1,
    max_stage: int = 1
) -> discord.Embed:
    """Create an embed for a mission"""
    
    tier_info = config.MISSION_TIERS[tier]
    tier_name = tier_info['name']
    
    # Color based on tier
    colors = {
        1: config.COLOR_INFO,
        2: 0x3498DB,
        3: config.COLOR_WARNING,
        4: config.COLOR_FAILURE
    }
    color = colors.get(tier, config.COLOR_INFO)
    
    embed = discord.Embed(
        title=f"🚨 {mission_name}",
        description=description,
        color=color
    )
    
    # Mission details
    embed.add_field(
        name="📊 Mission Details",
        value=(
            f"**Tier:** {tier_name} (Tier {tier})\n"
            f"**Difficulty:** {difficulty}/100\n"
            f"**Stage:** {stage}/{max_stage}"
        ),
        inline=True
    )
    
    # Credits estimate
    avg_credits = mission_data.get('average_credits', 500)
    embed.add_field(
        name="💰 Estimated Rewards",
        value=(
            f"**Credits:** ~{avg_credits:,}\n"
            f"**XP:** ~{int(avg_credits * tier_info['xp_mult']):,}"
        ),
        inline=True
    )
    
    # Requirements
    embed.add_field(
        name="📋 Requirements",
        value=requirements,
        inline=False
    )
    
    # Response options
    embed.add_field(
        name="🎯 Choose Your Response",
        value=(
            "**Minimal:** Lower cost, lower success chance (-15%)\n"
            "**Standard:** Balanced approach (base chance)\n"
            "**Full:** Higher cost, better success (+10%)\n"
            "**Overwhelming:** Highest cost, best success (+20%)"
        ),
        inline=False
    )
    
    # Timeout warning
    minutes = timeout_seconds // 60
    seconds = timeout_seconds % 60
    timeout_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
    embed.set_footer(text=f"⏱️ Time to respond: {timeout_str}")
    
    return embed


def create_outcome_embed(
    mission_name: str,
    outcome: str,
    result: Dict[str, Any]
) -> discord.Embed:
    """Create an embed for mission outcome"""
    
    # Determine color and title based on outcome
    if outcome == config.OUTCOME_FULL_SUCCESS:
        color = config.COLOR_SUCCESS
        title = "✅ Mission Success!"
    elif outcome == config.OUTCOME_PARTIAL_SUCCESS:
        color = config.COLOR_PARTIAL
        title = "⚠️ Partial Success"
    elif outcome == config.OUTCOME_FAILURE:
        color = config.COLOR_FAILURE
        title = "❌ Mission Failed"
    elif outcome == config.OUTCOME_ESCALATION:
        color = config.COLOR_WARNING
        title = "🚨 Mission Escalating!"
    else:
        color = config.COLOR_INFO
        title = "📋 Mission Complete"
    
    embed = discord.Embed(
        title=f"{title} - {mission_name}",
        description=result.get('description', 'Mission completed'),
        color=color
    )
    
    if not result.get('escalated', False):
        # Show rewards
        credits = result.get('credits', 0)
        xp = result.get('xp', 0)
        morale_change = result.get('morale_change', 0)
        
        morale_emoji = "📈" if morale_change >= 0 else "📉"
        
        embed.add_field(
            name="💰 Rewards & Changes",
            value=(
                f"**Credits:** {credits:,} {'💵' if credits > 0 else ''}\n"
                f"**XP:** {xp:,} {'⭐' if xp > 0 else ''}\n"
                f"**Morale:** {morale_change:+d} {morale_emoji}"
            ),
            inline=True
        )
        
        # Success chance info
        success_chance = result.get('success_chance', 0)
        embed.add_field(
            name="📊 Statistics",
            value=f"**Success Chance:** {success_chance:.1f}%",
            inline=True
        )
        
        # Level up notification
        level_info = result.get('level_info', {})
        if level_info.get('leveled_up', False):
            embed.add_field(
                name="🎉 Level Up!",
                value=(
                    f"**Level {level_info['old_level']} → {level_info['new_level']}**\n"
                    f"All stats increased!"
                ),
                inline=False
            )
    else:
        # Escalation message
        next_stage = result.get('next_stage', 2)
        max_stage = result.get('max_stage', 3)
        
        embed.add_field(
            name="⚠️ Situation Update",
            value=(
                f"The incident requires additional response!\n"
                f"**Moving to Stage {next_stage}/{max_stage}**\n\n"
                f"Prepare for the next phase..."
            ),
            inline=False
        )
        
        # Partial rewards
        credits = result.get('credits', 0)
        xp = result.get('xp', 0)
        embed.add_field(
            name="💰 Partial Rewards",
            value=f"**Credits:** {credits:,}\n**XP:** {xp:,}",
            inline=False
        )
    
    return embed


def create_profile_embed(player: Dict[str, Any], user: discord.User, credits: int = 0) -> discord.Embed:
    """Create profile embed"""
    
    embed = discord.Embed(
        title=f"🏢 {user.display_name}'s Station",
        color=config.COLOR_INFO
    )
    
    embed.set_thumbnail(url=user.display_avatar.url)
    
    # Basic stats
    level = player['station_level']
    xp = player['xp']
    xp_next = (level + 1) * config.XP_PER_LEVEL
    xp_current_level = level * config.XP_PER_LEVEL
    xp_progress = xp - xp_current_level
    xp_needed = xp_next - xp_current_level
    
    status = "🟢 Active" if player['is_active'] else "🔴 Inactive"
    
    embed.add_field(
        name="📊 Station Info",
        value=(
            f"**Level:** {level}\n"
            f"**Status:** {status}\n"
            f"**XP:** {xp_progress:,}/{xp_needed:,}\n"
            f"**Credits:** {credits:,} 💵"
        ),
        inline=True
    )
    
    # Morale
    morale = player['morale']
    morale_bar = "█" * (morale // 10) + "░" * (10 - morale // 10)
    embed.add_field(
        name="💪 Morale",
        value=f"{morale_bar} {morale}/100",
        inline=True
    )
    
    # Stats
    embed.add_field(
        name="⚡ Stats",
        value=(
            f"**Response:** {player['stat_response']}\n"
            f"**Tactics:** {player['stat_tactics']}\n"
            f"**Logistics:** {player['stat_logistics']}\n"
            f"**Medical:** {player['stat_medical']}\n"
            f"**Command:** {player['stat_command']}"
        ),
        inline=True
    )
    
    # Mission stats
    total = player['total_missions']
    success = player['successful_missions']
    failed = player['failed_missions']
    success_rate = (success / total * 100) if total > 0 else 0
    
    embed.add_field(
        name="🎯 Mission Record",
        value=(
            f"**Total:** {total}\n"
            f"**Successful:** {success}\n"
            f"**Failed:** {failed}\n"
            f"**Success Rate:** {success_rate:.1f}%\n"
            f"**Current Streak:** {player['mission_streak']} 🔥"
        ),
        inline=True
    )
    
    return embed
