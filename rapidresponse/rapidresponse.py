"""
Rapid Response Dispatch - A MissionChief-inspired emergency response game
"""
import discord
from redbot.core import commands, bank, Config
from redbot.core.bot import Red
import aiosqlite
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
import json

from . import config
from .models import Database
from .mission_manager import MissionManager
from .game_logic import GameLogic
from .scheduler import MissionScheduler
from .views import (
    MissionView, TrainView, ConfirmView,
    create_outcome_embed, create_profile_embed
)

log = logging.getLogger("red.rapidresponse")


class RapidResponse(commands.Cog):
    """
    A MissionChief-inspired emergency response simulation game.
    
    Manage your own emergency station, respond to real MissionChief missions,
    level up your stats, and climb the leaderboards!
    """
    
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890123456789)
        
        default_global = {
            "mission_channel_id": None,
        }
        self.config.register_global(**default_global)
        
        # Initialize database
        data_path = Path(__file__).parent
        db_path = data_path / config.DB_FILE
        self.db = Database(db_path)
        
        # Initialize managers
        self.mission_manager = MissionManager(self.db)
        self.game_logic = GameLogic(self.db, self.mission_manager)
        self.scheduler = MissionScheduler(self)
        
        # Start background tasks
        self.bot.loop.create_task(self._startup())
    
    async def _startup(self):
        """Startup tasks"""
        try:
            # Initialize database
            await self.db.initialize()
            log.info("Database initialized")
            
            # Load mission data
            await self.mission_manager.load_missions()
            log.info(f"Loaded {len(self.mission_manager.missions)} missions")
            
            # Sync mission channel config
            channel_id = await self.config.mission_channel_id()
            if channel_id:
                await self.db.set_config('mission_channel_id', str(channel_id))
            
            # Start scheduler
            self.scheduler.start()
            log.info("Scheduler started")
            
        except Exception as e:
            log.error(f"Error in startup: {e}", exc_info=True)
    
    def cog_unload(self):
        """Cleanup on unload"""
        self.scheduler.stop()
        log.info("RapidResponse cog unloaded")
    
    async def cog_check(self, ctx: commands.Context) -> bool:
        """Global check - only work in game server"""
        if ctx.guild and ctx.guild.id == config.GAME_SERVER_ID:
            return True
        
        # Allow in DMs for some commands
        if ctx.command.name in ['profile', 'status']:
            return True
        
        return False
    
    async def _get_or_create_player(self, user_id: int, guild_id: int) -> dict:
        """Get or create player profile"""
        player = await self.db.get_player(user_id)
        if not player:
            player = await self.db.create_player(user_id, guild_id)
            log.info(f"Created new player profile for {user_id}")
        return player
    
    @commands.group(name="rr", aliases=["rapidresponse"])
    async def rr(self, ctx: commands.Context):
        """Rapid Response Dispatch game commands"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @rr.command(name="status")
    async def status_cmd(self, ctx: commands.Context, state: Optional[str] = None):
        """
        View or change your duty status
        
        Usage:
        - `/rr status` - View current status
        - `/rr status on` - Go on duty (receive missions)
        - `/rr status off` - Go off duty (stop receiving missions)
        """
        player = await self._get_or_create_player(ctx.author.id, ctx.guild.id)
        
        if state is None:
            # Show current status
            embed = discord.Embed(
                title=f"📻 {ctx.author.display_name}'s Status",
                color=config.COLOR_INFO
            )
            
            status = "🟢 Active (On Duty)" if player['is_active'] else "🔴 Inactive (Off Duty)"
            embed.add_field(name="Status", value=status, inline=True)
            embed.add_field(
                name="Station Level",
                value=f"Level {player['station_level']}",
                inline=True
            )
            
            # Show cooldown info if active
            if player['is_active']:
                if player['current_cooldown_until']:
                    cooldown_until = datetime.fromisoformat(player['current_cooldown_until'])
                    if datetime.utcnow() < cooldown_until:
                        time_left = cooldown_until - datetime.utcnow()
                        minutes = int(time_left.total_seconds() / 60)
                        embed.add_field(
                            name="⏱️ Next Mission",
                            value=f"In ~{minutes} minutes",
                            inline=False
                        )
                    else:
                        embed.add_field(
                            name="⏱️ Next Mission",
                            value="Eligible now!",
                            inline=False
                        )
                else:
                    embed.add_field(
                        name="⏱️ Next Mission",
                        value="Calculating...",
                        inline=False
                    )
            
            # Show active mission
            active_mission = await self.db.get_active_mission(ctx.author.id)
            if active_mission:
                embed.add_field(
                    name="🚨 Active Mission",
                    value=f"**{active_mission['mission_name']}**\nRespond to it!",
                    inline=False
                )
            
            # Show active training
            training = await self.db.get_active_training(ctx.author.id)
            if training:
                completes_at = datetime.fromisoformat(training['completes_at'])
                time_left = completes_at - datetime.utcnow()
                minutes = int(time_left.total_seconds() / 60)
                embed.add_field(
                    name="📚 Training",
                    value=f"**{training['stat_type'].title()}**\nCompletes in {minutes} minutes",
                    inline=False
                )
            
            await ctx.send(embed=embed)
        
        elif state.lower() in ['on', 'active', 'online']:
            # Set active
            if player['is_active']:
                await ctx.send("✅ You're already on duty!")
                return
            
            await self.db.set_active(ctx.author.id, True)
            
            embed = discord.Embed(
                title="🟢 On Duty!",
                description=(
                    f"Welcome back, {ctx.author.mention}!\n\n"
                    "You are now **Active** and will receive missions.\n"
                    "Missions will appear in your dispatch thread.\n\n"
                    "Good luck out there! 🚒🚑🚓"
                ),
                color=config.COLOR_SUCCESS
            )
            await ctx.send(embed=embed)
            log.info(f"Player {ctx.author.id} went on duty")
        
        elif state.lower() in ['off', 'inactive', 'offline']:
            # Set inactive
            if not player['is_active']:
                await ctx.send("ℹ️ You're already off duty!")
                return
            
            await self.db.set_active(ctx.author.id, False)
            
            embed = discord.Embed(
                title="🔴 Off Duty",
                description=(
                    f"You are now **Inactive**, {ctx.author.mention}.\n\n"
                    "You will no longer receive new missions.\n"
                    "Use `/rr status on` when you're ready to return!"
                ),
                color=config.COLOR_FAILURE
            )
            await ctx.send(embed=embed)
            log.info(f"Player {ctx.author.id} went off duty")
        
        else:
            await ctx.send("❌ Invalid status. Use `on` or `off`.")
    
    @rr.command(name="profile", aliases=["stats", "me"])
    async def profile_cmd(self, ctx: commands.Context, user: Optional[discord.Member] = None):
        """View your station profile and stats"""
        target_user = user or ctx.author
        player = await self.db.get_player(target_user.id)
        
        if not player:
            if target_user == ctx.author:
                await ctx.send("❌ You haven't started playing yet! Missions will be automatically assigned when you go on duty.")
                return
            else:
                await ctx.send(f"❌ {target_user.mention} hasn't started playing yet!")
                return
        
        embed = create_profile_embed(player, target_user)
        await ctx.send(embed=embed)
    
    @rr.command(name="train")
    async def train_cmd(self, ctx: commands.Context):
        """Start a training session to improve your stats"""
        player = await self._get_or_create_player(ctx.author.id, ctx.guild.id)
        
        # Check if already training
        training = await self.db.get_active_training(ctx.author.id)
        if training:
            completes_at = datetime.fromisoformat(training['completes_at'])
            time_left = completes_at - datetime.utcnow()
            minutes = int(time_left.total_seconds() / 60)
            
            await ctx.send(
                f"⏱️ You're already training **{training['stat_type'].title()}**!\n"
                f"Training completes in **{minutes} minutes**."
            )
            return
        
        # Check if player has an active mission
        active_mission = await self.db.get_active_mission(ctx.author.id)
        if active_mission:
            await ctx.send(
                "🚨 You can't train while you have an active mission!\n"
                "Complete or wait for your current mission to expire first."
            )
            return
        
        # Show training menu
        embed = discord.Embed(
            title="📚 Training Center",
            description=(
                "Select a stat to train:\n\n"
                "Each training session takes **1 hour** and grants **+10** to the chosen stat.\n\n"
                "**Your Current Stats:**\n"
                f"⚡ Response: {player['stat_response']}\n"
                f"🎯 Tactics: {player['stat_tactics']}\n"
                f"📦 Logistics: {player['stat_logistics']}\n"
                f"🏥 Medical: {player['stat_medical']}\n"
                f"⭐ Command: {player['stat_command']}"
            ),
            color=config.COLOR_INFO
        )
        
        view = TrainView(self, ctx.author.id)
        await ctx.send(embed=embed, view=view)
    
    async def start_training(self, interaction: discord.Interaction, stat_type: str):
        """Process training start"""
        player = await self.db.get_player(interaction.user.id)
        
        # Calculate cost
        cost = await self.game_logic.calculate_training_cost(player, stat_type)
        
        # Check if player can afford it
        try:
            can_afford = await bank.can_spend(interaction.user, cost)
        except:
            can_afford = player['credits'] >= cost
        
        if not can_afford:
            await interaction.followup.send(
                f"❌ You need **{cost:,}** credits to train {stat_type.title()}!\n"
                f"You currently have **{player['credits']:,}** credits.",
                ephemeral=True
            )
            return
        
        # Deduct cost
        try:
            await bank.withdraw_credits(interaction.user, cost)
        except:
            # Fallback to internal credits
            await self.db.update_player(
                interaction.user.id,
                credits=player['credits'] - cost
            )
        
        # Start training
        training_id = await self.db.start_training(interaction.user.id, stat_type)
        
        completes_at = datetime.utcnow() + timedelta(hours=config.TRAINING_DURATION_HOURS)
        
        embed = discord.Embed(
            title="📚 Training Started!",
            description=(
                f"You've started training **{stat_type.title()}**!\n\n"
                f"**Cost:** {cost:,} credits\n"
                f"**Duration:** {config.TRAINING_DURATION_HOURS} hour(s)\n"
                f"**Completes:** <t:{int(completes_at.timestamp())}:R>\n\n"
                "You'll receive a notification when training is complete."
            ),
            color=config.COLOR_SUCCESS
        )
        
        await interaction.followup.send(embed=embed)
        log.info(f"Player {interaction.user.id} started training {stat_type}")
    
    @rr.command(name="leaderboard", aliases=["lb", "top"])
    async def leaderboard_cmd(
        self,
        ctx: commands.Context,
        category: str = "level"
    ):
        """
        View the leaderboards
        
        Categories: level, missions, streak, credits, success_rate
        """
        valid_categories = ['level', 'missions', 'streak', 'credits', 'success_rate']
        
        if category.lower() not in valid_categories:
            await ctx.send(
                f"❌ Invalid category! Choose from: {', '.join(valid_categories)}"
            )
            return
        
        # Get leaderboard data
        leaders = await self.db.get_leaderboard(category.lower(), limit=10)
        
        if not leaders:
            await ctx.send("❌ No leaderboard data available yet!")
            return
        
        # Create embed
        category_names = {
            'level': '📊 Station Level',
            'missions': '🎯 Total Missions',
            'streak': '🔥 Mission Streak',
            'credits': '💰 Total Credits',
            'success_rate': '✅ Success Rate'
        }
        
        embed = discord.Embed(
            title=f"🏆 {category_names[category.lower()]} Leaderboard",
            color=config.COLOR_INFO
        )
        
        description_lines = []
        for i, leader in enumerate(leaders, 1):
            user = self.bot.get_user(leader['user_id'])
            username = user.mention if user else f"User {leader['user_id']}"
            
            if category == 'level':
                value = f"Level {leader['station_level']}"
            elif category == 'missions':
                value = f"{leader['total_missions']} missions"
            elif category == 'streak':
                value = f"{leader['mission_streak']} 🔥"
            elif category == 'credits':
                value = f"{leader['credits']:,} 💵"
            elif category == 'success_rate':
                total = leader['total_missions']
                success = leader['successful_missions']
                rate = (success / total * 100) if total > 0 else 0
                value = f"{rate:.1f}% ({success}/{total})"
            else:
                value = "N/A"
            
            medal = ["🥇", "🥈", "🥉"][i-1] if i <= 3 else f"`{i}.`"
            description_lines.append(f"{medal} {username} - {value}")
        
        embed.description = "\n".join(description_lines)
        await ctx.send(embed=embed)
