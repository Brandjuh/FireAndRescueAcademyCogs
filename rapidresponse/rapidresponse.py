"""
Rapid Response Dispatch - A MissionChief-inspired emergency response game
"""
import discord
from redbot.core import commands, bank, Config
from redbot.core.bot import Red
import aiosqlite
import logging
import asyncio
import random
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
    
    async def process_mission_response(
        self,
        interaction: discord.Interaction,
        mission_instance_id: int,
        response_type: str
    ):
        """Process a mission response"""
        try:
            player = await self.db.get_player(interaction.user.id)
            if not player:
                await interaction.followup.send(
                    "❌ Error: Player profile not found!",
                    ephemeral=True
                )
                return
            
            # Resolve mission
            result = await self.game_logic.resolve_mission(
                mission_instance_id,
                response_type,
                player
            )
            
            if 'error' in result:
                await interaction.followup.send(
                    f"❌ {result['error']}",
                    ephemeral=True
                )
                return
            
            # Get mission data for name
            mission = await self.db.get_mission_by_id(mission_instance_id)
            mission_name = mission['mission_name']
            
            # Handle credits through Red bank
            credits = result.get('credits', 0)
            if credits > 0:
                try:
                    await bank.deposit_credits(interaction.user, credits)
                except Exception as e:
                    log.error(f"Error depositing credits: {e}")
                    # Fallback to internal credits
                    await self.db.update_player(
                        interaction.user.id,
                        credits=player['credits'] + credits
                    )
            elif credits < 0:
                try:
                    await bank.withdraw_credits(interaction.user, abs(credits))
                except Exception as e:
                    log.error(f"Error withdrawing credits: {e}")
                    # Fallback to internal credits
                    await self.db.update_player(
                        interaction.user.id,
                        credits=max(0, player['credits'] + credits)
                    )
            
            # Create outcome embed
            embed = create_outcome_embed(
                mission_name,
                result['outcome'],
                result
            )
            
            # Send outcome
            await interaction.followup.send(embed=embed)
            
            # If escalated, send new mission stage
            if result.get('escalated', False):
                await self._send_escalated_mission(
                    interaction.user,
                    player,
                    mission_instance_id,
                    result['next_stage'],
                    result['max_stage']
                )
            
            log.info(
                f"Mission {mission_instance_id} resolved for player {interaction.user.id}: "
                f"{result['outcome']}, {credits} credits, {result.get('xp', 0)} XP"
            )
        
        except Exception as e:
            log.error(f"Error processing mission response: {e}", exc_info=True)
            await interaction.followup.send(
                "❌ An error occurred while processing your response!",
                ephemeral=True
            )
    
    async def _send_escalated_mission(
        self,
        user: discord.User,
        player: dict,
        mission_instance_id: int,
        stage: int,
        max_stage: int
    ):
        """Send the next stage of an escalated mission"""
        try:
            # Get mission data
            mission = await self.db.get_mission_by_id(mission_instance_id)
            mission_data = json.loads(mission['mission_data'])
            
            # Get player's thread
            guild = self.bot.get_guild(config.GAME_SERVER_ID)
            if not guild or not player['thread_id']:
                return
            
            thread = guild.get_thread(player['thread_id'])
            if not thread:
                return
            
            # Calculate new timeout (slightly shorter for escalated stages)
            timeout_seconds = self.game_logic.calculate_mission_timeout(
                player['station_level'], mission['tier']
            )
            timeout_seconds = int(timeout_seconds * 0.8)  # 20% shorter
            
            # Update mission expiry
            new_expires_at = datetime.utcnow() + timedelta(seconds=timeout_seconds)
            await self.db.update_mission(
                mission_instance_id,
                expires_at=new_expires_at.isoformat()
            )
            
            # Generate escalation description
            descriptions = [
                "The situation has worsened! Additional complications have emerged.",
                "New developments require immediate attention! The incident is expanding.",
                "Critical update: The response must be intensified!",
                "Emergency escalation: Additional resources are urgently needed!",
            ]
            
            description = random.choice(descriptions)
            requirements = self.mission_manager.get_mission_requirements_text(mission_data)
            
            # Create embed
            from .views import create_mission_embed
            embed = create_mission_embed(
                mission_data=mission_data,
                mission_name=f"{mission_data['name']} (ESCALATED)",
                tier=mission['tier'],
                difficulty=mission['difficulty'],
                description=description,
                requirements=requirements,
                timeout_seconds=timeout_seconds,
                stage=stage,
                max_stage=max_stage
            )
            
            # Create new view
            view = MissionView(
                cog=self,
                mission_instance_id=mission_instance_id,
                user_id=user.id,
                timeout=timeout_seconds
            )
            
            # Send escalated mission
            await asyncio.sleep(5)  # Brief delay for dramatic effect
            message = await thread.send(
                content="🚨 **ESCALATION!** 🚨",
                embed=embed,
                view=view
            )
            
            # Update message ID
            await self.db.update_mission(mission_instance_id, message_id=message.id)
            
        except Exception as e:
            log.error(f"Error sending escalated mission: {e}", exc_info=True)
    
    # Admin commands
    @rr.group(name="admin")
    @commands.admin_or_permissions(administrator=True)
    async def rr_admin(self, ctx: commands.Context):
        """Admin commands for Rapid Response"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @rr_admin.command(name="setchannel")
    async def set_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where mission threads will be created"""
        await self.config.mission_channel_id.set(channel.id)
        await self.db.set_config('mission_channel_id', str(channel.id))
        
        embed = discord.Embed(
            title="✅ Mission Channel Set",
            description=f"Mission threads will be created in {channel.mention}",
            color=config.COLOR_SUCCESS
        )
        await ctx.send(embed=embed)
    
    @rr_admin.command(name="refreshmissions")
    async def refresh_missions(self, ctx: commands.Context):
        """Manually refresh mission cache from MissionChief API"""
        await ctx.send("🔄 Refreshing mission data...")
        
        success = await self.mission_manager.fetch_missions()
        
        if success:
            await ctx.send(
                f"✅ Successfully refreshed {len(self.mission_manager.missions)} missions!"
            )
        else:
            await ctx.send("❌ Failed to refresh missions. Check logs for details.")
    
    @rr_admin.command(name="givexp")
    async def give_xp(
        self,
        ctx: commands.Context,
        user: discord.Member,
        amount: int
    ):
        """Give XP to a player"""
        player = await self.db.get_player(user.id)
        if not player:
            await ctx.send(f"❌ {user.mention} hasn't started playing yet!")
            return
        
        level_info = await self.db.add_xp(user.id, amount)
        
        embed = discord.Embed(
            title="✅ XP Granted",
            description=f"Gave {amount:,} XP to {user.mention}",
            color=config.COLOR_SUCCESS
        )
        
        if level_info.get('leveled_up'):
            embed.add_field(
                name="🎉 Level Up!",
                value=f"Level {level_info['old_level']} → {level_info['new_level']}",
                inline=False
            )
        
        await ctx.send(embed=embed)
    
    @rr_admin.command(name="givecredits")
    async def give_credits(
        self,
        ctx: commands.Context,
        user: discord.Member,
        amount: int
    ):
        """Give credits to a player"""
        try:
            await bank.deposit_credits(user, amount)
            await ctx.send(f"✅ Deposited {amount:,} credits to {user.mention}")
        except Exception as e:
            await ctx.send(f"❌ Error: {e}")
    
    @rr_admin.command(name="setstat")
    async def set_stat(
        self,
        ctx: commands.Context,
        user: discord.Member,
        stat: str,
        value: int
    ):
        """Set a player's stat value"""
        player = await self.db.get_player(user.id)
        if not player:
            await ctx.send(f"❌ {user.mention} hasn't started playing yet!")
            return
        
        valid_stats = ['response', 'tactics', 'logistics', 'medical', 'command']
        if stat.lower() not in valid_stats:
            await ctx.send(f"❌ Invalid stat! Choose from: {', '.join(valid_stats)}")
            return
        
        stat_key = f'stat_{stat.lower()}'
        await self.db.update_player(user.id, **{stat_key: value})
        
        await ctx.send(
            f"✅ Set {user.mention}'s **{stat.title()}** to **{value}**"
        )
    
    @rr_admin.command(name="forcemission")
    async def force_mission(self, ctx: commands.Context, user: discord.Member):
        """Force assign a mission to a player"""
        player = await self.db.get_player(user.id)
        if not player:
            await ctx.send(f"❌ {user.mention} hasn't started playing yet!")
            return
        
        if not player['is_active']:
            await ctx.send(f"❌ {user.mention} is not on duty!")
            return
        
        # Check for existing mission
        active_mission = await self.db.get_active_mission(user.id)
        if active_mission:
            await ctx.send(f"❌ {user.mention} already has an active mission!")
            return
        
        await ctx.send(f"🔄 Assigning mission to {user.mention}...")
        
        await self.scheduler._assign_mission_to_player(player)
        
        await ctx.send(f"✅ Mission assigned to {user.mention}!")
    
    @rr_admin.command(name="stats")
    async def admin_stats(self, ctx: commands.Context):
        """View game statistics"""
        import aiosqlite
        
        async with aiosqlite.connect(str(self.db.db_path)) as db:
            # Total players
            cursor = await db.execute("SELECT COUNT(*) FROM players")
            total_players = (await cursor.fetchone())[0]
            
            # Active players
            cursor = await db.execute("SELECT COUNT(*) FROM players WHERE is_active = 1")
            active_players = (await cursor.fetchone())[0]
            
            # Total missions
            cursor = await db.execute("SELECT COUNT(*) FROM mission_history")
            total_missions = (await cursor.fetchone())[0]
            
            # Success rate
            cursor = await db.execute("""
                SELECT 
                    SUM(CASE WHEN outcome = 'full_success' THEN 1 ELSE 0 END) as success,
                    COUNT(*) as total
                FROM mission_history
            """)
            row = await cursor.fetchone()
            success_count = row[0] or 0
            mission_total = row[1] or 1
            success_rate = (success_count / mission_total * 100) if mission_total > 0 else 0
        
        embed = discord.Embed(
            title="📊 Rapid Response Statistics",
            color=config.COLOR_INFO
        )
        
        embed.add_field(
            name="👥 Players",
            value=f"**Total:** {total_players}\n**Active:** {active_players}",
            inline=True
        )
        
        embed.add_field(
            name="🎯 Missions",
            value=f"**Completed:** {total_missions}\n**Success Rate:** {success_rate:.1f}%",
            inline=True
        )
        
        embed.add_field(
            name="📁 Cached Missions",
            value=f"{len(self.mission_manager.missions)} missions",
            inline=True
        )
        
        await ctx.send(embed=embed)


# Required for Red-DiscordBot
async def setup(bot: Red):
    """Setup function for Red"""
    await bot.add_cog(RapidResponse(bot))
    log.info("RapidResponse cog loaded")
