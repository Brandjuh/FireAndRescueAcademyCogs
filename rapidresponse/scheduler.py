"""
Scheduler for automatic mission assignment
"""
import asyncio
import discord
import logging
import random
from datetime import datetime, timedelta
from typing import Optional
from . import config
from .views import MissionView, create_mission_embed

log = logging.getLogger("red.rapidresponse.scheduler")


class MissionScheduler:
    """Manages automatic mission assignment"""
    
    def __init__(self, cog):
        self.cog = cog
        self.db = cog.db
        self.mission_manager = cog.mission_manager
        self.game_logic = cog.game_logic
        self.task: Optional[asyncio.Task] = None
        self.running = False
    
    def start(self):
        """Start the scheduler"""
        if self.task is None or self.task.done():
            self.running = True
            self.task = asyncio.create_task(self._run())
            log.info("Mission scheduler started")
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False
        if self.task and not self.task.done():
            self.task.cancel()
            log.info("Mission scheduler stopped")
    
    async def _run(self):
        """Main scheduler loop"""
        await self.cog.bot.wait_until_ready()
        
        while self.running:
            try:
                # Refresh mission cache if needed
                await self.mission_manager.refresh_if_needed()
                
                # Clean up expired missions
                expired_count = await self.db.clean_expired_missions()
                if expired_count > 0:
                    log.info(f"Cleaned {expired_count} expired missions")
                    await self._handle_expired_missions()
                
                # Check for active players needing missions
                await self._assign_missions()
                
                # Check for completed trainings
                await self._check_trainings()
                
            except Exception as e:
                log.error(f"Error in scheduler loop: {e}", exc_info=True)
            
            # Wait for next check
            await asyncio.sleep(config.MISSION_CHECK_INTERVAL * 60)
    
    async def _assign_missions(self):
        """Assign missions to eligible players"""
        try:
            # Get all active players
            async with self.db.db_path.open() as _:
                import aiosqlite
                async with aiosqlite.connect(str(self.db.db_path)) as db:
                    db.row_factory = aiosqlite.Row
                    async with db.execute("""
                        SELECT * FROM players WHERE is_active = 1
                    """) as cursor:
                        active_players = await cursor.fetchall()
            
            for player_row in active_players:
                player = dict(player_row)
                
                # Check if player is eligible for a new mission
                if await self._is_eligible_for_mission(player):
                    await self._assign_mission_to_player(player)
        
        except Exception as e:
            log.error(f"Error assigning missions: {e}", exc_info=True)
    
    async def _is_eligible_for_mission(self, player: dict) -> bool:
        """Check if player is eligible for a new mission"""
        
        # Check if player has an active mission
        active_mission = await self.db.get_active_mission(player['user_id'])
        if active_mission:
            return False
        
        # Check if player has an active training
        training = await self.db.get_active_training(player['user_id'])
        if training:
            return False
        
        # Check cooldown
        if player['current_cooldown_until']:
            cooldown_until = datetime.fromisoformat(player['current_cooldown_until'])
            if datetime.utcnow() < cooldown_until:
                return False
        
        # Check time since last mission
        if player['last_mission_time']:
            last_mission = datetime.fromisoformat(player['last_mission_time'])
            
            # Calculate minimum cooldown based on level
            level = player['station_level']
            if level <= 5:
                min_cooldown = config.BASE_MISSION_COOLDOWN_MIN
                max_cooldown = config.BASE_MISSION_COOLDOWN_MAX
            else:
                min_cooldown = config.ADVANCED_MISSION_COOLDOWN_MIN
                max_cooldown = config.ADVANCED_MISSION_COOLDOWN_MAX
            
            cooldown_minutes = random.randint(min_cooldown, max_cooldown)
            next_mission_time = last_mission + timedelta(minutes=cooldown_minutes)
            
            if datetime.utcnow() < next_mission_time:
                return False
        
        return True
    
    async def _assign_mission_to_player(self, player: dict):
        """Assign a mission to a player"""
        try:
            # Select appropriate mission
            mission_data = self.mission_manager.select_mission_for_player(
                player['station_level']
            )
            
            if not mission_data:
                log.warning(f"No mission available for player {player['user_id']}")
                return
            
            # Calculate mission parameters
            tier = self.mission_manager.calculate_mission_tier(mission_data)
            difficulty = self.mission_manager.calculate_difficulty(mission_data, tier)
            timeout_seconds = self.game_logic.calculate_mission_timeout(
                player['station_level'], tier
            )
            max_stages = self.mission_manager.determine_max_stages(mission_data, tier)
            
            # Create mission in database
            import json
            mission_instance_id = await self.db.create_mission(
                user_id=player['user_id'],
                mission_id=mission_data['id'],
                mission_name=mission_data['name'],
                mission_data=json.dumps(mission_data),
                tier=tier,
                difficulty=difficulty,
                timeout_seconds=timeout_seconds,
                max_stage=max_stages
            )
            
            # Get player's thread
            thread = await self._get_or_create_player_thread(player)
            
            if not thread:
                log.error(f"Could not get thread for player {player['user_id']}")
                return
            
            # Generate mission description and requirements
            description = self.mission_manager.generate_mission_description(mission_data)
            requirements = self.mission_manager.get_mission_requirements_text(mission_data)
            
            # Create and send mission embed
            embed = create_mission_embed(
                mission_data=mission_data,
                mission_name=mission_data['name'],
                tier=tier,
                difficulty=difficulty,
                description=description,
                requirements=requirements,
                timeout_seconds=timeout_seconds,
                stage=1,
                max_stage=max_stages
            )
            
            # Create view with buttons
            view = MissionView(
                cog=self.cog,
                mission_instance_id=mission_instance_id,
                user_id=player['user_id'],
                timeout=timeout_seconds
            )
            
            # Send mission
            message = await thread.send(embed=embed, view=view)
            
            # Store message ID
            await self.db.update_mission(mission_instance_id, message_id=message.id)
            
            log.info(
                f"Assigned mission {mission_instance_id} "
                f"(tier {tier}, {mission_data['name']}) "
                f"to player {player['user_id']}"
            )
            
        except Exception as e:
            log.error(f"Error assigning mission to player {player['user_id']}: {e}", exc_info=True)
    
    async def _get_or_create_player_thread(self, player: dict) -> Optional[discord.Thread]:
        """Get or create a thread for the player"""
        try:
            guild = self.cog.bot.get_guild(config.GAME_SERVER_ID)
            if not guild:
                log.error(f"Game server {config.GAME_SERVER_ID} not found")
                return None
            
            # Get mission channel from config
            channel_id = await self.db.get_config('mission_channel_id')
            if not channel_id:
                log.error("Mission channel not configured")
                return None
            
            channel = guild.get_channel(int(channel_id))
            if not channel:
                log.error(f"Mission channel {channel_id} not found")
                return None
            
            # Check if player already has a thread
            if player['thread_id']:
                try:
                    thread = guild.get_thread(player['thread_id'])
                    if thread and not thread.archived:
                        return thread
                    elif thread and thread.archived:
                        # Unarchive
                        await thread.edit(archived=False)
                        return thread
                except:
                    pass
            
            # Create new thread
            user = guild.get_member(player['user_id'])
            if not user:
                log.error(f"User {player['user_id']} not found in guild")
                return None
            
            thread_name = f"🚨 {user.display_name}'s Dispatch"
            
            # Create thread
            thread = await channel.create_thread(
                name=thread_name,
                auto_archive_duration=10080,  # 1 week
                type=discord.ChannelType.public_thread
            )
            
            # Send welcome message
            welcome_embed = discord.Embed(
                title="📻 Dispatch Thread Created",
                description=(
                    f"Welcome to your personal dispatch thread, {user.mention}!\n\n"
                    "All your missions will appear here. You can respond to them "
                    "using the buttons below each mission.\n\n"
                    "Good luck out there! 🚒🚑🚓"
                ),
                color=config.COLOR_INFO
            )
            await thread.send(embed=welcome_embed)
            
            # Store thread ID
            await self.db.update_player(player['user_id'], thread_id=thread.id)
            
            log.info(f"Created thread {thread.id} for player {player['user_id']}")
            
            return thread
            
        except Exception as e:
            log.error(f"Error getting/creating thread for player {player['user_id']}: {e}", exc_info=True)
            return None
    
    async def _handle_expired_missions(self):
        """Handle missions that expired (timeout)"""
        try:
            import aiosqlite
            async with aiosqlite.connect(str(self.db.db_path)) as db:
                db.row_factory = aiosqlite.Row
                
                # Get missions that just timed out
                async with db.execute("""
                    SELECT am.*, p.* FROM active_missions am
                    JOIN players p ON am.user_id = p.user_id
                    WHERE am.status = 'timeout'
                    AND am.message_id IS NOT NULL
                """) as cursor:
                    timed_out = await cursor.fetchall()
            
            for mission_row in timed_out:
                mission = dict(mission_row)
                
                try:
                    # Get thread and update message
                    guild = self.cog.bot.get_guild(config.GAME_SERVER_ID)
                    if guild and mission['thread_id']:
                        thread = guild.get_thread(mission['thread_id'])
                        if thread:
                            try:
                                message = await thread.fetch_message(mission['message_id'])
                                
                                # Create timeout embed
                                embed = discord.Embed(
                                    title="⏰ Mission Timeout",
                                    description=(
                                        f"Mission **{mission['mission_name']}** timed out.\n\n"
                                        f"**Penalties:**\n"
                                        f"• -{config.TIMEOUT_PENALTY_MORALE} morale\n"
                                        f"• Mission streak reset"
                                    ),
                                    color=config.COLOR_FAILURE
                                )
                                
                                # Check if auto-inactive
                                if mission['ignored_missions'] >= config.MAX_IGNORED_MISSIONS:
                                    embed.add_field(
                                        name="⚠️ Automatic Deactivation",
                                        value=(
                                            f"You've ignored {config.MAX_IGNORED_MISSIONS} missions.\n"
                                            "Your station has been set to **Inactive**.\n\n"
                                            "Use `/status on` when you're ready to return to duty."
                                        ),
                                        inline=False
                                    )
                                
                                await message.edit(embed=embed, view=None)
                                
                            except discord.NotFound:
                                pass
                            except Exception as e:
                                log.error(f"Error updating timeout message: {e}")
                
                except Exception as e:
                    log.error(f"Error handling expired mission {mission['mission_instance_id']}: {e}")
        
        except Exception as e:
            log.error(f"Error in _handle_expired_missions: {e}", exc_info=True)
    
    async def _check_trainings(self):
        """Check for completed trainings"""
        try:
            import aiosqlite
            async with aiosqlite.connect(str(self.db.db_path)) as db:
                db.row_factory = aiosqlite.Row
                
                now = datetime.utcnow().isoformat()
                
                # Get completed trainings
                async with db.execute("""
                    SELECT t.*, p.* FROM training t
                    JOIN players p ON t.user_id = p.user_id
                    WHERE t.is_complete = 0 AND t.completes_at <= ?
                """, (now,)) as cursor:
                    completed = await cursor.fetchall()
            
            for training_row in completed:
                training = dict(training_row)
                
                try:
                    # Complete training and apply stat increase
                    result = await self.game_logic.complete_training_for_player(
                        training, training
                    )
                    
                    # Notify player
                    guild = self.cog.bot.get_guild(config.GAME_SERVER_ID)
                    if guild and training['thread_id']:
                        thread = guild.get_thread(training['thread_id'])
                        if thread:
                            embed = discord.Embed(
                                title="✅ Training Complete!",
                                description=(
                                    f"Your **{result['stat_type'].title()}** training has finished!\n\n"
                                    f"**{result['stat_type'].title()}:** "
                                    f"{result['old_value']} → {result['new_value']} "
                                    f"(+{result['gain']})"
                                ),
                                color=config.COLOR_SUCCESS
                            )
                            await thread.send(embed=embed)
                    
                    log.info(f"Completed training for player {training['user_id']}")
                    
                except Exception as e:
                    log.error(f"Error completing training {training['id']}: {e}")
        
        except Exception as e:
            log.error(f"Error in _check_trainings: {e}", exc_info=True)
