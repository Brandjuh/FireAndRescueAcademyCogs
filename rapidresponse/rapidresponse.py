"""
RapidResponse Cog - Main game logic
Author: BrandjuhNL
"""

import discord
from redbot.core import commands, Config, bank
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box, humanize_number
import asyncio
import aiohttp
import random
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List
import logging
import time

from .models import RapidResponseDB
from .state import GameManager, GameState
from .parsing import parse_vehicle_input, get_vehicle_display_name, VEHICLE_SYNONYMS
from .scoring import calculate_score, format_score_breakdown
from .views import LobbyView

log = logging.getLogger("red.rapidresponse")


class RapidResponse(commands.Cog):
    """
    Play Rapid Response - Guess the correct vehicles for MissionChief missions!
    
    Players compete to identify the correct vehicle requirements for random missions.
    """
    
    def __init__(self, bot: Red):
        self.bot = bot
        self.game_manager = GameManager()
        self.bank = bank
        
        # Config
        self.config = Config.get_conf(self, identifier=945732198234, force_registration=True)
        default_guild = {
            "entry_fee": 1000,
            "lobby_duration": 60,
            "round_duration": 60,
            "enabled": True,
            "mission_list_channel_id": 1436050384023191632,  # Channel to lock during games
            "notify_role_id": 1441473542725046312  # Role to ping for game notifications
        }
        self.config.register_guild(**default_guild)
        
        # Database
        data_path = Path(__file__).parent
        self.db = RapidResponseDB(data_path / "RapidResponseGame.db")
        
        # Mission cache
        self.missions_cache: List[Dict] = []
        self.missions_last_fetch: float = 0
        self.missions_cache_duration: float = 3600  # 1 hour
    
    async def cog_load(self):
        """Initialize the cog."""
        await self.db.initialize()
        
        # Recover from any bot restarts
        await self.recover_from_restart()
        
        log.info("RapidResponse cog loaded")
    
    async def cog_unload(self):
        """Clean up when cog is unloaded."""
        # Cancel all active games
        for channel_id in list(self.game_manager.games.keys()):
            game = self.game_manager.get_game(channel_id)
            if game:
                await self.cancel_game(game, reason="Bot shutting down")
        
        log.info("RapidResponse cog unloaded")
    
    async def recover_from_restart(self):
        """Recover and refund unfinished games after bot restart."""
        log.info("Checking for unfinished games...")
        
        for guild in self.bot.guilds:
            unfinished = await self.db.get_unfinished_games(guild.id)
            
            for game_data in unfinished:
                game_id = game_data['game_id']
                channel_id = game_data['channel_id']
                
                # Get players
                players = await self.db.get_game_players(game_id)
                
                # Unlock mission list for all locked players
                locked_users = await self.db.get_lockouts_for_game(game_id)
                for user_id in locked_users:
                    try:
                        await self.unlock_mission_list(guild.id, user_id)
                        await self.db.remove_lockout(game_id, user_id)
                    except Exception as e:
                        log.error(f"Error unlocking mission list for user {user_id}: {e}")
                
                # Refund all players
                for player_data in players:
                    user_id = player_data['user_id']
                    entry_fee = player_data['paid_entry']
                    
                    try:
                        user = await self.bot.fetch_user(user_id)
                        if user:
                            await bank.deposit_credits(user, entry_fee)
                            log.info(f"Refunded {entry_fee} credits to user {user_id}")
                    except Exception as e:
                        log.error(f"Error refunding user {user_id}: {e}")
                
                # Mark game as cancelled
                await self.db.end_game(game_id, status='restart_cancelled')
                
                # Try to notify channel
                try:
                    channel = self.bot.get_channel(channel_id)
                    if channel:
                        await channel.send(
                            "⚠️ A Rapid Response game was cancelled due to bot restart. "
                            "All entry fees have been refunded and mission list access restored."
                        )
                except Exception as e:
                    log.error(f"Error notifying channel {channel_id}: {e}")
        
        log.info("Recovery complete")
    
    async def lock_mission_list(self, guild_id: int, user_id: int):
        """Lock mission list channel for a user."""
        try:
            mission_channel_id = await self.config.guild_from_id(guild_id).mission_list_channel_id()
            guild = self.bot.get_guild(guild_id)
            
            if not guild:
                return
            
            channel = guild.get_channel(mission_channel_id)
            if not channel:
                log.warning(f"Mission list channel {mission_channel_id} not found")
                return
            
            member = guild.get_member(user_id)
            if not member:
                return
            
            # Set permission overwrite to deny view
            await channel.set_permissions(member, view_channel=False, reason="Rapid Response game active")
            log.info(f"Locked mission list for user {user_id}")
            
        except Exception as e:
            log.error(f"Error locking mission list for user {user_id}: {e}", exc_info=True)
    
    async def unlock_mission_list(self, guild_id: int, user_id: int):
        """Unlock mission list channel for a user."""
        try:
            mission_channel_id = await self.config.guild_from_id(guild_id).mission_list_channel_id()
            guild = self.bot.get_guild(guild_id)
            
            if not guild:
                return
            
            channel = guild.get_channel(mission_channel_id)
            if not channel:
                return
            
            member = guild.get_member(user_id)
            if not member:
                return
            
            # Remove the permission overwrite
            await channel.set_permissions(member, overwrite=None, reason="Rapid Response game ended")
            log.info(f"Unlocked mission list for user {user_id}")
            
        except Exception as e:
            log.error(f"Error unlocking mission list for user {user_id}: {e}", exc_info=True)
    
    async def fetch_missions(self) -> List[Dict]:
        """Fetch missions from MissionChief API."""
        current_time = time.time()
        
        # Use cache if available and not expired
        if self.missions_cache and (current_time - self.missions_last_fetch) < self.missions_cache_duration:
            return self.missions_cache
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://www.missionchief.com/einsaetze.json") as resp:
                    if resp.status == 200:
                        missions = await resp.json()
                        # Filter out missions without requirements
                        self.missions_cache = [m for m in missions if m.get('requirements')]
                        self.missions_last_fetch = current_time
                        log.info(f"Fetched {len(self.missions_cache)} missions from API")
                        return self.missions_cache
                    else:
                        log.error(f"Failed to fetch missions: HTTP {resp.status}")
                        return self.missions_cache if self.missions_cache else []
        except Exception as e:
            log.error(f"Error fetching missions: {e}", exc_info=True)
            return self.missions_cache if self.missions_cache else []
    
    def select_random_mission(self) -> Optional[Dict]:
        """Select a random mission from the cache."""
        if not self.missions_cache:
            return None
        return random.choice(self.missions_cache)
    
    @commands.group(name="rapidresponse", aliases=["rr"])
    @commands.guild_only()
    async def rapidresponse(self, ctx):
        """Rapid Response game commands."""
        pass
    
    @rapidresponse.command(name="start")
    @commands.guild_only()
    @commands.cooldown(1, 10, commands.BucketType.channel)
    async def rr_start(self, ctx):
        """Start a new Rapid Response game."""
        # Check if enabled
        if not await self.config.guild(ctx.guild).enabled():
            await ctx.send("❌ Rapid Response is currently disabled in this server.")
            return
        
        # Check if game already running in channel
        if self.game_manager.has_active_game(ctx.channel.id):
            await ctx.send("❌ There's already a game running in this channel!")
            return
        
        # Fetch missions
        missions = await self.fetch_missions()
        if not missions:
            await ctx.send("❌ Unable to fetch missions from MissionChief. Please try again later.")
            return
        
        # Get config
        entry_fee = await self.config.guild(ctx.guild).entry_fee()
        lobby_duration = await self.config.guild(ctx.guild).lobby_duration()
        round_duration = await self.config.guild(ctx.guild).round_duration()
        
        # Create game in database
        game_id = await self.db.create_game(
            ctx.guild.id,
            ctx.channel.id,
            mode='classic',
            solo=False,
            entry_fee=entry_fee,
            total_pot=0
        )
        
        # Create game state
        game = self.game_manager.create_game(
            game_id=game_id,
            guild_id=ctx.guild.id,
            channel_id=ctx.channel.id,
            entry_fee=entry_fee,
            lobby_duration=lobby_duration,
            round_duration=round_duration
        )
        
        # Create lobby embed
        embed = discord.Embed(
            title="🚒 Rapid Response - Game Lobby",
            description=(
                "**Get ready to test your MissionChief knowledge!**\n\n"
                "You'll see a random mission and must guess the correct vehicle requirements. "
                "The player with the highest score wins the pot!\n\n"
                f"**Entry Fee:** {humanize_number(entry_fee)} credits\n"
                f"**Round Time:** {round_duration} seconds\n"
                f"**Lobby closes in:** {lobby_duration} seconds"
            ),
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="How to Play",
            value=(
                "1️⃣ Click **Join Game** to enter\n"
                "2️⃣ Wait for the lobby to fill\n"
                "3️⃣ When the round starts, type your vehicle guess\n"
                "4️⃣ Highest score wins!"
            ),
            inline=False
        )
        
        embed.add_field(
            name="Answer Format",
            value=(
                "You can use short codes or full names:\n"
                "`FT2 BC1` or `2 fire trucks, 1 battalion chief`\n"
                "You can send multiple messages - they accumulate!"
            ),
            inline=False
        )
        
        embed.add_field(
            name="⚠️ Important",
            value=(
                "• Joining locks mission list access (prevents cheating)\n"
                "• Solo mode: Perfect match = 2x entry, Not perfect = Lose entry\n"
                "• Multiplayer: Winner takes all!"
            ),
            inline=False
        )
        
        embed.add_field(
            name="Players (0)",
            value="*No players yet*",
            inline=False
        )
        
        embed.set_footer(text=f"Game ID: {game_id}")
        
        # Create view
        view = LobbyView(self, ctx.channel.id)
        
        # Send lobby message
        lobby_msg = await ctx.send(embed=embed, view=view)
        game.lobby_message_id = lobby_msg.id
        
        # Ping notify role if configured
        notify_role_id = await self.config.guild(ctx.guild).notify_role_id()
        if notify_role_id:
            notify_role = ctx.guild.get_role(notify_role_id)
            if notify_role:
                try:
                    await ctx.send(
                        f"{notify_role.mention} A new Rapid Response game is starting!",
                        allowed_mentions=discord.AllowedMentions(roles=True)
                    )
                except Exception as e:
                    log.error(f"Error pinging notify role: {e}")
        
        # Start lobby timer
        game.lobby_task = self.bot.loop.create_task(self.lobby_timer(game))
    
    async def lobby_timer(self, game: GameState):
        """Handle lobby countdown."""
        try:
            await asyncio.sleep(game.lobby_duration)
            
            # Check if game still exists and is in lobby
            if game.channel_id not in self.game_manager.games:
                return
            if game.status != 'lobby':
                return
            
            # Start game or cancel if no players
            if len(game.players) == 0:
                await self.cancel_game(game, reason="No players joined")
            else:
                await self.start_game(game)
                
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"Error in lobby timer: {e}", exc_info=True)
    
    async def update_lobby_embed(self, game: GameState):
        """Update the lobby embed with current players."""
        try:
            channel = self.bot.get_channel(game.channel_id)
            if not channel or not game.lobby_message_id:
                return
            
            lobby_msg = await channel.fetch_message(game.lobby_message_id)
            embed = lobby_msg.embeds[0]
            
            # Update players field
            player_count = len(game.players)
            if player_count > 0:
                player_mentions = []
                for user_id in game.players:
                    user = self.bot.get_user(user_id)
                    if user:
                        player_mentions.append(user.mention)
                
                embed.set_field_at(
                    2,
                    name=f"Players ({player_count})",
                    value="\n".join(player_mentions),
                    inline=False
                )
            else:
                embed.set_field_at(
                    2,
                    name="Players (0)",
                    value="*No players yet*",
                    inline=False
                )
            
            # Update pot
            embed.description = embed.description.split("**Entry Fee:**")[0] + (
                f"**Entry Fee:** {humanize_number(game.entry_fee)} credits\n"
                f"**Current Pot:** {humanize_number(game.total_pot)} credits\n"
                f"**Round Time:** {game.round_duration} seconds"
            )
            
            await lobby_msg.edit(embed=embed)
            
        except Exception as e:
            log.error(f"Error updating lobby embed: {e}", exc_info=True)
    
    async def start_game(self, game: GameState):
        """Start the actual game round."""
        try:
            # Determine if solo
            game.solo = len(game.players) == 1
            
            # Select random mission
            mission = self.select_random_mission()
            if not mission:
                await self.cancel_game(game, reason="Unable to select mission")
                return
            
            game.mission_id = mission.get('id', 'unknown')
            game.mission_name = mission.get('name', 'Unknown Mission')
            game.mission_requirements = mission.get('requirements', {})
            
            # Filter requirements to only include vehicle types (not foam_needed, personnel_educations, etc)
            filtered_requirements = {}
            for key, value in game.mission_requirements.items():
                if key in VEHICLE_SYNONYMS:
                    filtered_requirements[key] = value
            game.mission_requirements = filtered_requirements
            
            if not game.mission_requirements:
                await self.cancel_game(game, reason="Mission has no vehicle requirements")
                return
            
            # Create round in database
            game.round_id = await self.db.create_round(
                game.game_id,
                game.mission_id,
                game.mission_name,
                game.mission_requirements,
                game.round_duration
            )
            
            # Update game status
            game.status = 'running'
            await self.db.end_game(game.game_id, status='running')
            
            # Create round embed
            channel = self.bot.get_channel(game.channel_id)
            if not channel:
                return
            
            # Delete lobby message
            if game.lobby_message_id:
                try:
                    lobby_msg = await channel.fetch_message(game.lobby_message_id)
                    await lobby_msg.delete()
                except:
                    pass
            
            embed = discord.Embed(
                title="🚨 ROUND START! 🚨",
                description=f"**Mission: {game.mission_name}**\n**Mission ID: {game.mission_id}**",
                color=discord.Color.red()
            )
            
            embed.add_field(
                name="📋 Your Task",
                value=(
                    "Identify the correct vehicle requirements for this mission!\n\n"
                    "Type your answer now using:\n"
                    "• Short codes: `FT2 BC1 HR1`\n"
                    "• Full names: `2 fire trucks, 1 chief, 1 heavy rescue`\n"
                    "• Or mix both!"
                ),
                inline=False
            )
            
            embed.add_field(
                name="⏱️ Time Remaining",
                value=f"{game.round_duration} seconds",
                inline=True
            )
            
            embed.add_field(
                name="👥 Players",
                value=str(len(game.players)),
                inline=True
            )
            
            if game.solo:
                embed.add_field(
                    name="🎮 Solo Mode",
                    value=(
                        "Playing solo!\n"
                        "🏆 Perfect match = **2x entry fee**\n"
                        "💔 Not perfect = **Entry fee lost**"
                    ),
                    inline=False
                )
            else:
                embed.add_field(
                    name="💰 Pot",
                    value=f"{humanize_number(game.total_pot)} credits",
                    inline=True
                )
            
            embed.set_footer(text="Answers sent after this message will count!")
            embed.timestamp = datetime.utcnow()
            
            round_msg = await channel.send("@here", embed=embed)
            game.round_message_id = round_msg.id
            game.round_start_time = time.time()
            
            # Start round timer
            game.round_task = self.bot.loop.create_task(self.round_timer(game))
            
        except Exception as e:
            log.error(f"Error starting game: {e}", exc_info=True)
            await self.cancel_game(game, reason="Error starting game")
    
    async def round_timer(self, game: GameState):
        """Handle round countdown."""
        try:
            await asyncio.sleep(game.round_duration)
            
            # Check if game still exists and is running
            if game.channel_id not in self.game_manager.games:
                return
            if game.status != 'running':
                return
            
            # End round
            await self.end_round(game)
            
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"Error in round timer: {e}", exc_info=True)
    
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """Listen for player answers during active rounds."""
        # Ignore bots and DMs
        if message.author.bot or not message.guild:
            return
        
        # Check if there's an active game in this channel
        game = self.game_manager.get_game(message.channel.id)
        if not game or game.status != 'running':
            return
        
        # Check if message is from a player
        if message.author.id not in game.players:
            return
        
        # Check if message was sent after round start
        if game.round_start_time and message.created_at.timestamp() < game.round_start_time:
            return
        
        # Rate limiting check
        current_time = time.time()
        if not game.can_player_answer(message.author.id, current_time, rate_limit=2.0):
            try:
                await message.reply(
                    "⏳ Please wait 2 seconds between answers!",
                    delete_after=3
                )
            except:
                pass
            return
        
        # Parse the message
        vehicles = parse_vehicle_input(message.content)
        if not vehicles:
            # Not a valid answer, ignore
            return
        
        # Record the answer
        game.record_answer(message.author.id, vehicles, current_time)
        
        # React to confirm
        try:
            await message.add_reaction("✅")
        except:
            pass
    
    async def end_round(self, game: GameState):
        """End the round and calculate winners."""
        try:
            channel = self.bot.get_channel(game.channel_id)
            if not channel:
                return
            
            # Mark round as ended
            await self.db.end_round(game.round_id)
            
            # Calculate scores
            scores = {}
            perfect_matches = {}
            for user_id in game.players:
                answer = game.player_answers[user_id]
                score, is_perfect = calculate_score(game.mission_requirements, answer.vehicles)
                scores[user_id] = score
                perfect_matches[user_id] = is_perfect
                
                # Save to database
                await self.db.save_answer(
                    game.round_id,
                    user_id,
                    score,
                    answer.vehicles,
                    is_perfect
                )
                await self.db.update_player_score(game.game_id, user_id, score)
            
            # Determine winner(s)
            if scores:
                max_score = max(scores.values())
                winners = [uid for uid, score in scores.items() if score == max_score]
            else:
                winners = []
                max_score = 0.0
            
            # Unlock mission list for all players
            for user_id in game.players:
                await self.unlock_mission_list(channel.guild.id, user_id)
                await self.db.remove_lockout(game.game_id, user_id)
            
            # Create results embed
            embed = discord.Embed(
                title="🏁 Round Complete!",
                description=f"**Mission: {game.mission_name}**",
                color=discord.Color.green()
            )
            
            # Show correct answer
            correct_answer = []
            for vehicle_key, count in sorted(game.mission_requirements.items()):
                vehicle_name = get_vehicle_display_name(vehicle_key)
                correct_answer.append(f"{count}x {vehicle_name}")
            
            embed.add_field(
                name="✅ Correct Answer",
                value="\n".join(correct_answer) if correct_answer else "No requirements",
                inline=False
            )
            
            # Show scores
            if scores:
                score_lines = []
                sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
                
                for user_id, score in sorted_scores:
                    user = self.bot.get_user(user_id)
                    if user:
                        emoji = "🏆" if user_id in winners else "📊"
                        score_lines.append(f"{emoji} {user.mention}: **{score:.1f} points**")
                
                embed.add_field(
                    name="📊 Final Scores",
                    value="\n".join(score_lines),
                    inline=False
                )
            
            # Handle payouts
            if game.solo and winners:
                # Solo mode: 2x entry fee for perfect, lose entry fee if not perfect
                winner_id = winners[0]
                is_perfect = perfect_matches.get(winner_id, False)
                
                if is_perfect:
                    # Perfect score: get 2x entry fee
                    winnings = game.entry_fee * 2
                    await self.db.set_winner(game.game_id, winner_id, winnings)
                    
                    try:
                        winner = await self.bot.fetch_user(winner_id)
                        await bank.deposit_credits(winner, winnings)
                    except Exception as e:
                        log.error(f"Error paying solo winner {winner_id}: {e}")
                    
                    winner_user = self.bot.get_user(winner_id)
                    embed.add_field(
                        name="🏆 Perfect Solo Victory!",
                        value=(
                            f"{winner_user.mention}\n"
                            f"Perfect match! You win: **{humanize_number(winnings)} credits** (2x entry fee)"
                        ),
                        inline=False
                    )
                else:
                    # Not perfect: lose entry fee
                    await self.db.set_winner(game.game_id, winner_id, 0)
                    
                    winner_user = self.bot.get_user(winner_id)
                    embed.add_field(
                        name="💔 Solo Game Complete",
                        value=(
                            f"{winner_user.mention}\n"
                            f"Not a perfect match - entry fee lost.\n"
                            f"💡 Get a perfect score next time to win 2x your entry fee!"
                        ),
                        inline=False
                    )
            elif winners:
                # Multiplayer mode: split pot among winners
                winnings_per_winner = game.total_pot // len(winners)
                
                for winner_id in winners:
                    await self.db.set_winner(game.game_id, winner_id, winnings_per_winner)
                    
                    try:
                        winner = await self.bot.fetch_user(winner_id)
                        await bank.deposit_credits(winner, winnings_per_winner)
                    except Exception as e:
                        log.error(f"Error paying winner {winner_id}: {e}")
                
                winner_mentions = []
                for winner_id in winners:
                    user = self.bot.get_user(winner_id)
                    if user:
                        winner_mentions.append(user.mention)
                
                embed.add_field(
                    name="💰 Winners",
                    value=(
                        f"{', '.join(winner_mentions)}\n"
                        f"Each wins: **{humanize_number(winnings_per_winner)} credits**"
                    ),
                    inline=False
                )
            
            # Show detailed breakdown for each player
            if len(game.players) <= 5:  # Only show details if not too many players
                for user_id in game.players:
                    answer = game.player_answers[user_id]
                    user = self.bot.get_user(user_id)
                    if user:
                        breakdown = format_score_breakdown(game.mission_requirements, answer.vehicles)
                        # Send as separate message to avoid embed size limits
                        await channel.send(
                            f"**Score Breakdown for {user.mention}:**\n{box(breakdown, lang='md')}"
                        )
            
            await channel.send(embed=embed)
            
            # Mark game as completed
            await self.db.end_game(game.game_id, status='completed')
            
            # Clean up
            self.game_manager.remove_game(game.channel_id)
            
        except Exception as e:
            log.error(f"Error ending round: {e}", exc_info=True)
    
    async def cancel_game(self, game: GameState, reason: str = "Game cancelled"):
        """Cancel a game and refund players."""
        try:
            channel = self.bot.get_channel(game.channel_id)
            
            # Unlock mission list for all players
            for user_id in game.players:
                await self.unlock_mission_list(game.guild_id, user_id)
                await self.db.remove_lockout(game.game_id, user_id)
            
            # Refund all players
            for user_id in game.players:
                try:
                    user = await self.bot.fetch_user(user_id)
                    await bank.deposit_credits(user, game.entry_fee)
                except Exception as e:
                    log.error(f"Error refunding user {user_id}: {e}")
            
            # Mark as cancelled in database
            await self.db.end_game(game.game_id, status='cancelled')
            
            # Notify channel
            if channel:
                await channel.send(
                    f"❌ {reason}. All entry fees have been refunded and mission list access restored."
                )
            
            # Clean up
            self.game_manager.remove_game(game.channel_id)
            
        except Exception as e:
            log.error(f"Error cancelling game: {e}", exc_info=True)
    
    @rapidresponse.command(name="notify")
    @commands.guild_only()
    async def rr_notify(self, ctx):
        """Toggle notifications for new Rapid Response games."""
        current = await self.db.get_notify_preference(ctx.guild.id, ctx.author.id)
        new_value = not current
        
        await self.db.set_notify_preference(ctx.guild.id, ctx.author.id, new_value)
        
        if new_value:
            await ctx.send(
                "✅ You will now be notified when new Rapid Response games start!\n"
                f"You'll receive a ping via the game notification role."
            )
        else:
            await ctx.send(
                "❌ You will no longer be notified about new Rapid Response games."
            )
    
    @rapidresponse.command(name="stats")
    @commands.guild_only()
    async def rr_stats(self, ctx, user: discord.Member = None):
        """View Rapid Response statistics."""
        user = user or ctx.author
        
        stats = await self.db.get_player_stats(user.id, ctx.guild.id)
        
        embed = discord.Embed(
            title=f"📊 Rapid Response Stats - {user.display_name}",
            color=discord.Color.blue()
        )
        
        embed.add_field(name="Games Played", value=humanize_number(stats['total_games']), inline=True)
        embed.add_field(name="Games Won", value=humanize_number(stats['total_wins']), inline=True)
        embed.add_field(name="Win Rate", value=f"{stats['win_rate']:.1f}%", inline=True)
        
        embed.add_field(name="Total Winnings", value=f"{humanize_number(stats['total_winnings'])} credits", inline=True)
        embed.add_field(name="Average Score", value=f"{stats['average_score']:.1f}", inline=True)
        embed.add_field(name="Perfect Rounds", value=humanize_number(stats['perfect_rounds']), inline=True)
        
        embed.set_thumbnail(url=user.display_avatar.url)
        
        await ctx.send(embed=embed)
    
    @rapidresponse.group(name="config")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def rr_config(self, ctx):
        """Configure Rapid Response settings."""
        pass
    
    @rr_config.command(name="view")
    async def rr_config_view(self, ctx):
        """View current configuration."""
        config = await self.config.guild(ctx.guild).all()
        
        embed = discord.Embed(
            title="⚙️ Rapid Response Configuration",
            color=discord.Color.blue()
        )
        
        embed.add_field(name="Enabled", value="✅ Yes" if config['enabled'] else "❌ No", inline=True)
        embed.add_field(name="Entry Fee", value=f"{humanize_number(config['entry_fee'])} credits", inline=True)
        embed.add_field(name="Lobby Duration", value=f"{config['lobby_duration']} seconds", inline=True)
        embed.add_field(name="Round Duration", value=f"{config['round_duration']} seconds", inline=True)
        
        # Mission list channel
        mission_channel = ctx.guild.get_channel(config['mission_list_channel_id'])
        embed.add_field(
            name="Mission List Channel",
            value=mission_channel.mention if mission_channel else "Not set",
            inline=True
        )
        
        # Notify role
        notify_role = ctx.guild.get_role(config['notify_role_id'])
        embed.add_field(
            name="Notify Role",
            value=notify_role.mention if notify_role else "Not set",
            inline=True
        )
        
        await ctx.send(embed=embed)
    
    @rr_config.command(name="entryfee")
    async def rr_config_entryfee(self, ctx, amount: int):
        """Set the entry fee for games."""
        if amount < 0:
            await ctx.send("❌ Entry fee must be 0 or greater.")
            return
        
        await self.config.guild(ctx.guild).entry_fee.set(amount)
        await ctx.send(f"✅ Entry fee set to {humanize_number(amount)} credits.")
    
    @rr_config.command(name="lobbytime")
    async def rr_config_lobbytime(self, ctx, seconds: int):
        """Set the lobby duration in seconds."""
        if seconds < 10 or seconds > 300:
            await ctx.send("❌ Lobby duration must be between 10 and 300 seconds.")
            return
        
        await self.config.guild(ctx.guild).lobby_duration.set(seconds)
        await ctx.send(f"✅ Lobby duration set to {seconds} seconds.")
    
    @rr_config.command(name="roundtime")
    async def rr_config_roundtime(self, ctx, seconds: int):
        """Set the round duration in seconds."""
        if seconds < 15 or seconds > 300:
            await ctx.send("❌ Round duration must be between 15 and 300 seconds.")
            return
        
        await self.config.guild(ctx.guild).round_duration.set(seconds)
        await ctx.send(f"✅ Round duration set to {seconds} seconds.")
    
    @rr_config.command(name="toggle")
    async def rr_config_toggle(self, ctx):
        """Enable or disable Rapid Response."""
        current = await self.config.guild(ctx.guild).enabled()
        await self.config.guild(ctx.guild).enabled.set(not current)
        
        status = "enabled" if not current else "disabled"
        await ctx.send(f"✅ Rapid Response {status}.")
    
    @rr_config.command(name="missionchannel")
    async def rr_config_missionchannel(self, ctx, channel: discord.TextChannel):
        """Set the mission list channel to lock during games."""
        await self.config.guild(ctx.guild).mission_list_channel_id.set(channel.id)
        await ctx.send(f"✅ Mission list channel set to {channel.mention}")
    
    @rr_config.command(name="notifyrole")
    async def rr_config_notifyrole(self, ctx, role: discord.Role):
        """Set the role to ping for game notifications."""
        await self.config.guild(ctx.guild).notify_role_id.set(role.id)
        await ctx.send(f"✅ Notify role set to {role.mention}")
    
    @rapidresponse.command(name="help")
    async def rr_help(self, ctx):
        """Show detailed help for Rapid Response."""
        embed = discord.Embed(
            title="🚒 Rapid Response - How to Play",
            description="Test your MissionChief knowledge by guessing vehicle requirements!",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="🎮 Starting a Game",
            value=f"Use `{ctx.prefix}rapidresponse start` to create a lobby. Players have time to join before the game starts.",
            inline=False
        )
        
        embed.add_field(
            name="📝 How to Answer",
            value=(
                "Type your vehicle guess using short codes or full names:\n"
                "• **Short codes**: `FT2 BC1 HR1`\n"
                "• **Full names**: `2 fire trucks, 1 chief, 1 heavy rescue`\n"
                "• **Mixed**: `FT2, battalion chief 1, heavy rescue 1`\n\n"
                "You can send multiple messages - they accumulate!"
            ),
            inline=False
        )
        
        embed.add_field(
            name="🏆 Scoring System",
            value=(
                "• **+2 points** for using a required vehicle type\n"
                "• **+1 point** per correctly matched vehicle\n"
                "• **-0.5 points** per over-deployed vehicle\n"
                "• **-1 point** per unnecessary vehicle type\n"
                "• **+4 bonus** for perfect match!"
            ),
            inline=False
        )
        
        embed.add_field(
            name="🚗 Common Vehicle Codes",
            value=(
                "`FT` = Fire Truck\n"
                "`BC` = Battalion Chief\n"
                "`PT` = Platform Truck (Ladder)\n"
                "`HR` = Heavy Rescue\n"
                "`MCV` = Mobile Command\n"
                "`MAV` = Mobile Air\n"
                "`WT` = Water Tanker\n"
                "`HM` = Hazmat\n"
                "`PC` = Police Car\n"
                "`AMB` = Ambulance"
            ),
            inline=False
        )
        
        embed.add_field(
            name="📊 Commands",
            value=(
                f"`{ctx.prefix}rr start` - Start a new game\n"
                f"`{ctx.prefix}rr notify` - Toggle game start notifications\n"
                f"`{ctx.prefix}rr stats [@user]` - View statistics\n"
                f"`{ctx.prefix}rr config` - Admin settings"
            ),
            inline=False
        )
        
        embed.add_field(
            name="🎮 Solo Mode",
            value=(
                "Play alone to practice!\n"
                "🏆 **Perfect match** = Win 2x your entry fee\n"
                "💔 **Not perfect** = Lose your entry fee\n"
                "High risk, high reward!"
            ),
            inline=False
        )
        
        embed.add_field(
            name="🔒 Mission List Lockout",
            value=(
                "When you join a game, you temporarily lose access to the mission list channel.\n"
                "This prevents cheating! Access is restored when the game ends."
            ),
            inline=False
        )
        
        await ctx.send(embed=embed)
