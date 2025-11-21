"""
Discord UI views for RapidResponse
Author: BrandjuhNL
"""

import discord
from typing import Optional
import logging

log = logging.getLogger("red.rapidresponse.views")


class LobbyView(discord.ui.View):
    """View for the game lobby with Join/Leave/Start buttons."""
    
    def __init__(self, cog, channel_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.channel_id = channel_id
    
    @discord.ui.button(label="Join Game", style=discord.ButtonStyle.green, emoji="✅")
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handle join button click."""
        await interaction.response.defer(ephemeral=True)
        
        game = self.cog.game_manager.get_game(self.channel_id)
        if not game or game.status != 'lobby':
            await interaction.followup.send("This game is no longer accepting players.", ephemeral=True)
            return
        
        # Check if already in game
        if interaction.user.id in game.players:
            await interaction.followup.send("You're already in this game!", ephemeral=True)
            return
        
        # Check if user has enough credits
        try:
            balance = await self.cog.bank.get_balance(interaction.user)
            if balance < game.entry_fee:
                await interaction.followup.send(
                    f"You need {game.entry_fee} credits to join. You have {balance}.",
                    ephemeral=True
                )
                return
            
            # Charge entry fee
            await self.cog.bank.withdraw_credits(interaction.user, game.entry_fee)
            
            # Add player
            game.add_player(interaction.user.id)
            await self.cog.db.add_player(game.game_id, interaction.user.id, game.entry_fee)
            
            await interaction.followup.send(
                f"✅ You've joined the game! Entry fee: {game.entry_fee} credits",
                ephemeral=True
            )
            
            # Update lobby embed
            await self.cog.update_lobby_embed(game)
            
        except Exception as e:
            log.error(f"Error joining game: {e}", exc_info=True)
            await interaction.followup.send(
                "An error occurred while joining the game.",
                ephemeral=True
            )
    
    @discord.ui.button(label="Leave Game", style=discord.ButtonStyle.red, emoji="❌")
    async def leave_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handle leave button click."""
        await interaction.response.defer(ephemeral=True)
        
        game = self.cog.game_manager.get_game(self.channel_id)
        if not game or game.status != 'lobby':
            await interaction.followup.send("Cannot leave this game.", ephemeral=True)
            return
        
        # Check if in game
        if interaction.user.id not in game.players:
            await interaction.followup.send("You're not in this game!", ephemeral=True)
            return
        
        try:
            # Refund entry fee
            await self.cog.bank.deposit_credits(interaction.user, game.entry_fee)
            
            # Remove player
            game.remove_player(interaction.user.id)
            
            await interaction.followup.send(
                f"You've left the game. Refunded {game.entry_fee} credits.",
                ephemeral=True
            )
            
            # Update lobby embed
            await self.cog.update_lobby_embed(game)
            
        except Exception as e:
            log.error(f"Error leaving game: {e}", exc_info=True)
            await interaction.followup.send(
                "An error occurred while leaving the game.",
                ephemeral=True
            )
    
    @discord.ui.button(label="Start Now", style=discord.ButtonStyle.blurple, emoji="🚀")
    async def start_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handle start now button click."""
        await interaction.response.defer(ephemeral=True)
        
        game = self.cog.game_manager.get_game(self.channel_id)
        if not game or game.status != 'lobby':
            await interaction.followup.send("Cannot start this game.", ephemeral=True)
            return
        
        # Only lobby creator can start (for now, just check if they're in the game)
        if interaction.user.id not in game.players:
            await interaction.followup.send("Only players can start the game!", ephemeral=True)
            return
        
        if len(game.players) == 0:
            await interaction.followup.send("No players in the game!", ephemeral=True)
            return
        
        await interaction.followup.send("Starting game now...", ephemeral=True)
        
        # Cancel lobby timer and start game
        if game.lobby_task and not game.lobby_task.done():
            game.lobby_task.cancel()
        
        await self.cog.start_game(game)
    
    @discord.ui.button(label="Cancel Game", style=discord.ButtonStyle.gray, emoji="🚫")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Handle cancel button click."""
        await interaction.response.defer(ephemeral=True)
        
        game = self.cog.game_manager.get_game(self.channel_id)
        if not game or game.status != 'lobby':
            await interaction.followup.send("Cannot cancel this game.", ephemeral=True)
            return
        
        # Only lobby creator can cancel (for now, just check if they're in the game)
        if interaction.user.id not in game.players:
            await interaction.followup.send("Only players can cancel the game!", ephemeral=True)
            return
        
        await interaction.followup.send("Cancelling game...", ephemeral=True)
        
        # Cancel and refund
        await self.cog.cancel_game(game, reason="Cancelled by player")
