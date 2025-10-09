import discord
from abc import ABC, abstractmethod
from typing import List, Optional, Type, Union
from datetime import datetime
from redbot.core import commands, bank


class BaseMinigameCog(commands.Cog):
    @abstractmethod
    async def base_minigame_cmd(self, game_cls: Type["Minigame"], ctx: Union[commands.Context, discord.Interaction], players: List[discord.Member], against_bot: bool):
        pass
        

class Minigame(ABC):
    def __init__(self, cog: BaseMinigameCog, players: List[discord.Member], channel: discord.TextChannel, bet_amount: int = 0):
        self.cog = cog
        self.players = players
        self.channel = channel
        self.message: Optional[discord.Message] = None
        self.last_interacted: datetime = datetime.now()
        self.bet_amount = bet_amount
        self.bets_placed = False

    @abstractmethod
    def is_finished(self) -> bool:
        pass

    @abstractmethod
    def is_cancelled(self) -> bool:
        pass

    @abstractmethod
    def cancel(self, player: Optional[discord.Member]) -> None:
        pass

    @abstractmethod
    def accept(self, player: discord.Member) -> None:
        pass

    @abstractmethod
    def get_content(self) -> Optional[str]:
        pass

    @abstractmethod
    def get_embed(self) -> discord.Embed:
        pass

    @abstractmethod
    def get_view(self) -> discord.ui.View:
        pass

    async def place_bets(self) -> tuple[bool, Optional[str]]:
        """
        Place bets for all players.
        Returns: (success, error_message)
        """
        if self.bets_placed or self.bet_amount == 0:
            return True, None

        currency_name = await bank.get_currency_name(self.channel.guild)
        
        for player in self.players:
            if player.bot:
                continue
            
            try:
                balance = await bank.get_balance(player)
                if balance < self.bet_amount:
                    return False, f"{player.mention} doesn't have enough {currency_name}! (Need: {self.bet_amount}, Have: {balance})"
                
                await bank.withdraw_credits(player, self.bet_amount)
            except Exception as e:
                return False, f"Error processing bet for {player.mention}: {str(e)}"
        
        self.bets_placed = True
        return True, None

    async def refund_bets(self):
        """Refund bets to all players (used when game is cancelled before starting)"""
        if not self.bets_placed or self.bet_amount == 0:
            return

        for player in self.players:
            if player.bot:
                continue
            try:
                await bank.deposit_credits(player, self.bet_amount)
            except Exception:
                pass

    async def payout_winner(self, winner: discord.Member, win_amount: int):
        """Pay out the winner"""
        if winner.bot or win_amount == 0:
            return

        try:
            await bank.deposit_credits(winner, win_amount)
        except Exception:
            pass
