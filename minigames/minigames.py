import logging
import discord
from typing import Dict, List, Optional, Type, Union
from datetime import datetime, timedelta
from redbot.core import commands, Config, bank
from redbot.core.bot import Red

from minigames.base import Minigame, BaseMinigameCog
from minigames.connect4 import ConnectFourGame
from minigames.tictactoe import TicTacToeGame
from minigames.views.replace_view import ReplaceView

log = logging.getLogger("red.crab-cogs.minigames")

TIME_LIMIT = 5  # minutes
COOLDOWN_TIME = 5  # seconds between games per player


class Minigames(BaseMinigameCog):
    """Play Connect 4 and Tic-Tac-Toe against your friends or the bot with economy integration."""

    def __init__(self, bot: Red):
        super().__init__()
        self.bot = bot
        self.games: Dict[int, Minigame] = {}
        self.config = Config.get_conf(self, identifier=7669699621, force_registration=True)
        
        default_guild = {
            "bet_amount": 100,
            "win_amount": 500,
        }
        
        default_member = {
            "total_games": 0,
            "total_wins": 0,
            "total_losses": 0,
            "total_ties": 0,
            "total_earnings": 0,
            "last_game": None,
        }
        
        self.config.register_guild(**default_guild)
        self.config.register_member(**default_member)

    @commands.group(name="minigameset", aliases=["mgset"])
    @commands.guild_only()
    @commands.admin_or_permissions(manage_guild=True)
    async def minigameset(self, ctx: commands.Context):
        """Configure minigame settings"""
        if ctx.invoked_subcommand is None:
            bet = await self.config.guild(ctx.guild).bet_amount()
            win = await self.config.guild(ctx.guild).win_amount()
            currency = await bank.get_currency_name(ctx.guild)
            
            embed = discord.Embed(
                title="⚙️ Minigames Configuration",
                color=await ctx.embed_color()
            )
            embed.add_field(name="Bet Amount", value=f"{bet} {currency}", inline=True)
            embed.add_field(name="Win Amount", value=f"{win} {currency}", inline=True)
            embed.set_footer(text=f"Use {ctx.prefix}help minigameset to see available commands")
            
            await ctx.send(embed=embed)

    @minigameset.command(name="bet")
    async def set_bet(self, ctx: commands.Context, amount: int):
        """Set the bet amount for starting a game"""
        if amount < 0:
            return await ctx.send("Bet amount must be 0 or positive!")
        
        await self.config.guild(ctx.guild).bet_amount.set(amount)
        currency = await bank.get_currency_name(ctx.guild)
        await ctx.send(f"✅ Bet amount set to {amount} {currency}")

    @minigameset.command(name="win")
    async def set_win(self, ctx: commands.Context, amount: int):
        """Set the win amount for winning a game"""
        if amount < 0:
            return await ctx.send("Win amount must be 0 or positive!")
        
        await self.config.guild(ctx.guild).win_amount.set(amount)
        currency = await bank.get_currency_name(ctx.guild)
        await ctx.send(f"✅ Win amount set to {amount} {currency}")

    @commands.hybrid_command(name="tictactoe", aliases=["ttt"])
    @commands.guild_only()
    @commands.cooldown(1, COOLDOWN_TIME, commands.BucketType.user)
    async def tictactoe(self, ctx: commands.Context, opponent: Optional[discord.Member] = None):
        """
        Play a game of Tic-Tac-Toe against the bot or another user.
        """
        assert ctx.guild and isinstance(ctx.author, discord.Member) and isinstance(ctx.channel, discord.TextChannel)
        opponent = opponent or ctx.guild.me
        players = [ctx.author, opponent] if opponent.bot else [opponent, ctx.author]
        await self.base_minigame_cmd(TicTacToeGame, ctx, players, opponent.bot)

    @commands.hybrid_command(name="connect4", aliases=["c4"])
    @commands.guild_only()
    @commands.cooldown(1, COOLDOWN_TIME, commands.BucketType.user)
    async def connectfour(self, ctx: commands.Context, opponent: Optional[discord.Member] = None):
        """
        Play a game of Connect 4 against the bot or another user.
        """
        assert ctx.guild and isinstance(ctx.author, discord.Member) and isinstance(ctx.channel, discord.TextChannel)
        opponent = opponent or ctx.guild.me
        players = [ctx.author, opponent] if opponent.bot else [opponent, ctx.author]
        await self.base_minigame_cmd(ConnectFourGame, ctx, players, opponent.bot)

    @commands.hybrid_command(name="gamestats", aliases=["gstats"])
    @commands.guild_only()
    async def gamestats(self, ctx: commands.Context, member: Optional[discord.Member] = None):
        """View your or someone else's minigame statistics"""
        member = member or ctx.author
        assert isinstance(member, discord.Member)
        
        stats = await self.config.member(member).all()
        currency = await bank.get_currency_name(ctx.guild)
        
        total_games = stats["total_games"]
        wins = stats["total_wins"]
        losses = stats["total_losses"]
        ties = stats["total_ties"]
        earnings = stats["total_earnings"]
        
        win_rate = (wins / total_games * 100) if total_games > 0 else 0
        
        embed = discord.Embed(
            title=f"🎮 {member.display_name}'s Game Statistics",
            color=await ctx.embed_color()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        
        embed.add_field(name="📊 Games Played", value=str(total_games), inline=True)
        embed.add_field(name="🏆 Wins", value=str(wins), inline=True)
        embed.add_field(name="💀 Losses", value=str(losses), inline=True)
        embed.add_field(name="🤝 Ties", value=str(ties), inline=True)
        embed.add_field(name="📈 Win Rate", value=f"{win_rate:.1f}%", inline=True)
        embed.add_field(name="💰 Total Earnings", value=f"{earnings:+,} {currency}", inline=True)
        
        await ctx.send(embed=embed)

    @commands.hybrid_command(name="gameleaderboard", aliases=["glb", "gtop"])
    @commands.guild_only()
    async def gameleaderboard(self, ctx: commands.Context, sort_by: str = "wins"):
        """
        View the minigame leaderboard
        
        Sort options: wins, earnings, games, winrate
        """
        sort_by = sort_by.lower()
        valid_sorts = ["wins", "earnings", "games", "winrate"]
        
        if sort_by not in valid_sorts:
            return await ctx.send(f"Invalid sort option! Choose from: {', '.join(valid_sorts)}")
        
        all_members = await self.config.all_members(ctx.guild)
        currency = await bank.get_currency_name(ctx.guild)
        
        # Filter and sort members
        leaderboard = []
        for member_id, stats in all_members.items():
            if stats["total_games"] == 0:
                continue
            
            member = ctx.guild.get_member(member_id)
            if not member:
                continue
            
            win_rate = (stats["total_wins"] / stats["total_games"] * 100) if stats["total_games"] > 0 else 0
            
            leaderboard.append({
                "member": member,
                "wins": stats["total_wins"],
                "earnings": stats["total_earnings"],
                "games": stats["total_games"],
                "winrate": win_rate
            })
        
        if not leaderboard:
            return await ctx.send("No one has played any games yet!")
        
        # Sort
        leaderboard.sort(key=lambda x: x[sort_by], reverse=True)
        
        # Create embed
        sort_names = {
            "wins": "🏆 Most Wins",
            "earnings": "💰 Highest Earnings",
            "games": "🎮 Most Games",
            "winrate": "📈 Highest Win Rate"
        }
        
        embed = discord.Embed(
            title=f"{sort_names[sort_by]} Leaderboard",
            color=await ctx.embed_color()
        )
        
        description = ""
        for idx, entry in enumerate(leaderboard[:10], 1):
            medal = "🥇" if idx == 1 else "🥈" if idx == 2 else "🥉" if idx == 3 else f"`{idx}.`"
            
            if sort_by == "wins":
                value = f"{entry['wins']} wins"
            elif sort_by == "earnings":
                value = f"{entry['earnings']:+,} {currency}"
            elif sort_by == "games":
                value = f"{entry['games']} games"
            else:  # winrate
                value = f"{entry['winrate']:.1f}%"
            
            description += f"{medal} **{entry['member'].display_name}** - {value}\n"
        
        embed.description = description
        embed.set_footer(text=f"Showing top {min(10, len(leaderboard))} players")
        
        await ctx.send(embed=embed)

    async def base_minigame_cmd(self,
                                game_cls: Type[Minigame],
                                ctx: Union[commands.Context, discord.Interaction],
                                players: List[discord.Member],
                                against_bot: bool,
                                ):
        author = ctx.author if isinstance(ctx, commands.Context) else ctx.user
        reply = ctx.reply if isinstance(ctx, commands.Context) else ctx.response.send_message
        assert ctx.guild and isinstance(ctx.channel, discord.TextChannel) and isinstance(author, discord.Member)
        
        # Get bet and win amounts
        bet_amount = await self.config.guild(ctx.guild).bet_amount()
        win_amount = await self.config.guild(ctx.guild).win_amount()
        currency = await bank.get_currency_name(ctx.guild)
        
        # Check if player has enough balance
        if not against_bot:
            for player in players:
                if not player.bot:
                    balance = await bank.get_balance(player)
                    if balance < bet_amount:
                        return await reply(f"{player.mention} doesn't have enough {currency}! (Need: {bet_amount}, Have: {balance})", ephemeral=True)
        else:
            balance = await bank.get_balance(author)
            if balance < bet_amount:
                return await reply(f"You don't have enough {currency}! (Need: {bet_amount}, Have: {balance})", ephemeral=True)
        
        # Game already exists
        if ctx.channel.id in self.games and not self.games[ctx.channel.id].is_finished():
            old_game = self.games[ctx.channel.id]
            old_message = await ctx.channel.fetch_message(old_game.message.id) if old_game.message else None
            
            if old_message:
                minutes_passed = int((datetime.now() - old_game.last_interacted).total_seconds() // 60)
                if minutes_passed >= TIME_LIMIT:
                    async def callback():
                        nonlocal ctx, players, old_game, against_bot
                        assert isinstance(author, discord.Member) and isinstance(ctx.channel, discord.TextChannel)
                        game = game_cls(self, players, ctx.channel, bet_amount)
                        game.win_amount = win_amount
                        if against_bot:
                            game.accept(author)
                            success, error = await game.place_bets()
                            if not success:
                                return await ctx.channel.send(error)
                        self.games[ctx.channel.id] = game
                        message = await ctx.channel.send(content=game.get_content(), embed=game.get_embed(), view=game.get_view())
                        game.message = message
                        if old_game.message:
                            try:
                                await old_game.message.delete()
                            except discord.NotFound:
                                pass

                    content = f"Someone else is playing a game in this channel, here: {old_message.jump_url}, but {minutes_passed} minutes have passed since their last interaction. Do you want to start a new game?"
                    embed = discord.Embed(title="Confirmation", description=content, color=await self.bot.get_embed_color(ctx.channel))
                    view = ReplaceView(self, callback, author)
                    message = await reply(embed=embed, view=view, ephemeral=True)
                    view.message = message if isinstance(ctx, commands.Context) else await ctx.original_response()
                    return
                
                else:
                    content = f"There is still an active game in this channel, here: {old_message.jump_url}\nTry again in a few minutes"
                    permissions = ctx.channel.permissions_for(author)
                    content += " or consider creating a thread." if permissions.create_public_threads or permissions.create_private_threads else "."
                    await reply(content, ephemeral=True)
                    return
        
        # New game
        game = game_cls(self, players, ctx.channel, bet_amount)
        game.win_amount = win_amount
        if against_bot:
            game.accept(author)
            success, error = await game.place_bets()
            if not success:
                return await reply(error, ephemeral=True)
        
        self.games[ctx.channel.id] = game
        message = await reply(content=game.get_content(), embed=game.get_embed(), view=game.get_view())
        game.message = message if isinstance(ctx, commands.Context) else await ctx.original_response()

    async def record_game_result(self, game: Minigame, winner: Optional[discord.Member]):
        """Record game statistics for all players"""
        if not hasattr(game, 'time') or game.time == 0:
            return
        
        is_tie = winner is None
        
        for player in game.players:
            if player.bot:
                continue
            
            stats = await self.config.member(player).all()
            
            stats["total_games"] += 1
            stats["last_game"] = datetime.now().isoformat()
            
            if is_tie:
                stats["total_ties"] += 1
            elif player == winner:
                stats["total_wins"] += 1
                stats["total_earnings"] += game.win_amount - game.bet_amount
            else:
                stats["total_losses"] += 1
                stats["total_earnings"] -= game.bet_amount
            
            # Save the updated stats
            await self.config.member(player).set(stats)
