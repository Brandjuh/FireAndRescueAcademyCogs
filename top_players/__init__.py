
from .top_players import TopPlayers

async def setup(bot):
    await bot.add_cog(TopPlayers(bot))
