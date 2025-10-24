from .asset_cog import AssetManager

async def setup(bot):
    await bot.add_cog(AssetManager(bot))
