from .trainings_manager import TrainingManager

async def setup(bot):
    await bot.add_cog(TrainingManager(bot))