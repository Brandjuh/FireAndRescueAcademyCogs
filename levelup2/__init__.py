from ._compat import ensure_typing_extensions_sentinel

ensure_typing_extensions_sentinel()

from redbot.core.bot import Red  # noqa: E402
from redbot.core.utils import get_end_user_data_statement  # noqa: E402

from .main import LevelUp  # noqa: E402

__red_end_user_data_statement__ = get_end_user_data_statement(__file__)


async def setup(bot: Red):
    await bot.add_cog(LevelUp(bot))
