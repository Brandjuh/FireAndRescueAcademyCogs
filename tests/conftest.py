import sys
import types


class _Decorator:
    def __call__(self, function):
        return function


class _CommandDecorator:
    def __call__(self, *args, **kwargs):
        del args, kwargs
        return _Decorator()


def pytest_configure():
    """Provide the minimal Redbot import surface required by isolated cog tests."""
    discord = types.ModuleType("discord")
    discord.File = lambda path, filename=None: types.SimpleNamespace(
        path=path,
        filename=filename,
    )
    redbot = types.ModuleType("redbot")
    redbot_core = types.ModuleType("redbot.core")
    commands = types.ModuleType("redbot.core.commands")

    commands.Cog = object
    commands.command = _CommandDecorator()
    commands.is_owner = _CommandDecorator()

    redbot_core.commands = commands
    redbot_core.Config = object
    redbot_core.data_manager = object
    redbot.core = redbot_core

    sys.modules.setdefault("discord", discord)
    sys.modules.setdefault("redbot", redbot)
    sys.modules.setdefault("redbot.core", redbot_core)
    sys.modules.setdefault("redbot.core.commands", commands)
