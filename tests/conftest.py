import sys
import types


class _Decorator:
    def __call__(self, function):
        return function


class _CommandDecorator:
    def __call__(self, *args, **kwargs):
        del args, kwargs
        return _Decorator()


class _Group:
    def __init__(self, function):
        self.function = function

    def __call__(self, *args, **kwargs):
        return self.function(*args, **kwargs)

    command = _CommandDecorator()


class _GroupDecorator:
    def __call__(self, *args, **kwargs):
        del args, kwargs
        return lambda function: _Group(function)


_Group.group = _GroupDecorator()


def pytest_configure():
    """Provide the minimal Redbot import surface required by isolated cog tests."""
    discord = types.ModuleType("discord")
    discord.File = lambda path, filename=None: types.SimpleNamespace(
        path=path,
        filename=filename,
    )
    discord.TextChannel = object
    discord.Role = object
    redbot = types.ModuleType("redbot")
    redbot_core = types.ModuleType("redbot.core")
    redbot_core_bot = types.ModuleType("redbot.core.bot")
    redbot_core_data_manager = types.ModuleType("redbot.core.data_manager")
    redbot_core_utils = types.ModuleType("redbot.core.utils")
    chat_formatting = types.ModuleType("redbot.core.utils.chat_formatting")
    commands = types.ModuleType("redbot.core.commands")
    checks = types.ModuleType("redbot.core.checks")

    commands.Cog = object
    commands.Context = object
    commands.command = _CommandDecorator()
    commands.group = _GroupDecorator()
    commands.is_owner = _CommandDecorator()
    commands.admin_or_permissions = _CommandDecorator()
    checks.is_owner = _CommandDecorator()

    redbot_core.commands = commands
    redbot_core.checks = checks
    redbot_core.Config = object
    redbot_core.data_manager = object
    redbot_core_bot.Red = object
    redbot_core_data_manager.cog_data_path = lambda *args, **kwargs: None
    chat_formatting.box = lambda value, **kwargs: value
    chat_formatting.pagify = lambda value, **kwargs: [value]
    redbot.core = redbot_core

    sys.modules.setdefault("discord", discord)
    sys.modules.setdefault("redbot", redbot)
    sys.modules.setdefault("redbot.core", redbot_core)
    sys.modules.setdefault("redbot.core.commands", commands)
    sys.modules.setdefault("redbot.core.checks", checks)
    sys.modules.setdefault("redbot.core.bot", redbot_core_bot)
    sys.modules.setdefault("redbot.core.data_manager", redbot_core_data_manager)
    sys.modules.setdefault("redbot.core.utils", redbot_core_utils)
    sys.modules.setdefault("redbot.core.utils.chat_formatting", chat_formatting)
