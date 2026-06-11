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


def pytest_configure():
    """Provide the minimal Redbot import surface required by isolated cog tests."""
    discord = types.ModuleType("discord")
    discord.File = lambda path, filename=None: types.SimpleNamespace(
        path=path,
        filename=filename,
    )
    discord.TextChannel = object
    redbot = types.ModuleType("redbot")
    redbot_core = types.ModuleType("redbot.core")
    commands = types.ModuleType("redbot.core.commands")
    checks = types.ModuleType("redbot.core.checks")

    commands.Cog = object
    commands.command = _CommandDecorator()
    commands.group = _GroupDecorator()
    commands.is_owner = _CommandDecorator()
    commands.admin_or_permissions = _CommandDecorator()

    redbot_core.commands = commands
    redbot_core.checks = checks
    redbot_core.Config = object
    redbot_core.data_manager = object
    redbot.core = redbot_core

    sys.modules.setdefault("discord", discord)
    sys.modules.setdefault("redbot", redbot)
    sys.modules.setdefault("redbot.core", redbot_core)
    sys.modules.setdefault("redbot.core.commands", commands)
    sys.modules.setdefault("redbot.core.checks", checks)
