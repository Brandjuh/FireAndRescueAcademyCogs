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


class _Cog:
    @classmethod
    def listener(cls, *args, **kwargs):
        del args, kwargs
        return _Decorator()


class _View:
    def __init__(self, *args, **kwargs):
        del args, kwargs
        self.children = []

    def add_item(self, item):
        self.children.append(item)

    def clear_items(self):
        self.children.clear()


class _Button:
    def __init__(self, *args, **kwargs):
        del args
        self.disabled = False
        for key, value in kwargs.items():
            setattr(self, key, value)


class _Select(_Button):
    pass


class _Modal:
    def __init_subclass__(cls, **kwargs):
        del kwargs
        return super().__init_subclass__()

    def __init__(self, *args, **kwargs):
        del args, kwargs


class _TextInput:
    def __init__(self, *args, **kwargs):
        del args
        self.value = ""
        for key, value in kwargs.items():
            setattr(self, key, value)


def _ui_button(*args, **kwargs):
    del args, kwargs
    return lambda function: function


class _ContextMenu:
    def __init__(self, *, name, callback):
        self.name = name
        self.callback = callback


class _Object:
    def __init__(self, *, id):
        self.id = id


class _SelectOption:
    def __init__(self, *args, **kwargs):
        del args
        for key, value in kwargs.items():
            setattr(self, key, value)


def pytest_configure():
    """Provide the minimal Redbot import surface required by isolated cog tests."""
    class _Embed:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.fields = []
            self.footer = None

        def add_field(self, **kwargs):
            self.fields.append(kwargs)

        def set_footer(self, **kwargs):
            self.footer = kwargs

    discord = types.ModuleType("discord")
    discord.File = lambda path, filename=None: types.SimpleNamespace(
        path=path,
        filename=filename,
    )
    discord.TextChannel = object
    discord.Role = object
    discord.Member = object
    discord.Guild = object
    discord.Message = object
    discord.Interaction = object
    discord.Object = _Object
    discord.SelectOption = _SelectOption
    discord.Embed = _Embed
    discord.Color = types.SimpleNamespace(
        blue=lambda: "blue",
        dark_blue=lambda: "dark_blue",
        dark_gray=lambda: "dark_gray",
        dark_gold=lambda: "dark_gold",
        gold=lambda: "gold",
        green=lambda: "green",
        orange=lambda: "orange",
        purple=lambda: "purple",
        red=lambda: "red",
    )
    discord.ButtonStyle = types.SimpleNamespace(
        primary="primary",
        secondary="secondary",
        success="success",
        danger="danger",
    )
    discord.TextStyle = types.SimpleNamespace(
        short="short",
        paragraph="paragraph",
    )
    discord.AppCommandType = types.SimpleNamespace(user="user")
    discord.NotFound = type("NotFound", (Exception,), {})
    discord.HTTPException = type("HTTPException", (Exception,), {})
    discord.Forbidden = type("Forbidden", (Exception,), {})
    discord.errors = types.SimpleNamespace(
        InteractionResponded=type("InteractionResponded", (Exception,), {})
    )
    discord.ui = types.SimpleNamespace(
        View=_View,
        Button=_Button,
        Select=_Select,
        Modal=_Modal,
        TextInput=_TextInput,
        button=_ui_button,
    )
    app_commands = types.ModuleType("discord.app_commands")
    app_commands.ContextMenu = _ContextMenu
    discord.app_commands = app_commands
    redbot = types.ModuleType("redbot")
    redbot_core = types.ModuleType("redbot.core")
    redbot_core_bot = types.ModuleType("redbot.core.bot")
    redbot_core_data_manager = types.ModuleType("redbot.core.data_manager")
    redbot_core_utils = types.ModuleType("redbot.core.utils")
    chat_formatting = types.ModuleType("redbot.core.utils.chat_formatting")
    commands = types.ModuleType("redbot.core.commands")
    checks = types.ModuleType("redbot.core.checks")

    commands.Cog = _Cog
    commands.Context = object
    commands.command = _CommandDecorator()
    commands.group = _GroupDecorator()
    commands.hybrid_group = _GroupDecorator()
    commands.hybrid_command = _CommandDecorator()
    commands.guild_only = _CommandDecorator()
    commands.admin = _CommandDecorator()
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
    sys.modules.setdefault("discord.app_commands", app_commands)
    sys.modules.setdefault("redbot", redbot)
    sys.modules.setdefault("redbot.core", redbot_core)
    sys.modules.setdefault("redbot.core.commands", commands)
    sys.modules.setdefault("redbot.core.checks", checks)
    sys.modules.setdefault("redbot.core.bot", redbot_core_bot)
    sys.modules.setdefault("redbot.core.data_manager", redbot_core_data_manager)
    sys.modules.setdefault("redbot.core.utils", redbot_core_utils)
    sys.modules.setdefault("redbot.core.utils.chat_formatting", chat_formatting)
