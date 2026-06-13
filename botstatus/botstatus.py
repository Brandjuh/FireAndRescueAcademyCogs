from __future__ import annotations

import asyncio
from typing import Optional

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red


DEFAULT_GUILD = {
    "enabled": True,
    "idle_type": "watching",
    "idle_text": "Fire And Rescue Academy",
    "command_tracking": False,
}


ACTIVITY_TYPES = {
    "playing": "playing",
    "listening": "listening",
    "watching": "watching",
    "competing": "competing",
}


def clean_activity_type(value: str) -> str:
    cleaned = (value or "").strip().lower()
    if cleaned not in ACTIVITY_TYPES:
        raise ValueError("invalid activity type")
    return cleaned


class BotStatus(commands.Cog):
    """Keep the bot presence aligned with manual or command activity."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFACA_B001, force_registration=True)
        self.config.register_guild(**DEFAULT_GUILD)
        self._restore_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        self._restore_task = asyncio.create_task(self.restore_idle_status())

    def cog_unload(self):
        if self._restore_task:
            self._restore_task.cancel()

    async def restore_idle_status(self):
        await self.bot.wait_until_red_ready()
        for guild in self.bot.guilds:
            cfg = await self.config.guild(guild).all()
            if cfg.get("enabled") and cfg.get("idle_text"):
                await self.set_presence(cfg["idle_type"], cfg["idle_text"])
                return

    async def set_presence(self, activity_type: str, text: str):
        cleaned_type = clean_activity_type(activity_type)
        activity_enum = getattr(discord.ActivityType, ACTIVITY_TYPES[cleaned_type])
        await self.bot.change_presence(
            activity=discord.Activity(type=activity_enum, name=text[:128])
        )

    async def restore_for_guild(self, guild: discord.Guild):
        cfg = await self.config.guild(guild).all()
        if cfg.get("enabled") and cfg.get("idle_text"):
            await self.set_presence(cfg["idle_type"], cfg["idle_text"])

    @commands.Cog.listener()
    async def on_command(self, ctx: commands.Context):
        if not ctx.guild:
            return
        cfg = await self.config.guild(ctx.guild).all()
        if not cfg.get("enabled") or not cfg.get("command_tracking"):
            return
        command_name = ctx.command.qualified_name if ctx.command else "command"
        await self.set_presence("watching", f"{command_name} run")

    @commands.Cog.listener()
    async def on_command_completion(self, ctx: commands.Context):
        if ctx.guild:
            await self.restore_for_guild(ctx.guild)

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: Exception):
        del error
        if ctx.guild:
            await self.restore_for_guild(ctx.guild)

    @commands.group(name="botstatusset")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def botstatusset(self, ctx: commands.Context):
        """Configure bot presence status."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @botstatusset.command(name="idle")
    async def botstatusset_idle(self, ctx: commands.Context, activity_type: str, *, text: str):
        """Set idle status. Type: playing, listening, watching, competing."""
        try:
            cleaned_type = clean_activity_type(activity_type)
        except ValueError:
            await ctx.send("Type must be: playing, listening, watching, competing.")
            return
        await self.config.guild(ctx.guild).idle_type.set(cleaned_type)
        await self.config.guild(ctx.guild).idle_text.set(text[:128])
        await self.config.guild(ctx.guild).enabled.set(True)
        await self.set_presence(cleaned_type, text[:128])
        await ctx.send("Bot idle status updated.")

    @botstatusset.command(name="commandtracking")
    async def botstatusset_commandtracking(self, ctx: commands.Context, enabled: bool):
        """Enable or disable temporary status while commands run."""
        await self.config.guild(ctx.guild).command_tracking.set(enabled)
        await ctx.send(f"Command status tracking {'enabled' if enabled else 'disabled'}.")

    @botstatusset.command(name="enable")
    async def botstatusset_enable(self, ctx: commands.Context):
        """Enable BotStatus presence management."""
        await self.config.guild(ctx.guild).enabled.set(True)
        await self.restore_for_guild(ctx.guild)
        await ctx.send("BotStatus enabled.")

    @botstatusset.command(name="disable")
    async def botstatusset_disable(self, ctx: commands.Context):
        """Disable BotStatus presence management and clear activity."""
        await self.config.guild(ctx.guild).enabled.set(False)
        await self.bot.change_presence(activity=None)
        await ctx.send("BotStatus disabled.")

    @commands.command(name="botstatus")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def botstatus(self, ctx: commands.Context):
        """Show BotStatus configuration."""
        cfg = await self.config.guild(ctx.guild).all()
        await ctx.send(
            "\n".join(
                [
                    f"Enabled: {cfg['enabled']}",
                    f"Idle: {cfg['idle_type']} {cfg['idle_text']}",
                    f"Command tracking: {cfg['command_tracking']}",
                ]
            )
        )
