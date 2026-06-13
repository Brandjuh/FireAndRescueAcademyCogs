from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import pagify

log = logging.getLogger("red.fara.admintimednotifications")


DEFAULT_GUILD = {
    "admin_channel_id": None,
    "admin_role_id": None,
    "reminders": [],
}


def parse_title_body(raw: str, *, default_title: str = "Admin reminder") -> tuple[str, str]:
    value = (raw or "").strip()
    if "|" not in value:
        return default_title, value
    title, body = value.split("|", 1)
    return title.strip() or default_title, body.strip()


def next_run(interval_minutes: int, *, now: Optional[datetime] = None) -> int:
    if interval_minutes < 1:
        raise ValueError("interval must be at least 1 minute")
    base = now or datetime.now(timezone.utc)
    return int((base + timedelta(minutes=interval_minutes)).timestamp())


def split_due_reminders(
    reminders: list[dict[str, Any]], *, now_ts: Optional[int] = None
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    current = int(datetime.now(timezone.utc).timestamp()) if now_ts is None else int(now_ts)
    due = []
    pending = []
    for reminder in reminders:
        if int(reminder.get("next_run", 0)) <= current:
            due.append(reminder)
        else:
            pending.append(reminder)
    return due, pending


class AdminTimedNotifications(commands.Cog):
    """Repeated reminders for admins."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFACA_D001, force_registration=True)
        self.config.register_guild(**DEFAULT_GUILD)
        self._task: Optional[asyncio.Task] = None

    async def cog_load(self):
        self._task = asyncio.create_task(self.reminder_loop())

    def cog_unload(self):
        if self._task:
            self._task.cancel()

    async def reminder_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                await self.run_due_reminders()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Admin reminder loop failed")
            await asyncio.sleep(60)

    async def run_due_reminders(self):
        current_ts = int(datetime.now(timezone.utc).timestamp())
        for guild in self.bot.guilds:
            channel_id = await self.config.guild(guild).admin_channel_id()
            channel = guild.get_channel(channel_id) if channel_id else None
            if not channel:
                continue
            role_id = await self.config.guild(guild).admin_role_id()
            role = guild.get_role(role_id) if role_id else None
            async with self.config.guild(guild).reminders() as reminders:
                due, pending = split_due_reminders(list(reminders), now_ts=current_ts)
                for reminder in due:
                    interval = int(reminder.get("interval_minutes") or 0)
                    if interval < 1:
                        continue
                    embed = discord.Embed(
                        title=reminder.get("title") or "Admin reminder",
                        description=reminder.get("body") or "",
                        color=discord.Color.orange(),
                        timestamp=datetime.now(timezone.utc),
                    )
                    content = role.mention if role else None
                    await channel.send(content=content, embed=embed)
                    reminder["next_run"] = next_run(interval)
                    pending.append(reminder)
                reminders[:] = pending

    @commands.group(name="admintimerset")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def admintimerset(self, ctx: commands.Context):
        """Configure admin timed notifications."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @admintimerset.command(name="channel")
    async def admintimerset_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where reminders are posted."""
        await self.config.guild(ctx.guild).admin_channel_id.set(channel.id)
        await ctx.send(f"Admin reminder channel set to {channel.mention}.")

    @admintimerset.command(name="role")
    async def admintimerset_role(self, ctx: commands.Context, role: discord.Role):
        """Set the role to ping for reminders."""
        await self.config.guild(ctx.guild).admin_role_id.set(role.id)
        await ctx.send(f"Admin reminder role set to {role.mention}.")

    @commands.group(name="admintimer")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def admintimer(self, ctx: commands.Context):
        """Manage repeated admin reminders."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @admintimer.command(name="add")
    async def admintimer_add(self, ctx: commands.Context, interval_minutes: int, *, content: str):
        """Add a repeated reminder. Format: `<minutes> Title | message`."""
        if interval_minutes < 1:
            await ctx.send("Interval must be at least 1 minute.")
            return
        title, body = parse_title_body(content)
        if not body:
            await ctx.send("Give me reminder text.")
            return
        item = {
            "id": int(datetime.now(timezone.utc).timestamp() * 1000),
            "title": title,
            "body": body,
            "interval_minutes": interval_minutes,
            "next_run": next_run(interval_minutes),
        }
        async with self.config.guild(ctx.guild).reminders() as reminders:
            reminders.append(item)
        await ctx.send(f"Admin reminder `{item['id']}` added.")

    @admintimer.command(name="list")
    async def admintimer_list(self, ctx: commands.Context):
        """List configured reminders."""
        reminders = await self.config.guild(ctx.guild).reminders()
        if not reminders:
            await ctx.send("No admin reminders configured.")
            return
        lines = []
        for item in reminders:
            run_at = datetime.fromtimestamp(item.get("next_run", 0), tz=timezone.utc).isoformat()
            lines.append(
                f"`{item['id']}` every {item.get('interval_minutes')}m next {run_at} - "
                f"{item.get('title')}"
            )
        for page in pagify("\n".join(lines), page_length=1800):
            await ctx.send(page)

    @admintimer.command(name="remove")
    async def admintimer_remove(self, ctx: commands.Context, reminder_id: int):
        """Remove a reminder by ID."""
        removed = False
        async with self.config.guild(ctx.guild).reminders() as reminders:
            kept = [item for item in reminders if int(item.get("id", 0)) != reminder_id]
            removed = len(kept) != len(reminders)
            reminders[:] = kept
        await ctx.send("Admin reminder removed." if removed else "Admin reminder not found.")

    @admintimer.command(name="run")
    async def admintimer_run(self, ctx: commands.Context, reminder_id: int):
        """Post a reminder immediately without changing its schedule."""
        channel_id = await self.config.guild(ctx.guild).admin_channel_id()
        channel = ctx.guild.get_channel(channel_id) if channel_id else None
        if not channel:
            await ctx.send("No admin reminder channel configured.")
            return
        reminders = await self.config.guild(ctx.guild).reminders()
        reminder = next((item for item in reminders if int(item.get("id", 0)) == reminder_id), None)
        if not reminder:
            await ctx.send("Admin reminder not found.")
            return
        role_id = await self.config.guild(ctx.guild).admin_role_id()
        role = ctx.guild.get_role(role_id) if role_id else None
        embed = discord.Embed(
            title=reminder.get("title") or "Admin reminder",
            description=reminder.get("body") or "",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        await channel.send(content=role.mention if role else None, embed=embed)
        await ctx.send("Admin reminder posted.")
