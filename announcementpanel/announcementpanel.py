from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Optional

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import pagify


DEFAULT_GUILD = {
    "admin_role_id": None,
    "embed_color": 0xD64032,
    "buttons": {},
}


def normalize_button_key(value: str) -> str:
    key = "".join(ch for ch in (value or "").lower().strip() if ch.isalnum() or ch in "_-")
    if not key:
        raise ValueError("button key cannot be empty")
    return key[:40]


def parse_label_message(raw: str) -> tuple[str, str]:
    value = (raw or "").strip()
    if "|" not in value:
        raise ValueError("message separator missing")
    label, message = value.split("|", 1)
    label = label.strip()
    message = message.strip()
    if not label or not message:
        raise ValueError("label and message are required")
    return label[:80], message[:4000]


class PanelButton(discord.ui.Button):
    def __init__(self, cog: "AnnouncementPanel", key: str, label: str):
        super().__init__(
            label=label[:80],
            style=discord.ButtonStyle.primary,
            custom_id=f"fara_announcementpanel:{key}",
        )
        self.cog = cog
        self.key = key

    async def callback(self, interaction: discord.Interaction):
        await self.cog.handle_button(interaction, self.key)


class PanelView(discord.ui.View):
    def __init__(self, cog: "AnnouncementPanel", buttons: dict[str, dict[str, Any]]):
        super().__init__(timeout=None)
        self.cog = cog
        for key, config in list(buttons.items())[:25]:
            self.add_item(PanelButton(cog, key, config.get("label", key)))


class AnnouncementPanel(commands.Cog):
    """Configurable buttons that post preset announcements to channels."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFACA_C001, force_registration=True)
        self.config.register_guild(**DEFAULT_GUILD)
        self._restore_task = None

    async def cog_load(self):
        self._restore_task = asyncio.create_task(self.restore_views())

    def cog_unload(self):
        if self._restore_task:
            self._restore_task.cancel()

    async def restore_views(self):
        await self.bot.wait_until_red_ready()
        for guild in self.bot.guilds:
            buttons = await self.config.guild(guild).buttons()
            if buttons:
                self.bot.add_view(PanelView(self, buttons))

    async def can_use(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            return False
        if getattr(interaction.user.guild_permissions, "administrator", False):
            return True
        role_id = await self.config.guild(interaction.guild).admin_role_id()
        if role_id is None:
            return True
        return any(role.id == role_id for role in getattr(interaction.user, "roles", []))

    async def handle_button(self, interaction: discord.Interaction, key: str):
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        if not await self.can_use(interaction):
            await interaction.response.send_message("You do not have permission.", ephemeral=True)
            return

        buttons = await self.config.guild(interaction.guild).buttons()
        config = buttons.get(key)
        if not config:
            await interaction.response.send_message("This panel button is no longer configured.", ephemeral=True)
            return

        channel_ids = [int(ch_id) for ch_id in config.get("channel_ids", [])]
        if not channel_ids:
            await interaction.response.send_message("This button has no target channels.", ephemeral=True)
            return

        embed = discord.Embed(
            title=config.get("label", "Announcement"),
            description=config.get("message", ""),
            color=discord.Color(await self.config.guild(interaction.guild).embed_color()),
            timestamp=datetime.now(timezone.utc),
        )
        sent = 0
        for channel_id in channel_ids:
            channel = interaction.guild.get_channel(channel_id) or self.bot.get_channel(channel_id)
            if channel:
                await channel.send(embed=embed)
                sent += 1
        await interaction.response.send_message(f"Posted to {sent} channel(s).", ephemeral=True)

    @commands.group(name="annpanelset")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def annpanelset(self, ctx: commands.Context):
        """Configure announcement panel buttons."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @annpanelset.command(name="adminrole")
    async def annpanelset_adminrole(self, ctx: commands.Context, role: discord.Role):
        """Restrict panel buttons to a role."""
        await self.config.guild(ctx.guild).admin_role_id.set(role.id)
        await ctx.send(f"Announcement panel admin role set to {role.mention}.")

    @annpanelset.command(name="add")
    async def annpanelset_add(self, ctx: commands.Context, key: str, *, label_and_message: str):
        """Add or update a button. Format: `<key> Label | message`."""
        try:
            button_key = normalize_button_key(key)
            label, message = parse_label_message(label_and_message)
        except ValueError:
            await ctx.send("Use: `[p]annpanelset add <key> Label | message`.")
            return
        async with self.config.guild(ctx.guild).buttons() as buttons:
            existing = buttons.get(button_key, {})
            buttons[button_key] = {
                "label": label,
                "message": message,
                "channel_ids": existing.get("channel_ids", []),
            }
        await ctx.send(f"Button `{button_key}` saved.")

    @annpanelset.command(name="channel")
    async def annpanelset_channel(self, ctx: commands.Context, key: str, channel: discord.TextChannel):
        """Add a target channel to a button."""
        button_key = normalize_button_key(key)
        async with self.config.guild(ctx.guild).buttons() as buttons:
            if button_key not in buttons:
                await ctx.send("That button does not exist yet.")
                return
            channel_ids = buttons[button_key].setdefault("channel_ids", [])
            if channel.id not in channel_ids:
                channel_ids.append(channel.id)
        await ctx.send(f"{channel.mention} added to `{button_key}`.")

    @annpanelset.command(name="removechannel")
    async def annpanelset_removechannel(self, ctx: commands.Context, key: str, channel: discord.TextChannel):
        """Remove a target channel from a button."""
        button_key = normalize_button_key(key)
        async with self.config.guild(ctx.guild).buttons() as buttons:
            if button_key not in buttons:
                await ctx.send("That button does not exist.")
                return
            channel_ids = buttons[button_key].setdefault("channel_ids", [])
            if channel.id in channel_ids:
                channel_ids.remove(channel.id)
        await ctx.send(f"{channel.mention} removed from `{button_key}`.")

    @annpanelset.command(name="remove")
    async def annpanelset_remove(self, ctx: commands.Context, key: str):
        """Remove a button."""
        button_key = normalize_button_key(key)
        async with self.config.guild(ctx.guild).buttons() as buttons:
            removed = buttons.pop(button_key, None)
        await ctx.send("Button removed." if removed else "That button was not configured.")

    @annpanelset.command(name="list")
    async def annpanelset_list(self, ctx: commands.Context):
        """List configured panel buttons."""
        buttons = await self.config.guild(ctx.guild).buttons()
        if not buttons:
            await ctx.send("No panel buttons configured.")
            return
        lines = []
        for key, item in buttons.items():
            channels = [
                (ctx.guild.get_channel(channel_id).mention if ctx.guild.get_channel(channel_id) else str(channel_id))
                for channel_id in item.get("channel_ids", [])
            ]
            lines.append(f"`{key}` {item.get('label', key)} -> {', '.join(channels) or 'no channels'}")
        for page in pagify("\n".join(lines), page_length=1800):
            await ctx.send(page)

    @commands.group(name="annpanel")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def annpanel(self, ctx: commands.Context):
        """Post announcement panels."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @annpanel.command(name="post")
    async def annpanel_post(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        """Post the configured announcement panel."""
        buttons = await self.config.guild(ctx.guild).buttons()
        if not buttons:
            await ctx.send("No buttons configured.")
            return
        target = channel or ctx.channel
        embed = discord.Embed(
            title="Announcement panel",
            description="Press a button to post its configured message.",
            color=discord.Color(await self.config.guild(ctx.guild).embed_color()),
        )
        view = PanelView(self, buttons)
        self.bot.add_view(view)
        await target.send(embed=embed, view=view)
        await ctx.send(f"Announcement panel posted in {target.mention}.")
