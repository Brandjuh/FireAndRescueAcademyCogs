from __future__ import annotations

import asyncio
import re
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
    "panel_messages": [],
}

MESSAGE_CONTENT_LIMIT = 2000


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


def parse_channel_ids(raw: str) -> list[int]:
    seen: set[int] = set()
    channel_ids: list[int] = []
    for match in re.findall(r"\d{15,25}", raw or ""):
        channel_id = int(match)
        if channel_id not in seen:
            seen.add(channel_id)
            channel_ids.append(channel_id)
    return channel_ids


def unique_button_key(label: str, existing_keys: set[str]) -> str:
    base = normalize_button_key(label)
    if base not in existing_keys:
        return base
    for index in range(2, 100):
        candidate = normalize_button_key(f"{base}-{index}")
        if candidate not in existing_keys:
            return candidate
    raise ValueError("could not create unique button key")


def panel_message_record(channel_id: int, message_id: int) -> dict[str, int]:
    return {"channel_id": int(channel_id), "message_id": int(message_id)}


def format_announcement_content_chunks(
    label: str,
    message: str,
    *,
    ping_role_id: Optional[int] = None,
    limit: int = MESSAGE_CONTENT_LIMIT,
) -> list[str]:
    del label
    body = (message or "").strip()
    prefix = f"<@&{int(ping_role_id)}>\n" if ping_role_id else ""
    content = f"{prefix}{body}".strip()
    if not content:
        return []

    if len(content) <= limit:
        return [content]

    chunks = []
    remaining = content

    while remaining:
        chunk = remaining[:limit].rstrip()
        chunks.append(chunk)
        remaining = remaining[len(chunk):].lstrip()

    return chunks


def selectable_channel_types():
    channel_type = getattr(discord, "ChannelType", None)
    if channel_type is None:
        return None
    values = [
        value
        for name in ("text", "news")
        if (value := getattr(channel_type, name, None)) is not None
    ]
    return values or None


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


class ConfirmPostView(discord.ui.View):
    def __init__(
        self,
        cog: "AnnouncementPanel",
        key: str,
        user_id: int,
    ):
        super().__init__(timeout=120)
        self.cog = cog
        self.key = key
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id

    @discord.ui.button(label="Post announcement", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        if not await self.cog.can_use(interaction):
            await interaction.response.send_message("You do not have permission.", ephemeral=True)
            return

        sent = await self.cog.post_button_announcement(interaction.guild, self.key)
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(
            content=f"Posted to {sent} channel(s).",
            view=self,
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="Cancelled.", view=self)


class ButtonConfigModal(discord.ui.Modal, title="Announcement panel button"):
    key_input = discord.ui.TextInput(
        label="Internal key",
        max_length=40,
        required=False,
        placeholder="Optional. Leave empty to generate from the label.",
    )
    label_input = discord.ui.TextInput(
        label="Button label",
        max_length=80,
        required=True,
        placeholder="Example: Double Credits",
    )
    message_input = discord.ui.TextInput(
        label="Message",
        style=discord.TextStyle.paragraph,
        max_length=4000,
        required=True,
        placeholder="The message this button will post.",
    )

    def __init__(
        self,
        cog: "AnnouncementPanel",
        *,
        existing_key: Optional[str] = None,
        existing_config: Optional[dict[str, Any]] = None,
    ):
        super().__init__()
        self.cog = cog
        self.existing_key = existing_key
        existing_config = existing_config or {}
        if existing_key:
            self.key_input.default = existing_key
        if existing_config:
            self.label_input.default = existing_config.get("label", "")
            self.message_input.default = existing_config.get("message", "")

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        if not await self.cog.can_use(interaction):
            await interaction.response.send_message("You do not have permission.", ephemeral=True)
            return

        label = str(self.label_input.value).strip()[:80]
        message = str(self.message_input.value).strip()[:4000]
        if not label or not message:
            await interaction.response.send_message("Label and message are required.", ephemeral=True)
            return

        async with self.cog.config.guild(interaction.guild).buttons() as buttons:
            raw_key = str(self.key_input.value).strip()
            if raw_key:
                button_key = normalize_button_key(raw_key)
            elif self.existing_key:
                button_key = self.existing_key
            else:
                button_key = unique_button_key(label, set(buttons))

            if self.existing_key and self.existing_key != button_key:
                buttons.pop(self.existing_key, None)
            existing = buttons.get(button_key, {})
            buttons[button_key] = {
                "label": label,
                "message": message,
                "channel_ids": existing.get("channel_ids", []),
                "ping_role_id": existing.get("ping_role_id"),
            }

        refreshed = await self.cog.refresh_panel_messages(interaction.guild)
        target_view = self.cog.target_config_view(button_key, interaction.user.id)
        target_text = await self.cog.target_config_message(
            interaction.guild,
            button_key,
            intro="Step 2/2: select where this button posts and which role it pings.",
        )
        await interaction.response.send_message(
            (
                f"Button `{button_key}` saved. Refreshed {refreshed} panel message(s)."
                f"\n\n{target_text}"
            ),
            view=target_view,
            ephemeral=True,
        )


class TargetChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, cog: "AnnouncementPanel", key: str):
        self.cog = cog
        self.key = key
        kwargs = {
            "placeholder": "Select target channels",
            "min_values": 1,
            "max_values": 25,
        }
        channel_types = selectable_channel_types()
        if channel_types:
            kwargs["channel_types"] = channel_types
        super().__init__(**kwargs)

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        if not await self.cog.can_use(interaction):
            await interaction.response.send_message("You do not have permission.", ephemeral=True)
            return

        channel_ids = [int(channel.id) for channel in getattr(self, "values", [])]
        async with self.cog.config.guild(interaction.guild).buttons() as buttons:
            if self.key not in buttons:
                await interaction.response.send_message("That button no longer exists.", ephemeral=True)
                return
            buttons[self.key]["channel_ids"] = channel_ids
        refreshed = await self.cog.refresh_panel_messages(interaction.guild)
        await interaction.response.edit_message(
            content=(
                f"Target channels updated. Refreshed {refreshed} panel message(s).\n\n"
                f"{await self.cog.target_config_message(interaction.guild, self.key)}"
            ),
            view=self.cog.target_config_view(self.key, interaction.user.id),
        )


class TargetRoleSelect(discord.ui.RoleSelect):
    def __init__(self, cog: "AnnouncementPanel", key: str):
        self.cog = cog
        self.key = key
        super().__init__(
            placeholder="Select role to ping",
            min_values=0,
            max_values=1,
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        if not await self.cog.can_use(interaction):
            await interaction.response.send_message("You do not have permission.", ephemeral=True)
            return

        roles = list(getattr(self, "values", []))
        role_id = int(roles[0].id) if roles else None
        async with self.cog.config.guild(interaction.guild).buttons() as buttons:
            if self.key not in buttons:
                await interaction.response.send_message("That button no longer exists.", ephemeral=True)
                return
            buttons[self.key]["ping_role_id"] = role_id
        refreshed = await self.cog.refresh_panel_messages(interaction.guild)
        await interaction.response.edit_message(
            content=(
                f"Ping role updated. Refreshed {refreshed} panel message(s).\n\n"
                f"{await self.cog.target_config_message(interaction.guild, self.key)}"
            ),
            view=self.cog.target_config_view(self.key, interaction.user.id),
        )


class ButtonTargetConfigView(discord.ui.View):
    def __init__(self, cog: "AnnouncementPanel", key: str, user_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.key = key
        self.user_id = user_id
        self.add_item(TargetChannelSelect(cog, key))
        self.add_item(TargetRoleSelect(cog, key))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id

    @discord.ui.button(label="Clear ping role", style=discord.ButtonStyle.secondary)
    async def clear_role(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        async with self.cog.config.guild(interaction.guild).buttons() as buttons:
            if self.key in buttons:
                buttons[self.key]["ping_role_id"] = None
        refreshed = await self.cog.refresh_panel_messages(interaction.guild)
        await interaction.response.edit_message(
            content=(
                f"Ping role cleared. Refreshed {refreshed} panel message(s).\n\n"
                f"{await self.cog.target_config_message(interaction.guild, self.key)}"
            ),
            view=self.cog.target_config_view(self.key, interaction.user.id),
        )

    @discord.ui.button(label="Done", style=discord.ButtonStyle.success)
    async def done(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(
            content=await self.cog.target_config_message(interaction.guild, self.key),
            view=self,
        )


class ButtonKeySelect(discord.ui.Select):
    def __init__(
        self,
        cog: "AnnouncementPanel",
        buttons: dict[str, dict[str, Any]],
        action: str,
    ):
        self.cog = cog
        self.action = action
        options = [
            discord.SelectOption(
                label=config.get("label", key)[:100],
                value=key,
                description=f"Key: {key}"[:100],
            )
            for key, config in list(buttons.items())[:25]
        ]
        super().__init__(placeholder="Choose a panel button", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        if not await self.cog.can_use(interaction):
            await interaction.response.send_message("You do not have permission.", ephemeral=True)
            return

        key = self.values[0]
        buttons = await self.cog.config.guild(interaction.guild).buttons()
        config = buttons.get(key)
        if not config:
            await interaction.response.send_message("That button no longer exists.", ephemeral=True)
            return

        if self.action == "edit":
            await interaction.response.send_modal(
                ButtonConfigModal(self.cog, existing_key=key, existing_config=config)
            )
            return

        if self.action == "targets":
            await interaction.response.edit_message(
                content=await self.cog.target_config_message(interaction.guild, key),
                view=self.cog.target_config_view(key, interaction.user.id),
            )
            return

        if self.action == "remove":
            await interaction.response.edit_message(
                content=f"Remove `{key}` ({config.get('label', key)})?",
                view=RemoveButtonConfirmView(self.cog, key, interaction.user.id),
            )


class ButtonKeySelectView(discord.ui.View):
    def __init__(
        self,
        cog: "AnnouncementPanel",
        buttons: dict[str, dict[str, Any]],
        action: str,
        user_id: int,
    ):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.add_item(ButtonKeySelect(cog, buttons, action))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id


class RemoveButtonConfirmView(discord.ui.View):
    def __init__(self, cog: "AnnouncementPanel", key: str, user_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.key = key
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.danger)
    async def remove(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        async with self.cog.config.guild(interaction.guild).buttons() as buttons:
            removed = buttons.pop(self.key, None)
        refreshed = await self.cog.refresh_panel_messages(interaction.guild)
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(
            content=(
                f"Button `{self.key}` removed. Refreshed {refreshed} panel message(s)."
                if removed
                else "That button was already removed."
            ),
            view=self,
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="Cancelled.", view=self)


class ManagePanelView(discord.ui.View):
    def __init__(self, cog: "AnnouncementPanel", user_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id

    @discord.ui.button(label="Add button", style=discord.ButtonStyle.success)
    async def add_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        await interaction.response.send_modal(ButtonConfigModal(self.cog))

    @discord.ui.button(label="Edit button", style=discord.ButtonStyle.primary)
    async def edit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        await self.cog.send_button_picker(interaction, "edit")

    @discord.ui.button(label="Targets / ping", style=discord.ButtonStyle.secondary)
    async def targets_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        await self.cog.send_button_picker(interaction, "targets")

    @discord.ui.button(label="Remove button", style=discord.ButtonStyle.danger)
    async def remove_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        await self.cog.send_button_picker(interaction, "remove")

    @discord.ui.button(label="Post panel here", style=discord.ButtonStyle.secondary)
    async def post_panel_here(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("This can only be used in a server channel.", ephemeral=True)
            return
        message = await self.cog.send_panel_message(interaction.guild, interaction.channel)
        await interaction.response.send_message(
            f"Announcement panel posted: {message.jump_url}",
            ephemeral=True,
        )

    @discord.ui.button(label="Refresh panels", style=discord.ButtonStyle.secondary)
    async def refresh_panels(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        refreshed = await self.cog.refresh_panel_messages(interaction.guild)
        await interaction.response.send_message(
            f"Refreshed {refreshed} panel message(s).",
            ephemeral=True,
        )


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

    async def build_panel_embed(self, guild: discord.Guild) -> discord.Embed:
        buttons = await self.config.guild(guild).buttons()
        description = "Use a button to prepare a preset announcement."
        if not buttons:
            description = "No announcement buttons are configured yet."
        embed = discord.Embed(
            title="Announcement panel",
            description=description,
            color=discord.Color(await self.config.guild(guild).embed_color()),
        )
        if buttons:
            lines = []
            for key, config in buttons.items():
                channel_ids = config.get("channel_ids", [])
                role_text = self.format_role_label(guild, config.get("ping_role_id"))
                lines.append(
                    f"`{key}` - {config.get('label', key)} "
                    f"({len(channel_ids)} channel(s), ping: {role_text})"
                )
            embed.add_field(name="Configured buttons", value="\n".join(lines)[:1024], inline=False)
        return embed

    async def build_announcement_embed(
        self,
        guild: discord.Guild,
        config: dict[str, Any],
    ) -> discord.Embed:
        embed = discord.Embed(
            title=config.get("label", "Announcement"),
            description=config.get("message", ""),
            color=discord.Color(await self.config.guild(guild).embed_color()),
            timestamp=datetime.now(timezone.utc),
        )
        role_id = config.get("ping_role_id")
        if role_id:
            embed.add_field(name="Ping role", value=self.format_role_label(guild, role_id), inline=False)
        return embed

    def announcement_allowed_mentions(self):
        allowed_mentions = getattr(discord, "AllowedMentions", None)
        if allowed_mentions is None:
            return None
        return allowed_mentions(everyone=False, users=True, roles=True, replied_user=False)

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

        embed = await self.build_announcement_embed(interaction.guild, config)
        channels = self.format_channel_list(interaction.guild, channel_ids)
        role_text = self.format_role_label(interaction.guild, config.get("ping_role_id"))
        await interaction.response.send_message(
            f"Post **{config.get('label', key)}** to: {channels}?\nPing role: {role_text}",
            embed=embed,
            view=ConfirmPostView(self, key, interaction.user.id),
            ephemeral=True,
        )

    def format_channel_list(self, guild: discord.Guild, channel_ids: list[int]) -> str:
        channels = []
        for channel_id in channel_ids:
            channel = guild.get_channel(channel_id) or self.bot.get_channel(channel_id)
            channels.append(channel.mention if channel else str(channel_id))
        return ", ".join(channels) or "no channels"

    def format_role_label(self, guild: discord.Guild, role_id: Optional[int]) -> str:
        if not role_id:
            return "none"
        role = guild.get_role(int(role_id)) if guild else None
        if role:
            return f"@{getattr(role, 'name', role_id)}"
        return str(role_id)

    async def post_button_announcement(self, guild: discord.Guild, key: str) -> int:
        buttons = await self.config.guild(guild).buttons()
        config = buttons.get(key)
        if not config:
            return 0

        chunks = format_announcement_content_chunks(
            config.get("label", "Announcement"),
            config.get("message", ""),
            ping_role_id=config.get("ping_role_id"),
        )
        allowed_mentions = self.announcement_allowed_mentions()
        send_kwargs = {"allowed_mentions": allowed_mentions} if allowed_mentions else {}
        sent = 0
        for channel_id in [int(ch_id) for ch_id in config.get("channel_ids", [])]:
            channel = guild.get_channel(channel_id) or self.bot.get_channel(channel_id)
            if channel:
                for chunk in chunks:
                    await channel.send(content=chunk, **send_kwargs)
                sent += 1
        return sent

    async def send_panel_message(
        self,
        guild: discord.Guild,
        channel: discord.TextChannel,
    ) -> discord.Message:
        buttons = await self.config.guild(guild).buttons()
        embed = await self.build_panel_embed(guild)
        view = PanelView(self, buttons) if buttons else None
        message = await channel.send(embed=embed, view=view)
        await self.remember_panel_message(guild, message)
        return message

    async def remember_panel_message(self, guild: discord.Guild, message: discord.Message):
        record = panel_message_record(message.channel.id, message.id)
        async with self.config.guild(guild).panel_messages() as panel_messages:
            panel_messages[:] = [
                item
                for item in panel_messages
                if not (
                    int(item.get("channel_id", 0)) == record["channel_id"]
                    and int(item.get("message_id", 0)) == record["message_id"]
                )
            ]
            panel_messages.append(record)

    async def refresh_panel_messages(self, guild: discord.Guild) -> int:
        buttons = await self.config.guild(guild).buttons()
        embed = await self.build_panel_embed(guild)
        refreshed = 0
        kept = []

        for item in await self.config.guild(guild).panel_messages():
            channel_id = int(item.get("channel_id", 0))
            message_id = int(item.get("message_id", 0))
            channel = guild.get_channel(channel_id) or self.bot.get_channel(channel_id)
            if not channel or not hasattr(channel, "fetch_message"):
                continue
            try:
                message = await channel.fetch_message(message_id)
                view = PanelView(self, buttons) if buttons else None
                await message.edit(embed=embed, view=view)
                kept.append(panel_message_record(channel_id, message_id))
                refreshed += 1
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                continue

        await self.config.guild(guild).panel_messages.set(kept)
        return refreshed

    def target_config_view(self, key: str, user_id: int):
        return ButtonTargetConfigView(self, key, user_id)

    async def target_config_message(
        self,
        guild: discord.Guild,
        key: str,
        *,
        intro: str = "Configure posting targets and ping role.",
    ) -> str:
        buttons = await self.config.guild(guild).buttons()
        config = buttons.get(key, {})
        channel_ids = [int(channel_id) for channel_id in config.get("channel_ids", [])]
        label = config.get("label", key)
        return "\n".join(
            [
                intro,
                f"Button: `{key}` - {label}",
                f"Channels: {self.format_channel_list(guild, channel_ids)}",
                f"Ping role: {self.format_role_label(guild, config.get('ping_role_id'))}",
            ]
        )

    async def send_button_picker(self, interaction: discord.Interaction, action: str):
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        if not await self.can_use(interaction):
            await interaction.response.send_message("You do not have permission.", ephemeral=True)
            return

        buttons = await self.config.guild(interaction.guild).buttons()
        if not buttons:
            await interaction.response.send_message("No buttons configured.", ephemeral=True)
            return
        if len(buttons) == 1:
            key, config = next(iter(buttons.items()))
            if action == "edit":
                await interaction.response.send_modal(
                    ButtonConfigModal(self, existing_key=key, existing_config=config)
                )
                return
            if action == "remove":
                await interaction.response.send_message(
                    f"Remove `{key}` ({config.get('label', key)})?",
                    view=RemoveButtonConfirmView(self, key, interaction.user.id),
                    ephemeral=True,
                )
                return
            if action == "targets":
                await interaction.response.send_message(
                    await self.target_config_message(interaction.guild, key),
                    view=self.target_config_view(key, interaction.user.id),
                    ephemeral=True,
                )
                return

        await interaction.response.send_message(
            "Choose a button.",
            view=ButtonKeySelectView(self, buttons, action, interaction.user.id),
            ephemeral=True,
        )

    async def send_manage_menu(self, ctx: commands.Context):
        buttons = await self.config.guild(ctx.guild).buttons()
        panel_messages = await self.config.guild(ctx.guild).panel_messages()
        embed = discord.Embed(
            title="Announcement Panel Manager",
            description="Use the buttons below to configure and refresh announcement panels.",
            color=discord.Color(await self.config.guild(ctx.guild).embed_color()),
        )
        embed.add_field(name="Buttons", value=str(len(buttons)), inline=True)
        embed.add_field(name="Posted panels", value=str(len(panel_messages)), inline=True)
        await ctx.send(embed=embed, view=ManagePanelView(self, ctx.author.id))

    async def refresh_after_config_change(self, guild: discord.Guild) -> int:
        return await self.refresh_panel_messages(guild)

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
                "ping_role_id": existing.get("ping_role_id"),
            }
        refreshed = await self.refresh_after_config_change(ctx.guild)
        await ctx.send(f"Button `{button_key}` saved. Refreshed {refreshed} panel message(s).")

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
        refreshed = await self.refresh_after_config_change(ctx.guild)
        await ctx.send(f"{channel.mention} added to `{button_key}`. Refreshed {refreshed} panel message(s).")

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
        refreshed = await self.refresh_after_config_change(ctx.guild)
        await ctx.send(f"{channel.mention} removed from `{button_key}`. Refreshed {refreshed} panel message(s).")

    @annpanelset.command(name="role")
    async def annpanelset_role(self, ctx: commands.Context, key: str, role: discord.Role):
        """Set the role that should be pinged by a button."""
        button_key = normalize_button_key(key)
        async with self.config.guild(ctx.guild).buttons() as buttons:
            if button_key not in buttons:
                await ctx.send("That button does not exist.")
                return
            buttons[button_key]["ping_role_id"] = role.id
        refreshed = await self.refresh_after_config_change(ctx.guild)
        role_name = f"@{getattr(role, 'name', role.id)}"
        await ctx.send(f"{role_name} set as ping role for `{button_key}`. Refreshed {refreshed} panel message(s).")

    @annpanelset.command(name="clearrole")
    async def annpanelset_clearrole(self, ctx: commands.Context, key: str):
        """Clear the ping role from a button."""
        button_key = normalize_button_key(key)
        async with self.config.guild(ctx.guild).buttons() as buttons:
            if button_key not in buttons:
                await ctx.send("That button does not exist.")
                return
            buttons[button_key]["ping_role_id"] = None
        refreshed = await self.refresh_after_config_change(ctx.guild)
        await ctx.send(f"Ping role cleared for `{button_key}`. Refreshed {refreshed} panel message(s).")

    @annpanelset.command(name="remove")
    async def annpanelset_remove(self, ctx: commands.Context, key: str):
        """Remove a button."""
        button_key = normalize_button_key(key)
        async with self.config.guild(ctx.guild).buttons() as buttons:
            removed = buttons.pop(button_key, None)
        refreshed = await self.refresh_after_config_change(ctx.guild)
        await ctx.send(
            f"Button removed. Refreshed {refreshed} panel message(s)."
            if removed
            else "That button was not configured."
        )

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
            role_text = self.format_role_label(ctx.guild, item.get("ping_role_id"))
            lines.append(
                f"`{key}` {item.get('label', key)} -> "
                f"{', '.join(channels) or 'no channels'} | ping: {role_text}"
            )
        for page in pagify("\n".join(lines), page_length=1800):
            await ctx.send(page)

    @commands.group(name="annpanel")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def annpanel(self, ctx: commands.Context):
        """Manage and post announcement panels."""
        if ctx.invoked_subcommand is None:
            await self.send_manage_menu(ctx)

    @annpanel.command(name="manage")
    async def annpanel_manage(self, ctx: commands.Context):
        """Open the interactive announcement panel manager."""
        await self.send_manage_menu(ctx)

    @annpanel.command(name="post")
    async def annpanel_post(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        """Post the configured announcement panel."""
        buttons = await self.config.guild(ctx.guild).buttons()
        if not buttons:
            await ctx.send("No buttons configured.")
            return
        target = channel or ctx.channel
        message = await self.send_panel_message(ctx.guild, target)
        await ctx.send(f"Announcement panel posted in {target.mention}: {message.jump_url}")
