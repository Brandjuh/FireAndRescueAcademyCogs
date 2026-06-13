from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red


DEFAULT_GUILD = {
    "announcement_channel_id": None,
    "admin_role_id": None,
    "embed_color": 0xD64032,
    "footer": "Fire And Rescue Academy",
}


def parse_title_body(raw: str, *, default_title: str = "Announcement") -> tuple[str, str]:
    value = (raw or "").strip()
    if not value:
        return default_title, ""
    if "|" not in value:
        return default_title, value
    title, body = value.split("|", 1)
    return title.strip() or default_title, body.strip()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AnnouncementModal(discord.ui.Modal, title="Announcement"):
    title_input = discord.ui.TextInput(
        label="Title",
        max_length=256,
        required=True,
        placeholder="Title shown at the top of the embed",
    )
    body_input = discord.ui.TextInput(
        label="Message",
        style=discord.TextStyle.paragraph,
        max_length=3500,
        required=True,
        placeholder="Message to announce",
    )

    def __init__(self, cog: "Announcer"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("This can only be used in a server.", ephemeral=True)
            return
        if not await self.cog.can_use(interaction):
            await interaction.response.send_message("You do not have permission.", ephemeral=True)
            return

        message = await self.cog.post_announcement(
            interaction.guild,
            str(self.title_input.value),
            str(self.body_input.value),
            author=interaction.user,
        )
        if message:
            await interaction.response.send_message("Announcement posted.", ephemeral=True)
        else:
            await interaction.response.send_message(
                "No announcement channel is configured.", ephemeral=True
            )


class AnnouncerButtonView(discord.ui.View):
    def __init__(self, cog: "Announcer"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Create announcement",
        style=discord.ButtonStyle.primary,
        custom_id="fara_announcer:create",
    )
    async def create_announcement(self, interaction: discord.Interaction, button: discord.ui.Button):
        del button
        await interaction.response.send_modal(AnnouncementModal(self.cog))


class Announcer(commands.Cog):
    """Format admin input into a consistent announcement embed."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFACA_A001, force_registration=True)
        self.config.register_guild(**DEFAULT_GUILD)

    async def cog_load(self):
        self.bot.add_view(AnnouncerButtonView(self))

    async def can_use(self, interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            return False
        if getattr(interaction.user.guild_permissions, "administrator", False):
            return True
        role_id = await self.config.guild(interaction.guild).admin_role_id()
        if role_id is None:
            return True
        return any(role.id == role_id for role in getattr(interaction.user, "roles", []))

    async def announcement_channel(self, guild: discord.Guild):
        channel_id = await self.config.guild(guild).announcement_channel_id()
        if not channel_id:
            return None
        return guild.get_channel(channel_id) or self.bot.get_channel(channel_id)

    async def build_embed(
        self,
        guild: discord.Guild,
        title: str,
        body: str,
        *,
        author: Optional[discord.abc.User] = None,
    ) -> discord.Embed:
        footer = await self.config.guild(guild).footer()
        color = await self.config.guild(guild).embed_color()
        embed = discord.Embed(
            title=title[:256],
            description=body[:4096],
            color=discord.Color(int(color)),
            timestamp=utcnow(),
        )
        if author:
            embed.add_field(name="Posted by", value=getattr(author, "mention", str(author)), inline=True)
        if footer:
            embed.set_footer(text=footer)
        return embed

    async def post_announcement(
        self,
        guild: discord.Guild,
        title: str,
        body: str,
        *,
        author: Optional[discord.abc.User] = None,
    ) -> Optional[discord.Message]:
        channel = await self.announcement_channel(guild)
        if not channel:
            return None
        embed = await self.build_embed(guild, title, body, author=author)
        return await channel.send(embed=embed)

    @commands.group(name="announcerset")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def announcerset(self, ctx: commands.Context):
        """Configure the announcement composer."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @announcerset.command(name="channel")
    async def announcerset_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set where Announcer posts messages."""
        await self.config.guild(ctx.guild).announcement_channel_id.set(channel.id)
        await ctx.send(f"Announcer channel set to {channel.mention}.")

    @announcerset.command(name="adminrole")
    async def announcerset_adminrole(self, ctx: commands.Context, role: discord.Role):
        """Restrict the composer button to a role."""
        await self.config.guild(ctx.guild).admin_role_id.set(role.id)
        await ctx.send(f"Announcer admin role set to {role.mention}.")

    @announcerset.command(name="footer")
    async def announcerset_footer(self, ctx: commands.Context, *, footer: str):
        """Set the embed footer."""
        await self.config.guild(ctx.guild).footer.set(footer[:2048])
        await ctx.send("Announcer footer updated.")

    @announcerset.command(name="color")
    async def announcerset_color(self, ctx: commands.Context, color: str):
        """Set embed color as hex, for example #d64032."""
        cleaned = color.strip().lstrip("#")
        try:
            value = int(cleaned, 16)
        except ValueError:
            await ctx.send("Use a hex color like `#d64032`.")
            return
        if not 0 <= value <= 0xFFFFFF:
            await ctx.send("Use a valid RGB hex color.")
            return
        await self.config.guild(ctx.guild).embed_color.set(value)
        await ctx.send(f"Announcer color set to #{value:06x}.")

    @commands.group(name="announcer")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def announcer(self, ctx: commands.Context):
        """Post formatted announcements."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @announcer.command(name="button")
    async def announcer_button(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        """Post the composer button."""
        target = channel or ctx.channel
        embed = discord.Embed(
            title="Announcement composer",
            description="Press the button to write an announcement.",
            color=discord.Color(await self.config.guild(ctx.guild).embed_color()),
        )
        await target.send(embed=embed, view=AnnouncerButtonView(self))
        await ctx.send(f"Announcer button posted in {target.mention}.")

    @announcer.command(name="send")
    async def announcer_send(self, ctx: commands.Context, *, content: str):
        """Post directly. Format: `Title | message`."""
        title, body = parse_title_body(content)
        if not body:
            await ctx.send("Give me a message to announce.")
            return
        message = await self.post_announcement(ctx.guild, title, body, author=ctx.author)
        await ctx.send("Announcement posted." if message else "No announcement channel is configured.")
