from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Sequence, Tuple, Union

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red

log = logging.getLogger("red.fara.channellist")

MESSAGE_CHAR_LIMIT = 1900
AUTO_UPDATE_DELAY_SECONDS = 10
MAX_HEADER_LENGTH = 1500
DEFAULT_HEADER = "All public channels and their description are listed below."
DEFAULT_EMOJI = "⏬"
EMPTY_LIST_PLACEHOLDER = "*There are no channels to display.*"

ACTION_SKIP = "skip"
ACTION_EDIT = "edit"
ACTION_REPOST = "repost"

DEFAULT_GUILD = {
    "channel_id": None,
    "message_ids": [],
    "message_channel_id": None,
    "role_id": None,
    "header": DEFAULT_HEADER,
    "emoji": DEFAULT_EMOJI,
    "include_voice": True,
    "auto_update": True,
    "ignored_ids": [],
}


def normalize_topic(topic: Optional[str]) -> str:
    """Collapse a channel topic to a single line of text."""
    if not topic:
        return ""
    return " ".join(topic.split())


def format_channel_line(mention: str, topic: Optional[str]) -> str:
    clean = normalize_topic(topic)
    if clean:
        return f"{mention} - {clean}"
    return mention


def format_category_header(name: str, emoji: str) -> str:
    if emoji:
        return f"**[{emoji}] [{name.upper()}] [{emoji}]**"
    return f"**[{name.upper()}]**"


def render_blocks(
    categories: Sequence[Tuple[Optional[str], Sequence[Tuple[str, Optional[str]]]]],
    emoji: str,
) -> List[List[str]]:
    """Turn (category name, [(mention, topic), ...]) pairs into blocks of message lines.

    A ``None`` category name means the channels sit above the first category and
    get no header, matching how Discord displays them.
    """
    blocks: List[List[str]] = []
    for name, entries in categories:
        if not entries:
            continue
        lines: List[str] = []
        if name is not None:
            lines.append(format_category_header(name, emoji))
        lines.extend(format_channel_line(mention, topic) for mention, topic in entries)
        blocks.append(lines)
    return blocks


def chunk_blocks(
    header: str,
    blocks: Sequence[Sequence[str]],
    limit: int = MESSAGE_CHAR_LIMIT,
) -> List[str]:
    """Pack the header and blocks into message-sized chunks.

    Blocks are separated by a blank line. A block's first line is kept together
    with the line after it, so a category header is never stranded at the
    bottom of a message while its channels start in the next one.
    """
    chunks: List[str] = []
    current = (header or "").strip()[:limit]
    for block in blocks:
        lines = [line[:limit] for line in block if line]
        for index, line in enumerate(lines):
            if index == 0:
                needed = len("\n".join(lines[:2]))
                if current and len(current) + 2 + needed > limit:
                    chunks.append(current)
                    current = ""
                separator = "\n\n" if current else ""
            else:
                separator = "\n" if current else ""
            candidate = f"{current}{separator}{line}"
            if current and len(candidate) > limit:
                chunks.append(current)
                current = line
            else:
                current = candidate
    if current:
        chunks.append(current)
    return chunks


def decide_action(
    new_chunks: Sequence[str],
    existing_contents: Optional[Sequence[str]],
) -> str:
    """Choose how the freshly rendered list should be applied.

    ``existing_contents`` must be ``None`` when nothing usable is posted (no
    stored messages, or at least one of them no longer exists); that always
    forces a repost. Editing is only possible when every stored message still
    exists and the new list does not need more messages than are posted.
    """
    if not existing_contents:
        return ACTION_REPOST
    if len(new_chunks) > len(existing_contents):
        return ACTION_REPOST
    if list(new_chunks) == list(existing_contents):
        return ACTION_SKIP
    return ACTION_EDIT


class ChannelList(commands.Cog):
    """Post a directory of all categories and channels and keep it up to date.

    The list shows every channel a configurable role can see, with the channel
    topic as its description. When channels change, the posted list is edited
    in place when possible and reposted when it has to grow or was deleted.
    """

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFACA_F001, force_registration=True)
        self.config.register_guild(**DEFAULT_GUILD)
        self._pending: Dict[int, asyncio.Task] = {}
        self._locks: Dict[int, asyncio.Lock] = {}
        self._startup_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        self._startup_task = asyncio.create_task(self._refresh_on_startup())

    def cog_unload(self):
        if self._startup_task is not None:
            self._startup_task.cancel()
        for task in self._pending.values():
            task.cancel()
        self._pending.clear()

    # ------------------------------------------------------------------ helpers

    def _lock(self, guild_id: int) -> asyncio.Lock:
        return self._locks.setdefault(guild_id, asyncio.Lock())

    def _visibility_role(self, guild: discord.Guild, role_id: Optional[int]) -> discord.Role:
        role = guild.get_role(role_id) if role_id else None
        return role or guild.default_role

    def _collect_categories(
        self,
        guild: discord.Guild,
        role: discord.Role,
        include_voice: bool,
        ignored: set,
    ) -> List[Tuple[Optional[str], List[Tuple[str, Optional[str]]]]]:
        """Gather the channels the role can see, grouped per category in UI order."""
        collected: List[Tuple[Optional[str], List[Tuple[str, Optional[str]]]]] = []
        for category, channels in guild.by_category():
            if category is not None and category.id in ignored:
                continue
            entries: List[Tuple[str, Optional[str]]] = []
            for channel in channels:
                if channel.id in ignored:
                    continue
                if not include_voice and isinstance(
                    channel, (discord.VoiceChannel, discord.StageChannel)
                ):
                    continue
                if not channel.permissions_for(role).view_channel:
                    continue
                entries.append((channel.mention, getattr(channel, "topic", None)))
            if entries:
                collected.append((category.name if category else None, entries))
        return collected

    async def _render_chunks(self, guild: discord.Guild) -> List[str]:
        conf = await self.config.guild(guild).all()
        role = self._visibility_role(guild, conf["role_id"])
        categories = self._collect_categories(
            guild, role, conf["include_voice"], set(conf["ignored_ids"])
        )
        chunks = chunk_blocks(conf["header"], render_blocks(categories, conf["emoji"]))
        return chunks or [EMPTY_LIST_PLACEHOLDER]

    async def _fetch_existing(
        self, channel: discord.TextChannel, message_ids: Sequence[int]
    ) -> Optional[List[discord.Message]]:
        """Fetch the stored list messages; ``None`` when any of them is gone."""
        messages: List[discord.Message] = []
        for message_id in message_ids:
            try:
                messages.append(await channel.fetch_message(message_id))
            except discord.HTTPException:
                return None
        return messages

    async def _delete_stored_messages(
        self,
        guild: discord.Guild,
        message_ids: Sequence[int],
        message_channel_id: Optional[int],
        fetched: Optional[Sequence[discord.Message]] = None,
    ) -> None:
        if fetched:
            for message in fetched:
                try:
                    await message.delete()
                except discord.HTTPException:
                    pass
            return
        if not message_ids or not message_channel_id:
            return
        channel = guild.get_channel(message_channel_id)
        if channel is None:
            return
        for message_id in message_ids:
            try:
                await channel.get_partial_message(message_id).delete()
            except discord.HTTPException:
                pass

    async def _refresh(self, guild: discord.Guild, *, force_repost: bool = False) -> str:
        """Bring the posted list in line with the current channels.

        Returns a result code: ``unconfigured``, ``missing_channel``,
        ``forbidden``, ``unchanged``, ``edited``, ``posted`` or ``reposted``.
        """
        conf = await self.config.guild(guild).all()
        channel_id = conf["channel_id"]
        if not channel_id:
            return "unconfigured"
        channel = guild.get_channel(channel_id)
        if channel is None:
            return "missing_channel"
        me = guild.me
        if me is not None:
            perms = channel.permissions_for(me)
            if not (perms.view_channel and perms.send_messages):
                return "forbidden"

        chunks = await self._render_chunks(guild)
        stored_ids: List[int] = conf["message_ids"]
        stored_channel_id: Optional[int] = conf["message_channel_id"]

        existing: Optional[List[discord.Message]] = None
        if stored_ids and stored_channel_id == channel.id:
            existing = await self._fetch_existing(channel, stored_ids)

        if force_repost:
            action = ACTION_REPOST
        else:
            contents = [message.content for message in existing] if existing is not None else None
            action = decide_action(chunks, contents)

        if action == ACTION_SKIP:
            return "unchanged"

        no_mentions = discord.AllowedMentions.none()

        if action == ACTION_EDIT and existing is not None:
            for message, content in zip(existing, chunks):
                if message.content != content:
                    await message.edit(content=content, allowed_mentions=no_mentions)
            for message in existing[len(chunks):]:
                try:
                    await message.delete()
                except discord.HTTPException:
                    pass
            await self.config.guild(guild).message_ids.set(
                [message.id for message in existing[: len(chunks)]]
            )
            await self.config.guild(guild).message_channel_id.set(channel.id)
            return "edited"

        await self._delete_stored_messages(guild, stored_ids, stored_channel_id, existing)
        new_ids: List[int] = []
        for content in chunks:
            message = await channel.send(content, allowed_mentions=no_mentions)
            new_ids.append(message.id)
        await self.config.guild(guild).message_ids.set(new_ids)
        await self.config.guild(guild).message_channel_id.set(channel.id)
        return "reposted" if stored_ids else "posted"

    # ------------------------------------------------------- automatic updates

    async def _auto_update_ready(self, guild: discord.Guild) -> bool:
        conf = await self.config.guild(guild).all()
        return bool(conf["auto_update"] and conf["channel_id"] and conf["message_ids"])

    async def _schedule_auto_update(self, guild: Optional[discord.Guild]) -> None:
        """Debounce an automatic refresh so bursts of events cause one update."""
        if guild is None:
            return
        if not await self._auto_update_ready(guild):
            return
        pending = self._pending.get(guild.id)
        if pending is not None and not pending.done():
            pending.cancel()
        self._pending[guild.id] = asyncio.create_task(self._delayed_refresh(guild))

    async def _delayed_refresh(self, guild: discord.Guild) -> None:
        try:
            await asyncio.sleep(AUTO_UPDATE_DELAY_SECONDS)
            if not await self._auto_update_ready(guild):
                return
            async with self._lock(guild.id):
                await self._refresh(guild)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Automatic channel list refresh failed for guild %s", guild.id)

    async def _refresh_on_startup(self) -> None:
        """Catch up on channel changes that happened while the bot was offline."""
        try:
            await self.bot.wait_until_red_ready()
            all_guilds = await self.config.all_guilds()
            for guild_id, data in all_guilds.items():
                if not (
                    data.get("auto_update")
                    and data.get("channel_id")
                    and data.get("message_ids")
                ):
                    continue
                guild = self.bot.get_guild(guild_id)
                if guild is None:
                    continue
                try:
                    async with self._lock(guild.id):
                        await self._refresh(guild)
                except Exception:
                    log.exception("Startup channel list refresh failed for guild %s", guild_id)
        except asyncio.CancelledError:
            raise

    @commands.Cog.listener()
    async def on_guild_channel_create(self, channel: discord.abc.GuildChannel):
        await self._schedule_auto_update(channel.guild)

    @commands.Cog.listener()
    async def on_guild_channel_delete(self, channel: discord.abc.GuildChannel):
        await self._schedule_auto_update(channel.guild)

    @commands.Cog.listener()
    async def on_guild_channel_update(
        self, before: discord.abc.GuildChannel, after: discord.abc.GuildChannel
    ):
        if (
            before.name != after.name
            or before.position != after.position
            or before.category_id != after.category_id
            or before.overwrites != after.overwrites
            or getattr(before, "topic", None) != getattr(after, "topic", None)
        ):
            await self._schedule_auto_update(after.guild)

    @commands.Cog.listener()
    async def on_guild_role_update(self, before: discord.Role, after: discord.Role):
        if before.permissions != after.permissions:
            await self._schedule_auto_update(after.guild)

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        await self._schedule_auto_update(role.guild)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        if payload.guild_id is None:
            return
        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return
        if payload.message_id in await self.config.guild(guild).message_ids():
            await self._schedule_auto_update(guild)

    @commands.Cog.listener()
    async def on_raw_bulk_message_delete(self, payload: discord.RawBulkMessageDeleteEvent):
        if payload.guild_id is None:
            return
        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return
        stored = set(await self.config.guild(guild).message_ids())
        if stored & set(payload.message_ids):
            await self._schedule_auto_update(guild)

    # ----------------------------------------------------------------- commands

    def _result_message(self, ctx: commands.Context, result: str) -> str:
        messages = {
            "unchanged": "The channel list is already up to date.",
            "edited": "The channel list was updated by editing the existing messages.",
            "posted": "The channel list has been posted.",
            "reposted": "The channel list was reposted.",
            "unconfigured": (
                f"Set a channel first with `{ctx.clean_prefix}channellistset channel`."
            ),
            "missing_channel": (
                "The configured channel no longer exists. Set a new one with "
                f"`{ctx.clean_prefix}channellistset channel`."
            ),
            "forbidden": (
                "I need permission to view and send messages in the configured channel."
            ),
        }
        return messages.get(result, result)

    @commands.group(name="channellist")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def channellist(self, ctx: commands.Context):
        """Post and maintain the channel list."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @channellist.command(name="post")
    async def channellist_post(
        self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None
    ):
        """Post the channel list, replacing any previously posted list."""
        if channel is not None:
            await self.config.guild(ctx.guild).channel_id.set(channel.id)
        async with self._lock(ctx.guild.id):
            result = await self._refresh(ctx.guild, force_repost=True)
        await ctx.send(self._result_message(ctx, result))

    @channellist.command(name="update")
    async def channellist_update(self, ctx: commands.Context):
        """Update the posted list now, editing it in place when possible."""
        async with self._lock(ctx.guild.id):
            result = await self._refresh(ctx.guild)
        await ctx.send(self._result_message(ctx, result))

    @channellist.command(name="remove")
    async def channellist_remove(self, ctx: commands.Context):
        """Delete the posted channel list."""
        conf = await self.config.guild(ctx.guild).all()
        if not conf["message_ids"]:
            await ctx.send("There is no posted channel list to remove.")
            return
        pending = self._pending.pop(ctx.guild.id, None)
        if pending is not None:
            pending.cancel()
        async with self._lock(ctx.guild.id):
            await self._delete_stored_messages(
                ctx.guild, conf["message_ids"], conf["message_channel_id"]
            )
            await self.config.guild(ctx.guild).message_ids.set([])
            await self.config.guild(ctx.guild).message_channel_id.set(None)
        await ctx.send("Channel list removed.")

    @commands.group(name="channellistset")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def channellistset(self, ctx: commands.Context):
        """Configure the channel list."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @channellistset.command(name="channel")
    async def channellistset_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel the list is posted in."""
        await self.config.guild(ctx.guild).channel_id.set(channel.id)
        await ctx.send(
            f"Channel list channel set to {channel.mention}. "
            f"Run `{ctx.clean_prefix}channellist post` to publish the list there."
        )

    @channellistset.command(name="role")
    async def channellistset_role(
        self, ctx: commands.Context, role: Optional[discord.Role] = None
    ):
        """Base channel visibility on this role.

        Only channels this role can see end up in the list. Leave the role
        empty to fall back to @everyone.
        """
        if role is None:
            await self.config.guild(ctx.guild).role_id.set(None)
            await ctx.send("The list now shows the channels @everyone can see.")
        else:
            await self.config.guild(ctx.guild).role_id.set(role.id)
            await ctx.send(f"The list now shows the channels **{role.name}** can see.")
        await self._schedule_auto_update(ctx.guild)

    @channellistset.command(name="header")
    async def channellistset_header(
        self, ctx: commands.Context, *, header: Optional[str] = None
    ):
        """Set the intro text above the list. Leave it empty to restore the default."""
        if header is None:
            await self.config.guild(ctx.guild).header.set(DEFAULT_HEADER)
            await ctx.send("Header restored to the default.")
        else:
            await self.config.guild(ctx.guild).header.set(header[:MAX_HEADER_LENGTH])
            await ctx.send("Header updated.")
        await self._schedule_auto_update(ctx.guild)

    @channellistset.command(name="emoji")
    async def channellistset_emoji(
        self, ctx: commands.Context, emoji: Optional[str] = None
    ):
        """Set the emoji decorating category headers.

        Leave it empty to restore ⏬, or pass `none` to drop the decoration.
        """
        if emoji is None:
            value = DEFAULT_EMOJI
        elif emoji.strip().lower() in {"none", "off"}:
            value = ""
        else:
            value = emoji.strip()[:80]
        await self.config.guild(ctx.guild).emoji.set(value)
        await ctx.send(
            f"Category headers will use {value}." if value else "Category emoji removed."
        )
        await self._schedule_auto_update(ctx.guild)

    @channellistset.command(name="voice")
    async def channellistset_voice(self, ctx: commands.Context):
        """Toggle whether voice and stage channels are included."""
        current = await self.config.guild(ctx.guild).include_voice()
        await self.config.guild(ctx.guild).include_voice.set(not current)
        if current:
            await ctx.send("Voice channels are now hidden from the list.")
        else:
            await ctx.send("Voice channels are now included in the list.")
        await self._schedule_auto_update(ctx.guild)

    @channellistset.command(name="autoupdate")
    async def channellistset_autoupdate(self, ctx: commands.Context):
        """Toggle automatic updates when channels or permissions change."""
        current = await self.config.guild(ctx.guild).auto_update()
        await self.config.guild(ctx.guild).auto_update.set(not current)
        if current:
            await ctx.send(
                "Automatic updates disabled. Use "
                f"`{ctx.clean_prefix}channellist update` to refresh manually."
            )
        else:
            await ctx.send("Automatic updates enabled.")
            await self._schedule_auto_update(ctx.guild)

    @channellistset.command(name="ignore")
    async def channellistset_ignore(
        self, ctx: commands.Context, *, channel: discord.abc.GuildChannel
    ):
        """Hide a channel or an entire category from the list."""
        async with self.config.guild(ctx.guild).ignored_ids() as ignored:
            if channel.id not in ignored:
                ignored.append(channel.id)
        await ctx.send(f"**{channel.name}** is now hidden from the list.")
        await self._schedule_auto_update(ctx.guild)

    @channellistset.command(name="unignore")
    async def channellistset_unignore(
        self, ctx: commands.Context, *, channel: Union[discord.abc.GuildChannel, int]
    ):
        """Show a previously ignored channel or category again.

        Accepts a raw ID as well, so entries for deleted channels can be cleaned up.
        """
        channel_id = channel if isinstance(channel, int) else channel.id
        async with self.config.guild(ctx.guild).ignored_ids() as ignored:
            if channel_id not in ignored:
                await ctx.send("That channel is not being ignored.")
                return
            ignored.remove(channel_id)
        name = str(channel_id) if isinstance(channel, int) else channel.name
        await ctx.send(f"**{name}** will show up in the list again.")
        await self._schedule_auto_update(ctx.guild)

    @channellistset.command(name="settings")
    async def channellistset_settings(self, ctx: commands.Context):
        """Show the current channel list configuration."""
        conf = await self.config.guild(ctx.guild).all()
        channel = ctx.guild.get_channel(conf["channel_id"]) if conf["channel_id"] else None
        if conf["role_id"]:
            role = ctx.guild.get_role(conf["role_id"])
            role_display = (
                role.name if role else f"deleted role ({conf['role_id']}), using @everyone"
            )
        else:
            role_display = "@everyone"
        ignored_display = []
        for ignored_id in conf["ignored_ids"]:
            target = ctx.guild.get_channel(ignored_id)
            ignored_display.append(target.mention if target else f"deleted ({ignored_id})")

        embed = discord.Embed(title="Channel list settings", color=discord.Color.blurple())
        embed.add_field(
            name="Channel", value=channel.mention if channel else "Not set", inline=True
        )
        embed.add_field(name="Visibility role", value=role_display, inline=True)
        embed.add_field(
            name="Auto update",
            value="Enabled" if conf["auto_update"] else "Disabled",
            inline=True,
        )
        embed.add_field(
            name="Voice channels",
            value="Included" if conf["include_voice"] else "Hidden",
            inline=True,
        )
        embed.add_field(name="Category emoji", value=conf["emoji"] or "None", inline=True)
        embed.add_field(
            name="Posted messages", value=str(len(conf["message_ids"])), inline=True
        )
        embed.add_field(name="Header", value=conf["header"][:1024] or "None", inline=False)
        if ignored_display:
            embed.add_field(
                name="Ignored", value=", ".join(ignored_display)[:1024], inline=False
            )
        await ctx.send(embed=embed)
