import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box, humanize_list
import asyncio
from typing import Optional, List, Union
from datetime import datetime
import io


class ForumThreadMover(commands.Cog):
    """Move text channel conversations to forum channels."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890, force_registration=True)
        self.admin_role_id = 544117282167586836
        self.log_channel_id = 668874839012016170
        
        # Throttle delay between posts (in seconds)
        self.throttle_delay = 1.5
        
        # Retry settings
        self.max_retries = 3
        self.retry_delay = 2

    async def _log_action(self, guild: discord.Guild, message: str, color: discord.Color = discord.Color.blue()):
        """Log actions to the configured log channel."""
        log_channel = guild.get_channel(self.log_channel_id)
        if log_channel and isinstance(log_channel, discord.TextChannel):
            embed = discord.Embed(
                description=message,
                color=color,
                timestamp=datetime.utcnow()
            )
            embed.set_footer(text=f"Guild: {guild.name}")
            try:
                await log_channel.send(embed=embed)
            except discord.HTTPException:
                pass  # Silently fail if logging fails

    def _has_admin_role(self, member: discord.Member) -> bool:
        """Check if member has the admin role."""
        return any(role.id == self.admin_role_id for role in member.roles)

    async def _fetch_messages_safely(self, channel: discord.TextChannel, start_message_id: int, count: int) -> List[discord.Message]:
        """Fetch messages with retry logic."""
        for attempt in range(self.max_retries):
            try:
                # Fetch the starting message
                start_message = await channel.fetch_message(start_message_id)
                messages = [start_message]
                
                # Fetch subsequent messages
                async for message in channel.history(after=start_message, limit=count, oldest_first=True):
                    messages.append(message)
                
                return messages
            except discord.NotFound:
                raise  # Don't retry if message doesn't exist
            except discord.HTTPException as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    raise

    async def _create_forum_post_safely(self, forum: discord.ForumChannel, title: str, content: str, **kwargs) -> discord.Thread:
        """Create forum post with retry logic."""
        for attempt in range(self.max_retries):
            try:
                thread = await forum.create_thread(name=title, content=content, **kwargs)
                return thread
            except discord.HTTPException as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    raise

    async def _send_message_safely(self, target: Union[discord.Thread, discord.TextChannel], content: str = None, **kwargs) -> Optional[discord.Message]:
        """Send message with retry logic."""
        for attempt in range(self.max_retries):
            try:
                return await target.send(content=content, **kwargs)
            except discord.HTTPException as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(self.retry_delay)
                else:
                    raise

    async def _handle_attachments(self, message: discord.Message) -> List[discord.File]:
        """Download and prepare attachments for re-upload."""
        files = []
        cdn_urls = []
        
        for attachment in message.attachments:
            # Check size limit (25MB for most servers, 8MB for non-boosted)
            if attachment.size > 25 * 1024 * 1024:
                cdn_urls.append(f"📎 [{attachment.filename}]({attachment.url}) (File too large, see original)")
                continue
            
            try:
                file_data = await attachment.read()
                files.append(discord.File(io.BytesIO(file_data), filename=attachment.filename))
            except discord.HTTPException:
                cdn_urls.append(f"📎 [{attachment.filename}]({attachment.url}) (Download failed, see original)")
        
        return files, cdn_urls

    def _recreate_embed(self, embed: discord.Embed) -> Optional[discord.Embed]:
        """Recreate a rich embed, or return None if it's a special embed type."""
        if embed.type != "rich":
            # For link previews, video embeds, etc., we can't recreate them
            return None
        
        new_embed = discord.Embed(
            title=embed.title,
            description=embed.description,
            url=embed.url,
            color=embed.color,
            timestamp=embed.timestamp
        )
        
        if embed.author:
            new_embed.set_author(
                name=embed.author.name,
                url=embed.author.url,
                icon_url=embed.author.icon_url
            )
        
        for field in embed.fields:
            new_embed.add_field(name=field.name, value=field.value, inline=field.inline)
        
        if embed.footer:
            new_embed.set_footer(text=embed.footer.text, icon_url=embed.footer.icon_url)
        
        if embed.image:
            new_embed.set_image(url=embed.image.url)
        
        if embed.thumbnail:
            new_embed.set_thumbnail(url=embed.thumbnail.url)
        
        return new_embed

    async def _format_message_content(self, message: discord.Message, source_channel: discord.TextChannel) -> tuple[str, List[discord.Embed], List[discord.File]]:
        """Format a message for posting in forum thread."""
        # Build header with author and timestamp
        header = f"**@{message.author.name}** • <t:{int(message.created_at.timestamp())}:f>\n"
        
        # Check if this was a reply
        reply_context = ""
        if message.reference and message.reference.resolved:
            replied_msg = message.reference.resolved
            replied_content = replied_msg.content[:80] + "..." if len(replied_msg.content) > 80 else replied_msg.content
            reply_context = f"↪️ In reply to @{replied_msg.author.name}: \"{replied_content}\"\n\n"
        
        # Main content
        content = message.content if message.content else ""
        
        # Handle attachments
        files, cdn_urls = await self._handle_attachments(message)
        
        # Add CDN URLs for large files
        if cdn_urls:
            content += "\n\n" + "\n".join(cdn_urls)
        
        # Handle embeds
        embeds = []
        embed_notes = []
        
        for embed in message.embeds:
            recreated = self._recreate_embed(embed)
            if recreated:
                embeds.append(recreated)
            else:
                # Non-rich embed (link preview, etc.)
                if embed.url:
                    embed_notes.append(f"[🔗 Original link preview]({embed.url})")
                else:
                    embed_notes.append("🔗 Original embed (cannot be copied)")
        
        if embed_notes:
            content += "\n\n" + "\n".join(embed_notes)
        
        # Check for interactive components
        if message.components:
            content += "\n\n*Interactive components are not copied. See original message [here](" + message.jump_url + ").*"
        
        # Add footer
        footer = f"\n\n*━━━━━━━━━━━━━━━━━━━━*\n*(Moved from {source_channel.mention} on {datetime.utcnow().strftime('%Y-%m-%d')})*"
        
        # Combine all parts
        full_content = header + reply_context + content + footer
        
        # Discord message limit is 2000 characters
        if len(full_content) > 2000:
            # Truncate content if needed
            available = 2000 - len(header) - len(reply_context) - len(footer) - 50
            content = content[:available] + f"...\n\n[See original message]({message.jump_url})"
            full_content = header + reply_context + content + footer
        
        return full_content, embeds, files

    @commands.command(name="movequestion")
    async def move_question(
        self,
        ctx: commands.Context,
        message_id: int,
        count: int,
        forum_channel: discord.ForumChannel,
        *,
        title: Optional[str] = None
    ):
        """
        Move a conversation from a text channel to a forum channel.
        
        **Arguments:**
        - `message_id`: The ID of the starting message (the question)
        - `count`: Number of messages to move after the starting message
        - `forum_channel`: The target forum channel (mention or ID)
        - `title`: Optional title for the forum post (default: first 80 chars of question)
        
        **Example:**
        `[p]movequestion 1437763879160647740 10 #helpdesk Purpose of Own Vehicle Class`
        """
        # Permission check
        if not self._has_admin_role(ctx.author):
            await ctx.send("❌ You need the admin role to use this command.")
            return
        
        # Validate inputs
        if count < 0:
            await ctx.send("❌ Count must be 0 or greater.")
            return
        
        if count > 100:
            await ctx.send("❌ Maximum count is 100 messages to avoid rate limits.")
            return
        
        # Check if source is a text channel
        if not isinstance(ctx.channel, discord.TextChannel):
            await ctx.send("❌ This command can only be used in text channels.")
            return
        
        # Check if message is in a thread
        try:
            original_msg = await ctx.channel.fetch_message(message_id)
            if isinstance(original_msg.channel, discord.Thread):
                await ctx.send("⚠️ Warning: The message appears to be in a thread. This might cause unexpected behavior.")
        except discord.NotFound:
            await ctx.send(f"❌ Message with ID `{message_id}` not found in this channel.")
            return
        except discord.HTTPException as e:
            await ctx.send(f"❌ Error fetching message: {e}")
            return
        
        # Check permissions
        bot_permissions = forum_channel.permissions_for(ctx.guild.me)
        required_perms = ["create_posts", "send_messages", "attach_files", "embed_links", "manage_threads"]
        missing_perms = [perm for perm in required_perms if not getattr(bot_permissions, perm, False)]
        
        if missing_perms:
            await ctx.send(f"❌ I'm missing these permissions in {forum_channel.mention}: {humanize_list(missing_perms)}")
            return
        
        # Create progress embed
        progress_embed = discord.Embed(
            title="📦 Moving Conversation",
            description="Preparing to move messages...",
            color=discord.Color.blue()
        )
        progress_embed.add_field(name="Status", value="🔄 Fetching messages...", inline=False)
        progress_msg = await ctx.send(embed=progress_embed)
        
        try:
            # Fetch messages
            progress_embed.set_field_at(0, name="Status", value="🔄 Fetching messages...", inline=False)
            await progress_msg.edit(embed=progress_embed)
            
            messages = await self._fetch_messages_safely(ctx.channel, message_id, count)
            
            if not messages:
                await progress_msg.edit(content="❌ No messages found.")
                return
            
            # Determine title
            if not title:
                first_content = messages[0].content or "Untitled"
                title = first_content[:80] + ("..." if len(first_content) > 80 else "")
            
            # Ensure title isn't empty
            if not title or title.strip() == "":
                title = "Moved Conversation"
            
            # Create forum post
            progress_embed.set_field_at(0, name="Status", value=f"🔄 Creating forum post: '{title}'...", inline=False)
            await progress_msg.edit(embed=progress_embed)
            
            # Format first message (the question)
            first_content, first_embeds, first_files = await self._format_message_content(messages[0], ctx.channel)
            
            # Create the forum thread
            thread = await self._create_forum_post_safely(
                forum_channel,
                title=title,
                content=first_content,
                files=first_files,
                embeds=first_embeds,
                allowed_mentions=discord.AllowedMentions.none()
            )
            
            # Post remaining messages
            total_messages = len(messages)
            for idx, message in enumerate(messages[1:], start=2):
                progress_embed.set_field_at(
                    0,
                    name="Status",
                    value=f"🔄 Posting message {idx}/{total_messages}...",
                    inline=False
                )
                await progress_msg.edit(embed=progress_embed)
                
                # Format message
                msg_content, msg_embeds, msg_files = await self._format_message_content(message, ctx.channel)
                
                # Post to thread
                await self._send_message_safely(
                    thread,
                    content=msg_content,
                    files=msg_files,
                    embeds=msg_embeds,
                    allowed_mentions=discord.AllowedMentions.none()
                )
                
                # Throttle to avoid rate limits
                await asyncio.sleep(self.throttle_delay)
            
            # Success!
            success_embed = discord.Embed(
                title="✅ Conversation Moved",
                description=f"This discussion was moved to {thread.mention}",
                color=discord.Color.green()
            )
            success_embed.add_field(name="Messages Moved", value=str(total_messages), inline=True)
            success_embed.add_field(name="Forum Post", value=f"[{title}]({thread.jump_url})", inline=False)
            await progress_msg.edit(embed=success_embed)
            
            # Log the action
            await self._log_action(
                ctx.guild,
                f"**Conversation Moved**\n"
                f"Moderator: {ctx.author.mention}\n"
                f"From: {ctx.channel.mention}\n"
                f"To: {forum_channel.mention}\n"
                f"Thread: [{title}]({thread.jump_url})\n"
                f"Messages: {total_messages}",
                discord.Color.green()
            )
            
        except discord.Forbidden:
            await progress_msg.edit(content="❌ I don't have permission to perform this action.")
            await self._log_action(ctx.guild, f"**Move Failed**: Permission denied in {forum_channel.mention}", discord.Color.red())
        except discord.HTTPException as e:
            await progress_msg.edit(content=f"❌ An error occurred: {e}")
            await self._log_action(ctx.guild, f"**Move Failed**: {e}", discord.Color.red())
        except Exception as e:
            await progress_msg.edit(content=f"❌ Unexpected error: {e}")
            await self._log_action(ctx.guild, f"**Move Failed**: {type(e).__name__}: {e}", discord.Color.red())

    @commands.command(name="topictitle")
    async def topic_title(self, ctx: commands.Context, thread: discord.Thread, *, new_title: str):
        """
        Change the title of a forum thread.
        
        **Arguments:**
        - `thread`: The forum thread (link or ID)
        - `new_title`: The new title for the thread
        
        **Example:**
        `[p]topictitle https://discord.com/channels/.../... New Title Here`
        """
        # Permission check
        if not self._has_admin_role(ctx.author):
            await ctx.send("❌ You need the admin role to use this command.")
            return
        
        # Check if it's a forum thread
        if not hasattr(thread.parent, 'type') or thread.parent.type != discord.ChannelType.forum:
            await ctx.send("❌ This command only works with forum threads.")
            return
        
        # Check permissions
        bot_permissions = thread.permissions_for(ctx.guild.me)
        if not bot_permissions.manage_threads:
            await ctx.send(f"❌ I need the 'Manage Threads' permission in {thread.parent.mention}.")
            return
        
        try:
            old_title = thread.name
            await thread.edit(name=new_title[:100])  # Discord limit is 100 chars
            
            await ctx.send(f"✅ Thread title changed from `{old_title}` to `{new_title}`")
            
            # Log the action
            await self._log_action(
                ctx.guild,
                f"**Thread Title Changed**\n"
                f"Moderator: {ctx.author.mention}\n"
                f"Thread: [{new_title}]({thread.jump_url})\n"
                f"Old Title: {old_title}",
                discord.Color.blue()
            )
        except discord.Forbidden:
            await ctx.send("❌ I don't have permission to edit this thread.")
        except discord.HTTPException as e:
            await ctx.send(f"❌ Error: {e}")

    @commands.command(name="moveinto")
    async def move_into(
        self,
        ctx: commands.Context,
        thread: discord.Thread,
        message_id: int,
        count: int
    ):
        """
        Move messages into an existing forum thread.
        
        **Arguments:**
        - `thread`: The target forum thread (link or ID)
        - `message_id`: The ID of the starting message
        - `count`: Number of messages to move after the starting message
        
        **Example:**
        `[p]moveinto https://discord.com/channels/.../... 1437763879160647740 5`
        """
        # Permission check
        if not self._has_admin_role(ctx.author):
            await ctx.send("❌ You need the admin role to use this command.")
            return
        
        # Validate inputs
        if count < 0:
            await ctx.send("❌ Count must be 0 or greater.")
            return
        
        if count > 100:
            await ctx.send("❌ Maximum count is 100 messages to avoid rate limits.")
            return
        
        # Check if target is a forum thread
        if not hasattr(thread.parent, 'type') or thread.parent.type != discord.ChannelType.forum:
            await ctx.send("❌ Target must be a forum thread.")
            return
        
        # Check permissions
        bot_permissions = thread.permissions_for(ctx.guild.me)
        required_perms = ["send_messages", "attach_files", "embed_links"]
        missing_perms = [perm for perm in required_perms if not getattr(bot_permissions, perm, False)]
        
        if missing_perms:
            await ctx.send(f"❌ I'm missing these permissions in {thread.mention}: {humanize_list(missing_perms)}")
            return
        
        # Create progress embed
        progress_embed = discord.Embed(
            title="📦 Moving Messages",
            description=f"Moving messages to {thread.mention}...",
            color=discord.Color.blue()
        )
        progress_embed.add_field(name="Status", value="🔄 Fetching messages...", inline=False)
        progress_msg = await ctx.send(embed=progress_embed)
        
        try:
            # Fetch messages
            messages = await self._fetch_messages_safely(ctx.channel, message_id, count)
            
            if not messages:
                await progress_msg.edit(content="❌ No messages found.")
                return
            
            # Post messages to thread
            total_messages = len(messages)
            for idx, message in enumerate(messages, start=1):
                progress_embed.set_field_at(
                    0,
                    name="Status",
                    value=f"🔄 Posting message {idx}/{total_messages}...",
                    inline=False
                )
                await progress_msg.edit(embed=progress_embed)
                
                # Format message
                msg_content, msg_embeds, msg_files = await self._format_message_content(message, ctx.channel)
                
                # Post to thread
                await self._send_message_safely(
                    thread,
                    content=msg_content,
                    files=msg_files,
                    embeds=msg_embeds,
                    allowed_mentions=discord.AllowedMentions.none()
                )
                
                # Throttle
                await asyncio.sleep(self.throttle_delay)
            
            # Success!
            success_embed = discord.Embed(
                title="✅ Messages Moved",
                description=f"Messages have been added to {thread.mention}",
                color=discord.Color.green()
            )
            success_embed.add_field(name="Messages Added", value=str(total_messages), inline=True)
            await progress_msg.edit(embed=success_embed)
            
            # Log the action
            await self._log_action(
                ctx.guild,
                f"**Messages Added to Thread**\n"
                f"Moderator: {ctx.author.mention}\n"
                f"From: {ctx.channel.mention}\n"
                f"To: [{thread.name}]({thread.jump_url})\n"
                f"Messages: {total_messages}",
                discord.Color.green()
            )
            
        except discord.Forbidden:
            await progress_msg.edit(content="❌ I don't have permission to perform this action.")
            await self._log_action(ctx.guild, f"**Move Into Failed**: Permission denied", discord.Color.red())
        except discord.HTTPException as e:
            await progress_msg.edit(content=f"❌ An error occurred: {e}")
            await self._log_action(ctx.guild, f"**Move Into Failed**: {e}", discord.Color.red())
        except Exception as e:
            await progress_msg.edit(content=f"❌ Unexpected error: {e}")
            await self._log_action(ctx.guild, f"**Move Into Failed**: {type(e).__name__}: {e}", discord.Color.red())
