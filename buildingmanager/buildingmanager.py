emb.add_field(name="Location Input", value=self.req.location_input[:100], inline=False)
        
        if self.req.coordinates:
            emb.add_field(name="📍 Coordinates", value=self.req.coordinates, inline=True)
        else:
            emb.add_field(name="📍 Coordinates", value="Not detected", inline=True)
        
        if self.req.address:
            emb.add_field(name="📫 Address", value=self.req.address[:200], inline=False)
        
        if self.req.notes:
            emb.add_field(name="Notes", value=self.req.notes[:200], inline=False)
        
        emb.set_footer(text=f"Request ID: {request_id}")

        view = AdminDecisionView(self.cog, requester_id=user.id, req=self.req)
        await admin_channel.send(embed=emb, view=view)

        # Log to log channel
        log_emb = discord.Embed(
            title="Request submitted",
            description=f"By {user.mention}",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        log_emb.add_field(name="Building", value=f"{self.req.building_type} - {self.req.building_name}", inline=False)
        log_emb.add_field(name="Request ID", value=str(request_id), inline=True)
        await log_channel.send(embed=log_emb)

        # Disable all buttons
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

        await safe_update(interaction, content="✅ Request submitted to Admin. You'll be notified of any updates.", embed=None, view=self)

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary, custom_id="bm:cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True
        
        await safe_update(interaction, content="❌ Request cancelled.", embed=None, view=self)

# ---------- Admin Decision ----------

class AdminDecisionView(discord.ui.View):
    def __init__(self, cog: "BuildingManager", requester_id: int, req: BuildingRequest):
        super().__init__(timeout=None)
        self.cog = cog
        self.requester_id = requester_id
        self.req = req

    async def _is_admin(self, interaction: discord.Interaction) -> bool:
        guild = interaction.guild
        if guild is None or not isinstance(interaction.user, discord.Member):
            return False
        role_id = await self.cog.config.guild(guild).admin_role_id()
        if role_id is None:
            return False
        role = guild.get_role(role_id)
        return role in interaction.user.roles if role else False

    @discord.ui.button(label="✅ Approve", style=discord.ButtonStyle.success, custom_id="bm:approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._is_admin(interaction):
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return

        guild = interaction.guild
        conf = await self.cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None

        # Update database
        self.cog.db.update_request_status(self.req.request_id, "approved")
        self.cog.db.add_action(
            request_id=self.req.request_id,
            guild_id=guild.id,
            admin_user_id=interaction.user.id,
            admin_username=str(interaction.user),
            action_type="approved"
        )

        user = guild.get_member(self.requester_id) if guild else None
        emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
        emoji = emoji_map.get(self.req.building_type, "🏢")
        
        ok_text = (
            f"✅ Your building request has been **APPROVED**.\n\n"
            f"{emoji} **{self.req.building_type}**: {self.req.building_name}\n"
        )
        if self.req.coordinates:
            ok_text += f"📍 Coordinates: {self.req.coordinates}\n"
        if self.req.address:
            ok_text += f"📫 Address: {self.req.address}\n"
        if self.req.notes:
            ok_text += f"\nNotes: {self.req.notes}"

        if user:
            try:
                await user.send(ok_text)
            except discord.Forbidden:
                pass

        if log_channel:
            emb = discord.Embed(
                title="Building request approved",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Requester", value=requester, inline=False)
            emb.add_field(name="Building", value=f"{self.req.building_type} - {self.req.building_name}", inline=False)
            if self.req.coordinates:
                emb.add_field(name="Coordinates", value=self.req.coordinates, inline=True)
            emb.add_field(name="Approved by", value=f"{interaction.user.mention} ({interaction.user.id})", inline=False)
            emb.add_field(name="Request ID", value=str(self.req.request_id), inline=True)
            await log_channel.send(embed=emb)

        try:
            await interaction.message.delete()
        except Exception:
            pass

        await interaction.response.send_message("Request approved and processed.", ephemeral=True)

    @discord.ui.button(label="❌ Deny", style=discord.ButtonStyle.danger, custom_id="bm:deny")
    async def deny(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._is_admin(interaction):
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return
        
        # Show denial reason selector
        await interaction.response.send_message(
            "Select a denial reason:",
            view=DenialReasonView(self.cog, self.requester_id, self.req, interaction.message, interaction.user),
            ephemeral=True
        )

class DenialReasonView(discord.ui.View):
    def __init__(self, cog: "BuildingManager", requester_id: int, req: BuildingRequest, admin_msg: discord.Message, admin_user: discord.User):
        super().__init__(timeout=600)
        self.cog = cog
        self.requester_id = requester_id
        self.req = req
        self.admin_msg = admin_msg
        self.admin_user = admin_user
        self.add_item(DenialReasonSelect(self.cog, requester_id, req, admin_msg, admin_user))

class DenialReasonSelect(discord.ui.Select):
    def __init__(self, cog: "BuildingManager", requester_id: int, req: BuildingRequest, admin_msg: discord.Message, admin_user: discord.User):
        self.cog = cog
        self.requester_id = requester_id
        self.req = req
        self.admin_msg = admin_msg
        self.admin_user = admin_user
        
        options = [
            discord.SelectOption(label="Location not found", value="Location not found"),
            discord.SelectOption(label="Not a real-life location", value="Not a real-life location"),
            discord.SelectOption(label="Duplicate building already exists", value="Duplicate building already exists"),
            discord.SelectOption(label="Insufficient detail provided", value="Insufficient detail provided"),
            discord.SelectOption(label="Other (custom reason)", value="custom"),
        ]
        super().__init__(placeholder="Choose a denial reason", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        reason = self.values[0]
        
        if reason == "custom":
            # Show modal for custom reason
            modal = CustomDenialModal(self.cog, self.requester_id, self.req, self.admin_msg, self.admin_user)
            await interaction.response.send_modal(modal)
        else:
            # Process denial with preset reason
            await self._process_denial(interaction, reason)

    async def _process_denial(self, interaction: discord.Interaction, reason: str):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Internal error: no guild.", ephemeral=True)
            return
        
        conf = await self.cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None

        # Update database
        self.cog.db.update_request_status(self.req.request_id, "denied")
        self.cog.db.add_action(
            request_id=self.req.request_id,
            guild_id=guild.id,
            admin_user_id=self.admin_user.id,
            admin_username=str(self.admin_user),
            action_type="denied",
            denial_reason=reason
        )

        user = guild.get_member(self.requester_id)
        text = (
            f"❌ Your building request has been **DENIED**.\n\n"
            f"**Building**: {self.req.building_type} - {self.req.building_name}\n"
            f"**Reason**: {reason}"
        )
        
        if user:
            try:
                await user.send(text)
            except discord.Forbidden:
                pass

        if log_channel:
            emb = discord.Embed(
                title="Building request denied",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Requester", value=requester, inline=False)
            emb.add_field(name="Building", value=f"{self.req.building_type} - {self.req.building_name}", inline=False)
            emb.add_field(name="Reason", value=reason, inline=False)
            emb.add_field(name="Denied by", value=f"{self.admin_user.mention} ({self.admin_user.id})", inline=False)
            emb.add_field(name="Request ID", value=str(self.req.request_id), inline=True)
            await log_channel.send(embed=emb)

        try:
            await self.admin_msg.delete()
        except Exception:
            pass

        await interaction.response.send_message("Denial processed and logged.", ephemeral=True)

class CustomDenialModal(discord.ui.Modal, title="Custom Denial Reason"):
    reason = discord.ui.TextInput(
        label="Reason",
        style=discord.TextStyle.paragraph,
        max_length=400,
        required=True,
        placeholder="Explain why this request is denied...",
    )

    def __init__(self, cog: "BuildingManager", requester_id: int, req: BuildingRequest, admin_msg: discord.Message, admin_user: discord.User):
        super().__init__()
        self.cog = cog
        self.requester_id = requester_id
        self.req = req
        self.admin_msg = admin_msg
        self.admin_user = admin_user

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Internal error: no guild.", ephemeral=True)
            return
        
        conf = await self.cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None

        reason_text = f"Other: {self.reason}"

        # Update database
        self.cog.db.update_request_status(self.req.request_id, "denied")
        self.cog.db.add_action(
            request_id=self.req.request_id,
            guild_id=guild.id,
            admin_user_id=self.admin_user.id,
            admin_username=str(self.admin_user),
            action_type="denied",
            denial_reason=reason_text
        )

        user = guild.get_member(self.requester_id)
        text = (
            f"❌ Your building request has been **DENIED**.\n\n"
            f"**Building**: {self.req.building_type} - {self.req.building_name}\n"
            f"**Reason**: {reason_text}"
        )
        
        if user:
            try:
                await user.send(text)
            except discord.Forbidden:
                pass

        if log_channel:
            emb = discord.Embed(
                title="Building request denied",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Requester", value=requester, inline=False)
            emb.add_field(name="Building", value=f"{self.req.building_type} - {self.req.building_name}", inline=False)
            emb.add_field(name="Reason", value=reason_text, inline=False)
            emb.add_field(name="Denied by", value=f"{self.admin_user.mention} ({self.admin_user.id})", inline=False)
            emb.add_field(name="Request ID", value=str(self.req.request_id), inline=True)
            await log_channel.send(embed=emb)

        try:
            await self.admin_msg.delete()
        except Exception:
            pass

        await interaction.response.send_message("Denial processed and logged.", ephemeral=True)

# ---------- Cog ----------

class BuildingManager(commands.Cog):
    """Building request system with location parsing and statistics."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xDEADBEEF, force_registration=True)
        default_guild = {
            "request_channel_id": None,
            "admin_channel_id": None,
            "log_channel_id": None,
            "admin_role_id": None,
            "button_message": None,
        }
        default_global = {
            "google_api_key": None,
        }
        self.config.register_guild(**default_guild)
        self.config.register_global(**default_global)

        # Initialize database
        db_path = str(self.bot.data_path / "building_manager.db")
        self.db = BuildingDatabase(db_path)

        # Persistent button
        self.bot.add_view(StartView(self))

    def cog_unload(self):
        pass

    # --------------- Commands ---------------

    @commands.group(name="buildset", invoke_without_command=True)
    @commands.admin()
    @commands.guild_only()
    async def buildset(self, ctx: commands.Context):
        """Configure Building Manager."""
        conf = await self.config.guild(ctx.guild).all()
        txt = (
            f"Request channel: {ctx.guild.get_channel(conf['request_channel_id']).mention if conf.get('request_channel_id') else '—'}\n"
            f"Admin channel: {ctx.guild.get_channel(conf['admin_channel_id']).mention if conf.get('admin_channel_id') else '—'}\n"
            f"Log channel: {ctx.guild.get_channel(conf['log_channel_id']).mention if conf.get('log_channel_id') else '—'}\n"
            f"Admin role: {ctx.guild.get_role(conf['admin_role_id']).mention if conf.get('admin_role_id') else '—'}\n"
            f"Custom button message: {'Set' if conf.get('button_message') else 'Not set (using default)'}\n"
        )
        google_key = await self.config.google_api_key()
        txt += f"Google API Key: {'Set' if google_key else 'Not set'}\n"
        
        await ctx.send(box(txt, lang="ini"))

    @buildset.command(name="requestchannel")
    @commands.admin()
    @commands.guild_only()
    async def requestchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where users can request buildings."""
        await self.config.guild(ctx.guild).request_channel_id.set(channel.id)
        await ctx.tick()

    @buildset.command(name="adminchannel")
    @commands.admin()
    @commands.guild_only()
    async def adminchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where admin approval requests are sent."""
        await self.config.guild(ctx.guild).admin_channel_id.set(channel.id)
        await ctx.tick()

    @buildset.command(name="logchannel")
    @commands.admin()
    @commands.guild_only()
    async def logchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where all actions are logged."""
        await self.config.guild(ctx.guild).log_channel_id.set(channel.id)
        await ctx.tick()

    @buildset.command(name="adminrole")
    @commands.admin()
    @commands.guild_only()
    async def adminrole(self, ctx: commands.Context, role: discord.Role):
        """Set the role that can approve/deny building requests."""
        await self.config.guild(ctx.guild).admin_role_id.set(role.id)
        await ctx.tick()

    @buildset.command(name="buttonmessage")
    @commands.admin()
    @commands.guild_only()
    async def buttonmessage(self, ctx: commands.Context, *, message: str = None):
        """Set a custom message above the Request Building button.
        
        Leave empty to reset to default message.
        """
        if message:
            await self.config.guild(ctx.guild).button_message.set(message)
            await ctx.send(f"Custom button message set. Use `{ctx.prefix}buildset post` to update the message.")
        else:
            await self.config.guild(ctx.guild).button_message.set(None)
            await ctx.send(f"Button message reset to default. Use `{ctx.prefix}buildset post` to update the message.")

    @buildset.command(name="googlekey")
    @commands.is_owner()
    async def googlekey(self, ctx: commands.Context, api_key: str = None):
        """Set Google Geocoding API key (optional, for address lookup fallback).
        
        Leave empty to remove the key.
        """
        if api_key:
            await self.config.google_api_key.set(api_key)
            await ctx.send("✅ Google API key set.")
            try:
                await ctx.message.delete()
            except:
                pass
        else:
            await self.config.google_api_key.set(None)
            await ctx.send("Google API key removed.")

    @buildset.command(name="post")
    @commands.admin()
    @commands.guild_only()
    async def post(self, ctx: commands.Context):
        """Post or update the 'Request Building' button in the request channel."""
        request_channel_id = await self.config.guild(ctx.guild).request_channel_id()
        if not request_channel_id:
            await ctx.send("Set the request channel first with `[p]buildset requestchannel #channel`.")
            return
        ch = ctx.guild.get_channel(request_channel_id)
        if not ch:
            await ctx.send("The configured request channel was not found.")
            return
        
        # Get custom message or use default
        custom_msg = await self.config.guild(ctx.guild).button_message()
        if custom_msg:
            description = custom_msg
        else:
            description = (
                "Request a new building placement by clicking the button below.\n\n"
                "You'll be asked to provide:\n"
                "• Building type (Hospital, Prison, etc.)\n"
                "• Building name\n"
                "• Location (Google Maps link or coordinates)\n"
                "• Optional notes\n\n"
                "Your request will be reviewed by admins."
            )
        
        emb = discord.Embed(
            title="🏢 Building Request System",
            description=description,
            color=discord.Color.blue(),
        )
        await ch.send(embed=emb, view=StartView(self))
        await ctx.tick()

    # --------------- Statistics Commands ---------------

    @commands.hybrid_group(name="buildstats", invoke_without_command=True)
    @commands.guild_only()
    async def buildstats(self, ctx: commands.Context):
        """View building request statistics."""
        stats = self.db.get_stats_overall(ctx.guild.id)
        
        status_counts = stats["status_counts"]
        total = sum(status_counts.values())
        
        if total == 0:
            await ctx.send("No building requests have been made yet.")
            return
        
        approved = status_counts.get("approved", 0)
        denied = status_counts.get("denied", 0)
        cancelled = status_counts.get("cancelled", 0)
        pending = status_counts.get("pending", 0)
        
        embed = discord.Embed(
            title="📊 Building Request Statistics",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        
        summary = (
            f"**Total Requests**: {total}\n"
            f"├─ ✅ Approved: {approved} ({approved*100//total if total else 0}%)\n"
            f"├─ ❌ Denied: {denied} ({denied*100//total if total else 0}%)\n"
            f"├─ ⏳ Pending: {pending} ({pending*100//total if total else 0}%)\n"
            f"└─ 🚫 Cancelled: {cancelled} ({cancelled*100//total if total else 0}%)\n"
        )
        embed.add_field(name="Overview", value=summary, inline=False)
        
        # By building type
        type_stats = stats["type_stats"]
        type_summary = {}
        for building_type, status, count in type_stats:
            if building_type not in type_summary:
                type_summary[building_type] = {"approved": 0, "denied": 0, "total": 0}
            type_summary[building_type][status] = count
            type_summary[building_type]["total"] += count
        
        if type_summary:
            type_text = ""
            emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
            for building_type, counts in type_summary.items():
                emoji = emoji_map.get(building_type, "🏢")
                approved_count = counts.get("approved", 0)
                total_count = counts["total"]
                type_text += f"{emoji} **{building_type}**: {total_count} requests ({approved_count} approved)\n"
            embed.add_field(name="By Building Type", value=type_text, inline=False)
        
        # Top requesters
        top_requesters = stats["top_requesters"]
        if top_requesters:
            requester_text = "\n".join([f"{i+1}. {username} - {count} requests" 
                                       for i, (username, count) in enumerate(top_requesters[:5])])
            embed.add_field(name="Top Requesters", value=requester_text, inline=True)
        
        # Top admins
        top_admins = stats["top_admins"]
        if top_admins:
            admin_text = "\n".join([f"{i+1}. {username} - {count} actions" 
                                   for i, (username, count) in enumerate(top_admins[:5])])
            embed.add_field(name="Most Active Admins", value=admin_text, inline=True)
        
        # Average response time
        avg_time = stats["avg_response_time"]
        if avg_time:
            hours = int(avg_time // 3600)
            minutes = int((avg_time % 3600) // 60)
            embed.add_field(name="Average Response Time", value=f"{hours}h {minutes}m", inline=False)
        
        await ctx.send(embed=embed)

    @buildstats.command(name="user")
    @commands.guild_only()
    async def buildstats_user(self, ctx: commands.Context, user: discord.Member = None):
        """View statistics for a specific user."""
        if user is None:
            user = ctx.author
        
        stats = self.db.get_stats_user(ctx.guild.id, user.id)
        
        status_counts = stats["status_counts"]
        total = sum(status_counts.values())
        
        if total == 0:
            await ctx.send(f"{user.mention} has not made any building requests yet.")
            return
        
        approved = status_counts.get("approved", 0)
        denied = status_counts.get("denied", 0)
        cancelled = status_counts.get("cancelled", 0)
        pending = status_counts.get("pending", 0)
        
        embed = discord.Embed(
            title=f"📊 Building Statistics for {user.display_name}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_thumbnail(url=user.display_avatar.url)
        
        summary = (
            f"**Total Requests**: {total}\n"
            f"├─ ✅ Approved: {approved} ({approved*100//total if total else 0}%)\n"
            f"├─ ❌ Denied: {denied} ({denied*100//total if total else 0}%)\n"
            f"├─ ⏳ Pending: {pending}\n"
            f"└─ 🚫 Cancelled: {cancelled}\n"
        )
        embed.add_field(name="Overview", value=summary, inline=False)
        
        # By building type
        type_stats = stats["type_stats"]
        type_summary = {}
        for building_type, status, count in type_stats:
            if building_type not in type_summary:
                type_summary[building_type] = {"approved": 0, "denied": 0}
            type_summary[building_type][status] = count
        
        if type_summary:
            type_text = ""
            emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
            for building_type, counts in type_summary.items():
                emoji = emoji_map.get(building_type, "🏢")
                approved_count = counts.get("approved", 0)
                denied_count = counts.get("denied", 0)
                type_text += f"{emoji} **{building_type}**: {approved_count} approved, {denied_count} denied\n"
            embed.add_field(name="By Building Type", value=type_text, inline=False)
        
        # Denial reasons breakdown
        denial_breakdown = stats["denial_breakdown"]
        if denial_breakdown:
            total_denials = sum([count for reason, count in denial_breakdown])
            denial_text = ""
            for reason, count in denial_breakdown[:5]:
                percentage = (count * 100 // total_denials) if total_denials else 0
                denial_text += f"├─ {reason}: {count} ({percentage}%)\n"
            embed.add_field(name="Denial Breakdown", value=denial_text, inline=False)
        
        # Response times
        response_times = stats["response_times"]
        avg_time, min_time, max_time = response_times
        if avg_time:
            avg_hours = int(avg_time // 3600)
            avg_minutes = int((avg_time % 3600) // 60)
            min_minutes = int(min_time // 60)
            max_hours = int(max_time // 3600)
            max_minutes = int((max_time % 3600) // 60)
            
            time_text = (
                f"Average: {avg_hours}h {avg_minutes}m\n"
                f"Fastest: {min_minutes} minutes\n"
                f"Slowest: {max_hours}h {max_minutes}m"
            )
            embed.add_field(name="Response Times", value=time_text, inline=False)
        
        # Recent actions
        recent_actions = stats["recent_actions"]
        if recent_actions:
            recent_text = ""
            action_emoji = {"approved": "✅", "denied": "❌"}
            for action_type, building_type, username, timestamp in recent_actions[:5]:
                emoji = action_emoji.get(action_type, "")
                time_ago = fmt_dt(timestamp)
                recent_text += f"{emoji} {action_type.capitalize()} {building_type} by {username} ({time_ago})\n"
            embed.add_field(name="Recent Actions (last 5)", value=recent_text, inline=False)
        
        await ctx.send(embed=embed)

    @buildstats.command(name="type")
    @commands.guild_only()
    async def buildstats_type(self, ctx: commands.Context, building_type: str):
        """View statistics for a specific building type (e.g., Hospital, Prison)."""
        # Capitalize first letter for consistency
        building_type = building_type.capitalize()
        
        stats = self.db.get_stats_type(ctx.guild.id, building_type)
        
        status_counts = stats["status_counts"]
        total = sum(status_counts.values())
        
        if total == 0:
            await ctx.send(f"No requests found for building type: {building_type}")
            return
        
        approved = status_counts.get("approved", 0)
        denied = status_counts.get("denied", 0)
        cancelled = status_counts.get("cancelled", 0)
        pending = status_counts.get("pending", 0)
        
        emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
        emoji = emoji_map.get(building_type, "🏢")
        
        embed = discord.Embed(
            title=f"📊 Statistics for {emoji} {building_type}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        
        summary = (
            f"**Total Requests**: {total}\n"
            f"├─ ✅ Approved: {approved} ({approved*100//total if total else 0}%)\n"
            f"├─ ❌ Denied: {denied} ({denied*100//total if total else 0}%)\n"
            f"├─ ⏳ Pending: {pending}\n"
            f"└─ 🚫 Cancelled: {cancelled}\n"
        )
        embed.add_field(name="Overview", value=summary, inline=False)
        
        # Top requesters
        top_requesters = stats["top_requesters"]
        if top_requesters:
            requester_text = "\n".join([f"{i+1}. {username} - {count} requests" 
                                       for i, (username, count) in enumerate(top_requesters[:5])])
            embed.add_field(name="Top Requesters", value=requester_text, inline=False)
        
        # Most common denial reason
        common_denial = stats["common_denial"]
        if common_denial:
            reason, count = common_denial
            embed.add_field(name="Most Common Denial Reason", value=f"{reason} ({count} times)", inline=False)
        
        # Approval rate by admin
        admin_rates = stats["admin_rates"]
        if admin_rates:
            admin_text = ""
            for admin_username, approved_count, total_count in admin_rates[:5]:
                rate = (approved_count * 100 // total_count) if total_count else 0
                admin_text += f"├─ {admin_username}: {rate}% ({approved_count}/{total_count})\n"
            embed.add_field(name="Approval Rate by Admin", value=admin_text, inline=False)
        
        await ctx.send(embed=embed)


async def setup(bot: Red):
    await bot.add_cog(BuildingManager(bot))d_count} denied\n"
            embed.add_field(name="By Building Type", value=type_text, inline=False)
        
        # Denial reasons
        denial_reasons = stats["denial_reasons"]
        if denial_reasons:
            denial_text = "\n".join([f"└─ {reason}: {count}" for reason, count in denial_reasons[:5]])
            embed.add_field(name="Denial Reasons", value=denial_text, inline=False)
        
        # Recent requests
        recent_requests = stats["recent_requests"]
        if recent_requests:
            recent_text = ""
            status_emoji = {"approved": "✅", "denied": "❌", "pending": "⏳", "cancelled": "🚫"}
            for building_type, building_name, status, created_at in recent_requests[:5]:
                emoji = status_emoji.get(status, "")
                time_ago = fmt_dt(created_at)
                recent_text += f"{emoji} {building_type} - {building_name[:30]} ({time_ago})\n"
            embed.add_field(name="Recent Requests (last 5)", value=recent_text, inline=False)
        
        await ctx.send(embed=embed)

    @buildstats.command(name="admin")
    @commands.guild_only()
    async def buildstats_admin(self, ctx: commands.Context, admin: discord.Member = None):
        """View statistics for a specific admin."""
        if admin is None:
            admin = ctx.author
        
        stats = self.db.get_stats_admin(ctx.guild.id, admin.id)
        
        action_counts = stats["action_counts"]
        total = sum(action_counts.values())
        
        if total == 0:
            await ctx.send(f"{admin.mention} has not taken any admin actions yet.")
            return
        
        approved = action_counts.get("approved", 0)
        denied = action_counts.get("denied", 0)
        
        embed = discord.Embed(
            title=f"📊 Admin Statistics for {admin.display_name}",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_thumbnail(url=admin.display_avatar.url)
        
        summary = (
            f"**Total Actions**: {total}\n"
            f"├─ ✅ Approvals: {approved} ({approved*100//total if total else 0}%)\n"
            f"└─ ❌ Denials: {denied} ({denied*100//total if total else 0}%)\n"
        )
        embed.add_field(name="Overview", value=summary, inline=False)
        
        # By building type
        type_stats = stats["type_stats"]
        type_summary = {}
        for building_type, action_type, count in type_stats:
            if building_type not in type_summary:
                type_summary[building_type] = {"approved": 0, "denied": 0}
            type_summary[building_type][action_type] = count
        
        if type_summary:
            type_text = ""
            emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
            for building_type, counts in type_summary.items():
                emoji = emoji_map.get(building_type, "🏢")
                approved_count = counts.get("approved", 0)
                denied_count = counts.get("denied", 0)
                type_text += f"{emoji} **{building_type}**: {approved_count} approved, {denieimport asyncio
import aiohttp
import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote, unquote

import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box, pagify

log = logging.getLogger("red.cog.building_manager")

# ---------- Utilities ----------

def ts() -> int:
    """Get current unix timestamp."""
    return int(datetime.now(timezone.utc).timestamp())

def fmt_dt(timestamp: int) -> str:
    """Format unix timestamp to Discord timestamp."""
    return f"<t:{timestamp}:F>"

async def safe_update(interaction: discord.Interaction, *, content=None, embed=None, view=None):
    """Robust message updater for component/modal callbacks."""
    try:
        if not interaction.response.is_done():
            await interaction.response.edit_message(content=content, embed=embed, view=view)
            return
    except Exception as e:
        log.debug("safe_update: response.edit_message failed: %r", e)
    try:
        if getattr(interaction, "message", None) is not None:
            await interaction.message.edit(content=content, embed=embed, view=view)
            return
    except Exception as e:
        log.debug("safe_update: message.edit failed: %r", e)
    try:
        await interaction.followup.send(content or "Updated.", embed=embed, view=view, ephemeral=True)
    except Exception as e:
        log.exception("safe_update completely failed: %r", e)
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(content or "Updated.", embed=embed, view=view, ephemeral=True)
        except Exception:
            pass

# ---------- Location Parser ----------

class LocationParser:
    """Parse and geocode location inputs."""
    
    # Rate limiting for Nominatim (1 req/sec)
    _last_nominatim_call = 0
    _nominatim_delay = 1.0
    
    @staticmethod
    def extract_coordinates(text: str) -> Optional[Tuple[float, float]]:
        """Extract coordinates from various formats."""
        # Pattern 1: Google Maps ?q=lat,lon
        pattern1 = r'[?&]q=(-?\d+\.?\d*),\s*(-?\d+\.?\d*)'
        match = re.search(pattern1, text)
        if match:
            return (float(match.group(1)), float(match.group(2)))
        
        # Pattern 2: Google Maps /@lat,lon
        pattern2 = r'/@(-?\d+\.?\d*),\s*(-?\d+\.?\d*)'
        match = re.search(pattern2, text)
        if match:
            return (float(match.group(1)), float(match.group(2)))
        
        # Pattern 3: Direct coordinates "lat, lon" or "lat,lon"
        pattern3 = r'^(-?\d+\.?\d*)[,\s]+(-?\d+\.?\d*)$'
        match = re.search(pattern3, text.strip())
        if match:
            return (float(match.group(1)), float(match.group(2)))
        
        # Pattern 4: X: lat, Y: lon format
        pattern4 = r'X:\s*(-?\d+\.?\d*)[,\s]+Y:\s*(-?\d+\.?\d*)'
        match = re.search(pattern4, text, re.IGNORECASE)
        if match:
            return (float(match.group(1)), float(match.group(2)))
        
        return None
    
    @classmethod
    async def geocode_nominatim(cls, lat: float, lon: float) -> Optional[str]:
        """Reverse geocode using Nominatim."""
        # Rate limiting
        now = time.time()
        elapsed = now - cls._last_nominatim_call
        if elapsed < cls._nominatim_delay:
            await asyncio.sleep(cls._nominatim_delay - elapsed)
        
        cls._last_nominatim_call = time.time()
        
        url = f"https://nominatim.openstreetmap.org/reverse"
        params = {
            "lat": lat,
            "lon": lon,
            "format": "json",
            "addressdetails": 1
        }
        headers = {
            "User-Agent": "DiscordBot-BuildingManager/1.0"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, headers=headers, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        display_name = data.get("display_name")
                        return display_name
        except Exception as e:
            log.warning("Nominatim geocoding failed: %r", e)
        
        return None
    
    @staticmethod
    async def geocode_google(lat: float, lon: float, api_key: str) -> Optional[str]:
        """Reverse geocode using Google Geocoding API."""
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "latlng": f"{lat},{lon}",
            "key": api_key
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "OK" and data.get("results"):
                            return data["results"][0].get("formatted_address")
        except Exception as e:
            log.warning("Google geocoding failed: %r", e)
        
        return None

# ---------- Database ----------

class BuildingDatabase:
    """SQLite database for building requests."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize database tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Building requests table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_requests (
                request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                building_type TEXT NOT NULL,
                building_name TEXT NOT NULL,
                location_input TEXT NOT NULL,
                coordinates TEXT,
                address TEXT,
                notes TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        ''')
        
        # Building actions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_actions (
                action_id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                guild_id INTEGER NOT NULL,
                admin_user_id INTEGER,
                admin_username TEXT,
                action_type TEXT NOT NULL,
                denial_reason TEXT,
                previous_values TEXT,
                timestamp INTEGER NOT NULL,
                FOREIGN KEY (request_id) REFERENCES building_requests(request_id)
            )
        ''')
        
        # Geocoding cache table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS geocoding_cache (
                location_input TEXT PRIMARY KEY,
                coordinates TEXT,
                address TEXT,
                provider TEXT,
                cached_at INTEGER
            )
        ''')
        
        # Building types table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS building_types (
                type_id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                type_name TEXT NOT NULL,
                emoji TEXT NOT NULL,
                enabled INTEGER DEFAULT 1,
                created_at INTEGER NOT NULL,
                UNIQUE(guild_id, type_name)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def add_request(self, guild_id: int, user_id: int, username: str, building_type: str,
                   building_name: str, location_input: str, coordinates: Optional[str],
                   address: Optional[str], notes: Optional[str]) -> int:
        """Add a new building request."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = ts()
        cursor.execute('''
            INSERT INTO building_requests 
            (guild_id, user_id, username, building_type, building_name, location_input,
             coordinates, address, notes, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
        ''', (guild_id, user_id, username, building_type, building_name, location_input,
              coordinates, address, notes, now, now))
        
        request_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return request_id
    
    def update_request_status(self, request_id: int, status: str):
        """Update request status."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE building_requests 
            SET status = ?, updated_at = ?
            WHERE request_id = ?
        ''', (status, ts(), request_id))
        
        conn.commit()
        conn.close()
    
    def add_action(self, request_id: int, guild_id: int, admin_user_id: Optional[int],
                  admin_username: Optional[str], action_type: str, denial_reason: Optional[str] = None,
                  previous_values: Optional[str] = None):
        """Log an action on a request."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO building_actions
            (request_id, guild_id, admin_user_id, admin_username, action_type, 
             denial_reason, previous_values, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (request_id, guild_id, admin_user_id, admin_username, action_type,
              denial_reason, previous_values, ts()))
        
        conn.commit()
        conn.close()
    
    def get_cached_geocode(self, location_input: str) -> Optional[Tuple[str, str, str]]:
        """Get cached geocoding result."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT coordinates, address, provider 
            FROM geocoding_cache 
            WHERE location_input = ?
        ''', (location_input,))
        
        result = cursor.fetchone()
        conn.close()
        
        return result if result else None
    
    def cache_geocode(self, location_input: str, coordinates: str, address: str, provider: str):
        """Cache geocoding result."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO geocoding_cache
            (location_input, coordinates, address, provider, cached_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (location_input, coordinates, address, provider, ts()))
        
        conn.commit()
        conn.close()
    
    def get_stats_overall(self, guild_id: int) -> dict:
        """Get overall statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total counts by status
        cursor.execute('''
            SELECT status, COUNT(*) 
            FROM building_requests 
            WHERE guild_id = ?
            GROUP BY status
        ''', (guild_id,))
        status_counts = dict(cursor.fetchall())
        
        # By building type
        cursor.execute('''
            SELECT building_type, status, COUNT(*)
            FROM building_requests
            WHERE guild_id = ?
            GROUP BY building_type, status
        ''', (guild_id,))
        type_stats = cursor.fetchall()
        
        # Top requesters
        cursor.execute('''
            SELECT username, COUNT(*) as count
            FROM building_requests
            WHERE guild_id = ?
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 5
        ''', (guild_id,))
        top_requesters = cursor.fetchall()
        
        # Top admins
        cursor.execute('''
            SELECT admin_username, COUNT(*) as count
            FROM building_actions
            WHERE guild_id = ? AND admin_user_id IS NOT NULL
            GROUP BY admin_user_id
            ORDER BY count DESC
            LIMIT 5
        ''', (guild_id,))
        top_admins = cursor.fetchall()
        
        # Average response time
        cursor.execute('''
            SELECT AVG(ba.timestamp - br.created_at) as avg_time
            FROM building_requests br
            JOIN building_actions ba ON br.request_id = ba.request_id
            WHERE br.guild_id = ? AND ba.action_type IN ('approved', 'denied')
        ''', (guild_id,))
        avg_response_result = cursor.fetchone()
        avg_response_time = avg_response_result[0] if avg_response_result[0] else 0
        
        conn.close()
        
        return {
            "status_counts": status_counts,
            "type_stats": type_stats,
            "top_requesters": top_requesters,
            "top_admins": top_admins,
            "avg_response_time": avg_response_time
        }
    
    def get_stats_user(self, guild_id: int, user_id: int) -> dict:
        """Get user statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total counts by status
        cursor.execute('''
            SELECT status, COUNT(*) 
            FROM building_requests 
            WHERE guild_id = ? AND user_id = ?
            GROUP BY status
        ''', (guild_id, user_id))
        status_counts = dict(cursor.fetchall())
        
        # By building type
        cursor.execute('''
            SELECT building_type, status, COUNT(*)
            FROM building_requests
            WHERE guild_id = ? AND user_id = ?
            GROUP BY building_type, status
        ''', (guild_id, user_id))
        type_stats = cursor.fetchall()
        
        # Denial reasons
        cursor.execute('''
            SELECT ba.denial_reason, COUNT(*) as count
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE br.guild_id = ? AND br.user_id = ? AND ba.action_type = 'denied'
            GROUP BY ba.denial_reason
            ORDER BY count DESC
        ''', (guild_id, user_id))
        denial_reasons = cursor.fetchall()
        
        # Recent requests
        cursor.execute('''
            SELECT building_type, building_name, status, created_at
            FROM building_requests
            WHERE guild_id = ? AND user_id = ?
            ORDER BY created_at DESC
            LIMIT 5
        ''', (guild_id, user_id))
        recent_requests = cursor.fetchall()
        
        conn.close()
        
        return {
            "status_counts": status_counts,
            "type_stats": type_stats,
            "denial_reasons": denial_reasons,
            "recent_requests": recent_requests
        }
    
    def get_stats_admin(self, guild_id: int, admin_user_id: int) -> dict:
        """Get admin statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total actions by type
        cursor.execute('''
            SELECT action_type, COUNT(*)
            FROM building_actions
            WHERE guild_id = ? AND admin_user_id = ?
            GROUP BY action_type
        ''', (guild_id, admin_user_id))
        action_counts = dict(cursor.fetchall())
        
        # By building type
        cursor.execute('''
            SELECT br.building_type, ba.action_type, COUNT(*)
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE ba.guild_id = ? AND ba.admin_user_id = ?
            GROUP BY br.building_type, ba.action_type
        ''', (guild_id, admin_user_id))
        type_stats = cursor.fetchall()
        
        # Denial reasons breakdown
        cursor.execute('''
            SELECT denial_reason, COUNT(*) as count
            FROM building_actions
            WHERE guild_id = ? AND admin_user_id = ? AND action_type = 'denied'
            GROUP BY denial_reason
            ORDER BY count DESC
        ''', (guild_id, admin_user_id))
        denial_breakdown = cursor.fetchall()
        
        # Response times
        cursor.execute('''
            SELECT 
                AVG(ba.timestamp - br.created_at) as avg_time,
                MIN(ba.timestamp - br.created_at) as min_time,
                MAX(ba.timestamp - br.created_at) as max_time
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE ba.guild_id = ? AND ba.admin_user_id = ? AND ba.action_type IN ('approved', 'denied')
        ''', (guild_id, admin_user_id))
        response_times = cursor.fetchone()
        
        # Recent actions
        cursor.execute('''
            SELECT ba.action_type, br.building_type, br.username, ba.timestamp
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE ba.guild_id = ? AND ba.admin_user_id = ?
            ORDER BY ba.timestamp DESC
            LIMIT 5
        ''', (guild_id, admin_user_id))
        recent_actions = cursor.fetchall()
        
        conn.close()
        
        return {
            "action_counts": action_counts,
            "type_stats": type_stats,
            "denial_breakdown": denial_breakdown,
            "response_times": response_times or (0, 0, 0),
            "recent_actions": recent_actions
        }
    
    def get_stats_type(self, guild_id: int, building_type: str) -> dict:
        """Get building type statistics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total counts by status
        cursor.execute('''
            SELECT status, COUNT(*)
            FROM building_requests
            WHERE guild_id = ? AND building_type = ?
            GROUP BY status
        ''', (guild_id, building_type))
        status_counts = dict(cursor.fetchall())
        
        # Top requesters for this type
        cursor.execute('''
            SELECT username, COUNT(*) as count
            FROM building_requests
            WHERE guild_id = ? AND building_type = ?
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 5
        ''', (guild_id, building_type))
        top_requesters = cursor.fetchall()
        
        # Most common denial reason
        cursor.execute('''
            SELECT ba.denial_reason, COUNT(*) as count
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE br.guild_id = ? AND br.building_type = ? AND ba.action_type = 'denied'
            GROUP BY ba.denial_reason
            ORDER BY count DESC
            LIMIT 1
        ''', (guild_id, building_type))
        common_denial = cursor.fetchone()
        
        # Approval rate by admin
        cursor.execute('''
            SELECT 
                ba.admin_username,
                SUM(CASE WHEN ba.action_type = 'approved' THEN 1 ELSE 0 END) as approved,
                COUNT(*) as total
            FROM building_actions ba
            JOIN building_requests br ON ba.request_id = br.request_id
            WHERE br.guild_id = ? AND br.building_type = ? 
              AND ba.action_type IN ('approved', 'denied')
              AND ba.admin_user_id IS NOT NULL
            GROUP BY ba.admin_user_id
        ''', (guild_id, building_type))
        admin_rates = cursor.fetchall()
        
        conn.close()
        
        return {
            "status_counts": status_counts,
            "top_requesters": top_requesters,
            "common_denial": common_denial,
            "admin_rates": admin_rates
        }

# ---------- Models ----------

class BuildingRequest:
    def __init__(
        self,
        user_id: int,
        username: str,
        building_type: str,
        building_name: str,
        location_input: str,
        coordinates: Optional[str] = None,
        address: Optional[str] = None,
        notes: Optional[str] = None,
        request_id: Optional[int] = None,
    ):
        self.user_id = user_id
        self.username = username
        self.building_type = building_type
        self.building_name = building_name
        self.location_input = location_input
        self.coordinates = coordinates
        self.address = address
        self.notes = notes
        self.request_id = request_id

# ---------- Views ----------

class StartView(discord.ui.View):
    def __init__(self, cog: "BuildingManager"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Request Building", style=discord.ButtonStyle.primary, custom_id="bm:start")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Select the type of building you want to request.",
            view=BuildingTypeView(self.cog),
            ephemeral=True,
        )

class BuildingTypeView(discord.ui.View):
    def __init__(self, cog: "BuildingManager"):
        super().__init__(timeout=600)
        self.cog = cog
        self.add_item(BuildingTypeSelect(self.cog))

class BuildingTypeSelect(discord.ui.Select):
    def __init__(self, cog: "BuildingManager"):
        self.cog = cog
        # Default types
        options = [
            discord.SelectOption(label="Hospital", emoji="🏥", description="Medical facility"),
            discord.SelectOption(label="Prison", emoji="🔒", description="Correctional facility"),
        ]
        super().__init__(placeholder="Choose a building type", min_values=1, max_values=1, options=options, custom_id="bm:type")

    async def callback(self, interaction: discord.Interaction):
        building_type = self.values[0]
        modal = BuildingRequestModal(self.cog, building_type)
        await interaction.response.send_modal(modal)

class BuildingRequestModal(discord.ui.Modal, title="Building Request"):
    building_name = discord.ui.TextInput(
        label="Building Name",
        style=discord.TextStyle.short,
        max_length=100,
        required=True,
        placeholder="e.g., Central Medical Center",
    )
    
    location = discord.ui.TextInput(
        label="Location (Google Maps link or coordinates)",
        style=discord.TextStyle.short,
        max_length=500,
        required=True,
        placeholder="Paste Google Maps link, coordinates, or description",
    )
    
    notes = discord.ui.TextInput(
        label="Additional Notes (Optional)",
        style=discord.TextStyle.paragraph,
        max_length=500,
        required=False,
        placeholder="Any additional information...",
    )

    def __init__(self, cog: "BuildingManager", building_type: str):
        super().__init__()
        self.cog = cog
        self.building_type = building_type

    async def on_submit(self, interaction: discord.Interaction):
        # Parse location
        await interaction.response.defer(ephemeral=True)
        
        location_input = str(self.location)
        coords = LocationParser.extract_coordinates(location_input)
        
        coordinates_str = None
        address = None
        
        if coords:
            lat, lon = coords
            coordinates_str = f"{lat}, {lon}"
            
            # Check cache first
            cached = self.cog.db.get_cached_geocode(location_input)
            if cached:
                _, address, _ = cached
            else:
                # Try Nominatim first
                address = await LocationParser.geocode_nominatim(lat, lon)
                provider = "nominatim"
                
                # Fallback to Google if available and Nominatim failed
                if not address:
                    google_key = await self.cog.config.google_api_key()
                    if google_key:
                        address = await LocationParser.geocode_google(lat, lon, google_key)
                        provider = "google"
                
                # Cache result
                if address:
                    self.cog.db.cache_geocode(location_input, coordinates_str, address, provider)
        
        # Create request object
        req = BuildingRequest(
            user_id=interaction.user.id,
            username=str(interaction.user),
            building_type=self.building_type,
            building_name=str(self.building_name),
            location_input=location_input,
            coordinates=coordinates_str,
            address=address,
            notes=str(self.notes) if self.notes.value else None,
        )
        
        # Show summary
        view = SummaryView(self.cog, req)
        await view.send_summary(interaction)

class SummaryView(discord.ui.View):
    def __init__(self, cog: "BuildingManager", req: BuildingRequest):
        super().__init__(timeout=600)
        self.cog = cog
        self.req = req

    async def send_summary(self, interaction: discord.Interaction):
        """Display the summary embed."""
        embed = self._create_embed(interaction.user)
        await safe_update(
            interaction,
            content="⚠️ **Warning**: Once submitted, you cannot edit this request!\n\nReview your request:",
            embed=embed,
            view=self
        )

    def _create_embed(self, user: discord.User) -> discord.Embed:
        """Create summary embed."""
        emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
        emoji = emoji_map.get(self.req.building_type, "🏢")
        
        embed = discord.Embed(
            title=f"{emoji} Building Request Summary",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        
        embed.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Building Type", value=self.req.building_type, inline=True)
        embed.add_field(name="Building Name", value=self.req.building_name, inline=True)
        embed.add_field(name="Location Input", value=self.req.location_input[:100], inline=False)
        
        if self.req.coordinates:
            embed.add_field(name="📍 Coordinates", value=self.req.coordinates, inline=True)
        else:
            embed.add_field(name="📍 Coordinates", value="Not detected", inline=True)
        
        if self.req.address:
            embed.add_field(name="📫 Address", value=self.req.address[:200], inline=False)
        
        if self.req.notes:
            embed.add_field(name="Notes", value=self.req.notes[:200], inline=False)
        
        return embed

    @discord.ui.button(label="✏️ Edit", style=discord.ButtonStyle.secondary, custom_id="bm:edit")
    async def edit(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = BuildingRequestModal(self.cog, self.req.building_type)
        modal.building_name.default = self.req.building_name
        modal.location.default = self.req.location_input
        if self.req.notes:
            modal.notes.default = self.req.notes
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="📤 Submit to Admin", style=discord.ButtonStyle.success, custom_id="bm:submit")
    async def submit(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This only works inside a server.", ephemeral=True)
            return

        conf = await self.cog.config.guild(guild).all()
        admin_channel_id = conf.get("admin_channel_id")
        log_channel_id = conf.get("log_channel_id")

        if not admin_channel_id or not log_channel_id:
            await interaction.response.send_message(
                "Admin/Log channels are not configured yet. Ask an admin to use [p]buildset.",
                ephemeral=True,
            )
            return

        admin_channel = guild.get_channel(admin_channel_id)
        log_channel = guild.get_channel(log_channel_id)

        if not admin_channel or not log_channel:
            await interaction.response.send_message("One or more configured channels could not be found.", ephemeral=True)
            return

        # Save to database
        request_id = self.cog.db.add_request(
            guild_id=guild.id,
            user_id=self.req.user_id,
            username=self.req.username,
            building_type=self.req.building_type,
            building_name=self.req.building_name,
            location_input=self.req.location_input,
            coordinates=self.req.coordinates,
            address=self.req.address,
            notes=self.req.notes
        )
        
        self.req.request_id = request_id

        # Send to admin channel
        emoji_map = {"Hospital": "🏥", "Prison": "🔒"}
        emoji = emoji_map.get(self.req.building_type, "🏢")
        
        emb = discord.Embed(
            title=f"{emoji} New Building Request",
            color=discord.Color.yellow(),
            timestamp=datetime.now(timezone.utc),
        )
        
        user = interaction.user
        emb.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        emb.add_field(name="Building Type", value=self.req.building_type, inline=True)
        emb.add_field(name="Building Name", value=self.req.building_name, inline=True)
        emb.add_field(name="Location Input", value=self.req.location_input[:100], inline=False)
