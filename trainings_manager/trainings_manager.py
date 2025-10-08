import asyncio
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple

import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box

log = logging.getLogger("red.cog.trainings_manager")

AMS = ZoneInfo("Europe/Amsterdam")

# ---------- Utilities ----------

def ts(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def fmt_dt(dt: datetime) -> str:
    unix = ts(dt)
    return f"<t:{unix}:F> (Amsterdam)"

async def safe_update(interaction: discord.Interaction, *, content=None, embed=None, view=None):
    """Robust message updater for component/modal callbacks.
    Tries response.edit_message first; if already responded, edits the message;
    if that's not possible, sends a followup ephemeral.
    """
    try:
        if not interaction.response.is_done():
            await interaction.response.edit_message(content=content, embed=embed, view=view)
            return
    except Exception:
        pass
    # Try editing the message directly
    try:
        if getattr(interaction, "message", None) is not None:
            await interaction.message.edit(content=content, embed=embed, view=view)
            return
    except Exception:
        pass
    # Fallback: followup ephemeral
    try:
        await interaction.followup.send(content or "Updated.", embed=embed, view=view, ephemeral=True)
    except Exception as e:
        log.exception("safe_update failed: %r", e)

# ---------- Static data ----------

DISCIPLINES: Dict[str, List[Tuple[str, int]]] = {
    "Coastal": [
        ("Coastal Air Rescue Operations", 5),
        ("Lifeguard Supervisor", 5),
        ("Lifeguard Training", 5),
        ("Ocean Navigation", 5),
        ("Sharpshooter Training", 5),
        ("Swift water rescue", 4),
        ("TACLET", 3),
    ],
    "Police": [
        ("Drone Operator", 5),
        ("Environmental Game Warden", 4),
        ("FBI Bomb Technician", 5),
        ("FBI Mobile Center Commander", 7),
        ("K-9", 5),
        ("Ocean Navigation", 5),
        ("Police Aviation", 7),
        ("Police Motorcycle", 3),
        ("Police Operations Management", 5),
        ("Police Supervisor / Sheriff", 5),
        ("Riot Police Training", 3),
        ("SWAT", 5),
        ("Sharpshooter Training", 5),
        ("Swift water rescue", 4),
        ("Tactical Rescue Training", 5),
        ("Traffic Control Training", 3),
    ],
    "Fire": [
        ("ALS Medical Training for Fire Apparatus", 3),
        ("ARFF-Training", 3),
        ("Airborne firefighting", 5),
        ("Critical Care", 5),
        ("EMS Mobile Command", 7),
        ("HazMat", 3),
        ("Heavy Machinery Operating", 3),
        ("Hotshot Crew Training", 3),
        ("Law Enforcement for Arson Investigation", 4),
        ("Lifeguard Supervisor", 5),
        ("Lifeguard Training", 5),
        ("Mobile command", 5),
        ("Ocean Navigation", 5),
        ("Search and Rescue Training", 4),
        ("Smoke Jumper Training", 3),
        ("Swift water rescue", 4),
        ("Tactical Medic Training", 4),
        ("Technical Rescue Training", 4),
        ("Traffic Control Training", 3),
        ("Truck Driver's License", 2),
        ("Wildland Lead Pilot Training", 7),
        ("Wildland Mobile Command Center Training", 5),
    ],
    "EMS": [
        ("ALS Medical Training for Fire Apparatus", 3),
        ("Critical Care", 5),
        ("EMS Mobile Command", 7),
        ("Hazmat Medic Training", 3),
        ("Tactical Medic Training", 4),
        ("Truck Driver's License", 2),
    ],
    "TEST": [
        ("TEST DEBUG", 0), 
    ],
}

FEE_CHOICES = [0, 100, 200, 300, 400, 500]

# ---------- Model ----------

class TrainingRequest:
    def __init__(
        self,
        user_id: int,
        discipline: str,
        training: str,
        days: int,
        fee_per_day: int,
        reference: Optional[str],
        want_reminder: bool,
        request_channel_id: int,
        summary_message_link: Optional[str] = None,
        reminder_only: bool = False,
    ):
        self.user_id = user_id
        self.discipline = discipline
        self.training = training
        self.days = days
        self.fee_per_day = fee_per_day
        self.reference = reference
        self.want_reminder = want_reminder
        self.request_channel_id = request_channel_id
        self.summary_message_link = summary_message_link
        self.reminder_only = reminder_only

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "discipline": self.discipline,
            "training": self.training,
            "days": self.days,
            "fee_per_day": self.fee_per_day,
            "reference": self.reference,
            "want_reminder": self.want_reminder,
            "request_channel_id": self.request_channel_id,
            "summary_message_link": self.summary_message_link,
            "reminder_only": self.reminder_only,
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            user_id=data["user_id"],
            discipline=data["discipline"],
            training=data["training"],
            days=int(data["days"]),
            fee_per_day=int(data["fee_per_day"]),
            reference=data.get("reference"),
            want_reminder=bool(data.get("want_reminder", False)),
            request_channel_id=int(data["request_channel_id"]),
            summary_message_link=data.get("summary_message_link"),
            reminder_only=bool(data.get("reminder_only", False)),
        )

# ---------- Views ----------

class StartView(discord.ui.View):
    def __init__(self, cog: "TrainingManager"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Start Request", style=discord.ButtonStyle.primary, custom_id="tm:start_request")
    async def start_request(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Let's set up your training request. First, pick a discipline.",
            view=DisciplineView(self.cog, reminder_only=False),
            ephemeral=True,
        )

    @discord.ui.button(label="Reminder Only", style=discord.ButtonStyle.secondary, custom_id="tm:reminder_only")
    async def reminder_only(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Let's set up a reminder for your training. First, pick a discipline.",
            view=DisciplineView(self.cog, reminder_only=True),
            ephemeral=True,
        )

class DisciplineView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", reminder_only: bool = False):
        super().__init__(timeout=600)
        self.cog = cog
        self.reminder_only = reminder_only
        self.add_item(DisciplineSelect(self.cog, reminder_only))

class DisciplineSelect(discord.ui.Select):
    def __init__(self, cog: "TrainingManager", reminder_only: bool = False):
        self.cog = cog
        self.reminder_only = reminder_only
        options = [discord.SelectOption(label=k, description=f"{len(v)} trainings") for k, v in DISCIPLINES.items()]
        super().__init__(placeholder="Choose a discipline", min_values=1, max_values=1, options=options, custom_id="tm:discipline")

    async def callback(self, interaction: discord.Interaction):
        discipline = self.values[0]
        await safe_update(
            interaction,
            content=f"Discipline selected: **{discipline}**. Now choose a training.",
            view=TrainingView(self.cog, discipline, self.reminder_only),
        )

class TrainingView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, reminder_only: bool = False):
        super().__init__(timeout=600)
        self.cog = cog
        self.discipline = discipline
        self.reminder_only = reminder_only
        self.add_item(TrainingSelect(self.cog, discipline, reminder_only))

class TrainingSelect(discord.ui.Select):
    def __init__(self, cog: "TrainingManager", discipline: str, reminder_only: bool = False):
        self.cog = cog
        self.discipline = discipline
        self.reminder_only = reminder_only
        options = []
        for name, days in DISCIPLINES[discipline]:
            label = name
            desc = f"Duration: {days} day" + ("" if days == 1 else "s")
            options.append(discord.SelectOption(label=label, description=desc))
        super().__init__(placeholder="Choose a training", min_values=1, max_values=1, options=options, custom_id="tm:training")

    async def callback(self, interaction: discord.Interaction):
        training = self.values[0]
        days = next(days for name, days in DISCIPLINES[self.discipline] if name == training)
        
        if self.reminder_only:
            # Skip fee selection for reminder only
            await safe_update(
                interaction,
                content=f"Selected: **{self.discipline} → {training}** ({days} days). Do you want to add a reference?",
                view=ReferenceAskView(self.cog, self.discipline, training, days, 0, reminder_only=True),
            )
        else:
            await safe_update(
                interaction,
                content=f"Selected: **{self.discipline} → {training}**. Now pick the fee per day, per trainee.",
                view=FeeView(self.cog, self.discipline, training, days),
            )

class FeeView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int):
        super().__init__(timeout=600)
        self.cog = cog
        self.discipline = discipline
        self.training = training
        self.days = days
        for fee in FEE_CHOICES:
            label = "Free" if fee == 0 else f"{fee} credits/day"
            self.add_item(FeeButton(label, fee))

class FeeButton(discord.ui.Button):
    def __init__(self, label: str, fee: int):
        super().__init__(label=label, style=discord.ButtonStyle.secondary, custom_id=f"tm:fee:{label}")
        self.fee = fee

    async def callback(self, interaction: discord.Interaction):
        view: FeeView = self.view  # type: ignore
        await safe_update(
            interaction,
            content=(
                f"Selected: **{view.discipline} → {view.training}** for "
                f"**{'Free' if self.fee == 0 else str(self.fee) + ' credits/day'}**.\n"
                "Do you want to add a reference?"
            ),
            view=ReferenceAskView(view.cog, view.discipline, view.training, view.days, self.fee, reminder_only=False),
        )

class ReferenceAskView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, fee: int, reminder_only: bool = False):
        super().__init__(timeout=600)
        self.cog = cog
        self.state = (discipline, training, days, fee, reminder_only)

    @discord.ui.button(label="Yes, add a reference", style=discord.ButtonStyle.primary, custom_id="tm:ref_yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ReferenceModal(self.cog, *self.state))

    @discord.ui.button(label="No, continue", style=discord.ButtonStyle.secondary, custom_id="tm:ref_no")
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee, reminder_only = self.state
        if reminder_only:
            await safe_update(
                interaction,
                content="Reference skipped.",
                view=ReminderOnlySummaryView(self.cog, interaction.user.id, discipline, training, days, None),
            )
        else:
            await safe_update(
                interaction,
                content="Reference skipped.",
                view=SummaryView(self.cog, interaction.user.id, discipline, training, days, fee, None),
            )

class ReferenceModal(discord.ui.Modal, title="Add reference"):
    ref = discord.ui.TextInput(
        label="Short description (max 100 characters)",
        style=discord.TextStyle.short,
        max_length=100,
        required=True,
        placeholder="e.g., SWAT Team East, batch 3",
    )

    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, fee: int, reminder_only: bool = False):
        super().__init__()
        self.cog = cog
        self.state = (discipline, training, days, fee, reminder_only)

    async def on_submit(self, interaction: discord.Interaction):
        discipline, training, days, fee, reminder_only = self.state
        if reminder_only:
            await safe_update(
                interaction,
                content="Reference added.",
                view=ReminderOnlySummaryView(self.cog, interaction.user.id, discipline, training, days, str(self.ref)),
            )
        else:
            await safe_update(
                interaction,
                content="Reference added.",
                view=SummaryView(self.cog, interaction.user.id, discipline, training, days, fee, str(self.ref)),
            )

# ---------- Reminder Only Summary View ----------

class ReminderOnlySummaryView(discord.ui.View):
    def __init__(
        self,
        cog: "TrainingManager",
        user_id: int,
        discipline: str,
        training: str,
        days: int,
        reference: Optional[str],
    ):
        super().__init__(timeout=600)
        self.cog = cog
        self.req = TrainingRequest(
            user_id=user_id,
            discipline=discipline,
            training=training,
            days=days,
            fee_per_day=0,
            reference=reference,
            want_reminder=True,
            request_channel_id=0,
            reminder_only=True,
        )

    @discord.ui.button(label="Start Reminder", style=discord.ButtonStyle.success, custom_id="tm:start_reminder")
    async def start_reminder(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This only works inside a server.", ephemeral=True)
            return

        conf = await self.cog.config.guild(guild).all()
        log_channel_id = conf.get("log_channel_id")
        request_channel_id = conf.get("request_channel_id")

        if not log_channel_id or not request_channel_id:
            await interaction.response.send_message(
                "Log/Request channels are not configured yet. Ask an admin to use [p]tmset.",
                ephemeral=True,
            )
            return

        log_channel = guild.get_channel(log_channel_id)
        request_channel = guild.get_channel(request_channel_id)

        if not log_channel or not request_channel:
            await interaction.response.send_message("One or more configured channels could not be found.", ephemeral=True)
            return

        self.req.request_channel_id = request_channel.id
        end_at = datetime.now(AMS) + timedelta(days=self.req.days)

        # Add reminder
        await self.cog._add_reminder(
            guild_id=guild.id,
            user_id=interaction.user.id,
            text=f"Your **{self.req.training}** class has finished." + (f" Reference: {self.req.reference}" if self.req.reference else ""),
            when=end_at.astimezone(timezone.utc),
            fallback_channel_id=self.req.request_channel_id,
        )

        # Log to log channel
        emb = discord.Embed(
            title="Reminder created",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        user = interaction.user
        emb.add_field(name="User", value=f"{user.mention} ({user.id})", inline=False)
        emb.add_field(name="Training", value=f"{self.req.discipline} → {self.req.training} ({self.req.days}d)", inline=False)
        if self.req.reference:
            emb.add_field(name="Reference", value=self.req.reference, inline=False)
        emb.add_field(name="End time", value=fmt_dt(end_at), inline=False)
        await log_channel.send(embed=emb)

        # Confirm to user
        await safe_update(
            interaction,
            content=f"Reminder set! You'll be notified when your **{self.req.training}** class finishes on {fmt_dt(end_at)}.",
            embed=None,
            view=None
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, custom_id="tm:cancel_reminder")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_update(interaction, content="Reminder cancelled.", embed=None, view=None)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        user = interaction.user
        end_at = datetime.now(AMS) + timedelta(days=self.req.days)
        embed = discord.Embed(
            title="Reminder - Summary",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="User", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Discipline", value=self.req.discipline, inline=True)
        embed.add_field(name="Training", value=self.req.training, inline=True)
        embed.add_field(name="Duration", value=f"{self.req.days} days", inline=True)
        embed.add_field(name="Expected end time", value=fmt_dt(end_at), inline=False)
        embed.add_field(name="Reference", value=self.req.reference or "—", inline=False)
        embed.set_footer(text="Click 'Start Reminder' to confirm or 'Cancel' to abort.")

        await safe_update(interaction, content="Review your reminder:", embed=embed, view=self)
        return False  # Prevent default interaction handling

# Reminder buttons
class ReminderOn(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Notify at end: On", style=discord.ButtonStyle.success, custom_id="tm:rem_on")

    async def callback(self, interaction: discord.Interaction):
        parent: SummaryView = self.view  # type: ignore
        parent.req.want_reminder = True
        await parent.send_or_update(interaction)

class ReminderOff(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Notify at end: Off", style=discord.ButtonStyle.secondary, custom_id="tm:rem_off")

    async def callback(self, interaction: discord.Interaction):
        parent: SummaryView = self.view  # type: ignore
        parent.req.want_reminder = False
        await parent.send_or_update(interaction)

class SubmitButton(discord.ui.Button):
    def __init__(self, parent_view: "SummaryView"):
        super().__init__(label="Send to Admin", style=discord.ButtonStyle.success, custom_id="tm:submit")
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        cog: TrainingManager = self.parent_view.cog  # type: ignore
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This only works inside a server.", ephemeral=True)
            return

        conf = await cog.config.guild(guild).all()
        admin_channel_id = conf.get("admin_channel_id")
        log_channel_id = conf.get("log_channel_id")
        request_channel_id = conf.get("request_channel_id")

        if not admin_channel_id or not log_channel_id or not request_channel_id:
            await interaction.response.send_message(
                "Admin/Log/Request channels are not configured yet. Ask an admin to use [p]tmset.",
                ephemeral=True,
            )
            return

        admin_channel = guild.get_channel(admin_channel_id)
        log_channel = guild.get_channel(log_channel_id)
        request_channel = guild.get_channel(request_channel_id)

        if not admin_channel or not log_channel or not request_channel:
            await interaction.response.send_message("One or more configured channels could not be found.", ephemeral=True)
            return

        req = self.parent_view.req
        req.request_channel_id = request_channel.id

        end_at = datetime.now(AMS) + timedelta(days=req.days)
        emb = discord.Embed(
            title="New training request",
            color=discord.Color.yellow(),
            timestamp=datetime.now(timezone.utc),
        )
        user = interaction.user
        emb.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        emb.add_field(name="Discipline", value=req.discipline, inline=True)
        emb.add_field(name="Training", value=req.training, inline=True)
        emb.add_field(name="Duration", value=f"{req.days} days", inline=True)
        fee_txt = "Free" if req.fee_per_day == 0 else f"{req.fee_per_day} credits/day/trainee"
        emb.add_field(name="Fee", value=fee_txt, inline=True)
        emb.add_field(name="Reference", value=req.reference or "—", inline=False)
        emb.add_field(name="Expected end time", value=fmt_dt(end_at), inline=False)
        emb.set_footer(text="Use the buttons below to approve or reject.")

        view = AdminDecisionView(cog, requester_id=user.id, req=req)
        await admin_channel.send(embed=emb, view=view)

        queue_emb = discord.Embed(
            title="Request submitted",
            description=f"By {user.mention} in {request_channel.mention}.",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        queue_emb.add_field(name="Training", value=f"{req.discipline} → {req.training} ({req.days}d)", inline=False)
        if req.reference:
            queue_emb.add_field(name="Reference", value=req.reference, inline=False)
        await log_channel.send(embed=queue_emb)

        await safe_update(interaction, content="Sent to Admin. You'll be notified on any change.", embed=None, view=None)

class SummaryView(discord.ui.View):
    def __init__(
        self,
        cog: "TrainingManager",
        user_id: int,
        discipline: str,
        training: str,
        days: int,
        fee: int,
        reference: Optional[str],
    ):
        super().__init__(timeout=600)
        self.cog = cog
        self.req = TrainingRequest(
            user_id=user_id,
            discipline=discipline,
            training=training,
            days=days,
            fee_per_day=fee,
            reference=reference,
            want_reminder=False,
            request_channel_id=0,
        )
        # Reminder buttons + submit
        self.add_item(ReminderOff())
        self.add_item(ReminderOn())
        self.add_item(SubmitButton(self))

    async def send_or_update(self, interaction: discord.Interaction):
        user = interaction.user
        end_at = datetime.now(AMS) + timedelta(days=self.req.days)
        embed = discord.Embed(
            title="Training request - Summary",
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Discipline", value=self.req.discipline, inline=True)
        embed.add_field(name="Training", value=self.req.training, inline=True)
        embed.add_field(name="Duration", value=f"{self.req.days} days", inline=True)
        fee_txt = "Free" if self.req.fee_per_day == 0 else f"{self.req.fee_per_day} credits/day/trainee"
        embed.add_field(name="Fee", value=fee_txt, inline=True)
        embed.add_field(name="Expected end time", value=fmt_dt(end_at), inline=False)
        embed.add_field(name="Reference", value=self.req.reference or "—", inline=False)
        embed.add_field(name="Notify when class finishes", value="Yes" if self.req.want_reminder else "No", inline=True)

        # Update button styles to reflect state
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.custom_id == "tm:rem_on":
                    child.style = discord.ButtonStyle.success if self.req.want_reminder else discord.ButtonStyle.secondary
                if child.custom_id == "tm:rem_off":
                    child.style = discord.ButtonStyle.success if not self.req.want_reminder else discord.ButtonStyle.secondary

        await safe_update(interaction, content="Review and submit to Admin.", embed=embed, view=self)

# ---------- Admin decision ----------

class AdminDecisionView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", requester_id: int, req: TrainingRequest):
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

    @discord.ui.button(label="Start Education", style=discord.ButtonStyle.success, custom_id="tm:approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._is_admin(interaction):
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return

        guild = interaction.guild
        conf = await self.cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None

        user = guild.get_member(self.requester_id) if guild else None
        end_at = datetime.now(AMS) + timedelta(days=self.req.days)
        ok_text = (
            f"Your training request has been **APPROVED**.\n"
            f"**{self.req.discipline} → {self.req.training}** for "
            f"{'Free' if self.req.fee_per_day == 0 else str(self.req.fee_per_day) + ' credits/day/trainee'}.\n"
            f"End time: {fmt_dt(end_at)}."
        )
        if self.req.reference:
            ok_text += f"\nReference: {self.req.reference}"

        if user:
            try:
                await user.send(ok_text)
            except discord.Forbidden:
                pass

        if self.req.want_reminder:
            await self.cog._add_reminder(
                guild_id=guild.id,
                user_id=self.requester_id,
                text=f"Your **{self.req.training}** class has finished." + (f" Reference: {self.req.reference}" if self.req.reference else ""),
                when=end_at.astimezone(timezone.utc),
                fallback_channel_id=self.req.request_channel_id,
            )

        if log_channel:
            emb = discord.Embed(
                title="Training approved",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Requester", value=requester, inline=False)
            emb.add_field(name="Training", value=f"{self.req.discipline} → {self.req.training} ({self.req.days}d)", inline=False)
            fee_txt = "Free" if self.req.fee_per_day == 0 else f"{self.req.fee_per_day} c/day"
            emb.add_field(name="Fee", value=fee_txt, inline=True)
            if self.req.reference:
                emb.add_field(name="Reference", value=self.req.reference, inline=False)
            emb.add_field(name="Reminder", value="Yes" if self.req.want_reminder else "No", inline=True)
            emb.add_field(name="End time", value=fmt_dt(end_at), inline=False)
            emb.add_field(name="Approved by", value=f"{interaction.user.mention} ({interaction.user.id})", inline=False)
            await log_channel.send(embed=emb)

        try:
            await interaction.message.delete()
        except Exception:
            pass

        await interaction.response.send_message("Request approved and processed.", ephemeral=True)

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger, custom_id="tm:reject")
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._is_admin(interaction):
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return
        await interaction.response.send_modal(RejectModal(self.cog, self.requester_id, self.req, admin_msg=interaction.message, admin_user=interaction.user))

class RejectModal(discord.ui.Modal, title="Rejection reason"):
    reason = discord.ui.TextInput(
        label="Reason",
        style=discord.TextStyle.paragraph,
        max_length=400,
        required=True,
        placeholder="Briefly explain why this request is rejected.",
    )

    def __init__(self, cog: "TrainingManager", requester_id: int, req: TrainingRequest, admin_msg: discord.Message, admin_user: discord.User):
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

        user = guild.get_member(self.requester_id)
        text = (
            f"Your training request for **{self.req.discipline} → {self.req.training}** has been **REJECTED**.\n"
            f"Reason: {self.reason}"
        )
        if user:
            try:
                await user.send(text)
            except discord.Forbidden:
                ch = guild.get_channel(self.req.request_channel_id)
                if ch:
                    await ch.send(f"{user.mention} {text}")

        if log_channel:
            emb = discord.Embed(
                title="Training rejected",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Requester", value=requester, inline=False)
            emb.add_field(name="Training", value=f"{self.req.discipline} → {self.req.training}", inline=False)
            emb.add_field(name="Reason", value=str(self.reason), inline=False)
            emb.add_field(name="Rejected by", value=f"{self.admin_user.mention} ({self.admin_user.id})", inline=False)
            await log_channel.send(embed=emb)

        try:
            await self.admin_msg.delete()
        except Exception:
            pass

        await interaction.response.send_message("Rejection forwarded and logged.", ephemeral=True)

# ---------- Cog ----------

class TrainingManager(commands.Cog):
    """Training requests with approvals and end-of-class reminders."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xBEEFCAFE, force_registration=True)
        default_guild = {
            "request_channel_id": None,
            "admin_channel_id": None,
            "log_channel_id": None,
            "admin_role_id": None,
            "reminders": [],  # list of dicts: {user_id, text, when_ts, fallback_channel_id}
            "button_message": None,  # Custom message for the start buttons
        }
        self.config.register_guild(**default_guild)

        # Persistent Start button
        self.bot.add_view(StartView(self))

        # Background reminder loop
        self._reminder_task = self.bot.loop.create_task(self._reminder_loop())

    def cog_unload(self):
        if self._reminder_task:
            self._reminder_task.cancel()

    # --------------- Reminder machinery ---------------

    async def _add_reminder(self, guild_id: int, user_id: int, text: str, when, fallback_channel_id: int):
        """Persist a reminder and let the loop pick it up."""
        if isinstance(when, datetime):
            when_ts = int(when.replace(tzinfo=timezone.utc).timestamp())
        else:
            when_ts = int(when)
        async with self.config.guild_from_id(guild_id).reminders() as rems:
            rems.append({
                "user_id": int(user_id),
                "text": str(text),
                "when_ts": int(when_ts),
                "fallback_channel_id": int(fallback_channel_id),
            })

    async def _reminder_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                for guild in self.bot.guilds:
                    conf = await self.config.guild(guild).all()
                    rems = conf.get("reminders", [])
                    now_ts = int(datetime.now(timezone.utc).timestamp())
                    due = [r for r in rems if r.get("when_ts", 10**18) <= now_ts]
                    if not due:
                        continue
                    keep: List[dict] = []
                    for r in rems:
                        if r in due:
                            await self._deliver_reminder(guild, r)
                        else:
                            keep.append(r)
                    await self.config.guild(guild).reminders.set(keep)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("Reminder loop error: %r", e)
            await asyncio.sleep(30)

    async def _deliver_reminder(self, guild: discord.Guild, r: dict):
        user = guild.get_member(int(r["user_id"]))
        text = str(r["text"])
        # Try DM first
        sent = False
        if user:
            try:
                await user.send(text)
                sent = True
            except discord.Forbidden:
                sent = False
        if not sent:
            ch = guild.get_channel(int(r["fallback_channel_id"]))
            if ch:
                try:
                    await ch.send(f"<@{user.id}> {text}" if user else text)
                except Exception:
                    pass

    # --------------- Commands ---------------

    @commands.group(name="tmset", invoke_without_command=True)
    @commands.admin()
    async def tmset(self, ctx: commands.Context):
        """Configure Training Manager. Subcommands: requestchannel, adminchannel, logchannel, adminrole, buttonmessage, post."""
        conf = await self.config.guild(ctx.guild).all()
        txt = (
            f"Request channel: {ctx.guild.get_channel(conf['request_channel_id']).mention if conf.get('request_channel_id') else '—'}\n"
            f"Admin channel: {ctx.guild.get_channel(conf['admin_channel_id']).mention if conf.get('admin_channel_id') else '—'}\n"
            f"Log channel: {ctx.guild.get_channel(conf['log_channel_id']).mention if conf.get('log_channel_id') else '—'}\n"
            f"Admin role: {ctx.guild.get_role(conf['admin_role_id']).mention if conf.get('admin_role_id') else '—'}\n"
            f"Custom button message: {'Set' if conf.get('button_message') else 'Not set (using default)'}\n"
        )
        await ctx.send(box(txt, lang="ini"))

    @tmset.command()
    @commands.admin()
    async def requestchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where users can request trainings."""
        await self.config.guild(ctx.guild).request_channel_id.set(channel.id)
        await ctx.tick()

    @tmset.command()
    @commands.admin()
    async def adminchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where admin approval requests are sent."""
        await self.config.guild(ctx.guild).admin_channel_id.set(channel.id)
        await ctx.tick()

    @tmset.command()
    @commands.admin()
    async def logchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where all actions are logged."""
        await self.config.guild(ctx.guild).log_channel_id.set(channel.id)
        await ctx.tick()

    @tmset.command()
    @commands.admin()
    async def adminrole(self, ctx: commands.Context, role: discord.Role):
        """Set the role that can approve/reject training requests."""
        await self.config.guild(ctx.guild).admin_role_id.set(role.id)
        await ctx.tick()

    @tmset.command()
    @commands.admin()
    async def buttonmessage(self, ctx: commands.Context, *, message: str = None):
        """Set a custom message above the Start Request and Reminder Only buttons.
        
        Use this to explain the difference between the two options.
        Leave empty to reset to default message.
        
        Example: [p]tmset buttonmessage Use **Start Request** to submit a full training request that requires admin approval. Use **Reminder Only** to simply set a reminder for yourself without admin approval.
        """
        if message:
            await self.config.guild(ctx.guild).button_message.set(message)
            await ctx.send(f"Custom button message set. Use `{ctx.prefix}tmset post` to update the message in the request channel.")
        else:
            await self.config.guild(ctx.guild).button_message.set(None)
            await ctx.send(f"Button message reset to default. Use `{ctx.prefix}tmset post` to update the message in the request channel.")

    @tmset.command()
    @commands.admin()
    async def post(self, ctx: commands.Context):
        """Post or update the 'Start Request' and 'Reminder Only' buttons in the request channel."""
        request_channel_id = await self.config.guild(ctx.guild).request_channel_id()
        if not request_channel_id:
            await ctx.send("Set the request channel first with `[p]tmset requestchannel #channel`.")
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
                "**Start Request**: Submit a full training request with fee and admin approval required.\n"
                "**Reminder Only**: Set a reminder for your training without needing admin approval.\n\n"
                "Choose an option below to get started."
            )
        
        emb = discord.Embed(
            title="Training Request System",
            description=description,
            color=discord.Color.blurple(),
        )
        await ch.send(embed=emb, view=StartView(self))
        await ctx.tick()
