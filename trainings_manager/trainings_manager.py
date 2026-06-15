from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from html.parser import HTMLParser
import logging
import re
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
        ("Hooklift Truck Driving", 4),
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
        ("Mountain Dog Training", 5),
        ("Mountain Rescue Certificate", 5)
    ],
    "OTHER": [
        ("Not specified (USE REFERENCE)", 0),
        ("TEST DEBUG (DO NOT USE)", 0),
    ],
}

FEE_CHOICES = [0, 100, 200, 300, 400, 500]
DEVELOPER_PANEL_CHANNEL_ID = 1421242306136113254
AUTO_ACADEMY_BUILDINGS = {
    "Fire": 4951748,
    "Police": 4951746,
}
AUTO_ALLIANCE_DURATION_SECONDS = 3600
AUTO_MIN_CONTRIBUTION_RATE = 5.0
AUTO_MAX_CLASSES = 4


@dataclass
class AcademyCourse:
    label: str
    normalized_label: str
    value: str


@dataclass
class AcademyPage:
    action: Optional[str]
    authenticity_token: Optional[str]
    available_rooms: int
    costs: List[int]
    courses: List[AcademyCourse]


@dataclass
class AutoTrainingResult:
    success: bool
    reason: str
    academy_id: Optional[int] = None
    mc_user_id: Optional[str] = None
    mc_username: Optional[str] = None
    contribution_rate: Optional[float] = None
    course_value: Optional[str] = None
    classes_opened: int = 0
    status: Optional[int] = None


def _normalize_training_name(name: str) -> str:
    cleaned = re.sub(r"\(\s*\d+\s+days?\s*\)", "", str(name), flags=re.IGNORECASE)
    cleaned = cleaned.replace("’", "'")
    return re.sub(r"\s+", " ", cleaned).strip().casefold()


class AcademyPageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.action: Optional[str] = None
        self.authenticity_token: Optional[str] = None
        self.room_options: List[int] = []
        self.cost_options: List[int] = []
        self.courses: List[AcademyCourse] = []
        self._select_name: Optional[str] = None
        self._current_option_value: Optional[str] = None
        self._current_option_text: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        attr = {key: value for key, value in attrs}
        if tag == "form":
            action = attr.get("action")
            if action and "/education" in action:
                self.action = action
        elif tag == "input" and attr.get("name") == "authenticity_token":
            self.authenticity_token = attr.get("value")
        elif tag == "select":
            self._select_name = attr.get("name")
        elif tag == "option" and self._select_name:
            self._current_option_value = attr.get("value") or ""
            self._current_option_text = []

    def handle_data(self, data: str):
        if self._current_option_value is not None:
            self._current_option_text.append(data)

    def handle_endtag(self, tag: str):
        if tag == "option" and self._select_name and self._current_option_value is not None:
            text = re.sub(r"\s+", " ", "".join(self._current_option_text)).strip()
            value = self._current_option_value
            if self._select_name == "building_rooms_use":
                try:
                    self.room_options.append(int(value))
                except ValueError:
                    pass
            elif self._select_name == "alliance[cost]":
                try:
                    self.cost_options.append(int(value))
                except ValueError:
                    pass
            elif self._select_name == "education_select" and value and text:
                self.courses.append(
                    AcademyCourse(
                        label=text,
                        normalized_label=_normalize_training_name(text),
                        value=value,
                    )
                )
            self._current_option_value = None
            self._current_option_text = []
        elif tag == "select":
            self._select_name = None

    def page(self) -> AcademyPage:
        return AcademyPage(
            action=self.action,
            authenticity_token=self.authenticity_token,
            available_rooms=max(self.room_options) if self.room_options else 0,
            costs=sorted(set(self.cost_options)),
            courses=self.courses,
        )


def parse_academy_page(html: str) -> AcademyPage:
    parser = AcademyPageParser()
    parser.feed(html)
    return parser.page()

# ---------- Model ----------

class TrainingRequest:
    def __init__(
        self,
        user_id: int,
        discipline: str,
        training: str,
        days: int,
        fee_per_day: int,
        num_classes: int,
        references: Optional[List[str]],
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
        self.num_classes = num_classes
        self.references = references or []
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
            "num_classes": self.num_classes,
            "references": self.references,
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
            num_classes=int(data.get("num_classes", 1)),
            references=data.get("references", []),
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
            # Skip fee and class count for reminder only
            await safe_update(
                interaction,
                content=f"Selected: **{self.discipline} → {training}** ({days} days). Do you want to add a reference?",
                view=ReferenceAskView(self.cog, self.discipline, training, days, 0, 1, reminder_only=True),
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
                "How many classes do you want to request?"
            ),
            view=ClassCountView(view.cog, view.discipline, view.training, view.days, self.fee),
        )

class ClassCountView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, fee: int):
        super().__init__(timeout=600)
        self.cog = cog
        self.state = (discipline, training, days, fee)

    @discord.ui.button(label="No, just one", style=discord.ButtonStyle.primary, custom_id="tm:class_1")
    async def one_class(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee = self.state
        await safe_update(
            interaction,
            content="1 class selected. Do you want to add a reference?",
            view=ReferenceAskView(self.cog, discipline, training, days, fee, 1, reminder_only=False),
        )

    @discord.ui.button(label="2", style=discord.ButtonStyle.secondary, custom_id="tm:class_2")
    async def two_classes(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee = self.state
        await safe_update(
            interaction,
            content="2 classes selected. Do you want to add references?",
            view=ReferenceAskView(self.cog, discipline, training, days, fee, 2, reminder_only=False),
        )

    @discord.ui.button(label="3", style=discord.ButtonStyle.secondary, custom_id="tm:class_3")
    async def three_classes(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee = self.state
        await safe_update(
            interaction,
            content="3 classes selected. Do you want to add references?",
            view=ReferenceAskView(self.cog, discipline, training, days, fee, 3, reminder_only=False),
        )

    @discord.ui.button(label="4", style=discord.ButtonStyle.secondary, custom_id="tm:class_4")
    async def four_classes(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee = self.state
        await safe_update(
            interaction,
            content="4 classes selected. Do you want to add references?",
            view=ReferenceAskView(self.cog, discipline, training, days, fee, 4, reminder_only=False),
        )

class ReferenceAskView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, fee: int, num_classes: int, reminder_only: bool = False):
        super().__init__(timeout=600)
        self.cog = cog
        self.state = (discipline, training, days, fee, num_classes, reminder_only)

    @discord.ui.button(label="Yes, add references", style=discord.ButtonStyle.primary, custom_id="tm:ref_yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee, num_classes, reminder_only = self.state
        
        if num_classes > 1 and not reminder_only:
            # Ask if same or individual
            await safe_update(
                interaction,
                content="Do you want the same reference for all classes, or individual references?",
                view=ReferenceModeView(self.cog, discipline, training, days, fee, num_classes, reminder_only),
            )
        else:
            # Single reference
            await interaction.response.send_modal(ReferenceModal(self.cog, discipline, training, days, fee, num_classes, reminder_only, mode="single"))

    @discord.ui.button(label="No, continue", style=discord.ButtonStyle.secondary, custom_id="tm:ref_no")
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee, num_classes, reminder_only = self.state
        if reminder_only:
            view = ReminderOnlySummaryView(self.cog, interaction.user.id, discipline, training, days, [])
            await view.send_summary(interaction)
        else:
            await safe_update(
                interaction,
                content="References skipped.",
                view=SummaryView(self.cog, interaction.user.id, discipline, training, days, fee, num_classes, []),
            )

class ReferenceModeView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, fee: int, num_classes: int, reminder_only: bool):
        super().__init__(timeout=600)
        self.cog = cog
        self.state = (discipline, training, days, fee, num_classes, reminder_only)

    @discord.ui.button(label="Same for all", style=discord.ButtonStyle.primary, custom_id="tm:ref_same")
    async def same_ref(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee, num_classes, reminder_only = self.state
        await interaction.response.send_modal(ReferenceModal(self.cog, discipline, training, days, fee, num_classes, reminder_only, mode="same"))

    @discord.ui.button(label="Individual", style=discord.ButtonStyle.secondary, custom_id="tm:ref_individual")
    async def individual_ref(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee, num_classes, reminder_only = self.state
        await interaction.response.send_modal(ReferenceModal(self.cog, discipline, training, days, fee, num_classes, reminder_only, mode="individual"))

class ReferenceModal(discord.ui.Modal):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, fee: int, num_classes: int, reminder_only: bool, mode: str = "single"):
        self.cog = cog
        self.state = (discipline, training, days, fee, num_classes, reminder_only)
        self.mode = mode
        
        if mode == "individual":
            super().__init__(title="Add references for each class")
            for i in range(1, num_classes + 1):
                self.add_item(discord.ui.TextInput(
                    label=f"Class {i} reference (max 100 chars)",
                    style=discord.TextStyle.short,
                    max_length=100,
                    required=False,
                    placeholder=f"e.g., SWAT Team {i}",
                    custom_id=f"ref_{i}"
                ))
        else:
            super().__init__(title="Add reference")
            self.ref = discord.ui.TextInput(
                label="Reference (max 100 characters)",
                style=discord.TextStyle.short,
                max_length=100,
                required=True,
                placeholder="e.g., SWAT Team East, batch 3",
            )
            # FIX: Add the TextInput to the modal
            self.add_item(self.ref)

    async def on_submit(self, interaction: discord.Interaction):
        discipline, training, days, fee, num_classes, reminder_only = self.state
        
        if self.mode == "individual":
            references = []
            for child in self.children:
                if isinstance(child, discord.ui.TextInput):
                    val = str(child.value).strip()
                    references.append(val if val else None)
        elif self.mode == "same":
            ref_text = str(self.ref).strip()
            references = [ref_text] * num_classes if ref_text else []
        else:  # single
            ref_text = str(self.ref).strip()
            references = [ref_text] if ref_text else []
        
        if reminder_only:
            view = ReminderOnlySummaryView(self.cog, interaction.user.id, discipline, training, days, references)
            await view.send_summary(interaction)
        else:
            await safe_update(
                interaction,
                content="References added.",
                view=SummaryView(self.cog, interaction.user.id, discipline, training, days, fee, num_classes, references),
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
        references: List[str],
    ):
        super().__init__(timeout=600)
        self.cog = cog
        self.user_id = user_id
        self.req = TrainingRequest(
            user_id=user_id,
            discipline=discipline,
            training=training,
            days=days,
            fee_per_day=0,
            num_classes=1,
            references=references,
            want_reminder=True,
            request_channel_id=0,
            reminder_only=True,
        )

    async def send_summary(self, interaction: discord.Interaction):
        """Display the summary embed with action buttons."""
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
        ref_text = self.req.references[0] if self.req.references else "—"
        embed.add_field(name="Reference", value=ref_text, inline=False)
        embed.set_footer(text="Click 'Start Reminder' to confirm or 'Cancel' to abort.")

        await safe_update(interaction, content="Review your reminder:", embed=embed, view=self)

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

        ref_text = self.req.references[0] if self.req.references else ""
        await self.cog._add_reminder(
            guild_id=guild.id,
            user_id=interaction.user.id,
            text=f"Your **{self.req.training}** class has finished." + (f" Reference: {ref_text}" if ref_text else ""),
            when=end_at.astimezone(timezone.utc),
            fallback_channel_id=self.req.request_channel_id,
        )

        user = interaction.user
        dm_text = (
            f"✅ Your reminder has been **STARTED**.\n"
            f"**{self.req.discipline} → {self.req.training}** ({self.req.days} days)\n"
            f"End time: {fmt_dt(end_at)}"
        )
        if ref_text:
            dm_text += f"\nReference: {ref_text}"
        dm_text += "\n\nYou will be notified when the training finishes."
        
        dm_sent = False
        try:
            await user.send(dm_text)
            dm_sent = True
        except discord.Forbidden:
            pass

        emb = discord.Embed(
            title="Reminder created",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="User", value=f"{user.mention} ({user.id})", inline=False)
        emb.add_field(name="Training", value=f"{self.req.discipline} → {self.req.training} ({self.req.days}d)", inline=False)
        if ref_text:
            emb.add_field(name="Reference", value=ref_text, inline=False)
        emb.add_field(name="End time", value=fmt_dt(end_at), inline=False)
        await log_channel.send(embed=emb)

        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

        confirm_msg = "✅ Reminder started!" + ("" if dm_sent else " (Check your DMs)")
        await safe_update(interaction, content=confirm_msg, embed=None, view=self)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, custom_id="tm:cancel_reminder")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True
        await safe_update(interaction, content="❌ Reminder cancelled.", embed=None, view=self)

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
        user = interaction.user

        end_at = datetime.now(AMS) + timedelta(days=req.days)
        emb = discord.Embed(
            title="New training request" + (" - MULTIPLE CLASSES" if req.num_classes > 1 else ""),
            color=discord.Color.orange() if req.num_classes > 1 else discord.Color.yellow(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        emb.add_field(name="Discipline", value=req.discipline, inline=True)
        emb.add_field(name="Training", value=req.training, inline=True)
        emb.add_field(name="Duration", value=f"{req.days} days", inline=True)
        fee_txt = "Free" if req.fee_per_day == 0 else f"{req.fee_per_day} credits/day/trainee"
        emb.add_field(name="Fee", value=fee_txt, inline=True)
        
        # Classes field with emphasis if multiple
        class_txt = f"**{req.num_classes}**" if req.num_classes > 1 else "1"
        emb.add_field(name="Classes", value=class_txt, inline=True)
        
        # References handling
        if req.references and any(req.references):
            if len(req.references) == 1 or all(r == req.references[0] for r in req.references if r):
                ref_display = req.references[0] if req.references[0] else "—"
            else:
                ref_lines = []
                for i, ref in enumerate(req.references, 1):
                    ref_lines.append(f"Class {i}: {ref if ref else '—'}")
                ref_display = "\n".join(ref_lines)
            emb.add_field(name="Reference(s)", value=ref_display, inline=False)
        else:
            emb.add_field(name="Reference", value="—", inline=False)
        
        emb.add_field(name="Expected end time", value=fmt_dt(end_at), inline=False)
        
        if req.num_classes > 1:
            emb.set_footer(text=f"⚠️ This request is for {req.num_classes} classes! Use the buttons below to approve or reject ALL classes.")
        else:
            emb.set_footer(text="Use the buttons below to approve or reject.")

        view = AdminDecisionView(cog, requester_id=user.id, req=req)
        await admin_channel.send(embed=emb, view=view)

        queue_emb = discord.Embed(
            title="Request submitted",
            description=f"By {user.mention} in {request_channel.mention}.",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        training_text = f"{req.discipline} → {req.training} ({req.days}d)"
        if req.num_classes > 1:
            training_text += f" × **{req.num_classes} classes**"
        queue_emb.add_field(name="Training", value=training_text, inline=False)
        
        if req.references and any(req.references):
            if len(req.references) == 1 or all(r == req.references[0] for r in req.references if r):
                queue_emb.add_field(name="Reference", value=req.references[0] if req.references[0] else "—", inline=False)
            else:
                ref_lines = []
                for i, ref in enumerate(req.references, 1):
                    ref_lines.append(f"Class {i}: {ref if ref else '—'}")
                queue_emb.add_field(name="References", value="\n".join(ref_lines), inline=False)
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
        num_classes: int,
        references: List[str],
    ):
        super().__init__(timeout=600)
        self.cog = cog
        self.req = TrainingRequest(
            user_id=user_id,
            discipline=discipline,
            training=training,
            days=days,
            fee_per_day=fee,
            num_classes=num_classes,
            references=references,
            want_reminder=False,
            request_channel_id=0,
        )
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
        embed.add_field(name="Classes", value=str(self.req.num_classes), inline=True)
        embed.add_field(name="Expected end time", value=fmt_dt(end_at), inline=False)
        
        # References handling
        if self.req.references and any(self.req.references):
            if len(self.req.references) == 1 or all(r == self.req.references[0] for r in self.req.references if r):
                ref_display = self.req.references[0] if self.req.references[0] else "—"
            else:
                ref_lines = []
                for i, ref in enumerate(self.req.references, 1):
                    ref_lines.append(f"Class {i}: {ref if ref else '—'}")
                ref_display = "\n".join(ref_lines)
            embed.add_field(name="Reference(s)", value=ref_display, inline=False)
        else:
            embed.add_field(name="Reference", value="—", inline=False)
        
        embed.add_field(name="Notify when class finishes", value="Yes" if self.req.want_reminder else "No", inline=True)

        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.custom_id == "tm:rem_on":
                    child.style = discord.ButtonStyle.success if self.req.want_reminder else discord.ButtonStyle.secondary
                if child.custom_id == "tm:rem_off":
                    child.style = discord.ButtonStyle.success if not self.req.want_reminder else discord.ButtonStyle.secondary

        await safe_update(interaction, content="Review and submit to Admin.", embed=embed, view=self)

# ---------- Admin decision ----------

class DeveloperAutoTrainingModal(discord.ui.Modal, title="Developer Training Auto-Open"):
    discipline = discord.ui.TextInput(
        label="Discipline",
        style=discord.TextStyle.short,
        max_length=20,
        required=True,
        placeholder="Fire or Police",
    )
    training = discord.ui.TextInput(
        label="Training name",
        style=discord.TextStyle.short,
        max_length=100,
        required=True,
        placeholder="Exact MissionChief training name",
    )
    fee = discord.ui.TextInput(
        label="Alliance cost",
        style=discord.TextStyle.short,
        max_length=10,
        required=True,
        placeholder="0, 100, 200, 300, 400 or 500",
    )
    classes = discord.ui.TextInput(
        label="Classes",
        style=discord.TextStyle.short,
        max_length=1,
        required=True,
        placeholder="1-4",
    )

    def __init__(self, cog: "TrainingManager"):
        super().__init__()
        self.cog = cog

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This only works inside a server.", ephemeral=True)
            return

        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
        except Exception as exc:
            log.debug("Could not defer developer training auto-open interaction: %s", exc)

        discipline = str(self.discipline.value).strip()
        training = str(self.training.value).strip()
        try:
            fee = int(str(self.fee.value).strip())
            num_classes = int(str(self.classes.value).strip())
        except ValueError:
            await interaction.followup.send("Fee and classes must be numbers.", ephemeral=True)
            return

        days = next(
            (
                duration
                for name, duration in DISCIPLINES.get(discipline, [])
                if _normalize_training_name(name) == _normalize_training_name(training)
            ),
            0,
        )
        req = TrainingRequest(
            user_id=interaction.user.id,
            discipline=discipline,
            training=training,
            days=days,
            fee_per_day=fee,
            num_classes=num_classes,
            references=["Developer auto-open test"],
            want_reminder=False,
            request_channel_id=interaction.channel.id if interaction.channel else 0,
        )
        result = await self.cog._try_auto_open_training(guild, interaction.user, req)
        conf = await self.cog.config.guild(guild).all()
        admin_channel = guild.get_channel(conf.get("admin_channel_id")) if conf.get("admin_channel_id") else None
        log_channel = guild.get_channel(conf.get("log_channel_id")) if conf.get("log_channel_id") else None

        if result.success:
            if admin_channel and log_channel:
                await self.cog._send_auto_open_success(
                    guild=guild,
                    user=interaction.user,
                    req=req,
                    result=result,
                    admin_channel=admin_channel,
                    log_channel=log_channel,
                )
            await interaction.followup.send(
                f"Developer test succeeded: opened **{training}** in academy `{result.academy_id}`.",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f"Developer test did not open a class: {result.reason}",
            ephemeral=True,
        )


class DeveloperTrainingPanelView(discord.ui.View):
    def __init__(self, cog: "TrainingManager"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Open Test Training",
        style=discord.ButtonStyle.danger,
        custom_id="tmdev:auto_open_training",
    )
    async def open_test_training(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(DeveloperAutoTrainingModal(self.cog))


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
        
        # Show modal for optional admin message
        await interaction.response.send_modal(ApproveModal(self.cog, self.requester_id, self.req, admin_msg=interaction.message, admin_user=interaction.user))

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger, custom_id="tm:reject")
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._is_admin(interaction):
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return
        await interaction.response.send_modal(RejectModal(self.cog, self.requester_id, self.req, admin_msg=interaction.message, admin_user=interaction.user))

class ApproveModal(discord.ui.Modal, title="Approve training request"):
    message = discord.ui.TextInput(
        label="Optional message to requester",
        style=discord.TextStyle.paragraph,
        max_length=400,
        required=False,
        placeholder="Add a message that will be sent via DM (optional)",
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

        user = guild.get_member(self.requester_id) if guild else None
        end_at = datetime.now(AMS) + timedelta(days=self.req.days)
        
        class_text = f"{self.req.num_classes} classes" if self.req.num_classes > 1 else "class"
        ok_text = (
            f"Your training request has been **APPROVED**.\n"
            f"**{self.req.discipline} → {self.req.training}** ({class_text}) for "
            f"{'Free' if self.req.fee_per_day == 0 else str(self.req.fee_per_day) + ' credits/day/trainee'}.\n"
            f"End time: {fmt_dt(end_at)}."
        )
        
        if self.req.references and any(self.req.references):
            if len(self.req.references) == 1 or all(r == self.req.references[0] for r in self.req.references if r):
                ok_text += f"\nReference: {self.req.references[0]}"
            else:
                ok_text += "\nReferences:\n"
                for i, ref in enumerate(self.req.references, 1):
                    ok_text += f"  Class {i}: {ref if ref else '—'}\n"
        
        admin_message = str(self.message).strip()
        if admin_message:
            ok_text += f"\n\n**Message from admin:**\n{admin_message}"

        if user:
            try:
                await user.send(ok_text)
            except discord.Forbidden:
                pass

        if self.req.want_reminder:
            ref_text = ""
            if self.req.references and any(self.req.references):
                if len(self.req.references) == 1 or all(r == self.req.references[0] for r in self.req.references if r):
                    ref_text = f" Reference: {self.req.references[0]}"
                else:
                    ref_text = " (Multiple references)"
            
            await self.cog._add_reminder(
                guild_id=guild.id,
                user_id=self.requester_id,
                text=f"Your **{self.req.training}** class has finished.{ref_text}",
                when=end_at.astimezone(timezone.utc),
                fallback_channel_id=self.req.request_channel_id,
            )

        if log_channel:
            emb = discord.Embed(
                title="Training approved" + (f" - {self.req.num_classes} CLASSES" if self.req.num_classes > 1 else ""),
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Requester", value=requester, inline=False)
            
            training_text = f"{self.req.discipline} → {self.req.training} ({self.req.days}d)"
            if self.req.num_classes > 1:
                training_text += f" × **{self.req.num_classes} classes**"
            emb.add_field(name="Training", value=training_text, inline=False)
            
            fee_txt = "Free" if self.req.fee_per_day == 0 else f"{self.req.fee_per_day} c/day"
            emb.add_field(name="Fee", value=fee_txt, inline=True)
            emb.add_field(name="Classes", value=str(self.req.num_classes), inline=True)
            
            if self.req.references and any(self.req.references):
                if len(self.req.references) == 1 or all(r == self.req.references[0] for r in self.req.references if r):
                    emb.add_field(name="Reference", value=self.req.references[0] if self.req.references[0] else "—", inline=False)
                else:
                    ref_lines = []
                    for i, ref in enumerate(self.req.references, 1):
                        ref_lines.append(f"Class {i}: {ref if ref else '—'}")
                    emb.add_field(name="References", value="\n".join(ref_lines), inline=False)
            
            emb.add_field(name="Reminder", value="Yes" if self.req.want_reminder else "No", inline=True)
            emb.add_field(name="End time", value=fmt_dt(end_at), inline=False)
            emb.add_field(name="Approved by", value=f"{self.admin_user.mention} ({self.admin_user.id})", inline=False)
            
            if admin_message:
                emb.add_field(name="Admin message", value=admin_message, inline=False)
            
            await log_channel.send(embed=emb)

        try:
            await self.admin_msg.delete()
        except Exception:
            pass

        await interaction.response.send_message("Request approved and processed.", ephemeral=True)

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
        
        class_text = f"{self.req.num_classes} classes" if self.req.num_classes > 1 else "class"
        text = (
            f"Your training request for **{self.req.discipline} → {self.req.training}** ({class_text}) has been **REJECTED**.\n"
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
                title="Training rejected" + (f" - {self.req.num_classes} CLASSES" if self.req.num_classes > 1 else ""),
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Requester", value=requester, inline=False)
            
            training_text = f"{self.req.discipline} → {self.req.training}"
            if self.req.num_classes > 1:
                training_text += f" × **{self.req.num_classes} classes**"
            emb.add_field(name="Training", value=training_text, inline=False)
            
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
            "reminders": [],
            "button_message": None,
            "developer_panel_channel_id": DEVELOPER_PANEL_CHANNEL_ID,
            "developer_panel_message_id": None,
        }
        self.config.register_guild(**default_guild)

        self.bot.add_view(StartView(self))
        self.bot.add_view(DeveloperTrainingPanelView(self))

        self._reminder_task = self.bot.loop.create_task(self._reminder_loop())
        self._developer_panel_task = self.bot.loop.create_task(self._ensure_developer_panels())

    def cog_unload(self):
        if self._reminder_task:
            self._reminder_task.cancel()
        if self._developer_panel_task:
            self._developer_panel_task.cancel()

    @asynccontextmanager
    async def _bot_status(self, detail: str, *, priority: int = 70):
        bot = getattr(self, "bot", None)
        botstatus = bot.get_cog("BotStatus") if bot else None
        if botstatus and hasattr(botstatus, "track_activity"):
            async with botstatus.track_activity("TrainingManager", detail, priority=priority):
                yield
        else:
            yield

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
                    async with self._bot_status(
                        f"sending {len(due)} training reminders in {guild.name}"
                    ):
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

    # --------------- Automatic MissionChief opening ---------------

    async def _resolve_verified_requester(
        self,
        guild: discord.Guild,
        user: discord.abc.User,
    ) -> Tuple[bool, Optional[str], Optional[str], str]:
        membersync = self.bot.get_cog("MemberSync")
        if not membersync:
            return False, None, None, "MemberSync is not loaded"

        link = None
        get_link = getattr(membersync, "get_link_for_discord", None)
        if get_link:
            link = await get_link(user.id)
        if link:
            mc_user_id = link.get("mc_user_id")
            mc_username = link.get("mc_username") or link.get("mc_name") or link.get("name")
            return True, str(mc_user_id) if mc_user_id else None, mc_username, "approved MemberSync link"

        role_id = None
        try:
            role_id = await membersync.config.verified_role_id()
        except Exception as exc:
            log.debug("Could not read MemberSync verified role: %s", exc)

        member = guild.get_member(user.id)
        if role_id and member:
            role = guild.get_role(int(role_id))
            if role and role in getattr(member, "roles", []):
                return True, None, None, "verified Discord role"

        return False, None, None, "requester is not verified"

    async def _get_latest_contribution_rate(self, mc_user_id: Optional[str]) -> Optional[float]:
        if not mc_user_id:
            return None
        members_scraper = self.bot.get_cog("MembersScraper")
        if not members_scraper:
            return None
        get_snapshot = getattr(members_scraper, "get_member_snapshot", None)
        if not get_snapshot:
            return None
        try:
            snapshot = await get_snapshot(str(mc_user_id))
        except Exception as exc:
            log.warning("Could not fetch latest contribution snapshot for %s: %s", mc_user_id, exc)
            return None
        if not snapshot:
            return None
        value = snapshot.get("contribution_rate")
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _find_academy_course(self, page: AcademyPage, training_name: str) -> Optional[AcademyCourse]:
        wanted = _normalize_training_name(training_name)
        for course in page.courses:
            if course.normalized_label == wanted:
                return course
        return None

    async def _try_auto_open_training(
        self,
        guild: discord.Guild,
        user: discord.abc.User,
        req: TrainingRequest,
    ) -> AutoTrainingResult:
        academy_id = AUTO_ACADEMY_BUILDINGS.get(req.discipline)
        if not academy_id:
            return AutoTrainingResult(False, f"Auto-open is not configured for {req.discipline}")

        if req.num_classes < 1 or req.num_classes > AUTO_MAX_CLASSES:
            return AutoTrainingResult(False, f"Requested class count must be between 1 and {AUTO_MAX_CLASSES}")

        verified, mc_user_id, mc_username, verified_source = await self._resolve_verified_requester(guild, user)
        if not verified:
            return AutoTrainingResult(False, verified_source, academy_id=academy_id)

        contribution_rate = await self._get_latest_contribution_rate(mc_user_id)
        if contribution_rate is not None and contribution_rate < AUTO_MIN_CONTRIBUTION_RATE:
            return AutoTrainingResult(
                False,
                f"Latest contribution rate is {contribution_rate:.1f}%, below {AUTO_MIN_CONTRIBUTION_RATE:.1f}%",
                academy_id=academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
            )

        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager or not hasattr(cookie_manager, "get_session"):
            return AutoTrainingResult(
                False,
                "CookieManager is not loaded",
                academy_id=academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
            )

        session = await cookie_manager.get_session()
        building_url = f"https://www.missionchief.com/buildings/{academy_id}"
        async with session.get(building_url, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            return AutoTrainingResult(False, f"Academy page returned HTTP {status}", academy_id=academy_id, status=status)

        page = parse_academy_page(html)
        if not page.action or not page.authenticity_token:
            return AutoTrainingResult(
                False,
                "Academy education form was not found; MissionChief session may not be logged in",
                academy_id=academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
                status=status,
            )

        if page.available_rooms < req.num_classes:
            return AutoTrainingResult(
                False,
                f"Only {page.available_rooms} classroom(s) available, request needs {req.num_classes}",
                academy_id=academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
                status=status,
            )

        if req.fee_per_day not in page.costs:
            return AutoTrainingResult(
                False,
                f"Fee {req.fee_per_day} is not available on the academy page",
                academy_id=academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
                status=status,
            )

        course = self._find_academy_course(page, req.training)
        if not course:
            return AutoTrainingResult(
                False,
                f"Training `{req.training}` was not found in academy {academy_id}",
                academy_id=academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
                status=status,
            )

        post_url = f"https://www.missionchief.com{page.action}" if page.action.startswith("/") else page.action
        payload = {
            "utf8": "✓",
            "authenticity_token": page.authenticity_token,
            "building_rooms_use": str(req.num_classes),
            "education_select": course.value,
            "alliance[duration]": str(AUTO_ALLIANCE_DURATION_SECONDS),
            "alliance[cost]": str(req.fee_per_day),
            "commit": "Educate",
        }
        async with session.post(post_url, data=payload, allow_redirects=True) as response:
            post_status = getattr(response, "status", None)
            await response.text()

        if post_status is None or int(post_status) >= 400:
            return AutoTrainingResult(
                False,
                f"MissionChief education POST failed with HTTP {post_status}",
                academy_id=academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
                course_value=course.value,
                status=post_status,
            )

        return AutoTrainingResult(
            True,
            "Training opened automatically",
            academy_id=academy_id,
            mc_user_id=mc_user_id,
            mc_username=mc_username,
            contribution_rate=contribution_rate,
            course_value=course.value,
            classes_opened=req.num_classes,
            status=post_status,
        )

    async def _send_auto_open_success(
        self,
        guild: discord.Guild,
        user: discord.abc.User,
        req: TrainingRequest,
        result: AutoTrainingResult,
        admin_channel: discord.abc.Messageable,
        log_channel: discord.abc.Messageable,
    ) -> None:
        end_at = datetime.now(AMS) + timedelta(days=req.days)
        fee_txt = "Free" if req.fee_per_day == 0 else f"{req.fee_per_day} credits/day/trainee"
        training_text = f"{req.discipline} → {req.training} ({req.days}d)"
        if req.num_classes > 1:
            training_text += f" × **{req.num_classes} classes**"

        embed = discord.Embed(
            title="Training automatically opened",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Training", value=training_text, inline=False)
        embed.add_field(name="Academy", value=f"[Building {result.academy_id}](https://www.missionchief.com/buildings/{result.academy_id})", inline=True)
        embed.add_field(name="Fee", value=fee_txt, inline=True)
        embed.add_field(name="Alliance share", value="1 hour", inline=True)
        if result.mc_user_id or result.mc_username:
            embed.add_field(
                name="MissionChief member",
                value=f"{result.mc_username or 'Unknown'} ({result.mc_user_id or 'unknown ID'})",
                inline=False,
            )
        if result.contribution_rate is not None:
            embed.add_field(name="Contribution", value=f"{result.contribution_rate:.1f}%", inline=True)
        embed.add_field(name="Expected end time", value=fmt_dt(end_at), inline=False)
        embed.set_footer(text=f"Course value: {result.course_value} • HTTP {result.status}")

        await admin_channel.send(embed=embed)
        await log_channel.send(embed=embed)

        if req.want_reminder:
            ref_text = ""
            if req.references and any(req.references):
                ref_text = " (Multiple references)" if len(set(req.references)) > 1 else f" Reference: {req.references[0]}"
            await self._add_reminder(
                guild_id=guild.id,
                user_id=user.id,
                text=f"Your **{req.training}** class has finished.{ref_text}",
                when=end_at.astimezone(timezone.utc),
                fallback_channel_id=req.request_channel_id,
            )

    async def _send_developer_panel(self, guild: discord.Guild, channel: discord.TextChannel) -> discord.Message:
        embed = discord.Embed(
            title="Developer Training Auto-Open",
            description=(
                "Use this private test panel to open MissionChief training classes automatically "
                "without changing the normal member request flow.\n\n"
                "This is for controlled testing only."
            ),
            color=discord.Color.orange(),
        )
        message = await channel.send(embed=embed, view=DeveloperTrainingPanelView(self))
        await self.config.guild(guild).developer_panel_message_id.set(message.id)
        return message

    async def _ensure_developer_panel_for_guild(self, guild: discord.Guild) -> None:
        conf = await self.config.guild(guild).all()
        channel_id = int(conf.get("developer_panel_channel_id") or DEVELOPER_PANEL_CHANNEL_ID)
        channel = guild.get_channel(channel_id)
        if channel is None:
            log.warning("TrainingManager developer panel channel not found: %s", channel_id)
            return

        message_id = conf.get("developer_panel_message_id")
        if message_id:
            try:
                await channel.fetch_message(int(message_id))
                return
            except Exception as exc:
                log.info("TrainingManager developer panel message missing; reposting: %s", exc)

        await self._send_developer_panel(guild, channel)

    async def _ensure_developer_panels(self) -> None:
        await self.bot.wait_until_red_ready()
        for guild in self.bot.guilds:
            try:
                await self._ensure_developer_panel_for_guild(guild)
            except Exception as exc:
                log.exception("Could not ensure TrainingManager developer panel in %s: %s", guild, exc)

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

    @tmset.command(name="devpost")
    @commands.admin()
    async def developer_post(self, ctx: commands.Context):
        """Repost the developer-only TrainingManager auto-open test panel."""
        channel_id = await self.config.guild(ctx.guild).developer_panel_channel_id()
        channel = ctx.guild.get_channel(int(channel_id or DEVELOPER_PANEL_CHANNEL_ID))
        if not channel:
            await ctx.send("The configured developer panel channel was not found.")
            return
        await self._send_developer_panel(ctx.guild, channel)
        await ctx.send(f"Developer TrainingManager panel posted in {channel.mention}.")
