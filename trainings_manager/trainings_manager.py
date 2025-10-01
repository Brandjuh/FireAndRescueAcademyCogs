
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
}

FEE_CHOICES = [0, 100, 200, 300, 400, 500]

def ts(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def fmt_dt(dt: datetime) -> str:
    # Discord timestamp + human fallback
    unix = ts(dt)
    return f"<t:{unix}:F> (Amsterdam)"

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
        )


class StartView(discord.ui.View):
    def __init__(self, cog: "TrainingManager"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Start training", style=discord.ButtonStyle.primary, custom_id="tm:start")
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Laten we je trainingsverzoek opzetten. Eerst de discipline.",
            view=DisciplineView(self.cog),
            ephemeral=True,
        )


class DisciplineView(discord.ui.View):
    def __init__(self, cog: "TrainingManager"):
        super().__init__(timeout=180)
        self.cog = cog
        self.add_item(DisciplineSelect(self.cog))

class DisciplineSelect(discord.ui.Select):
    def __init__(self, cog: "TrainingManager"):
        self.cog = cog
        options = [discord.SelectOption(label=k, description=f"{len(v)} trainingen") for k, v in DISCIPLINES.items()]
        super().__init__(placeholder="Kies een discipline", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        discipline = self.values[0]
        await interaction.response.edit_message(
            content=f"Discipline gekozen: **{discipline}**. Kies nu een training.",
            view=TrainingView(self.cog, discipline),
        )

class TrainingView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str):
        super().__init__(timeout=180)
        self.cog = cog
        self.discipline = discipline
        self.add_item(TrainingSelect(self.cog, discipline))

class TrainingSelect(discord.ui.Select):
    def __init__(self, cog: "TrainingManager", discipline: str):
        self.cog = cog
        self.discipline = discipline
        options = []
        for name, days in DISCIPLINES[discipline]:
            label = name
            desc = f"Duur: {days} {'dag' if days==1 else 'dagen'}"
            options.append(discord.SelectOption(label=label, description=desc))
        super().__init__(placeholder="Kies een training", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        training = self.values[0]
        # Find days
        days = next(days for name, days in DISCIPLINES[self.discipline] if name == training)
        await interaction.response.edit_message(
            content=f"Gekozen: **{self.discipline} → {training}**. Kies nu de vergoeding per dag, per trainee.",
            view=FeeView(self.cog, self.discipline, training, days),
        )

class FeeView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.discipline = discipline
        self.training = training
        self.days = days
        for fee in FEE_CHOICES:
            label = "Free" if fee == 0 else f"{fee} credits/dag"
            self.add_item(FeeButton(label, fee))

    async def on_timeout(self):
        return

class FeeButton(discord.ui.Button):
    def __init__(self, label: str, fee: int):
        super().__init__(label=label, style=discord.ButtonStyle.secondary)
        self.fee = fee

    async def callback(self, interaction: discord.Interaction):
        view: FeeView = self.view  # type: ignore
        await interaction.response.edit_message(
            content=(
                f"Gekozen: **{view.discipline} → {view.training}** voor "
                f"**{'Free' if self.fee == 0 else str(self.fee) + ' credits/dag'}**.\n"
                "Wil je een referentie toevoegen?"
            ),
            view=ReferenceAskView(view.cog, view.discipline, view.training, view.days, self.fee),
        )

class ReferenceAskView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, fee: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.state = (discipline, training, days, fee)

    @discord.ui.button(label="Ja, referentie toevoegen", style=discord.ButtonStyle.primary)
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ReferenceModal(self.cog, *self.state))

    @discord.ui.button(label="Nee, ga verder", style=discord.ButtonStyle.secondary)
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee = self.state
        await interaction.response.edit_message(
            content="Referentie overgeslagen.",
            view=SummaryView(self.cog, interaction.user.id, discipline, training, days, fee, None),
        )

class ReferenceModal(discord.ui.Modal, title="Referentie toevoegen"):
    ref = discord.ui.TextInput(
        label="Korte beschrijving (max 100 tekens)",
        style=discord.TextStyle.short,
        max_length=100,
        required=True,
        placeholder="Bijv. SWAT team East, batch 3",
    )

    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, fee: int):
        super().__init__()
        self.cog = cog
        self.state = (discipline, training, days, fee)

    async def on_submit(self, interaction: discord.Interaction):
        discipline, training, days, fee = self.state
        await interaction.response.edit_message(
            content="Referentie toegevoegd.",
            view=SummaryView(self.cog, interaction.user.id, discipline, training, days, fee, str(self.ref)),
        )

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
        super().__init__(timeout=300)
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

        self.add_item(ReminderToggle(self))

        self.add_item(SubmitButton(self))

    async def send_or_update(self, interaction: discord.Interaction):
        user = interaction.user
        end_at = datetime.now(AMS) + timedelta(days=self.req.days)
        embed = discord.Embed(
            title="Training aanvraag - Overzicht",
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Aanvrager", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Discipline", value=self.req.discipline, inline=True)
        embed.add_field(name="Training", value=self.req.training, inline=True)
        embed.add_field(name="Duur", value=f"{self.req.days} dagen", inline=True)
        fee_txt = "Free" if self.req.fee_per_day == 0 else f"{self.req.fee_per_day} credits/dag/trainee"
        embed.add_field(name="Vergoeding", value=fee_txt, inline=True)
        embed.add_field(name="Eindtijd (verwacht)", value=fmt_dt(end_at), inline=False)
        embed.add_field(name="Referentie", value=self.req.reference or "—", inline=False)
        embed.add_field(name="Herinnering sturen", value="Ja" if self.req.want_reminder else "Nee", inline=True)
        await interaction.edit_original_response(content="Controleer en verzend naar Admin.", embed=embed, view=self)

class ReminderToggle(discord.ui.Select):
    def __init__(self, parent_view: SummaryView):
        self.parent_view = parent_view
        options = [
            discord.SelectOption(label="Geen herinnering", description="Ik onthoud het zelf wel", value="no"),
            discord.SelectOption(label="Wel herinnering", description="Ping me als de klas klaar is", value="yes"),
        ]
        super().__init__(placeholder="Herinnering sturen?", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        self.parent_view.req.want_reminder = self.values[0] == "yes"
        await self.parent_view.send_or_update(interaction)

class SubmitButton(discord.ui.Button):
    def __init__(self, parent_view: SummaryView):
        super().__init__(label="Verstuur naar Admin", style=discord.ButtonStyle.success)
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        cog: TrainingManager = self.parent_view.cog  # type: ignore
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Dit werkt alleen in een server.", ephemeral=True)
            return

        conf = await cog.config.guild(guild).all()
        admin_channel_id = conf.get("admin_channel_id")
        log_channel_id = conf.get("log_channel_id")
        request_channel_id = conf.get("request_channel_id")

        if not admin_channel_id or not log_channel_id or not request_channel_id:
            await interaction.response.send_message(
                "De Admin/Log/Request kanalen zijn nog niet ingesteld. Vraag een beheerder om [p]tmset te gebruiken.",
                ephemeral=True,
            )
            return

        admin_channel = guild.get_channel(admin_channel_id)
        log_channel = guild.get_channel(log_channel_id)
        request_channel = guild.get_channel(request_channel_id)

        if not admin_channel or not log_channel or not request_channel:
            await interaction.response.send_message("Kan een of meer ingestelde kanalen niet vinden.", ephemeral=True)
            return

        req = self.parent_view.req
        req.request_channel_id = request_channel.id

        # Build admin embed
        end_at = datetime.now(AMS) + timedelta(days=req.days)
        emb = discord.Embed(
            title="Nieuw trainingsverzoek",
            color=discord.Color.yellow(),
            timestamp=datetime.now(timezone.utc),
        )
        user = interaction.user
        emb.add_field(name="Aanvrager", value=f"{user.mention} ({user.id})", inline=False)
        emb.add_field(name="Discipline", value=req.discipline, inline=True)
        emb.add_field(name="Training", value=req.training, inline=True)
        emb.add_field(name="Duur", value=f"{req.days} dagen", inline=True)
        fee_txt = "Free" if req.fee_per_day == 0 else f"{req.fee_per_day} credits/dag/trainee"
        emb.add_field(name="Vergoeding", value=fee_txt, inline=True)
        emb.add_field(name="Referentie", value=req.reference or "—", inline=False)
        emb.add_field(name="Eindtijd (verwacht)", value=fmt_dt(end_at), inline=False)
        emb.set_footer(text="Gebruik de knoppen hieronder om te keuren of af te wijzen.")

        view = AdminDecisionView(cog, requester_id=user.id, req=req)
        msg = await admin_channel.send(embed=emb, view=view)

        # Log queueing
        queue_emb = discord.Embed(
            title="Aanvraag ingediend",
            description=f"Door {user.mention} in {request_channel.mention}.",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        queue_emb.add_field(name="Training", value=f"{req.discipline} → {req.training} ({req.days}d)", inline=False)
        if req.reference:
            queue_emb.add_field(name="Referentie", value=req.reference, inline=False)
        await log_channel.send(embed=queue_emb)

        await interaction.response.edit_message(content="Verzonden naar Admin. Je krijgt een bericht zodra er iets wijzigt.", embed=None, view=None)

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
            await interaction.response.send_message("Je hebt geen rechten om dit te doen.", ephemeral=True)
            return

        guild = interaction.guild
        conf = await self.cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None

        # Notify requester
        user = guild.get_member(self.requester_id) if guild else None
        end_at = datetime.now(AMS) + timedelta(days=self.req.days)
        ok_text = (
            f"Je trainingsverzoek is **GOEDGEKEURD**.\n"
            f"**{self.req.discipline} → {self.req.training}** voor "
            f"{'Free' if self.req.fee_per_day == 0 else str(self.req.fee_per_day) + ' credits/dag/trainee'}.\n"
            f"Eindtijd: {fmt_dt(end_at)}."
        )
        if self.req.reference:
            ok_text += f"\nReferentie: {self.req.reference}"

        dm_ok = False
        if user:
            try:
                await user.send(ok_text)
                dm_ok = True
            except discord.Forbidden:
                dm_ok = False

        # Schedule reminder if wanted
        if self.req.want_reminder:
            await self.cog._add_reminder(
                guild_id=guild.id,
                user_id=self.requester_id,
                text=f"Je **{self.req.training}** klas is klaar." + (f" Referentie: {self.req.reference}" if self.req.reference else ""),
                when=end_at.astimezone(timezone.utc),
                fallback_channel_id=self.req.request_channel_id,
            )

        # Log + clean
        if log_channel:
            emb = discord.Embed(
                title="Training goedgekeurd",
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Aanvrager", value=requester, inline=False)
            emb.add_field(name="Training", value=f"{self.req.discipline} → {self.req.training} ({self.req.days}d)", inline=False)
            fee_txt = "Free" if self.req.fee_per_day == 0 else f"{self.req.fee_per_day} c/dag"
            emb.add_field(name="Vergoeding", value=fee_txt, inline=True)
            if self.req.reference:
                emb.add_field(name="Referentie", value=self.req.reference, inline=False)
            emb.add_field(name="Herinnering", value="Ja" if self.req.want_reminder else "Nee", inline=True)
            emb.add_field(name="Eindtijd", value=fmt_dt(end_at), inline=False)
            await log_channel.send(embed=emb)

        try:
            await interaction.message.delete()
        except Exception:
            pass

        await interaction.response.send_message("Aanvraag goedgekeurd en verwerkt.", ephemeral=True)

    @discord.ui.button(label="Afwijzen", style=discord.ButtonStyle.danger, custom_id="tm:reject")
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._is_admin(interaction):
            await interaction.response.send_message("Je hebt geen rechten om dit te doen.", ephemeral=True)
            return
        await interaction.response.send_modal(RejectModal(self.cog, self.requester_id, self.req, admin_msg=interaction.message))

class RejectModal(discord.ui.Modal, title="Reden voor afwijzing"):
    reason = discord.ui.TextInput(
        label="Reden",
        style=discord.TextStyle.paragraph,
        max_length=400,
        required=True,
        placeholder="Leg kort uit waarom dit verzoek is afgewezen.",
    )

    def __init__(self, cog: "TrainingManager", requester_id: int, req: TrainingRequest, admin_msg: discord.Message):
        super().__init__()
        self.cog = cog
        self.requester_id = requester_id
        self.req = req
        self.admin_msg = admin_msg

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Interne fout: geen guild.", ephemeral=True)
            return
        conf = await self.cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None

        user = guild.get_member(self.requester_id)
        text = (
            f"Je trainingsverzoek voor **{self.req.discipline} → {self.req.training}** is **AFGEWEZEN**.\n"
            f"Reden: {self.reason}"
        )
        if user:
            try:
                await user.send(text)
            except discord.Forbidden:
                # Fallback: post in request channel
                ch = guild.get_channel(self.req.request_channel_id)
                if ch:
                    await ch.send(f"{user.mention} {text}")

        if log_channel:
            emb = discord.Embed(
                title="Training afgewezen",
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Aanvrager", value=requester, inline=False)
            emb.add_field(name="Training", value=f"{self.req.discipline} → {self.req.training}", inline=False)
            emb.add_field(name="Reden", value=str(self.reason), inline=False)
            await log_channel.send(embed=emb)

        try:
            await self.admin_msg.delete()
        except Exception:
            pass

        await interaction.response.send_message("Afwijzing doorgestuurd en gelogd.", ephemeral=True)


class TrainingManager(commands.Cog):
    """Training requests met goedkeuring en herinneringen."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xBEEFCAFE, force_registration=True)
        default_guild = {
            "request_channel_id": None,
            "admin_channel_id": None,
            "log_channel_id": None,
            "admin_role_id": None,
            "reminders": [],  # list of dicts: {user_id, text, when_ts, fallback_channel_id}
        }
        self.config.register_guild(**default_guild)

        # Re-register persistent start view
        self.bot.add_view(StartView(self))

        # Background task
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
                    # deliver
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
        """Configureer Training Manager. Subcommands: requestchannel, adminchannel, logchannel, adminrole, post."""
        conf = await self.config.guild(ctx.guild).all()
        txt = (
            f"Request kanaal: {ctx.guild.get_channel(conf['request_channel_id']).mention if conf.get('request_channel_id') else '—'}\n"
            f"Admin kanaal: {ctx.guild.get_channel(conf['admin_channel_id']).mention if conf.get('admin_channel_id') else '—'}\n"
            f"Log kanaal: {ctx.guild.get_channel(conf['log_channel_id']).mention if conf.get('log_channel_id') else '—'}\n"
            f"Admin rol: {ctx.guild.get_role(conf['admin_role_id']).mention if conf.get('admin_role_id') else '—'}\n"
        )
        await ctx.send(box(txt, lang="ini"))

    @tmset.command()
    @commands.admin()
    async def requestchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.guild(ctx.guild).request_channel_id.set(channel.id)
        await ctx.tick()

    @tmset.command()
    @commands.admin()
    async def adminchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.guild(ctx.guild).admin_channel_id.set(channel.id)
        await ctx.tick()

    @tmset.command()
    @commands.admin()
    async def logchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        await self.config.guild(ctx.guild).log_channel_id.set(channel.id)
        await ctx.tick()

    @tmset.command()
    @commands.admin()
    async def adminrole(self, ctx: commands.Context, role: discord.Role):
        await self.config.guild(ctx.guild).admin_role_id.set(role.id)
        await ctx.tick()

    @tmset.command()
    @commands.admin()
    async def post(self, ctx: commands.Context):
        """Post de 'Start training' knop in het request kanaal."""
        request_channel_id = await self.config.guild(ctx.guild).request_channel_id()
        if not request_channel_id:
            await ctx.send("Stel eerst het request kanaal in met `[p]tmset requestchannel #kanaal`.")
            return
        ch = ctx.guild.get_channel(request_channel_id)
        if not ch:
            await ctx.send("Het ingestelde request kanaal werd niet gevonden.")
            return
        emb = discord.Embed(
            title="Training aanvragen",
            description=(
                "Klik op **Start training** om een verzoek in te dienen. "
                "Je kiest een discipline, training, optie voor vergoeding, optionele referentie, en of je een herinnering wilt."
            ),
            color=discord.Color.blurple(),
        )
        await ch.send(embed=emb, view=StartView(self))
        await ctx.tick()
