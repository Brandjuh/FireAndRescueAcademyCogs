
from __future__ import annotations

import asyncio
import datetime as dt
import random
from typing import Any, Dict, List, Optional

import discord
from redbot.core import commands, Config


class FireStationCommand(commands.Cog):
    """Fire station incident workflow mini-game."""

    __version__ = "0.1.0"

    def __init__(self, bot):
        self.bot = bot
        # simple numeric identifier; change if you reuse this skeleton elsewhere
        self.config = Config.get_conf(self, identifier=0xF15701, force_registration=True)

        default_global: Dict[str, Any] = {
            "normal_turnout_minutes": 15.0,
            "emergency_turnout_minutes": 5.0,
            "realert_minutes_min": 1.0,
            "realert_minutes_max": 3.0,
            "travel_minutes_min": 3.0,
            "travel_minutes_max": 8.0,
        }

        default_user: Dict[str, Any] = {
            "started": False,
            "credits": 100_000,
            "vehicles": [],             # [{id, name, crew_capacity}]
            "next_vehicle_id": 1,
            "active_mission": None,     # see below
        }

        self.config.register_global(**default_global)
        self.config.register_user(**default_user)

    # --------------------------------------------------
    # Internal helpers
    # --------------------------------------------------

    async def _ensure_started(self, ctx: commands.Context) -> bool:
        data = await self.config.user(ctx.author).all()
        if not data["started"]:
            await ctx.send("You have not started yet. Use `[p]fsc start` first.")
            return False
        return True

    def _make_timestamp_pair(self, minutes: float) -> Dict[str, Any]:
        finish = dt.datetime.utcnow() + dt.timedelta(minutes=minutes)
        ts = int(finish.timestamp())
        return {
            "relative": f"<t:{ts}:R>",
            "absolute": f"<t:{ts}:T>",
            "raw": ts,
        }

    # very small built-in incident list
    INCIDENTS: List[Dict[str, Any]] = [
        {
            "id": "house_fire",
            "name": "House Fire",
            "required_staff": 8,
            "hint": "Reports of smoke from a residential building.",
            "detail": "On approach you see smoke from the roof and people outside waving.",
        },
        {
            "id": "car_crash",
            "name": "Traffic Collision",
            "required_staff": 6,
            "hint": "Multiple calls of a crash at an intersection.",
            "detail": "Police report two vehicles involved, possible entrapment.",
        },
        {
            "id": "small_fire",
            "name": "Trash Fire",
            "required_staff": 4,
            "hint": "Caller reports a small fire near containers.",
            "detail": "On arrival, smoke visible but no exposures yet.",
        },
    ]

    def _pick_random_incident(self) -> Dict[str, Any]:
        return random.choice(self.INCIDENTS)

    def _simulate_emergency_turnout(self, required: int) -> Dict[str, Any]:
        """Emergency alert: faster but more chance on no-shows."""
        if required <= 0:
            return {
                "required": 0,
                "first_arrived": 0,
                "second_arrived": 0,
                "total_arrived": 0,
            }

        # first alert
        base_no_show = random.uniform(0.10, 0.25)
        first_arrived = 0
        for _ in range(required):
            if random.random() > base_no_show:
                first_arrived += 1

        return {
            "required": required,
            "first_arrived": first_arrived,
            "second_arrived": 0,
            "total_arrived": first_arrived,
        }

    def _simulate_realert(self, current_total: int, required: int) -> Dict[str, int]:
        """Re-alert: shorter delay, somewhat better attendance."""
        if current_total >= required:
            return {"second_arrived": 0, "total_arrived": current_total}

        remaining = required - current_total
        no_show_second = random.uniform(0.0, 0.10)
        second_arrived = 0
        for _ in range(remaining):
            if random.random() > no_show_second:
                second_arrived += 1
        return {
            "second_arrived": second_arrived,
            "total_arrived": current_total + second_arrived,
        }

    async def _get_user_vehicles(self, user: discord.abc.User) -> List[Dict[str, Any]]:
        data = await self.config.user(user).all()
        return data.get("vehicles", [])


class AlertChoiceView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, user: discord.abc.User):
        super().__init__(timeout=120)
        self.cog = cog
        self.user = user

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Normal turnout (15 min, full crew)", style=discord.ButtonStyle.secondary)
    async def normal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_alert_choice(interaction, self.user, "normal")
        self.stop()

    @discord.ui.button(label="Emergency turnout (fast, risk of shortage)", style=discord.ButtonStyle.danger)
    async def emergency(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_alert_choice(interaction, self.user, "emergency")
        self.stop()


class TurnoutDecisionView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, user: discord.abc.User):
        super().__init__(timeout=180)
        self.cog = cog
        self.user = user

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Re-alert", style=discord.ButtonStyle.secondary)
    async def realert(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_realert(interaction, self.user)
        self.stop()

    @discord.ui.button(label="Proceed to vehicle selection", style=discord.ButtonStyle.success)
    async def proceed(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_proceed_to_vehicles(interaction, self.user)
        self.stop()

    @discord.ui.button(label="Cancel incident", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.handle_cancel_incident(interaction, self.user)
        self.stop()


class VehicleSelect(discord.ui.Select):
    def __init__(self, cog: FireStationCommand, user: discord.abc.User, vehicles: List[Dict[str, Any]]):
        options = []
        for v in vehicles:
            label = f"{v['name']} (cap {v['crew_capacity']})"
            options.append(discord.SelectOption(label=label, value=str(v["id"])))
        if not options:
            options = [discord.SelectOption(label="No vehicles available", value="none")]
        super().__init__(
            placeholder="Select vehicles to dispatch",
            min_values=1,
            max_values=len(options),
            options=options,
        )
        self.cog = cog
        self.user = user

    async def callback(self, interaction: discord.Interaction):
        if self.values == ["none"]:
            await interaction.response.send_message("You have no vehicles to dispatch.", ephemeral=True)
            return
        await self.cog.handle_vehicle_selection(interaction, self.user, list(self.values))


class VehicleSelectView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, user: discord.abc.User, vehicles: List[Dict[str, Any]]):
        super().__init__(timeout=180)
        self.cog = cog
        self.user = user
        self.add_item(VehicleSelect(cog, user, vehicles))


class VehicleShopSelect(discord.ui.Select):
    CATALOG = {
        "pumper": {"name": "Fire Engine", "crew_capacity": 6, "price": 50_000},
        "ladder": {"name": "Aerial Ladder", "crew_capacity": 3, "price": 75_000},
        "rescue": {"name": "Rescue Unit", "crew_capacity": 4, "price": 65_000},
    }

    def __init__(self, cog: FireStationCommand, user: discord.abc.User):
        options = []
        for vid, v in self.CATALOG.items():
            label = f"{v['name']} ({v['price']} cr, cap {v['crew_capacity']})"
            options.append(discord.SelectOption(label=label, value=vid))
        super().__init__(
            placeholder="Select a vehicle to buy",
            min_values=1,
            max_values=1,
            options=options,
        )
        self.cog = cog
        self.user = user

    async def callback(self, interaction: discord.Interaction):
        choice = self.values[0]
        await self.cog.handle_vehicle_purchase(interaction, self.user, choice)


class VehicleShopView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, user: discord.abc.User):
        super().__init__(timeout=180)
        self.add_item(VehicleShopSelect(cog, user))


# --------------------------------------------------
# Commands
# --------------------------------------------------


class FireStationCommand(FireStationCommand):  # type: ignore[misc]
    @commands.group(name="fsc")
    async def fsc_group(self, ctx: commands.Context):
        """Fire Station Command main group."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @fsc_group.command(name="start")
    async def fsc_start(self, ctx: commands.Context):
        """Start your fire station career."""
        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        if data["started"]:
            await ctx.send("You already started.")
            return

        starter_vehicle = {
            "id": 1,
            "name": "Starter Fire Engine",
            "crew_capacity": 6,
        }
        await user_conf.started.set(True)
        await user_conf.credits.set(100_000)
        await user_conf.vehicles.set([starter_vehicle])
        await user_conf.next_vehicle_id.set(2)
        await user_conf.active_mission.set(None)

        await ctx.send(
            "You are now the commander of a small volunteer station.\n"
            "You received one **Starter Fire Engine** with 6 crew capacity."
        )

    @fsc_group.command(name="status")
    async def fsc_status(self, ctx: commands.Context):
        """Show your basic status and active mission stage."""
        if not await self._ensure_started(ctx):
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        vehicles = data.get("vehicles", [])
        active = data.get("active_mission")

        lines = [
            f"Credits: **{data.get('credits', 0)}**",
            f"Vehicles: **{len(vehicles)}**",
        ]

        if active:
            lines.append(f"Active incident: **{active.get('title', 'Unknown')}** (stage: `{active.get('stage')}`)")
        else:
            lines.append("Active incident: **None**")

        await ctx.send("\n".join(lines))

    @fsc_group.command(name="mission")
    async def fsc_mission(self, ctx: commands.Context):
        """Start a new incident if none is active."""
        if not await self._ensure_started(ctx):
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        active = data.get("active_mission")
        if active:
            await ctx.send("You already have an active incident. Finish or cancel it first.")
            return

        incident = self._pick_random_incident()
        mission = {
            "id": incident["id"],
            "title": incident["name"],
            "required_staff": incident["required_staff"],
            "hint": incident["hint"],
            "detail": incident["detail"],
            "stage": "ALERT_CHOICE",
            "alert_mode": None,
        }
        await user_conf.active_mission.set(mission)

        view = AlertChoiceView(self, ctx.author)
        await ctx.send(
            content=(
                f"🚨 New incident: **{incident['name']}**\n"
                f"{incident['hint']}\n\n"
                "Choose how to alert your crew:"
            ),
            view=view,
        )

    @fsc_group.command(name="shop")
    async def fsc_shop(self, ctx: commands.Context):
        """Open the vehicle shop (dropdown)."""
        if not await self._ensure_started(ctx):
            return

        view = VehicleShopView(self, ctx.author)
        await ctx.send("🚗 Vehicle shop – select a vehicle to purchase:", view=view)

    # --------------------------------------------------
    # Workflow handlers
    # --------------------------------------------------

    async def handle_alert_choice(self, interaction: discord.Interaction, user: discord.abc.User, mode: str):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission") or {}
        if mission.get("stage") != "ALERT_CHOICE":
            await interaction.response.send_message("This incident is no longer in the alert stage.", ephemeral=True)
            return

        glb = await self.config.all()
        required = int(mission.get("required_staff", 6))

        if mode == "normal":
            minutes = float(glb.get("normal_turnout_minutes", 15.0))
            first_arrived = required
            total = required
        else:
            minutes = float(glb.get("emergency_turnout_minutes", 5.0))
            res = self._simulate_emergency_turnout(required)
            first_arrived = res["first_arrived"]
            total = res["total_arrived"]

        ts = self._make_timestamp_pair(minutes)

        mission.update(
            {
                "stage": "STAFF_TURNOUT",
                "alert_mode": mode,
                "turnout_required": required,
                "turnout_first_arrived": first_arrived,
                "turnout_total_arrived": total,
                "turnout_finish_ts": ts["raw"],
            }
        )
        await user_conf.active_mission.set(mission)

        await interaction.response.send_message(
            f"📟 Crew alerted with **{mode}** mode.\n"
            f"Turnout expected {ts['relative']} ({ts['absolute']}).",
            ephemeral=True,
        )

        await asyncio.sleep(int(minutes * 60))

        await self._show_turnout_result(user)

    async def _show_turnout_result(self, user: discord.abc.User):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission") or {}
        if mission.get("stage") != "STAFF_TURNOUT":
            return

        required = mission.get("turnout_required", 0)
        arrived = mission.get("turnout_total_arrived", 0)

        text = (
            "👥 Turnout finished.\n"
            f"Required staff: **{required}**\n"
            f"Arrived: **{arrived}**\n\n"
            "You can re-page for more, proceed with current crew, or cancel the call."
        )

        view = TurnoutDecisionView(self, user)
        try:
            await user.send(text, view=view)
        except Exception:
            # optionally fall back to a guild channel
            pass

    async def handle_realert(self, interaction: discord.Interaction, user: discord.abc.User):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission") or {}
        if mission.get("stage") != "STAFF_TURNOUT":
            await interaction.response.send_message("This incident is no longer in turnout stage.", ephemeral=True)
            return

        glb = await self.config.all()
        required = int(mission.get("turnout_required", 0))
        current_total = int(mission.get("turnout_total_arrived", 0))

        res = self._simulate_realert(current_total, required)
        mission["turnout_total_arrived"] = res["total_arrived"]
        mission["turnout_second_arrived"] = res["second_arrived"]

        min_minutes = float(glb.get("realert_minutes_min", 1.0))
        max_minutes = float(glb.get("realert_minutes_max", 3.0))
        minutes = random.uniform(min_minutes, max_minutes)
        ts = self._make_timestamp_pair(minutes)
        mission["turnout_finish_ts"] = ts["raw"]

        await user_conf.active_mission.set(mission)

        await interaction.response.send_message(
            f"📟 Re-alert sent. Additional turnout expected {ts['relative']} ({ts['absolute']}).",
            ephemeral=True,
        )

        await asyncio.sleep(int(minutes * 60))
        await self._show_turnout_result(user)

    async def handle_proceed_to_vehicles(self, interaction: discord.Interaction, user: discord.abc.User):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission") or {}
        if mission.get("stage") != "STAFF_TURNOUT":
            await interaction.response.send_message("This incident is no longer in turnout stage.", ephemeral=True)
            return

        arrived = int(mission.get("turnout_total_arrived", 0))
        if arrived <= 0:
            await interaction.response.send_message(
                "No one turned out. You cannot dispatch any units.", ephemeral=True
            )
            return

        mission["stage"] = "VEHICLE_SELECT"
        await user_conf.active_mission.set(mission)

        vehicles = await self._get_user_vehicles(user)
        view = VehicleSelectView(self, user, vehicles)
        await interaction.response.send_message(
            f"🚒 {arrived} personnel available. Select vehicles to dispatch:",
            view=view,
            ephemeral=True,
        )

    async def handle_cancel_incident(self, interaction: discord.Interaction, user: discord.abc.User):
        user_conf = self.config.user(user)
        await user_conf.active_mission.set(None)
        await interaction.response.send_message("Incident cancelled.", ephemeral=True)

    async def handle_vehicle_selection(
        self,
        interaction: discord.Interaction,
        user: discord.abc.User,
        values: List[str],
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission") or {}
        if mission.get("stage") != "VEHICLE_SELECT":
            await interaction.response.send_message("This incident is not in vehicle selection stage.", ephemeral=True)
            return

        if not values:
            await interaction.response.send_message("No vehicles selected.", ephemeral=True)
            return

        glb = await self.config.all()
        min_minutes = float(glb.get("travel_minutes_min", 3.0))
        max_minutes = float(glb.get("travel_minutes_max", 8.0))
        minutes = random.uniform(min_minutes, max_minutes)
        ts = self._make_timestamp_pair(minutes)

        mission["selected_vehicle_ids"] = [int(v) for v in values]
        mission["stage"] = "TRAVEL"
        mission["travel_finish_ts"] = ts["raw"]
        await user_conf.active_mission.set(mission)

        await interaction.response.send_message(
            f"🚨 Units are en route. ETA {ts['relative']} ({ts['absolute']}).",
            ephemeral=True,
        )

        await asyncio.sleep(int(minutes * 60))
        await self._send_travel_update(user)

    async def _send_travel_update(self, user: discord.abc.User):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission") or {}
        if mission.get("stage") != "TRAVEL":
            return

        title = mission.get("title", "Incident")
        detail = mission.get("detail", "Units report additional information en route.")

        embed = discord.Embed(
            title=f"On-scene update – {title}",
            description=detail,
            color=discord.Color.orange(),
        )
        embed.set_footer(text="Use this information to judge if your dispatch was sufficient.")

        try:
            await user.send(embed=embed)
        except Exception:
            pass

        await self._resolve_incident(user)

    async def _resolve_incident(self, user: discord.abc.User):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission") or {}
        if mission.get("stage") not in {"TRAVEL", "VEHICLE_SELECT"}:
            return

        required = int(mission.get("required_staff", 0))
        arrived = int(mission.get("turnout_total_arrived", 0))
        vehicles = await self._get_user_vehicles(user)
        selected_ids = mission.get("selected_vehicle_ids", [])
        selected = [v for v in vehicles if v["id"] in selected_ids]

        total_capacity = sum(v.get("crew_capacity", 0) for v in selected)

        ratio_staff = arrived / required if required else 1.0
        ratio_capacity = total_capacity / required if required else 1.0

        success_score = (ratio_staff * 0.6) + (ratio_capacity * 0.4)
        success_score = max(0.0, min(1.5, success_score))

        if success_score >= 1.0:
            outcome = "✅ Incident successfully handled."
            reward = int(1_000 * success_score)
        elif success_score >= 0.6:
            outcome = "⚠️ Incident handled with difficulties."
            reward = int(500 * success_score)
        else:
            outcome = "❌ Incident not successfully handled."
            reward = int(100 * success_score)

        credits = data.get("credits", 0) + reward
        await user_conf.credits.set(credits)
        await user_conf.active_mission.set(None)

        lines = [
            f"Incident: **{mission.get('title', 'Unknown')}**",
            "",
            f"Required staff: **{required}**",
            f"Arrived staff: **{arrived}**",
            f"Vehicles dispatched: **{len(selected)}** (cap {total_capacity})",
            "",
            outcome,
            f"Reward: **{reward}** credits",
            f"Total credits: **{credits}**",
        ]

        try:
            await user.send("\n".join(lines))
        except Exception:
            pass

    # --------------------------------------------------
    # Vehicle shop handling
    # --------------------------------------------------

    async def handle_vehicle_purchase(self, interaction: discord.Interaction, user: discord.abc.User, vehicle_id: str):
        user_conf = self.config.user(user)
        data = await user_conf.all()

        catalog = VehicleShopSelect.CATALOG
        if vehicle_id not in catalog:
            await interaction.response.send_message("Unknown vehicle type.", ephemeral=True)
            return

        vdef = catalog[vehicle_id]
        price = int(vdef["price"])
        credits = int(data.get("credits", 0))
        if credits < price:
            await interaction.response.send_message(
                f"You do not have enough credits. You need {price} but only have {credits}.",
                ephemeral=True,
            )
            return

        vehicles = data.get("vehicles", [])
        next_id = int(data.get("next_vehicle_id", 1))
        new_vehicle = {
            "id": next_id,
            "name": vdef["name"],
            "crew_capacity": int(vdef["crew_capacity"]),
        }
        vehicles.append(new_vehicle)

        await user_conf.credits.set(credits - price)
        await user_conf.vehicles.set(vehicles)
        await user_conf.next_vehicle_id.set(next_id + 1)

        await interaction.response.send_message(
            f"Purchased **{vdef['name']}** for {price} credits. It has been added to your fleet.",
            ephemeral=True,
        )
