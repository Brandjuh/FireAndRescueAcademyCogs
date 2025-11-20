
from __future__ import annotations

import asyncio
import random
from typing import Any, Dict, List

import discord
from redbot.core import commands, Config, bank


class FireStationCommand(commands.Cog):
    """Fire station management & incident mini-game."""

    __version__ = "1.1.1"

    def __init__(self, bot):
        self.bot = bot
        # fresh identifier
        self.config = Config.get_conf(self, identifier=0xF15704, force_registration=True)

        default_global: Dict[str, Any] = {
            "volunteer_normal_minutes": 15.0,
            "volunteer_emergency_minutes": 5.0,
            "career_turnout_minutes": 0.0,
            "realert_minutes_min": 1.0,
            "realert_minutes_max": 3.0,
            "travel_minutes_min": 3.0,
            "travel_minutes_max": 8.0,
            "staff_cost": 2000,
            "upgrade_base_cost": 50000,
            "career_convert_cost": 250000,
            "max_station_level": 5,
        }

        default_user: Dict[str, Any] = {
            "started": False,
            "credits": 0,
            "vehicles": [],
            "next_vehicle_id": 1,
            "station_level": 1,
            "station_type": "volunteer",  # volunteer | career
            "staff_total": 6,
            "staff_trained": 0,
            "active_mission": {},
        }

        self.config.register_global(**default_global)
        self.config.register_user(**default_user)

        self.INCIDENTS: List[Dict[str, Any]] = [
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

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------

    async def _ensure_started(self, ctx: commands.Context) -> bool:
        data = await self.config.user(ctx.author).all()
        if not data["started"]:
            await ctx.send("You have not started yet. Use `[p]fsc start` first.")
            return False
        return True

    async def _get_credits(self, user: discord.abc.User) -> int:
        try:
            return int(await bank.get_balance(user))
        except Exception:
            data = await self.config.user(user).all()
            return int(data.get("credits", 0))

    async def _give(self, user: discord.abc.User, amount: int) -> None:
        if amount <= 0:
            return
        try:
            await bank.deposit_credits(user, amount)
            return
        except Exception:
            pass
        user_conf = self.config.user(user)
        data = await user_conf.all()
        local = int(data.get("credits", 0))
        await user_conf.credits.set(local + amount)

    async def _spend(self, user: discord.abc.User, amount: int) -> bool:
        if amount <= 0:
            return True
        try:
            can = await bank.can_spend(user, amount)  # type: ignore[attr-defined]
            if can:
                await bank.withdraw_credits(user, amount)
                return True
        except Exception:
            pass
        user_conf = self.config.user(user)
        data = await user_conf.all()
        local = int(data.get("credits", 0))
        if local < amount:
            return False
        await user_conf.credits.set(local - amount)
        return True

    async def _get_user_vehicles(self, user: discord.abc.User) -> List[Dict[str, Any]]:
        data = await self.config.user(user).all()
        return data.get("vehicles", [])

    def _max_staff(self, level: int) -> int:
        if level < 1:
            level = 1
        return 6 + (level - 1) * 2

    def _max_vehicles(self, level: int) -> int:
        if level < 1:
            level = 1
        return 1 + (level - 1)

    def _pick_random_incident(self) -> Dict[str, Any]:
        return random.choice(self.INCIDENTS)

    def _make_relative_text(self, minutes: float) -> str:
        mins = int(round(minutes))
        if mins <= 0:
            return "now"
        if mins == 1:
            return "in 1 minute"
        return f"in {mins} minutes"

    def _simulate_emergency_turnout(self, available: int, required: int) -> Dict[str, int]:
        if available <= 0:
            return {"available": 0, "arrived": 0}
        arrived = 0
        base_no_show = random.uniform(0.10, 0.25)
        for _ in range(available):
            if random.random() > base_no_show:
                arrived += 1
        return {"available": available, "arrived": arrived}

    def _simulate_realert(self, current_total: int, available: int, required: int) -> Dict[str, int]:
        if current_total >= available:
            return {"second_arrived": 0, "total_arrived": current_total}
        remaining = available - current_total
        no_show_second = random.uniform(0.0, 0.10)
        second_arrived = 0
        for _ in range(remaining):
            if random.random() > no_show_second:
                second_arrived += 1
        return {
            "second_arrived": second_arrived,
            "total_arrived": current_total + second_arrived,
        }

    # --------------------------------------------------
    # Commands
    # --------------------------------------------------

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
        await user_conf.vehicles.set([starter_vehicle])
        await user_conf.next_vehicle_id.set(2)
        await user_conf.station_level.set(1)
        await user_conf.station_type.set("volunteer")
        await user_conf.staff_total.set(6)
        await user_conf.staff_trained.set(0)
        await user_conf.active_mission.set({})

        await self._give(ctx.author, 100_000)

        credits = await self._get_credits(ctx.author)
        embed = discord.Embed(
            title="Station created",
            description="You are now the commander of a small **volunteer** station.",
            color=discord.Color.red(),
        )
        embed.add_field(name="Starter vehicle", value="🚒 Starter Fire Engine (crew 6)", inline=False)
        embed.add_field(name="Staff", value="6 volunteers (untrained)", inline=True)
        embed.add_field(name="Credits", value=f"{credits:,}", inline=True)
        try:
            await ctx.send(embed=embed)
        except Exception:
            # Fallback if Discord temporarily disconnects
            await ctx.send("Station created. (Discord had trouble sending the embed.)")

    @fsc_group.command(name="status")
    async def fsc_status(self, ctx: commands.Context):
        """Show station, staff, vehicles, and active mission."""
        if not await self._ensure_started(ctx):
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        vehicles = data.get("vehicles", [])
        credits = await self._get_credits(ctx.author)

        lvl = int(data.get("station_level", 1))
        stype = data.get("station_type", "volunteer")
        staff_total = int(data.get("staff_total", 0))
        staff_trained = int(data.get("staff_trained", 0))
        active = data.get("active_mission", {}) or {}

        max_staff = self._max_staff(lvl)
        max_veh = self._max_vehicles(lvl)

        embed = discord.Embed(
            title="Fire Station Status",
            color=discord.Color.red(),
        )
        embed.add_field(name="Credits", value=f"{credits:,}", inline=True)
        embed.add_field(name="Station level", value=str(lvl), inline=True)
        embed.add_field(name="Type", value=stype.capitalize(), inline=True)

        embed.add_field(
            name="Staff",
            value=f"{staff_total} total ({staff_trained} trained) / max {max_staff}",
            inline=False,
        )
        embed.add_field(
            name="Vehicles",
            value=f"{len(vehicles)} / max {max_veh}",
            inline=False,
        )

        if active:
            embed.add_field(
                name="Active incident",
                value=f"{active.get('title', 'Unknown')} (stage: `{active.get('stage', 'unknown')}`)",
                inline=False,
            )
        else:
            embed.add_field(name="Active incident", value="None", inline=False)

        await ctx.send(embed=embed)

    @fsc_group.command(name="station")
    async def fsc_station(self, ctx: commands.Context):
        """Detailed overview of your station (with image by bay count)."""
        if not await self._ensure_started(ctx):
            return

        data = await self.config.user(ctx.author).all()
        lvl = int(data.get("station_level", 1))
        stype = data.get("station_type", "volunteer")
        staff_total = int(data.get("staff_total", 0))
        staff_trained = int(data.get("staff_trained", 0))
        vehicles = data.get("vehicles", [])

        max_staff = self._max_staff(lvl)
        max_veh = self._max_vehicles(lvl)

        embed = discord.Embed(
            title="Station overview",
            color=discord.Color.dark_red(),
        )
        embed.add_field(name="Level", value=str(lvl), inline=True)
        embed.add_field(name="Type", value=stype.capitalize(), inline=True)
        embed.add_field(name="Vehicle capacity", value=f"{len(vehicles)} / {max_veh}", inline=True)

        embed.add_field(
            name="Staff",
            value=f"{staff_total} total ({staff_trained} trained) / max {max_staff}",
            inline=False,
        )

        if stype == "volunteer":
            embed.add_field(
                name="Turnout profile",
                value="Volunteer: slower turnout, chance of no-shows.",
                inline=False,
            )
        else:
            embed.add_field(
                name="Turnout profile",
                value="Career: instant turnout (in-game), full crew expected.",
                inline=False,
            )

        # Choose image by bay capacity (parking places)
        image_url = None
        if max_veh <= 1:
            image_url = "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/refs/heads/main/FireStationCommand/Images/FDT1B1.png"
        elif max_veh == 2:
            image_url = "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/refs/heads/main/FireStationCommand/Images/FDT1B2.png"
        elif max_veh >= 3:
            image_url = "https://raw.githubusercontent.com/Brandjuh/FireAndRescueAcademyCogs/refs/heads/main/FireStationCommand/Images/FDT1B3.png"

        if image_url:
            embed.set_image(url=image_url)

        await ctx.send(embed=embed)

    @fsc_group.command(name="recruit")
    async def fsc_recruit(self, ctx: commands.Context, amount: int):
        """Recruit new staff (with confirmation)."""
        if not await self._ensure_started(ctx):
            return
        if amount <= 0:
            await ctx.send("Amount must be positive.")
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        lvl = int(data.get("station_level", 1))
        staff_total = int(data.get("staff_total", 0))

        max_staff = self._max_staff(lvl)
        if staff_total >= max_staff:
            await ctx.send("You are already at maximum staff for your current station level.")
            return

        if staff_total + amount > max_staff:
            amount = max_staff - staff_total

        glb = await self.config.all()
        cost_per = int(glb.get("staff_cost", 2000))
        total_cost = amount * cost_per

        credits = await self._get_credits(ctx.author)
        if credits < total_cost:
            await ctx.send(
                f"You do not have enough credits. Recruiting {amount} costs {total_cost:,}, "
                f"but you only have {credits:,}."
            )
            return

        embed = discord.Embed(
            title="Confirm recruitment",
            description=f"Recruit **{amount}** new staff for **{total_cost:,}** credits?",
            color=discord.Color.green(),
        )
        embed.add_field(name="After recruitment", value=f"{staff_total + amount} / {max_staff} staff", inline=False)

        view = ConfirmRecruitView(self, ctx.author, amount, total_cost)
        await ctx.send(embed=embed, view=view)

    async def _confirm_recruit(self, interaction: discord.Interaction, user: discord.abc.User, amount: int, total_cost: int):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        lvl = int(data.get("station_level", 1))
        staff_total = int(data.get("staff_total", 0))
        max_staff = self._max_staff(lvl)

        if staff_total >= max_staff:
            await interaction.response.send_message("Recruitment failed: already at maximum staff.", ephemeral=True)
            return

        if staff_total + amount > max_staff:
            amount = max_staff - staff_total
            total_cost = 0  # we will recompute
            glb = await self.config.all()
            cost_per = int(glb.get("staff_cost", 2000))
            total_cost = amount * cost_per

        credits = await self._get_credits(user)
        if credits < total_cost or not await self._spend(user, total_cost):
            await interaction.response.send_message("Recruitment failed: not enough credits.", ephemeral=True)
            return

        staff_total += amount
        await user_conf.staff_total.set(staff_total)

        embed = discord.Embed(
            title="Recruitment complete",
            description=f"Recruited **{amount}** new staff.",
            color=discord.Color.green(),
        )
        embed.add_field(name="Total staff", value=f"{staff_total} / {max_staff}", inline=True)
        embed.add_field(name="Cost", value=f"{total_cost:,} credits", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=False)

    @fsc_group.command(name="upgrade")
    async def fsc_upgrade(self, ctx: commands.Context):
        """Upgrade your station level (confirmation)."""
        if not await self._ensure_started(ctx):
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        lvl = int(data.get("station_level", 1))

        glb = await self.config.all()
        max_lvl = int(glb.get("max_station_level", 5))
        if lvl >= max_lvl:
            await ctx.send("Your station is already at the maximum level.")
            return

        base = int(glb.get("upgrade_base_cost", 50000))
        cost = base * lvl

        credits = await self._get_credits(ctx.author)
        if credits < cost:
            await ctx.send(
                f"You do not have enough credits to upgrade. "
                f"Level {lvl} → {lvl + 1} costs {cost:,}, you have {credits:,}."
            )
            return

        new_lvl = lvl + 1
        max_staff = self._max_staff(new_lvl)
        max_veh = self._max_vehicles(new_lvl)

        embed = discord.Embed(
            title="Confirm station upgrade",
            description=f"Upgrade station from level **{lvl}** to **{new_lvl}** for **{cost:,}** credits?",
            color=discord.Color.blue(),
        )
        embed.add_field(name="New staff capacity", value=f"max {max_staff}", inline=True)
        embed.add_field(name="New vehicle capacity", value=f"max {max_veh}", inline=True)

        view = ConfirmUpgradeView(self, ctx.author, new_lvl, cost)
        await ctx.send(embed=embed, view=view)

    async def _confirm_upgrade(self, interaction: discord.Interaction, user: discord.abc.User, new_level: int, cost: int):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        current_lvl = int(data.get("station_level", 1))

        if new_level <= current_lvl:
            await interaction.response.send_message("Upgrade cancelled: level already changed.", ephemeral=True)
            return

        credits = await self._get_credits(user)
        if credits < cost or not await self._spend(user, cost):
            await interaction.response.send_message("Upgrade failed: not enough credits.", ephemeral=True)
            return

        await user_conf.station_level.set(new_level)

        max_staff = self._max_staff(new_level)
        max_veh = self._max_vehicles(new_level)

        embed = discord.Embed(
            title="Station upgraded",
            description=f"Your station is now level **{new_level}**.",
            color=discord.Color.blue(),
        )
        embed.add_field(name="Staff capacity", value=f"max {max_staff}", inline=True)
        embed.add_field(name="Vehicle capacity", value=f"max {max_veh}", inline=True)
        embed.add_field(name="Cost", value=f"{cost:,} credits", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=False)

    @fsc_group.command(name="career")
    async def fsc_career(self, ctx: commands.Context):
        """Convert your volunteer station to a career station (confirmation)."""
        if not await self._ensure_started(ctx):
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        stype = data.get("station_type", "volunteer")
        lvl = int(data.get("station_level", 1))

        if stype == "career":
            await ctx.send("Your station is already a career station.")
            return

        glb = await self.config.all()
        required_lvl = 2
        if lvl < required_lvl:
            await ctx.send(f"You must be at least station level {required_lvl} to convert to a career station.")
            return

        cost = int(glb.get("career_convert_cost", 250000))
        credits = await self._get_credits(ctx.author)
        if credits < cost:
            await ctx.send(
                f"You do not have enough credits. Converting to a career station costs {cost:,}, "
                f"but you only have {credits:,}."
            )
            return

        embed = discord.Embed(
            title="Confirm career conversion",
            description=f"Convert your station to **career** for **{cost:,}** credits?",
            color=discord.Color.gold(),
        )
        embed.add_field(
            name="Effect",
            value="Turnout becomes effectively instant and more reliable.",
            inline=False,
        )

        view = ConfirmCareerView(self, ctx.author, cost)
        await ctx.send(embed=embed, view=view)

    async def _confirm_career(self, interaction: discord.Interaction, user: discord.abc.User, cost: int):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        stype = data.get("station_type", "volunteer")
        if stype == "career":
            await interaction.response.send_message("Your station is already a career station.", ephemeral=True)
            return

        credits = await self._get_credits(user)
        if credits < cost or not await self._spend(user, cost):
            await interaction.response.send_message("Conversion failed: not enough credits.", ephemeral=True)
            return

        await user_conf.station_type.set("career")

        embed = discord.Embed(
            title="Station converted",
            description="Your station is now a **career** station. Turnout is effectively instant.",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Cost", value=f"{cost:,} credits", inline=True)
        await interaction.response.send_message(embed=embed, ephemeral=False)

    @fsc_group.command(name="shop")
    async def fsc_shop(self, ctx: commands.Context):
        """Open the vehicle shop (dropdown)."""
        if not await self._ensure_started(ctx):
            return

        data = await self.config.user(ctx.author).all()
        lvl = int(data.get("station_level", 1))
        vehicles = data.get("vehicles", [])
        max_veh = self._max_vehicles(lvl)
        if len(vehicles) >= max_veh:
            await ctx.send("You are at maximum vehicle capacity. Upgrade your station to buy more vehicles.")
            return

        view = VehicleShopView(self, ctx.channel, ctx.author)
        embed = discord.Embed(
            title="Vehicle shop",
            description="Select a vehicle to purchase from the menu below.",
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Capacity",
            value=f"{len(vehicles)} / {max_veh} vehicles currently in your station.",
            inline=False,
        )
        await ctx.send(embed=embed, view=view)

    @fsc_group.command(name="mission")
    async def fsc_mission(self, ctx: commands.Context):
        """Start a new incident if none is active."""
        if not await self._ensure_started(ctx):
            return

        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        active = data.get("active_mission", {}) or {}
        if active:
            await ctx.send("You already have an active incident. Finish or cancel it first.")
            return

        staff_total = int(data.get("staff_total", 0))
        if staff_total <= 0:
            await ctx.send("You have no staff at your station. Recruit staff before taking incidents.")
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
            "channel_id": ctx.channel.id,
            "guild_id": ctx.guild.id if ctx.guild else None,
        }
        await user_conf.active_mission.set(mission)

        view = AlertChoiceView(self, ctx.channel, ctx.author)

        embed = discord.Embed(
            title=f"🚨 New incident: {incident['name']}",
            description=incident["hint"],
            color=discord.Color.red(),
        )
        embed.add_field(name="Required staff", value=str(incident["required_staff"]), inline=True)
        embed.set_footer(text="Choose how to alert your crew below.")
        await ctx.send(embed=embed, view=view)

    # --------------------------------------------------
    # Workflow handlers
    # --------------------------------------------------

    async def handle_alert_choice(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        mode: str,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if mission.get("stage") != "ALERT_CHOICE":
            await interaction.response.send_message("This incident is no longer in the alert stage.", ephemeral=True)
            return

        glb = await self.config.all()
        stype = data.get("station_type", "volunteer")
        required = int(mission.get("required_staff", 6))
        staff_total = int(data.get("staff_total", 0))

        if staff_total <= 0:
            await interaction.response.send_message("You have no staff at your station.", ephemeral=True)
            await user_conf.active_mission.set({})
            return

        available = staff_total

        if stype == "career":
            minutes = float(glb.get("career_turnout_minutes", 0.0))
            first_arrived = min(available, required)
            total = first_arrived
        else:
            if mode == "normal":
                minutes = float(glb.get("volunteer_normal_minutes", 15.0))
                first_arrived = min(available, required)
                total = first_arrived
            else:
                minutes = float(glb.get("volunteer_emergency_minutes", 5.0))
                sim = self._simulate_emergency_turnout(available, required)
                first_arrived = sim["arrived"]
                total = first_arrived

        rel = self._make_relative_text(minutes)

        mission.update(
            {
                "stage": "STAFF_TURNOUT",
                "alert_mode": mode,
                "turnout_required": required,
                "turnout_available": available,
                "turnout_first_arrived": first_arrived,
                "turnout_total_arrived": total,
            }
        )
        await user_conf.active_mission.set(mission)

        await interaction.response.send_message(
            f"📟 Crew alerted with **{mode}** mode. Turnout expected {rel}.",
            ephemeral=False,
        )

        if minutes > 0:
            await asyncio.sleep(int(minutes * 60))

        await self._show_turnout_result(channel, user)

    async def _show_turnout_result(self, channel: discord.abc.Messageable, user: discord.abc.User):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if mission.get("stage") != "STAFF_TURNOUT":
            return

        required = int(mission.get("turnout_required", 0))
        arrived = int(mission.get("turnout_total_arrived", 0))
        available = int(mission.get("turnout_available", 0))

        embed = discord.Embed(
            title="Turnout result",
            color=discord.Color.orange(),
        )
        embed.add_field(name="Required staff", value=str(required), inline=True)
        embed.add_field(name="Available staff", value=str(available), inline=True)
        embed.add_field(name="Arrived staff", value=str(arrived), inline=True)
        embed.set_footer(text="Re-alert for more crew, proceed with current crew, or cancel the call.")

        view = TurnoutDecisionView(self, channel, user)

        try:
            await channel.send(embed=embed, view=view)
        except Exception:
            try:
                await user.send(embed=embed, view=view)
            except Exception:
                pass

    async def handle_realert(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
        if mission.get("stage") != "STAFF_TURNOUT":
            await interaction.response.send_message("This incident is no longer in turnout stage.", ephemeral=True)
            return

        glb = await self.config.all()
        required = int(mission.get("turnout_required", 0))
        available = int(mission.get("turnout_available", 0))
        current_total = int(mission.get("turnout_total_arrived", 0))

        sim = self._simulate_realert(current_total, available, required)
        mission["turnout_total_arrived"] = sim["total_arrived"]
        mission["turnout_second_arrived"] = sim["second_arrived"]

        min_minutes = float(glb.get("realert_minutes_min", 1.0))
        max_minutes = float(glb.get("realert_minutes_max", 3.0))
        minutes = random.uniform(min_minutes, max_minutes)
        rel = self._make_relative_text(minutes)

        await user_conf.active_mission.set(mission)

        await interaction.response.send_message(
            f"📟 Re-alert sent. Additional turnout expected {rel}.",
            ephemeral=False,
        )

        await asyncio.sleep(int(minutes * 60))
        await self._show_turnout_result(channel, user)

    async def handle_proceed_to_vehicles(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
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
        view = VehicleSelectView(self, channel, user, vehicles)

        embed = discord.Embed(
            title="Vehicle selection",
            description=f"{arrived} personnel available. Select vehicles to dispatch.",
            color=discord.Color.blue(),
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=False)

    async def handle_cancel_incident(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
    ):
        user_conf = self.config.user(user)
        await user_conf.active_mission.set({})
        await interaction.response.send_message("Incident cancelled.", ephemeral=False)

    async def handle_vehicle_selection(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        values: List[str],
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
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
        rel = self._make_relative_text(minutes)

        mission["selected_vehicle_ids"] = [int(v) for v in values]
        mission["stage"] = "TRAVEL"
        await user_conf.active_mission.set(mission)

        await interaction.response.send_message(
            f"🚨 Units are en route. ETA {rel}.",
            ephemeral=False,
        )

        await asyncio.sleep(int(minutes * 60))
        await self._send_travel_update(channel, user)

    async def _send_travel_update(self, channel: discord.abc.Messageable, user: discord.abc.User):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
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
            await channel.send(embed=embed)
        except Exception:
            try:
                await user.send(embed=embed)
            except Exception:
                pass

        await self._resolve_incident(channel, user)

    async def _resolve_incident(self, channel: discord.abc.Messageable, user: discord.abc.User):
        user_conf = self.config.user(user)
        data = await user_conf.all()
        mission = data.get("active_mission", {}) or {}
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

        await self._give(user, reward)
        total_credits = await self._get_credits(user)
        await user_conf.active_mission.set({})

        embed = discord.Embed(
            title=f"Incident result – {mission.get('title', 'Unknown')}",
            color=discord.Color.green() if success_score >= 1.0 else discord.Color.orange(),
        )
        embed.add_field(name="Required staff", value=str(required), inline=True)
        embed.add_field(name="Arrived staff", value=str(arrived), inline=True)
        embed.add_field(name="Vehicles dispatched", value=f"{len(selected)} (cap {total_capacity})", inline=True)
        embed.add_field(name="Outcome", value=outcome, inline=False)
        embed.add_field(name="Reward", value=f"{reward:,} credits", inline=True)
        embed.add_field(name="Total credits", value=f"{total_credits:,}", inline=True)

        try:
            await channel.send(embed=embed)
        except Exception:
            try:
                await user.send(embed=embed)
            except Exception:
                pass

    # --------------------------------------------------
    # Vehicle shop handling
    # --------------------------------------------------

    async def _confirm_vehicle_purchase(
        self,
        interaction: discord.Interaction,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        vehicle_id: str,
    ):
        user_conf = self.config.user(user)
        data = await user_conf.all()

        catalog = VehicleShopSelect.CATALOG
        if vehicle_id not in catalog:
            await interaction.response.send_message("Unknown vehicle type.", ephemeral=True)
            return

        vdef = catalog[vehicle_id]
        price = int(vdef["price"])

        # Check capacity again
        lvl = int(data.get("station_level", 1))
        vehicles = data.get("vehicles", [])
        max_veh = self._max_vehicles(lvl)
        if len(vehicles) >= max_veh:
            await interaction.response.send_message(
                "Purchase failed: you are at maximum vehicle capacity.", ephemeral=True
            )
            return

        credits = await self._get_credits(user)
        if credits < price or not await self._spend(user, price):
            await interaction.response.send_message(
                "You do not have enough credits to complete this purchase.",
                ephemeral=True,
            )
            return

        next_id = int(data.get("next_vehicle_id", 1))
        new_vehicle = {
            "id": next_id,
            "name": vdef["name"],
            "crew_capacity": int(vdef["crew_capacity"]),
        }
        vehicles.append(new_vehicle)

        await user_conf.vehicles.set(vehicles)
        await user_conf.next_vehicle_id.set(next_id + 1)

        await interaction.response.send_message(
            f"Purchased **{vdef['name']}** for {price:,} credits. It has been added to your fleet.",
            ephemeral=False,
        )


class AlertChoiceView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, channel: discord.abc.Messageable, user: discord.abc.User):
        super().__init__(timeout=120)
        self.cog = cog
        self.channel = channel
        self.user = user

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Normal turnout (volunteer)", style=discord.ButtonStyle.secondary)
    async def normal(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Disable buttons to prevent double-clicks
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog.handle_alert_choice(interaction, self.channel, self.user, "normal")
        self.stop()

    @discord.ui.button(label="Emergency turnout (volunteer)", style=discord.ButtonStyle.danger)
    async def emergency(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog.handle_alert_choice(interaction, self.channel, self.user, "emergency")
        self.stop()


class TurnoutDecisionView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, channel: discord.abc.Messageable, user: discord.abc.User):
        super().__init__(timeout=180)
        self.cog = cog
        self.channel = channel
        self.user = user

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    async def _disable_and_edit(self, interaction: discord.Interaction):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass

    @discord.ui.button(label="Re-alert", style=discord.ButtonStyle.secondary)
    async def realert(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._disable_and_edit(interaction)
        await self.cog.handle_realert(interaction, self.channel, self.user)
        self.stop()

    @discord.ui.button(label="Proceed to vehicle selection", style=discord.ButtonStyle.success)
    async def proceed(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._disable_and_edit(interaction)
        await self.cog.handle_proceed_to_vehicles(interaction, self.channel, self.user)
        self.stop()

    @discord.ui.button(label="Cancel incident", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._disable_and_edit(interaction)
        await self.cog.handle_cancel_incident(interaction, self.channel, self.user)
        self.stop()


class VehicleSelect(discord.ui.Select):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        vehicles: List[Dict[str, Any]],
    ):
        options: List[discord.SelectOption] = []
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
        self.channel = channel
        self.user = user

    async def callback(self, interaction: discord.Interaction):
        if self.values == ["none"]:
            await interaction.response.send_message("You have no vehicles to dispatch.", ephemeral=True)
            return
        await self.cog.handle_vehicle_selection(interaction, self.channel, self.user, list(self.values))


class VehicleSelectView(discord.ui.View):
    def __init__(
        self,
        cog: FireStationCommand,
        channel: discord.abc.Messageable,
        user: discord.abc.User,
        vehicles: List[Dict[str, Any]],
    ):
        super().__init__(timeout=180)
        self.add_item(VehicleSelect(cog, channel, user, vehicles))


class VehicleShopSelect(discord.ui.Select):
    CATALOG = {
        "pumper": {"name": "Fire Engine", "crew_capacity": 6, "price": 50_000},
        "ladder": {"name": "Aerial Ladder", "crew_capacity": 3, "price": 75_000},
        "rescue": {"name": "Rescue Unit", "crew_capacity": 4, "price": 65_000},
    }

    def __init__(self, cog: FireStationCommand, channel: discord.abc.Messageable, user: discord.abc.User):
        self.cog = cog
        self.channel = channel
        self.user = user

        options: List[discord.SelectOption] = []
        for vid, v in self.CATALOG.items():
            label = f"{v['name']} ({v['price']:,} cr, cap {v['crew_capacity']})"
            options.append(discord.SelectOption(label=label, value=vid))

        super().__init__(
            placeholder="Select a vehicle to buy",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        choice = self.values[0]
        vdef = self.CATALOG.get(choice)
        if not vdef:
            await interaction.response.send_message("Unknown vehicle type.", ephemeral=True)
            return

        price = int(vdef["price"])
        embed = discord.Embed(
            title="Confirm vehicle purchase",
            description=f"Buy **{vdef['name']}** for **{price:,}** credits?",
            color=discord.Color.blurple(),
        )
        embed.add_field(name="Crew capacity", value=str(vdef["crew_capacity"]), inline=True)

        view = ConfirmVehiclePurchaseView(self.cog, self.channel, self.user, choice)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class VehicleShopView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, channel: discord.abc.Messageable, user: discord.abc.User):
        super().__init__(timeout=180)
        self.add_item(VehicleShopSelect(cog, channel, user))


class ConfirmRecruitView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, user: discord.abc.User, amount: int, cost: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.user = user
        self.amount = amount
        self.cost = cost

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog._confirm_recruit(interaction, self.user, self.amount, self.cost)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
        await interaction.response.send_message("Recruitment cancelled.", ephemeral=True)
        self.stop()


class ConfirmUpgradeView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, user: discord.abc.User, new_level: int, cost: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.user = user
        self.new_level = new_level
        self.cost = cost

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog._confirm_upgrade(interaction, self.user, self.new_level, self.cost)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
        await interaction.response.send_message("Upgrade cancelled.", ephemeral=True)
        self.stop()


class ConfirmCareerView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, user: discord.abc.User, cost: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.user = user
        self.cost = cost

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog._confirm_career(interaction, self.user, self.cost)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
        await interaction.response.send_message("Conversion cancelled.", ephemeral=True)
        self.stop()


class ConfirmVehiclePurchaseView(discord.ui.View):
    def __init__(self, cog: FireStationCommand, channel: discord.abc.Messageable, user: discord.abc.User, vehicle_id: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.channel = channel
        self.user = user
        self.vehicle_id = vehicle_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user.id

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        await interaction.message.edit(view=self)
        await self.cog._confirm_vehicle_purchase(interaction, self.channel, self.user, self.vehicle_id)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            child.disabled = True
        try:
            await interaction.message.edit(view=self)
        except Exception:
            pass
        await interaction.response.send_message("Purchase cancelled.", ephemeral=True)
        self.stop()
