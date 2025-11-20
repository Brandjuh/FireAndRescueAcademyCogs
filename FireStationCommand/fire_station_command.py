import asyncio
import logging
import random
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Any

import discord
from redbot.core import commands, Config, checks
from redbot.core.bot import Red

import yaml

log = logging.getLogger("red.FireStationCommand")


# ------------------------------
# Dataclasses for content
# ------------------------------


@dataclass
class VehicleDef:
    id: str
    name: str
    category: str
    tier_min: int
    base_cost: int
    maintenance_cost: int
    response_speed: float
    reliability: float
    required_staff: int
    required_training: List[str]
    equipment_slots: List[str]


@dataclass
class EquipmentDef:
    id: str
    name: str
    category: str
    base_cost: int
    durability: int
    required_training: List[str]


@dataclass
class ExpansionDef:
    id: str
    name: str
    description: str
    base_cost: int
    build_time_hours: int
    effects: Dict[str, Any]


@dataclass
class TrainingDef:
    id: str
    name: str
    duration_hours: int
    cost: int
    unlocks_vehicles: List[str]
    unlocks_missions: List[str]


@dataclass
class MissionDef:
    id: str
    name: str
    min_tier: int
    base_xp: int
    base_credits: int
    required_vehicles: List[str]
    description: str


@dataclass
class BalanceConfig:
    data: Dict[str, Any]


class GameContent:
    """Holds all loaded game content in memory."""

    def __init__(self):
        self.vehicles: Dict[str, VehicleDef] = {}
        self.equipment: Dict[str, EquipmentDef] = {}
        self.expansions: Dict[str, ExpansionDef] = {}
        self.trainings: Dict[str, TrainingDef] = {}
        self.missions: Dict[str, MissionDef] = {}
        self.balance: BalanceConfig = BalanceConfig({})

    @classmethod
    def from_files(cls, base_path: Path) -> "GameContent":
        content = cls()

        def load_yaml(name: str) -> Dict[str, Any]:
            path = base_path / f"{name}.yaml"
            if not path.exists():
                log.warning("Config file %s not found", path)
                return {}
            with path.open("r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}

        vehicles_raw = load_yaml("vehicles")
        for v in vehicles_raw.get("vehicles", []):
            try:
                content.vehicles[v["id"]] = VehicleDef(
                    id=v["id"],
                    name=v["name"],
                    category=v["category"],
                    tier_min=int(v.get("tier_min", 1)),
                    base_cost=int(v["base_cost"]),
                    maintenance_cost=int(v["maintenance_cost"]),
                    response_speed=float(v.get("response_speed", 1.0)),
                    reliability=float(v.get("reliability", 1.0)),
                    required_staff=int(v.get("required_staff", 1)),
                    required_training=list(v.get("required_training", [])),
                    equipment_slots=list(v.get("equipment_slots", [])),
                )
            except Exception:
                log.exception("Failed to load vehicle definition: %r", v)

        equipment_raw = load_yaml("equipment")
        for e in equipment_raw.get("equipment", []):
            try:
                content.equipment[e["id"]] = EquipmentDef(
                    id=e["id"],
                    name=e["name"],
                    category=e["category"],
                    base_cost=int(e["base_cost"]),
                    durability=int(e.get("durability", 100)),
                    required_training=list(e.get("required_training", [])),
                )
            except Exception:
                log.exception("Failed to load equipment definition: %r", e)

        expansions_raw = load_yaml("expansions")
        for ex in expansions_raw.get("expansions", []):
            try:
                content.expansions[ex["id"]] = ExpansionDef(
                    id=ex["id"],
                    name=ex["name"],
                    description=ex.get("description", ""),
                    base_cost=int(ex["base_cost"]),
                    build_time_hours=int(ex.get("build_time_hours", 1)),
                    effects=dict(ex.get("effects", {})),
                )
            except Exception:
                log.exception("Failed to load expansion definition: %r", ex)

        trainings_raw = load_yaml("trainings")
        for t in trainings_raw.get("trainings", []):
            try:
                content.trainings[t["id"]] = TrainingDef(
                    id=t["id"],
                    name=t["name"],
                    duration_hours=int(t["duration_hours"]),
                    cost=int(t["cost"]),
                    unlocks_vehicles=list(t.get("unlocks_vehicles", [])),
                    unlocks_missions=list(t.get("unlocks_missions", [])),
                )
            except Exception:
                log.exception("Failed to load training definition: %r", t)

        missions_raw = load_yaml("missions")
        for m in missions_raw.get("missions", []):
            try:
                content.missions[m["id"]] = MissionDef(
                    id=m["id"],
                    name=m["name"],
                    min_tier=int(m.get("min_tier", 1)),
                    base_xp=int(m["base_xp"]),
                    base_credits=int(m["base_credits"]),
                    required_vehicles=list(m.get("required_vehicles", [])),
                    description=m.get("description", ""),
                )
            except Exception:
                log.exception("Failed to load mission definition: %r", m)

        balance_raw = load_yaml("balance")
        content.balance = BalanceConfig(balance_raw.get("balance", {}))

        log.info(
            "Loaded content: %d vehicles, %d equipment, %d expansions, %d trainings, %d missions",
            len(content.vehicles),
            len(content.equipment),
            len(content.expansions),
            len(content.trainings),
            len(content.missions),
        )
        return content


class FireStationCommand(commands.Cog):
    """Fire Station Command – management game."""

    __author__ = "You + ChatGPT"
    __version__ = "0.5.0"

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self,
            identifier=1234567890123,
            force_registration=True,
        )

        default_user = {
            "started": False,
            "station_tier": 1,
            "xp": 0,
            "credits_earned": 0,
            "reputation": 50,
            "is_active": False,
            "last_mission_ts": None,
            "mission_streak": 0,
            "vehicles": [],
            "expansions": [],
            "personnel": [],
            "training_jobs": [],
            "repair_jobs": [],
            "completed_trainings": [],
        }

        self.config.register_user(**default_user)

        base_path = Path(__file__).parent / "data" / "config"
        self.content = GameContent.from_files(base_path)

        self.mission_task: Optional[asyncio.Task] = self.bot.loop.create_task(
            self.mission_scheduler()
        )

    async def cog_unload(self):
        if self.mission_task:
            self.mission_task.cancel()

    # ------------------------------
    # Helpers
    # ------------------------------

    def calc_station_tier(self, xp: int) -> int:
        return max(1, min(3, xp // 1000 + 1))

    def get_balance(self, key: str, default: Any = None) -> Any:
        return self.content.balance.data.get(key, default)

    def simulate_volunteers(self, required_staff: int) -> Dict[str, Any]:
        if required_staff <= 0:
            return {
                "required": 0,
                "first_arrived": 0,
                "second_arrived": 0,
                "total_arrived": 0,
                "used_realert": False,
                "turnout_seconds": 0,
            }

        min_no_show = float(self.get_balance("volunteer_no_show_min", 0.05))
        max_no_show = float(self.get_balance("volunteer_no_show_max", 0.2))
        base_no_show = random.uniform(min_no_show, max_no_show)
        realert_bonus = float(self.get_balance("volunteer_realert_bonus", 0.1))

        alert_min = int(self.get_balance("volunteer_alert_min_seconds", 60))
        alert_max = int(self.get_balance("volunteer_alert_max_seconds", 180))

        first_arrived = 0
        for _ in range(required_staff):
            if random.random() > base_no_show:
                first_arrived += 1

        turnout_first = random.randint(alert_min, alert_max)

        used_realert = False
        second_arrived = 0
        turnout_second = 0
        total_arrived = first_arrived

        if total_arrived < required_staff:
            used_realert = True
            no_show_second = max(0.0, base_no_show - realert_bonus)
            remaining = required_staff - total_arrived
            for _ in range(remaining):
                if random.random() > no_show_second:
                    second_arrived += 1

            total_arrived += second_arrived
            turnout_second = random.randint(alert_min, alert_max)

        turnout_seconds = turnout_first + (turnout_second if used_realert else 0)

        return {
            "required": required_staff,
            "first_arrived": first_arrived,
            "second_arrived": second_arrived,
            "total_arrived": total_arrived,
            "used_realert": used_realert,
            "turnout_seconds": turnout_seconds,
        }

    async def _process_training_jobs(self, user_conf, data) -> List[TrainingDef]:
        """Check running training jobs and mark completed ones."""
        now = int(dt.datetime.utcnow().timestamp())
        jobs = list(data.get("training_jobs", []))
        completed_trainings = set(data.get("completed_trainings", []))

        updated = False
        newly_completed: List[TrainingDef] = []

        for job in jobs:
            if job.get("completed"):
                continue
            start_ts = job.get("start_ts", 0)
            duration = job.get("duration", 0)
            if now >= start_ts + duration:
                job["completed"] = True
                updated = True
                t_id = job.get("training_id")
                t_def = self.content.trainings.get(t_id)
                if t_def:
                    newly_completed.append(t_def)
                if t_id and t_id not in completed_trainings:
                    completed_trainings.add(t_id)

        if updated:
            await user_conf.training_jobs.set(jobs)
            await user_conf.completed_trainings.set(list(completed_trainings))

        return newly_completed

    # ------------------------------
    # Mission scheduler (stub)
    # ------------------------------

    async def mission_scheduler(self):
        await self.bot.wait_until_ready()
        while True:
            try:
                log.debug("Mission scheduler tick")
            except Exception:
                log.exception("Error in mission scheduler")
            await asyncio.sleep(300)

    # ------------------------------
    # Core commands
    # ------------------------------

    @commands.group(name="fsc")
    async def fsc_group(self, ctx: commands.Context):
        """Fire Station Command main command."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @fsc_group.command(name="start")
    async def fsc_start(self, ctx: commands.Context):
        """Start your Fire Station Command career."""
        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        if data["started"]:
            await ctx.send("You already started your station.")
            return

        vehicles = []
        vehicles.append(
            {
                "id": 1,
                "def_id": "engine_basic",
                "condition": 100,
                "equipment": ["hose", "ba_basic", "basic_tools"],
            }
        )

        await user_conf.started.set(True)
        await user_conf.vehicles.set(vehicles)
        await user_conf.station_tier.set(1)
        await ctx.send(
            "Your **Tier 1 Volunteer Fire Station** has been created.\n"
            "- You start with 1 Standard Fire Engine.\n"
            "- You have 6 untrained volunteers (abstracted).\n"
            "Use `[p]fsc on` to go on duty and `[p]fsc mission` to request a mission."
        )

    @fsc_group.command(name="status")
    async def fsc_status(self, ctx: commands.Context):
        """Show your duty status and basic station info."""
        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        await self._process_training_jobs(user_conf, data)

        station_tier = self.calc_station_tier(data["xp"])
        is_active = data["is_active"]

        embed = discord.Embed(
            title="Fire Station Command – Status",
            color=discord.Color.red(),
        )
        embed.add_field(name="Started", value="Yes" if data["started"] else "No", inline=True)
        embed.add_field(name="Station Tier", value=str(station_tier), inline=True)
        embed.add_field(
            name="On Duty",
            value="Yes" if is_active else "No",
            inline=True,
        )
        embed.add_field(name="XP", value=str(data["xp"]), inline=True)
        embed.add_field(name="Reputation", value=str(data["reputation"]), inline=True)
        await ctx.send(embed=embed)

    @fsc_group.command(name="on")
    async def fsc_on(self, ctx: commands.Context):
        """Set yourself on duty (active)."""
        await self.config.user(ctx.author).is_active.set(True)
        await ctx.send("You are now **on duty**. You can request missions with `[p]fsc mission`.")

    @fsc_group.command(name="off")
    async def fsc_off(self, ctx: commands.Context):
        """Set yourself off duty (inactive)."""
        await self.config.user(ctx.author).is_active.set(False)
        await ctx.send("You are now **off duty**. You will no longer receive missions.")

    @fsc_group.command(name="profile")
    async def fsc_profile(self, ctx: commands.Context, member: Optional[discord.Member] = None):
        """Show your (or another player's) station profile."""
        target = member or ctx.author
        data = await self.config.user(target).all()
        station_tier = self.calc_station_tier(data["xp"])

        embed = discord.Embed(
            title=f"{target.display_name} – Fire Station Profile",
            color=discord.Color.orange(),
        )
        embed.add_field(name="Started", value="Yes" if data["started"] else "No", inline=True)
        embed.add_field(name="Station Tier", value=str(station_tier), inline=True)
        embed.add_field(name="XP", value=str(data["xp"]), inline=True)
        embed.add_field(name="Reputation", value=str(data["reputation"]), inline=True)
        embed.add_field(
            name="On Duty",
            value="Yes" if data["is_active"] else "No",
            inline=True,
        )
        embed.add_field(
            name="Vehicles Owned",
            value=str(len(data["vehicles"])),
            inline=True,
        )
        embed.add_field(
            name="Expansions",
            value=", ".join(data["expansions"]) if data["expansions"] else "None",
            inline=False,
        )
        await ctx.send(embed=embed)

    # ------------------------------
    # TRAINING MODULE
    # ------------------------------

    @fsc_group.group(name="training")
    async def fsc_training(self, ctx: commands.Context):
        """Training commands for Fire Station Command."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @fsc_training.command(name="list")
    async def training_list(self, ctx: commands.Context):
        """List available trainings from config."""
        if not self.content.trainings:
            await ctx.send("No trainings configured.")
            return

        lines = []
        for t in self.content.trainings.values():
            lines.append(
                f"- `{t.id}`: **{t.name}** – {t.duration_hours}h, cost {t.cost}"
            )
        await ctx.send("\n".join(lines))

    @fsc_training.command(name="status")
    async def training_status(self, ctx: commands.Context):
        """Show your running and completed trainings."""
        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        newly_completed = await self._process_training_jobs(user_conf, data)

        if newly_completed:
            names = ", ".join(t.name for t in newly_completed)
            await ctx.send(f"The following trainings have just completed: {names}")

        data = await user_conf.all()  # refresh
        jobs = data.get("training_jobs", [])
        completed_ids = set(data.get("completed_trainings", []))

        embed = discord.Embed(
            title="Training Status",
            color=discord.Color.green(),
        )

        running_lines = []
        now = int(dt.datetime.utcnow().timestamp())
        for job in jobs:
            t_id = job.get("training_id")
            t_def = self.content.trainings.get(t_id)
            name = t_def.name if t_def else t_id
            if not job.get("completed"):
                start_ts = job.get("start_ts", 0)
                duration = job.get("duration", 0)
                remaining = max(0, (start_ts + duration) - now)
                remaining_min = remaining // 60
                running_lines.append(
                    f"- {name} (`{t_id}`): ~{remaining_min} minutes remaining"
                )

        if running_lines:
            embed.add_field(
                name="Running trainings",
                value="\n".join(running_lines),
                inline=False,
            )
        else:
            embed.add_field(
                name="Running trainings",
                value="None",
                inline=False,
            )

        if completed_ids:
            comp_names = []
            for t_id in completed_ids:
                t_def = self.content.trainings.get(t_id)
                comp_names.append(t_def.name if t_def else t_id)
            embed.add_field(
                name="Completed trainings",
                value=", ".join(comp_names),
                inline=False,
            )
        else:
            embed.add_field(
                name="Completed trainings",
                value="None",
                inline=False,
            )

        await ctx.send(embed=embed)

    @fsc_training.command(name="start")
    async def training_start(self, ctx: commands.Context, training_id: str):
        """Start a training (abstract: trains part of your crew)."""
        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()

        if not data["started"]:
            await ctx.send("You have not started yet. Use `[p]fsc start` first.")
            return

        t_def = self.content.trainings.get(training_id)
        if not t_def:
            await ctx.send(f"Training `{training_id}` does not exist.")
            return

        jobs = data.get("training_jobs", [])
        for job in jobs:
            if not job.get("completed") and job.get("training_id") == training_id:
                await ctx.send("You already have this training running.")
                return

        multiplier = float(self.get_balance("training_time_multiplier", 1.0))
        duration_seconds = int(t_def.duration_hours * 3600 * multiplier)
        now = int(dt.datetime.utcnow().timestamp())

        new_id = 1
        if jobs:
            new_id = max(j.get("id", 0) for j in jobs) + 1

        job = {
            "id": new_id,
            "training_id": training_id,
            "start_ts": now,
            "duration": duration_seconds,
            "completed": False,
        }
        jobs.append(job)
        await user_conf.training_jobs.set(jobs)

        finish_time = dt.datetime.utcfromtimestamp(now + duration_seconds)
        finish_str = finish_time.strftime("%Y-%m-%d %H:%M UTC")

        await ctx.send(
            f"Started training **{t_def.name}** (`{training_id}`).\n"
            f"Estimated completion: `{finish_str}`."
        )

    # ------------------------------
    # Missions with volunteer simulation
    # ------------------------------

    @fsc_group.command(name="mission")
    async def fsc_mission(self, ctx: commands.Context):
        """
        Request a mission and resolve it, including volunteer turnout simulation.
        """
        user_conf = self.config.user(ctx.author)
        data = await user_conf.all()
        await self._process_training_jobs(user_conf, data)

        if not data["started"]:
            await ctx.send("You have not started yet. Use `[p]fsc start` first.")
            return
        if not data["is_active"]:
            await ctx.send("You are currently off duty. Use `[p]fsc on` to go on duty.")
            return

        station_tier = self.calc_station_tier(data["xp"])

        candidates = [
            m for m in self.content.missions.values()
            if m.min_tier <= station_tier
        ]
        if not candidates:
            await ctx.send("No missions are configured for your current tier.")
            return

        mission = random.choice(candidates)

        owned_defs = {v["def_id"] for v in data["vehicles"]}
        missing_vehicles = [vid for vid in mission.required_vehicles if vid not in owned_defs]

        required_staff = 0
        for vid in mission.required_vehicles:
            vdef = self.content.vehicles.get(vid)
            if vdef:
                required_staff += vdef.required_staff

        if required_staff <= 0:
            required_staff = 4

        vol_result = self.simulate_volunteers(required_staff)
        arrived = vol_result["total_arrived"]
        used_realert = vol_result["used_realert"]
        turnout_seconds = vol_result["turnout_seconds"]

        staffing_ratio = arrived / required_staff if required_staff > 0 else 1.0
        understaffed = arrived < required_staff

        base_chance = 0.6
        xp_factor = min(0.2, data["xp"] / 5000.0)
        tier_factor = (station_tier - mission.min_tier) * 0.05
        missing_vehicle_penalty = -0.25 if missing_vehicles else 0.0

        if understaffed:
            staffing_penalty = -0.3 * (1.0 - staffing_ratio)
        else:
            staffing_penalty = 0.05

        success_chance = base_chance + xp_factor + tier_factor + missing_vehicle_penalty + staffing_penalty
        success_chance = max(0.05, min(0.95, success_chance))

        roll = random.random()
        success = roll < success_chance

        base_xp = mission.base_xp
        xp_gain = base_xp if success else max(5, base_xp // 4)
        credits_gain = int(mission.base_credits * (1.0 if success else 0.25))

        data = await user_conf.all()  # refresh after training job processing
        new_xp = data["xp"] + xp_gain
        rep_change = 2 if success else -3
        new_rep = max(0, min(100, data["reputation"] + rep_change))

        await user_conf.xp.set(new_xp)
        await user_conf.reputation.set(new_rep)
        await user_conf.credits_earned.set(data["credits_earned"] + credits_gain)

        title = f"Mission: {mission.name}"
        description = mission.description or "A new incident has been dispatched."
        embed = discord.Embed(title=title, description=description, color=discord.Color.blurple())
        embed.add_field(
            name="Required vehicles",
            value=", ".join(mission.required_vehicles) or "None",
            inline=False,
        )
        if missing_vehicles:
            embed.add_field(
                name="Missing vehicles",
                value=", ".join(missing_vehicles),
                inline=False,
            )

        embed.add_field(name="Required crew", value=str(required_staff), inline=True)
        embed.add_field(name="Crew arrived", value=str(arrived), inline=True)
        extra_info = []
        extra_info.append(f"First alert: {vol_result['first_arrived']} arrived.")
        if used_realert:
            extra_info.append(f"Re-alert: {vol_result['second_arrived']} extra arrived.")
        extra_info.append(f"Simulated turnout time: {turnout_seconds} seconds.")
        if understaffed:
            extra_info.append("Departed **understaffed** – heavy risk.")
        else:
            extra_info.append("Departed with **full crew**.")
        embed.add_field(
            name="Turnout",
            value="\n".join(extra_info),
            inline=False,
        )

        embed.add_field(name="Success chance", value=f"{int(success_chance * 100)}%", inline=True)
        embed.add_field(name="Roll", value=f"{roll:.2f}", inline=True)
        if success:
            outcome = "✅ Mission **successful**!"
        else:
            outcome = "❌ Mission **failed**."
        embed.add_field(name="Outcome", value=outcome, inline=False)
        embed.add_field(name="XP gained", value=str(xp_gain), inline=True)
        embed.add_field(
            name="Credits gained (internal stat)",
            value=str(credits_gain),
            inline=True,
        )
        embed.set_footer(text="Volunteer turnout, trainings and difficulty are simplified for this module.")

        await ctx.send(embed=embed)

    # ------------------------------
    # Admin / owner tools
    # ------------------------------

    @fsc_group.group(name="admin")
    @checks.is_owner()
    async def fsc_admin(self, ctx: commands.Context):
        """Admin tools for Fire Station Command."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @fsc_admin.command(name="reloadconfig")
    async def admin_reload_config(self, ctx: commands.Context):
        """Reload YAML config files."""
        base_path = Path(__file__).parent / "data" / "config"
        self.content = GameContent.from_files(base_path)
        await ctx.send("Config reloaded from YAML files.")

    @fsc_admin.command(name="listvehicles")
    async def admin_list_vehicles(self, ctx: commands.Context):
        """List all configured vehicle definitions."""
        if not self.content.vehicles:
            await ctx.send("No vehicles configured.")
            return
        lines = [
            f"- `{v.id}`: {v.name} (tier ≥ {v.tier_min})"
            for v in self.content.vehicles.values()
        ]
        await ctx.send("\n".join(lines))

    @fsc_admin.command(name="listequipment")
    async def admin_list_equipment(self, ctx: commands.Context):
        """List all configured equipment definitions."""
        if not self.content.equipment:
            await ctx.send("No equipment configured.")
            return
        lines = [
            f"- `{e.id}`: {e.name} ({e.category})"
            for e in self.content.equipment.values()
        ]
        await ctx.send("\n".join(lines))

    @fsc_admin.command(name="listexpansions")
    async def admin_list_expansions(self, ctx: commands.Context):
        """List all configured expansions."""
        if not self.content.expansions:
            await ctx.send("No expansions configured.")
            return
        lines = [
            f"- `{ex.id}`: {ex.name}"
            for ex in self.content.expansions.values()
        ]
        await ctx.send("\n".join(lines))

    @fsc_admin.command(name="listtrainings")
    async def admin_list_trainings(self, ctx: commands.Context):
        """List all configured trainings."""
        if not self.content.trainings:
            await ctx.send("No trainings configured.")
            return
        lines = [
            f"- `{t.id}`: {t.name} ({t.duration_hours}h)"
            for t in self.content.trainings.values()
        ]
        await ctx.send("\n".join(lines))

    @fsc_admin.command(name="listmissions")
    async def admin_list_missions(self, ctx: commands.Context):
        """List all configured missions."""
        if not self.content.missions:
            await ctx.send("No missions configured.")
            return
        lines = [
            f"- `{m.id}`: {m.name} (min tier {m.min_tier})"
            for m in self.content.missions.values()
        ]
        await ctx.send("\n".join(lines))

    @fsc_admin.command(name="showbalance")
    async def admin_show_balance(self, ctx: commands.Context):
        """Show balance/difficulty configuration."""
        if not self.content.balance.data:
            await ctx.send("No balance config loaded.")
            return

        lines = [f"- **{k}**: {v}" for k, v in self.content.balance.data.items()]
        chunks: List[str] = []
        chunk = ""
        for line in lines:
            if len(chunk) + len(line) + 1 > 1900:
                chunks.append(chunk)
                chunk = ""
            chunk += line + "\n"
        if chunk:
            chunks.append(chunk)
        for c in chunks:
            await ctx.send(c)
