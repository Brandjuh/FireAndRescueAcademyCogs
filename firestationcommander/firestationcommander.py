"""FireStationCommander Red Discord Bot cog."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import discord
from redbot.core import Config, checks, commands
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path

from .constants import (
    BASE_GARAGE_SLOTS,
    BASE_STORAGE_SLOTS,
    DATA_DIR,
    INCIDENT_STATUS_ACTIVE,
    STARTER_EQUIPMENT_KEYS,
)
from .database import FireStationCommanderDatabase
from .models import Incident, IncidentReport, Player
from .services.economy import EconomyService
from .services.incidents import IncidentService
from .services.maintenance import MaintenanceService
from .services.personnel import PersonnelService
from .services.training import TRAINING_KEY_ALIASES, TrainingService
from .services.vehicles import VehicleService
from .views.dashboard import DashboardView
from .views.incident_view import IncidentActionView, VehicleDispatchView
from .views.shop_view import MaintenanceView

log = logging.getLogger("red.firestationcommander")

LEGACY_INCIDENT_KEY_ALIASES = {
    "containerbrand": "dumpster_fire",
    "buitenbrand": "grass_fire",
    "keukenbrand": "kitchen_fire",
    "woningbrand": "house_fire",
    "verkeersongeval_beknelling": "vehicle_crash_entrapment",
    "outdoor_fire": "grass_fire",
    "residential_fire": "house_fire",
    "vehicle_extrication_crash": "vehicle_crash_entrapment",
}


class FireStationCommander(commands.Cog):
    """Playable SQLite-backed fire station management MVP."""

    __version__ = "0.1.0"

    def __init__(self, bot: Red, db_path: str | Path | None = None):
        self.bot = bot
        self.config = self._build_config()
        self.vehicle_templates = _load_catalog("vehicles.json", "vehicles")
        self.equipment_templates = _load_catalog("equipment.json", "equipment")
        self.training_templates = _load_catalog("trainings.json", "trainings")
        self.incident_templates = _load_catalog("incidents.json", "incidents")
        self.equipment_by_key = {item["key"]: item for item in self.equipment_templates}
        self.incidents_by_key = {item["key"]: item for item in self.incident_templates}
        self.incidents_by_key.update(
            {
                legacy_key: self.incidents_by_key[canonical_key]
                for legacy_key, canonical_key in LEGACY_INCIDENT_KEY_ALIASES.items()
            }
        )
        self.training_by_key = {item["key"]: item for item in self.training_templates}
        self.training_by_key.update(
            {
                legacy_key: self.training_by_key[canonical_key]
                for legacy_key, canonical_key in TRAINING_KEY_ALIASES.items()
            }
        )

        self.economy = EconomyService()
        self.personnel_service = PersonnelService()
        self.training_service = TrainingService()
        self.vehicle_service = VehicleService(self.vehicle_templates)
        self.maintenance_service = MaintenanceService()
        self.incident_service = IncidentService(
            self.incident_templates,
            self.vehicle_service,
            self.training_service,
            self.personnel_service,
        )

        data_path = Path(db_path) if db_path is not None else _safe_cog_data_path(self)
        self.db = FireStationCommanderDatabase(data_path / "firestationcommander.sqlite3")

    def _build_config(self) -> Config | None:
        try:
            config = Config.get_conf(self, identifier=0xF5C02026, force_registration=True)
        except AttributeError:
            return None
        config.register_guild(incident_channel_id=None)
        return config

    async def cog_load(self) -> None:
        """Initialize persistent storage."""
        await self.db.initialize()
        log.info("FireStationCommander loaded")

    async def cog_unload(self) -> None:
        """Close persistent storage."""
        await self.db.close()
        log.info("FireStationCommander unloaded")

    @commands.hybrid_group(name="fsc", invoke_without_command=True)
    async def fsc_group(self, ctx: commands.Context) -> None:
        """Open the FireStationCommander dashboard."""
        await self.fsc_status(ctx)

    @fsc_group.command(name="start")
    async def fsc_start(self, ctx: commands.Context) -> None:
        """Create your player, starter station, vehicle, staff, and equipment."""
        guild_id = await self._ctx_guild_id(ctx)
        if guild_id is None:
            return

        player, created = await self.db.get_or_create_player(guild_id, ctx.author.id)
        await self.db.create_station(
            player.id,
            "Station 1",
            BASE_GARAGE_SLOTS,
            BASE_STORAGE_SLOTS,
        )
        if created:
            await self._seed_starter_assets(player.id)

        embed = await self._build_status_embed(player.id)
        embed.add_field(
            name="Start",
            value="Station created." if created else "Station already exists.",
            inline=False,
        )
        view = DashboardView(self, ctx.author.id)
        message = await ctx.send(embed=embed, view=view)
        view.message = message

    @fsc_group.command(name="status")
    async def fsc_status(self, ctx: commands.Context) -> None:
        """Show your current station dashboard."""
        player = await self._player_for_context(ctx)
        if player is None:
            return
        embed = await self._build_status_embed(player.id)
        view = DashboardView(self, ctx.author.id)
        message = await ctx.send(embed=embed, view=view)
        view.message = message

    @fsc_group.command(name="vehicles")
    async def fsc_vehicles(self, ctx: commands.Context) -> None:
        """Show your station vehicles."""
        player = await self._player_for_context(ctx)
        if player is None:
            return
        await ctx.send(embed=await self._build_vehicle_embed(player.id))

    @fsc_group.command(name="personnel", aliases=["staff"])
    async def fsc_personnel(self, ctx: commands.Context) -> None:
        """Show your station personnel."""
        player = await self._player_for_context(ctx)
        if player is None:
            return
        await ctx.send(embed=await self._build_personnel_embed(player.id))

    @fsc_group.command(name="incident")
    async def fsc_incident(self, ctx: commands.Context) -> None:
        """Generate a new incident for your station."""
        player = await self._player_for_context(ctx)
        if player is None:
            return
        incident = await self._get_or_create_incident(ctx.guild.id, player)
        embed = self._build_incident_embed(incident)
        view = IncidentActionView(self, ctx.author.id, incident.id)
        message = await ctx.send(embed=embed, view=view)
        view.message = message

    @fsc_group.command(name="maintenance")
    async def fsc_maintenance(self, ctx: commands.Context) -> None:
        """Show maintenance needs and repair options."""
        player = await self._player_for_context(ctx)
        if player is None:
            return
        embed = await self._build_maintenance_embed(player.id)
        view = MaintenanceView(self, ctx.author.id)
        message = await ctx.send(embed=embed, view=view)
        view.message = message

    @fsc_group.command(name="report")
    async def fsc_report(self, ctx: commands.Context) -> None:
        """Show your latest incident report."""
        player = await self._player_for_context(ctx)
        if player is None:
            return
        report = await self.db.latest_report(player.id)
        if report is None:
            await ctx.send("No incident report has been stored yet.")
            return
        await ctx.send(embed=self._build_report_embed(report))

    @fsc_group.command(name="reset")
    @checks.admin_or_permissions(administrator=True)
    async def fsc_reset(self, ctx: commands.Context, confirmation: str | None = None) -> None:
        """Reset all FireStationCommander player progress in this server."""
        guild_id = await self._ctx_guild_id(ctx)
        if guild_id is None:
            return
        if confirmation != "CONFIRM":
            await ctx.send(
                "This permanently resets all FireStationCommander player progress in this server. "
                "Run `[p]fsc reset CONFIRM` to continue."
            )
            return

        deleted = await self.db.reset_guild_players(guild_id)
        await ctx.send(f"FireStationCommander reset complete. Removed {deleted} player profile(s).")

    async def show_status_panel(self, interaction: discord.Interaction) -> None:
        """Edit an interaction message to the status dashboard."""
        player = await self._player_for_interaction(interaction)
        if player is None:
            return
        view = DashboardView(self, interaction.user.id)
        view.message = getattr(interaction, "message", None)
        await interaction.response.edit_message(
            embed=await self._build_status_embed(player.id),
            view=view,
        )

    async def show_vehicle_panel(self, interaction: discord.Interaction) -> None:
        """Edit an interaction message to the vehicle panel."""
        player = await self._player_for_interaction(interaction)
        if player is None:
            return
        await interaction.response.edit_message(embed=await self._build_vehicle_embed(player.id), view=None)

    async def show_personnel_panel(self, interaction: discord.Interaction) -> None:
        """Edit an interaction message to the personnel panel."""
        player = await self._player_for_interaction(interaction)
        if player is None:
            return
        await interaction.response.edit_message(embed=await self._build_personnel_embed(player.id), view=None)

    async def start_incident_from_interaction(self, interaction: discord.Interaction) -> None:
        """Create or show an active incident from a dashboard button."""
        player = await self._player_for_interaction(interaction)
        if player is None or interaction.guild is None:
            return
        incident = await self._get_or_create_incident(interaction.guild.id, player)
        view = IncidentActionView(self, interaction.user.id, incident.id)
        view.message = getattr(interaction, "message", None)
        await interaction.response.edit_message(
            embed=self._build_incident_embed(incident),
            view=view,
        )

    async def show_incident_requirements(
        self,
        interaction: discord.Interaction,
        incident_id: int,
    ) -> None:
        """Show incident requirements as an ephemeral message."""
        incident = await self.db.get_incident(incident_id)
        if incident is None:
            await interaction.response.send_message("Incident not found.", ephemeral=True)
            return
        await interaction.response.send_message(
            embed=self._build_incident_requirements_embed(incident),
            ephemeral=True,
        )

    async def show_vehicle_dispatch(self, interaction: discord.Interaction, incident_id: int) -> None:
        """Show a select menu for dispatchable vehicles."""
        player = await self._player_for_interaction(interaction)
        if player is None:
            return
        vehicles = self.vehicle_service.available(await self.db.list_vehicles(player.id))
        if not vehicles:
            await interaction.response.send_message("No available vehicles to dispatch.", ephemeral=True)
            return
        view = VehicleDispatchView(self, interaction.user.id, incident_id, vehicles)
        view.message = getattr(interaction, "message", None)
        await interaction.response.edit_message(
            embed=_basic_embed(
                "Alarm vehicles",
                "Select the vehicles that should respond to this incident.",
                discord.Color.red(),
            ),
            view=view,
        )

    async def ignore_incident(self, interaction: discord.Interaction, incident_id: int) -> None:
        """Ignore an active incident."""
        await self.db.mark_incident_ignored(incident_id)
        await interaction.response.edit_message(
            embed=_basic_embed(
                "Incident ignored",
                "The incident was cleared from your active board without rewards.",
                discord.Color.orange(),
            ),
            view=None,
        )

    async def finish_incident_dispatch(
        self,
        interaction: discord.Interaction,
        incident_id: int,
        vehicle_ids: list[int],
    ) -> None:
        """Resolve an incident after the player selects response vehicles."""
        player = await self._player_for_interaction(interaction)
        if player is None:
            return
        incident = await self.db.get_incident(incident_id)
        if incident is None or incident.status != INCIDENT_STATUS_ACTIVE:
            await interaction.response.send_message("This incident is no longer active.", ephemeral=True)
            return

        owned_vehicles = await self.db.list_vehicles(player.id)
        selected_vehicles = [vehicle for vehicle in owned_vehicles if vehicle.id in vehicle_ids]
        if not selected_vehicles:
            await interaction.response.send_message("Select at least one owned vehicle.", ephemeral=True)
            return
        if len(selected_vehicles) != len(set(vehicle_ids)):
            await interaction.response.send_message("One or more selected vehicles are unavailable.", ephemeral=True)
            return
        if len(self.vehicle_service.available(selected_vehicles)) != len(selected_vehicles):
            await interaction.response.send_message(
                "One or more selected vehicles are no longer available.",
                ephemeral=True,
            )
            return

        personnel = self.personnel_service.available(await self.db.list_personnel(player.id))
        training_map = {
            member.id: await self.db.trainings_for_personnel(member.id)
            for member in personnel
        }
        equipment_rows = await self.db.list_equipment(player.id)
        template = self.incidents_by_key[incident.template_key]
        score = self.incident_service.calculate_score(
            template=template,
            selected_vehicles=selected_vehicles,
            available_personnel=personnel,
            equipment_rows=equipment_rows,
            equipment_templates=self.equipment_by_key,
            training_map=training_map,
        )
        rewards = self.economy.rewards(
            base_reward=incident.base_reward,
            base_xp=incident.base_xp,
            score=score.score,
            current_xp=player.xp,
        )

        for vehicle in selected_vehicles:
            risk = max(1, incident.risk_level)
            await self.db.update_vehicle_after_incident(
                vehicle.id,
                condition_delta=-(4 + risk * 3),
                fuel_delta=-(8 + risk * 4),
                mileage_delta=5 + risk * 3,
            )
        await self.db.update_personnel_after_incident(player.id, stress_delta=3 + incident.risk_level * 3)
        report = await self.db.complete_incident(
            incident.id,
            player.id,
            score.score,
            rewards.cash_reward,
            rewards.xp_reward,
            rewards.reputation_delta,
            rewards.safety_delta,
            score.summary,
        )
        await self.db.update_player_after_incident(
            player.id,
            rewards.cash_reward,
            rewards.xp_reward,
            rewards.reputation_delta,
            rewards.safety_delta,
            rewards.new_level,
        )
        embed = self._build_report_embed(report, breakdown=score.breakdown, leveled_up=rewards.leveled_up)
        await interaction.response.edit_message(embed=embed, view=None)

    async def show_maintenance_panel(self, interaction: discord.Interaction) -> None:
        """Edit an interaction message to the maintenance panel."""
        player = await self._player_for_interaction(interaction)
        if player is None:
            return
        view = MaintenanceView(self, interaction.user.id)
        view.message = getattr(interaction, "message", None)
        await interaction.response.edit_message(
            embed=await self._build_maintenance_embed(player.id),
            view=view,
        )

    async def repair_all_from_interaction(self, interaction: discord.Interaction) -> None:
        """Repair all worn vehicles and equipment if the player can pay."""
        player = await self._player_for_interaction(interaction)
        if player is None:
            return
        vehicles = await self.db.list_vehicles(player.id)
        equipment = await self.db.list_equipment(player.id)
        vehicle_costs = self.maintenance_service.vehicles_needing_work(vehicles)
        equipment_costs = [
            (int(row["id"]), self.maintenance_service.equipment_repair_cost(int(row["condition_score"])))
            for row in equipment
        ]
        total_cost = sum(cost for _, cost in vehicle_costs) + sum(cost for _, cost in equipment_costs)
        if total_cost <= 0:
            await interaction.response.send_message("No maintenance is needed.", ephemeral=True)
            return
        if not await self.db.spend_cash(player.id, total_cost):
            await interaction.response.send_message(
                f"Maintenance costs {total_cost:,} cash, but you only have {player.cash:,}.",
                ephemeral=True,
            )
            return
        for vehicle, _cost in vehicle_costs:
            await self.db.repair_vehicle(vehicle.id)
        for equipment_id, cost in equipment_costs:
            if cost > 0:
                await self.db.repair_equipment(equipment_id)
        embed = await self._build_maintenance_embed(player.id)
        embed.add_field(name="Maintenance complete", value=f"Spent {total_cost:,} cash.", inline=False)
        view = MaintenanceView(self, interaction.user.id)
        view.message = getattr(interaction, "message", None)
        await interaction.response.edit_message(embed=embed, view=view)

    async def _ctx_guild_id(self, ctx: commands.Context) -> int | None:
        if getattr(ctx, "guild", None) is None:
            await ctx.send("FireStationCommander can only be played inside a server.")
            return None
        return int(ctx.guild.id)

    async def _player_for_context(self, ctx: commands.Context) -> Player | None:
        guild_id = await self._ctx_guild_id(ctx)
        if guild_id is None:
            return None
        player = await self.db.get_player(guild_id, ctx.author.id)
        if player is None:
            await ctx.send("No station found. Use `[p]fsc start` first.")
            return None
        return player

    async def _player_for_interaction(self, interaction: discord.Interaction) -> Player | None:
        if interaction.guild is None:
            await interaction.response.send_message(
                "FireStationCommander can only be played inside a server.",
                ephemeral=True,
            )
            return None
        player = await self.db.get_player(interaction.guild.id, interaction.user.id)
        if player is None:
            await interaction.response.send_message("Use `[p]fsc start` first.", ephemeral=True)
            return None
        return player

    async def _seed_starter_assets(self, player_id: int) -> None:
        await self.db.add_vehicle(player_id, self.vehicle_service.starter_vehicle())
        for payload in self.personnel_service.starter_personnel():
            trainings = [str(training) for training in payload.pop("trainings", [])]
            personnel_id = await self.db.add_personnel(player_id, payload)
            for training_key in trainings:
                await self.db.add_personnel_training(personnel_id, training_key)
        for equipment_key in STARTER_EQUIPMENT_KEYS:
            await self.db.add_equipment(player_id, equipment_key)

    async def _get_or_create_incident(self, guild_id: int, player: Player) -> Incident:
        active = await self.db.get_active_incident(player.id)
        if active is not None:
            return active
        template = self.incident_service.choose_template(player)
        return await self.db.create_incident(
            guild_id,
            player.id,
            template,
            expires_at=self.incident_service.expires_at(template),
        )

    async def _build_status_embed(self, player_id: int) -> discord.Embed:
        player = await self.db.get_player_by_id(player_id)
        station = await self.db.get_station(player_id)
        vehicles = await self.db.list_vehicles(player_id)
        personnel = await self.db.list_personnel(player_id)
        active = await self.db.get_active_incident(player_id)
        if player is None or station is None:
            return _basic_embed("FireStationCommander", "No station data found.", discord.Color.red())

        embed = discord.Embed(
            title="Fire Station Commander",
            description=f"{station.name} dashboard",
            color=discord.Color.red(),
        )
        embed.add_field(name="Cash", value=f"{player.cash:,}", inline=True)
        embed.add_field(name="Reputation", value=str(player.reputation), inline=True)
        embed.add_field(name="Command level", value=f"{player.command_level} ({player.xp} XP)", inline=True)
        embed.add_field(name="Safety", value=f"{player.safety_score}/100", inline=True)
        embed.add_field(name="Morale", value=f"{player.morale_score}/100", inline=True)
        embed.add_field(name="Station level", value=str(station.level), inline=True)
        embed.add_field(name="Vehicles", value=f"{len(vehicles)} / {station.garage_slots}", inline=True)
        embed.add_field(name="Personnel", value=str(len(personnel)), inline=True)
        embed.add_field(name="Active incidents", value="1" if active else "0", inline=True)
        return embed

    async def _build_vehicle_embed(self, player_id: int) -> discord.Embed:
        vehicles = await self.db.list_vehicles(player_id)
        embed = _basic_embed("Vehicles", "Station vehicle overview.", discord.Color.blue())
        if not vehicles:
            embed.add_field(name="Fleet", value="No vehicles.", inline=False)
            return embed
        for vehicle in vehicles[:10]:
            template = self.vehicle_service.template(vehicle.template_key)
            embed.add_field(
                name=f"{vehicle.callsign} - {template['name']}",
                value=(
                    f"Type: {template['type']} | Condition: {vehicle.condition_score}% | "
                    f"Reliability: {vehicle.reliability_score}% | Fuel: {vehicle.fuel}% | "
                    f"Status: {vehicle.status}"
                ),
                inline=False,
            )
        if len(vehicles) > 10:
            embed.add_field(name="More", value=f"+{len(vehicles) - 10} vehicles", inline=False)
        return embed

    async def _build_personnel_embed(self, player_id: int) -> discord.Embed:
        personnel = await self.db.list_personnel(player_id)
        embed = _basic_embed("Personnel", "Station personnel overview.", discord.Color.gold())
        if not personnel:
            embed.add_field(name="Personnel", value="No personnel.", inline=False)
            return embed
        for member in personnel[:10]:
            trainings = await self.db.trainings_for_personnel(member.id)
            embed.add_field(
                name=f"{member.name} - {member.rank}",
                value=(
                    f"Contract: {member.contract_type} | Condition: {member.condition_score}% | "
                    f"Stress: {member.stress_score}% | Morale: {member.morale_score}% | "
                    f"Training: {self._format_training_names(trainings)}"
                ),
                inline=False,
            )
        if len(personnel) > 10:
            embed.add_field(name="More", value=f"+{len(personnel) - 10} personnel", inline=False)
        return embed

    def _build_incident_embed(self, incident: Incident) -> discord.Embed:
        template = self.incidents_by_key[incident.template_key]
        vehicles = json.loads(incident.required_vehicle_types_json)
        trainings = json.loads(incident.required_trainings_json)
        tags = json.loads(incident.required_tags_json)
        embed = discord.Embed(
            title=f"Incident: {template.get('title', incident.title)}",
            description=template.get("description", "A new incident is waiting for command."),
            color=discord.Color.orange(),
        )
        embed.add_field(name="Risk", value=str(incident.risk_level), inline=True)
        embed.add_field(name="Reward", value=f"{incident.base_reward:,} cash", inline=True)
        embed.add_field(name="XP", value=str(incident.base_xp), inline=True)
        embed.add_field(
            name="Time limit",
            value=f"{int(template.get('time_limit_minutes', 30))} minutes",
            inline=True,
        )
        embed.add_field(
            name="Requirements",
            value=(
                f"Vehicles: {_format_list(vehicles)}\n"
                f"Training: {self._format_training_names(trainings)}\n"
                f"Capabilities: {_format_list(tags)}"
            ),
            inline=False,
        )
        embed.set_footer(text="Use the buttons below to manage the response.")
        return embed

    def _build_incident_requirements_embed(self, incident: Incident) -> discord.Embed:
        template = self.incidents_by_key[incident.template_key]
        embed = discord.Embed(
            title=f"Requirements: {template.get('title', incident.title)}",
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Vehicle types",
            value=", ".join(json.loads(incident.required_vehicle_types_json)) or "None",
            inline=False,
        )
        embed.add_field(
            name="Trainings",
            value=self._format_training_names(json.loads(incident.required_trainings_json)),
            inline=False,
        )
        embed.add_field(
            name="Tags",
            value=", ".join(json.loads(incident.required_tags_json)) or "None",
            inline=False,
        )
        return embed

    def _build_report_embed(
        self,
        report: IncidentReport,
        *,
        breakdown: dict[str, int] | None = None,
        leveled_up: bool = False,
    ) -> discord.Embed:
        embed = discord.Embed(
            title="Incident report",
            description=report.summary,
            color=discord.Color.green() if report.score >= 70 else discord.Color.orange(),
        )
        embed.add_field(name="Score", value=f"{report.score}/100", inline=True)
        embed.add_field(name="Cash", value=f"+{report.cash_reward:,}", inline=True)
        embed.add_field(name="XP", value=f"+{report.xp_reward}", inline=True)
        embed.add_field(name="Reputation", value=f"{report.reputation_delta:+}", inline=True)
        embed.add_field(name="Safety", value=f"{report.safety_delta:+}", inline=True)
        if leveled_up:
            embed.add_field(name="Level up", value="Command level increased.", inline=False)
        if breakdown:
            lines = [f"{key}: {value}" for key, value in breakdown.items()]
            embed.add_field(name="Score breakdown", value="\n".join(lines), inline=False)
        return embed

    async def _build_maintenance_embed(self, player_id: int) -> discord.Embed:
        vehicles = await self.db.list_vehicles(player_id)
        equipment = await self.db.list_equipment(player_id)
        vehicle_costs = self.maintenance_service.vehicles_needing_work(vehicles)
        equipment_costs = []
        for row in equipment:
            cost = self.maintenance_service.equipment_repair_cost(int(row["condition_score"]))
            if cost > 0:
                equipment_costs.append((row, cost))
        total = sum(cost for _, cost in vehicle_costs) + sum(cost for _, cost in equipment_costs)
        embed = _basic_embed("Maintenance", "Repair worn vehicles and equipment.", discord.Color.dark_gray())
        if not vehicle_costs and not equipment_costs:
            embed.add_field(name="Status", value="No maintenance needed.", inline=False)
        for vehicle, cost in vehicle_costs[:8]:
            embed.add_field(
                name=vehicle.callsign,
                value=f"Condition {vehicle.condition_score}% | Fuel {vehicle.fuel}% | Cost {cost:,}",
                inline=False,
            )
        for row, cost in equipment_costs[:8]:
            template_key = str(row["template_key"])
            template = self.equipment_by_key.get(template_key, {})
            name = template.get("name", template_key)
            embed.add_field(
                name=f"Equipment: {name}",
                value=f"Condition {int(row['condition_score'])}% | Cost {cost:,}",
                inline=False,
            )
        if len(equipment_costs) > 8:
            embed.add_field(name="More equipment", value=f"+{len(equipment_costs) - 8} items", inline=False)
        embed.add_field(name="Total repair cost", value=f"{total:,}", inline=True)
        return embed

    def _format_training_names(self, training_keys: list[str]) -> str:
        names = []
        for key in training_keys:
            canonical_key = self.training_service.normalize_key(str(key))
            template = self.training_by_key.get(canonical_key, self.training_by_key.get(str(key), {}))
            names.append(str(template.get("name", canonical_key)))
        return ", ".join(names) if names else "None"


def _load_catalog(file_name: str, key: str) -> list[dict[str, Any]]:
    path = DATA_DIR / file_name
    return json.loads(path.read_text(encoding="utf-8"))[key]


def _safe_cog_data_path(cog: commands.Cog) -> Path:
    path = cog_data_path(cog)
    if path is None:
        return Path(__file__).parent
    return Path(path)


def _basic_embed(title: str, description: str, color: Any) -> discord.Embed:
    return discord.Embed(title=title, description=description, color=color)


def _format_list(values: list[Any]) -> str:
    return ", ".join(str(value) for value in values) if values else "None"
