"""Incident interaction views for FireStationCommander."""

from __future__ import annotations

from typing import Any

import discord

from ..models import Vehicle


class IncidentActionView(discord.ui.View):
    """Buttons shown on an active incident."""

    def __init__(self, cog: Any, owner_id: int, incident_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.owner_id = owner_id
        self.incident_id = incident_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only allow the incident owner to use the controls."""
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the incident owner can control this response.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(
        label="Alarmeer voertuigen",
        style=discord.ButtonStyle.success,
        custom_id="fsc:incident:dispatch",
    )
    async def dispatch(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.show_vehicle_dispatch(interaction, self.incident_id)

    @discord.ui.button(
        label="Bekijk vereisten",
        style=discord.ButtonStyle.secondary,
        custom_id="fsc:incident:requirements",
    )
    async def requirements(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.show_incident_requirements(interaction, self.incident_id)

    @discord.ui.button(
        label="Negeer",
        style=discord.ButtonStyle.danger,
        custom_id="fsc:incident:ignore",
    )
    async def ignore(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.ignore_incident(interaction, self.incident_id)


class VehicleDispatchSelect(discord.ui.Select):
    """Select menu used to choose responding vehicles."""

    def __init__(self, cog: Any, incident_id: int, vehicles: list[Vehicle]):
        self.cog = cog
        self.incident_id = incident_id
        options = [
            discord.SelectOption(
                label=f"{vehicle.callsign} ({vehicle.template_key.upper()})",
                value=str(vehicle.id),
                description=f"Condition {vehicle.condition_score}% | Fuel {vehicle.fuel}%",
            )
            for vehicle in vehicles[:25]
        ]
        if not options:
            options = [discord.SelectOption(label="No available vehicles", value="none")]
        super().__init__(
            placeholder="Select vehicles to alarm",
            min_values=1,
            max_values=max(1, len(options)),
            options=options,
            custom_id="fsc:incident:vehicle_select",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if self.values == ["none"]:
            await interaction.response.send_message("No vehicles are available.", ephemeral=True)
            return
        vehicle_ids = [int(value) for value in self.values if str(value).isdigit()]
        await self.cog.finish_incident_dispatch(interaction, self.incident_id, vehicle_ids)


class VehicleDispatchView(discord.ui.View):
    """Container view for the vehicle dispatch select menu."""

    def __init__(self, cog: Any, owner_id: int, incident_id: int, vehicles: list[Vehicle]):
        super().__init__(timeout=300)
        self.cog = cog
        self.owner_id = owner_id
        self.incident_id = incident_id
        self.add_item(VehicleDispatchSelect(cog, incident_id, vehicles))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only allow the incident owner to select vehicles."""
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the incident owner can dispatch vehicles.",
                ephemeral=True,
            )
            return False
        return True
