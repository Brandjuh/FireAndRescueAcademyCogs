"""Dashboard view for FireStationCommander."""

from __future__ import annotations

from typing import Any

import discord


class DashboardView(discord.ui.View):
    """Main dashboard buttons for a single player."""

    def __init__(self, cog: Any, owner_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only allow the dashboard owner to use the controls."""
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the station commander can use this dashboard.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(
        label="Station",
        style=discord.ButtonStyle.primary,
        custom_id="fsc:dashboard:station",
    )
    async def station(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.show_status_panel(interaction)

    @discord.ui.button(
        label="Voertuigen",
        style=discord.ButtonStyle.secondary,
        custom_id="fsc:dashboard:vehicles",
    )
    async def vehicles(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.show_vehicle_panel(interaction)

    @discord.ui.button(
        label="Personeel",
        style=discord.ButtonStyle.secondary,
        custom_id="fsc:dashboard:personnel",
    )
    async def personnel(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.show_personnel_panel(interaction)

    @discord.ui.button(
        label="Meldingen",
        style=discord.ButtonStyle.success,
        custom_id="fsc:dashboard:incidents",
    )
    async def incidents(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.start_incident_from_interaction(interaction)

    @discord.ui.button(
        label="Onderhoud",
        style=discord.ButtonStyle.danger,
        custom_id="fsc:dashboard:maintenance",
    )
    async def maintenance(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.show_maintenance_panel(interaction)
