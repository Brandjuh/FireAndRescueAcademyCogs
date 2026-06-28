"""Shop and maintenance views for FireStationCommander."""

from __future__ import annotations

from typing import Any

import discord


class MaintenanceView(discord.ui.View):
    """Maintenance action buttons for a single player."""

    def __init__(self, cog: Any, owner_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.owner_id = owner_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only allow the station owner to run maintenance."""
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "Only the station commander can run maintenance.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(
        label="Repair all",
        style=discord.ButtonStyle.success,
        custom_id="fsc:maintenance:repair_all",
    )
    async def repair_all(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.repair_all_from_interaction(interaction)

    @discord.ui.button(
        label="Refresh",
        style=discord.ButtonStyle.secondary,
        custom_id="fsc:maintenance:refresh",
    )
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.show_maintenance_panel(interaction)
