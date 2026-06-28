"""Shared Discord UI helpers for FireStationCommander views."""

from __future__ import annotations

import logging
from typing import Any

import discord

log = logging.getLogger("red.firestationcommander.views")


class FireStationCommanderViewMixin:
    """Shared error handling for interactive FireStationCommander views."""

    async def on_error(
        self,
        interaction: discord.Interaction,
        error: Exception,
        item: Any,
    ) -> None:
        """Log view callback failures and give the user a clean ephemeral message."""
        del item
        log.error(
            "FireStationCommander view callback failed",
            exc_info=(type(error), error, error.__traceback__),
        )
        await _send_ephemeral_error(
            interaction,
            "This FireStationCommander control hit an error. "
            "Open a fresh dashboard with `[p]fsc status` and try again.",
        )


async def _send_ephemeral_error(interaction: discord.Interaction, message: str) -> None:
    """Send an ephemeral error through response or followup depending on response state."""
    response = getattr(interaction, "response", None)
    if response is not None:
        is_done = getattr(response, "is_done", None)
        already_done = is_done() if callable(is_done) else False
        if not already_done:
            try:
                await response.send_message(message, ephemeral=True)
                return
            except Exception:
                log.debug("Could not send interaction response error", exc_info=True)

    followup = getattr(interaction, "followup", None)
    if followup is not None:
        try:
            await followup.send(message, ephemeral=True)
        except Exception:
            log.debug("Could not send interaction followup error", exc_info=True)
