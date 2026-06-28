"""Shared Discord UI helpers for FireStationCommander views."""

from __future__ import annotations

import logging
from typing import Any

import discord

log = logging.getLogger("red.firestationcommander.views")


class FireStationCommanderViewMixin:
    """Shared error handling for interactive FireStationCommander views."""

    timeout_notice = "This menu timed out. Open `[p]fsc status` again to continue."

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.message = None

    def _disable_children(self) -> None:
        for child in self.children:
            child.disabled = True
            if isinstance(child, discord.ui.Button):
                child.style = discord.ButtonStyle.secondary

    async def _edit_timeout_message(
        self,
        *,
        content: str | None = None,
        view: discord.ui.View | None = None,
    ) -> None:
        if self.message is None:
            return
        self._disable_children()
        try:
            await self.message.edit(content=content or self.timeout_notice, view=view or self)
        except Exception:
            log.debug("Could not edit timed-out FireStationCommander view", exc_info=True)

    async def on_timeout(self) -> None:
        await self._edit_timeout_message()

    async def on_error(
        self,
        interaction: discord.Interaction,
        error: Exception,
        item: Any,
    ) -> None:
        """Log view callback failures and give the user a clean ephemeral message."""
        log.error(
            "FireStationCommander view callback failed on %r",
            item,
            exc_info=(type(error), error, error.__traceback__),
        )
        label = getattr(item, "label", None) or getattr(item, "placeholder", None)
        component_type = "menu" if isinstance(item, discord.ui.Select) else "button"
        action_text = f" `{label}`" if isinstance(label, str) and label else ""
        await _send_ephemeral_error(
            interaction,
            f"This FireStationCommander menu hit an error while handling the{action_text} "
            f"{component_type}. "
            "Open a fresh dashboard with `[p]fsc status` and try again.",
        )

    def stop(self) -> None:
        try:
            super().stop()
        except AttributeError:
            pass


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
