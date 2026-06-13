from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Optional
from uuid import uuid4

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import pagify


DEFAULT_GLOBAL = {
    "enabled": True,
    "idle_type": "watching",
    "idle_text": "Fire And Rescue Academy dispatch",
    "presence_max_length": 42,
}


ACTIVITY_TYPES = {
    "playing": "playing",
    "listening": "listening",
    "watching": "watching",
    "competing": "competing",
}


SOURCE_LABELS = {
    "AdminTimedNotifications": "Admin timer",
    "AllianceLogsPub": "Log pub",
    "AllianceReports": "Reports",
    "ApplicationsScraper": "Applications",
    "BuildingsScraper": "Buildings",
    "CookieManager": "MC session",
    "IncomeScraper": "Treasury",
    "LogsScraper": "Logs",
    "MemberSync": "Sync",
    "MembersScraper": "Members",
    "NewMemberNotify": "New apps",
    "RoleBasedCredits": "Ranks",
    "TrainingManager": "Training",
}


@dataclass
class StatusActivity:
    token: str
    source: str
    detail: str
    priority: int
    activity_type: str
    started_at: datetime
    updated_at: datetime
    expires_at: Optional[datetime] = None


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def clean_activity_type(value: str) -> str:
    cleaned = (value or "").strip().lower()
    if cleaned not in ACTIVITY_TYPES:
        raise ValueError("invalid activity type")
    return cleaned


def format_activity_text(source: str, detail: str, *, max_length: int = 128) -> str:
    source = (source or "Bot").strip()
    detail = (detail or "working").strip()
    text = f"{source}: {detail}" if detail else source
    if len(text) <= max_length:
        return text
    if max_length <= 3:
        return text[:max_length]
    return text[: max_length - 3].rstrip() + "..."


def compact_detail(source: str, detail: str) -> str:
    detail = (detail or "working").strip()
    lowered = detail.lower()

    if source == "MembersScraper":
        page = re.search(r"page (\d+)", lowered)
        if page:
            return f"page {page.group(1)}"
        if "backfilling" in lowered:
            return "backfill"
        return "scraping"

    if source == "BuildingsScraper":
        return "scraping"

    if source == "ApplicationsScraper":
        return "fetching" if "fetching" in lowered else "checking"

    if source == "IncomeScraper":
        page = re.search(r"expenses page (\d+)", lowered)
        if page:
            return f"expenses p{page.group(1)}"
        if "daily" in lowered:
            return "daily tab"
        if "monthly" in lowered:
            return "monthly tab"
        if "pre-reset" in lowered:
            return "pre-reset"
        return "scraping"

    if source == "LogsScraper":
        page = re.search(r"page (\d+)", lowered)
        if page:
            return f"page {page.group(1)}"
        pages = re.search(r"\((\d+) pages\)", lowered)
        if pages:
            return f"scraping {pages.group(1)}p"
        return "scraping"

    if source == "MemberSync":
        items = re.search(r"\((\d+) items\)", lowered)
        if "queue" in lowered and items:
            return f"queue ({items.group(1)})"
        if "departed" in lowered:
            return "checking departures"
        return "syncing"

    if source == "RoleBasedCredits":
        if "departed" in lowered:
            return "removing departed"
        if "checking" in lowered:
            return "dry run"
        return "syncing"

    if source == "AdminTimedNotifications":
        amount = re.search(r"posting (\d+)", lowered)
        return f"posting {amount.group(1)}" if amount else "posting"

    if source == "AllianceReports":
        if "daily member" in lowered:
            return "daily member"
        if "daily admin" in lowered:
            return "daily admin"
        if "monthly member" in lowered:
            return "monthly member"
        if "monthly admin" in lowered:
            return "monthly admin"
        if "daily" in lowered:
            return "daily"
        if "monthly" in lowered:
            return "monthly"
        return "building"

    if source == "AllianceLogsPub":
        amount = re.search(r"publishing (\d+)", lowered)
        return f"posting {amount.group(1)}" if amount else "checking"

    if source == "NewMemberNotify":
        return "checking"

    if source == "TrainingManager":
        amount = re.search(r"sending (\d+)", lowered)
        return f"reminders {amount.group(1)}" if amount else "reminders"

    if source == "CookieManager":
        return "refresh" if "refreshing" in lowered else "check"

    return (
        detail.replace("MissionChief", "MC")
        .replace("alliance ", "")
        .replace("credit rank roles", "rank roles")
        .strip()
    )


def compact_activity_text(source: str, detail: str, *, max_length: int = 42) -> str:
    label = SOURCE_LABELS.get((source or "").strip(), (source or "Bot").strip())
    return format_activity_text(label, compact_detail(source, detail), max_length=max_length)


def choose_activity(
    activities: list[StatusActivity],
    *,
    now: Optional[datetime] = None,
) -> Optional[StatusActivity]:
    current = now or utcnow()
    active = [
        activity
        for activity in activities
        if activity.expires_at is None or activity.expires_at > current
    ]
    if not active:
        return None
    return max(
        active,
        key=lambda activity: (
            activity.priority,
            activity.updated_at,
            activity.started_at,
            activity.token,
        ),
    )


class BotStatus(commands.Cog):
    """Expose the bot presence as a live background-task status."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFACA_B001, force_registration=True)
        self.config.register_global(**DEFAULT_GLOBAL)
        self._restore_task: Optional[asyncio.Task] = None
        self._activities: dict[str, StatusActivity] = {}
        self._report_tokens: dict[str, str] = {}
        self._presence_lock = asyncio.Lock()

    async def cog_load(self):
        self._restore_task = asyncio.create_task(self.restore_status())

    def cog_unload(self):
        if self._restore_task:
            self._restore_task.cancel()

    async def restore_status(self):
        wait_ready = getattr(self.bot, "wait_until_red_ready", None) or self.bot.wait_until_ready
        await wait_ready()
        await self.refresh_presence()

    async def set_presence(self, activity_type: str, text: str):
        cleaned_type = clean_activity_type(activity_type)
        activity_enum = getattr(discord.ActivityType, ACTIVITY_TYPES[cleaned_type])
        await self.bot.change_presence(
            activity=discord.Activity(type=activity_enum, name=text[:128])
        )

    def _purge_expired(self, *, now: Optional[datetime] = None) -> None:
        current = now or utcnow()
        expired = [
            token
            for token, activity in self._activities.items()
            if activity.expires_at is not None and activity.expires_at <= current
        ]
        for token in expired:
            self._activities.pop(token, None)
        if expired:
            self._report_tokens = {
                source: token
                for source, token in self._report_tokens.items()
                if token in self._activities
            }

    async def refresh_presence(self):
        async with self._presence_lock:
            self._purge_expired()
            if not await self.config.enabled():
                return

            active = choose_activity(list(self._activities.values()))
            if active:
                max_length = max(16, min(128, int(await self.config.presence_max_length())))
                text = compact_activity_text(active.source, active.detail, max_length=max_length)
                await self.set_presence(active.activity_type, text)
                return

            idle_text = await self.config.idle_text()
            if idle_text:
                await self.set_presence(await self.config.idle_type(), idle_text)

    async def start_activity(
        self,
        source: str,
        detail: str,
        *,
        priority: int = 50,
        activity_type: str = "watching",
        ttl_seconds: Optional[int] = None,
    ) -> str:
        now = utcnow()
        token = uuid4().hex
        self._activities[token] = StatusActivity(
            token=token,
            source=(source or "Bot").strip(),
            detail=(detail or "working").strip(),
            priority=int(priority),
            activity_type=clean_activity_type(activity_type),
            started_at=now,
            updated_at=now,
            expires_at=now + timedelta(seconds=ttl_seconds) if ttl_seconds else None,
        )
        await self.refresh_presence()
        return token

    async def update_activity(
        self,
        token: str,
        *,
        detail: Optional[str] = None,
        priority: Optional[int] = None,
        activity_type: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
    ) -> bool:
        activity = self._activities.get(token)
        if not activity:
            return False
        now = utcnow()
        if detail is not None:
            activity.detail = detail.strip()
        if priority is not None:
            activity.priority = int(priority)
        if activity_type is not None:
            activity.activity_type = clean_activity_type(activity_type)
        if ttl_seconds is not None:
            activity.expires_at = now + timedelta(seconds=ttl_seconds)
        activity.updated_at = now
        await self.refresh_presence()
        return True

    async def report_activity(
        self,
        source: str,
        detail: str,
        *,
        priority: int = 50,
        activity_type: str = "watching",
        ttl_seconds: int = 300,
    ) -> str:
        source = (source or "Bot").strip()
        token = self._report_tokens.get(source)
        if token and token in self._activities:
            await self.update_activity(
                token,
                detail=detail,
                priority=priority,
                activity_type=activity_type,
                ttl_seconds=ttl_seconds,
            )
            return token

        token = await self.start_activity(
            source,
            detail,
            priority=priority,
            activity_type=activity_type,
            ttl_seconds=ttl_seconds,
        )
        self._report_tokens[source] = token
        return token

    async def finish_activity(self, token: str):
        self._activities.pop(token, None)
        self._report_tokens = {
            source: report_token
            for source, report_token in self._report_tokens.items()
            if report_token != token
        }
        await self.refresh_presence()

    @asynccontextmanager
    async def track_activity(
        self,
        source: str,
        detail: str,
        *,
        priority: int = 50,
        activity_type: str = "watching",
        ttl_seconds: Optional[int] = None,
    ) -> AsyncIterator[str]:
        token = await self.start_activity(
            source,
            detail,
            priority=priority,
            activity_type=activity_type,
            ttl_seconds=ttl_seconds,
        )
        try:
            yield token
        finally:
            await self.finish_activity(token)

    @commands.group(name="botstatusset")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def botstatusset(self, ctx: commands.Context):
        """Configure live bot presence status."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @botstatusset.command(name="idle")
    async def botstatusset_idle(self, ctx: commands.Context, activity_type: str, *, text: str):
        """Set idle status. Type: playing, listening, watching, competing."""
        try:
            cleaned_type = clean_activity_type(activity_type)
        except ValueError:
            await ctx.send("Type must be: playing, listening, watching, competing.")
            return
        await self.config.idle_type.set(cleaned_type)
        await self.config.idle_text.set(text[:128])
        await self.config.enabled.set(True)
        await self.refresh_presence()
        await ctx.send("Bot idle status updated.")

    @botstatusset.command(name="enable")
    async def botstatusset_enable(self, ctx: commands.Context):
        """Enable BotStatus presence management."""
        await self.config.enabled.set(True)
        await self.refresh_presence()
        await ctx.send("BotStatus enabled.")

    @botstatusset.command(name="disable")
    async def botstatusset_disable(self, ctx: commands.Context):
        """Disable BotStatus presence management and clear activity."""
        await self.config.enabled.set(False)
        await self.bot.change_presence(activity=None)
        await ctx.send("BotStatus disabled.")

    @botstatusset.command(name="commandtracking")
    async def botstatusset_commandtracking(self, ctx: commands.Context, enabled: bool):
        """Legacy no-op; background cogs now report their own work."""
        del enabled
        await ctx.send(
            "Command tracking is no longer used. BotStatus now follows scraper, sync, "
            "report, and notification tasks directly."
        )

    @botstatusset.command(name="maxlength")
    async def botstatusset_maxlength(self, ctx: commands.Context, length: int):
        """Set compact presence text max length between 16 and 128."""
        if length < 16 or length > 128:
            await ctx.send("Length must be between 16 and 128.")
            return
        await self.config.presence_max_length.set(length)
        await self.refresh_presence()
        await ctx.send(f"BotStatus presence max length set to {length}.")

    @commands.command(name="botstatus")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def botstatus(self, ctx: commands.Context):
        """Show the current live bot status state."""
        cfg = await self.config.all()
        self._purge_expired()
        active = choose_activity(list(self._activities.values()))
        lines = [
            f"Enabled: {cfg['enabled']}",
            f"Idle: {cfg['idle_type']} {cfg['idle_text']}",
            f"Presence max length: {cfg['presence_max_length']}",
            "Current: "
            + (
                format_activity_text(active.source, active.detail)
                if active
                else "idle"
            ),
        ]

        if self._activities:
            lines.append("")
            lines.append("Active tasks:")
            for activity in sorted(
                self._activities.values(),
                key=lambda item: (-item.priority, item.source, item.detail),
            ):
                age = int((utcnow() - activity.started_at).total_seconds())
                expires = (
                    f", expires in {max(0, int((activity.expires_at - utcnow()).total_seconds()))}s"
                    if activity.expires_at
                    else ""
                )
                lines.append(
                    f"- {activity.source}: {activity.detail} "
                    f"(priority {activity.priority}, {age}s{expires})"
                )

        for page in pagify("\n".join(lines), page_length=1800):
            await ctx.send(page)
