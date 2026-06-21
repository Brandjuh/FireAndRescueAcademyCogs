"""
MissionChief possible missions publisher.

This cog publishes possible missions from https://www.missionchief.com/einsaetze.json to a
Discord channel. It is intentionally conservative: normal syncs are limited test syncs, and a
full sync requires an explicit confirmation argument.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any

import discord
from redbot.core import commands
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path
from redbot.core.utils.chat_formatting import box, pagify

from .database import MissionsDatabase as MissionStore
from .mappings import get_tags_for_mission
from .mission_fetcher import MissionFetcher
from .mission_formatter import MissionFormatter

log = logging.getLogger("red.missionsdatabase")


DEFAULT_CHANNEL_ID = 1518038840152031262
SAFE_SYNC_LIMIT = 5
MAX_TEST_SYNC_LIMIT = 25
FULL_SYNC_CONFIRMATION = "CONFIRM"


class MissionsDatabase(commands.Cog):
    """Publish MissionChief possible missions without creating duplicate Discord posts."""

    POSTS_PER_BATCH = 5
    BATCH_DELAY_SECONDS = 10
    POST_DELAY_SECONDS = 1
    EXISTING_MESSAGE_SCAN_LIMIT = 500

    def __init__(self, bot: Red):
        self.bot = bot
        self.fetcher = MissionFetcher()
        self.formatter = MissionFormatter()
        self.last_sync_errors: list[str] = []
        self.sync_task: asyncio.Task | None = None
        self.stop_generation = 0

        data_path = self._data_path()
        self.db = MissionStore(data_path / "missions_v2.db")

    def _data_path(self) -> Path:
        path = cog_data_path(self)
        if path is None:
            return Path(__file__).parent
        return Path(path)

    async def cog_load(self) -> None:
        await self.db.initialize()
        self.sync_task = asyncio.create_task(self.auto_sync_loop())
        log.info("MissionsDatabase cog loaded")

    async def cog_unload(self) -> None:
        if self.sync_task:
            self.sync_task.cancel()
        await self.fetcher.close()
        log.info("MissionsDatabase cog unloaded")

    async def auto_sync_loop(self) -> None:
        """Run full syncs once per day only for guilds where auto sync is enabled."""
        await self.bot.wait_until_ready()

        while True:
            try:
                now = datetime.now()
                target = datetime.combine(now.date(), time(hour=3, minute=0))
                if now >= target:
                    target += timedelta(days=1)
                await asyncio.sleep((target - now).total_seconds())
                await self.run_auto_sync()
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("Unexpected error in MissionsDatabase auto sync loop")
                await asyncio.sleep(3600)

    async def run_auto_sync(self) -> None:
        for guild in self.bot.guilds:
            config = await self.db.get_config(guild.id)
            if not config or not config.get("auto_sync_enabled"):
                continue
            try:
                await self._sync_missions(guild, limit=None, query=None, force_update=False)
            except Exception:
                log.exception("Auto sync failed for guild %s", getattr(guild, "id", "unknown"))

    @commands.group(name="missions")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def missions(self, ctx: commands.Context) -> None:
        """Manage MissionChief possible mission publishing."""
        pass

    @missions.command(name="setup")
    async def missions_setup(self, ctx: commands.Context, channel: discord.TextChannel = None) -> None:
        """Configure the channel used for possible mission posts."""
        target_channel = channel or ctx.guild.get_channel(DEFAULT_CHANNEL_ID)
        if target_channel is None:
            await ctx.send(
                "Default missions channel was not found. Provide a channel: "
                f"`{ctx.prefix}missions setup #channel`"
            )
            return

        await self.db.set_config(ctx.guild.id, target_channel.id)
        await ctx.send(
            "Mission publishing configured.\n"
            f"Target channel: {target_channel.mention}\n"
            f"Test with `{ctx.prefix}missions sync 5` before running a full sync."
        )

    @missions.command(name="sync")
    async def missions_sync(
        self,
        ctx: commands.Context,
        limit: int = SAFE_SYNC_LIMIT,
        *,
        query: str | None = None,
    ) -> None:
        """
        Safely publish or update a limited number of missions.

        This is the default test flow. It never posts more than 25 missions in one run.
        """
        limit = max(1, min(int(limit), MAX_TEST_SYNC_LIMIT))
        message = await ctx.send(f"Starting safe mission sync for up to {limit} missions...")
        try:
            stats = await self._sync_missions(
                ctx.guild,
                limit=limit,
                query=query,
                force_update=False,
                progress_message=message,
            )
            await message.edit(content=self._format_sync_stats(stats, safe_mode=True))
        except Exception as exc:
            log.exception("Safe mission sync failed")
            await message.edit(content=f"Mission sync failed: {exc}")

    @missions.command(name="syncall")
    async def missions_syncall(self, ctx: commands.Context, confirmation: str = "") -> None:
        """
        Publish or update every MissionChief possible mission.

        Requires the exact confirmation argument CONFIRM.
        """
        if confirmation != FULL_SYNC_CONFIRMATION:
            await ctx.send(
                "Full sync is intentionally locked.\n"
                f"Run `{ctx.prefix}missions sync 5` first.\n"
                f"When ready, run `{ctx.prefix}missions syncall {FULL_SYNC_CONFIRMATION}`."
            )
            return

        message = await ctx.send("Starting full MissionChief possible missions sync...")
        try:
            stats = await self._sync_missions(
                ctx.guild,
                limit=None,
                query=None,
                force_update=False,
                progress_message=message,
            )
            await message.edit(content=self._format_sync_stats(stats, safe_mode=False))
        except Exception as exc:
            log.exception("Full mission sync failed")
            await message.edit(content=f"Full mission sync failed: {exc}")

    @missions.command(name="update")
    async def missions_update(self, ctx: commands.Context, *, search: str) -> None:
        """Publish or update one mission by ID, overlay key, or name search."""
        message = await ctx.send(f"Looking up mission `{search}`...")
        try:
            missions = await self.fetcher.fetch_missions()
            mission = self._find_single_mission(missions, search)
            if mission is None:
                await message.edit(content=f"No mission found for `{search}`.")
                return

            stats = await self._sync_missions(
                ctx.guild,
                limit=1,
                query=MissionFetcher.mission_key(mission),
                force_update=True,
                progress_message=message,
            )
            await message.edit(content=self._format_sync_stats(stats, safe_mode=True))
        except Exception as exc:
            log.exception("Mission update failed")
            await message.edit(content=f"Mission update failed: {exc}")

    @missions.command(name="view")
    async def missions_view(self, ctx: commands.Context, *, search: str) -> None:
        """Preview a mission embed without posting it to the configured channel."""
        message = await ctx.send(f"Looking up mission `{search}`...")
        try:
            missions = await self.fetcher.fetch_missions()
            mission = self._find_single_mission(missions, search)
            if mission is None:
                await message.edit(content=f"No mission found for `{search}`.")
                return

            await message.delete()
            content = self.formatter.build_content(mission)
            embed = self.formatter.build_embed(mission)
            await ctx.send(content=content, embed=embed)
        except Exception as exc:
            log.exception("Mission preview failed")
            await message.edit(content=f"Mission preview failed: {exc}")

    @missions.command(name="check")
    async def missions_check(self, ctx: commands.Context) -> None:
        """Show current configuration and publication statistics."""
        config = await self._get_config_or_default(ctx.guild)
        stats = await self.db.get_statistics(ctx.guild.id)
        channel = ctx.guild.get_channel(int(config["channel_id"]))

        lines = [
            "MissionChief Possible Missions",
            f"Channel: {channel.mention if channel else config['channel_id']}",
            f"Auto sync: {'enabled' if config.get('auto_sync_enabled') else 'disabled'}",
            f"Tracked posts: {stats['total']}",
            f"Text messages: {stats['messages']}",
            f"Forum threads: {stats['threads']}",
            f"Last sync: {config.get('last_sync_at') or 'Never'}",
        ]
        await ctx.send(box("\n".join(lines)))

    @missions.command(name="auto")
    async def missions_auto(self, ctx: commands.Context, state: str = "") -> None:
        """Enable or disable daily full auto-sync. Use on/off."""
        normalized = state.casefold().strip()
        if normalized not in {"on", "off"}:
            await ctx.send(f"Use `{ctx.prefix}missions auto on` or `{ctx.prefix}missions auto off`.")
            return

        await self._ensure_config(ctx.guild)
        enabled = normalized == "on"
        await self.db.set_auto_sync(ctx.guild.id, enabled)
        await ctx.send(f"Automatic full sync {'enabled' if enabled else 'disabled'}.")

    @missions.command(name="stop")
    async def missions_stop(self, ctx: commands.Context) -> None:
        """Stop the current mission posting run and disable automatic full sync."""
        self._request_stop()
        config = await self.db.get_config(ctx.guild.id)
        if config:
            await self.db.set_auto_sync(ctx.guild.id, False)

        await ctx.send(
            "Mission posting stop requested.\n"
            "Automatic full sync is disabled. Existing mission posts were not deleted."
        )

    @missions.command(name="wipe")
    async def missions_wipe(self, ctx: commands.Context, confirmation: str = "") -> None:
        """
        Delete mission posts from the configured channel.

        Requires the exact confirmation argument CONFIRM.
        """
        if confirmation != FULL_SYNC_CONFIRMATION:
            await ctx.send(
                "Mission post wipe is intentionally locked.\n"
                f"Run `{ctx.prefix}missions stop` first if a sync is active.\n"
                f"When ready, run `{ctx.prefix}missions wipe {FULL_SYNC_CONFIRMATION}`."
            )
            return

        message = await ctx.send("Deleting mission posts from the configured channel...")
        try:
            stats = await self._wipe_configured_posts(ctx.guild)
            await message.edit(content=self._format_wipe_stats(stats))
        except Exception as exc:
            log.exception("Mission post wipe failed")
            await message.edit(content=f"Mission post wipe failed: {exc}")

    @missions.command(name="errors")
    async def missions_errors(self, ctx: commands.Context) -> None:
        """Show errors from the last sync run."""
        if not self.last_sync_errors:
            await ctx.send("No errors from the last mission sync.")
            return

        report = "Errors from the last mission sync:\n" + "\n".join(
            f"- {error}" for error in self.last_sync_errors[:30]
        )
        for page in pagify(report):
            await ctx.send(box(page))

    def _request_stop(self) -> int:
        self.stop_generation += 1
        return self.stop_generation

    async def _sleep_unless_stopped(self, seconds: float, stop_generation: int) -> bool:
        if seconds <= 0:
            return self.stop_generation == stop_generation

        loop = asyncio.get_running_loop()
        deadline = loop.time() + seconds
        while True:
            if self.stop_generation != stop_generation:
                return False

            remaining = deadline - loop.time()
            if remaining <= 0:
                return True

            await asyncio.sleep(min(0.5, remaining))

    async def _sync_missions(
        self,
        guild: discord.Guild,
        *,
        limit: int | None,
        query: str | None,
        force_update: bool,
        progress_message: discord.Message | None = None,
    ) -> dict[str, int]:
        config = await self._get_config_or_default(guild)
        channel = guild.get_channel(int(config["channel_id"]))
        if channel is None:
            raise ValueError(f"Configured channel {config['channel_id']} was not found")

        all_missions = await self.fetcher.fetch_missions()
        selected = self._select_missions(all_missions, limit=limit, query=query)
        stop_generation = self.stop_generation
        stats = {
            "source_missions": len(all_missions),
            "selected_missions": len(selected),
            "created": 0,
            "updated": 0,
            "recovered": 0,
            "skipped": 0,
            "failed": 0,
            "stopped": 0,
        }
        errors: list[str] = []

        for index, mission in enumerate(selected, start=1):
            if self.stop_generation != stop_generation:
                stats["stopped"] = 1
                break

            mission_key = MissionFetcher.mission_key(mission)
            try:
                status = await self._publish_mission(guild, channel, mission, force_update=force_update)
                stats[status] += 1
                if not await self._sleep_unless_stopped(self.POST_DELAY_SECONDS, stop_generation):
                    stats["stopped"] = 1
                    break

                changed = stats["created"] + stats["updated"] + stats["recovered"]
                if changed and changed % self.POSTS_PER_BATCH == 0:
                    if not await self._sleep_unless_stopped(self.BATCH_DELAY_SECONDS, stop_generation):
                        stats["stopped"] = 1
                        break

                if progress_message and index % 10 == 0:
                    await progress_message.edit(
                        content=(
                            f"Syncing missions... {index}/{len(selected)}\n"
                            f"Created: {stats['created']} | Updated: {stats['updated']} | "
                            f"Skipped: {stats['skipped']} | Failed: {stats['failed']}"
                        )
                    )
            except Exception as exc:
                stats["failed"] += 1
                errors.append(f"{mission_key}: {exc}")
                log.exception("Failed to publish mission %s", mission_key)

        if not stats["stopped"]:
            await self.db.update_last_sync(guild.id)
        self.last_sync_errors = errors
        return stats

    async def _wipe_configured_posts(self, guild: discord.Guild) -> dict[str, int]:
        config = await self._get_config_or_default(guild)
        channel = guild.get_channel(int(config["channel_id"]))
        if channel is None:
            raise ValueError(f"Configured channel {config['channel_id']} was not found")

        if self._is_forum_channel(channel):
            stats = await self._wipe_forum_channel(guild, channel)
        else:
            stats = await self._wipe_text_channel(guild, channel)

        if stats["failed"] == 0:
            await self.db.clear_publications(guild.id)
        return stats

    async def _wipe_text_channel(self, guild: discord.Guild, channel: Any) -> dict[str, int]:
        stats = {"deleted": 0, "missing": 0, "failed": 0, "scanned": 0}
        deleted_message_ids: set[int] = set()

        records = await self.db.get_all_publications(guild.id)
        for record in records:
            if record.get("target_kind") != "message":
                continue
            if str(record.get("channel_id")) != str(channel.id):
                continue

            publication = await self._get_recorded_publication(channel, record)
            if publication is None:
                stats["missing"] += 1
                continue

            message = publication["message"]
            if await self._delete_discord_object(message, stats):
                deleted_message_ids.add(int(message.id))

        history = getattr(channel, "history", None)
        if history is None:
            return stats

        async for message in channel.history(limit=self.EXISTING_MESSAGE_SCAN_LIMIT):
            stats["scanned"] += 1
            if int(getattr(message, "id", 0)) in deleted_message_ids:
                continue
            if not self._message_has_mission_marker(message):
                continue

            if await self._delete_discord_object(message, stats):
                deleted_message_ids.add(int(message.id))

        return stats

    async def _wipe_forum_channel(self, guild: discord.Guild, channel: Any) -> dict[str, int]:
        stats = {"deleted": 0, "missing": 0, "failed": 0, "scanned": 0}
        deleted_thread_ids: set[int] = set()

        records = await self.db.get_all_publications(guild.id)
        for record in records:
            if record.get("target_kind") != "forum_thread":
                continue
            if str(record.get("channel_id")) != str(channel.id):
                continue

            thread_id = record.get("thread_id")
            if not thread_id:
                stats["missing"] += 1
                continue

            thread = await self._resolve_thread(channel, int(thread_id))
            if thread is None:
                stats["missing"] += 1
                continue

            if await self._delete_discord_object(thread, stats):
                deleted_thread_ids.add(int(thread.id))

        async for thread in self._iter_forum_threads(channel):
            stats["scanned"] += 1
            thread_id = int(getattr(thread, "id", 0))
            if thread_id in deleted_thread_ids:
                continue

            if await self._delete_discord_object(thread, stats):
                deleted_thread_ids.add(thread_id)

        return stats

    async def _iter_forum_threads(self, channel: Any):
        seen: set[int] = set()
        for thread in getattr(channel, "threads", []) or []:
            thread_id = int(getattr(thread, "id", 0))
            if thread_id in seen:
                continue
            seen.add(thread_id)
            yield thread

        archived_threads = getattr(channel, "archived_threads", None)
        if archived_threads is None:
            return

        try:
            archived_iterator = channel.archived_threads(limit=None)
        except TypeError:
            archived_iterator = channel.archived_threads(limit=100)

        try:
            async for thread in archived_iterator:
                thread_id = int(getattr(thread, "id", 0))
                if thread_id in seen:
                    continue
                seen.add(thread_id)
                yield thread
        except (discord.HTTPException, discord.Forbidden, AttributeError):
            return

    async def _delete_discord_object(self, target: Any, stats: dict[str, int]) -> bool:
        try:
            await target.delete()
        except discord.NotFound:
            stats["missing"] += 1
            return False
        except (discord.Forbidden, discord.HTTPException, AttributeError):
            stats["failed"] += 1
            return False

        stats["deleted"] += 1
        if self.POST_DELAY_SECONDS > 0:
            await asyncio.sleep(self.POST_DELAY_SECONDS)
        return True

    async def _publish_mission(
        self,
        guild: discord.Guild,
        channel: Any,
        mission: dict[str, Any],
        *,
        force_update: bool,
    ) -> str:
        mission_key = MissionFetcher.mission_key(mission)
        content_hash = MissionFetcher.calculate_hash(
            mission,
            format_version=self.formatter.FORMAT_VERSION,
        )
        content = self.formatter.build_content(mission)
        embed = self.formatter.build_embed(mission)
        title = self.formatter.thread_title(mission)
        detail_url = MissionFetcher.detail_url(mission)
        record = await self.db.get_publication(guild.id, mission_key)

        if record and record.get("content_hash") == content_hash and not force_update:
            existing = await self._get_recorded_publication(channel, record)
            if existing:
                await self.db.touch_publication(guild.id, mission_key)
                return "skipped"

        if record:
            updated = await self._try_update_recorded_publication(
                channel,
                record,
                mission,
                content,
                embed,
                title,
            )
            if updated:
                await self._save_publication(
                    guild,
                    mission_key=mission_key,
                    channel=channel,
                    target_kind=record["target_kind"],
                    message_id=updated.get("message_id"),
                    thread_id=updated.get("thread_id"),
                    content_hash=content_hash,
                    title=title,
                    detail_url=detail_url,
                )
                return "updated"

        recovered = await self._find_existing_publication(channel, mission_key)
        if recovered:
            await self._edit_publication(recovered, content, embed, title)
            await self._save_publication(
                guild,
                mission_key=mission_key,
                channel=channel,
                target_kind=recovered["target_kind"],
                message_id=recovered.get("message_id"),
                thread_id=recovered.get("thread_id"),
                content_hash=content_hash,
                title=title,
                detail_url=detail_url,
            )
            return "recovered"

        created = await self._create_publication(channel, mission, content, embed, title)
        await self._save_publication(
            guild,
            mission_key=mission_key,
            channel=channel,
            target_kind=created["target_kind"],
            message_id=created.get("message_id"),
            thread_id=created.get("thread_id"),
            content_hash=content_hash,
            title=title,
            detail_url=detail_url,
        )
        return "created"

    async def _try_update_recorded_publication(
        self,
        channel: Any,
        record: dict[str, Any],
        mission: dict[str, Any],
        content: str,
        embed: discord.Embed,
        title: str,
    ) -> dict[str, Any] | None:
        publication = await self._get_recorded_publication(channel, record)
        if publication is None:
            return None

        try:
            if publication["target_kind"] == "message":
                message = publication["message"]
                await message.edit(content=content, embed=embed)
                return {
                    "target_kind": "message",
                    "message_id": message.id,
                    "thread_id": None,
                }

            if publication["target_kind"] == "forum_thread":
                thread = publication["thread"]
                starter = publication["message"]
                await starter.edit(content=content, embed=embed)
                await thread.edit(name=title, applied_tags=self._forum_tags_for_mission(channel, mission))
                return {
                    "target_kind": "forum_thread",
                    "message_id": starter.id,
                    "thread_id": thread.id,
                }
        except (discord.NotFound, discord.Forbidden, discord.HTTPException, AttributeError, ValueError):
            return None
        return None

    async def _get_recorded_publication(
        self,
        channel: Any,
        record: dict[str, Any],
    ) -> dict[str, Any] | None:
        try:
            if record["target_kind"] == "message" and record.get("message_id"):
                message = await channel.fetch_message(int(record["message_id"]))
                return {
                    "target_kind": "message",
                    "message": message,
                    "message_id": message.id,
                    "thread_id": None,
                }

            if record["target_kind"] == "forum_thread" and record.get("thread_id"):
                thread = await self._resolve_thread(channel, int(record["thread_id"]))
                if thread is None:
                    return None
                message_id = record.get("message_id") or record.get("thread_id")
                starter = await thread.fetch_message(int(message_id))
                return {
                    "target_kind": "forum_thread",
                    "thread": thread,
                    "message": starter,
                    "message_id": starter.id,
                    "thread_id": thread.id,
                }
        except (discord.NotFound, discord.Forbidden, discord.HTTPException, AttributeError, ValueError):
            return None
        return None

    async def _create_publication(
        self,
        channel: Any,
        mission: dict[str, Any],
        content: str,
        embed: discord.Embed,
        title: str,
    ) -> dict[str, Any]:
        if self._is_forum_channel(channel):
            created = await channel.create_thread(
                name=title,
                content=content,
                embed=embed,
                applied_tags=self._forum_tags_for_mission(channel, mission),
            )
            thread = getattr(created, "thread", created)
            starter_message = getattr(created, "message", None)
            return {
                "target_kind": "forum_thread",
                "thread_id": thread.id,
                "message_id": getattr(starter_message, "id", thread.id),
            }

        kwargs = {"content": content, "embed": embed}
        if hasattr(discord, "AllowedMentions"):
            kwargs["allowed_mentions"] = discord.AllowedMentions.none()
        message = await channel.send(**kwargs)
        return {
            "target_kind": "message",
            "message_id": message.id,
            "thread_id": None,
        }

    async def _edit_publication(
        self,
        publication: dict[str, Any],
        content: str,
        embed: discord.Embed,
        title: str,
    ) -> None:
        if publication["target_kind"] == "forum_thread":
            thread = publication["thread"]
            message = publication["message"]
            await message.edit(content=content, embed=embed)
            await thread.edit(name=title)
            return

        await publication["message"].edit(content=content, embed=embed)

    async def _find_existing_publication(self, channel: Any, mission_key: str) -> dict[str, Any] | None:
        if self._is_forum_channel(channel):
            return await self._find_existing_forum_thread(channel, mission_key)
        return await self._find_existing_message(channel, mission_key)

    async def _find_existing_message(self, channel: Any, mission_key: str) -> dict[str, Any] | None:
        history = getattr(channel, "history", None)
        if history is None:
            return None

        async for message in channel.history(limit=self.EXISTING_MESSAGE_SCAN_LIMIT):
            if self._message_has_mission_key(message, mission_key):
                return {
                    "target_kind": "message",
                    "message": message,
                    "message_id": message.id,
                    "thread_id": None,
                }
        return None

    async def _find_existing_forum_thread(self, channel: Any, mission_key: str) -> dict[str, Any] | None:
        prefix = f"[{mission_key}]"
        threads = list(getattr(channel, "threads", []) or [])

        archived_threads = getattr(channel, "archived_threads", None)
        if archived_threads is not None:
            try:
                async for thread in channel.archived_threads(limit=100):
                    threads.append(thread)
            except (discord.HTTPException, discord.Forbidden, AttributeError):
                pass

        for thread in threads:
            if not getattr(thread, "name", "").startswith(prefix):
                continue
            try:
                message = await thread.fetch_message(thread.id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException, AttributeError):
                continue
            return {
                "target_kind": "forum_thread",
                "thread": thread,
                "message": message,
                "thread_id": thread.id,
                "message_id": message.id,
            }
        return None

    @staticmethod
    def _message_has_mission_key(message: Any, mission_key: str) -> bool:
        content = getattr(message, "content", "") or ""
        if f"`{mission_key}`" in content or f"Mission ID: {mission_key}" in content:
            return True

        for embed in getattr(message, "embeds", []) or []:
            footer = getattr(embed, "footer", None)
            footer_text = getattr(footer, "text", None)
            if footer_text is None and isinstance(footer, dict):
                footer_text = footer.get("text")
            if footer_text and f"Mission ID: {mission_key}" in footer_text:
                return True
        return False

    @staticmethod
    def _message_has_mission_marker(message: Any) -> bool:
        content = getattr(message, "content", "") or ""
        if MissionFormatter.MARKER_PREFIX in content:
            return True

        for embed in getattr(message, "embeds", []) or []:
            footer = getattr(embed, "footer", None)
            footer_text = getattr(footer, "text", None)
            if footer_text is None and isinstance(footer, dict):
                footer_text = footer.get("text")
            if footer_text and "Source: MissionChief Possible Missions" in footer_text:
                return True
        return False

    async def _resolve_thread(self, forum_channel: Any, thread_id: int) -> Any | None:
        get_thread = getattr(forum_channel, "get_thread", None)
        if get_thread is not None:
            thread = get_thread(thread_id)
            if thread is not None:
                return thread

        get_channel = getattr(self.bot, "get_channel", None)
        if get_channel is not None:
            thread = get_channel(thread_id)
            if thread is not None:
                return thread

        fetch_channel = getattr(self.bot, "fetch_channel", None)
        if fetch_channel is not None:
            try:
                return await fetch_channel(thread_id)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                return None
        return None

    async def _save_publication(
        self,
        guild: discord.Guild,
        *,
        mission_key: str,
        channel: Any,
        target_kind: str,
        message_id: int | None,
        thread_id: int | None,
        content_hash: str,
        title: str,
        detail_url: str,
    ) -> None:
        await self.db.upsert_publication(
            guild_id=guild.id,
            mission_key=mission_key,
            channel_id=channel.id,
            target_kind=target_kind,
            message_id=message_id,
            thread_id=thread_id,
            content_hash=content_hash,
            title=title,
            detail_url=detail_url,
        )

    async def _get_config_or_default(self, guild: discord.Guild) -> dict[str, Any]:
        config = await self.db.get_config(guild.id)
        if config:
            return config

        channel = guild.get_channel(DEFAULT_CHANNEL_ID)
        if channel is None:
            raise ValueError(
                f"MissionsDatabase is not configured and default channel {DEFAULT_CHANNEL_ID} "
                "was not found"
            )
        await self.db.set_config(guild.id, channel.id)
        config = await self.db.get_config(guild.id)
        assert config is not None
        return config

    async def _ensure_config(self, guild: discord.Guild) -> dict[str, Any]:
        return await self._get_config_or_default(guild)

    @staticmethod
    def _select_missions(
        missions: list[dict[str, Any]],
        *,
        limit: int | None,
        query: str | None,
    ) -> list[dict[str, Any]]:
        selected = [mission for mission in missions if MissionFetcher.matches_query(mission, query)]
        if limit is not None:
            return selected[:limit]
        return selected

    @staticmethod
    def _find_single_mission(
        missions: list[dict[str, Any]],
        search: str,
    ) -> dict[str, Any] | None:
        needle = search.casefold().strip()
        exact = [
            mission
            for mission in missions
            if MissionFetcher.mission_key(mission).casefold() == needle
            or MissionFetcher.mission_name(mission).casefold() == needle
        ]
        if exact:
            return exact[0]

        matches = [mission for mission in missions if MissionFetcher.matches_query(mission, search)]
        return matches[0] if matches else None

    @staticmethod
    def _is_forum_channel(channel: Any) -> bool:
        return hasattr(channel, "create_thread") and hasattr(channel, "available_tags")

    @staticmethod
    def _forum_tags_for_mission(channel: Any, mission: dict[str, Any]) -> list[Any]:
        tag_names = get_tags_for_mission(mission.get("mission_categories", []) or [])
        available_tags = getattr(channel, "available_tags", []) or []
        return [tag for tag in available_tags if getattr(tag, "name", None) in tag_names][:5]

    @staticmethod
    def _format_sync_stats(stats: dict[str, int], *, safe_mode: bool) -> str:
        mode = "safe test sync" if safe_mode else "full sync"
        lines = [
            f"Mission {mode} complete.\n"
            f"Source missions: {stats['source_missions']}",
            f"Selected missions: {stats['selected_missions']}",
            f"Created: {stats['created']}",
            f"Updated: {stats['updated']}",
            f"Recovered existing posts: {stats['recovered']}",
            f"Skipped unchanged: {stats['skipped']}",
            f"Failed: {stats['failed']}",
        ]
        if stats.get("stopped"):
            lines.append("Stopped early: Yes")
        return "\n".join(lines)

    @staticmethod
    def _format_wipe_stats(stats: dict[str, int]) -> str:
        lines = [
            "Mission post wipe complete.",
            f"Deleted: {stats['deleted']}",
            f"Missing already: {stats['missing']}",
            f"Scanned existing posts: {stats['scanned']}",
            f"Failed: {stats['failed']}",
        ]
        if stats["failed"]:
            lines.append("Publication tracking was kept because one or more deletes failed.")
        else:
            lines.append("Publication tracking was cleared.")
        return "\n".join(lines)
