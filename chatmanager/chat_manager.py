from __future__ import annotations

import asyncio
import logging
import re
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import discord
from redbot.core import Config, commands

log = logging.getLogger("red.fara.chatmanager")

BASE_URL = "https://www.missionchief.com"
MAIN_URL = f"{BASE_URL}/"
ALLIANCE_CHATS_URL = f"{BASE_URL}/alliance_chats"
DEFAULT_CHANNEL_ID = 1518029674570055720
DEFAULT_POLL_INTERVAL_SECONDS = 30
MIN_MC_POST_INTERVAL_SECONDS = 30
OUTGOING_ECHO_TTL_SECONDS = 30 * 60
MAX_MC_CHAT_LENGTH = 1000
MAX_EMBED_MESSAGE_LENGTH = 1024


@dataclass(frozen=True)
class ChatForm:
    action: str
    method: str
    hidden_fields: dict[str, str]
    message_field: str


@dataclass(frozen=True)
class ChatMessage:
    chat_id: int
    username: str
    user_id: str
    message: str
    timestamp: str


def parse_chat_form(html: str, page_url: str = MAIN_URL) -> ChatForm:
    soup = BeautifulSoup(html or "", "html.parser")
    form = soup.find("form", id="new_alliance_chat")
    if not form:
        raise ValueError("MissionChief alliance chat form not found.")

    message_input = form.find(attrs={"name": "alliance_chat[message]"})
    if not message_input:
        raise ValueError("MissionChief alliance chat message field not found.")

    hidden_fields = {}
    for field in form.find_all("input"):
        name = field.get("name")
        field_type = str(field.get("type") or "").lower()
        if name and field_type == "hidden":
            hidden_fields[str(name)] = str(field.get("value") or "")

    return ChatForm(
        action=urljoin(page_url, str(form.get("action") or "/alliance_chats")),
        method=str(form.get("method") or "post").lower(),
        hidden_fields=hidden_fields,
        message_field=str(message_input.get("name")),
    )


def build_chat_payload(form: ChatForm, message: str) -> dict[str, str]:
    text = normalize_mc_message(message)
    if not text:
        raise ValueError("Chat message cannot be empty.")
    payload = dict(form.hidden_fields)
    payload[form.message_field] = text
    return payload


def parse_chat_history(html: str) -> list[ChatMessage]:
    soup = BeautifulSoup(html or "", "html.parser")
    messages = []
    for node in soup.find_all(id=re.compile(r"^chat_message_\d+$")):
        raw_id = str(node.get("id") or "").rsplit("_", 1)[-1]
        try:
            chat_id = int(raw_id)
        except ValueError:
            continue

        username_node = node.select_one("strong a") or node.find("strong")
        username = username_node.get_text(" ", strip=True) if username_node else "Unknown"
        href = username_node.get("href") if username_node and username_node.name == "a" else ""
        user_match = re.search(r"/profile/(\d+)", str(href or ""))
        user_id = user_match.group(1) if user_match else ""

        content = node.select_one(".message-content")
        message = content.get_text("\n", strip=True) if content else ""
        if not message:
            continue

        messages.append(
            ChatMessage(
                chat_id=chat_id,
                username=username,
                user_id=user_id,
                message=message,
                timestamp=str(node.get("data-message-time") or "").strip(),
            )
        )

    return sorted(messages, key=lambda item: item.chat_id)


def discord_timestamp(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Unknown"
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw
    unix_ts = int(parsed.timestamp())
    return f"<t:{unix_ts}:F> (<t:{unix_ts}:R>)"


def normalize_mc_message(message: str) -> str:
    text = re.sub(r"\s+", " ", str(message or "")).strip()
    if len(text) > MAX_MC_CHAT_LENGTH:
        return text[: MAX_MC_CHAT_LENGTH - 3].rstrip() + "..."
    return text


def format_discord_message_for_mc(username: str, message: str) -> str:
    return normalize_mc_message(f"[{username}] {message}")


def truncate_embed_value(value: str, limit: int = MAX_EMBED_MESSAGE_LENGTH) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text or "No message content."
    return text[: limit - 3].rstrip() + "..."


class ChatManager(commands.Cog):
    """Synchronize MissionChief alliance chat with a Discord channel."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFA20260621, force_registration=True)
        self.config.register_global(
            channel_id=DEFAULT_CHANNEL_ID,
            enabled=True,
            poll_interval_seconds=DEFAULT_POLL_INTERVAL_SECONDS,
            last_seen_chat_id=0,
            outgoing_echoes=[],
        )
        self._sync_task: Optional[asyncio.Task] = None
        self._post_lock = asyncio.Lock()
        self._last_mc_post_at = 0.0

    async def cog_load(self):
        self._sync_task = asyncio.create_task(self._sync_loop())

    async def cog_unload(self):
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass

    def _cookie_manager(self):
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager or not hasattr(cookie_manager, "get_session"):
            return None
        return cookie_manager

    async def _get_session(self):
        cookie_manager = self._cookie_manager()
        if not cookie_manager:
            raise RuntimeError("CookieManager is not loaded.")
        session = await cookie_manager.get_session()
        if not session:
            raise RuntimeError("CookieManager did not return a session.")
        return session

    async def _get_channel(self):
        channel_id = int(await self.config.channel_id() or 0)
        return self.bot.get_channel(channel_id) if channel_id else None

    async def _fetch_chat_form(self) -> ChatForm:
        session = await self._get_session()
        async with session.get(MAIN_URL, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            raise RuntimeError(f"MissionChief main page returned HTTP {status}.")
        return parse_chat_form(html, MAIN_URL)

    async def _fetch_chat_history(self) -> list[ChatMessage]:
        session = await self._get_session()
        async with session.get(ALLIANCE_CHATS_URL, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            raise RuntimeError(f"MissionChief alliance chat history returned HTTP {status}.")
        return parse_chat_history(html)

    async def _send_to_missionchief(self, message: str) -> None:
        async with self._post_lock:
            elapsed = time.monotonic() - self._last_mc_post_at
            if elapsed < MIN_MC_POST_INTERVAL_SECONDS:
                await asyncio.sleep(MIN_MC_POST_INTERVAL_SECONDS - elapsed)

            form = await self._fetch_chat_form()
            if form.method != "post":
                raise RuntimeError(f"Unexpected MissionChief chat form method `{form.method}`.")
            payload = build_chat_payload(form, message)
            session = await self._get_session()
            headers = {
                "Origin": BASE_URL,
                "Referer": MAIN_URL,
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "text/javascript, application/javascript, */*; q=0.01",
            }
            async with session.post(
                form.action,
                data=payload,
                allow_redirects=True,
                headers=headers,
            ) as response:
                status = getattr(response, "status", None)
                response_text = await response.text()
            if status is not None and int(status) >= 400:
                summary = re.sub(r"\s+", " ", response_text or "").strip()[:250]
                raise RuntimeError(f"MissionChief chat post returned HTTP {status}: {summary}")
            self._last_mc_post_at = time.monotonic()

    async def _remember_outgoing_echo(self, text: str) -> None:
        now = int(time.time())
        async with self.config.outgoing_echoes() as echoes:
            echoes[:] = [
                item
                for item in echoes
                if now - int(item.get("created_at") or 0) <= OUTGOING_ECHO_TTL_SECONDS
            ]
            echoes.append({"message": text, "created_at": now})

    async def _consume_outgoing_echo(self, text: str) -> bool:
        now = int(time.time())
        consumed = False
        async with self.config.outgoing_echoes() as echoes:
            kept = []
            for item in echoes:
                is_expired = now - int(item.get("created_at") or 0) > OUTGOING_ECHO_TTL_SECONDS
                if not consumed and not is_expired and str(item.get("message") or "") == text:
                    consumed = True
                    continue
                if not is_expired:
                    kept.append(item)
            echoes[:] = kept
        return consumed

    def _build_chat_embed(self, chat: ChatMessage) -> discord.Embed:
        embed = discord.Embed(
            title="MissionChief Alliance Chat",
            color=discord.Color.blue(),
        )
        embed.add_field(name="Name", value=chat.username, inline=True)
        embed.add_field(name="Time", value=discord_timestamp(chat.timestamp), inline=False)
        embed.add_field(name="Message", value=truncate_embed_value(chat.message), inline=False)
        embed.set_footer(text=f"MissionChief chat ID: {chat.chat_id}")
        return embed

    async def _post_game_chat_to_discord(self, chat: ChatMessage) -> bool:
        channel = await self._get_channel()
        if not channel:
            log.warning("ChatManager Discord channel is not configured or not found.")
            return False
        await channel.send(
            embed=self._build_chat_embed(chat),
            allowed_mentions=discord.AllowedMentions.none(),
        )
        return True

    async def _sync_once(self) -> dict[str, int]:
        messages = await self._fetch_chat_history()
        if not messages:
            return {"seen": 0, "posted": 0, "skipped_echoes": 0}

        last_seen = int(await self.config.last_seen_chat_id() or 0)
        newest_id = max(item.chat_id for item in messages)
        if last_seen <= 0:
            await self.config.last_seen_chat_id.set(newest_id)
            return {"seen": len(messages), "posted": 0, "skipped_echoes": 0}

        new_messages = [item for item in messages if item.chat_id > last_seen]
        posted = 0
        skipped_echoes = 0
        for chat in new_messages:
            if await self._consume_outgoing_echo(chat.message):
                skipped_echoes += 1
            else:
                if await self._post_game_chat_to_discord(chat):
                    posted += 1
                else:
                    break
            last_seen = max(last_seen, chat.chat_id)

        await self.config.last_seen_chat_id.set(last_seen)
        return {"seen": len(new_messages), "posted": posted, "skipped_echoes": skipped_echoes}

    async def _sync_loop(self):
        await self.bot.wait_until_ready()
        await asyncio.sleep(15)
        while True:
            try:
                if await self.config.enabled():
                    result = await self._sync_once()
                    if result["posted"] or result["skipped_echoes"]:
                        log.info(
                            "ChatManager sync posted=%s skipped_echoes=%s",
                            result["posted"],
                            result["skipped_echoes"],
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("ChatManager sync failed: %s", exc)
            await asyncio.sleep(max(DEFAULT_POLL_INTERVAL_SECONDS, int(await self.config.poll_interval_seconds())))

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if getattr(message.author, "bot", False):
            return
        channel_id = int(await self.config.channel_id() or 0)
        if not channel_id or getattr(message.channel, "id", None) != channel_id:
            return
        if not await self.config.enabled():
            return

        attachment_urls = [attachment.url for attachment in getattr(message, "attachments", [])]
        body_parts = [str(message.content or "").strip(), *attachment_urls]
        body = " ".join(part for part in body_parts if part).strip()
        if not body:
            return

        username = getattr(message.author, "display_name", None) or str(message.author)
        mc_message = format_discord_message_for_mc(str(username), body)
        try:
            await self._send_to_missionchief(mc_message)
            await self._remember_outgoing_echo(mc_message)
        except Exception as exc:
            log.exception("Could not send Discord chat message to MissionChief: %s", exc)
            with suppress(discord.HTTPException, discord.Forbidden, discord.NotFound):
                await message.add_reaction("\u26a0\ufe0f")

    @commands.group(name="chatmanager", aliases=["chatbridge"], invoke_without_command=True)
    @commands.admin()
    async def chatmanager(self, ctx: commands.Context):
        """Manage the MissionChief alliance chat bridge."""
        enabled = await self.config.enabled()
        channel_id = await self.config.channel_id()
        poll_interval = await self.config.poll_interval_seconds()
        last_seen = await self.config.last_seen_chat_id()
        await ctx.send(
            "ChatManager status:\n"
            f"- Enabled: `{enabled}`\n"
            f"- Discord channel: <#{channel_id}> (`{channel_id}`)\n"
            f"- Poll interval: `{poll_interval}` seconds\n"
            f"- Last seen MissionChief chat ID: `{last_seen}`"
        )

    @chatmanager.command(name="enable")
    @commands.admin()
    async def chatmanager_enable(self, ctx: commands.Context, enabled: bool):
        """Enable or disable chat synchronization."""
        await self.config.enabled.set(bool(enabled))
        await ctx.send(f"ChatManager enabled set to `{bool(enabled)}`.")

    @chatmanager.command(name="channel")
    @commands.admin()
    async def chatmanager_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the Discord channel used for chat synchronization."""
        await self.config.channel_id.set(channel.id)
        await ctx.send(f"ChatManager channel set to {channel.mention}.")

    @chatmanager.command(name="interval")
    @commands.admin()
    async def chatmanager_interval(self, ctx: commands.Context, seconds: int):
        """Set the MissionChief chat polling interval."""
        if seconds < 30 or seconds > 3600:
            await ctx.send("Interval must be between 30 and 3600 seconds.")
            return
        await self.config.poll_interval_seconds.set(int(seconds))
        await ctx.send(f"ChatManager poll interval set to `{seconds}` seconds.")

    @chatmanager.command(name="syncnow")
    @commands.admin()
    async def chatmanager_syncnow(self, ctx: commands.Context):
        """Run one MissionChief-to-Discord sync pass."""
        async with ctx.typing():
            try:
                result = await self._sync_once()
            except Exception as exc:
                await ctx.send(f"ChatManager sync failed: {exc}")
                return
        await ctx.send(
            "ChatManager sync complete:\n"
            f"- Seen in this pass: `{result['seen']}`\n"
            f"- Posted to Discord: `{result['posted']}`\n"
            f"- Skipped outgoing echoes: `{result['skipped_echoes']}`"
        )

    @chatmanager.command(name="reset")
    @commands.admin()
    async def chatmanager_reset(self, ctx: commands.Context):
        """Reset the last seen MissionChief chat ID. The next sync marks current history as seen."""
        await self.config.last_seen_chat_id.set(0)
        await ctx.send("ChatManager last seen ID reset. Next sync will mark current history as seen.")
