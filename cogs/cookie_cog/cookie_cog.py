# cookie_cog.py
# Red cog: Cookie/session manager for MissionChief scraping
# Requirements: aiohttp, cryptography, bs4
from __future__ import annotations
import os
import asyncio
import json
import pickle
import base64
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

import aiohttp
from aiohttp import ClientSession, ClientTimeout, CookieJar
from bs4 import BeautifulSoup
from cryptography.fernet import Fernet

from redbot.core import commands, Config, checks
from redbot.core.utils import chat_formatting as cf
from redbot.core.utils import cog_data_path

DEFAULTS = {
    "login_url": "https://www.missionchief.com/login",
    "check_url": "https://www.missionchief.com/alliances/1621",
    "user_agent": "Mozilla/5.0 (Windows NT; rv:109.0) Gecko/20100101 Firefox/117.0",
    "auto_refresh_minutes": 30,
    "cookie_warn_before_minutes": 60,
    "admin_channel_id": None,
}

class CookieCog(commands.Cog):
    """Cookie/session manager for MissionChief (login, store, expose session)."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xF1A2B3C4D5E6, force_registration=True)
        self.config.register_global(DEFAULTS)
        self.data_path = cog_data_path(self)
        os.makedirs(self.data_path, exist_ok=True)
        # file paths
        self._keyfile = os.path.join(self.data_path, "fernet.key")
        self._credfile = os.path.join(self.data_path, "credentials.bin")
        self._cookiefile = os.path.join(self.data_path, "cookies.bin")
        self._meta_file = os.path.join(self.data_path, "cookie_meta.json")

        # runtime
        self._fernet: Optional[Fernet] = None
        self._cookie_lock = asyncio.Lock()
        self._bg_task: Optional[asyncio.Task] = None

        # initialize key
        self._init_key()
        # start background worker later in bot_ready
        self.bot.loop.create_task(self._maybe_start_background())

    # ----------------------
    # Key / encryption utils
    # ----------------------
    def _init_key(self):
        if not os.path.exists(self._keyfile):
            key = Fernet.generate_key()
            with open(self._keyfile, "wb") as f:
                f.write(key)
            try:
                os.chmod(self._keyfile, 0o600)
            except Exception:
                pass
        with open(self._keyfile, "rb") as f:
            key = f.read()
        self._fernet = Fernet(key)

    def _encrypt(self, data: bytes) -> bytes:
        assert self._fernet is not None
        return self._fernet.encrypt(data)

    def _decrypt(self, token: bytes) -> bytes:
        assert self._fernet is not None
        return self._fernet.decrypt(token)

    # ----------------------
    # Credential management
    # ----------------------
    async def _save_credentials(self, username: str, password: str):
        data = json.dumps({"username": username, "password": password}).encode()
        token = self._encrypt(data)
        with open(self._credfile, "wb") as f:
            f.write(token)
        try:
            os.chmod(self._credfile, 0o600)
        except Exception:
            pass

    async def _load_credentials(self) -> Optional[Dict[str, str]]:
        if not os.path.exists(self._credfile):
            return None
        with open(self._credfile, "rb") as f:
            token = f.read()
        try:
            data = self._decrypt(token)
            return json.loads(data.decode())
        except Exception:
            return None

    # ----------------------
    # Cookie storage (serialize/deserialize minimal cookie info)
    # ----------------------
    async def _save_cookies(self, cookies: List[Dict[str, Any]], meta: Dict[str, Any]):
        payload = {"cookies": cookies, "meta": meta}
        token = self._encrypt(json.dumps(payload).encode())
        with open(self._cookiefile, "wb") as f:
            f.write(token)
        try:
            os.chmod(self._cookiefile, 0o600)
        except Exception:
            pass
        with open(self._meta_file, "w", encoding="utf-8") as mf:
            json.dump(meta, mf, default=str)

    async def _load_cookies(self) -> Optional[Dict[str, Any]]:
        if not os.path.exists(self._cookiefile):
            return None
        with open(self._cookiefile, "rb") as f:
            token = f.read()
        try:
            data = json.loads(self._decrypt(token).decode())
            return data
        except Exception:
            return None

    # ----------------------
    # Build a session from stored cookies
    # ----------------------
    async def build_session_from_store(self) -> ClientSession:
        """
        Returns an aiohttp.ClientSession with cookies loaded from storage (if any).
        Other cogs should use await get_session() instead of calling this directly.
        """
        jar = CookieJar()
        stored = await self._load_cookies()
        if stored and "cookies" in stored:
            for c in stored["cookies"]:
                name = c.get("name")
                value = c.get("value")
                domain = c.get("domain", "")
                # create a fake URL so update_cookies can associate domain/path
                url = f"https://{domain.lstrip('.')}/"
                jar.update_cookies({name: value}, response_url=url)
        timeout = ClientTimeout(total=30)
        session = aiohttp.ClientSession(cookie_jar=jar, timeout=timeout, headers={"User-Agent": await self.config.user_agent()})
        return session

    async def get_session(self) -> ClientSession:
        """
        Public method other cogs call to get a ready-to-use aiohttp.ClientSession.
        This will create a session with stored cookies. Caller should close the session if they don't need it.
        """
        async with self._cookie_lock:
            return await self.build_session_from_store()

    # ----------------------
    # Login flow
    # ----------------------
    async def _perform_login(self) -> bool:
        """Internal: perform login using stored credentials and persist cookies on success."""
        creds = await self._load_credentials()
        if not creds:
            return False
        username = creds["username"]
        password = creds["password"]
        login_url = await self.config.login_url()
        async with aiohttp.ClientSession(headers={"User-Agent": await self.config.user_agent()}) as s:
            # GET login page (to gather tokens if needed)
            try:
                r = await s.get(login_url)
                text = await r.text()
            except Exception:
                await self._log_admin(f"Login GET failed for {login_url}")
                return False

            # try to parse possible hidden csrf token
            soup = BeautifulSoup(text, "lxml")
            token_input = soup.find("input", {"name": "authenticity_token"}) or soup.find("input", {"name": "csrf_token"})
            token = token_input["value"] if token_input and token_input.has_attr("value") else None

            payload = {"username": username, "password": password}
            if token:
                # some sites expect token field named authenticity_token
                payload.setdefault("authenticity_token", token)

            # Attempt post
            try:
                post = await s.post(login_url, data=payload)
                await post.text()  # just to ensure fetch
            except Exception as e:
                await self._log_admin(f"Login POST failed: {e}")
                return False

            # Validate by requesting check_url
            check_url = await self.config.check_url()
            try:
                chk = await s.get(check_url)
                chk_text = await chk.text()
                # naive success test: page contains 'Logout' or alliance path OR username. You can change this per-site.
                if "Logout" in chk_text or "/logout" in chk_text or username in chk_text:
                    # store cookies
                    cookies_list = []
                    for domain, morsels in s.cookie_jar._cookies.items():
                        for path, cookie_dict in morsels.items():
                            for name, morsel in cookie_dict.items():
                                cookies_list.append({
                                    "name": name,
                                    "value": morsel.value,
                                    "domain": morsel['domain'] if morsel.get('domain') else domain,
                                    "path": morsel.get('path', path),
                                    "expires": morsel.get('expires'),
                                    "secure": bool(morsel.get('secure')),
                                })
                    meta = {
                        "saved_at": datetime.utcnow().isoformat(),
                        "saved_by": username,
                    }
                    await self._save_cookies(cookies_list, meta)
                    await self._log_admin(f"Login successful for `{username}`; cookies saved.")
                    # dispatch event for other cogs
                    try:
                        self.bot.dispatch("fara_cookie_updated", meta)
                    except Exception:
                        pass
                    return True
                else:
                    await self._log_admin("Login appears to have failed: check URL did not show expected content.")
                    return False
            except Exception as e:
                await self._log_admin(f"Login validation failed: {e}")
                return False

    # ----------------------
    # Commands (admin)
    # ----------------------
    @commands.group()
    @checks.is_owner()
    async def fara(self, ctx: commands.Context):
        """FireAndRescue Academy utility commands (admin only)."""

    @fara.command()
    async def setcreds(self, ctx: commands.Context, username: str = None, password: str = None):
        """
        Set credentials for MissionChief.
        Usage: !fara setcreds <username> <password>
        For safety use in DM or in an admin-only channel.
        """
        if username is None or password is None:
            await ctx.send("Usage: `!fara setcreds <username> <password>` — voer dit bij voorkeur via DM.")
            return
        await self._save_credentials(username, password)
        await ctx.send("Credentials opgeslagen (versleuteld). Ik probeer nu in te loggen.")
        ok = await self._perform_login()
        if ok:
            await ctx.send("Login succesvol en cookies opgeslagen.")
        else:
            await ctx.send("Login faalde. Check logs (of probeer `!fara login` opnieuw).")

    @fara.command()
    async def login(self, ctx: commands.Context):
        """Forceer een login/refresh nu."""
        await ctx.send("Proberen in te loggen...")
        ok = await self._perform_login()
        if ok:
            await ctx.send("Login gelukt.")
        else:
            await ctx.send("Login faalde. Check credentials of logs.")

    @fara.command()
    async def status(self, ctx: commands.Context):
        """Toon status van cookie/credentials."""
        creds = await self._load_credentials()
        stored = await self._load_cookies()
        cfg = await self.config.all()
        lines = []
        lines.append(f"Credentials set: {'ja' if creds else 'nee'}")
        if stored and stored.get("meta"):
            lines.append(f"Cookies saved: ja")
            lines.append(f"Cookies saved at: {stored['meta'].get('saved_at')}")
        else:
            lines.append("Cookies saved: nee")
        lines.append(f"Login URL: {cfg['login_url']}")
        lines.append(f"Check URL: {cfg['check_url']}")
        await ctx.send("```\n" + "\n".join(lines) + "\n```")

    @fara.command()
    async def setadminchannel(self, ctx: commands.Context, channel_id: int = None):
        """Set a discord channel id for admin logs. Use channel id (copy link to channel)."""
        if channel_id is None:
            await ctx.send("Gebruik: `!fara setadminchannel <channel_id>`")
            return
        await self.config.admin_channel_id.set(channel_id)
        await ctx.send(f"Admin channel opgeslagen: {channel_id}")

    @fara.command()
    async def logout(self, ctx: commands.Context):
        """Clear stored cookies (will require re-login)."""
        if os.path.exists(self._cookiefile):
            os.remove(self._cookiefile)
        if os.path.exists(self._meta_file):
            os.remove(self._meta_file)
        await ctx.send("Cookies verwijderd. Je moet opnieuw inloggen.")

    @fara.command()
    async def testrequest(self, ctx: commands.Context):
        """Make a test request using the stored session and report success/failure."""
        session = await self.get_session()
        try:
            check = await session.get(await self.config.check_url())
            text = await check.text()
            await session.close()
            if "Logout" in text or "/logout" in text:
                await ctx.send("Testrequest OK — ingelogd (Logout gevonden).")
            else:
                await ctx.send("Testrequest voltooid — login niet bevestigd (Logout niet gevonden).")
        except Exception as e:
            await ctx.send(f"Testrequest mislukt: {e}")

    # ----------------------
    # Background tasks
    # ----------------------
    async def _maybe_start_background(self):
        # wait until bot ready
        await self.bot.wait_until_red_ready()
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._background_worker())

    async def _background_worker(self):
        """
        Periodically check cookie expiry (naive) and attempt refresh if needed.
        """
        while True:
            try:
                cfg = await self.config.all()
                refresh_minutes = int(cfg.get("auto_refresh_minutes", 30))
                # load cookie meta
                stored = await self._load_cookies()
                if stored and stored.get("meta"):
                    # naive expiry check: if cookie was saved > (warn_before) minutes ago, try refresh
                    saved_at = stored["meta"].get("saved_at")
                    if saved_at:
                        saved_dt = datetime.fromisoformat(saved_at)
                        warn_before = int(cfg.get("cookie_warn_before_minutes", 60))
                        if datetime.utcnow() - saved_dt > timedelta(minutes=warn_before):
                            await self._log_admin("Cookie ouder dan warn threshold. Proberen te refreshen (login).")
                            ok = await self._perform_login()
                            if not ok:
                                await self._log_admin("Automatische refresh FAILED.")
                else:
                    # no cookie present: try a login if creds exist
                    creds = await self._load_credentials()
                    if creds:
                        await self._log_admin("Geen cookies gevonden maar credentials aanwezig. Proberen in te loggen.")
                        await self._perform_login()
            except Exception as e:
                await self._log_admin(f"Background worker error: {e}")
            await asyncio.sleep(60 * refresh_minutes)

    # ----------------------
    # Admin logging helper
    # ----------------------
    async def _log_admin(self, message: str):
        # send to configured channel if set, else to bot owner
        cfg = await self.config.all()
        cid = cfg.get("admin_channel_id")
        prefix = "[FARA CookieCog]"
        try:
            if cid:
                ch = self.bot.get_channel(int(cid))
                if ch:
                    await ch.send(f"{prefix} {message}")
                    return
        except Exception:
            pass
        # fallback: send to owner if available
        owners = await self.bot.owner_id()
        try:
            if isinstance(owners, int):
                owner = self.bot.get_user(owners)
                if owner:
                    await owner.send(f"{prefix} {message}")
                    return
        except Exception:
            pass
        # last fallback: log to console
        try:
            self.bot.log.info(f"{prefix} {message}")
        except Exception:
            print(f"{prefix} {message}")
