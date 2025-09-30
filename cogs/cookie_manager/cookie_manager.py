# cookie_manager.py
# Red cog: Cookie/session manager for MissionChief scraping
# Requirements: aiohttp, cryptography, bs4
# Language in user-facing strings: English (per server policy)
from __future__ import annotations

import os
import asyncio
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from http.cookies import SimpleCookie

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
    "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "auto_refresh_minutes": 30,
    "cookie_warn_before_minutes": 60,
    "admin_channel_id": None,
    "csrf_field_names": ["authenticity_token", "csrf_token", "_token", "__RequestVerificationToken"],
    "username_field": "username",
    "password_field": "password",
    "extra_form_fields": {},  # dict of {field: value} if needed
}

class CookieManager(commands.Cog):
    """Cookie/session manager for MissionChief (login, store, expose session)."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xC00C1EABC123, force_registration=True)
        self.config.register_global(**DEFAULTS)
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

        self._init_key()
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
    # Cookie storage
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
    # Build sessions
    # ----------------------
    def _cookie_dicts_to_simplecookie(self, cookies: List[Dict[str, Any]]) -> SimpleCookie:
        sc = SimpleCookie()
        # Preserve common attributes in case site is picky; note that aiohttp uses response_url to scope
        for c in cookies:
            name = c.get("name")
            value = c.get("value")
            if name is None or value is None:
                continue
            sc[name] = value
            morsel = sc[name]
            # Preserve flags/attrs as notes (best-effort)
            if c.get("domain"):
                morsel["domain"] = c["domain"]
            if c.get("path"):
                morsel["path"] = c["path"]
            if c.get("expires"):
                morsel["expires"] = str(c["expires"])
            if c.get("secure"):
                morsel["secure"] = True
            if c.get("httponly"):
                morsel["httponly"] = True
            if c.get("samesite"):
                morsel["samesite"] = c["samesite"]
        return sc

    async def build_session_from_store(self) -> ClientSession:
        jar = CookieJar()
        stored = await self._load_cookies()
        timeout = ClientTimeout(total=40)
        headers = {"User-Agent": await self.config.user_agent()}
        session = aiohttp.ClientSession(cookie_jar=jar, timeout=timeout, headers=headers)

        if stored and "cookies" in stored and stored["cookies"]:
            simple = self._cookie_dicts_to_simplecookie(stored["cookies"])
            # Attach per cookie using a plausible response_url to scope domain/path
            for c in stored["cookies"]:
                domain = c.get("domain", "www.missionchief.com")
                url = f"https://{domain.lstrip('.').strip('/')}/"
                try:
                    jar.update_cookies({c["name"]: c["value"]}, response_url=url)
                except Exception:
                    # fallback: update whole jar with SimpleCookie (less precise scoping)
                    jar.update_cookies(simple)
                    break
        return session

    async def get_session(self) -> ClientSession:
        async with self._cookie_lock:
            return await self.build_session_from_store()

    # ----------------------
    # Login flow
    # ----------------------
    async def _perform_login(self) -> bool:
        creds = await self._load_credentials()
        if not creds:
            await self._log_admin("No credentials stored. Use DM: !cookie setcreds <username> <password>")
            return False

        username = creds["username"]
        password = creds["password"]
        cfg = await self.config.all()
        login_url = cfg["login_url"]
        check_url = cfg["check_url"]
        ua = cfg["user_agent"]
        csrf_field_names: List[str] = cfg.get("csrf_field_names", DEFAULTS["csrf_field_names"])
        username_field = cfg.get("username_field", DEFAULTS["username_field"])
        password_field = cfg.get("password_field", DEFAULTS["password_field"])
        extra_fields = cfg.get("extra_form_fields", {}) or {}

        async with aiohttp.ClientSession(headers={"User-Agent": ua}) as s:
            # GET login page
            try:
                r = await s.get(login_url, allow_redirects=True)
                text = await r.text()
            except Exception as e:
                await self._log_admin(f"Login GET failed: {e}")
                return False

            token_value = None
            try:
                soup = BeautifulSoup(text, "lxml")
                # Try multiple CSRF field names
                for fname in csrf_field_names:
                    token_input = soup.find("input", {"name": fname})
                    if token_input and token_input.has_attr("value"):
                        token_value = token_input["value"]
                        break
            except Exception:
                pass

            payload = {
                username_field: username,
                password_field: password,
            }
            if token_value is not None:
                # insert token under first matching name
                payload[next((n for n in csrf_field_names if n), "csrf_token")] = token_value
            # Merge any extra required fields (site-specific)
            payload.update(extra_fields)

            # POST login
            try:
                post = await s.post(login_url, data=payload, allow_redirects=True)
                await post.text()  # ensure body read
            except Exception as e:
                await self._log_admin(f"Login POST failed: {e}")
                return False

            # Validate by hitting check_url
            try:
                chk = await s.get(check_url, allow_redirects=True)
                chk_text = await chk.text()
                # Heuristics: look for logout link, account name, or alliance slug
                success = any(x in chk_text for x in ["Logout", "/logout", username, "Alliance"])
                if success:
                    # dump cookies best-effort including flags
                    cookies_list = []
                    # Access underlying cookies
                    try:
                        cj = s.cookie_jar._cookies  # type: ignore[attr-defined]
                        for domain, path_map in cj.items():
                            for path, cookie_dict in path_map.items():
                                for name, morsel in cookie_dict.items():
                                    item = {
                                        "name": name,
                                        "value": morsel.value,
                                        "domain": morsel["domain"] or domain,
                                        "path": morsel["path"] or path,
                                        "secure": bool(morsel["secure"]),
                                        "httponly": bool(morsel["httponly"]),
                                    }
                                    if morsel.get("expires"):
                                        item["expires"] = morsel["expires"]
                                    if morsel.get("samesite"):
                                        item["samesite"] = morsel["samesite"]
                                    cookies_list.append(item)
                    except Exception:
                        # Fallback: best effort from jar filter_cookies
                        fc = s.cookie_jar.filter_cookies(check_url)
                        for k, v in fc.items():
                            cookies_list.append({"name": k, "value": v.value, "domain": "www.missionchief.com", "path": "/"})

                    meta = {
                        "saved_at_utc": datetime.utcnow().isoformat(),
                        "saved_for_user": username,
                        "login_url": login_url,
                        "check_url": check_url,
                        "user_agent": ua,
                    }
                    await self._save_cookies(cookies_list, meta)
                    await self._log_admin("Login successful; cookies stored.")
                    try:
                        self.bot.dispatch("fara_cookie_updated", meta)
                    except Exception:
                        pass
                    return True
                else:
                    await self._log_admin("Login appears to have failed: validation page did not contain expected markers.")
                    return False
            except Exception as e:
                await self._log_admin(f"Login validation failed: {e}")
                return False

    # ----------------------
    # Commands (owner only)
    # ----------------------
    @commands.group(name="cookie")
    @checks.is_owner()
    async def cookie(self, ctx: commands.Context):
        """Cookie Manager commands (owner only)."""

    @cookie.command(name="setcreds")
    async def setcreds(self, ctx: commands.Context, username: str = None, password: str = None):
        """
        Store credentials (encrypted) and attempt login. Use this in DM.
        Usage: !cookie setcreds <username> <password>
        """
        if not isinstance(ctx.channel, (type(ctx.author.dm_channel),)) and ctx.guild is not None:
            await ctx.send("For security, please DM the bot: `!cookie setcreds <username> <password>`")
            return
        if username is None or password is None:
            await ctx.send("Usage: `!cookie setcreds <username> <password>` (send via DM).")
            return
        await self._save_credentials(username, password)
        await ctx.send("Credentials stored (encrypted). Attempting login...")
        ok = await self._perform_login()
        if ok:
            await ctx.send("Login successful and cookies saved.")
        else:
            await ctx.send("Login failed. Check credentials and try `!cookie login`.")

    @cookie.command(name="login")
    async def login(self, ctx: commands.Context):
        """Force a login/refresh now."""
        await ctx.send("Attempting login...")
        ok = await self._perform_login()
        if ok:
            await ctx.send("Login successful.")
        else:
            await ctx.send("Login failed. Use DM `!cookie setcreds <u> <p>` if not set.")

    @cookie.command(name="logout")
    async def logout(self, ctx: commands.Context):
        """Clear stored cookies (requires re-login)."""
        if os.path.exists(self._cookiefile):
            os.remove(self._cookiefile)
        if os.path.exists(self._meta_file):
            os.remove(self._meta_file)
        await ctx.send("Cookies cleared. You will need to login again.")

    @cookie.command(name="status")
    async def status(self, ctx: commands.Context):
        """Show cookie/credential status and config summary."""
        creds = await self._load_credentials()
        stored = await self._load_cookies()
        cfg = await self.config.all()
        lines = []
        lines.append(f"Credentials set: {'yes' if creds else 'no'}")
        if stored and stored.get("meta"):
            lines.append(f"Cookies saved: yes")
            lines.append(f"Saved at (UTC): {stored['meta'].get('saved_at_utc')}")
        else:
            lines.append("Cookies saved: no")
        lines.append(f"Login URL: {cfg['login_url']}")
        lines.append(f"Check URL: {cfg['check_url']}")
        lines.append(f"User-Agent: {cfg['user_agent']}")
        lines.append(f"Auto refresh minutes: {cfg['auto_refresh_minutes']}")
        lines.append(f"Warn before minutes: {cfg['cookie_warn_before_minutes']}")
        await ctx.send("```\n" + "\n".join(lines) + "\n```")

    @cookie.group(name="config")
    async def config_group(self, ctx: commands.Context):
        """Configure cookie manager settings."""

    @config_group.command(name="set")
    async def config_set(self, ctx: commands.Context, key: str, *, value: str):
        """
        Set a config value.
        Keys: login_url, check_url, user_agent, auto_refresh_minutes, cookie_warn_before_minutes, username_field, password_field
        Special keys: csrf_field_names (comma-separated), extra_form_fields (JSON dict)
        """
        key = key.strip().lower()
        if key == "csrf_field_names":
            arr = [x.strip() for x in value.split(",") if x.strip()]
            await self.config.csrf_field_names.set(arr or DEFAULTS["csrf_field_names"])
            await ctx.send(f"Set csrf_field_names to: {arr}")
        elif key == "extra_form_fields":
            try:
                obj = json.loads(value)
                if not isinstance(obj, dict):
                    raise ValueError("Not a JSON object")
                await self.config.extra_form_fields.set(obj)
                await ctx.send("Set extra_form_fields JSON.")
            except Exception as e:
                await ctx.send(f"Invalid JSON for extra_form_fields: {e}")
        elif key in DEFAULTS:
            # coerce ints if needed
            if key in ["auto_refresh_minutes", "cookie_warn_before_minutes"]:
                try:
                    value_int = int(value)
                except Exception:
                    await ctx.send("Value must be an integer.")
                    return
                await getattr(self.config, key).set(value_int)
            else:
                await getattr(self.config, key).set(value)
            await ctx.send(f"Set {key}.")
        elif key in ["username_field", "password_field"]:
            await getattr(self.config, key).set(value)
            await ctx.send(f"Set {key}.")
        else:
            await ctx.send("Unknown key.")

    @cookie.command(name="setadminchannel")
    async def setadminchannel(self, ctx: commands.Context, channel_id: int = None):
        """Set a Discord channel id for admin logs."""
        if channel_id is None:
            await ctx.send("Usage: `!cookie setadminchannel <channel_id>`")
            return
        await self.config.admin_channel_id.set(channel_id)
        await ctx.send(f"Admin channel set: {channel_id}")

    @cookie.command(name="testrequest")
    async def testrequest(self, ctx: commands.Context):
        """Make a test request using stored cookies and report success/failure."""
        session = await self.get_session()
        try:
            check = await session.get(await self.config.check_url())
            text = await check.text()
            await session.close()
            if ("Logout" in text) or ("/logout" in text):
                await ctx.send("Test request OK — login markers detected.")
            else:
                await ctx.send("Test request completed — login markers not detected.")
        except Exception as e:
            await ctx.send(f"Test request failed: {e}")

    # ----------------------
    # Background tasks
    # ----------------------
    async def _maybe_start_background(self):
        await self.bot.wait_until_red_ready()
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._background_worker())

    async def _background_worker(self):
        while True:
            try:
                cfg = await self.config.all()
                refresh_minutes = int(cfg.get("auto_refresh_minutes", 30))
                stored = await self._load_cookies()
                if stored and stored.get("meta"):
                    saved_at = stored["meta"].get("saved_at_utc")
                    if saved_at:
                        try:
                            saved_dt = datetime.fromisoformat(saved_at)
                        except Exception:
                            saved_dt = datetime.utcnow() - timedelta(minutes=10000)
                        warn_before = int(cfg.get("cookie_warn_before_minutes", 60))
                        if datetime.utcnow() - saved_dt > timedelta(minutes=warn_before):
                            await self._log_admin("Cookie older than warn threshold. Attempting automatic refresh...")
                            ok = await self._perform_login()
                            if not ok:
                                await self._log_admin("Automatic refresh FAILED.")
                else:
                    creds = await self._load_credentials()
                    if creds:
                        await self._log_admin("No cookies found but credentials present. Attempting login...")
                        await self._perform_login()
            except Exception as e:
                await self._log_admin(f"Background worker error: {e}")
            await asyncio.sleep(max(60, 60 * int((await self.config.auto_refresh_minutes()))))

    # ----------------------
    # Admin logging helper
    # ----------------------
    async def _log_admin(self, message: str):
        cfg = await self.config.all()
        cid = cfg.get("admin_channel_id")
        prefix = "[CookieManager]"
        try:
            if cid:
                ch = self.bot.get_channel(int(cid))
                if ch:
                    await ch.send(f"{prefix} {message}")
                    return
        except Exception:
            pass
        # Fallback: log to console
        try:
            self.bot.log.info(f"{prefix} {message}")
        except Exception:
            print(f"{prefix} {message}")


async def setup(bot):
    cog = CookieManager(bot)
    await bot.add_cog(cog)
