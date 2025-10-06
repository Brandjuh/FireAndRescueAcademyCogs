# cookie_manager.py (v5.2)
from __future__ import annotations

import os
import asyncio
import json
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
from http.cookies import SimpleCookie
import logging
from urllib.parse import urljoin

import aiohttp
from aiohttp import ClientSession, ClientTimeout, CookieJar
from bs4 import BeautifulSoup
from cryptography.fernet import Fernet

from redbot.core import commands, Config, checks
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.FARA.CookieManager")

DEFAULTS = {
    "login_url": "https://www.missionchief.com/users/sign_in",
    "check_url": "https://www.missionchief.com/buildings",
    "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "auto_refresh_minutes": 30,
    "cookie_warn_before_minutes": 60,
    "admin_channel_id": None,
    "csrf_field_names": ["authenticity_token", "csrf_token", "_token", "__RequestVerificationToken"],
    "username_field": "user[email]",
    "password_field": "user[password]",
    "extra_form_fields": {},
    "success_markers": ["Logout", "/logout", "Sign out", "My profile"],
    "success_url_contains": ["/buildings", "/dashboard", "/missions"],
    "login_failure_url_contains": ["/users/sign_in", "/login"],
    "validation_mode": "url_or_markers"  # url_or_markers | url_only | markers_only
}

class CookieManager(commands.Cog):
    """Cookie/session manager for MissionChief (login, store, expose session)."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xC00C1EABC123, force_registration=True)
        self.config.register_global(**DEFAULTS)
        self.data_path = cog_data_path(self)
        os.makedirs(self.data_path, exist_ok=True)

        self._keyfile = os.path.join(self.data_path, "fernet.key")
        self._credfile = os.path.join(self.data_path, "credentials.bin")
        self._cookiefile = os.path.join(self.data_path, "cookies.bin")
        self._meta_file = os.path.join(self.data_path, "cookie_meta.json")

        self._fernet: Optional[Fernet] = None
        self._cookie_lock = asyncio.Lock()
        self._bg_task: Optional[asyncio.Task] = None

        self._init_key()
        self.bot.loop.create_task(self._maybe_start_background())

    # Encryption
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
        return self._fernet.encrypt(data)  # type: ignore

    def _decrypt(self, token: bytes) -> bytes:
        return self._fernet.decrypt(token)  # type: ignore

    # Credentials
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

    # Cookies
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

    # Helpers
    def _cookie_dicts_to_simplecookie(self, cookies: List[Dict[str, Any]]) -> SimpleCookie:
        sc = SimpleCookie()
        for c in cookies:
            name = c.get("name")
            value = c.get("value")
            if name is None or value is None:
                continue
            sc[name] = value
            morsel = sc[name]
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
            for c in stored["cookies"]:
                domain = c.get("domain", "www.missionchief.com")
                url = f"https://{domain.lstrip('.').strip('/')}/"
                try:
                    jar.update_cookies({c["name"]: c["value"]}, response_url=url)
                except Exception:
                    jar.update_cookies(simple)
                    break
        return session

    async def get_session(self) -> ClientSession:
        async with self._cookie_lock:
            return await self.build_session_from_store()

    # Login
    def _parse_login_form(self, html: str, login_url: str) -> Tuple[str, Dict[str, str]]:
        soup = BeautifulSoup(html, "lxml")
        form = soup.find("form", method=lambda x: x and x.lower() == "post")
        action_url = login_url
        fields: Dict[str, str] = {}
        if form is not None:
            action = form.get("action")
            if action:
                action_url = urljoin(login_url, action)
            for inp in form.find_all("input"):
                name = inp.get("name")
                if not name:
                    continue
                val = inp.get("value", "")
                fields[name] = val
        return action_url, fields

    def _inject_credentials(self, fields: Dict[str, str], username: str, password: str, cfg: Dict[str, Any]) -> Dict[str, str]:
        ufield = cfg.get("username_field", DEFAULTS["username_field"])
        pfield = cfg.get("password_field", DEFAULTS["password_field"])
        fields[ufield] = username
        fields[pfield] = password
        extra = cfg.get("extra_form_fields", {}) or {}
        for k, v in extra.items():
            fields[k] = v
        return fields

    async def _do_login_flow(self) -> Dict[str, Any]:
        cfg = await self.config.all()
        creds = await self._load_credentials()
        if not creds:
            return {"ok": False, "reason": "no_creds"}
        username = creds["username"]
        password = creds["password"]
        login_url = cfg["login_url"]
        ua = cfg["user_agent"]

        out: Dict[str, Any] = {"steps": []}

        async with aiohttp.ClientSession(headers={"User-Agent": ua}) as s:
            step = {"step": "GET_login", "url": login_url}
            try:
                r = await s.get(login_url, allow_redirects=True)
                step["status"] = r.status
                html = await r.text()
                step["final_url"] = str(r.url)
                out["steps"].append(step)
            except Exception as e:
                step["error"] = str(e)
                out["steps"].append(step)
                out["ok"] = False
                out["reason"] = "login_get_failed"
                return out

            action_url, parsed_fields = self._parse_login_form(html, login_url)
            payload = self._inject_credentials(parsed_fields, username, password, cfg)

            step = {"step": "POST_login", "url": action_url, "referer": login_url, "field_names": list(payload.keys())}
            try:
                post = await s.post(action_url, data=payload, allow_redirects=True, headers={"Referer": login_url})
                step["status"] = post.status
                step["final_url"] = str(post.url)
                post_html = await post.text()
                out["steps"].append(step)
            except Exception as e:
                step["error"] = str(e)
                out["steps"].append(step)
                out["ok"] = False
                out["reason"] = "login_post_failed"
                return out

            # validation against check_url
            check_url = cfg["check_url"]
            step = {"step": "GET_check", "url": check_url, "referer": action_url}
            try:
                chk = await s.get(check_url, allow_redirects=True, headers={"Referer": action_url})
                chk_text = await chk.text()
                final_url = str(chk.url)
                step["status"] = chk.status
                step["final_url"] = final_url
                out["steps"].append(step)

                failure_frags: List[str] = cfg.get("login_failure_url_contains", DEFAULTS["login_failure_url_contains"])
                success_frags: List[str] = cfg.get("success_url_contains", DEFAULTS["success_url_contains"])
                markers: List[str] = cfg.get("success_markers", DEFAULTS["success_markers"])
                mode: str = cfg.get("validation_mode", DEFAULTS["validation_mode"])

                fail = any(frag in final_url for frag in failure_frags)
                ok_by_url = any(frag in final_url for frag in success_frags) and not fail
                ok_by_markers = any(m in chk_text for m in markers)

                if mode == "url_only":
                    success = ok_by_url
                elif mode == "markers_only":
                    success = ok_by_markers and not fail
                else:
                    success = (ok_by_url or ok_by_markers) and not fail

                if success:
                    cookies_list = []
                    try:
                        cj = s.cookie_jar._cookies  # type: ignore[attr-defined]
                        for domain, path_map in cj.items():
                            for path, cookie_dict in path_map.items():
                                for name, morsel in cookie_dict.items():
                                    item = {"name": name, "value": morsel.value,
                                            "domain": morsel["domain"] or domain,
                                            "path": morsel["path"] or path,
                                            "secure": bool(morsel["secure"]),
                                            "httponly": bool(morsel["httponly"])}
                                    if morsel.get("expires"):
                                        item["expires"] = morsel["expires"]
                                    if morsel.get("samesite"):
                                        item["samesite"] = morsel["samesite"]
                                    cookies_list.append(item)
                    except Exception:
                        fc = s.cookie_jar.filter_cookies(check_url)
                        for k, v in fc.items():
                            cookies_list.append({"name": k, "value": v.value, "domain": "www.missionchief.com", "path": "/"})
                    meta = {
                        "saved_at_utc": datetime.utcnow().isoformat(),
                        "saved_for_user": username,
                        "login_url": login_url,
                        "action_url": action_url,
                        "check_url": check_url,
                        "final_url": final_url,
                        "user_agent": ua,
                    }
                    await self._save_cookies(cookies_list, meta)
                    out["ok"] = True
                    out["reason"] = "success"
                    out["meta"] = meta
                    return out
                else:
                    out["ok"] = False
                    out["reason"] = "validation_failed"
                    return out
            except Exception as e:
                step["error"] = str(e)
                out["steps"].append(step)
                out["ok"] = False
                out["reason"] = "check_failed"
                return out

    async def _perform_login(self) -> bool:
        res = await self._do_login_flow()
        if res.get("ok"):
            await self._log_admin("Login successful; cookies stored.")
            try:
                self.bot.dispatch("fara_cookie_updated", res.get("meta", {}))
            except Exception:
                pass
            return True
        reason = res.get("reason", "unknown")
        if reason == "validation_failed":
            await self._log_admin("Login appears to have failed: validation page missing expected markers/URLs.")
        else:
            await self._log_admin(f"Login failed: {reason}.")
        return False

    # Commands
    @commands.group(name="cookie")
    @checks.is_owner()
    async def cookie(self, ctx: commands.Context):
        """Cookie Manager commands (owner only)."""

    @cookie.command(name="setcreds")
    async def setcreds(self, ctx: commands.Context, username: str = None, password: str = None):
        """Store credentials (encrypted) and attempt login. Use this in DM."""
        if ctx.guild is not None:
            await ctx.send("For security, please DM the bot: `!cookie setcreds <username> <password>`")
            return
        if username is None or password is None:
            await ctx.send("Usage: `!cookie setcreds <username> <password>` (send via DM).")
            return
        await self._save_credentials(username, password)
        await ctx.send("Credentials stored (encrypted). Attempting login...")
        ok = await self._perform_login()
        await ctx.send("Login successful and cookies saved." if ok else "Login failed. Check credentials or adjust config (fields/tokens).")

    @cookie.command(name="login")
    async def login(self, ctx: commands.Context):
        """Force a login/refresh now."""
        await ctx.send("Attempting login...")
        ok = await self._perform_login()
        await ctx.send("Login successful." if ok else "Login failed. See `!cookie debug trace`.")

    @cookie.command(name="logout")
    async def logout(self, ctx: commands.Context):
        """Clear stored cookies (requires re-login)."""
        for p in [self._cookiefile, self._meta_file]:
            if os.path.exists(p):
                os.remove(p)
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
            if stored["meta"].get("final_url"):
                lines.append(f"Last final URL: {stored['meta'].get('final_url')}")
        else:
            lines.append("Cookies saved: no")
        lines.append(f"Login URL: {cfg['login_url']}")
        lines.append(f"Check URL: {cfg['check_url']}")
        lines.append(f"Validation: {cfg.get('validation_mode','url_or_markers')}")
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
        List keys: csrf_field_names, success_markers, success_url_contains, login_failure_url_contains
        Special: extra_form_fields (JSON), validation_mode (url_or_markers|url_only|markers_only)
        """
        key = key.strip().lower()
        listy = ["csrf_field_names", "success_markers", "success_url_contains", "login_failure_url_contains"]
        if key in listy:
            arr = [x.strip() for x in value.split(",") if x.strip()]
            await getattr(self.config, key).set(arr)
            await ctx.send(f"Set {key} to: {arr}")
            return
        if key == "extra_form_fields":
            try:
                obj = json.loads(value)
                if not isinstance(obj, dict):
                    raise ValueError("Not a JSON object")
                await self.config.extra_form_fields.set(obj)
                await ctx.send("Set extra_form_fields JSON.")
            except Exception as e:
                await ctx.send(f"Invalid JSON for extra_form_fields: {e}")
            return
        if key in ["auto_refresh_minutes", "cookie_warn_before_minutes"]:
            try:
                value_int = int(value)
            except Exception:
                await ctx.send("Value must be an integer.")
                return
            await getattr(self.config, key).set(value_int)
            await ctx.send(f"Set {key}.")
            return
        if key in ["login_url", "check_url", "user_agent", "username_field", "password_field", "validation_mode"]:
            await getattr(self.config, key).set(value)
            await ctx.send(f"Set {key}.")
            return
        await ctx.send("Unknown key.")

    @cookie.command(name="testrequest")
    async def testrequest(self, ctx: commands.Context):
        """Make a test request using stored cookies and report success/failure."""
        session = await self.get_session()
        try:
            url = await self.config.check_url()
            r = await session.get(url, allow_redirects=True)
            text = await r.text()
            final_url = str(r.url)
            await session.close()
            failure = any(f in final_url for f in (await self.config.login_failure_url_contains()))
            success_frags = await self.config.success_url_contains()
            markers = await self.config.success_markers()
            mode = await self.config.validation_mode()

            ok_by_url = any(s in final_url for s in success_frags) and not failure
            ok_by_markers = any(m in text for m in markers)

            if mode == "url_only":
                ok = ok_by_url
            elif mode == "markers_only":
                ok = ok_by_markers and not failure
            else:
                ok = (ok_by_url or ok_by_markers) and not failure

            if failure:
                await ctx.send(f"Test request indicates NOT logged in (final url: {final_url}).")
            elif ok:
                await ctx.send("Test request OK — session valid.")
            else:
                await ctx.send(f"Test request completed — could not confirm, final url: {final_url}.")
        except Exception as e:
            await ctx.send(f"Test request failed: {e}")

    @cookie.command(name="debug")
    async def debug(self, ctx: commands.Context, action: str):
        """Debug helpers: `!cookie debug loginflow` or `!cookie debug trace`"""
        action = action.lower().strip()
        if action == "loginflow":
            cfg = await self.config.all()
            await ctx.send("```\n"
                        f"login_url={cfg['login_url']}\n"
                        f"check_url={cfg['check_url']}\n"
                        f"username_field={cfg['username_field']}\n"
                        f"password_field={cfg['password_field']}\n"
                        f"csrf_field_names={cfg['csrf_field_names']}\n"
                        f"success_markers={cfg['success_markers']}\n"
                        f"success_url_contains={cfg['success_url_contains']}\n"
                        f"login_failure_url_contains={cfg['login_failure_url_contains']}\n"
                        f"validation_mode={cfg.get('validation_mode','url_or_markers')}\n"
                        "```")
            return
        if action == "trace":
            res = await self._do_login_flow()
            pretty = json.dumps(res, ensure_ascii=False, indent=2)
            if len(pretty) > 1800:
                pretty = pretty[:1800] + "... (truncated)"
            await ctx.send("```json\n" + pretty + "\n```")
            return
        await ctx.send("Unknown debug action. Try: `!cookie debug loginflow` or `!cookie debug trace`.")

    async def _maybe_start_background(self):
        try:
            await self.bot.wait_until_red_ready()
        except Exception:
            pass
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._background_worker())

    async def _background_worker(self):
        while True:
            try:
                cfg = await self.config.all()
                refresh_minutes = int(cfg.get("auto_refresh_minutes", 30))
                warn_before = int(cfg.get("cookie_warn_before_minutes", 60))

                stored = await self._load_cookies()
                needs_check = False
                if stored and stored.get("meta"):
                    saved_at = stored["meta"].get("saved_at_utc")
                    if saved_at:
                        try:
                            saved_dt = datetime.fromisoformat(saved_at)
                        except Exception:
                            saved_dt = datetime.utcnow() - timedelta(days=365)
                        if datetime.utcnow() - saved_dt > timedelta(minutes=warn_before):
                            # Before attempting re-login, verify if cookies still work
                            ok = await self._quick_session_check()
                            if ok:
                                # bump timestamp to avoid unnecessary relogin
                                stored["meta"]["saved_at_utc"] = datetime.utcnow().isoformat()
                                await self._save_cookies(stored.get("cookies", []), stored["meta"])
                                await self._log_admin("Cookie older than warn threshold but session still valid. Timestamp refreshed.")
                            else:
                                await self._log_admin("Cookie older than warn threshold. Attempting automatic refresh...")
                                ok2 = await self._perform_login()
                                if not ok2:
                                    await self._log_admin("Automatic refresh FAILED.")
                else:
                    creds = await self._load_credentials()
                    if creds:
                        await self._log_admin("No cookies found but credentials present. Attempting login...")
                        await self._perform_login()
            except Exception as e:
                await self._log_admin(f"Background worker error: {e}")
            await asyncio.sleep(max(60, 60 * int((await self.config.auto_refresh_minutes()))))

    async def _quick_session_check(self) -> bool:
        """Lightweight check if current cookies still pass the check_url validation."""
        session = await self.get_session()
        try:
            url = await self.config.check_url()
            r = await session.get(url, allow_redirects=True)
            text = await r.text()
            final_url = str(r.url)
            failure = any(f in final_url for f in (await self.config.login_failure_url_contains()))
            success_frags = await self.config.success_url_contains()
            markers = await self.config.success_markers()
            mode = await self.config.validation_mode()
            ok_by_url = any(s in final_url for s in success_frags) and not failure
            ok_by_markers = any(m in text for m in markers)
            if mode == "url_only":
                ok = ok_by_url
            elif mode == "markers_only":
                ok = ok_by_markers and not failure
            else:
                ok = (ok_by_url or ok_by_markers) and not failure
            await session.close()
            return ok
        except Exception:
            try:
                await session.close()
            except Exception:
                pass
            return False

    async def _log_admin(self, message: str):
        cfg = await self.config.all()
        cid = cfg.get("admin_channel_id")
        prefix = "[CookieManager]"
        if cid:
            try:
                ch = self.bot.get_channel(int(cid))
                if ch:
                    await ch.send(f"{prefix} {message}")
                    return
            except Exception:
                pass
        log.info(f"{prefix} {message}")


async def setup(bot):
    await bot.add_cog(CookieManager(bot))
