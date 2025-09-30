# Cookie Manager Cog (MissionChief)
Owner-only Red Discord Bot cog that logs into MissionChief, stores encrypted credentials and cookies locally, exposes a ready-to-use `aiohttp.ClientSession` to other cogs, and auto-refreshes sessions.

## Features
- DM-based `!cookie setcreds <user> <pass>` stores encrypted credentials (Fernet) on disk (RPI-only).
- Robust login with optional CSRF detection and configurable form field names.
- Cookies saved with common flags (domain/path/expires/secure/httponly/samesite where available).
- Auto-refresh background worker with warnings threshold.
- Admin logging to a configured channel.
- Public API for other cogs: `get_session()`.

## Install
```bash
pip install aiohttp cryptography beautifulsoup4
```
Drop the `cogs/cookie_manager` folder into your Red instance cogs path.

Load the cog:
```
[p]load cookie_manager
```

## Commands (owner only)
- `!cookie setcreds <username> <password>` — in DM only, stores credentials and attempts login
- `!cookie login` — force a login/refresh now
- `!cookie logout` — clear stored cookies
- `!cookie status` — show cookie/meta/config
- `!cookie setadminchannel <channel_id>` — channel for admin logs
- `!cookie config set <key> <value>` — set config:
  - keys: `login_url`, `check_url`, `user_agent`, `auto_refresh_minutes`, `cookie_warn_before_minutes`, `username_field`, `password_field`
  - special: `csrf_field_names` (comma-separated), `extra_form_fields` (JSON)

## Using the session in other cogs
```python
cog = bot.get_cog("CookieManager")
if not cog:
    raise RuntimeError("CookieManager not loaded")
session = await cog.get_session()
r = await session.get("https://www.missionchief.com/alliances/1621")
text = await r.text()
await session.close()
```

## Security
- Credentials are encrypted at rest using a randomly generated Fernet key stored in the cog data path.
- RPI-local only. Do not commit any generated files to GitHub.
- Consider restricting bot to a single guild and owner-only for this cog.
