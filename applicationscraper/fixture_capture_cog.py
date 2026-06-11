from datetime import datetime
from pathlib import Path

import discord
from redbot.core import commands, data_manager

from .fixture_capture import sanitize_applications_fixture


class ApplicationsFixtureCapture(commands.Cog):
    """Owner-only helper for capturing local ApplicationsScraper fixtures."""

    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="capture_applications_fixture")
    @commands.is_owner()
    async def capture_applications_fixture(self, ctx):
        """Capture private raw HTML and a sanitized review fixture locally."""
        applications_scraper = self.bot.get_cog("ApplicationsScraper")
        cookie_manager = self.bot.get_cog("CookieManager")
        if not applications_scraper or not cookie_manager:
            await ctx.send(
                "Fixture capture failed: ApplicationsScraper and CookieManager must be loaded."
            )
            return

        try:
            session = await cookie_manager.get_session()
            async with session.get(
                applications_scraper.applications_url,
                allow_redirects=True,
            ) as response:
                if response.status != 200:
                    await ctx.send(f"Fixture capture failed: HTTP status {response.status}.")
                    return
                html = await response.text()
                final_url = str(response.url)
        except Exception as exc:
            await ctx.send(f"Fixture capture failed: {exc}")
            return

        login_failure_fragments = await cookie_manager.config.login_failure_url_contains()
        if any(fragment in final_url for fragment in login_failure_fragments):
            await ctx.send(
                f"Fixture capture failed: MissionChief redirected to a login page ({final_url})."
            )
            return

        capture_dir = Path(
            data_manager.cog_data_path(raw_name="applicationscraper_fixture_captures")
        )
        capture_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        raw_path = capture_dir / f"applications-{timestamp}.raw-private.html"
        sanitized_path = capture_dir / f"applications-{timestamp}.sanitized-review.html"

        raw_path.write_text(html, encoding="utf-8")
        sanitized_path.write_text(sanitize_applications_fixture(html), encoding="utf-8")

        await ctx.send(
            "Fixture capture completed.\n"
            f"Private raw file: `{raw_path}`\n"
            "The sanitized review file is attached. Review it manually before sharing or "
            "committing it. The private raw file remains local and is not attached.",
            file=discord.File(
                sanitized_path,
                filename=sanitized_path.name,
            ),
        )
