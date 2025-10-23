# Verwijder alles na regel 548 (na de run command)
# En voeg alleen dit toe aan het einde:

    @alog_group.command(name="version")
    async def version(self, ctx: commands.Context):
        cfg = await self.config.all()
        lines = [
            "```",
            f"AllianceLogsPub version: {__version__}",
            f"Style: {cfg['style']}  Emoji titles: {cfg['emoji_titles']}",
            f"Max posts per run: {cfg['max_posts_per_run']}",
            f"Mirrors configured: {len(cfg.get('mirrors', {}))}",
            "```",
        ]
        await ctx.send(NL.join(lines))

    @alog_group.command(name="status")
    async def status(self, ctx: commands.Context):
        """Show current status and last processed ID."""
        last_id = await self._get_last_id()
        
        sc = self.bot.get_cog("LogsScraper")
        scraper_available = sc is not None and hasattr(sc, "get_logs_after")
        
        total_logs = "N/A"
        if scraper_available:
            try:
                import sqlite3
                conn = sqlite3.connect(sc.db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*), MAX(id) FROM logs")
                row = cursor.fetchone()
                if row:
                    total_logs = f"{row[0]} (max ID: {row[1]})"
                conn.close()
            except Exception as e:
                total_logs = f"Error: {e}"
        
        posted_count = 0
        try:
            async with aiosqlite.connect(self.db_path) as db:
                cur = await db.execute("SELECT COUNT(*) FROM posted_logs")
                row = await cur.fetchone()
                if row:
                    posted_count = row[0]
        except Exception:
            pass
        
        cfg = await self.config.all()
        
        lines = [
            "```",
            "=== AllianceLogsPub Status ===",
            f"Last processed ID: {last_id}",
            f"Total logs in DB: {total_logs}",
            f"Posted logs tracked: {posted_count}",
            f"Scraper available: {scraper_available}",
            f"Max posts per run: {cfg['max_posts_per_run']}",
            f"Interval: {cfg['interval_minutes']} minutes",
            f"Main channel: {cfg.get('main_channel_id')}",
            "```",
        ]
        await ctx.send("\n".join(lines))

    @alog_group.command(name="setlastid")
    async def setlastid(self, ctx: commands.Context, new_id: int):
        """Manually set the last processed ID (use with caution!)."""
        old_id = await self._get_last_id()
        await self._set_last_id(int(new_id))
        await ctx.send(f"✅ Updated last_id from {old_id} to {new_id}")
        log.info("Manual last_id update: %d -> %d (by %s)", old_id, new_id, ctx.author)

    @alog_group.command(name="setchannel")
    async def setchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the main posting channel."""
        await self.config.main_channel_id.set(int(channel.id))
        await ctx.send(f"✅ Main channel set to {channel.mention}")

    @alog_group.command(name="setinterval")
    async def setinterval(self, ctx: commands.Context, minutes: int):
        """Set posting interval in minutes (min: 1)."""
        minutes = max(1, int(minutes))
        await self.config.interval_minutes.set(minutes)
        await ctx.send(f"✅ Interval set to {minutes} minutes")

    @alog_group.command(name="setmaxposts")
    async def setmaxposts(self, ctx: commands.Context, max_posts: int):
        """Set max posts per run (1-100)."""
        max_posts = max(1, min(100, int(max_posts)))
        await self.config.max_posts_per_run.set(max_posts)
        await ctx.send(f"✅ Max posts per run set to {max_posts}")

    @alog_group.command(name="setstyle")
    async def setstyle(self, ctx: commands.Context, style: str):
        """Set embed style (minimal/compact/fields)."""
        style = style.lower().strip()
        if style not in {"minimal", "compact", "fields"}:
            await ctx.send("❌ Style must be `minimal`, `compact`, or `fields`.")
            return
        await self.config.style.set(style)
        await ctx.send(f"✅ Style set to {style}")

    @alog_group.command(name="mirrors")
    async def list_mirrors(self, ctx: commands.Context):
        """List all configured mirrors."""
        mirrors = await self.config.mirrors()
        if not mirrors:
            await ctx.send("```\nNo mirrors configured.\n```")
            return
        
        lines = []
        for k, v in mirrors.items():
            chans = ", ".join(f"<#{cid}>" for cid in (v.get("channels") or []))
            action_name = DISPLAY.get(k, (k, "", ""))[0]
            enabled = "✅" if v.get("enabled") else "❌"
            lines.append(f"{enabled} **{action_name}** → {chans if chans else '(none)'}")
        
        await ctx.send("\n".join(lines))

    @alog_group.command(name="addmirror")
    async def add_mirror(self, ctx: commands.Context, action: str, channel: discord.TextChannel):
        """Add a mirror channel for a specific action."""
        key = _map_user_action_input(action)
        if not key:
            await ctx.send("❌ Unknown action. Use `!alog listactions` to see valid options.")
            return
        
        mirrors = await self.config.mirrors()
        m = mirrors.get(key, {"enabled": True, "channels": []})
        if int(channel.id) not in m["channels"]:
            m["channels"].append(int(channel.id))
        m["enabled"] = True
        mirrors[key] = m
        await self.config.mirrors.set(mirrors)
        
        action_name = DISPLAY[key][0]
        await ctx.send(f"✅ Mirror added: **{action_name}** → {channel.mention}")


async def setup(bot):
    cog = AllianceLogsPub(bot)
    await bot.add_cog(cog)
