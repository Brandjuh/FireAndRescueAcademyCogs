def __init__(self, bot, config_manager):
    """Initialize daily admin report generator."""
    self.bot = bot
    self.config = config_manager
    self.aggregator = DataAggregator(config_manager)
    self.calculator = ActivityCalculator()
    self.formatter = EmbedFormatter()

async def generate(self) -> List[discord.Embed]:
    """
    Generate daily admin report.
    
    Returns:
        List of Discord embeds (admin reports can be multiple embeds)
    """
    try:
        tz = ZoneInfo(await self.config.get_timezone())
        now = datetime.now(tz)
        
        # Gather all data
        log.info("Gathering data for daily admin report...")
        
        membership = await self.aggregator.get_membership_data(hours=24)
        training = await self.aggregator.get_training_data(hours=24)
        buildings = await self.aggregator.get_building_data(hours=24)
        treasury = await self.aggregator.get_treasury_data(hours=24)
        operations = await self.aggregator.get_operations_data(hours=24)
        sanctions = await self.aggregator.get_sanctions_data(hours=24)
        
        # Get enabled sections
        sections = await self.config.get_daily_admin_sections()
        
        embeds = []
        
        # Main embed with header
        main_embed = await self._create_header_embed(now)
        embeds.append(main_embed)
        
        # Detailed sections
        if sections.get("membership_detailed", True):
            embeds.append(await self._create_membership_detailed(membership))
        
        if sections.get("training_detailed", True):
            embeds.append(await self._create_training_detailed(training))
        
        if sections.get("buildings_detailed", True):
            embeds.append(await self._create_buildings_detailed(buildings))
        
        if sections.get("operations_detailed", True):
            embeds.append(await self._create_operations_detailed(operations))
        
        if sections.get("treasury_detailed", True):
            embeds.append(await self._create_treasury_detailed(treasury))
        
        if sections.get("sanctions", True):
            embeds.append(await self._create_sanctions_section(sanctions))
        
        if sections.get("admin_activity", True):
            admin_data = await self.aggregator.get_admin_activity(hours=24)
            embeds.append(await self._create_admin_activity(admin_data))
        
        if sections.get("action_items", True):
            embeds.append(await self._create_action_items(
                membership, training, buildings, sanctions
            ))
        
        log.info(f"Generated {len(embeds)} embeds for daily admin report")
        return embeds
        
    except Exception as e:
        log.exception(f"Error generating daily admin report: {e}")
        return [self.formatter.create_error_embed(
            "Daily Admin Report Error",
            f"Failed to generate report: {str(e)}"
        )]

async def _create_header_embed(self, now: datetime) -> discord.Embed:
    """Create header embed with title and timestamp."""
    embed = discord.Embed(
        title="🛡️ ADMIN DAILY BRIEFING",
        description=f"📅 {now.strftime('%A, %B %d, %Y')}",
        color=discord.Color.blue(),
        timestamp=datetime.now()
    )
    
    embed.set_footer(text="Detailed Metrics - Last 24 Hours")
    return embed

async def _create_membership_detailed(self, data: Dict) -> discord.Embed:
    """Create detailed membership section."""
    embed = discord.Embed(
        title="👥 MEMBERSHIP DETAILS",
        color=discord.Color.blue()
    )
    
    # New members breakdown
    new_total = data.get("new_members", 0)
    left = data.get("left_members", 0)
    kicked = data.get("kicked_members", 0)
    net_growth = new_total - left - kicked
    total = data.get("total_members", 0)
    
    growth_pct = (net_growth / total * 100) if total > 0 else 0
    
    new_members_text = (
        f"✅ **New Members:** {new_total}\n"
        f"   • Joined: {new_total} | Left: {left} | Kicked: {kicked}\n"
        f"   • Net growth: {net_growth:+d} ({growth_pct:+.1f}%)"
    )
    embed.add_field(name="Members", value=new_members_text, inline=False)
    
    # Verifications
    verif = data.get("verifications", {})
    approved = verif.get("approved", 0)
    denied = verif.get("denied", 0)
    pending = verif.get("pending", 0)
    avg_time = verif.get("avg_processing_time", 0)
    
    hours = int(avg_time // 3600)
    minutes = int((avg_time % 3600) // 60)
    
    verif_text = (
        f"🔗 **Verifications:**\n"
        f"   • Approved: {approved} | Denied: {denied} | Pending: {pending}\n"
        f"   • Avg processing time: {hours}h {minutes}m"
    )
    embed.add_field(name="Verification System", value=verif_text, inline=False)
    
    # Inactive watch
    inactive = data.get("inactive_members", {})
    inactive_30_60 = inactive.get("30_60_days", 0)
    inactive_60_plus = inactive.get("60_plus_days", 0)
    
    inactive_text = (
        f"📉 **Inactive Watch:**\n"
        f"   • 30-60 days: {inactive_30_60} members\n"
        f"   • 60+ days: {inactive_60_plus} members (prune candidates)"
    )
    embed.add_field(name="Activity Monitoring", value=inactive_text, inline=False)
    
    return embed

async def _create_training_detailed(self, data: Dict) -> discord.Embed:
    """Create detailed training section."""
    embed = discord.Embed(
        title="🎓 EDUCATION SYSTEM",
        color=discord.Color.green()
    )
    
    # Training requests
    submitted = data.get("submitted", 0)
    approved = data.get("approved", 0)
    denied = data.get("denied", 0)
    avg_approval = data.get("avg_approval_time", 0)
    
    hours = int(avg_approval // 3600)
    minutes = int((avg_approval % 3600) // 60)
    
    target_hours = 2
    status = "✅" if hours < target_hours else "⚠️"
    
    requests_text = (
        f"📚 **Training Requests:**\n"
        f"   • Submitted: {submitted} | Approved: {approved} | Denied: {denied}\n"
        f"   • Avg approval time: {hours}h {minutes}m {status}"
    )
    embed.add_field(name="Requests", value=requests_text, inline=False)
    
    # Completions
    completed = data.get("completed", 0)
    reminders = data.get("reminders_sent", 0)
    success_rate = (completed / submitted * 100) if submitted > 0 else 0
    
    completions_text = (
        f"🏁 **Completions:**\n"
        f"   • Finished: {completed} trainings\n"
        f"   • Reminders sent: {reminders}\n"
        f"   • Success rate: {success_rate:.1f}%"
    )
    embed.add_field(name="Progress", value=completions_text, inline=False)
    
    # By discipline
    by_discipline = data.get("by_discipline", {})
    if by_discipline:
        discipline_lines = []
        for disc, stats in sorted(by_discipline.items()):
            started = stats.get("started", 0)
            completed = stats.get("completed", 0)
            discipline_lines.append(f"   • {disc}: {started} started, {completed} completed")
        
        discipline_text = "📋 **By Discipline:**\n" + "\n".join(discipline_lines)
        embed.add_field(name="Breakdown", value=discipline_text, inline=False)
    
    return embed

async def _create_buildings_detailed(self, data: Dict) -> discord.Embed:
    """Create detailed buildings section."""
    embed = discord.Embed(
        title="🏗️ BUILDING MANAGEMENT",
        color=discord.Color.orange()
    )
    
    # Requests
    submitted = data.get("submitted", 0)
    approved = data.get("approved", 0)
    denied = data.get("denied", 0)
    pending = data.get("pending", 0)
    
    approval_rate = (approved / submitted * 100) if submitted > 0 else 0
    rejection_rate = (denied / submitted * 100) if submitted > 0 else 0
    
    avg_review = data.get("avg_review_time", 0)
    minutes = int(avg_review // 60)
    
    requests_text = (
        f"📝 **Requests:**\n"
        f"   • Submitted: {submitted} | Approved: {approved} | Denied: {denied} | Pending: {pending}\n"
        f"   • Avg review time: {minutes}m\n"
        f"   • Rejection rate: {rejection_rate:.1f}%"
    )
    embed.add_field(name="Processing", value=requests_text, inline=False)
    
    # By type
    by_type = data.get("by_type", {})
    if by_type:
        type_lines = []
        for btype, count in sorted(by_type.items(), key=lambda x: x[1], reverse=True):
            type_lines.append(f"   • {btype}: {count} approved")
        
        type_text = "🏥 **By Type:**\n" + "\n".join(type_lines)
        embed.add_field(name="Building Types", value=type_text, inline=False)
    
    # Extensions
    ext_started = data.get("extensions_started", 0)
    ext_completed = data.get("extensions_completed", 0)
    ext_in_progress = data.get("extensions_in_progress", 0)
    
    prev_started = data.get("prev_extensions_started", 0)
    prev_completed = data.get("prev_extensions_completed", 0)
    
    trend_started = ((ext_started - prev_started) / prev_started * 100) if prev_started > 0 else 0
    trend_completed = ((ext_completed - prev_completed) / prev_completed * 100) if prev_completed > 0 else 0
    
    extensions_text = (
        f"🔨 **Extensions:**\n"
        f"   • Started: {ext_started} (trend: {trend_started:+.0f}%)\n"
        f"   • Completed: {ext_completed} (trend: {trend_completed:+.0f}%)\n"
        f"   • In progress: {ext_in_progress} total"
    )
    embed.add_field(name="Expansion Activity", value=extensions_text, inline=False)
    
    return embed

async def _create_operations_detailed(self, data: Dict) -> discord.Embed:
    """Create detailed operations section."""
    embed = discord.Embed(
        title="🎯 ALLIANCE OPERATIONS",
        color=discord.Color.purple()
    )
    
    large_missions = data.get("large_missions", 0)
    events = data.get("alliance_events", 0)
    custom_created = data.get("custom_missions_created", 0)
    custom_removed = data.get("custom_missions_removed", 0)
    
    operations_text = (
        f"• Large missions: {large_missions} started\n"
        f"• Events: {events} started, {data.get('events_completed', 0)} completed\n"
        f"• Custom missions: {custom_created} created, {custom_removed} removed"
    )
    
    embed.description = operations_text
    return embed

async def _create_treasury_detailed(self, data: Dict) -> discord.Embed:
    """Create detailed treasury section."""
    embed = discord.Embed(
        title="💰 TREASURY MANAGEMENT",
        color=discord.Color.gold()
    )
    
    # Income
    contributions = data.get("contributions_24h", 0)
    contributor_count = data.get("contributor_count", 0)
    avg_per_contributor = (contributions / contributor_count) if contributor_count > 0 else 0
    
    income_text = (
        f"💵 **Income (24h):**\n"
        f"   • Contributions: +{contributions:,} credits\n"
        f"   • Member contributions: {contributor_count} members contributed\n"
        f"   • Avg per contributor: {avg_per_contributor:,.0f} credits"
    )
    embed.add_field(name="Income", value=income_text, inline=False)
    
    # Expenses
    expenses = data.get("expenses_24h", 0)
    largest_expense = data.get("largest_expense", 0)
    
    expenses_text = (
        f"💸 **Expenses (24h):**\n"
        f"   • Total spent: -{expenses:,} credits\n"
        f"   • Largest expense: {largest_expense:,}"
    )
    embed.add_field(name="Expenses", value=expenses_text, inline=False)
    
    # Balance analysis
    opening = data.get("opening_balance", 0)
    closing = data.get("current_balance", 0)
    change = closing - opening
    change_pct = (change / opening * 100) if opening > 0 else 0
    
    trend_7d = data.get("trend_7d", 0)
    trend_7d_pct = data.get("trend_7d_pct", 0)
    
    balance_text = (
        f"📈 **Balance Analysis:**\n"
        f"   • Opening: {opening:,} | Closing: {closing:,}\n"
        f"   • Net change: {change:+,} ({change_pct:+.1f}%)\n"
        f"   • 7-day trend: {trend_7d:+,} ({trend_7d_pct:+.1f}%)"
    )
    embed.add_field(name="Balance", value=balance_text, inline=False)
    
    return embed

async def _create_sanctions_section(self, data: Dict) -> discord.Embed:
    """Create sanctions section."""
    embed = discord.Embed(
        title="⚖️ DISCIPLINE & SANCTIONS",
        color=discord.Color.red()
    )
    
    issued = data.get("sanctions_issued", 0)
    
    # By type
    by_type = data.get("by_type", {})
    type_lines = []
    for stype, count in sorted(by_type.items(), key=lambda x: x[1], reverse=True):
        type_lines.append(f"   • {stype}: {count}")
    
    sanctions_text = f"• Sanctions issued: {issued}\n"
    if type_lines:
        sanctions_text += "\n**By Type:**\n" + "\n".join(type_lines)
    
    embed.add_field(name="Summary", value=sanctions_text or "No sanctions issued", inline=False)
    
    # Active warnings
    active = data.get("active_warnings", {})
    first = active.get("first", 0)
    second = active.get("second", 0)
    third = active.get("third", 0)
    
    if first or second or third:
        warnings_text = (
            f"⚠️ **Active Warning Status:**\n"
            f"   • 1st warning: {first} members\n"
            f"   • 2nd warning: {second} members\n"
            f"   • 3rd warning: {third} members {'⚠️ monitor' if third > 0 else ''}"
        )
        embed.add_field(name="Active Warnings", value=warnings_text, inline=False)
    
    return embed

async def _create_admin_activity(self, data: Dict) -> discord.Embed:
    """Create admin activity section."""
    embed = discord.Embed(
        title="📋 ADMIN ACTIVITY",
        color=discord.Color.blurple()
    )
    
    # Total actions
    building_actions = data.get("building_reviews", 0)
    training_actions = data.get("training_approvals", 0)
    verification_actions = data.get("verifications", 0)
    sanction_actions = data.get("sanctions", 0)
    
    total = building_actions + training_actions + verification_actions + sanction_actions
    
    summary_text = (
        f"• Building reviews: {building_actions} actions\n"
        f"• Training approvals: {training_actions} actions\n"
        f"• Verifications: {verification_actions} actions\n"
        f"• Sanctions: {sanction_actions} actions"
    )
    embed.add_field(name=f"Total Actions: {total}", value=summary_text, inline=False)
    
    # Most active admin
    most_active = data.get("most_active_admin")
    if most_active:
        admin_name = most_active.get("name", "Unknown")
        admin_count = most_active.get("count", 0)
        embed.add_field(
            name="Most Active Admin",
            value=f"👤 {admin_name} ({admin_count} actions)",
            inline=False
        )
    
    return embed

async def _create_action_items(
    self,
    membership: Dict,
    training: Dict,
    buildings: Dict,
    sanctions: Dict
) -> discord.Embed:
    """Create action items section."""
    embed = discord.Embed(
        title="🔔 ACTION ITEMS",
        color=discord.Color.red()
    )
    
    action_items = []
    
    # Pending building requests
    pending_buildings = buildings.get("pending", 0)
    if pending_buildings > 0:
        oldest_hours = buildings.get("oldest_pending_hours", 0)
        action_items.append(
            f"⚠️ {pending_buildings} building request(s) pending review (oldest: {oldest_hours:.0f}h)"
        )
    
    # Pending verifications
    pending_verif = membership.get("verifications", {}).get("pending", 0)
    if pending_verif > 0:
        oldest_hours = membership.get("oldest_verification_hours", 0)
        action_items.append(
            f"⚠️ {pending_verif} verification(s) waiting (oldest: {oldest_hours:.0f}h)"
        )
    
    # Third warnings
    third_warnings = sanctions.get("active_warnings", {}).get("third", 0)
    if third_warnings > 0:
        action_items.append(
            f"⚠️ {third_warnings} member(s) on 3rd warning - monitor closely"
        )
    
    if action_items:
        embed.description = "\n".join(action_items)
    else:
        embed.description = "✅ No urgent action items"
        embed.color = discord.Color.green()
    
    return embed

async def post(self, channel: discord.TextChannel) -> bool:
    """
    Generate and post daily admin report to channel.
    
    Args:
        channel: Discord channel to post to
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        embeds = await self.generate()
        
        if not embeds:
            log.error("No embeds generated for daily admin report")
            return False
        
        # Post all embeds
        for embed in embeds:
            await channel.send(embed=embed)
        
        log.info(f"Posted daily admin report to {channel.name} ({len(embeds)} embeds)")
        return True
        
    except discord.Forbidden:
        log.error(f"No permission to post in {channel.name}")
        return False
    except Exception as e:
        log.exception(f"Error posting daily admin report: {e}")
        return False
</artifact>
Nu moeten we de scheduler updaten om ook deze daily admin report te genereren:
<artifact identifier="scheduler-update-phase3" type="application/vnd.ant.code" language="python" title="Scheduler Update - Add Daily Admin Report">
# Voeg deze code toe aan scheduler.py in de _execute_daily_reports method
# Vervang de huidige _execute_daily_reports method met deze:
async def _execute_daily_reports(self):
"""Execute daily reports generation."""
try:
log.info("Executing daily reports...")
    # Get enabled reports
    member_enabled = await self.config.get_daily_member_enabled()
    admin_enabled = await self.config.get_daily_admin_enabled()
    
    if not member_enabled and not admin_enabled:
        log.info("Both daily reports disabled, skipping")
        return
    
    # Get channels
    member_channel_id = await self.config.get_daily_member_channel()
    admin_channel_id = await self.config.get_daily_admin_channel()
    error_channel_id = await self.config.get_error_channel()
    
    # Test mode check
    test_mode = await self.config.get_test_mode()
    
    # Daily Member Report
    if member_enabled and member_channel_id:
        try:
            log.info("Generating daily member report...")
            
            from .templates.daily_member import DailyMemberReport
            report = DailyMemberReport(self.bot, self.config)
            
            if test_mode:
                embed = await report.generate()
                log.info("TEST MODE: Daily member report generated (not posted)")
            else:
                channel = self.bot.get_channel(int(member_channel_id))
                if channel:
                    success = await report.post(channel)
                    if success:
                        log.info("Daily member report posted successfully")
                    else:
                        log.error("Failed to post daily member report")
                else:
                    log.error(f"Daily member channel not found: {member_channel_id}")
                    
        except Exception as e:
            log.exception(f"Error in daily member report: {e}")
            await self._notify_error("Daily Member Report", str(e), error_channel_id)
    
    # Daily Admin Report
    if admin_enabled and admin_channel_id:
        try:
            log.info("Generating daily admin report...")
            
            from .templates.daily_admin import DailyAdminReport
            report = DailyAdminReport(self.bot, self.config)
            
            if test_mode:
                embeds = await report.generate()
                log.info(f"TEST MODE: Daily admin report generated ({len(embeds)} embeds, not posted)")
            else:
                channel = self.bot.get_channel(int(admin_channel_id))
                if channel:
                    success = await report.post(channel)
                    if success:
                        log.info("Daily admin report posted successfully")
                    else:
                        log.error("Failed to post daily admin report")
                else:
                    log.error(f"Daily admin channel not found: {admin_channel_id}")
                    
        except Exception as e:
            log.exception(f"Error in daily admin report: {e}")
            await self._notify_error("Daily Admin Report", str(e), error_channel_id)
    
    log.info("Daily reports execution completed")
    
except Exception as e:
    log.exception(f"Critical error in daily reports execution: {e}")
</artifact>
En we moeten ook een test command toevoegen:
<artifact identifier="alliance-reports-test-command" type="application/vnd.ant.code" language="python" title="Add Test Command for Daily Admin">
# Voeg deze command toe aan alliance_reports.py in de report_group
@report_group.command(name="dailyadmin")
async def report_daily_admin(self, ctx: commands.Context):
"""Generate daily admin report now."""
if not await self._is_authorized(ctx):
await ctx.send("❌ You don't have permission to use this command.")
return
await ctx.send("🔄 Generating daily admin report...")

try:
    from .templates.daily_admin import DailyAdminReport
    
    # Create report generator
    report_gen = DailyAdminReport(self.bot, self.config_manager)
    
    # Generate embeds
    embeds = await report_gen.generate()
    
    if not embeds:
        await ctx.send("❌ Failed to generate report")
        return
    
    # Check if channel is configured
    channel_id = await self.config.daily_admin_channel()
    if not channel_id:
        # Post in current channel as test
        await ctx.send("ℹ️ No channel configured, posting here:")
        for embed in embeds:
            await ctx.send(embed=embed)
        await ctx.send("✅ Set channel with `[p]reportset channel dailyadmin #channel`")
        return
    
    # Get configured channel
    channel = self.bot.get_channel(int(channel_id))
    if not channel:
        await ctx.send(f"❌ Configured channel not found (ID: {channel_id})")
        return
    
    # Post to configured channel
    success = await report_gen.post(channel)
    
    if success:
        await ctx.send(f"✅ Daily admin report posted to {channel.mention} ({len(embeds)} embeds)")
    else:
        await ctx.send("❌ Failed to post report (check logs)")

except Exception as e:
    log.exception(f"Error generating daily admin report: {e}")
    await ctx.send(f"❌ Error: {e}")
