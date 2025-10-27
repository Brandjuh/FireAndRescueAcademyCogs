"""
Configuration Manager for AllianceReports - V2 DATABASE SUPPORT
Handles all configuration, validation, and database path detection.
Updated for V2 scraper databases structure.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any
from redbot.core import Config
from redbot.core.data_manager import cog_data_path

log = logging.getLogger("red.FARA.AllianceReports.ConfigManager")


class ConfigManager:
    """Manages configuration and database paths for AllianceReports."""
    
    # Default configuration
    DEFAULTS = {
        # Channels
        "daily_member_channel": None,
        "daily_admin_channel": None,
        "monthly_member_channel": None,
        "monthly_admin_channel": None,
        "error_channel": 1422729594103926804,
        
        # Timing (Amsterdam timezone)
        "daily_time": "06:00",
        "monthly_day": 1,
        "monthly_time": "06:00",
        "timezone": "Europe/Amsterdam",
        
        # Report Toggles - Daily Member
        "daily_member_enabled": True,
        "daily_member_sections": {
            "membership": True,
            "training": True,
            "buildings": True,
            "operations": True,
            "treasury": True,
            "activity_score": True,
        },
        
        # Report Toggles - Daily Admin
        "daily_admin_enabled": True,
        "daily_admin_sections": {
            "membership_detailed": True,
            "training_detailed": True,
            "buildings_detailed": True,
            "operations_detailed": True,
            "treasury_detailed": True,
            "sanctions": True,
            "admin_activity": True,
            "action_items": True,
        },
        
        # Report Toggles - Monthly Member
        "monthly_member_enabled": True,
        "monthly_member_sections": {
            "highlights": True,
            "membership": True,
            "training": True,
            "buildings": True,
            "operations": True,
            "treasury": True,
            "achievements": True,
            "comparison": True,
            "fun_facts": True,
            "predictions": True,
        },
        
        # Report Toggles - Monthly Admin
        "monthly_admin_enabled": True,
        "monthly_admin_sections": {
            "executive_summary": True,
            "membership_analysis": True,
            "training_analysis": True,
            "buildings_analysis": True,
            "treasury_analysis": True,
            "sanctions_analysis": True,
            "operations_analysis": True,
            "admin_performance": True,
            "system_health": True,
            "risk_analysis": True,
        },
        
        # Thresholds & Alerts
        "thresholds": {
            "inactive_warning_days": 30,
            "inactive_critical_days": 60,
            "low_contributor_rate": 40.0,
            "response_time_target_hours": 2,
            "treasury_runway_months": 3,
            "sanction_rate_concern": 8.0,
        },
        
        # Activity Score Weights (must sum to 100)
        "activity_weights": {
            "membership": 20,
            "training": 20,
            "buildings": 20,
            "treasury": 20,
            "operations": 20,
        },
        
        # Comparison Settings
        "enable_day_over_day": True,
        "enable_week_over_week": True,
        "enable_month_over_month": True,
        "enable_year_over_year": True,
        
        # Fun Facts Settings
        "fun_facts_enabled": True,
        "fun_facts_count": 5,
        
        # Predictions Settings
        "predictions_enabled": True,
        "prediction_confidence": "medium",
        
        # Database Paths (auto-detected) - V2 STRUCTURE
        "members_v2_db_path": None,      # NEW: members_v2.db
        "logs_v2_db_path": None,         # NEW: logs_v2.db
        "income_v2_db_path": None,       # NEW: income_v2.db
        "buildings_v2_db_path": None,    # NEW: buildings_v2.db
        "alliance_db_path": None,        # KEEP: alliance.db (treasury only)
        "membersync_db_path": None,      # KEEP: membersync.db
        "building_manager_db_path": None,# KEEP: building_manager.db
        "sanctions_db_path": None,       # KEEP: sanctions.db
        
        # Advanced
        "test_mode": False,
        "verbose_logging": False,
        "previous_report_links": True,
        "milestone_alerts": True,
        
        # Admin role for permissions
        "admin_role_id": None,
    }
    
    def __init__(self, config: Config, bot):
        """Initialize ConfigManager."""
        self.config = config
        self.bot = bot
        self._db_cache: Dict[str, Optional[Path]] = {}
    
    @staticmethod
    def get_defaults() -> Dict[str, Any]:
        """Get default configuration."""
        return ConfigManager.DEFAULTS.copy()
    
    async def detect_database_paths(self, instance_name: Optional[str] = None) -> Dict[str, Optional[Path]]:
        """Auto-detect database paths for all required databases (V2 + legacy)."""
        if self._db_cache:
            return self._db_cache
        
        try:
            base_path = Path.home() / ".local" / "share" / "Red-DiscordBot" / "data"
            
            if not base_path.exists():
                log.warning(f"Red-DiscordBot data directory not found: {base_path}")
                return {}
            
            # Determine instance path
            if instance_name:
                instance_path = base_path / instance_name
            else:
                try:
                    current_cog_path = cog_data_path(raw_name="AllianceReports")
                    instance_path = current_cog_path.parent.parent
                except Exception:
                    instances = [d for d in base_path.iterdir() if d.is_dir()]
                    if not instances:
                        log.error("No Red-DiscordBot instances found")
                        return {}
                    instance_path = instances[0]
            
            if not instance_path.exists():
                log.warning(f"Instance path not found: {instance_path}")
                return {}
            
            cogs_path = instance_path / "cogs"
            
            # V2 SCRAPER DATABASES (in scraper_databases/)
            scraper_db_path = cogs_path / "scraper_databases"
            
            db_locations = {
                # V2 Databases
                "members_v2_db_path": scraper_db_path / "members_v2.db",
                "logs_v2_db_path": scraper_db_path / "logs_v2.db",
                "income_v2_db_path": scraper_db_path / "income_v2.db",
                "buildings_v2_db_path": scraper_db_path / "buildings_v2.db",
                
                # Legacy Databases
                "alliance_db_path": cogs_path / "AllianceScraper" / "alliance.db",
                "membersync_db_path": cogs_path / "MemberSync" / "membersync.db",
                "building_manager_db_path": cogs_path / "BuildingManager" / "building_manager.db",
                "sanctions_db_path": cogs_path / "SanctionsManager" / "sanctions.db",
            }
            
            result = {}
            for key, path in db_locations.items():
                if path.exists():
                    result[key] = path
                    log.info(f"Found database: {key} at {path}")
                else:
                    result[key] = None
                    log.warning(f"Database not found: {key} at {path}")
            
            self._db_cache = result
            return result
        
        except Exception as e:
            log.exception(f"Error detecting database paths: {e}")
            return {}
    
    async def get_db_path(self, db_name: str) -> Optional[Path]:
        """Get a specific database path."""
        config_path = await self.config.get_raw(db_name, default=None)
        if config_path:
            path = Path(config_path)
            if path.exists():
                return path
        
        if not self._db_cache:
            await self.detect_database_paths()
        
        return self._db_cache.get(db_name)
    
    async def validate_time_format(self, time_str: str) -> bool:
        """Validate time string format (HH:MM)."""
        try:
            parts = time_str.split(":")
            if len(parts) != 2:
                return False
            
            hour, minute = int(parts[0]), int(parts[1])
            return 0 <= hour <= 23 and 0 <= minute <= 59
        except (ValueError, AttributeError):
            return False
    
    async def get_all_settings(self) -> Dict[str, Any]:
        """Get all current settings."""
        return await self.config.all()
    
    async def reset_to_defaults(self):
        """Reset all settings to defaults."""
        await self.config.clear_all()
        log.info("Configuration reset to defaults")
    
    def format_settings_display(self, settings: Dict[str, Any]) -> str:
        """Format settings for display in Discord."""
        lines = []
        
        lines.append("📢 CHANNELS")
        lines.append(f"  Daily Member: {settings.get('daily_member_channel') or 'Not set'}")
        lines.append(f"  Daily Admin: {settings.get('daily_admin_channel') or 'Not set'}")
        lines.append(f"  Monthly Member: {settings.get('monthly_member_channel') or 'Not set'}")
        lines.append(f"  Monthly Admin: {settings.get('monthly_admin_channel') or 'Not set'}")
        lines.append(f"  Error Channel: {settings.get('error_channel') or 'Not set'}")
        lines.append("")
        
        lines.append("⏰ TIMING")
        lines.append(f"  Daily: {settings.get('daily_time', '06:00')} {settings.get('timezone', 'Europe/Amsterdam')}")
        lines.append(f"  Monthly: Day {settings.get('monthly_day', 1)} at {settings.get('monthly_time', '06:00')}")
        lines.append("")
        
        lines.append("📊 REPORT STATUS")
        lines.append(f"  Daily Member: {'✅ Enabled' if settings.get('daily_member_enabled') else '❌ Disabled'}")
        lines.append(f"  Daily Admin: {'✅ Enabled' if settings.get('daily_admin_enabled') else '❌ Disabled'}")
        lines.append(f"  Monthly Member: {'✅ Enabled' if settings.get('monthly_member_enabled') else '❌ Disabled'}")
        lines.append(f"  Monthly Admin: {'✅ Enabled' if settings.get('monthly_admin_enabled') else '❌ Disabled'}")
        lines.append("")
        
        lines.append("💾 DATABASES (V2)")
        db_paths = {
            "Members V2": settings.get('members_v2_db_path'),
            "Logs V2": settings.get('logs_v2_db_path'),
            "Income V2": settings.get('income_v2_db_path'),
            "Buildings V2": settings.get('buildings_v2_db_path'),
            "Alliance (Treasury)": settings.get('alliance_db_path'),
            "MemberSync": settings.get('membersync_db_path'),
            "BuildingManager": settings.get('building_manager_db_path'),
            "Sanctions": settings.get('sanctions_db_path'),
        }
        for name, path in db_paths.items():
            status = "✅ Found" if path and Path(path).exists() else "❌ Not found"
            lines.append(f"  {name}: {status}")
        lines.append("")
        
        lines.append("⚙️ ADVANCED")
        lines.append(f"  Test Mode: {'✅ On' if settings.get('test_mode') else '❌ Off'}")
        lines.append(f"  Verbose Logging: {'✅ On' if settings.get('verbose_logging') else '❌ Off'}")
        lines.append(f"  Fun Facts: {'✅ On' if settings.get('fun_facts_enabled') else '❌ Off'}")
        lines.append(f"  Predictions: {'✅ On' if settings.get('predictions_enabled') else '❌ Off'}")
        
        return "\n".join(lines)
