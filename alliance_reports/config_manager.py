"""
Configuration Manager for AllianceReports
Handles all configuration, validation, and database path detection.
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
        
        # Database Paths (auto-detected)
        "alliance_db_path": None,
        "membersync_db_path": None,
        "building_db_path": None,
        "sanctions_db_path": None,
        
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
        """
        Auto-detect database paths for all required cogs.
        
        Args:
            instance_name: Bot instance name (optional, will detect if None)
        
        Returns:
            Dictionary with database paths
        """
        if self._db_cache:
            return self._db_cache
        
        try:
            # Get Red-DiscordBot data directory
            base_path = Path.home() / ".local" / "share" / "Red-DiscordBot" / "data"
            
            if not base_path.exists():
                log.warning(f"Red-DiscordBot data directory not found: {base_path}")
                return {}
            
            # Find instance directory
            if instance_name:
                instance_path = base_path / instance_name
            else:
                # Try to detect from current cog data path
                try:
                    current_cog_path = cog_data_path(raw_name="AllianceReports")
                    instance_path = current_cog_path.parent.parent
                except Exception:
                    # Fallback: use first instance found
                    instances = [d for d in base_path.iterdir() if d.is_dir()]
                    if not instances:
                        log.error("No Red-DiscordBot instances found")
                        return {}
                    instance_path = instances[0]
            
            if not instance_path.exists():
                log.warning(f"Instance path not found: {instance_path}")
                return {}
            
            cogs_path = instance_path / "cogs"
            
            # Define expected database locations
            db_locations = {
                "alliance_db_path": cogs_path / "AllianceScraper" / "alliance.db",
                "membersync_db_path": cogs_path / "MemberSync" / "membersync.db",
                "building_db_path": cogs_path / "BuildingManager" / "building_manager.db",
                "sanctions_db_path": cogs_path / "SanctionsManager" / "sanctions.db",
            }
            
            # Verify and cache paths
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
        """
        Get a specific database path.
        
        Args:
            db_name: Name of database (alliance_db_path, membersync_db_path, etc.)
        
        Returns:
            Path to database or None if not found
        """
        # Check config override first
        config_path = await self.config.get_raw(db_name, default=None)
        if config_path:
            path = Path(config_path)
            if path.exists():
                return path
        
        # Auto-detect
        if not self._db_cache:
            await self.detect_database_paths()
        
        return self._db_cache.get(db_name)
    
    async def validate_channels(self, guild) -> Dict[str, bool]:
        """
        Validate that configured channels exist and are accessible.
        
        Returns:
            Dictionary with validation results
        """
        results = {}
        channel_keys = [
            "daily_member_channel",
            "daily_admin_channel", 
            "monthly_member_channel",
            "monthly_admin_channel",
            "error_channel",
        ]
        
        for key in channel_keys:
            channel_id = await self.config.get_raw(key, default=None)
            if channel_id is None:
                results[key] = False
                continue
            
            channel = guild.get_channel(int(channel_id))
            results[key] = channel is not None
        
        return results
    
    async def validate_time_format(self, time_str: str) -> bool:
        """
        Validate time string format (HH:MM).
        
        Args:
            time_str: Time string to validate
        
        Returns:
            True if valid, False otherwise
        """
        try:
            parts = time_str.split(":")
            if len(parts) != 2:
                return False
            
            hour, minute = int(parts[0]), int(parts[1])
            return 0 <= hour <= 23 and 0 <= minute <= 59
        except (ValueError, AttributeError):
            return False
    
    async def validate_activity_weights(self, weights: Dict[str, int]) -> bool:
        """
        Validate that activity weights sum to 100.
        
        Args:
            weights: Dictionary of activity weights
        
        Returns:
            True if valid (sum = 100), False otherwise
        """
        try:
            total = sum(weights.values())
            return total == 100
        except (TypeError, AttributeError):
            return False
    
    async def get_all_settings(self) -> Dict[str, Any]:
        """Get all current settings."""
        return await self.config.all()
    
    async def reset_to_defaults(self):
        """Reset all settings to defaults."""
        await self.config.clear_all()
        log.info("Configuration reset to defaults")
    
    def format_settings_display(self, settings: Dict[str, Any]) -> str:
        """
        Format settings for display in Discord.
        
        Args:
            settings: Dictionary of settings
        
        Returns:
            Formatted string for display
        """
        lines = []
        
        # Channels
        lines.append("📢 CHANNELS")
        lines.append(f"  Daily Member: {settings.get('daily_member_channel') or 'Not set'}")
        lines.append(f"  Daily Admin: {settings.get('daily_admin_channel') or 'Not set'}")
        lines.append(f"  Monthly Member: {settings.get('monthly_member_channel') or 'Not set'}")
        lines.append(f"  Monthly Admin: {settings.get('monthly_admin_channel') or 'Not set'}")
        lines.append(f"  Error Channel: {settings.get('error_channel') or 'Not set'}")
        lines.append("")
        
        # Timing
        lines.append("⏰ TIMING")
        lines.append(f"  Daily: {settings.get('daily_time', '06:00')} {settings.get('timezone', 'Europe/Amsterdam')}")
        lines.append(f"  Monthly: Day {settings.get('monthly_day', 1)} at {settings.get('monthly_time', '06:00')}")
        lines.append("")
        
        # Report Status
        lines.append("📊 REPORT STATUS")
        lines.append(f"  Daily Member: {'✅ Enabled' if settings.get('daily_member_enabled') else '❌ Disabled'}")
        lines.append(f"  Daily Admin: {'✅ Enabled' if settings.get('daily_admin_enabled') else '❌ Disabled'}")
        lines.append(f"  Monthly Member: {'✅ Enabled' if settings.get('monthly_member_enabled') else '❌ Disabled'}")
        lines.append(f"  Monthly Admin: {'✅ Enabled' if settings.get('monthly_admin_enabled') else '❌ Disabled'}")
        lines.append("")
        
        # Database Status
        lines.append("💾 DATABASES")
        db_paths = {
            "Alliance": settings.get('alliance_db_path'),
            "MemberSync": settings.get('membersync_db_path'),
            "Building": settings.get('building_db_path'),
            "Sanctions": settings.get('sanctions_db_path'),
        }
        for name, path in db_paths.items():
            status = "✅ Found" if path and Path(path).exists() else "❌ Not found"
            lines.append(f"  {name}: {status}")
        lines.append("")
        
        # Advanced
        lines.append("⚙️ ADVANCED")
        lines.append(f"  Test Mode: {'✅ On' if settings.get('test_mode') else '❌ Off'}")
        lines.append(f"  Verbose Logging: {'✅ On' if settings.get('verbose_logging') else '❌ Off'}")
        lines.append(f"  Fun Facts: {'✅ On' if settings.get('fun_facts_enabled') else '❌ Off'}")
        lines.append(f"  Predictions: {'✅ On' if settings.get('predictions_enabled') else '❌ Off'}")
        
        return "\n".join(lines)
