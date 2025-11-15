"""
Color presets for IconGen
"""

COLOR_PRESETS = {
    # Fire & Rescue
    "fire": {
        "color": "#DC2626",
        "name": "Fire (Red)",
        "emergency_glow": "#EF4444"
    },
    "rescue": {
        "color": "#F59E0B",
        "name": "Rescue (Orange)",
        "emergency_glow": "#FBBF24"
    },
    
    # Police
    "police": {
        "color": "#2563EB",
        "name": "Police (Blue)",
        "emergency_glow": "#3B82F6"
    },
    
    # EMS
    "ems": {
        "color": "#F59E0B",
        "name": "EMS (Yellow-Orange)",
        "emergency_glow": "#FBBF24"
    },
    
    # Coastal
    "coastal": {
        "color": "#F97316",
        "name": "Coastal (Orange)",
        "emergency_glow": "#FB923C"
    },
    
    # Forestry
    "forestry": {
        "color": "#059669",
        "name": "Forestry (Green)",
        "emergency_glow": "#10B981"
    },
    
    # Federal / FBI
    "federal": {
        "color": "#1F2937",
        "name": "Federal (Black)",
        "emergency_glow": "#374151"
    },
    "fbi": {
        "color": "#1F2937",
        "name": "FBI (Black)",
        "emergency_glow": "#374151"
    },
    
    # Tow
    "tow": {
        "color": "#6B7280",
        "name": "Tow (Grey)",
        "emergency_glow": "#9CA3AF"
    },
    
    # Additional useful colors
    "medical": {
        "color": "#DC2626",
        "name": "Medical (Red)",
        "emergency_glow": "#EF4444"
    },
    "sheriff": {
        "color": "#92400E",
        "name": "Sheriff (Brown)",
        "emergency_glow": "#B45309"
    },
    "swat": {
        "color": "#1E3A8A",
        "name": "SWAT (Dark Blue)",
        "emergency_glow": "#1E40AF"
    }
}

def get_preset(preset_name: str) -> dict:
    """Get a color preset by name (case-insensitive)"""
    return COLOR_PRESETS.get(preset_name.lower())

def get_all_presets() -> dict:
    """Get all available color presets"""
    return COLOR_PRESETS

def is_valid_preset(preset_name: str) -> bool:
    """Check if a preset name exists"""
    return preset_name.lower() in COLOR_PRESETS

def hex_to_rgb(hex_color: str) -> tuple:
    """Convert hex color to RGB tuple"""
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
