#!/usr/bin/env python3
"""
Configuration Management for COG Conversion Engine
"""

import json
import os
from typing import Any, Dict, Optional


class ConfigurationManager:
    """Manages configuration for the COG conversion engine"""

    DEFAULT_CONFIG = {
        "temp_directory": "/tmp/cog_conversion",
        "output_directory": "./cog_output",
        "cog_parameters": {
            "compression": "LZW",
            "blocksize": "512",
            "overview_resampling": "average",
            "overview_levels": "auto",
        },
        "supported_formats": {
            "geotiff": [".tif", ".tiff"],
            "worldimage": [".jpg", ".jpeg", ".png"],
            "grid": [".adf", ".bil", ".bip", ".bsq"],
            "ecw": [".ecw"],
        },
        "error_handling": {
            "max_retries": 3,
            "retry_delay": 5,
            "error_log": "conversion_errors.log",
        },
        "performance": {"max_workers": 4, "memory_limit_mb": 4096},
        "storage": {
            "enabled": False,
            "provider": "azure",  # azure, aws, gcp, or local
            "azure_connection_string": "",
            "container_name": "cog-conversions",
            "metadata_file": "./conversion_metadata.json",
            "upload_successful_only": True,
            "preserve_local_cogs": False,
        },
    }

    def __init__(self, config_file: Optional[str] = None):
        self.config = self.DEFAULT_CONFIG.copy()
        if config_file:
            self.load_config(config_file)

        # Ensure directories exist
        self._ensure_directories()

    def load_config(self, config_file: str):
        """Load configuration from JSON file"""
        try:
            with open(config_file, "r") as f:
                file_config = json.load(f)
            self._deep_update(self.config, file_config)
        except Exception as e:
            print(f"Warning: Could not load config file: {e}")

    def _deep_update(self, original: Dict, update: Dict):
        """Recursively update dictionary"""
        for key, value in update.items():
            if isinstance(value, dict) and key in original:
                self._deep_update(original[key], value)
            else:
                original[key] = value

    def _ensure_directories(self):
        """Ensure required directories exist"""
        directories = [self.config["temp_directory"], self.config["output_directory"]]

        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with dot notation"""
        keys = key.split(".")
        current = self.config
        for k in keys:
            if k in current:
                current = current[k]
            else:
                return default
        return current

    def save_config(self, config_file: str):
        """Save current configuration to file"""
        try:
            with open(config_file, "w") as f:
                json.dump(self.config, f, indent=2)
            return True
        except Exception as e:
            print(f"Error saving config: {e}")
            return False
