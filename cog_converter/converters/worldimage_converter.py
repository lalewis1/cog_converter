#!/usr/bin/env python3
"""
World Image to COG Converter
Handles JPG/PNG with world files (.jpw, .pgw, etc.)
"""

import os

from .base_converter import BaseRasterConverter
from .geotiff_converter import GeoTiffToCogConverter


class WorldImageToCogConverter(BaseRasterConverter):
    """Converts world images (JPG/PNG + world files) to COG format"""

    def can_handle(self, file_path: str) -> bool:
        """Check if this converter can handle the file"""
        ext = os.path.splitext(file_path)[1].lower()
        return ext in (".jpg", ".jpeg", ".png") and self._has_world_file(file_path)

    def _has_world_file(self, image_path: str) -> bool:
        """Check for corresponding world file"""
        world_extensions = {".jpg": ".jpw", ".jpeg": ".jpw", ".png": ".pgw"}
        base_path = os.path.splitext(image_path)[0]
        expected_world_file = base_path + world_extensions.get(
            os.path.splitext(image_path)[1].lower(), ""
        )
        return os.path.exists(expected_world_file)

    def _get_world_file_path(self, image_path: str) -> str:
        """Get path to corresponding world file"""
        world_extensions = {".jpg": ".jpw", ".jpeg": ".jpw", ".png": ".pgw"}
        base_path = os.path.splitext(image_path)[0]
        return base_path + world_extensions.get(
            os.path.splitext(image_path)[1].lower(), ""
        )

    def convert(self, input_path: str, output_path: str) -> bool:
        """Convert world image to COG format"""

        # Create temporary GeoTIFF first
        temp_geotiff = os.path.join(
            self.temp_dir, f"temp_{os.path.basename(input_path)}.tif"
        )

        # Step 1: Create GeoTIFF from world image
        world_file = self._get_world_file_path(input_path)
        geotiff_command = ["gdal_translate", world_file, temp_geotiff, "-co", "TFW=YES"]

        success, message = self._run_gdal_command(geotiff_command)
        if not success:
            print(f"World image to GeoTIFF conversion failed: {message}")
            return False

        # Step 2: Convert GeoTIFF to COG
        cog_success = GeoTiffToCogConverter(self.config).convert(
            temp_geotiff, output_path
        )

        # Clean up temporary file
        try:
            if os.path.exists(temp_geotiff):
                os.remove(temp_geotiff)
        except Exception:
            pass  # Ignore cleanup errors

        return cog_success
