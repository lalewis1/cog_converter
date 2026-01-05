#!/usr/bin/env python3
"""
GeoTIFF to COG Converter
"""

import os

from .base_converter import BaseRasterConverter


class GeoTiffToCogConverter(BaseRasterConverter):
    """Converts GeoTIFF files directly to COG format"""

    def can_handle(self, file_path: str) -> bool:
        """Check if this converter can handle the file"""
        ext = os.path.splitext(file_path)[1].lower()
        return ext in (".tif", ".tiff")

    def convert(self, input_path: str, output_path: str) -> bool:
        """Convert GeoTIFF to COG format"""

        # Generate COG conversion command
        cog_command = self._get_cog_command(input_path, output_path)

        # Run the conversion
        success, message = self._run_gdal_command(cog_command)

        if not success:
            print(f"GeoTIFF conversion failed: {message}")
            return False

        return True
