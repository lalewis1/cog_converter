#!/usr/bin/env python3
"""
ECW to COG Converter
Converts ERDAS Imagine ECW files to COG format
"""

import os
from .base_converter import BaseRasterConverter


class EcwToCogConverter(BaseRasterConverter):
    """Converts ECW files to COG format"""

    def can_handle(self, file_path: str) -> bool:
        """Check if this converter can handle the file"""
        ext = os.path.splitext(file_path)[1].lower()
        return ext == ".ecw"

    def convert(self, input_path: str, output_path: str) -> bool:
        """Convert ECW to COG format"""

        # Generate COG conversion command for ECW files
        # ECW files often need special handling due to their proprietary format
        cog_command = self._get_cog_command(input_path, output_path)

        # Add ECW-specific parameters if needed
        # Some ECW files may require additional GDAL configuration
        # For example, setting GDAL_ECW_JPEG_QUALITY environment variable
        env = os.environ.copy()
        env['GDAL_ECW_JPEG_QUALITY'] = '90'  # Set JPEG quality for ECW conversion

        # Run the conversion with the modified environment
        success, message = self._run_gdal_command(cog_command, env=env)

        if not success:
            print(f"ECW conversion failed: {message}")
            return False

        return True