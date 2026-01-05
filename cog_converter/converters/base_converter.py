#!/usr/bin/env python3
"""
Base Converter Class for COG Conversion Engine
"""

import os
import subprocess
from typing import List, Tuple


class BaseRasterConverter:
    """Base class for all raster format converters"""

    def __init__(self, config: dict):
        self.config = config
        self.temp_dir = config["temp_directory"]
        self.cog_params = config["cog_parameters"]

        # Ensure temp directory exists
        os.makedirs(self.temp_dir, exist_ok=True)

    def can_handle(self, file_path: str) -> bool:
        """Check if this converter can handle the file"""
        raise NotImplementedError("Subclasses must implement can_handle()")

    def convert(self, input_path: str, output_path: str) -> bool:
        """Convert to COG format"""
        raise NotImplementedError("Subclasses must implement convert()")

    def _run_gdal_command(self, command: List[str]) -> Tuple[bool, str]:
        """Helper for running GDAL commands with error handling"""
        try:
            result = subprocess.run(
                command,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            return True, result.stdout
        except subprocess.CalledProcessError as e:
            error_msg = f"GDAL command failed: {' '.join(command)}\n"
            error_msg += f"Error: {e.stderr}"
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error running GDAL command: {str(e)}"
            return False, error_msg

    def _create_temp_file(self, suffix: str = ".tif") -> str:
        """Create a temporary file path"""
        return os.path.join(
            self.temp_dir, f"temp_{os.getpid()}_{os.path.basename(suffix)}"
        )

    def _get_cog_command(self, input_path: str, output_path: str) -> List[str]:
        """Generate GDAL command for COG conversion"""
        cmd = [
            "gdal_translate",
            input_path,
            output_path,
            "-of",
            "COG",
            "-co",
            f'COMPRESS={self.cog_params["compression"]}',
            "-co",
            "TILED=YES",
            "-co",
            f'BLOCKSIZE={self.cog_params["blocksize"]}',
            "-co",
            "BIGTIFF=IF_SAFER",
        ]

        # Add overview parameters if specified
        if self.cog_params.get("overview_levels"):
            cmd.extend(["-co", f'OVERVIEW_LEVELS={self.cog_params["overview_levels"]}'])
        if self.cog_params.get("overview_resampling"):
            cmd.extend(
                ["-co", f'OVERVIEW_RESAMPLING={self.cog_params["overview_resampling"]}']
            )

        return cmd
