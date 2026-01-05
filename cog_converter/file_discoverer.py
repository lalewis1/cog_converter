#!/usr/bin/env python3
"""
File Discovery System for COG Conversion Engine
"""

import os
from typing import List, Set

try:
    from osgeo import gdal
    gdal.UseExceptions()

    GDAL_AVAILABLE = True
except ImportError:
    GDAL_AVAILABLE = False
    print("Warning: GDAL not available - file validation will be limited")


class FileDiscoverer:
    """Discovers raster files in the file system"""

    def __init__(self, config: dict):
        self.config = config
        self.supported_extensions = self._get_supported_extensions()

    def _get_supported_extensions(self) -> Set[str]:
        """Get all supported file extensions from config"""
        extensions = []
        for format_extensions in self.config.get("supported_formats", {}).values():
            extensions.extend(format_extensions)
        return set(extensions)

    def find_raster_files(self, root_path: str) -> List[str]:
        """Find all raster files in the given path"""
        raster_files = []

        if not os.path.exists(root_path):
            print(f"Warning: Path does not exist: {root_path}")
            return raster_files

        for root, dirs, files in os.walk(root_path):
            for file in files:
                file_path = os.path.join(root, file)
                ext = os.path.splitext(file)[1].lower()

                if ext in self.supported_extensions:
                    # Additional verification if needed
                    if self._is_valid_raster(file_path):
                        raster_files.append(file_path)

        return raster_files

    def _is_valid_raster(self, file_path: str) -> bool:
        """Additional validation for raster files"""
        if not GDAL_AVAILABLE:
            # If GDAL not available, assume valid based on extension
            return True

        try:
            # Quick GDAL check
            dataset = gdal.Open(file_path)
            if dataset is None:
                return False
            dataset = None  # Close dataset
            return True
        except Exception:
            return False

    def get_file_stats(self, file_path: str) -> dict:
        """Get basic file statistics"""
        if not GDAL_AVAILABLE:
            return {
                "size": os.path.getsize(file_path),
                "extension": os.path.splitext(file_path)[1].lower(),
            }

        try:
            dataset = gdal.Open(file_path)
            if dataset is None:
                return {"size": os.path.getsize(file_path), "valid": False}

            stats = {
                "size": os.path.getsize(file_path),
                "extension": os.path.splitext(file_path)[1].lower(),
                "driver": dataset.GetDriver().ShortName,
                "bands": dataset.RasterCount,
                "width": dataset.RasterXSize,
                "height": dataset.RasterYSize,
                "valid": True,
            }

            dataset = None  # Close dataset
            return stats
        except Exception as e:
            return {
                "size": os.path.getsize(file_path),
                "extension": os.path.splitext(file_path)[1].lower(),
                "error": str(e),
                "valid": False,
            }
