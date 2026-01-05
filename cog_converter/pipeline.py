#!/usr/bin/env python3
"""
Conversion Pipeline for COG Conversion Engine
"""

import os
import time
from typing import Any, Dict, List

from .converters.geotiff_converter import GeoTiffToCogConverter
from .converters.worldimage_converter import WorldImageToCogConverter
from .error_handler import ErrorHandler


class ConversionPipeline:
    """Manages the conversion process from discovery to final output"""

    def __init__(self, config: dict):
        self.config = config
        self.converters = self._initialize_converters()
        self.error_handler = ErrorHandler(config["error_handling"])
        self.stats = self._initialize_stats()

    def _initialize_converters(self) -> List[Any]:
        """Initialize all available converters"""
        return [
            GeoTiffToCogConverter(self.config),
            WorldImageToCogConverter(self.config),
            # Add more converters here as needed
            # GridToCogConverter(self.config),
            # EcwToCogConverter(self.config)
        ]

    def _initialize_stats(self) -> Dict[str, int]:
        """Initialize statistics counters"""
        return {
            "total_files": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "retries": 0,
        }

    def process_file(self, file_path: str) -> bool:
        """Process a single file through the conversion pipeline"""
        self.stats["total_files"] += 1

        # Find appropriate converter
        converter = None
        for conv in self.converters:
            if conv.can_handle(file_path):
                converter = conv
                break

        if converter is None:
            self.stats["skipped"] += 1
            self.error_handler.log_skip(file_path, "No suitable converter found")
            return False

        # Generate output path
        output_path = self._generate_output_path(file_path)

        # Create output directory if needed
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        # Process with retries
        attempt = 0
        while attempt <= self.error_handler.max_retries:
            try:
                # Perform conversion
                success = converter.convert(file_path, output_path)

                if success:
                    self.stats["successful"] += 1
                    self.error_handler.log_success(file_path, output_path)
                    return True
                else:
                    self.stats["failed"] += 1
                    self.error_handler.log_failure(file_path, "Conversion failed")
                    return False

            except Exception as e:
                attempt += 1
                if attempt <= self.error_handler.max_retries:
                    self.stats["retries"] += 1
                    self.error_handler.log_retry(file_path, attempt, str(e))
                    time.sleep(self.error_handler.get_retry_delay())
                else:
                    self.stats["failed"] += 1
                    self.error_handler.log_exception(file_path, e)
                    return False

        return False

    def _generate_output_path(self, input_path: str) -> str:
        """Generate output path while preserving directory structure"""
        # Get relative path from input directory
        input_dir = self.config.get("input_directory", "")
        if input_dir and input_path.startswith(input_dir):
            relative_path = os.path.relpath(input_path, input_dir)
        else:
            relative_path = os.path.basename(input_path)

        # Create output path
        output_dir = self.config["output_directory"]
        output_filename = os.path.splitext(relative_path)[0] + ".tif"

        return os.path.join(output_dir, output_filename)

    def get_stats(self) -> Dict[str, int]:
        """Get current statistics"""
        return self.stats.copy()

    def reset_stats(self):
        """Reset statistics counters"""
        self.stats = self._initialize_stats()
