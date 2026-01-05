#!/usr/bin/env python3
"""
Advanced Conversion Pipeline with Blob Storage Integration
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

from .converters.geotiff_converter import GeoTiffToCogConverter
from .converters.worldimage_converter import WorldImageToCogConverter
from .error_handler import ErrorHandler
from .storage.blob_uploader import BlobStorageUploader, MockBlobStorageUploader
from .storage.metadata_manager import ConversionMetadataManager


class AdvancedConversionPipeline:
    """
    Enhanced conversion pipeline with blob storage upload and metadata tracking.
    """

    def __init__(self, config: dict):
        self.config = config
        self.converters = self._initialize_converters()
        self.error_handler = ErrorHandler(config["error_handling"])
        self.stats = self._initialize_stats()

        # Setup logging
        self.logger = logging.getLogger(__name__)

        # Initialize storage components
        self.storage_enabled = config.get("storage", {}).get("enabled", False)
        self.uploader = None
        self.metadata_manager = None
        self._initialize_storage()

    def _initialize_converters(self) -> List[Any]:
        """Initialize all available converters"""
        return [
            GeoTiffToCogConverter(self.config),
            WorldImageToCogConverter(self.config),
            # Add more converters here as needed
        ]

    def _initialize_stats(self) -> Dict[str, int]:
        """Initialize statistics counters"""
        return {
            "total_files": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "retries": 0,
            "uploaded": 0,
            "upload_failed": 0,
        }

    def _initialize_storage(self):
        """Initialize storage components if enabled"""
        if not self.storage_enabled:
            self.logger.info("Storage integration disabled")
            return

        storage_config = self.config.get("storage", {})
        provider = storage_config.get("provider", "azure")

        try:
            if provider == "azure":
                connection_string = storage_config.get("azure_connection_string", "")
                container_name = storage_config.get("container_name", "cog-conversions")

                if connection_string:
                    self.uploader = BlobStorageUploader(
                        connection_string=connection_string,
                        container_name=container_name,
                    )
                else:
                    self.logger.warning(
                        "Azure connection string not provided, using mock uploader"
                    )
                    self.uploader = MockBlobStorageUploader(
                        container_name=container_name
                    )
            else:
                self.logger.warning(
                    f"Provider '{provider}' not implemented, using mock uploader"
                )
                self.uploader = MockBlobStorageUploader()

            # Initialize metadata manager
            metadata_file = storage_config.get(
                "metadata_file", "conversion_metadata.json"
            )
            self.metadata_manager = ConversionMetadataManager(metadata_file)
            self.logger.info(f"Storage integration enabled with {provider} provider")

        except Exception as e:
            self.logger.error(f"Failed to initialize storage: {str(e)}")
            self.storage_enabled = False

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
        conversion_success = False
        cog_file_path = None

        while attempt <= self.error_handler.max_retries:
            try:
                # Perform conversion
                success = converter.convert(file_path, output_path)

                if success:
                    conversion_success = True
                    cog_file_path = output_path
                    self.stats["successful"] += 1
                    self.error_handler.log_success(file_path, output_path)
                    break
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

        # If conversion was successful and storage is enabled, upload to blob storage
        if conversion_success and self.storage_enabled and cog_file_path:
            return self._handle_post_conversion(file_path, cog_file_path)

        return conversion_success

    def _handle_post_conversion(
        self, original_file_path: str, cog_file_path: str
    ) -> bool:
        """
        Handle post-conversion tasks: upload to blob storage and record metadata.

        Args:
            original_file_path: Path to original file
            cog_file_path: Path to converted COG file

        Returns:
            True if upload and metadata recording succeeded, False otherwise
        """
        if not self.uploader or not self.metadata_manager:
            self.logger.error("Storage components not initialized")
            return False

        try:
            # Upload to blob storage with metadata
            upload_result = self.uploader.upload_with_metadata(
                local_file_path=cog_file_path, original_file_path=original_file_path
            )

            self.stats["uploaded"] += 1
            self.logger.info(
                f"Uploaded {cog_file_path} to {upload_result['blob_path']}"
            )

            # Record conversion in metadata
            self.metadata_manager.create_conversion_record_from_upload(
                original_file_path=original_file_path, upload_result=upload_result
            )

            # Optionally clean up local COG file
            storage_config = self.config.get("storage", {})
            if not storage_config.get("preserve_local_cogs", False):
                try:
                    os.remove(cog_file_path)
                    self.logger.debug(f"Removed local COG file: {cog_file_path}")
                except Exception as e:
                    self.logger.warning(f"Could not remove local COG file: {str(e)}")

            return True

        except Exception as e:
            self.stats["upload_failed"] += 1
            self.logger.error(f"Failed to upload {cog_file_path}: {str(e)}")

            # Record failed upload
            if self.metadata_manager:
                self.metadata_manager.add_failed_conversion(
                    original_file_path=original_file_path,
                    error_message=str(e),
                    error_type="upload_failure",
                )

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

    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage-specific statistics"""
        return {
            "uploaded_files": self.stats.get("uploaded", 0),
            "upload_failed": self.stats.get("upload_failed", 0),
        }

    def reset_stats(self):
        """Reset statistics counters"""
        self.stats = self._initialize_stats()

    def get_metadata_manager(self) -> Optional[ConversionMetadataManager]:
        """Get the metadata manager instance"""
        return self.metadata_manager

    def get_uploader(self) -> Optional[BlobStorageUploader]:
        """Get the blob storage uploader instance"""
        return self.uploader


class HybridConversionPipeline(AdvancedConversionPipeline):
    """
    Hybrid pipeline that can work with or without storage integration.
    Falls back gracefully if storage is not available.
    """

    def _handle_post_conversion(
        self, original_file_path: str, cog_file_path: str
    ) -> bool:
        """
        Handle post-conversion with graceful fallback if storage fails.
        """
        if not self.storage_enabled:
            self.logger.info("Storage disabled, skipping upload")
            return True

        try:
            return super()._handle_post_conversion(original_file_path, cog_file_path)
        except Exception as e:
            self.logger.error(
                f"Storage operation failed, but conversion succeeded: {str(e)}"
            )
            # Still consider the overall operation successful if conversion worked
            return True
