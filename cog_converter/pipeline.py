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
from .storage.hash_utils import calculate_content_hash
from .storage.sqlite_metadata_manager import SQLiteMetadataManager


class ConversionPipeline:
    """
    Conversion pipeline with blob storage upload and metadata tracking.
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
        self._initialize_storage()

        # Initialize metadata manager separately
        self.metadata_enabled = config.get("metadata", {}).get("enabled", False)
        self.metadata_manager = None
        self._initialize_metadata_manager()

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
            "duplicates_referenced": 0,
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

        except Exception as e:
            self.logger.error(f"Failed to initialize storage: {str(e)}")
            self.storage_enabled = False

    def _initialize_metadata_manager(self):
        """Initialize metadata manager if enabled"""
        if not self.metadata_enabled:
            self.logger.info("Metadata tracking disabled")
            return

        try:
            metadata_config = self.config.get("metadata", {})
            database_file = metadata_config.get(
                "database_file", "conversion_metadata.db"
            )
            self.metadata_manager = SQLiteMetadataManager(database_file)
            self.logger.info(f"SQLite metadata database initialized: {database_file}")

        except Exception as e:
            self.logger.error(f"Failed to initialize metadata manager: {str(e)}")
            self.metadata_enabled = False

    def process_file(self, file_path: str, run_id: Optional[int] = None) -> bool:
        """Process a single file through the conversion pipeline"""
        self.stats["total_files"] += 1

        # Check for duplicates before processing
        if self._should_skip_duplicate(file_path, run_id):
            return False

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

                    # Mark file as failed in metadata if available
                    if self.metadata_manager:
                        self.metadata_manager.mark_file_failed(file_path, str(e))

                    return False

        # If conversion was successful and storage is enabled, upload to blob storage
        if conversion_success and self.storage_enabled and cog_file_path:
            return self._handle_post_conversion(file_path, cog_file_path, run_id)
        elif conversion_success and self.metadata_enabled and cog_file_path:
            # If only metadata is enabled (no storage), still record the conversion
            return self._handle_metadata_only(file_path, cog_file_path, run_id)

        return conversion_success

    def _should_skip_duplicate(
        self, file_path: str, run_id: Optional[int] = None
    ) -> bool:
        """
        Check if a file should be skipped due to duplicate content.

        Args:
            file_path: Path to the file to check
            run_id: Optional run ID for tracking

        Returns:
            True if file should be skipped, False otherwise
        """
        if not self.metadata_enabled or not self.metadata_manager:
            return False

        # Get duplicate detection configuration
        detect_duplicates = self.config.get("processing", {}).get(
            "detect_duplicates", True
        )
        duplicate_strategy = self.config.get("processing", {}).get(
            "duplicate_strategy", "skip"
        )

        if not detect_duplicates:
            return False

        try:
            # Calculate content hash
            content_hash = calculate_content_hash(file_path)

            # Check if this content already exists
            if self.metadata_manager.is_duplicate_content(content_hash, file_path):
                if duplicate_strategy == "reference":
                    # Create reference to existing blob
                    success = self.metadata_manager.handle_duplicate_file(
                        file_path,
                        content_hash,
                        duplicate_strategy,
                        run_id=run_id,
                    )
                    if success:
                        self.stats["duplicates_referenced"] = (
                            self.stats.get("duplicates_referenced", 0) + 1
                        )
                        self.logger.info(
                            f"Skipped duplicate file {file_path} - referenced existing blob"
                        )
                        return True
                elif duplicate_strategy == "skip":
                    # Just skip the file
                    self.stats["skipped"] += 1
                    self.logger.info(f"Skipped duplicate file {file_path}")
                    return True
                elif duplicate_strategy == "warn":
                    self.logger.warning(
                        f"Duplicate content detected for {file_path} - processing anyway"
                    )
                # For "process" strategy, continue processing

        except Exception as e:
            self.logger.warning(
                f"Could not check for duplicates for {file_path}: {str(e)}"
            )

        return False

    def _handle_post_conversion(
        self, original_file_path: str, cog_file_path: str, run_id: Optional[int] = None
    ) -> bool:
        """
        Handle post-conversion tasks: upload to blob storage and record metadata.

        Args:
            original_file_path: Path to original file
            cog_file_path: Path to converted COG file
            run_id: Optional run ID for tracking

        Returns:
            True if upload and metadata recording succeeded, False otherwise
        """
        if (
            not self.storage_enabled
            or not self.uploader
            or not self.metadata_enabled
            or not self.metadata_manager
        ):
            self.logger.error("Storage components not initialized")
            return False

        try:
            # Check for duplicates before uploading and recording
            if self._should_handle_duplicate_after_conversion(
                original_file_path, cog_file_path, run_id
            ):
                return True

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
                original_file_path=original_file_path,
                upload_result=upload_result,
                run_id=run_id,
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

    def _handle_metadata_only(
        self, original_file_path: str, cog_file_path: str, run_id: Optional[int] = None
    ) -> bool:
        """
        Handle post-conversion tasks when only metadata tracking is enabled (no storage).

        Args:
            original_file_path: Path to original file
            cog_file_path: Path to converted COG file
            run_id: Optional run ID for tracking

        Returns:
            True if metadata recording succeeded, False otherwise
        """
        if not self.metadata_enabled or not self.metadata_manager:
            self.logger.error("Metadata manager not initialized")
            return False

        try:
            # Check for duplicates
            if self._should_handle_duplicate_after_conversion(
                original_file_path, cog_file_path, run_id
            ):
                return True

            # Record conversion in metadata (without upload info)
            self.metadata_manager.create_conversion_record(
                original_file_path=original_file_path,
                cog_file_path=cog_file_path,
                run_id=run_id,
            )

            self.logger.info(
                f"Recorded conversion metadata for {original_file_path} -> {cog_file_path}"
            )

            return True

        except Exception as e:
            self.logger.error(
                f"Failed to record metadata for {cog_file_path}: {str(e)}"
            )

            # Record failed conversion
            if self.metadata_manager:
                self.metadata_manager.add_failed_conversion(
                    original_file_path=original_file_path,
                    error_message=str(e),
                    error_type="metadata_failure",
                )

            return False

    def _should_handle_duplicate_after_conversion(
        self,
        original_file_path: str,
        cog_file_path: str,
        run_id: Optional[int] = None,
    ) -> bool:
        """
        Check if a converted file should be handled as a duplicate after conversion.
        This is useful when duplicate detection is done after conversion (e.g., when
        the conversion process itself might change the content hash).

        Args:
            original_file_path: Path to the original file
            cog_file_path: Path to the converted COG file
            run_id: Optional run ID for tracking

        Returns:
            True if duplicate was handled, False otherwise
        """
        if not self.metadata_enabled or not self.metadata_manager:
            return False

        # Get duplicate detection configuration
        detect_duplicates = self.config.get("processing", {}).get(
            "detect_duplicates", True
        )
        duplicate_strategy = self.config.get("processing", {}).get(
            "duplicate_strategy", "skip"
        )

        if not detect_duplicates:
            return False

        try:
            # Calculate content hash of the original file (not the COG)
            content_hash = calculate_content_hash(original_file_path)

            # Check if this content already exists
            if self.metadata_manager.is_duplicate_content(
                content_hash, original_file_path
            ):
                if duplicate_strategy == "reference":
                    # Create reference to existing blob
                    success = self.metadata_manager.handle_duplicate_file(
                        original_file_path,
                        content_hash,
                        duplicate_strategy,
                        run_id=run_id,
                    )
                    if success:
                        self.stats["duplicates_referenced"] = (
                            self.stats.get("duplicates_referenced", 0) + 1
                        )
                        self.logger.info(
                            f"Handled duplicate file {original_file_path} after conversion - referenced existing blob"
                        )

                        # Clean up the local COG file since we're referencing existing blob
                        try:
                            os.remove(cog_file_path)
                            self.logger.debug(
                                f"Removed duplicate COG file: {cog_file_path}"
                            )
                        except Exception as e:
                            self.logger.warning(
                                f"Could not remove duplicate COG file: {str(e)}"
                            )

                        return True
                elif duplicate_strategy == "skip":
                    # Just skip creating metadata for this file
                    self.stats["skipped"] += 1
                    self.logger.info(
                        f"Skipped creating metadata for duplicate file {original_file_path}"
                    )

                    # Clean up the local COG file
                    try:
                        os.remove(cog_file_path)
                        self.logger.debug(
                            f"Removed duplicate COG file: {cog_file_path}"
                        )
                    except Exception as e:
                        self.logger.warning(
                            f"Could not remove duplicate COG file: {str(e)}"
                        )

                    return True

        except Exception as e:
            self.logger.warning(
                f"Could not check for duplicates after conversion for {original_file_path}: {str(e)}"
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

    def get_metadata_manager(self):
        """Get the metadata manager instance"""
        return self.metadata_manager

    def get_uploader(self) -> Optional[BlobStorageUploader]:
        """Get the blob storage uploader instance"""
        return self.uploader
