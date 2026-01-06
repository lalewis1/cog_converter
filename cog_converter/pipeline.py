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

        # Note: Duplicate detection moved to after successful conversion
        # This ensures the first file gets uploaded, and duplicates reference it

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
            upload_success = self._handle_post_conversion(
                file_path, cog_file_path, run_id
            )
            if upload_success:
                # Check for duplicates after successful upload
                content_hash = calculate_content_hash(file_path)
                self._handle_duplicate_after_conversion(file_path, content_hash, run_id)
            return upload_success
        elif conversion_success and self.metadata_enabled and cog_file_path:
            # If only metadata is enabled (no storage), still record the conversion
            metadata_success = self._handle_metadata_only(
                file_path, cog_file_path, run_id
            )
            if metadata_success:
                # Check for duplicates after successful conversion
                content_hash = calculate_content_hash(file_path)
                self._handle_duplicate_after_conversion(file_path, content_hash, run_id)
            return metadata_success

        return conversion_success

    def _handle_duplicate_after_conversion(
        self, file_path: str, content_hash: str, run_id: Optional[int] = None
    ) -> bool:
        """
        Handle duplicate content AFTER successful conversion.
        This ensures the first file gets uploaded, and subsequent duplicates reference it.

        Args:
            file_path: Path to the file that was just converted
            content_hash: Content hash of the file
            run_id: Optional run ID for tracking

        Returns:
            True if this was a duplicate and was handled, False otherwise
        """
        if not self.metadata_enabled or not self.metadata_manager:
            return False

        # Simplified duplicate handling - always use "reference" strategy
        detect_duplicates = self.config.get("processing", {}).get(
            "detect_duplicates", True
        )

        if not detect_duplicates:
            return False

        try:
            # Check if this content already exists (from a different file)
            self.logger.debug(
                f"Checking for duplicates of {file_path} with hash {content_hash}"
            )
            if self.metadata_manager.is_duplicate_content(content_hash, file_path):
                # Get existing blob info
                existing_blob = self.metadata_manager.get_existing_blob_for_content(
                    content_hash
                )

                if existing_blob:
                    # Additional safety check: ensure we're not referencing ourselves
                    if existing_blob["original_file_path"] == file_path:
                        self.logger.warning(
                            f"Potential self-reference detected for {file_path} - skipping duplicate handling"
                        )
                        return False

                    # Check if this file already has a conversion record (it should, since we just processed it)
                    conn = self.metadata_manager._get_connection()
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT conversion_id FROM conversions WHERE original_file_path = ?",
                        (file_path,),
                    )
                    existing_record = cursor.fetchone()

                    if existing_record:
                        # Update the existing record to be a duplicate reference
                        conversion_id = existing_record[0]

                        # Log detailed information about the duplicate reference
                        self.logger.info(
                            f"Creating duplicate reference: {file_path} -> {existing_blob['original_file_path']} "
                            f"(blob: {existing_blob['blob_path']})"
                        )

                        cursor.execute(
                            """
                            UPDATE conversions SET
                                status = 'duplicate_referenced',
                                duplicate_of_conversion_id = ?,
                                duplicate_of_file_path = ?,
                                is_duplicate = TRUE,
                                blob_path = ?,
                                blob_url = ?
                            WHERE conversion_id = ?
                            """,
                            (
                                existing_blob["original_conversion_id"],
                                existing_blob["original_file_path"],
                                existing_blob["blob_path"],
                                existing_blob["blob_url"],
                                conversion_id,
                            ),
                        )
                        conn.commit()

                        self.stats["duplicates_referenced"] = (
                            self.stats.get("duplicates_referenced", 0) + 1
                        )
                        self.logger.info(
                            f"Duplicate file {file_path} - referenced existing blob {existing_blob['blob_path']}"
                        )
                        return True

        except Exception as e:
            self.logger.warning(f"Could not handle duplicate for {file_path}: {str(e)}")

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

    # Removed _should_handle_duplicate_after_conversion method
    # as we now use single-point duplicate detection before processing

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
