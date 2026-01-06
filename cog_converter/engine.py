#!/usr/bin/env python3
"""
Advanced COG Conversion Engine with Blob Storage Integration
"""

import logging
from typing import Dict, List, Optional

# Import engine components
from .config import ConfigurationManager
from .file_discoverer import FileDiscoverer
from .pipeline import ConversionPipeline


class ConversionEngine:
    """
    COG Conversion Engine with optional blob storage integration.
    """

    def __init__(
        self,
        config: Optional[dict] = None,
        config_file: Optional[str] = None,
    ):
        """Initialize the conversion engine"""
        # Apply configuration in proper order: defaults -> config_file -> CLI args
        self.config = ConfigurationManager()
        if config_file:
            self.config.load_config(config_file)
        if config:
            # Use deep update to ensure CLI args override config file args
            self.config._deep_update(self.config.config, config)

        self.file_discoverer = FileDiscoverer(self.config.config)
        self.pipeline = ConversionPipeline(self.config.config)

        # Setup logging
        self.logger = logging.getLogger(__name__)
        self._configure_logging()

    def _configure_logging(self):
        """Configure logging for the engine"""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()],
        )

    def run(self, input_path: str) -> Dict[str, int]:
        """Main entry point for conversion process"""
        self.logger.info("Starting COG conversion process...")
        self.logger.info(f"Input: {input_path}")
        self.logger.info(f"Output: {self.config.get('output_directory')}")

        # Check if storage is enabled
        storage_enabled = self.config.get("storage.enabled", False)
        if storage_enabled:
            self.logger.info("Blob storage integration enabled")
            self.logger.info(
                f"Provider: {self.config.get('storage.provider', 'azure')}"
            )
            self.logger.info(
                f"Container: {self.config.get('storage.container_name', 'cog-conversions')}"
            )
        else:
            self.logger.info("Blob storage integration disabled")

        # Start a new run for tracking
        self.current_run_id = None
        if hasattr(self.pipeline, "get_metadata_manager"):
            metadata_manager = self.pipeline.get_metadata_manager()
            if metadata_manager and hasattr(metadata_manager, "start_new_run"):
                self.current_run_id = metadata_manager.start_new_run(
                    input_path, self.config.config
                )
                self.logger.info(f"Started conversion run {self.current_run_id}")

        # Discover files to process
        self.logger.info("Discovering raster files...")
        files_to_process = self.file_discoverer.find_raster_files(input_path)

        self.logger.info(f"Found {len(files_to_process)} raster files to process")

        if not files_to_process:
            self.logger.info("No raster files found to process")
            return self.pipeline.get_stats()

        # Filter files based on processing configuration
        files_to_process = self._filter_files_for_processing(
            files_to_process,
            self.current_run_id if hasattr(self, "current_run_id") else None,
        )

        self.logger.info(f"Processing {len(files_to_process)} files after filtering")

        # Process files
        for i, file_path in enumerate(files_to_process, 1):
            self.logger.info(f"Processing {i}/{len(files_to_process)}: {file_path}")
            success = self.pipeline.process_file(file_path, self.current_run_id)
            if success:
                self.logger.info("  ✓ Successfully converted")
            else:
                self.logger.error("  ✗ Failed to convert")

        # Print summary
        self._print_summary()

        # End the run
        if self.current_run_id is not None:
            if hasattr(self.pipeline, "get_metadata_manager"):
                metadata_manager = self.pipeline.get_metadata_manager()
                if metadata_manager and hasattr(metadata_manager, "end_run"):
                    stats = self.pipeline.get_stats()
                    metadata_manager.end_run(self.current_run_id, stats)
                    self.logger.info(f"Completed conversion run {self.current_run_id}")

        return self.pipeline.get_stats()

    def _filter_files_for_processing(
        self, files: List[str], current_run_id: Optional[int] = None
    ) -> List[str]:
        """
        Filter files based on processing configuration to enable efficient rerunning.

        Args:
            files: List of file paths to filter
            current_run_id: Current run ID for tracking

        Returns:
            Filtered list of files that should be processed
        """
        # Get processing configuration
        force_reprocess = self.config.get("processing.force_reprocess", False)
        skip_already_processed = self.config.get("processing.skip_already_processed", True)
        detect_duplicates = self.config.get("processing.detect_duplicates", True)

        # Get metadata manager if available
        metadata_manager = self.get_metadata_manager()

        filtered_files = []
        skipped_files = []

        for file_path in files:
            should_process = True
            skip_reason = None

            # Check if we should force reprocessing
            if force_reprocess:
                filtered_files.append(file_path)
                continue

            # Check if metadata manager is available for smart filtering
            if metadata_manager:
                # Only skip already processed files if skip_already_processed is True
                # Note: Duplicate handling is now done after conversion, not before
                if skip_already_processed:
                    if not metadata_manager.should_process_file(file_path, skip_already_processed=skip_already_processed):
                        should_process = False
                        skip_reason = "already processed"

            # Add to appropriate list
            if should_process:
                filtered_files.append(file_path)
            else:
                skipped_files.append((file_path, skip_reason))
                if metadata_manager:
                    metadata_manager.mark_file_skipped(
                        file_path, skip_reason or "unknown reason"
                    )

        # Log filtering results
        if skipped_files:
            self.logger.info(f"Skipped {len(skipped_files)} files:")
            for file_path, reason in skipped_files[:5]:  # Show first 5
                self.logger.info(f"  - {file_path}: {reason}")
            if len(skipped_files) > 5:
                self.logger.info(f"  ... and {len(skipped_files) - 5} more")

        return filtered_files

    def _print_summary(self):
        """Print processing summary"""
        stats = self.pipeline.get_stats()

        print("\n" + "=" * 60)
        print("Conversion Summary:")
        print(f"  Total files: {stats['total_files']}")
        print(f"  Successful: {stats['successful']}")
        print(f"  Failed: {stats['failed']}")
        print(f"  Skipped: {stats['skipped']}")
        print(f"  Retries: {stats['retries']}")
        if "duplicates_referenced" in stats and stats["duplicates_referenced"] > 0:
            print(f"  Duplicates referenced: {stats['duplicates_referenced']}")

        # Add storage statistics if available
        if hasattr(self.pipeline, "get_storage_stats"):
            storage_stats = self.pipeline.get_storage_stats()
            print(f"  Uploaded to storage: {storage_stats.get('uploaded_files', 0)}")
            print(f"  Upload failed: {storage_stats.get('upload_failed', 0)}")

        if stats["total_files"] > 0:
            success_rate = stats["successful"] / stats["total_files"] * 100
            print(f"  Success rate: {success_rate:.1f}%")
        else:
            print("  Success rate: 0%")

        # Print metadata file location if available
        if hasattr(self.pipeline, "get_metadata_manager"):
            metadata_manager = self.pipeline.get_metadata_manager()
            if metadata_manager:
                print(f"  Metadata file: {metadata_manager.database_file}")

        print("=" * 60)

    def process_single_file(self, input_path: str, output_path: str = None) -> bool:
        """Process a single file"""
        if output_path is None:
            # Generate output path automatically
            output_path = self.pipeline._generate_output_path(input_path)

        return self.pipeline.process_file(
            input_path, getattr(self, "current_run_id", None)
        )

    def get_config(self) -> dict:
        """Get current configuration"""
        return self.config.config

    def save_config(self, config_file: str) -> bool:
        """Save current configuration to file"""
        return self.config.save_config(config_file)

    def get_metadata_manager(self):
        """Get the metadata manager if available"""
        if hasattr(self.pipeline, "get_metadata_manager"):
            return self.pipeline.get_metadata_manager()
        return None

    def get_uploader(self):
        """Get the blob storage uploader if available"""
        if hasattr(self.pipeline, "get_uploader"):
            return self.pipeline.get_uploader()
        return None

    def enable_storage_integration(
        self,
        connection_string: str,
        container_name: str = "cog-conversions",
        provider: str = "azure",
    ) -> bool:
        """
        Enable storage integration dynamically.

        Args:
            connection_string: Storage connection string
            container_name: Container name
            provider: Storage provider (azure, aws, gcp)

        Returns:
            True if successfully enabled, False otherwise
        """
        try:
            # Update configuration
            self.config.config["storage"]["enabled"] = True
            self.config.config["storage"]["provider"] = provider
            self.config.config["storage"]["azure_connection_string"] = connection_string
            self.config.config["storage"]["container_name"] = container_name

            # Reinitialize pipeline with storage
            self.pipeline = ConversionPipeline(self.config.config)

            self.logger.info("Storage integration enabled dynamically")
            return True

        except Exception as e:
            self.logger.error(f"Failed to enable storage integration: {str(e)}")
            return False

    def disable_storage_integration(self) -> bool:
        """
        Disable storage integration.

        Returns:
            True if successfully disabled, False otherwise
        """
        try:
            self.config.config["storage"]["enabled"] = False
            self.pipeline = ConversionPipeline(self.config.config)

            self.logger.info("Storage integration disabled")
            return True

        except Exception as e:
            self.logger.error(f"Failed to disable storage integration: {str(e)}")
            return False
