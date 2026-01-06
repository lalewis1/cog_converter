"""
Metadata manager for tracking file conversions and maintaining traceability
between original files and uploaded COGs.
"""

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from .hash_utils import calculate_content_hash


class ConversionMetadataManager:
    """
    Manages metadata for file conversions, providing traceability between
    original files and uploaded COGs in blob storage.
    """

    def __init__(self, metadata_file: str = "conversion_metadata.json"):
        """
        Initialize metadata manager.

        Args:
            metadata_file: Path to JSON file for storing metadata
        """
        self.metadata_file = metadata_file
        self.logger = logging.getLogger(__name__)
        self.metadata = self._load_metadata()

    def _load_metadata(self) -> Dict[str, Any]:
        """
        Load existing metadata from file or return empty structure.
        """
        try:
            with open(self.metadata_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.logger.info(f"Creating new metadata file: {self.metadata_file}")
            return {
                "conversions": [],
                "version": "1.1",  # Updated version for rerun support
                "created_at": datetime.now().isoformat(),
                "processing_state": {},  # Track file processing state
                "content_hash_index": {},  # Index for duplicate detection
            }
        except Exception as e:
            self.logger.error(f"Error loading metadata: {str(e)}")
            return {
                "conversions": [],
                "version": "1.1",
                "created_at": datetime.now().isoformat(),
                "processing_state": {},
                "content_hash_index": {},
            }

    def _save_metadata(self) -> bool:
        """
        Save metadata to file.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.metadata_file), exist_ok=True)

            with open(self.metadata_file, "w", encoding="utf-8") as f:
                json.dump(self.metadata, f, indent=2, ensure_ascii=False)

            self.logger.debug(f"Metadata saved to {self.metadata_file}")
            return True

        except Exception as e:
            self.logger.error(f"Error saving metadata: {str(e)}")
            return False

    def add_conversion_record(
        self,
        original_file_path: str,
        cog_file_path: str,
        blob_path: str,
        content_hash: str,
        blob_url: Optional[str] = None,
        additional_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Add a conversion record to the metadata.

        Args:
            original_file_path: Path to the original file
            cog_file_path: Path to the converted COG file
            blob_path: Path in blob storage
            content_hash: Content hash of the original file
            blob_url: Optional URL to access the blob
            additional_metadata: Additional metadata to include

        Returns:
            The created conversion record
        """
        # Calculate file sizes
        original_size = (
            os.path.getsize(original_file_path)
            if os.path.exists(original_file_path)
            else 0
        )
        cog_size = (
            os.path.getsize(cog_file_path) if os.path.exists(cog_file_path) else 0
        )

        # Get file modification time
        file_mtime = (
            os.path.getmtime(original_file_path)
            if os.path.exists(original_file_path)
            else 0
        )

        # Create conversion record
        record = {
            "conversion_id": len(self.metadata["conversions"]) + 1,
            "original_file_path": original_file_path,
            "cog_file_path": cog_file_path,
            "blob_path": blob_path,
            "content_hash": content_hash,
            "blob_url": blob_url,
            "original_file_size": original_size,
            "cog_file_size": cog_size,
            "conversion_timestamp": datetime.now().isoformat(),
            "file_modification_time": file_mtime,
            "status": "completed",
        }

        # Add additional metadata
        if additional_metadata:
            record.update(additional_metadata)

        # Add to metadata
        self.metadata["conversions"].append(record)
        self.metadata["last_updated"] = datetime.now().isoformat()

        # Update processing state
        self._update_processing_state(original_file_path, content_hash, "completed")

        # Update content hash index for duplicate detection
        self._update_content_hash_index(original_file_path, content_hash)

        # Save metadata
        if not self._save_metadata():
            record["status"] = "metadata_save_failed"

        self.logger.info(
            f"Added conversion record for {original_file_path} -> {blob_path}"
        )

        return record

    def add_failed_conversion(
        self,
        original_file_path: str,
        error_message: str,
        error_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Add a record for a failed conversion.

        Args:
            original_file_path: Path to the original file
            error_message: Error message
            error_type: Type of error

        Returns:
            The created failed conversion record
        """
        record = {
            "conversion_id": len(self.metadata["conversions"]) + 1,
            "original_file_path": original_file_path,
            "status": "failed",
            "error_message": error_message,
            "error_type": error_type,
            "failed_timestamp": datetime.now().isoformat(),
        }

        self.metadata["conversions"].append(record)
        self.metadata["last_updated"] = datetime.now().isoformat()

        if not self._save_metadata():
            self.logger.error(
                f"Failed to save metadata for failed conversion: {original_file_path}"
            )

        self.logger.error(
            f"Failed conversion recorded for {original_file_path}: {error_message}"
        )

        return record

    def find_by_original_path(self, original_file_path: str) -> List[Dict[str, Any]]:
        """
        Find conversion records by original file path.

        Args:
            original_file_path: Original file path to search for

        Returns:
            List of matching conversion records
        """
        return [
            record
            for record in self.metadata["conversions"]
            if record.get("original_file_path") == original_file_path
        ]

    def find_by_content_hash(self, content_hash: str) -> List[Dict[str, Any]]:
        """
        Find conversion records by content hash.

        Args:
            content_hash: Content hash to search for

        Returns:
            List of matching conversion records
        """
        return [
            record
            for record in self.metadata["conversions"]
            if record.get("content_hash") == content_hash
        ]

    def find_by_blob_path(self, blob_path: str) -> List[Dict[str, Any]]:
        """
        Find conversion records by blob path.

        Args:
            blob_path: Blob path to search for

        Returns:
            List of matching conversion records
        """
        return [
            record
            for record in self.metadata["conversions"]
            if record.get("blob_path") == blob_path
        ]

    def get_all_conversions(self) -> List[Dict[str, Any]]:
        """
        Get all conversion records.

        Returns:
            List of all conversion records
        """
        return self.metadata["conversions"]

    def get_successful_conversions(self) -> List[Dict[str, Any]]:
        """
        Get all successful conversion records.

        Returns:
            List of successful conversion records
        """
        return [
            record
            for record in self.metadata["conversions"]
            if record.get("status") == "completed"
        ]

    def get_failed_conversions(self) -> List[Dict[str, Any]]:
        """
        Get all failed conversion records.

        Returns:
            List of failed conversion records
        """
        return [
            record
            for record in self.metadata["conversions"]
            if record.get("status") == "failed"
        ]

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about conversions.

        Returns:
            Dictionary with conversion statistics
        """
        total = len(self.metadata["conversions"])
        successful = len(self.get_successful_conversions())
        failed = len(self.get_failed_conversions())

        return {
            "total_conversions": total,
            "successful_conversions": successful,
            "failed_conversions": failed,
            "success_rate": successful / total if total > 0 else 0.0,
            "metadata_file": self.metadata_file,
            "last_updated": self.metadata.get("last_updated"),
            "created_at": self.metadata.get("created_at"),
        }

    def export_metadata(self, output_file: Optional[str] = None) -> Dict[str, Any]:
        """
        Export metadata to a file or return the current metadata.

        Args:
            output_file: Optional file path to export to

        Returns:
            The current metadata
        """
        if output_file:
            try:
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(self.metadata, f, indent=2, ensure_ascii=False)
                self.logger.info(f"Metadata exported to {output_file}")
            except Exception as e:
                self.logger.error(f"Error exporting metadata: {str(e)}")

        return self.metadata

    def create_conversion_record_from_upload(
        self,
        original_file_path: str,
        upload_result: Dict[str, Any],
        run_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Create a conversion record from an upload result dictionary.

        Args:
            original_file_path: Original file path
            upload_result: Upload result from BlobStorageUploader.upload_with_metadata()
            run_id: Optional run ID for tracking

        Returns:
            Created conversion record
        """
        # Calculate content hash from the original file for consistency
        original_content_hash = calculate_content_hash(original_file_path)

        return self.add_conversion_record(
            original_file_path=original_file_path,
            cog_file_path=original_file_path,  # This should be the actual COG path
            blob_path=upload_result["blob_path"],
            content_hash=original_content_hash,  # Use the hash from original file
            blob_url=upload_result["blob_url"],
            additional_metadata={
                "upload_timestamp": upload_result["upload_timestamp"],
                "upload_content_hash": upload_result[
                    "content_hash"
                ],  # Store upload hash too
            },
            run_id=run_id,
        )

    def _update_processing_state(
        self, file_path: str, content_hash: str, status: str
    ) -> None:
        """
        Update the processing state for a file.

        Args:
            file_path: Path to the file
            content_hash: Content hash of the file
            status: Processing status (completed, failed, skipped)
        """
        # Ensure processing_state exists
        if "processing_state" not in self.metadata:
            self.metadata["processing_state"] = {}

        # Update or add processing state
        self.metadata["processing_state"][file_path] = {
            "status": status,
            "content_hash": content_hash,
            "last_processed": datetime.now().isoformat(),
            "file_modification_time": (
                os.path.getmtime(file_path) if os.path.exists(file_path) else 0
            ),
        }

    def _update_content_hash_index(self, file_path: str, content_hash: str) -> None:
        """
        Update the content hash index for duplicate detection.

        Args:
            file_path: Path to the file
            content_hash: Content hash of the file
        """
        # Ensure content_hash_index exists
        if "content_hash_index" not in self.metadata:
            self.metadata["content_hash_index"] = {}

        # Add file to content hash index
        if content_hash not in self.metadata["content_hash_index"]:
            self.metadata["content_hash_index"][content_hash] = []

        # Avoid duplicates in the index
        if file_path not in self.metadata["content_hash_index"][content_hash]:
            self.metadata["content_hash_index"][content_hash].append(file_path)

    def should_process_file(self, file_path: str, force: bool = False) -> bool:
        """
        Determine if a file should be processed based on its state and modification time.

        Args:
            file_path: Path to the file to check
            force: If True, force processing regardless of previous state

        Returns:
            True if file should be processed, False if it can be skipped
        """
        if force:
            return True

        # Check if file exists
        if not os.path.exists(file_path):
            return False

        # Get current file modification time
        current_mtime = os.path.getmtime(file_path)

        # Check processing state
        processing_state = self.metadata.get("processing_state", {}).get(file_path)

        if processing_state is None:
            # File has never been processed
            return True

        # Check if file has been modified since last processing
        last_mtime = processing_state.get("file_modification_time", 0)

        if current_mtime > last_mtime:
            # File has been modified since last processing
            return True

        # Check if processing was successful
        if processing_state.get("status") == "completed":
            # File was successfully processed and hasn't changed
            return False

        # For failed or skipped files, we might want to retry
        return True

    def is_duplicate_content(self, content_hash: str, file_path: str) -> bool:
        """
        Check if content with the given hash has already been processed.

        Args:
            content_hash: Content hash to check
            file_path: Current file path (for comparison)

        Returns:
            True if duplicate content exists, False otherwise
        """
        content_hash_index = self.metadata.get("content_hash_index", {})

        if content_hash not in content_hash_index:
            return False

        # Check if this exact file is already in the index
        files_with_same_hash = content_hash_index[content_hash]

        # If only this file has this hash, it's not a duplicate
        if len(files_with_same_hash) <= 1:
            return False

        # If multiple files have the same hash, it's a duplicate
        return True

    def get_duplicate_files(self, content_hash: str) -> List[str]:
        """
        Get all files that have the same content hash.

        Args:
            content_hash: Content hash to look up

        Returns:
            List of file paths with the same content hash
        """
        content_hash_index = self.metadata.get("content_hash_index", {})
        return content_hash_index.get(content_hash, [])

    def get_existing_blob_for_content(
        self, content_hash: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get existing blob information for content with the given hash.

        Args:
            content_hash: Content hash to look up

        Returns:
            Dictionary with blob_path and blob_url if found, None otherwise
        """
        # Look for any completed conversion with this content hash
        for conversion in self.metadata.get("conversions", []):
            if (
                conversion.get("status") == "completed"
                and conversion.get("content_hash") == content_hash
                and conversion.get("blob_path")
            ):
                return {
                    "blob_path": conversion["blob_path"],
                    "blob_url": conversion.get("blob_url"),
                    "original_conversion_id": conversion["conversion_id"],
                    "original_file_path": conversion["original_file_path"],
                }

        return None

    def create_duplicate_reference(
        self,
        original_file_path: str,
        existing_blob_info: Dict[str, Any],
        content_hash: str,
        additional_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create a metadata record for a duplicate file that references an existing blob.

        Args:
            original_file_path: Path to the duplicate file
            existing_blob_info: Information about the existing blob to reference
            content_hash: Content hash of the file
            additional_metadata: Additional metadata to include

        Returns:
            The created duplicate reference record
        """
        # Calculate file size
        file_size = (
            os.path.getsize(original_file_path)
            if os.path.exists(original_file_path)
            else 0
        )

        # Get file modification time
        file_mtime = (
            os.path.getmtime(original_file_path)
            if os.path.exists(original_file_path)
            else 0
        )

        # Create duplicate reference record
        record = {
            "conversion_id": len(self.metadata["conversions"]) + 1,
            "original_file_path": original_file_path,
            "cog_file_path": "",  # No separate COG file created
            "blob_path": existing_blob_info["blob_path"],
            "content_hash": content_hash,
            "blob_url": existing_blob_info["blob_url"],
            "original_file_size": file_size,
            "cog_file_size": 0,  # No separate COG file
            "conversion_timestamp": datetime.now().isoformat(),
            "file_modification_time": file_mtime,
            "status": "duplicate_referenced",
            "duplicate_of_conversion_id": existing_blob_info["original_conversion_id"],
            "duplicate_of_file_path": existing_blob_info["original_file_path"],
            "is_duplicate": True,
        }

        # Add additional metadata
        if additional_metadata:
            record.update(additional_metadata)

        # Add to metadata
        self.metadata["conversions"].append(record)
        self.metadata["last_updated"] = datetime.now().isoformat()

        # Update processing state
        self._update_processing_state(
            original_file_path, content_hash, "duplicate_referenced"
        )

        # Update content hash index for duplicate detection
        self._update_content_hash_index(original_file_path, content_hash)

        # Save metadata
        if not self._save_metadata():
            record["status"] = "metadata_save_failed"

        self.logger.info(
            f"Created duplicate reference: {original_file_path} -> {existing_blob_info['blob_path']} "
            f"(duplicate of {existing_blob_info['original_file_path']})"
        )

        return record

    def handle_duplicate_file(
        self,
        file_path: str,
        content_hash: str,
        duplicate_strategy: str = "reference",
        additional_metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Handle a duplicate file according to the specified strategy.

        Args:
            file_path: Path to the duplicate file
            content_hash: Content hash of the file
            duplicate_strategy: Strategy to use (reference, skip, process)
            additional_metadata: Additional metadata to include

        Returns:
            True if handled successfully, False otherwise
        """
        # Check if we have an existing blob for this content
        existing_blob = self.get_existing_blob_for_content(content_hash)

        if not existing_blob:
            # No existing blob found, this shouldn't happen if duplicate detection is working
            self.logger.warning(
                f"No existing blob found for duplicate content hash {content_hash}"
            )
            return False

        if duplicate_strategy == "reference":
            # Create a reference to the existing blob
            self.create_duplicate_reference(
                original_file_path=file_path,
                existing_blob_info=existing_blob,
                content_hash=content_hash,
                additional_metadata=additional_metadata,
            )
            return True

        elif duplicate_strategy == "skip":
            # Just mark as skipped
            self.mark_file_skipped(file_path, "duplicate content (skipped)")
            return True

        elif duplicate_strategy == "process":
            # This would be handled by normal processing
            return False

        else:
            self.logger.warning(f"Unknown duplicate strategy: {duplicate_strategy}")
            return False

    def get_processing_state(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Get the processing state for a specific file.

        Args:
            file_path: Path to the file

        Returns:
            Processing state dictionary or None if not found
        """
        return self.metadata.get("processing_state", {}).get(file_path)

    def mark_file_skipped(self, file_path: str, reason: str) -> None:
        """
        Mark a file as skipped with a reason.

        Args:
            file_path: Path to the file
            reason: Reason for skipping
        """
        # Calculate content hash for tracking
        try:
            content_hash = calculate_content_hash(file_path)
        except Exception:
            content_hash = "unknown"

        # Update processing state
        self._update_processing_state(file_path, content_hash, "skipped")

        # Add to content hash index
        self._update_content_hash_index(file_path, content_hash)

        # Add failed conversion record
        self.add_failed_conversion(
            original_file_path=file_path,
            error_message=f"Skipped: {reason}",
            error_type="skipped",
        )

        self._save_metadata()

    def mark_file_failed(self, file_path: str, error_message: str) -> None:
        """
        Mark a file as failed with an error message.

        Args:
            file_path: Path to the file
            error_message: Error message
        """
        # Calculate content hash for tracking
        try:
            content_hash = calculate_content_hash(file_path)
        except Exception:
            content_hash = "unknown"

        # Update processing state
        self._update_processing_state(file_path, content_hash, "failed")

        # Add to content hash index
        self._update_content_hash_index(file_path, content_hash)

        # Add failed conversion record
        self.add_failed_conversion(
            original_file_path=file_path,
            error_message=error_message,
            error_type="processing_failed",
        )

        self._save_metadata()


class SimpleMetadataTracker:
    """
    Simplified metadata tracker for basic traceability needs.
    """

    def __init__(self, csv_file: str = "conversion_traceability.csv"):
        """
        Initialize simple metadata tracker using CSV format.

        Args:
            csv_file: Path to CSV file for storing metadata
        """
        self.csv_file = csv_file
        self.logger = logging.getLogger(__name__)
        self._initialize_csv()

    def _initialize_csv(self) -> None:
        """Initialize CSV file with headers if it doesn't exist."""
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, "w", encoding="utf-8") as f:
                f.write(
                    "conversion_id,original_file_path,blob_path,content_hash,status,timestamp\n"
                )

    def add_record(
        self,
        original_file_path: str,
        blob_path: str,
        content_hash: str,
        status: str = "completed",
    ) -> None:
        """
        Add a simple conversion record.

        Args:
            original_file_path: Original file path
            blob_path: Blob storage path
            content_hash: Content hash
            status: Conversion status
        """
        try:
            # Get next ID
            try:
                with open(self.csv_file, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                    conversion_id = len(lines)  # Header is line 1
            except Exception:
                conversion_id = 1

            # Write record
            with open(self.csv_file, "a", encoding="utf-8") as f:
                f.write(
                    f'{conversion_id},"{original_file_path}","{blob_path}",{content_hash},{status},"{datetime.now().isoformat()}"\n'
                )

            self.logger.info(f"Added CSV record: {original_file_path} -> {blob_path}")

        except Exception as e:
            self.logger.error(f"Error adding CSV record: {str(e)}")
