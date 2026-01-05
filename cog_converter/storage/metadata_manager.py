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
                "version": "1.0",
                "created_at": datetime.now().isoformat(),
            }
        except Exception as e:
            self.logger.error(f"Error loading metadata: {str(e)}")
            return {
                "conversions": [],
                "version": "1.0",
                "created_at": datetime.now().isoformat(),
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
            "status": "completed",
        }

        # Add additional metadata
        if additional_metadata:
            record.update(additional_metadata)

        # Add to metadata
        self.metadata["conversions"].append(record)
        self.metadata["last_updated"] = datetime.now().isoformat()

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
        self, original_file_path: str, upload_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create a conversion record from an upload result dictionary.

        Args:
            original_file_path: Original file path
            upload_result: Upload result from BlobStorageUploader.upload_with_metadata()

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
        )


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
