"""
Blob storage uploader for uploading converted COGs to cloud storage.
Currently supports Azure Blob Storage.
"""

import logging
import os
from typing import Any, Dict, Optional

from azure.storage.blob import BlobServiceClient

from .hash_utils import generate_blob_path


class BlobStorageUploader:
    """
    Handles uploading files to blob storage with support for different providers.
    Currently implements Azure Blob Storage.
    """

    def __init__(
        self,
        connection_string: str,
        container_name: str,
        create_container_if_not_exists: bool = True,
    ):
        """
        Initialize blob storage uploader.

        Args:
            connection_string: Connection string for blob storage
            container_name: Name of the container/blob
            create_container_if_not_exists: Create container if it doesn't exist
        """
        self.connection_string = connection_string
        self.container_name = container_name
        self.logger = logging.getLogger(__name__)

        # Initialize Azure Blob Storage client
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        self.container_client = self.blob_service_client.get_container_client(
            container_name
        )

        # Create container if it doesn't exist
        if create_container_if_not_exists:
            try:
                self.container_client.create_container()
                self.logger.info(f"Created container '{container_name}'")
            except Exception as e:
                # Container likely already exists
                if "ContainerAlreadyExists" not in str(e):
                    self.logger.warning(
                        f"Could not create container '{container_name}': {str(e)}"
                    )

    def upload_file(
        self,
        local_file_path: str,
        blob_path: Optional[str] = None,
        original_file_path: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        overwrite: bool = True,
    ) -> str:
        """
        Upload a file to blob storage.

        Args:
            local_file_path: Path to local file to upload
            blob_path: Optional custom blob path. If None, will be generated from original_file_path.
            original_file_path: Original file path (used for generating blob path if needed)
            metadata: Optional metadata to attach to the blob
            overwrite: Whether to overwrite existing blob

        Returns:
            The blob path where the file was uploaded

        Raises:
            FileNotFoundError: If local file doesn't exist
            ValueError: If neither blob_path nor original_file_path is provided
        """
        # Validate local file exists
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Local file not found: {local_file_path}")

        if not os.path.isfile(local_file_path):
            raise ValueError(f"Local path is not a file: {local_file_path}")

        # Generate blob path if not provided
        if blob_path is None:
            if original_file_path is None:
                raise ValueError(
                    "Either blob_path or original_file_path must be provided"
                )

            # Calculate content hash and generate blob path
            from .hash_utils import calculate_content_hash

            content_hash = calculate_content_hash(local_file_path)
            blob_path = generate_blob_path(original_file_path, content_hash)

        # Get blob client
        blob_client = self.container_client.get_blob_client(blob_path)

        # Upload file
        try:
            with open(local_file_path, "rb") as data:
                blob_client.upload_blob(
                    data, overwrite=overwrite, metadata=metadata or {}
                )

            self.logger.info(f"Uploaded {local_file_path} to {blob_path}")
            return blob_path

        except Exception as e:
            self.logger.error(
                f"Failed to upload {local_file_path} to {blob_path}: {str(e)}"
            )
            raise

    def file_exists(self, blob_path: str) -> bool:
        """
        Check if a blob exists in storage.

        Args:
            blob_path: Path to the blob

        Returns:
            True if blob exists, False otherwise
        """
        blob_client = self.container_client.get_blob_client(blob_path)
        return blob_client.exists()

    def get_blob_url(self, blob_path: str) -> str:
        """
        Get the URL for a blob.

        Args:
            blob_path: Path to the blob

        Returns:
            Full URL to access the blob
        """
        blob_client = self.container_client.get_blob_client(blob_path)
        return blob_client.url

    def upload_with_metadata(
        self,
        local_file_path: str,
        original_file_path: str,
        additional_metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Upload file with comprehensive metadata including traceability information.

        Args:
            local_file_path: Path to local file to upload
            original_file_path: Original file path for traceability
            additional_metadata: Additional metadata to include

        Returns:
            Dictionary with upload information including blob_path, content_hash, etc.
        """
        import datetime

        from .hash_utils import calculate_content_hash

        # Calculate content hash
        content_hash = calculate_content_hash(local_file_path)

        # Generate blob path
        blob_path = generate_blob_path(original_file_path, content_hash)

        # Prepare metadata
        metadata = {
            "original_path": original_file_path,
            "content_hash": content_hash,
            "upload_timestamp": datetime.datetime.now().isoformat(),
            "content_type": self._get_content_type(local_file_path),
        }

        # Add additional metadata
        if additional_metadata:
            metadata.update(additional_metadata)

        # Upload file
        self.upload_file(local_file_path, blob_path, metadata=metadata)

        return {
            "blob_path": blob_path,
            "content_hash": content_hash,
            "original_path": original_file_path,
            "blob_url": self.get_blob_url(blob_path),
            "upload_timestamp": metadata["upload_timestamp"],
        }

    def _get_content_type(self, file_path: str) -> str:
        """
        Get content type based on file extension.
        """
        ext = os.path.splitext(file_path)[1].lower()

        content_types = {
            ".tif": "image/tiff",
            ".tiff": "image/tiff",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".json": "application/json",
            ".txt": "text/plain",
            ".xml": "application/xml",
        }

        return content_types.get(ext, "application/octet-stream")


class MockBlobStorageUploader:
    """
    Mock implementation for testing without actual blob storage.
    """

    def __init__(self, container_name: str = "test-container"):
        self.container_name = container_name
        self.uploaded_files = []
        self.logger = logging.getLogger(__name__)

    def upload_file(
        self,
        local_file_path: str,
        blob_path: Optional[str] = None,
        original_file_path: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        overwrite: bool = True,
    ) -> str:
        """Mock upload that just records the operation."""
        if blob_path is None:
            from .hash_utils import calculate_content_hash

            content_hash = calculate_content_hash(local_file_path)
            blob_path = generate_blob_path(
                original_file_path or local_file_path, content_hash
            )

        record = {
            "local_file": local_file_path,
            "blob_path": blob_path,
            "original_file": original_file_path,
            "metadata": metadata or {},
        }

        self.uploaded_files.append(record)
        self.logger.info(f"[MOCK] Would upload {local_file_path} to {blob_path}")

        return blob_path

    def file_exists(self, blob_path: str) -> bool:
        """Mock file existence check."""
        return any(f["blob_path"] == blob_path for f in self.uploaded_files)

    def get_blob_url(self, blob_path: str) -> str:
        """Mock blob URL generation."""
        return f"https://mockstorage.blob.core.windows.net/{self.container_name}/{blob_path}"

    def upload_with_metadata(
        self,
        local_file_path: str,
        original_file_path: str,
        additional_metadata: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Mock upload with metadata that records the operation.
        """
        import datetime

        from .hash_utils import calculate_content_hash

        # Calculate content hash
        content_hash = calculate_content_hash(local_file_path)

        # Generate blob path
        blob_path = generate_blob_path(original_file_path, content_hash)

        # Prepare metadata
        metadata = {
            "original_path": original_file_path,
            "content_hash": content_hash,
            "upload_timestamp": datetime.datetime.now().isoformat(),
            "content_type": self._get_content_type(local_file_path),
        }

        # Add additional metadata
        if additional_metadata:
            metadata.update(additional_metadata)

        # Record the upload
        record = {
            "local_file": local_file_path,
            "blob_path": blob_path,
            "original_file": original_file_path,
            "metadata": metadata,
            "upload_type": "with_metadata",
        }

        self.uploaded_files.append(record)
        self.logger.info(
            f"[MOCK] Would upload {local_file_path} to {blob_path} with metadata"
        )

        return {
            "blob_path": blob_path,
            "content_hash": content_hash,
            "original_path": original_file_path,
            "blob_url": self.get_blob_url(blob_path),
            "upload_timestamp": metadata["upload_timestamp"],
        }

    def _get_content_type(self, file_path: str) -> str:
        """
        Get content type based on file extension.
        """
        ext = os.path.splitext(file_path)[1].lower()

        content_types = {
            ".tif": "image/tiff",
            ".tiff": "image/tiff",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".json": "application/json",
            ".txt": "text/plain",
            ".xml": "application/xml",
        }

        return content_types.get(ext, "application/octet-stream")
