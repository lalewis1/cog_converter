#!/usr/bin/env python3
"""
Test script for storage integration functionality using pytest.
"""

import os
import sys
import tempfile

import pytest

# Add the parent directory to Python path so cog_converter can be imported
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cog_converter.storage.blob_uploader import MockBlobStorageUploader
from cog_converter.storage.hash_utils import calculate_content_hash, generate_blob_path
from cog_converter.storage.metadata_manager import ConversionMetadataManager


def test_hash_utils():
    """Test hash utilities"""
    # Test with a sample file
    # Use absolute path to work from any working directory
    import os

    test_file = os.path.join(os.path.dirname(__file__), "..", "test_data", "domain.tif")

    if not os.path.exists(test_file):
        pytest.skip(f"Test file not found: {test_file}")

    # Test content hash calculation
    content_hash = calculate_content_hash(test_file)

    # Test blob path generation
    blob_path = generate_blob_path(test_file, content_hash)

    # Verify the hash is consistent
    content_hash2 = calculate_content_hash(test_file)
    assert content_hash == content_hash2, "Hash should be consistent"


def test_mock_uploader():
    """Test mock blob storage uploader"""
    # Create mock uploader
    uploader = MockBlobStorageUploader("test-container")

    # Test with sample file
    # Use absolute path to work from any working directory
    test_file = os.path.join(os.path.dirname(__file__), "..", "test_data", "domain.tif")

    if not os.path.exists(test_file):
        pytest.skip(f"Test file not found: {test_file}")

    # Upload file
    blob_path = uploader.upload_file(
        local_file_path=test_file, original_file_path=test_file
    )

    # Test upload with metadata
    upload_result = uploader.upload_with_metadata(
        local_file_path=test_file,
        original_file_path=test_file,
        additional_metadata={"test": "value"},
    )

    # Check that file was recorded
    assert len(uploader.uploaded_files) == 2, "Should have 2 uploaded files recorded"


def test_metadata_manager():
    """Test metadata manager"""
    # Create temporary metadata file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        temp_metadata_file = f.name

    try:
        # Create metadata manager
        metadata_manager = ConversionMetadataManager(temp_metadata_file)

        # Test adding a conversion record
        # Use absolute paths to work from any working directory
        test_file = os.path.join(
            os.path.dirname(__file__), "..", "test_data", "domain.tif"
        )
        cog_file = os.path.join(
            os.path.dirname(__file__), "..", "cog_output", "domain.tif"
        )

        if not os.path.exists(test_file):
            pytest.skip(f"Test file not found: {test_file}")

        if not os.path.exists(cog_file):
            pytest.skip(f"COG file not found: {cog_file}")

        # Calculate hash
        content_hash = calculate_content_hash(test_file)
        blob_path = generate_blob_path(test_file, content_hash)

        # Add conversion record
        record = metadata_manager.add_conversion_record(
            original_file_path=test_file,
            cog_file_path=cog_file,
            blob_path=blob_path,
            content_hash=content_hash,
            blob_url=f"https://storage.blob.core.windows.net/test/{blob_path}",
        )

        # Test finding records
        found_by_path = metadata_manager.find_by_original_path(test_file)
        found_by_hash = metadata_manager.find_by_content_hash(content_hash)
        found_by_blob = metadata_manager.find_by_blob_path(blob_path)

        assert len(found_by_path) == 1, "Should find 1 record by path"
        assert len(found_by_hash) == 1, "Should find 1 record by hash"
        assert len(found_by_blob) == 1, "Should find 1 record by blob path"

        # Test failed conversion
        failed_record = metadata_manager.add_failed_conversion(
            original_file_path=test_file,
            error_message="Test error",
            error_type="test_failure",
        )

    finally:
        # Clean up
        if os.path.exists(temp_metadata_file):
            os.unlink(temp_metadata_file)


def test_integration():
    """Test full integration workflow"""
    # Create temporary files
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        temp_metadata_file = f.name

    try:
        # Create components
        uploader = MockBlobStorageUploader("integration-test")
        metadata_manager = ConversionMetadataManager(temp_metadata_file)

        # Test file
        # Use absolute paths to work from any working directory
        test_file = os.path.join(
            os.path.dirname(__file__), "..", "test_data", "domain.tif"
        )
        cog_file = os.path.join(
            os.path.dirname(__file__), "..", "cog_output", "domain.tif"
        )

        if not os.path.exists(test_file):
            pytest.skip(f"Test file not found: {test_file}")

        if not os.path.exists(cog_file):
            pytest.skip(f"COG file not found: {cog_file}")

        # Step 1: Calculate hash
        content_hash = calculate_content_hash(test_file)

        # Step 2: Upload to blob storage
        upload_result = uploader.upload_with_metadata(
            local_file_path=cog_file,
            original_file_path=test_file,
            additional_metadata={
                "source": "test_integration",
                "environment": "testing",
            },
        )

        # Step 3: Record metadata
        record = metadata_manager.create_conversion_record_from_upload(
            original_file_path=test_file, upload_result=upload_result
        )

        # Step 4: Verify traceability
        found_records = metadata_manager.find_by_original_path(test_file)
        assert len(found_records) == 1, "Should find exactly 1 record"

        found_record = found_records[0]
        assert found_record["content_hash"] == content_hash, "Content hash should match"
        assert (
            found_record["blob_path"] == upload_result["blob_path"]
        ), "Blob path should match"

    finally:
        # Clean up
        if os.path.exists(temp_metadata_file):
            os.unlink(temp_metadata_file)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
