"""
Hash utilities for calculating file content hashes.
"""

import hashlib
import os


def calculate_content_hash(
    file_path: str, chunk_size: int = 8192, hash_algorithm: str = "md5"
) -> str:
    """
    Calculate hash of file content using chunked reading for memory efficiency.

    Args:
        file_path: Path to the file
        chunk_size: Size of chunks to read in bytes (default: 8KB)
        hash_algorithm: Hash algorithm to use (default: "md5")

    Returns:
        Hexadecimal string representation of the hash

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If hash algorithm is not supported
    """
    # Validate file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    if not os.path.isfile(file_path):
        raise ValueError(f"Path is not a file: {file_path}")

    # Create hash object based on algorithm
    try:
        hash_obj = hashlib.new(hash_algorithm)
    except ValueError as e:
        raise ValueError(f"Unsupported hash algorithm '{hash_algorithm}': {str(e)}")

    # Read file in chunks and update hash
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            hash_obj.update(chunk)

    return hash_obj.hexdigest()


def generate_blob_path(original_path: str, content_hash: str) -> str:
    """
    Generate simple blob storage path using content hash.

    Args:
        original_path: Original file path (for extension)
        content_hash: Hash of file content

    Returns:
        Simple blob path like "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0.tif"
    """
    # Get file extension
    _, ext = os.path.splitext(original_path)
    ext = ext.lower()  # Normalize to lowercase

    # Remove leading dot if no extension
    if ext == "":
        ext = ""
    else:
        ext = ext[1:]  # Remove the dot

    # Handle case where we want to preserve extension
    if ext:
        return f"{content_hash}.{ext}"
    else:
        return content_hash


if __name__ == "__main__":
    # Simple test
    import sys

    if len(sys.argv) > 1:
        test_file = sys.argv[1]
        try:
            file_hash = calculate_content_hash(test_file)
            blob_path = generate_blob_path(test_file, file_hash)
            print(f"File: {test_file}")
            print(f"Content Hash: {file_hash}")
            print(f"Blob Path: {blob_path}")
        except Exception as e:
            print(f"Error: {str(e)}")
            sys.exit(1)
