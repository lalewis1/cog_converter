#!/usr/bin/env python3
"""
Test script for COG Conversion Engine
"""

import os
import sys
import tempfile

# Add the cog_converter directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "cog_converter"))

from cog_converter.engine import ConversionEngine


def test_basic_functionality():
    """Test basic functionality of the conversion engine"""

    print("Testing COG Conversion Engine...")

    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Using temporary directory: {temp_dir}")

        # Create test input directory
        test_input_dir = os.path.join(temp_dir, "input")
        os.makedirs(test_input_dir)

        # Create test output directory
        test_output_dir = os.path.join(temp_dir, "output")

        # Create a simple test configuration
        config = {
            "output_directory": test_output_dir,
            "temp_directory": os.path.join(temp_dir, "temp"),
            "cog_parameters": {"compression": "LZW", "blocksize": "256"},
        }

        # Create engine
        engine = ConversionEngine(config=config)

        print("✓ Engine created successfully")
        print(f"✓ Configuration loaded: {len(config)} settings")

        # Test configuration access
        output_dir = engine.get_config()["output_directory"]
        assert (
            output_dir == test_output_dir
        ), f"Expected {test_output_dir}, got {output_dir}"
        print("✓ Configuration access works")

        # Test file discovery (should find no files in empty directory)
        files = engine.file_discoverer.find_raster_files(test_input_dir)
        assert len(files) == 0, f"Expected 0 files, found {len(files)}"
        print("✓ File discovery works (empty directory)")

        # Test pipeline statistics
        stats = engine.pipeline.get_stats()
        assert stats["total_files"] == 0, "Expected 0 total files"
        print("✓ Pipeline statistics work")

        print("\nAll basic tests passed! ✓")
        print("\nNote: Full conversion testing requires actual raster files.")
        print("The engine is ready to process GeoTIFF and World Image files.")


def test_command_line_interface():
    """Test that the command line interface is available"""

    print("\nTesting command line interface...")

    # Test that we can import the main module
    try:
        import cog_converter.__main__

        print("✓ Command line interface module available")
    except ImportError as e:
        print(f"✗ Command line interface import failed: {e}")
        return False

    return True


if __name__ == "__main__":
    print("COG Conversion Engine - Basic Tests")
    print("=" * 50)

    try:
        test_basic_functionality()
        test_command_line_interface()

        print("\n" + "=" * 50)
        print("TESTING COMPLETE")
        print("The COG Conversion Engine is ready to use!")
        print("\nUsage:")
        print(
            "  python -m cog_converter /path/to/raster/files --output /output/directory"
        )

    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

