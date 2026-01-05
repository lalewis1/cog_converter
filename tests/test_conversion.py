#!/usr/bin/env python3
"""
Test script for COG Conversion Engine using pytest
"""

import os

# Add the parent directory to Python path so cog_converter can be imported
import sys
import tempfile
from importlib.util import find_spec

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cog_converter.engine import ConversionEngine


def test_basic_functionality():
    """Test basic functionality of the conversion engine"""

    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
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

        # Test configuration access
        output_dir = engine.get_config()["output_directory"]
        assert (
            output_dir == test_output_dir
        ), f"Expected {test_output_dir}, got {output_dir}"

        # Test file discovery (should find no files in empty directory)
        files = engine.file_discoverer.find_raster_files(test_input_dir)
        assert len(files) == 0, f"Expected 0 files, found {len(files)}"

        # Test pipeline statistics
        stats = engine.pipeline.get_stats()
        assert stats["total_files"] == 0, "Expected 0 total files"


def test_command_line_interface():
    """Test that the command line interface is available"""
    # Test that we can import the main module
    try:
        cli = find_spec("cog_converter.__main__")
        assert cli is not None
    except [ImportError, AssertionError] as e:
        pytest.fail(f"Command line interface import failed: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
