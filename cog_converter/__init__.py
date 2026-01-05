#!/usr/bin/env python3
"""
COG Conversion Engine Package
"""

from .config import ConfigurationManager
from .engine import ConversionEngine
from .error_handler import ErrorHandler
from .file_discoverer import FileDiscoverer
from .pipeline import ConversionPipeline

__version__ = "0.1.0"
__author__ = "COG Conversion Engine"
__license__ = "MIT"

# Export main classes
__all__ = [
    "ConversionEngine",
    "ConfigurationManager",
    "ErrorHandler",
    "FileDiscoverer",
    "ConversionPipeline",
]
