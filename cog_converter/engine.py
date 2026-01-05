#!/usr/bin/env python3
"""
Core COG Conversion Engine
Handles conversion of various raster formats to Cloud Optimized GeoTIFFs
"""

from typing import Dict, Optional

# Import engine components
from .config import ConfigurationManager
from .file_discoverer import FileDiscoverer
from .pipeline import ConversionPipeline


class ConversionEngine:
    """Main COG Conversion Engine"""

    def __init__(
        self, config: Optional[dict] = None, config_file: Optional[str] = None
    ):
        """Initialize the conversion engine"""
        if config:
            self.config = ConfigurationManager()
            self.config.config.update(config)
        else:
            self.config = ConfigurationManager(config_file)

        self.file_discoverer = FileDiscoverer(self.config.config)
        self.pipeline = ConversionPipeline(self.config.config)

    def run(self, input_path: str) -> Dict[str, int]:
        """Main entry point for conversion process"""
        print("Starting COG conversion process...")
        print(f"Input: {input_path}")
        print(f"Output: {self.config.get('output_directory')}")

        # Discover files to process
        print("Discovering raster files...")
        files_to_process = self.file_discoverer.find_raster_files(input_path)

        print(f"Found {len(files_to_process)} raster files to process")

        if not files_to_process:
            print("No raster files found to process")
            return self.pipeline.get_stats()

        # Process files
        for i, file_path in enumerate(files_to_process, 1):
            print(f"Processing {i}/{len(files_to_process)}: {file_path}")
            success = self.pipeline.process_file(file_path)
            if success:
                print("  ✓ Successfully converted")
            else:
                print("  ✗ Failed to convert")

        # Print summary
        self._print_summary()

        return self.pipeline.get_stats()

    def _print_summary(self):
        """Print processing summary"""
        stats = self.pipeline.get_stats()
        print("\n" + "=" * 50)
        print("Conversion Summary:")
        print(f"  Total files: {stats['total_files']}")
        print(f"  Successful: {stats['successful']}")
        print(f"  Failed: {stats['failed']}")
        print(f"  Skipped: {stats['skipped']}")
        print(f"  Retries: {stats['retries']}")

        if stats["total_files"] > 0:
            success_rate = stats["successful"] / stats["total_files"] * 100
            print(f"  Success rate: {success_rate:.1f}%")
        else:
            print("  Success rate: 0%")
        print("=" * 50)

    def process_single_file(self, input_path: str, output_path: str = None) -> bool:
        """Process a single file"""
        if output_path is None:
            # Generate output path automatically
            output_path = self.pipeline._generate_output_path(input_path)

        return self.pipeline.process_file(input_path)

    def get_config(self) -> dict:
        """Get current configuration"""
        return self.config.config

    def save_config(self, config_file: str) -> bool:
        """Save current configuration to file"""
        return self.config.save_config(config_file)


# Command line interface
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="COG Conversion Engine - Convert raster files to Cloud Optimized GeoTIFFs"
    )

    parser.add_argument("input", help="Input file or directory containing raster files")

    parser.add_argument(
        "--config", "-c", help="Configuration file (JSON format)", default=None
    )

    parser.add_argument(
        "--output", "-o", help="Output directory for COG files", default=None
    )

    parser.add_argument(
        "--temp", "-t", help="Temporary directory for intermediate files", default=None
    )

    args = parser.parse_args()

    # Create engine with configuration
    engine_config = {}
    if args.output:
        engine_config["output_directory"] = args.output
    if args.temp:
        engine_config["temp_directory"] = args.temp

    engine = ConversionEngine(config=engine_config, config_file=args.config)

    # Run conversion
    stats = engine.run(args.input)

    print(f"\nConversion completed. Processed {stats['total_files']} files.")
