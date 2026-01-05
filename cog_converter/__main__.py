#!/usr/bin/env python3
"""
Main entry point for COG Conversion Engine
"""

import argparse

from .engine import ConversionEngine


def main():
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

    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

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


if __name__ == "__main__":
    main()
