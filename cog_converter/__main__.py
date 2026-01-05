#!/usr/bin/env python3
"""
Main entry point for COG Conversion Engine
"""

import argparse

from .advanced_engine import AdvancedConversionEngine


def main():
    parser = argparse.ArgumentParser(
        description="Advanced COG Conversion Engine with Blob Storage Integration"
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

    parser.add_argument(
        "--enable-storage", action="store_true", help="Enable blob storage integration"
    )

    parser.add_argument(
        "--connection-string", help="Azure blob storage connection string", default=None
    )

    parser.add_argument(
        "--container", help="Blob storage container name", default="cog-conversions"
    )

    args = parser.parse_args()

    # Create engine configuration
    engine_config = {}
    if args.output:
        engine_config["output_directory"] = args.output
    if args.temp:
        engine_config["temp_directory"] = args.temp

    # Handle storage configuration
    if args.enable_storage:
        engine_config["storage"] = {
            "enabled": True,
            "provider": "azure",
            "azure_connection_string": args.connection_string or "",
            "container_name": args.container,
        }

    # Create and run engine
    engine = AdvancedConversionEngine(config=engine_config, config_file=args.config)

    # Run conversion
    stats = engine.run(args.input)

    print(f"\nConversion completed. Processed {stats['total_files']} files.")

    # Print storage stats if available
    if hasattr(engine.pipeline, "get_storage_stats"):
        storage_stats = engine.pipeline.get_storage_stats()
        print(f"Uploaded {storage_stats.get('uploaded_files', 0)} files to storage.")


if __name__ == "__main__":
    main()
