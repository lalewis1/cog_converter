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

    # Efficient rerun options
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reprocessing of all files, ignoring previous state",
    )

    parser.add_argument(
        "--skip-processed",
        action="store_true",
        help="Skip files that have already been successfully processed",
    )

    parser.add_argument(
        "--no-skip-processed",
        dest="skip_processed",
        action="store_false",
        help="Process all files, including previously processed ones",
    )

    parser.add_argument(
        "--detect-duplicates",
        action="store_true",
        help="Detect and handle duplicate files based on content hash",
    )

    parser.add_argument(
        "--no-detect-duplicates",
        dest="detect_duplicates",
        action="store_false",
        help="Disable duplicate detection",
    )

    parser.add_argument(
        "--duplicate-strategy",
        choices=["reference", "skip", "process", "warn"],
        default="reference",
        help="Strategy for handling duplicate files (reference, skip, process, warn)",
    )

    parser.add_argument(
        "--track-changes",
        action="store_true",
        help="Track file changes and only reprocess modified files",
    )

    parser.add_argument(
        "--no-track-changes",
        dest="track_changes",
        action="store_false",
        help="Disable file change tracking",
    )

    # Set defaults
    parser.set_defaults(skip_processed=True, detect_duplicates=True, track_changes=True)

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

    # Handle processing configuration for efficient rerunning
    engine_config["processing"] = {
        "skip_already_processed": args.skip_processed,
        "detect_duplicates": args.detect_duplicates,
        "duplicate_strategy": args.duplicate_strategy,
        "force_reprocess": args.force,
        "track_file_changes": args.track_changes,
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
