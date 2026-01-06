#!/usr/bin/env python3
"""
Main entry point for COG Conversion Engine
"""

import argparse

from .engine import ConversionEngine


def main():
    parser = argparse.ArgumentParser(
        description="Advanced COG Conversion Engine with Blob Storage Integration"
    )

    # Add flag for dumping default config before requiring input argument
    parser.add_argument(
        "--default-config",
        action="store_true",
        help="Dump default configuration as JSON to stdout and exit",
    )

    # Add flag for showing final config after all overrides
    parser.add_argument(
        "--show-config",
        action="store_true",
        help="Show final configuration after applying all overrides (can be combined with other flags)",
    )

    parser.add_argument(
        "input", help="Input file or directory containing raster files", nargs="?"
    )

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

    # Set defaults to a special sentinel value to distinguish "not provided" from "False"
    parser.set_defaults(
        skip_processed="NOT_SET", detect_duplicates="NOT_SET", track_changes="NOT_SET",
        enable_storage="NOT_SET", connection_string="NOT_SET", container="NOT_SET"
    )

    args = parser.parse_args()

    # Handle default config flag
    if args.default_config:
        from .config import ConfigurationManager

        config_manager = ConfigurationManager()
        config_manager.dump_config_json()
        return

    # Validate that input is provided when not showing default config
    if not args.input and not args.show_config:
        parser.error("the following arguments are required: input")

    # Create engine configuration
    engine_config = {}
    if args.output:
        engine_config["output_directory"] = args.output
    if args.temp:
        engine_config["temp_directory"] = args.temp

    # Handle storage configuration
    # Only include values that were explicitly set by the user via flags
    storage_config = {}

    # For boolean flags, check if they were explicitly set (not 'NOT_SET')
    if hasattr(args, "enable_storage") and args.enable_storage != "NOT_SET":
        storage_config["enabled"] = args.enable_storage
        storage_config["provider"] = "azure"
    
    if hasattr(args, "connection_string") and args.connection_string != "NOT_SET":
        storage_config["azure_connection_string"] = args.connection_string or ""
    
    if hasattr(args, "container") and args.container != "NOT_SET":
        storage_config["container_name"] = args.container

    if storage_config:
        engine_config["storage"] = storage_config

    # Handle processing configuration for efficient rerunning
    # Only include values that were explicitly set by the user via flags
    processing_config = {}

    # For boolean flags, check if they were explicitly set (not 'NOT_SET')
    if hasattr(args, "skip_processed") and args.skip_processed != "NOT_SET":
        processing_config["skip_already_processed"] = args.skip_processed
    if hasattr(args, "detect_duplicates") and args.detect_duplicates != "NOT_SET":
        processing_config["detect_duplicates"] = args.detect_duplicates
    if hasattr(args, "track_changes") and args.track_changes != "NOT_SET":
        processing_config["track_file_changes"] = args.track_changes
    if hasattr(args, "force") and args.force:
        processing_config["force_reprocess"] = args.force

    # For duplicate_strategy, we can use the value directly since it has an explicit default
    if hasattr(args, "duplicate_strategy"):
        processing_config["duplicate_strategy"] = args.duplicate_strategy

    if processing_config:
        engine_config["processing"] = processing_config

    # Create and run engine
    engine = ConversionEngine(config=engine_config, config_file=args.config)

    # Handle show config flag - show final configuration after all overrides
    if args.show_config:
        print("\nFinal Configuration (after all overrides):")
        print("=" * 50)
        engine.config.dump_config_json()
        print("=" * 50)

        # If no input provided and only showing config, exit here
        if not args.input:
            return

    # Run conversion
    stats = engine.run(args.input)

    print(f"\nConversion completed. Processed {stats['total_files']} files.")

    # Print storage stats if available
    if hasattr(engine.pipeline, "get_storage_stats"):
        storage_stats = engine.pipeline.get_storage_stats()
        print(f"Uploaded {storage_stats.get('uploaded_files', 0)} files to storage.")


if __name__ == "__main__":
    main()
