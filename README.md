# COG Conversion Engine

**Convert raster files to Cloud Optimized GeoTIFFs (COGs) with advanced features
including blob storage integration, duplicate detection, and efficient rerunning.**

## Features

- **Multi-format Support**: Convert GeoTIFF, WorldImage (JPG/PNG), and other raster formats to COGs
- **Blob Storage Integration**: Optional upload to Azure Blob Storage (or other providers)
- **Efficient Rerunning**: Skip already processed files, detect duplicates, and track file changes
- **Metadata Tracking**: SQLite database tracks conversion history and statistics
- **Error Handling**: Robust retry mechanism with detailed error logging
- **Configuration Management**: JSON-based configuration with CLI overrides
- **Performance Optimization**: Parallel processing with configurable worker limits

## Installation

### Prerequisites

- Python 3.12+
- GDAL 3.12.0+
- Azure Storage Blob SDK (for storage integration)

### Install with UV (recommended)

```bash
# Clone the repository
git clone https://github.com/your-repo/cog_converter.git
cd cog_converter

# Install dependencies using UV
uv pip install -e .
```

### Install with pip

```bash
pip install -e .
```

## Quick Start

### Basic Conversion

```bash
# Convert files in a directory to COGs
python -m cog_converter /path/to/raster/files --output ./cog_output
```

### With Configuration File

```bash
# Use a configuration file
python -m cog_converter /path/to/raster/files --config sample_config.json
```

### Enable Blob Storage

```bash
# Convert and upload to Azure Blob Storage
python -m cog_converter /path/to/raster/files \
  --enable-storage \
  --connection-string "your_connection_string" \
  --container "your-container-name"
```

## Configuration

The engine uses a hierarchical configuration system with sensible defaults. Configuration can be provided via:

1. **Default values** (built-in)
2. **JSON configuration file** (optional)
3. **CLI arguments** (override everything)

### Sample Configuration

Run `python -m cog_converter --default-config` for a complete example with all available options.

### Key Configuration Sections

- **`temp_directory`**: Directory for intermediate files
- **`output_directory`**: Where to save COG files
- **`cog_parameters`**: COG creation parameters (compression, blocksize, etc.)
- **`supported_formats`**: File extensions to recognize as raster files
- **`error_handling`**: Retry settings and error logging
- **`performance`**: Worker count and memory limits
- **`storage`**: Blob storage configuration
- **`metadata`**: Database settings for tracking conversions
- **`processing`**: Skip processed files, detect duplicates, etc.

## CLI Options

```bash
# Show help
python -m cog_converter --help

# Dump default configuration
python -m cog_converter --default-config

# Show final configuration after all overrides
python -m cog_converter input_path --show-config

# Force reprocessing of all files
python -m cog_converter input_path --force

# Skip already processed files (default behavior)
python -m cog_converter input_path --skip-processed

# Disable duplicate detection
python -m cog_converter input_path --no-detect-duplicates

# Disable file change tracking
python -m cog_converter input_path --no-track-changes
```

## Advanced Features

### Efficient Rerunning

The engine tracks which files have been processed and can skip them on subsequent runs:

```bash
# First run - process all files
python -m cog_converter /data/rasters --output ./cogs

# Second run - only process new/modified files
python -m cog_converter /data/rasters --output ./cogs --skip-processed

# Force reprocess everything
python -m cog_converter /data/rasters --output ./cogs --force
```

### Duplicate Detection

When enabled, the engine detects duplicate files based on content hash and can reference
existing COGs instead of reprocessing:

```bash
# Enable duplicate detection (default)
python -m cog_converter input_path --detect-duplicates

# Disable duplicate detection
python -m cog_converter input_path --no-detect-duplicates
```

### Blob Storage Integration

Upload converted COGs directly to cloud storage:

```bash
# Basic storage upload
python -m cog_converter input_path \
  --enable-storage \
  --connection-string "your_connection_string" \
  --container "container-name"

# Upload only successful conversions
python -m cog_converter input_path \
  --enable-storage \
  --connection-string "your_connection_string" \
  --container "container-name"
```

## Development

### Running Tests

```bash
# Install development dependencies
uv pip install -e ".[dev]"

# Run tests (test files will be added)
pytest tests/
```

### Code Quality

```bash
# Format code
uv run black .
uv run isort .

# Lint code
uv run ruff check .
```

### Project Structure

```
cog_converter/
├── converters/          # Format-specific converters
├── storage/             # Storage integration modules
├── config.py            # Configuration management
├── engine.py            # Main conversion engine
├── pipeline.py          # Conversion pipeline
├── file_discoverer.py   # File discovery logic
└── error_handler.py     # Error handling
```

## Performance Optimization

Configure performance settings in your config file:

```json
{
  "performance": {
    "max_workers": 8,        # Number of parallel workers
    "memory_limit_mb": 8192  # Memory limit per worker
  }
}
```

## Error Handling

The engine includes robust error handling with:

- Automatic retries (configurable count and delay)
- Detailed error logging to `conversion_errors.log`
- Statistics tracking for failed conversions

## Metadata Tracking

All conversion runs are tracked in a SQLite database (`conversion_metadata.db` by default):

- File processing history
- Conversion statistics
- Run metadata
- Duplicate detection information

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Support

For issues, questions, or contributions, please open an issue or pull request on GitHub.

## Roadmap

- Support for additional cloud providers (AWS S3, Google Cloud Storage)
- Enhanced duplicate detection algorithms
- Web-based monitoring dashboard
- API for programmatic integration
- Comprehensive test suite
