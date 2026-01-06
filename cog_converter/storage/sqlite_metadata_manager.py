#!/usr/bin/env python3
"""
SQLite Database Backend for COG Conversion Metadata

This module provides a SQLite-based metadata manager that replaces the JSON backend.
It offers better performance, scalability, and multi-run support.
"""

import json
import logging
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional


class SQLiteMetadataManager:
    """
    SQLite-based metadata manager for COG conversion tracking.

    This class handles all metadata operations using SQLite database,
    providing efficient storage, fast queries, and support for multiple
    conversion runs.
    """

    def __init__(self, database_file: str = "conversion_metadata.db"):
        """
        Initialize SQLite metadata manager.

        Args:
            database_file: Path to SQLite database file
        """
        self.database_file = database_file
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self._initialize_database()

    def _initialize_database(self):
        """Initialize database connection and create tables if needed."""
        try:
            # Create database directory if it doesn't exist
            db_dir = os.path.dirname(self.database_file)
            if (
                db_dir
            ):  # Only create directory if it's not empty (i.e., not current directory)
                os.makedirs(db_dir, exist_ok=True)

            # Connect to database with optimized settings
            self.connection = sqlite3.connect(
                self.database_file,
                timeout=30.0,  # 30 second timeout for concurrent access
                isolation_level=None,  # Use explicit transactions
                check_same_thread=False,
            )

            # Optimize database settings
            self.connection.execute(
                "PRAGMA journal_mode=WAL"
            )  # Better for concurrent access
            self.connection.execute(
                "PRAGMA synchronous=NORMAL"
            )  # Balance between safety and speed
            self.connection.execute(
                "PRAGMA foreign_keys=ON"
            )  # Enable foreign key constraints
            self.connection.execute("PRAGMA busy_timeout=5000")  # 5 second busy timeout

            # Create tables
            self._create_tables()

            # Ensure database schema is up to date
            self._ensure_schema_up_to_date()

            self.logger.info(f"Initialized SQLite database: {self.database_file}")

        except Exception as e:
            self.logger.error(f"Failed to initialize database: {str(e)}")
            raise

    def _create_tables(self):
        """Create database tables if they don't exist."""
        cursor = self.connection.cursor()

        # SQL for creating all tables
        create_sql = """
        CREATE TABLE IF NOT EXISTS conversions (
            conversion_id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_file_path TEXT NOT NULL UNIQUE,
            cog_file_path TEXT,
            blob_path TEXT,
            content_hash TEXT NOT NULL,
            blob_url TEXT,
            original_file_size INTEGER,
            cog_file_size INTEGER,
            conversion_timestamp TEXT NOT NULL,
            file_modification_time REAL,
            status TEXT NOT NULL,
            duplicate_of_conversion_id INTEGER,
            duplicate_of_file_path TEXT,
            is_duplicate BOOLEAN DEFAULT FALSE,
            run_id INTEGER,
            FOREIGN KEY (duplicate_of_conversion_id) REFERENCES conversions(conversion_id),
            FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS processing_state (
            file_path TEXT NOT NULL,
            run_id INTEGER,
            status TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            last_processed TEXT NOT NULL,
            file_modification_time REAL,
            PRIMARY KEY (file_path, run_id),
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );

        CREATE TABLE IF NOT EXISTS content_hash_index (
            content_hash TEXT NOT NULL,
            file_path TEXT NOT NULL,
            PRIMARY KEY (content_hash, file_path)
        );

        CREATE TABLE IF NOT EXISTS runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time TEXT NOT NULL,
            end_time TEXT,
            input_directory TEXT NOT NULL,
            total_files INTEGER DEFAULT 0,
            successful INTEGER DEFAULT 0,
            failed INTEGER DEFAULT 0,
            skipped INTEGER DEFAULT 0,
            duplicates_referenced INTEGER DEFAULT 0,
            config_snapshot TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_conversions_content_hash ON conversions(content_hash);
        CREATE INDEX IF NOT EXISTS idx_conversions_status ON conversions(status);
        CREATE INDEX IF NOT EXISTS idx_conversions_blob_path ON conversions(blob_path);
        CREATE INDEX IF NOT EXISTS idx_processing_state_status ON processing_state(status);
        CREATE INDEX IF NOT EXISTS idx_runs_input_directory ON runs(input_directory);
        """

        cursor.executescript(create_sql)
        self.connection.commit()

    def _ensure_schema_up_to_date(self):
        """Ensure database schema is up to date with required columns."""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Check if error_message and error_type columns exist
            cursor.execute("PRAGMA table_info(conversions)")
            columns = [column[1] for column in cursor.fetchall()]

            # Add missing columns if they don't exist
            if "error_message" not in columns:
                cursor.execute("ALTER TABLE conversions ADD COLUMN error_message TEXT")
                self.logger.info("Added error_message column to conversions table")

            if "error_type" not in columns:
                cursor.execute("ALTER TABLE conversions ADD COLUMN error_type TEXT")
                self.logger.info("Added error_type column to conversions table")

            if "failed_timestamp" not in columns:
                cursor.execute(
                    "ALTER TABLE conversions ADD COLUMN failed_timestamp TEXT"
                )
                self.logger.info("Added failed_timestamp column to conversions table")

            # Add columns for additional metadata (upload information)
            if "upload_timestamp" not in columns:
                cursor.execute(
                    "ALTER TABLE conversions ADD COLUMN upload_timestamp TEXT"
                )
                self.logger.info("Added upload_timestamp column to conversions table")

            if "upload_content_hash" not in columns:
                cursor.execute(
                    "ALTER TABLE conversions ADD COLUMN upload_content_hash TEXT"
                )
                self.logger.info(
                    "Added upload_content_hash column to conversions table"
                )

            conn.commit()

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update schema: {str(e)}")
            raise

    def _get_connection(self):
        """Get database connection, ensuring it's valid."""
        if self.connection is None:
            self._initialize_database()
        return self.connection

    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def __del__(self):
        """Destructor to ensure connection is closed."""
        self.close()

    # Core metadata operations
    def add_conversion_record(
        self,
        original_file_path: str,
        cog_file_path: str,
        blob_path: str,
        content_hash: str,
        blob_url: Optional[str] = None,
        additional_metadata: Optional[Dict[str, Any]] = None,
        run_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Add a conversion record to the database.

        Args:
            original_file_path: Path to the original file
            cog_file_path: Path to the converted COG file
            blob_path: Path in blob storage
            content_hash: Content hash of the original file
            blob_url: Optional URL to access the blob
            additional_metadata: Additional metadata to include
            run_id: Optional run ID for tracking

        Returns:
            The created conversion record with conversion_id
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Calculate file stats
        file_size = (
            os.path.getsize(original_file_path)
            if os.path.exists(original_file_path)
            else 0
        )
        cog_size = (
            os.path.getsize(cog_file_path) if os.path.exists(cog_file_path) else 0
        )
        file_mtime = (
            os.path.getmtime(original_file_path)
            if os.path.exists(original_file_path)
            else 0
        )

        # Prepare data
        record_data = {
            "original_file_path": original_file_path,
            "cog_file_path": cog_file_path,
            "blob_path": blob_path,
            "content_hash": content_hash,
            "blob_url": blob_url,
            "original_file_size": file_size,
            "cog_file_size": cog_size,
            "conversion_timestamp": datetime.now().isoformat(),
            "file_modification_time": file_mtime,
            "status": "completed",
            "is_duplicate": False,
            "run_id": run_id,
        }

        # Add additional metadata to main record if needed
        if additional_metadata:
            record_data.update(additional_metadata)

        try:
            # Begin transaction
            cursor.execute("BEGIN TRANSACTION")

            # Insert into conversions table
            columns = ", ".join(record_data.keys())
            placeholders = ", ".join(["?"] * len(record_data))
            sql = f"INSERT INTO conversions ({columns}) VALUES ({placeholders})"

            cursor.execute(sql, list(record_data.values()))
            conversion_id = cursor.lastrowid

            # Update processing state
            self._update_processing_state(
                original_file_path, content_hash, "completed", file_mtime, run_id
            )

            # Update content hash index
            self._update_content_hash_index(original_file_path, content_hash)

            # Commit transaction
            conn.commit()

            record_data["conversion_id"] = conversion_id
            self.logger.info(
                f"Added conversion record {conversion_id}: {original_file_path}"
            )

            return record_data

        except sqlite3.IntegrityError as e:
            conn.rollback()
            self.logger.warning(
                f"Integrity error adding {original_file_path}: {str(e)}"
            )
            # Check if this is actually a duplicate file path
            cursor.execute(
                "SELECT original_file_path FROM conversions WHERE original_file_path = ?",
                (original_file_path,),
            )
            existing = cursor.fetchone()
            if existing:
                self.logger.info(
                    f"File path already exists, updating record: {original_file_path}"
                )
                # Update existing record instead
                return self._update_existing_record(original_file_path, record_data)
            else:
                # This is unexpected - re-raise the error
                self.logger.error(
                    f"Unexpected integrity error for non-existent file path: {original_file_path}"
                )
                raise
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to add conversion record: {str(e)}")
            raise

    def _update_existing_record(
        self, file_path: str, new_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update existing conversion record."""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Get existing record
            cursor.execute(
                "SELECT * FROM conversions WHERE original_file_path = ?", (file_path,)
            )
            existing = cursor.fetchone()

            if existing:
                # Update with new data, preserving conversion_id
                update_data = {}
                for key, value in new_data.items():
                    if key != "conversion_id":  # Don't update conversion_id
                        update_data[key] = value

                # Build UPDATE statement
                set_clause = ", ".join([f"{key} = ?" for key in update_data.keys()])
                sql = (
                    f"UPDATE conversions SET {set_clause} WHERE original_file_path = ?"
                )

                cursor.execute(sql, list(update_data.values()) + [file_path])

                # Also update content hash index if content_hash changed
                if "content_hash" in update_data:
                    # Remove old content hash index entry
                    cursor.execute(
                        "DELETE FROM content_hash_index WHERE file_path = ?",
                        (file_path,),
                    )
                    # Add new content hash index entry
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO content_hash_index
                        (content_hash, file_path)
                        VALUES (?, ?)
                    """,
                        (update_data["content_hash"], file_path),
                    )

                conn.commit()

                update_data["conversion_id"] = existing[0]  # Preserve original ID
                self.logger.info(
                    f"Updated existing record {existing[0]} for {file_path}"
                )
                return update_data

            return new_data

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update existing record: {str(e)}")
            raise

    def _update_processing_state(
        self,
        file_path: str,
        content_hash: str,
        status: str,
        file_modification_time: float,
        run_id: Optional[int] = None,
    ) -> None:
        """Update the processing state for a file."""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT OR REPLACE INTO processing_state
                (file_path, status, content_hash, last_processed, file_modification_time, run_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    file_path,
                    status,
                    content_hash,
                    datetime.now().isoformat(),
                    file_modification_time,
                    run_id,
                ),
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update processing state: {str(e)}")
            raise

    def _update_content_hash_index(self, file_path: str, content_hash: str) -> None:
        """Update the content hash index for duplicate detection."""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                INSERT OR IGNORE INTO content_hash_index
                (content_hash, file_path)
                VALUES (?, ?)
            """,
                (content_hash, file_path),
            )
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to update content hash index: {str(e)}")
            raise

    def add_failed_conversion(
        self,
        original_file_path: str,
        error_message: str,
        error_type: Optional[str] = None,
        run_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Add a record for a failed conversion.

        Args:
            original_file_path: Path to the original file
            error_message: Error message
            error_type: Type of error
            run_id: Optional run ID for tracking

        Returns:
            The created failed conversion record
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Calculate file stats
        file_size = (
            os.path.getsize(original_file_path)
            if os.path.exists(original_file_path)
            else 0
        )
        file_mtime = (
            os.path.getmtime(original_file_path)
            if os.path.exists(original_file_path)
            else 0
        )

        # Calculate content hash for tracking
        try:
            content_hash = self._calculate_content_hash(original_file_path)
        except Exception:
            content_hash = "unknown"

        # Create failed conversion record
        record = {
            "original_file_path": original_file_path,
            "cog_file_path": "",  # No COG file created
            "blob_path": "",  # No blob created
            "content_hash": content_hash,
            "blob_url": None,
            "original_file_size": file_size,
            "cog_file_size": 0,  # No COG file
            "conversion_timestamp": datetime.now().isoformat(),
            "file_modification_time": file_mtime,
            "status": "failed",
            "error_message": error_message,
            "error_type": error_type,
            "failed_timestamp": datetime.now().isoformat(),
            "run_id": run_id,
        }

        try:
            # Begin transaction
            cursor.execute("BEGIN TRANSACTION")

            # Insert into conversions table
            columns = ", ".join(record.keys())
            placeholders = ", ".join(["?"] * len(record))
            sql = f"INSERT INTO conversions ({columns}) VALUES ({placeholders})"

            cursor.execute(sql, list(record.values()))
            conversion_id = cursor.lastrowid

            # Update processing state
            self._update_processing_state(
                original_file_path, content_hash, "failed", file_mtime, run_id
            )

            # Update content hash index
            self._update_content_hash_index(original_file_path, content_hash)

            # Commit transaction
            conn.commit()

            record["conversion_id"] = conversion_id
            self.logger.error(
                f"Failed conversion recorded for {original_file_path}: {error_message}"
            )

            return record

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to add failed conversion record: {str(e)}")
            raise

    def _calculate_content_hash(self, file_path: str) -> str:
        """Calculate content hash for a file."""
        try:
            from .hash_utils import calculate_content_hash

            return calculate_content_hash(file_path)
        except Exception as e:
            self.logger.warning(f"Failed to calculate content hash: {str(e)}")
            return "unknown"

    def mark_file_failed(
        self, file_path: str, error_message: str, run_id: Optional[int] = None
    ) -> None:
        """
        Mark a file as failed with an error message.

        Args:
            file_path: Path to the file
            error_message: Error message
            run_id: Optional run ID for tracking
        """
        # Calculate content hash for tracking
        try:
            content_hash = self._calculate_content_hash(file_path)
        except Exception:
            content_hash = "unknown"

        # Update processing state
        file_mtime = os.path.getmtime(file_path) if os.path.exists(file_path) else 0
        self._update_processing_state(
            file_path, content_hash, "failed", file_mtime, run_id
        )

        # Add to content hash index
        self._update_content_hash_index(file_path, content_hash)

        # Add failed conversion record
        self.add_failed_conversion(
            original_file_path=file_path,
            error_message=error_message,
            error_type="processing_failed",
            run_id=run_id,
        )

    def create_conversion_record(
        self,
        original_file_path: str,
        cog_file_path: str,
        run_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Create a conversion record for local COG files (no blob storage).

        Args:
            original_file_path: Original file path
            cog_file_path: Path to the converted COG file
            run_id: Optional run ID for tracking

        Returns:
            Created conversion record
        """
        # Calculate content hash from the original file
        content_hash = self._calculate_content_hash(original_file_path)

        return self.add_conversion_record(
            original_file_path=original_file_path,
            cog_file_path=cog_file_path,
            blob_path="",  # No blob storage
            content_hash=content_hash,
            blob_url=None,  # No blob URL
            run_id=run_id,
        )

    def create_conversion_record_from_upload(
        self,
        original_file_path: str,
        upload_result: Dict[str, Any],
        run_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Create a conversion record from an upload result dictionary.

        Args:
            original_file_path: Original file path
            upload_result: Upload result from BlobStorageUploader.upload_with_metadata()
            run_id: Optional run ID for tracking

        Returns:
            Created conversion record
        """
        # Calculate content hash from the original file for consistency
        original_content_hash = self._calculate_content_hash(original_file_path)

        return self.add_conversion_record(
            original_file_path=original_file_path,
            cog_file_path=original_file_path,  # This should be the actual COG path
            blob_path=upload_result["blob_path"],
            content_hash=original_content_hash,  # Use the hash from original file
            blob_url=upload_result["blob_url"],
            additional_metadata={
                "upload_timestamp": upload_result["upload_timestamp"],
                "upload_content_hash": upload_result[
                    "content_hash"
                ],  # Store upload hash too
            },
            run_id=run_id,
        )

    def _file_has_conversion_record(self, file_path: str) -> bool:
        """
        Check if a file already has a conversion record.

        Args:
            file_path: Path to the file to check

        Returns:
            True if the file has a conversion record, False otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM conversions WHERE original_file_path = ?", (file_path,)
        )
        return cursor.fetchone() is not None

    def mark_file_skipped(
        self, file_path: str, reason: str, run_id: Optional[int] = None
    ) -> None:
        """
        Mark a file as skipped with a reason.

        Args:
            file_path: Path to the file
            reason: Reason for skipping
            run_id: Optional run ID for tracking
        """
        # Calculate content hash for tracking
        try:
            content_hash = self._calculate_content_hash(file_path)
        except Exception:
            content_hash = "unknown"

        # Update processing state
        file_mtime = os.path.getmtime(file_path) if os.path.exists(file_path) else 0
        self._update_processing_state(
            file_path, content_hash, "skipped", file_mtime, run_id
        )

        # Add to content hash index
        self._update_content_hash_index(file_path, content_hash)

        # Only add failed conversion record if file doesn't already have a conversion record
        if not self._file_has_conversion_record(file_path):
            self.add_failed_conversion(
                original_file_path=file_path,
                error_message=f"Skipped: {reason}",
                error_type="skipped",
                run_id=run_id,
            )
        else:
            self.logger.debug(
                f"File {file_path} already has a conversion record, skipping failed conversion entry"
            )

    def should_process_file(
        self, file_path: str, force: bool = False, current_run_id: Optional[int] = None
    ) -> bool:
        """
        Determine if a file should be processed based on its state and modification time.

        Args:
            file_path: Path to the file to check
            force: If True, force processing regardless of previous state
            current_run_id: Current run ID for tracking

        Returns:
            True if file should be processed, False if it can be skipped
        """
        if force:
            return True

        if not os.path.exists(file_path):
            return False

        current_mtime = os.path.getmtime(file_path)

        # Get processing state from ANY run (not just current)
        state = self._get_processing_state(file_path)

        if state is None:
            # File has never been processed
            return True

        # Check if file has been modified since last processing
        last_mtime = state.get("file_modification_time", 0)

        if current_mtime > last_mtime:
            # File has been modified
            return True

        # Check if processing was successful
        if state.get("status") in ["completed", "duplicate_referenced", "skipped"]:
            # File was successfully processed and hasn't changed
            # Update processing state to current run but still skip
            if current_run_id:
                self._update_processing_state_for_run(
                    file_path,
                    state["content_hash"],
                    "skipped",
                    current_mtime,
                    current_run_id,
                )
            return False

        # For failed or skipped files, we might want to retry
        return True

    def _get_processing_state(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Get the processing state for a specific file."""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT file_path, status, content_hash, last_processed, file_modification_time, run_id
                FROM processing_state
                WHERE file_path = ?
            """,
                (file_path,),
            )

            row = cursor.fetchone()
            if row:
                return {
                    "file_path": row[0],
                    "status": row[1],
                    "content_hash": row[2],
                    "last_processed": row[3],
                    "file_modification_time": row[4],
                    "run_id": row[5],
                }
            return None
        except Exception as e:
            self.logger.error(f"Failed to get processing state: {str(e)}")
            return None

    def _update_processing_state_for_run(
        self,
        file_path: str,
        content_hash: str,
        status: str,
        file_modification_time: float,
        run_id: int,
    ) -> None:
        """Update processing state specifically for a run."""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Check if this run already has a state for this file
            cursor.execute(
                """
                SELECT run_id FROM processing_state
                WHERE file_path = ? AND run_id = ?
            """,
                (file_path, run_id),
            )

            if cursor.fetchone():
                # Update existing run-specific state
                cursor.execute(
                    """
                    UPDATE processing_state
                    SET status = ?, last_processed = ?, file_modification_time = ?
                    WHERE file_path = ? AND run_id = ?
                """,
                    (
                        status,
                        datetime.now().isoformat(),
                        file_modification_time,
                        file_path,
                        run_id,
                    ),
                )
            else:
                # Insert new run-specific state
                cursor.execute(
                    """
                    INSERT INTO processing_state
                    (file_path, status, content_hash, last_processed, file_modification_time, run_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        file_path,
                        status,
                        content_hash,
                        datetime.now().isoformat(),
                        file_modification_time,
                        run_id,
                    ),
                )

            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(
                f"Failed to update run-specific processing state: {str(e)}"
            )
            raise

    def is_duplicate_content(self, content_hash: str, file_path: str) -> bool:
        """
        Check if content with the given hash has already been processed.

        Args:
            content_hash: Content hash to check
            file_path: Current file path (for comparison)

        Returns:
            True if duplicate content exists, False otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Count files with this content hash (excluding the current file_path if it exists)
            cursor.execute(
                """
                SELECT COUNT(*) FROM content_hash_index
                WHERE content_hash = ? AND file_path != ?
            """,
                (content_hash, file_path),
            )

            count = cursor.fetchone()[0]

            # If there's already at least one file with this hash (other than current file), it's a duplicate
            return count >= 1

        except Exception as e:
            self.logger.error(f"Failed to check duplicate content: {str(e)}")
            return False

    def get_duplicate_files(self, content_hash: str) -> List[str]:
        """
        Get all files that have the same content hash.

        Args:
            content_hash: Content hash to look up

        Returns:
            List of file paths with the same content hash
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT file_path FROM content_hash_index
                WHERE content_hash = ?
            """,
                (content_hash,),
            )

            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"Failed to get duplicate files: {str(e)}")
            return []

    def get_existing_blob_for_content(
        self, content_hash: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get existing blob information for content with the given hash.

        Args:
            content_hash: Content hash to look up

        Returns:
            Dictionary with blob_path and blob_url if found, None otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT blob_path, blob_url, conversion_id, original_file_path
                FROM conversions
                WHERE status = 'completed' AND content_hash = ? AND blob_path IS NOT NULL
                LIMIT 1
            """,
                (content_hash,),
            )

            row = cursor.fetchone()
            if row:
                return {
                    "blob_path": row[0],
                    "blob_url": row[1],
                    "original_conversion_id": row[2],
                    "original_file_path": row[3],
                }
            return None
        except Exception as e:
            self.logger.error(f"Failed to get existing blob: {str(e)}")
            return None

    def create_duplicate_reference(
        self,
        original_file_path: str,
        existing_blob_info: Dict[str, Any],
        content_hash: str,
        additional_metadata: Optional[Dict[str, Any]] = None,
        run_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Create a metadata record for a duplicate file that references an existing blob.

        Args:
            original_file_path: Path to the duplicate file
            existing_blob_info: Information about the existing blob to reference
            content_hash: Content hash of the file
            additional_metadata: Additional metadata to include
            run_id: Optional run ID for tracking

        Returns:
            The created duplicate reference record
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # Calculate file size
        file_size = (
            os.path.getsize(original_file_path)
            if os.path.exists(original_file_path)
            else 0
        )
        file_mtime = (
            os.path.getmtime(original_file_path)
            if os.path.exists(original_file_path)
            else 0
        )

        # Create duplicate reference record
        record = {
            "original_file_path": original_file_path,
            "cog_file_path": "",  # No separate COG file created
            "blob_path": existing_blob_info["blob_path"],
            "content_hash": content_hash,
            "blob_url": existing_blob_info["blob_url"],
            "original_file_size": file_size,
            "cog_file_size": 0,  # No separate COG file
            "conversion_timestamp": datetime.now().isoformat(),
            "file_modification_time": file_mtime,
            "status": "duplicate_referenced",
            "duplicate_of_conversion_id": existing_blob_info["original_conversion_id"],
            "duplicate_of_file_path": existing_blob_info["original_file_path"],
            "is_duplicate": True,
            "run_id": run_id,
        }

        # Add additional metadata
        if additional_metadata:
            record.update(additional_metadata)

        try:
            # Begin transaction
            cursor.execute("BEGIN TRANSACTION")

            # Insert into conversions table
            columns = ", ".join(record.keys())
            placeholders = ", ".join(["?"] * len(record))
            sql = f"INSERT INTO conversions ({columns}) VALUES ({placeholders})"

            cursor.execute(sql, list(record.values()))
            conversion_id = cursor.lastrowid

            # Update processing state
            self._update_processing_state(
                original_file_path,
                content_hash,
                "duplicate_referenced",
                file_mtime,
                run_id,
            )

            # Update content hash index
            self._update_content_hash_index(original_file_path, content_hash)

            # Commit transaction
            conn.commit()

            record["conversion_id"] = conversion_id
            self.logger.info(
                f"Created duplicate reference: {original_file_path} -> {existing_blob_info['blob_path']} "
                f"(duplicate of {existing_blob_info['original_file_path']})"
            )

            return record

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to create duplicate reference: {str(e)}")
            raise

    def handle_duplicate_file(
        self,
        file_path: str,
        content_hash: str,
        duplicate_strategy: str = "reference",
        additional_metadata: Optional[Dict[str, Any]] = None,
        run_id: Optional[int] = None,
    ) -> bool:
        """
        Handle a duplicate file according to the specified strategy.

        Args:
            file_path: Path to the duplicate file
            content_hash: Content hash of the file
            duplicate_strategy: Strategy to use (reference, skip, process)
            additional_metadata: Additional metadata to include
            run_id: Optional run ID for tracking

        Returns:
            True if handled successfully, False otherwise
        """
        # Check if we have an existing blob for this content
        existing_blob = self.get_existing_blob_for_content(content_hash)

        if not existing_blob:
            # No existing blob found
            self.logger.warning(
                f"No existing blob found for duplicate content hash {content_hash}"
            )
            return False

        if duplicate_strategy == "reference":
            # Create a reference to the existing blob
            self.create_duplicate_reference(
                original_file_path=file_path,
                existing_blob_info=existing_blob,
                content_hash=content_hash,
                additional_metadata=additional_metadata,
                run_id=run_id,
            )
            return True

        elif duplicate_strategy == "skip":
            # Just mark as skipped
            self._update_processing_state(
                file_path,
                content_hash,
                "skipped",
                os.path.getmtime(file_path) if os.path.exists(file_path) else 0,
                run_id,
            )
            return True

        elif duplicate_strategy == "process":
            # This would be handled by normal processing
            return False

        else:
            self.logger.warning(f"Unknown duplicate strategy: {duplicate_strategy}")
            return False

    # Run management methods
    def start_new_run(self, input_directory: str, config: dict) -> int:
        """
        Start a new conversion run and return run_id.

        Args:
            input_directory: Input directory for this run
            config: Configuration snapshot

        Returns:
            Run ID
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Store config snapshot as JSON
            config_snapshot = json.dumps(config, default=str)

            cursor.execute(
                """
                INSERT INTO runs (start_time, input_directory, config_snapshot)
                VALUES (?, ?, ?)
            """,
                (datetime.now().isoformat(), input_directory, config_snapshot),
            )

            run_id = cursor.lastrowid
            conn.commit()

            self.logger.info(
                f"Started new run {run_id} for directory: {input_directory}"
            )
            return run_id

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to start new run: {str(e)}")
            raise

    def end_run(self, run_id: int, stats: dict) -> bool:
        """
        End a conversion run and update statistics.

        Args:
            run_id: Run ID to end
            stats: Run statistics

        Returns:
            True if successful, False otherwise
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                UPDATE runs
                SET end_time = ?, total_files = ?, successful = ?,
                    failed = ?, skipped = ?, duplicates_referenced = ?
                WHERE run_id = ?
            """,
                (
                    datetime.now().isoformat(),
                    stats.get("total_files", 0),
                    stats.get("successful", 0),
                    stats.get("failed", 0),
                    stats.get("skipped", 0),
                    stats.get("duplicates_referenced", 0),
                    run_id,
                ),
            )

            conn.commit()
            self.logger.info(f"Completed run {run_id} with stats: {stats}")
            return True

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Failed to end run {run_id}: {str(e)}")
            return False

    def get_all_runs(self) -> List[Dict[str, Any]]:
        """
        Get all conversion runs.

        Returns:
            List of run records
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT run_id, start_time, end_time, input_directory,
                       total_files, successful, failed, skipped, duplicates_referenced
                FROM runs
                ORDER BY start_time DESC
            """
            )

            runs = []
            for row in cursor.fetchall():
                runs.append(
                    {
                        "run_id": row[0],
                        "start_time": row[1],
                        "end_time": row[2],
                        "input_directory": row[3],
                        "total_files": row[4],
                        "successful": row[5],
                        "failed": row[6],
                        "skipped": row[7],
                        "duplicates_referenced": row[8],
                    }
                )

            return runs

        except Exception as e:
            self.logger.error(f"Failed to get runs: {str(e)}")
            return []

    # Query and reporting methods
    def get_all_conversions(self) -> List[Dict[str, Any]]:
        """
        Get all conversion records.

        Returns:
            List of all conversion records
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT conversion_id, original_file_path, cog_file_path, blob_path,
                       content_hash, blob_url, original_file_size, cog_file_size,
                       conversion_timestamp, file_modification_time, status,
                       duplicate_of_conversion_id, duplicate_of_file_path, is_duplicate, run_id
                FROM conversions
                ORDER BY conversion_timestamp DESC
            """
            )

            conversions = []
            for row in cursor.fetchall():
                conversions.append(
                    {
                        "conversion_id": row[0],
                        "original_file_path": row[1],
                        "cog_file_path": row[2],
                        "blob_path": row[3],
                        "content_hash": row[4],
                        "blob_url": row[5],
                        "original_file_size": row[6],
                        "cog_file_size": row[7],
                        "conversion_timestamp": row[8],
                        "file_modification_time": row[9],
                        "status": row[10],
                        "duplicate_of_conversion_id": row[11],
                        "duplicate_of_file_path": row[12],
                        "is_duplicate": bool(row[13]),
                        "run_id": row[14],
                    }
                )

            return conversions

        except Exception as e:
            self.logger.error(f"Failed to get conversions: {str(e)}")
            return []

    def get_successful_conversions(self) -> List[Dict[str, Any]]:
        """
        Get all successful conversion records.

        Returns:
            List of successful conversion records
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT conversion_id, original_file_path, cog_file_path, blob_path,
                       content_hash, blob_url, original_file_size, cog_file_size,
                       conversion_timestamp, file_modification_time, status,
                       duplicate_of_conversion_id, duplicate_of_file_path, is_duplicate, run_id
                FROM conversions
                WHERE status = 'completed'
                ORDER BY conversion_timestamp DESC
            """
            )

            conversions = []
            for row in cursor.fetchall():
                conversions.append(
                    {
                        "conversion_id": row[0],
                        "original_file_path": row[1],
                        "cog_file_path": row[2],
                        "blob_path": row[3],
                        "content_hash": row[4],
                        "blob_url": row[5],
                        "original_file_size": row[6],
                        "cog_file_size": row[7],
                        "conversion_timestamp": row[8],
                        "file_modification_time": row[9],
                        "status": row[10],
                        "duplicate_of_conversion_id": row[11],
                        "duplicate_of_file_path": row[12],
                        "is_duplicate": bool(row[13]),
                        "run_id": row[14],
                    }
                )

            return conversions

        except Exception as e:
            self.logger.error(f"Failed to get successful conversions: {str(e)}")
            return []

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about conversions.

        Returns:
            Dictionary with conversion statistics
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Get basic counts
            cursor.execute("SELECT COUNT(*) FROM conversions")
            total = cursor.fetchone()[0]

            cursor.execute(
                "SELECT COUNT(*) FROM conversions WHERE status = 'completed'"
            )
            successful = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM conversions WHERE status = 'failed'")
            failed = cursor.fetchone()[0]

            cursor.execute(
                "SELECT COUNT(*) FROM conversions WHERE status = 'duplicate_referenced'"
            )
            duplicates_referenced = cursor.fetchone()[0]

            # Get database file size
            db_size = (
                os.path.getsize(self.database_file)
                if os.path.exists(self.database_file)
                else 0
            )

            return {
                "total_conversions": total,
                "successful_conversions": successful,
                "failed_conversions": failed,
                "duplicates_referenced": duplicates_referenced,
                "success_rate": successful / total if total > 0 else 0.0,
                "database_file": self.database_file,
                "database_size_bytes": db_size,
                "last_updated": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error(f"Failed to get statistics: {str(e)}")
            return {
                "total_conversions": 0,
                "successful_conversions": 0,
                "failed_conversions": 0,
                "duplicates_referenced": 0,
                "success_rate": 0.0,
                "database_file": self.database_file,
                "database_size_bytes": 0,
                "last_updated": datetime.now().isoformat(),
            }

    # Database maintenance methods
    def create_backup(self) -> bool:
        """
        Create database backup.

        Returns:
            True if successful, False otherwise
        """
        try:
            backup_dir = os.path.join(os.path.dirname(self.database_file), "backups")
            os.makedirs(backup_dir, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = os.path.join(backup_dir, f"metadata_backup_{timestamp}.db")

            # Use sqlite3 backup API for atomic backup
            with sqlite3.connect(backup_file) as backup_conn:
                self.connection.backup(backup_conn)

            self.logger.info(f"Created database backup: {backup_file}")

            # Clean up old backups
            self._cleanup_old_backups(backup_dir)

            return True

        except Exception as e:
            self.logger.error(f"Backup failed: {str(e)}")
            return False

    def _cleanup_old_backups(self, backup_dir: str):
        """Clean up old backup files."""
        try:
            max_backups = 7  # Default value
            backups = []

            for filename in os.listdir(backup_dir):
                if filename.startswith("metadata_backup_") and filename.endswith(".db"):
                    file_path = os.path.join(backup_dir, filename)
                    backups.append((os.path.getmtime(file_path), file_path))

            # Sort by modification time (oldest first)
            backups.sort(key=lambda x: x[0])

            # Delete oldest backups if we exceed max_backups
            while len(backups) > max_backups:
                os.remove(backups[0][1])
                self.logger.info(f"Removed old backup: {backups[0][1]}")
                backups.pop(0)

        except Exception as e:
            self.logger.error(f"Backup cleanup failed: {str(e)}")

    def vacuum_database(self) -> bool:
        """
        Run VACUUM to optimize database.

        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute("VACUUM")
            self.connection.commit()
            self.logger.info("Database vacuum completed")
            return True
        except Exception as e:
            self.logger.error(f"VACUUM failed: {str(e)}")
            return False

    def export_to_json(self, output_file: str) -> bool:
        """
        Export database contents to JSON file for backup/compatibility.

        Args:
            output_file: Path to output JSON file

        Returns:
            True if successful, False otherwise
        """
        try:
            data = {
                "version": "2.0",
                "export_timestamp": datetime.now().isoformat(),
                "database_file": self.database_file,
                "conversions": self.get_all_conversions(),
                "processing_state": self._get_all_processing_state(),
                "content_hash_index": self._get_content_hash_index(),
                "runs": self.get_all_runs(),
            }

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            self.logger.info(f"Exported database to JSON: {output_file}")
            return True

        except Exception as e:
            self.logger.error(f"Export to JSON failed: {str(e)}")
            return False

    def _get_all_processing_state(self) -> Dict[str, Any]:
        """Get all processing state records."""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                "SELECT file_path, status, content_hash, last_processed, file_modification_time, run_id FROM processing_state"
            )

            state = {}
            for row in cursor.fetchall():
                state[row[0]] = {
                    "status": row[1],
                    "content_hash": row[2],
                    "last_processed": row[3],
                    "file_modification_time": row[4],
                    "run_id": row[5],
                }

            return state
        except Exception as e:
            self.logger.error(f"Failed to get processing state: {str(e)}")
            return {}

    def _get_content_hash_index(self) -> Dict[str, List[str]]:
        """Get content hash index."""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                "SELECT content_hash, file_path FROM content_hash_index ORDER BY content_hash"
            )

            index = {}
            for row in cursor.fetchall():
                if row[0] not in index:
                    index[row[0]] = []
                index[row[0]].append(row[1])

            return index
        except Exception as e:
            self.logger.error(f"Failed to get content hash index: {str(e)}")
            return {}

    # Context manager support
    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
