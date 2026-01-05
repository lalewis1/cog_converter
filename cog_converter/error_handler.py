#!/usr/bin/env python3
"""
Error Handling System for COG Conversion Engine
"""

import traceback
from datetime import datetime


class ErrorHandler:
    """Handles error logging and retry logic for the conversion process"""

    def __init__(self, config: dict):
        self.config = config
        self.log_file = config.get("error_log", "conversion_errors.log")
        self.max_retries = config.get("max_retries", 3)
        self.retry_delay = config.get("retry_delay", 5)  # seconds

        # Ensure log file exists
        with open(self.log_file, "a") as f:
            f.write(f"\n{'='*50}\n")
            f.write(f"COG Conversion Log - {datetime.now()}\n")
            f.write(f"{'='*50}\n\n")

    def log_success(self, input_path: str, output_path: str):
        """Log successful conversion"""
        with open(self.log_file, "a") as f:
            f.write(f"SUCCESS: {datetime.now()} - {input_path} -> {output_path}\n")

    def log_skip(self, file_path: str, reason: str):
        """Log skipped files"""
        with open(self.log_file, "a") as f:
            f.write(f"SKIP: {datetime.now()} - {file_path} - {reason}\n")

    def log_failure(self, file_path: str, reason: str):
        """Log conversion failures"""
        with open(self.log_file, "a") as f:
            f.write(f"FAIL: {datetime.now()} - {file_path} - {reason}\n")

    def log_exception(self, file_path: str, exception: Exception):
        """Log exceptions with stack trace"""
        with open(self.log_file, "a") as f:
            f.write(f"EXCEPTION: {datetime.now()} - {file_path}\n")
            f.write(f"  {str(exception)}\n")
            f.write(f"  {traceback.format_exc()}\n")

    def should_retry(self, attempt: int) -> bool:
        """Determine if operation should be retried"""
        return attempt < self.max_retries

    def log_retry(self, file_path: str, attempt: int, error: str):
        """Log retry attempts"""
        with open(self.log_file, "a") as f:
            f.write(
                f"RETRY {attempt}/{self.max_retries}: {datetime.now()} - {file_path} - {error}\n"
            )

    def get_retry_delay(self) -> int:
        """Get delay between retries in seconds"""
        return self.retry_delay
