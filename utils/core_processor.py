"""
core_processor.py
-----------------
Executes the main batch processing logic driven by a MANIFEST.json.

Key improvements:
  - All paths come from config.py (no hardcoded C:\\DATA_PIPELINE)
  - Idempotent: copies files to archive instead of moving on first pass,
    then removes originals only on confirmed success — safe for Prefect retries
  - Structured error log written to 6_logs/ on failure
  - Real forex data loaded from the manifest's forex file
"""

import json
import os
import shutil
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from utils.config import ARCHIVE_DIR, ERROR_DIR, LOG_DIR, ensure_dirs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_manifest(manifest_file: str) -> dict:
    with open(manifest_file, "r", encoding="utf-8") as f:
        return json.load(f)


def _copy_to_folder(files: list, dest_folder: Path, logger=None):
    """Copies a list of file paths into dest_folder (skips missing files)."""
    for src in files:
        if os.path.exists(src):
            shutil.copy2(src, dest_folder / Path(src).name)
        else:
            if logger:
                logger.warning(f"File not found during archive copy: {src}")


def _remove_files(files: list, logger=None):
    """Deletes files from their original locations after successful copy."""
    for src in files:
        try:
            if os.path.exists(src):
                os.remove(src)
        except OSError as exc:
            if logger:
                logger.warning(f"Could not remove {src}: {exc}")


def _write_error_log(batch_id: str, error: Exception):
    ensure_dirs()
    log_path = LOG_DIR / f"{batch_id}_error.log"
    with open(log_path, "w", encoding="utf-8") as f:
        import traceback
        f.write(f"Batch: {batch_id}\n")
        f.write(f"Timestamp: {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"Error: {error}\n\n")
        f.write(traceback.format_exc())


# ---------------------------------------------------------------------------
# Core transformation
# ---------------------------------------------------------------------------

def _transform_data(manifest: dict, logger=None) -> pd.DataFrame:
    """
    Loads the forex, partner, and units files from the manifest and
    produces the processed output DataFrame.

    ⚙️  Replace or extend this function with your real business logic.
    Currently it:
      - Loads the exchange rates CSV
      - Adds a processing timestamp column
      - Returns the DataFrame ready for archiving
    """
    forex_path = manifest["files"]["forex"]

    if not os.path.exists(forex_path):
        raise FileNotFoundError(f"Forex file not found: {forex_path}")

    df = pd.read_csv(forex_path, encoding="utf-8-sig")

    if logger:
        logger.info(f"Loaded forex data: {len(df)} rows from {Path(forex_path).name}")

    # ----------------------------------------------------------------
    # Add your real transformation logic below.
    # Example: merge with partner data, apply business rules, etc.
    # ----------------------------------------------------------------
    df["Processed_At"] = datetime.now(timezone.utc).isoformat()

    # Load partner data if present and not a placeholder
    partners_path = manifest["files"].get("partners", "")
    if partners_path and os.path.exists(partners_path):
        try:
            partners_df = pd.read_csv(partners_path, encoding="utf-8-sig")
            if "Country" in partners_df.columns:
                df = df.merge(partners_df, on="Country", how="left")
                if logger:
                    logger.info(f"Merged partner data: {len(partners_df)} rows")
        except Exception as exc:
            if logger:
                logger.warning(f"Could not merge partner data: {exc}")

    return df


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_core_processing(manifest_file: str, logger=None):
    """
    Full batch processing cycle:
      1. Load manifest
      2. Transform data
      3. Write output CSV to archive
      4. Copy all source files to archive
      5. Remove originals
      On any failure: copy files to error folder and write error log.

    Idempotent design:
      Files are COPIED first, removed only after success — so Prefect
      retries find originals intact if processing fails mid-way.
    """
    ensure_dirs()

    manifest  = _load_manifest(manifest_file)
    batch_id  = manifest["batch_id"]

    all_files = (
        [manifest_file]
        + list(manifest["files"].values())
        + [f for f in manifest.get("raw_data", []) if os.path.exists(f)]
    )

    try:
        # ------------------------------------------------------------------
        # Transform
        # ------------------------------------------------------------------
        df = _transform_data(manifest, logger=logger)

        # ------------------------------------------------------------------
        # Archive
        # ------------------------------------------------------------------
        batch_archive = ARCHIVE_DIR / batch_id
        batch_archive.mkdir(parents=True, exist_ok=True)

        output_path = batch_archive / f"processed_output_{batch_id}.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

        if logger:
            logger.info(f"Output written: {output_path}")

        # Copy all source files into archive
        _copy_to_folder(all_files, batch_archive, logger=logger)

        # Remove originals only after confirmed successful copy
        _remove_files(all_files, logger=logger)

        # Write a success log
        success_log = LOG_DIR / f"{batch_id}_success.log"
        with open(success_log, "w", encoding="utf-8") as f:
            f.write(f"Batch {batch_id} completed successfully at "
                    f"{datetime.now(timezone.utc).isoformat()}\n")
            f.write(f"Output rows: {len(df)}\n")
            f.write(f"Archive: {batch_archive}\n")

        if logger:
            logger.info(f"Batch {batch_id} archived successfully.")

    except Exception as exc:
        # ------------------------------------------------------------------
        # Error handling — copy to error folder, write log
        # ------------------------------------------------------------------
        error_folder = ERROR_DIR / batch_id
        error_folder.mkdir(parents=True, exist_ok=True)
        _copy_to_folder(all_files, error_folder, logger=logger)
        _write_error_log(batch_id, exc)

        if logger:
            logger.error(f"Batch {batch_id} failed — files copied to {error_folder}")

        raise
