"""
process_batch_flow.py
---------------------
Flow 3 of 3 in the hybrid execution model.

Execution:  Local Python via Prefect worker (chained from Flow 2)
Monitoring: Prefect Cloud (logs, run history)

Triggered automatically by prepare_batch_flow on success.
Can also be run manually via: scripts\run_ProcessBatch.bat
"""

import os
from pathlib import Path

from prefect import flow, get_run_logger

from utils.core_processor import run_core_processing
from utils.config import HOT_DIR, ensure_dirs


@flow(name="process_batch_flow", retries=2, retry_delay_seconds=30)
def process_batch_flow(manifest_file: str = ""):
    """
    Step 3: Loads the MANIFEST.json, runs core transformation, and
    archives results to 4_archive/.

    Args:
        manifest_file: Path to manifest JSON. When called from
                       prepare_batch_flow this is always provided
                       explicitly. If empty (manual run), the latest
                       manifest in the hotfolder is used.

    Triggered by: prepare_batch_flow (local chain)
    Monitored by: Prefect Cloud
    """
    logger = get_run_logger()
    ensure_dirs()

    # ------------------------------------------------------------------
    # Resolve manifest path
    # ------------------------------------------------------------------
    if not manifest_file:
        # Manual re-run: pick the newest manifest by modification time
        # (getmtime is consistent across Windows and Linux)
        manifests = sorted(
            HOT_DIR.glob("*_MANIFEST.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        if not manifests:
            raise FileNotFoundError(f"No manifest files found in {HOT_DIR}")
        manifest_file = str(manifests[0])
        logger.info(f"Auto-selected latest manifest: {Path(manifest_file).name}")
    else:
        if not os.path.exists(manifest_file):
            raise FileNotFoundError(f"Manifest not found: {manifest_file}")
        logger.info(f"Using manifest: {Path(manifest_file).name}")

    # ------------------------------------------------------------------
    # Process
    # ------------------------------------------------------------------
    run_core_processing(manifest_file, logger=logger)
    logger.info("Batch processing completed successfully.")


if __name__ == "__main__":
    process_batch_flow()
