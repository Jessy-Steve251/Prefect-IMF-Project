"""
prepare_batch_flow.py
---------------------
Flow 2 of 3 in the hybrid execution model.

Execution:  Local Python via Prefect worker (chained from Flow 1)
Monitoring: Prefect Cloud (logs, run history)

Triggered automatically by currency_acquisition_flow on success.
Can also be run manually via: scripts\run_PrepareBatch.bat
"""

from prefect import flow, get_run_logger

from utils.batch_prepare import create_batch_manifest
from utils.config import ensure_dirs


@flow(name="prepare_batch_flow", retries=1, retry_delay_seconds=30)
def prepare_batch_flow():
    """
    Step 2: Prepares preprocessing artefacts from the latest exchange
    rate CSV and generates a MANIFEST.json.
    On success, chains directly into process_batch_flow.

    Triggered by: currency_acquisition_flow (local chain)
    Monitored by: Prefect Cloud
    """
    logger = get_run_logger()
    ensure_dirs()

    logger.info("Starting batch preparation...")

    manifest_path = create_batch_manifest(logger=logger)

    if not manifest_path:
        raise RuntimeError("create_batch_manifest() returned an empty path.")

    logger.info(f"Manifest created: {manifest_path}")

    # ------------------------------------------------------------------
    # Chain to Flow 3 — direct local call.
    # Passes the manifest path explicitly so Flow 3 uses exactly the
    # file we just created, not whatever happens to be newest on disk.
    # ------------------------------------------------------------------
    logger.info("Handing off to process_batch_flow...")
    from flows.process_batch_flow import process_batch_flow
    process_batch_flow(manifest_file=manifest_path)

    return manifest_path


if __name__ == "__main__":
    prepare_batch_flow()
