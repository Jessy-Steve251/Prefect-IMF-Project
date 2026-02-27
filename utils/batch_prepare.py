"""
batch_prepare.py
----------------
Scans the 1_input directory, copies the latest exchange rate CSV into
the preprocessing folder, then generates a MANIFEST.json that the
process-batch flow consumes.

Real logic replaces the original TODO placeholders:
  - Finds the most recent exchange_rates_*.csv in data/
  - Copies it as the Forex file into 2_preprocessing/
  - Scans 1_input/ for any partner/unit CSVs placed there manually
  - Builds and writes the MANIFEST to 3_processing_hotfolder/
"""

import os
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from utils.config import (
    DATA_DIR, INPUT_DIR, PRE_DIR, HOT_DIR, ensure_dirs,
)


def _latest_exchange_rate_csv() -> Path | None:
    """Returns the most recently modified exchange_rates_*.csv in DATA_DIR."""
    candidates = sorted(
        DATA_DIR.glob("exchange_rates_[0-9][0-9][0-9][0-9]_[0-9][0-9].csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def create_batch_manifest(logger=None) -> str:
    """
    Prepares preprocessing artefacts and writes a MANIFEST.json.

    Returns:
        Absolute path to the created manifest file.

    Raises:
        FileNotFoundError: if no exchange rate CSV is available in data/.
    """
    ensure_dirs()

    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")

    # ------------------------------------------------------------------
    # 1. Locate the latest exchange rate file (Forex)
    # ------------------------------------------------------------------
    source_forex = _latest_exchange_rate_csv()
    if source_forex is None:
        raise FileNotFoundError(
            "No exchange_rates_YYYY_MM.csv found in data/ — "
            "run the currency_acquisition_flow first."
        )

    if logger:
        logger.info(f"Using exchange rate file: {source_forex.name}")

    # ------------------------------------------------------------------
    # 2. Copy Forex CSV into preprocessing folder
    # ------------------------------------------------------------------
    forex_dest = PRE_DIR / f"Forex_{batch_id}.csv"
    shutil.copy2(source_forex, forex_dest)
    if logger:
        logger.info(f"Copied forex data → {forex_dest.name}")

    # ------------------------------------------------------------------
    # 3. Discover partner and units files in 1_input/
    #    Convention:
    #      Partner files  → filenames containing "partner" (case-insensitive)
    #      Units files    → filenames containing "unit"    (case-insensitive)
    #    If none found, empty placeholder files are created so the
    #    manifest is always structurally valid for core_processor.
    # ------------------------------------------------------------------
    input_csvs = list(INPUT_DIR.glob("*.csv"))

    partner_files = [f for f in input_csvs if "partner" in f.name.lower()]
    units_files   = [f for f in input_csvs if "unit"    in f.name.lower()]

    # Merge partner files → single preprocessing file
    partners_dest = PRE_DIR / f"Partner_Data_{batch_id}.csv"
    if partner_files:
        import pandas as pd
        pd.concat([pd.read_csv(f, encoding="utf-8-sig") for f in partner_files]) \
          .to_csv(partners_dest, index=False, encoding="utf-8-sig")
        if logger:
            logger.info(f"Merged {len(partner_files)} partner file(s) → {partners_dest.name}")
    else:
        partners_dest.write_text("No partner data found", encoding="utf-8")
        if logger:
            logger.warning("No partner files found in 1_input/ — placeholder written")

    # Merge units files → single preprocessing file
    units_dest = PRE_DIR / f"Merged_Units_{batch_id}.csv"
    if units_files:
        import pandas as pd
        pd.concat([pd.read_csv(f, encoding="utf-8-sig") for f in units_files]) \
          .to_csv(units_dest, index=False, encoding="utf-8-sig")
        if logger:
            logger.info(f"Merged {len(units_files)} units file(s) → {units_dest.name}")
    else:
        units_dest.write_text("No units data found", encoding="utf-8")
        if logger:
            logger.warning("No units files found in 1_input/ — placeholder written")

    # ------------------------------------------------------------------
    # 4. Build and write MANIFEST
    # ------------------------------------------------------------------
    raw_files = [str(f) for f in input_csvs]

    manifest = {
        "batch_id":            batch_id,
        "creation_timestamp":  datetime.now(timezone.utc).isoformat(),
        "source_forex_file":   str(source_forex),
        "status":              "READY_FOR_PROCESSING",
        "files": {
            "partners": str(partners_dest),
            "units":    str(units_dest),
            "forex":    str(forex_dest),
        },
        "raw_data": raw_files,
    }

    manifest_path = HOT_DIR / f"{batch_id}_MANIFEST.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4, ensure_ascii=False)

    if logger:
        logger.info(f"Manifest written: {manifest_path.name}")

    return str(manifest_path)
