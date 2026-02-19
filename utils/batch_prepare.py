import os
import json
from datetime import datetime
from pathlib import Path

# Use a relative path that works in any environment
BASE_DIR = os.path.join(os.getcwd(), "data_pipeline")
INPUT_DIR = os.path.join(BASE_DIR, "1_input")
PRE_DIR = os.path.join(BASE_DIR, "2_preprocessing")
HOT_DIR = os.path.join(BASE_DIR, "3_processing_hotfolder")

def create_batch_manifest():
    """
    Prepares the batch (partners, units, forex merged files),
    then generates a _MANIFEST.json file containing all required metadata.
    """

    # Step 1: Create batch ID
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    manifest_filename = f"{batch_id}_MANIFEST.json"
    manifest_path = os.path.join(HOT_DIR, manifest_filename)

    # Step 2: Run preprocessing logic
    # (You fill in your actual business logic)
    partners_file = os.path.join(PRE_DIR, f"Partner_Data_{batch_id}.csv")
    units_file = os.path.join(PRE_DIR, f"Merged_Units_{batch_id}.csv")
    forex_file = os.path.join(PRE_DIR, f"Forex_{batch_id}.csv")

    # Ensure directories exist
    os.makedirs(PRE_DIR, exist_ok=True)
    os.makedirs(HOT_DIR, exist_ok=True)

    # TODO: Replace these with your real processing functions
    Path(partners_file).write_text("partner sample data")
    Path(units_file).write_text("units sample data")
    Path(forex_file).write_text("forex sample data")

    # Raw file list (your real files from 1_input)
    raw_files = [
        str(f) for f in Path(INPUT_DIR).glob("*.csv")
    ]

    # Step 3: Build manifest dictionary
    manifest = {
        "batch_id": batch_id,
        "creation_timestamp": datetime.utcnow().isoformat(),
        "status": "READY_FOR_PROCESSING",
        "files": {
            "partners": partners_file,
            "units": units_file,
            "forex": forex_file
        },
        "raw_data": raw_files,
    }

    # Step 4: Save manifest
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=4)

    return manifest_path
