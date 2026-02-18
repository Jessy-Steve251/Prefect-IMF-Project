import json
import os
import shutil
from datetime import datetime
from pathlib import Path


BASE_DIR = r"C:\DATA_PIPELINE"
ARCHIVE_DIR = os.path.join(BASE_DIR, "4_archive")
ERROR_DIR = os.path.join(BASE_DIR, "5_error")
LOG_DIR = os.path.join(BASE_DIR, "6_logs")


def load_manifest(manifest_file):
    with open(manifest_file, "r", encoding="utf-8") as f:
        return json.load(f)


def run_core_processing(manifest_file: str):
    """
    Executes the main business logic:
    - Read manifest
    - Load files
    - Run core transformation
    - Archive or error folder movement
    """
    manifest = load_manifest(manifest_file)
    batch_id = manifest["batch_id"]

    try:
        # Load the files defined in manifest
        partners = manifest["files"]["partners"]
        units = manifest["files"]["units"]
        forex = manifest["files"]["forex"]
        raw_files = manifest["raw_data"]

        # TODO — insert your real processing logic here
        # Example:
        # processed_df = transform_data(partners, units, forex, raw_files)
        # processed_df.to_csv(...)

        # Fake output to show structure:
        output_file = f"{BASE_DIR}\\processed_output_{batch_id}.csv"
        Path(output_file).write_text("processed placeholder")

        # On success → move batch to archive
        batch_folder = os.path.join(ARCHIVE_DIR, batch_id)
        os.makedirs(batch_folder, exist_ok=True)

        shutil.move(manifest_file, os.path.join(batch_folder, os.path.basename(manifest_file)))

        for f in manifest["files"].values():
            shutil.move(f, os.path.join(batch_folder, os.path.basename(f)))

        for f in raw_files:
            if os.path.exists(f):
                shutil.move(f, os.path.join(batch_folder, os.path.basename(f)))

    except Exception as e:
        # On failure → move batch to error folder
        error_folder = os.path.join(ERROR_DIR, batch_id)
        os.makedirs(error_folder, exist_ok=True)

        shutil.move(manifest_file, os.path.join(error_folder, os.path.basename(manifest_file)))

        for f in manifest["files"].values():
            if os.path.exists(f):
                shutil.move(f, os.path.join(error_folder, os.path.basename(f)))

        for f in manifest["raw_data"]:
            if os.path.exists(f):
                shutil.move(f, os.path.join(error_folder, os.path.basename(f)))

        # Write error log
        log_file = os.path.join(LOG_DIR, f"{batch_id}_error.log")
        with open(log_file, "w", encoding="utf-8") as log:
            log.write(str(e))

        raise
