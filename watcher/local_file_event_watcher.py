"""
local_file_event_watcher.py
----------------------------
Monitors the processing hotfolder for new MANIFEST.json files and
emits a Prefect event when one is detected.

This is an optional companion process — it provides an event-driven
trigger alternative to the time-based cron chain. To use it, run this
script in a separate terminal alongside the Prefect worker:

    python watcher/local_file_event_watcher.py

The Prefect Cloud automation should listen for:
    event:    "local.manifest.created"
    resource: prefect.deployment.name = "process-batch/process-batch"
"""

import os
import time
from prefect.events import emit_event
from utils.config import HOT_DIR


def watcher(interval: int = 5):
    """
    Polls HOT_DIR every `interval` seconds for new *_MANIFEST.json files.
    Emits a Prefect event for each new file detected.
    """
    watch_folder = str(HOT_DIR)
    print(f"Starting hotfolder watcher...")
    print(f"Monitoring: {watch_folder}")
    print(f"Poll interval: {interval}s")

    # Ensure the folder exists before watching
    HOT_DIR.mkdir(parents=True, exist_ok=True)

    seen: set = set()

    while True:
        try:
            for filename in os.listdir(watch_folder):
                if filename.endswith("_MANIFEST.json") and filename not in seen:
                    filepath = os.path.join(watch_folder, filename)
                    seen.add(filename)

                    print(f"Detected new manifest: {filepath}")

                    emit_event(
                        event="local.manifest.created",
                        resource={
                            "prefect.resource.id": f"manifest:{filename}",
                            "file_path":           filepath,
                            "event_type":          "manifest_ready",
                        },
                    )
                    print(f"Event emitted for: {filename}")

        except FileNotFoundError:
            # Folder was temporarily unavailable — recreate and continue
            HOT_DIR.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            print(f"Watcher error: {exc}")

        time.sleep(interval)


if __name__ == "__main__":
    watcher()
