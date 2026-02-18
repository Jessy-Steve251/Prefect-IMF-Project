import os
import time
from prefect.events import emit_event

WATCH_FOLDER = r"C:\DATA_PIPELINE\3_processing_hotfolder"
EVENT_NAME = "local.manifest.created"


def watcher(interval=5):
    print("Starting Prefect hotfolder watcher...")
    print(f"Monitoring: {WATCH_FOLDER}")
    seen = set()

    while True:
        for f in os.listdir(WATCH_FOLDER):
            if f.endswith("_MANIFEST.json") and f not in seen:
                filepath = os.path.join(WATCH_FOLDER, f)
                seen.add(f)

                print(f"Detected new manifest: {filepath}")

                emit_event(
                    event=EVENT_NAME,
                    resource={
                        "file_path": filepath,
                        "event_type": "manifest_ready"
                    }
                )

                print("Event emitted to Prefect Cloud.")

        time.sleep(interval)


if __name__ == "__main__":
    watcher()
