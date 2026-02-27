"""
Runs outside Docker on the Windows host.
Monitors a directory for .flag files and triggers local notifications.
"""

import argparse
import logging
import os
import time
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("local_watcher")


def parse_flag_filename(filename: str) -> dict:
    name = Path(filename).stem
    parts = name.split("_", 3)
    if len(parts) < 4:
        return {"severity": "UNKNOWN", "event_type": "UNKNOWN"}
    return {"severity": parts[2], "event_type": parts[3]}


def process_flag_file(path: str) -> dict:
    content = Path(path).read_text(encoding="utf-8")
    first_line = content.split("\n", 1)[0]
    parts = first_line.split("|", 2)
    severity = parts[0] if len(parts) > 0 else "UNKNOWN"
    event_type = parts[1] if len(parts) > 1 else "UNKNOWN"
    message = parts[2] if len(parts) > 2 else ""

    logger.warning("ALERT [%s] %s: %s", severity, event_type, message)

    try:
        from winotify import Notification, audio

        toast = Notification(
            app_id="ETL System",
            title=f"[{severity}] {event_type}",
            msg=message[:200],
            duration="long",
        )
        if severity == "CRITICAL":
            toast.set_audio(audio.Default, loop=False)
        toast.show()
    except ImportError:
        logger.info("winotify not available, skipping toast")
    except Exception as exc:
        logger.error("Toast failed: %s", exc)

    archive_path = path.replace(".flag", ".processed")
    os.rename(path, archive_path)

    return {"severity": severity, "event_type": event_type, "message": message}


def watch(flag_dir: str, poll_interval: int = 5):
    logger.info("Watching %s for .flag files every %ss", flag_dir, poll_interval)
    os.makedirs(flag_dir, exist_ok=True)
    while True:
        for filename in os.listdir(flag_dir):
            if filename.endswith(".flag"):
                process_flag_file(os.path.join(flag_dir, filename))
        time.sleep(poll_interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL local alert watcher")
    parser.add_argument("--dir", default="./alerts", help="Directory to watch")
    parser.add_argument("--interval", type=int, default=5, help="Poll interval in seconds")
    args = parser.parse_args()
    watch(args.dir, args.interval)
