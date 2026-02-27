import os
from datetime import datetime, timezone

from notifier.strategies.base import NotificationStrategy


class FlagFileStrategy(NotificationStrategy):
    def __init__(self, flag_dir: str = "/app/alerts"):
        self.flag_dir = flag_dir
        os.makedirs(self.flag_dir, exist_ok=True)

    def send(self, event_type: str, severity: str, message: str, metadata: dict) -> None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{severity}_{event_type}.flag"
        path = os.path.join(self.flag_dir, filename)
        content = f"{severity}|{event_type}|{message}\n{metadata}"

        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content)
