try:
    import httpx
except ModuleNotFoundError:
    httpx = None  # type: ignore[assignment]

from notifier.strategies.base import NotificationStrategy


class TelegramStrategy(NotificationStrategy):
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id

    def send(self, event_type: str, severity: str, message: str, metadata: dict) -> None:
        if httpx is None:
            raise RuntimeError("httpx is required for TelegramStrategy")

        text = f"[{severity}] {event_type}\n\n{message}"
        if metadata:
            details = "\n".join(f"- {k}: {v}" for k, v in metadata.items())
            text += f"\n\n{details}"

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        response = httpx.post(
            url,
            json={"chat_id": self.chat_id, "text": text},
            timeout=15,
        )
        response.raise_for_status()
