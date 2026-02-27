from abc import ABC, abstractmethod


class NotificationStrategy(ABC):
    @abstractmethod
    def send(self, event_type: str, severity: str, message: str, metadata: dict) -> None:
        raise NotImplementedError
