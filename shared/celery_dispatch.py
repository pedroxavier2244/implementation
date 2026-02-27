from functools import lru_cache

try:
    from celery import Celery
except ModuleNotFoundError:
    class _DummyResult:
        id = "mock-task-id"

    class Celery:  # type: ignore[override]
        def __init__(self, *args, **kwargs):
            pass

        def send_task(self, *args, **kwargs):
            return _DummyResult()

from shared.config import get_settings


@lru_cache
def get_celery_client() -> Celery:
    settings = get_settings()
    return Celery(
        "dispatcher",
        broker=settings.celery_broker_url,
        backend=settings.REDIS_URL,
    )


def enqueue_task(task_name: str, kwargs: dict | None = None, queue: str | None = None):
    app = get_celery_client()
    return app.send_task(task_name, kwargs=kwargs or {}, queue=queue)
