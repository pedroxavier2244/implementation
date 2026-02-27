try:
    from celery import Celery
except ModuleNotFoundError:
    class _DummyTaskResult:
        id = "mock-task-id"

    class Celery:  # type: ignore[override]
        def __init__(self, *args, **kwargs):
            self.conf = {}

        def task(self, *args, **kwargs):
            def decorator(func):
                func.__wrapped__ = func
                func.apply_async = lambda *a, **k: _DummyTaskResult()
                return func

            return decorator

from shared.config import get_settings

settings = get_settings()

app = Celery(
    "worker",
    broker=settings.celery_broker_url,
    backend=settings.REDIS_URL,
    include=["worker.tasks"],
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
)
