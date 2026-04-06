try:
    from celery import Celery
    from celery.schedules import crontab
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

    crontab = None  # type: ignore[assignment]

from shared.config import get_settings
from shared.logging_config import setup_logging

setup_logging()

settings = get_settings()

app = Celery(
    "worker",
    broker=settings.celery_broker_url,
    backend=settings.REDIS_URL,
    include=["worker.tasks", "checker.checker"],
)

_beat_schedule = {}
if crontab is not None:
    _beat_schedule = {
        "drive-sync-daily": {
            "task": "checker.checker.run_daily",
            "schedule": crontab(
                hour=settings.ETL_SCHEDULE_HOUR,
                minute=settings.ETL_SCHEDULE_MINUTE,
            ),
        },
    }

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone=settings.ETL_TIMEZONE,
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    beat_schedule=_beat_schedule,
    worker_hijack_root_logger=False,
    worker_log_color=False,
)
