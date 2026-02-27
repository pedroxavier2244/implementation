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

    def crontab(*args, **kwargs):  # type: ignore[override]
        return {"args": args, "kwargs": kwargs}

from shared.config import get_settings

settings = get_settings()

app = Celery(
    "checker",
    broker=settings.celery_broker_url,
    backend=settings.REDIS_URL,
    include=["checker.checker"],
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone=settings.ETL_TIMEZONE,
    enable_utc=True,
    beat_schedule={
        "run-daily-checker": {
            "task": "checker.checker.run_daily",
            "schedule": crontab(
                hour=settings.ETL_SCHEDULE_HOUR,
                minute=settings.ETL_SCHEDULE_MINUTE,
            ),
        }
    },
)
