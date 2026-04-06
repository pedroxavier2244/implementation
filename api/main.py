import logging

from fastapi import FastAPI
from shared.logging_config import setup_logging

setup_logging()
from fastapi.responses import JSONResponse
import redis as redis_lib

logger = logging.getLogger(__name__)

from api.routes import data, files, jobs
from shared.config import get_settings
from shared.db import get_engine

try:
    from prometheus_fastapi_instrumentator import Instrumentator
except ModuleNotFoundError:  # pragma: no cover
    Instrumentator = None  # type: ignore[assignment]

app = FastAPI(title="ETL System API", version="1.0.0")

app.include_router(files.router, prefix="/v1")
app.include_router(jobs.router, prefix="/v1")
app.include_router(data.router, prefix="/v1")

if Instrumentator is not None:
    Instrumentator().instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready():
    status = {"postgres": "ok", "redis": "ok", "minio": "ok", "ready": True}
    http_status = 200

    try:
        engine = get_engine()
        with engine.connect() as conn:
            from sqlalchemy import text

            conn.execute(text("SELECT 1"))
    except Exception as exc:
        logger.error("Readiness check failed for %s: %s", "postgres", exc)
        status["postgres"] = "connection failed"
        status["ready"] = False
        http_status = 503

    try:
        settings = get_settings()
        redis_client = redis_lib.from_url(settings.REDIS_URL, socket_timeout=3)
        redis_client.ping()
    except Exception as exc:
        logger.error("Readiness check failed for %s: %s", "redis", exc)
        status["redis"] = "connection failed"
        status["ready"] = False
        http_status = 503

    try:
        from shared.minio_client import MinioClient

        MinioClient()
    except Exception as exc:
        logger.error("Readiness check failed for %s: %s", "minio", exc)
        status["minio"] = "connection failed"
        status["ready"] = False
        http_status = 503

    return JSONResponse(content=status, status_code=http_status)
