# ETL System Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a fully automated ETL system with daily scheduling, multicanal alerting, idempotent pipeline, and REST API for monitoring and reprocessing.

**Architecture:** Celery Beat schedules `checker.run_daily` daily; Checker downloads from external API, saves to MinIO, validates (hash via PostgreSQL) and routes to `etl_jobs` (valid) or `notification_jobs` (invalid). Worker ETL processes in 6 idempotent, checkpointed steps with exponential backoff retry. Notifier dispatches Telegram + Email + flag file with per-channel retry and deterministic dedup_key.

**Tech Stack:** Python 3.12, FastAPI, Celery 5, Redis 7, PostgreSQL 16, MinIO, SQLAlchemy 2, Alembic, Pydantic v2, pytest, Docker Compose v2

---

## Reference: Approved Design

Full design doc: `docs/plans/2026-02-27-etl-system-design.md`

---

## Task 1: Project Scaffold

**Files:**
- Create: `requirements/base.txt`
- Create: `requirements/api.txt`
- Create: `requirements/worker.txt`
- Create: `requirements/notifier.txt`
- Create: `requirements/local_watcher.txt`
- Create: `requirements/test.txt`
- Create: `.env.example`
- Create: `docker-compose.yml`
- Create: `api/Dockerfile`
- Create: `worker/Dockerfile`
- Create: `checker/Dockerfile`
- Create: `notifier/Dockerfile` (symlink or copy of checker/Dockerfile)

**Step 1: Create requirements files**

`requirements/base.txt`:
```
sqlalchemy==2.0.36
alembic==1.14.0
pydantic-settings==2.7.0
redis==5.2.1
celery==5.4.0
boto3==1.35.0
psycopg2-binary==2.9.10
python-dotenv==1.0.1
```

`requirements/api.txt`:
```
-r base.txt
fastapi==0.115.6
uvicorn[standard]==0.32.1
httpx==0.28.1
```

`requirements/worker.txt`:
```
-r base.txt
pandas==2.2.3
openpyxl==3.1.5
httpx==0.28.1
```

`requirements/notifier.txt`:
```
-r base.txt
python-telegram-bot==21.9
```

`requirements/local_watcher.txt`:
```
redis==5.2.1
winotify==1.1.3
```

`requirements/test.txt`:
```
-r base.txt
pytest==8.3.4
pytest-asyncio==0.24.0
pytest-mock==3.14.0
httpx==0.28.1
factory-boy==3.3.1
```

**Step 2: Create `.env.example`**

```env
# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=etl_db
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=change_me

# Redis
REDIS_URL=redis://redis:6379/0

# MinIO
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=change_me
MINIO_BUCKET=etl-files
MINIO_SECURE=false

# Notifier
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=
SMTP_PASSWORD=

# ETL Config
ETL_SCHEDULE_HOUR=6
ETL_SCHEDULE_MINUTE=0
ETL_SOURCE_API_URL=https://example.com/api/file
ETL_SOURCE_API_KEY=
BAD_ROW_THRESHOLD_PCT=5.0
MAX_RETRIES=3

# Alerts
FLAG_FILE_DIR=/app/alerts
```

**Step 3: Create `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: ${POSTGRES_DB:-etl_db}
      POSTGRES_USER: ${POSTGRES_USER:-etl_user}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-etl_user} -d ${POSTGRES_DB:-etl_db}"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    command: >
      redis-server
      --save ""
      --appendonly no
      --maxmemory 256mb
      --maxmemory-policy allkeys-lru
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 5

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_ACCESS_KEY:-minioadmin}
      MINIO_ROOT_PASSWORD: ${MINIO_SECRET_KEY}
    volumes:
      - minio_data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 5

  api:
    build:
      context: .
      dockerfile: api/Dockerfile
    ports:
      - "8000:8000"
    env_file: .env
    depends_on:
      postgres: { condition: service_healthy }
      redis:    { condition: service_healthy }
      minio:    { condition: service_healthy }

  worker-etl:
    build:
      context: .
      dockerfile: worker/Dockerfile
    command: celery -A worker.celery_app worker -Q etl_jobs -c 2 --loglevel=info
    env_file: .env
    volumes:
      - alerts_data:/app/alerts
    depends_on:
      postgres: { condition: service_healthy }
      redis:    { condition: service_healthy }
      minio:    { condition: service_healthy }

  worker-notifier:
    build:
      context: .
      dockerfile: checker/Dockerfile
    command: celery -A notifier.celery_app worker -Q notification_jobs -c 4 --loglevel=info
    env_file: .env
    volumes:
      - alerts_data:/app/alerts
    depends_on:
      postgres: { condition: service_healthy }
      redis:    { condition: service_healthy }

  beat:
    build:
      context: .
      dockerfile: checker/Dockerfile
    command: celery -A checker.celery_app beat --loglevel=info
    env_file: .env
    depends_on:
      postgres: { condition: service_healthy }
      redis:    { condition: service_healthy }

volumes:
  postgres_data:
  minio_data:
  alerts_data:
```

**Step 4: Create Dockerfiles**

`api/Dockerfile`:
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements/api.txt requirements/api.txt
COPY requirements/base.txt requirements/base.txt
RUN pip install --no-cache-dir -r requirements/api.txt
COPY shared/ shared/
COPY api/ api/
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

`worker/Dockerfile`:
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements/worker.txt requirements/worker.txt
COPY requirements/base.txt requirements/base.txt
RUN pip install --no-cache-dir -r requirements/worker.txt
COPY shared/ shared/
COPY worker/ worker/
```

`checker/Dockerfile` (shared with beat and worker-notifier):
```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements/notifier.txt requirements/notifier.txt
COPY requirements/base.txt requirements/base.txt
RUN pip install --no-cache-dir -r requirements/notifier.txt
COPY shared/ shared/
COPY checker/ checker/
COPY notifier/ notifier/
```

**Step 5: Create `__init__.py` files for all packages**

```bash
touch api/__init__.py api/routes/__init__.py api/schemas/__init__.py
touch worker/__init__.py worker/steps/__init__.py
touch checker/__init__.py
touch notifier/__init__.py notifier/strategies/__init__.py
touch shared/__init__.py
touch local_watcher/__init__.py
touch tests/__init__.py tests/unit/__init__.py tests/integration/__init__.py
```

**Step 6: Commit**

```bash
git add .
git commit -m "chore: project scaffold — requirements, docker-compose, dockerfiles"
```

---

## Task 2: Shared Config

**Files:**
- Create: `shared/config.py`
- Create: `tests/unit/test_config.py`

**Step 1: Write the failing test**

`tests/unit/test_config.py`:
```python
import pytest
from unittest.mock import patch
import os

def test_settings_loads_from_env():
    env = {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_DB": "testdb",
        "POSTGRES_USER": "user",
        "POSTGRES_PASSWORD": "pass",
        "REDIS_URL": "redis://localhost:6379/0",
        "MINIO_ENDPOINT": "localhost:9000",
        "MINIO_ACCESS_KEY": "key",
        "MINIO_SECRET_KEY": "secret",
        "MINIO_BUCKET": "bucket",
        "ETL_SOURCE_API_URL": "http://example.com/file",
    }
    with patch.dict(os.environ, env, clear=True):
        from shared.config import Settings
        s = Settings()
        assert s.POSTGRES_HOST == "localhost"
        assert s.REDIS_URL == "redis://localhost:6379/0"
        assert s.BAD_ROW_THRESHOLD_PCT == 5.0  # default

def test_settings_database_url():
    env = {
        "POSTGRES_HOST": "pg", "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "db", "POSTGRES_USER": "u",
        "POSTGRES_PASSWORD": "p",
        "REDIS_URL": "redis://r:6379/0",
        "MINIO_ENDPOINT": "m:9000", "MINIO_ACCESS_KEY": "k",
        "MINIO_SECRET_KEY": "s", "MINIO_BUCKET": "b",
        "ETL_SOURCE_API_URL": "http://x.com",
    }
    with patch.dict(os.environ, env, clear=True):
        from shared.config import Settings
        s = Settings()
        assert "pg" in s.database_url
        assert "db" in s.database_url
```

**Step 2: Run test to verify it fails**

```bash
cd /c/Users/MB\ NEGOCIOS/etl-system
pip install pydantic-settings python-dotenv
pytest tests/unit/test_config.py -v
```
Expected: FAIL with `ModuleNotFoundError: No module named 'shared.config'`

**Step 3: Write implementation**

`shared/config.py`:
```python
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # PostgreSQL
    POSTGRES_HOST: str
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str

    # Redis
    REDIS_URL: str

    # MinIO
    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_BUCKET: str = "etl-files"
    MINIO_SECURE: bool = False

    # Notifier
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""
    SMTP_HOST: str = ""
    SMTP_PORT: int = 587
    SMTP_USER: str = ""
    SMTP_PASSWORD: str = ""

    # ETL Config
    ETL_SCHEDULE_HOUR: int = 6
    ETL_SCHEDULE_MINUTE: int = 0
    ETL_SOURCE_API_URL: str
    ETL_SOURCE_API_KEY: str = ""
    BAD_ROW_THRESHOLD_PCT: float = 5.0
    MAX_RETRIES: int = 3

    # Alerts
    FLAG_FILE_DIR: str = "/app/alerts"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def celery_broker_url(self) -> str:
        return self.REDIS_URL


@lru_cache
def get_settings() -> Settings:
    return Settings()
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/test_config.py -v
```
Expected: 2 PASSED

**Step 5: Commit**

```bash
git add shared/config.py tests/unit/test_config.py
git commit -m "feat: add shared config with pydantic-settings"
```

---

## Task 3: Database Models

**Files:**
- Create: `shared/models.py`
- Create: `tests/unit/test_models.py`

**Step 1: Write the failing test**

`tests/unit/test_models.py`:
```python
import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import Session

def test_all_tables_created():
    engine = create_engine("sqlite:///:memory:")
    from shared.models import Base
    Base.metadata.create_all(engine)
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    expected = [
        "etl_file", "etl_job_run", "etl_job_step",
        "etl_bad_rows", "alert_event", "alert_event_channel"
    ]
    for table in expected:
        assert table in tables, f"Missing table: {table}"

def test_etl_file_unique_constraint():
    from sqlalchemy import text
    engine = create_engine("sqlite:///:memory:")
    from shared.models import Base, EtlFile
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        import uuid
        from datetime import date, datetime, timezone
        f1 = EtlFile(
            id=str(uuid.uuid4()), file_date=date(2026, 2, 27),
            hash_sha256="abc123", downloaded_at=datetime.now(timezone.utc),
        )
        s.add(f1)
        s.commit()
        f2 = EtlFile(
            id=str(uuid.uuid4()), file_date=date(2026, 2, 27),
            hash_sha256="abc123", downloaded_at=datetime.now(timezone.utc),
        )
        s.add(f2)
        with pytest.raises(Exception):  # IntegrityError
            s.commit()
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_models.py -v
```
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write implementation**

`shared/models.py`:
```python
import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Boolean, Integer, Float, Text, DateTime,
    Date, ForeignKey, UniqueConstraint, Index, JSON
)
from sqlalchemy.orm import DeclarativeBase, relationship


def utcnow():
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class EtlFile(Base):
    __tablename__ = "etl_file"
    __table_args__ = (
        UniqueConstraint("file_date", "hash_sha256", name="uq_file_date_hash"),
    )

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    file_date = Column(Date, nullable=False)
    source_url = Column(Text)
    filename = Column(Text)
    hash_sha256 = Column(String(64), nullable=False)
    minio_path = Column(Text)
    downloaded_at = Column(DateTime(timezone=True), default=utcnow)
    is_valid = Column(Boolean, default=True)
    is_processed = Column(Boolean, default=False)
    validation_error = Column(Text)

    jobs = relationship("EtlJobRun", back_populates="file")


class EtlJobRun(Base):
    __tablename__ = "etl_job_run"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    file_id = Column(String(36), ForeignKey("etl_file.id"), nullable=False)
    status = Column(String(20), nullable=False, default="QUEUED")
    triggered_by = Column(String(20), nullable=False, default="scheduler")
    started_at = Column(DateTime(timezone=True))
    finished_at = Column(DateTime(timezone=True))
    rows_total = Column(Integer)
    rows_ok = Column(Integer)
    rows_bad = Column(Integer)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    last_retry_at = Column(DateTime(timezone=True))
    error_message = Column(Text)

    file = relationship("EtlFile", back_populates="jobs")
    steps = relationship("EtlJobStep", back_populates="job")
    bad_rows = relationship("EtlBadRow", back_populates="job")


class EtlJobStep(Base):
    __tablename__ = "etl_job_step"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    job_id = Column(String(36), ForeignKey("etl_job_run.id"), nullable=False)
    step_name = Column(String(20), nullable=False)
    status = Column(String(20), nullable=False, default="RUNNING")
    started_at = Column(DateTime(timezone=True), default=utcnow)
    finished_at = Column(DateTime(timezone=True))
    error_message = Column(Text)

    job = relationship("EtlJobRun", back_populates="steps")


class EtlBadRow(Base):
    __tablename__ = "etl_bad_rows"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    job_id = Column(String(36), ForeignKey("etl_job_run.id"), nullable=False)
    row_number = Column(Integer)
    raw_data = Column(JSON)
    reason = Column(Text)
    created_at = Column(DateTime(timezone=True), default=utcnow)

    job = relationship("EtlJobRun", back_populates="bad_rows")


class AlertEvent(Base):
    __tablename__ = "alert_event"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    dedup_key = Column(Text, unique=True, nullable=False)
    event_type = Column(String(30), nullable=False)
    severity = Column(String(10), nullable=False)
    message = Column(Text)
    metadata_ = Column("metadata", JSON)
    created_at = Column(DateTime(timezone=True), default=utcnow)

    channels = relationship("AlertEventChannel", back_populates="alert")


class AlertEventChannel(Base):
    __tablename__ = "alert_event_channel"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    alert_id = Column(String(36), ForeignKey("alert_event.id"), nullable=False)
    channel = Column(String(20), nullable=False)
    status = Column(String(20), nullable=False, default="RETRYING")
    sent_at = Column(DateTime(timezone=True))
    error_message = Column(Text)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    last_retry_at = Column(DateTime(timezone=True))
    next_retry_at = Column(DateTime(timezone=True))

    alert = relationship("AlertEvent", back_populates="channels")


# Indexes defined after models
Index("idx_job_status",  EtlJobRun.status)
Index("idx_job_file_id", EtlJobRun.file_id)
Index("idx_alert_severity", AlertEvent.severity)
Index("idx_file_date", EtlFile.file_date)
```

**Step 4: Run tests to verify they pass**

```bash
pip install sqlalchemy psycopg2-binary
pytest tests/unit/test_models.py -v
```
Expected: 2 PASSED

**Step 5: Commit**

```bash
git add shared/models.py tests/unit/test_models.py
git commit -m "feat: add SQLAlchemy models for all ETL entities"
```

---

## Task 4: Database Session + Alembic Migrations

**Files:**
- Create: `shared/db.py`
- Create: `alembic.ini`
- Create: `migrations/env.py` (Alembic env)
- Create: `migrations/versions/` (directory)

**Step 1: Create `shared/db.py`**

```python
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from shared.config import get_settings


def get_engine():
    settings = get_settings()
    return create_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )


SessionLocal = sessionmaker(autocommit=False, autoflush=False)


@contextmanager
def get_db_session() -> Session:
    engine = get_engine()
    SessionLocal.configure(bind=engine)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
```

**Step 2: Initialize Alembic**

```bash
cd /c/Users/MB\ NEGOCIOS/etl-system
pip install alembic
alembic init migrations
```

**Step 3: Edit `migrations/env.py`** — replace the `target_metadata` line and add import:

```python
# In migrations/env.py, find and replace the metadata section:
from shared.models import Base
target_metadata = Base.metadata

# Also set the sqlalchemy.url from config:
from shared.config import get_settings
config.set_main_option("sqlalchemy.url", get_settings().database_url)
```

**Step 4: Generate initial migration**

```bash
# Requires PostgreSQL running — use docker-compose for this step
docker-compose up postgres -d
# Wait for healthy state, then:
alembic revision --autogenerate -m "initial schema"
```

Expected: Creates `migrations/versions/xxxx_initial_schema.py`

**Step 5: Verify migration file looks correct**

Check that `migrations/versions/xxxx_initial_schema.py` contains `create_table` calls for all 6 tables.

**Step 6: Apply migration**

```bash
alembic upgrade head
```
Expected: All tables created, no errors.

**Step 7: Commit**

```bash
git add shared/db.py alembic.ini migrations/
git commit -m "feat: add db session manager and alembic migrations"
```

---

## Task 5: MinIO Client

**Files:**
- Create: `shared/minio_client.py`
- Create: `tests/unit/test_minio_client.py`

**Step 1: Write the failing test**

`tests/unit/test_minio_client.py`:
```python
import pytest
from unittest.mock import patch, MagicMock
import io


def test_upload_file_returns_path(tmp_path):
    file_content = b"test content"
    with patch("shared.minio_client.boto3") as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_client.head_bucket.return_value = {}

        from shared.minio_client import MinioClient
        client = MinioClient.__new__(MinioClient)
        client._client = mock_client
        client.bucket = "test-bucket"

        path = client.upload_file(
            file_bytes=file_content,
            object_name="2026/02/27/test.xlsx"
        )
        assert path == "2026/02/27/test.xlsx"
        mock_client.put_object.assert_called_once()


def test_download_file_returns_bytes():
    with patch("shared.minio_client.boto3") as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        mock_body = MagicMock()
        mock_body.read.return_value = b"file bytes"
        mock_client.get_object.return_value = {"Body": mock_body}

        from shared.minio_client import MinioClient
        client = MinioClient.__new__(MinioClient)
        client._client = mock_client
        client.bucket = "test-bucket"

        result = client.download_file("some/path.xlsx")
        assert result == b"file bytes"
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_minio_client.py -v
```
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write implementation**

`shared/minio_client.py`:
```python
import io
import boto3
from botocore.client import Config
from shared.config import get_settings


class MinioClient:
    def __init__(self):
        settings = get_settings()
        self.bucket = settings.MINIO_BUCKET
        self._client = boto3.client(
            "s3",
            endpoint_url=f"{'https' if settings.MINIO_SECURE else 'http'}://{settings.MINIO_ENDPOINT}",
            aws_access_key_id=settings.MINIO_ACCESS_KEY,
            aws_secret_access_key=settings.MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        self._ensure_bucket()

    def _ensure_bucket(self):
        try:
            self._client.head_bucket(Bucket=self.bucket)
        except Exception:
            self._client.create_bucket(Bucket=self.bucket)

    def upload_file(self, file_bytes: bytes, object_name: str) -> str:
        self._client.put_object(
            Bucket=self.bucket,
            Key=object_name,
            Body=io.BytesIO(file_bytes),
            ContentLength=len(file_bytes),
        )
        return object_name

    def download_file(self, object_name: str) -> bytes:
        response = self._client.get_object(Bucket=self.bucket, Key=object_name)
        return response["Body"].read()

    def object_exists(self, object_name: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=object_name)
            return True
        except Exception:
            return False
```

**Step 4: Run tests to verify they pass**

```bash
pip install boto3
pytest tests/unit/test_minio_client.py -v
```
Expected: 2 PASSED

**Step 5: Commit**

```bash
git add shared/minio_client.py tests/unit/test_minio_client.py
git commit -m "feat: add MinIO client wrapper (boto3/S3)"
```

---

## Task 6: Celery App Configuration

**Files:**
- Create: `worker/celery_app.py`
- Create: `checker/celery_app.py`
- Create: `notifier/celery_app.py`

**Step 1: Create `worker/celery_app.py`**

```python
from celery import Celery
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
```

**Step 2: Create `checker/celery_app.py`**

```python
from celery import Celery
from celery.schedules import crontab
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
    timezone="UTC",
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
```

**Step 3: Create `notifier/celery_app.py`**

```python
from celery import Celery
from shared.config import get_settings

settings = get_settings()

app = Celery(
    "notifier",
    broker=settings.celery_broker_url,
    backend=settings.REDIS_URL,
    include=["notifier.tasks"],
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    worker_prefetch_multiplier=4,
)
```

**Step 4: Commit**

```bash
git add worker/celery_app.py checker/celery_app.py notifier/celery_app.py
git commit -m "feat: add Celery app configs for worker, checker, notifier"
```

---

## Task 7: Checkpoint System (Idempotent Steps)

**Files:**
- Create: `worker/steps/checkpoint.py`
- Create: `tests/unit/test_checkpoint.py`

**Step 1: Write the failing test**

`tests/unit/test_checkpoint.py`:
```python
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone


def make_mock_session(existing_step=None):
    session = MagicMock()
    mock_query = MagicMock()
    mock_filter = MagicMock()
    mock_filter.first.return_value = existing_step
    mock_query.filter_by.return_value = mock_filter
    session.query.return_value = mock_query
    return session


def test_is_done_returns_true_when_step_completed():
    from shared.models import EtlJobStep
    done_step = EtlJobStep(status="DONE")
    session = make_mock_session(existing_step=done_step)

    from worker.steps.checkpoint import is_step_done
    assert is_step_done(session, "job-1", "extract") is True


def test_is_done_returns_false_when_no_step():
    session = make_mock_session(existing_step=None)
    from worker.steps.checkpoint import is_step_done
    assert is_step_done(session, "job-1", "extract") is False


def test_mark_step_done_updates_existing():
    from shared.models import EtlJobStep
    existing = EtlJobStep(id="s1", job_id="job-1", step_name="extract", status="RUNNING")
    session = make_mock_session(existing_step=existing)

    from worker.steps.checkpoint import mark_step_done
    mark_step_done(session, "job-1", "extract")
    assert existing.status == "DONE"
    assert existing.finished_at is not None
    session.flush.assert_called_once()
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_checkpoint.py -v
```
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write implementation**

`worker/steps/checkpoint.py`:
```python
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from shared.models import EtlJobStep


def is_step_done(session: Session, job_id: str, step_name: str) -> bool:
    step = session.query(EtlJobStep).filter_by(
        job_id=job_id, step_name=step_name
    ).first()
    return step is not None and step.status == "DONE"


def begin_step(session: Session, job_id: str, step_name: str) -> EtlJobStep:
    step = session.query(EtlJobStep).filter_by(
        job_id=job_id, step_name=step_name
    ).first()
    if step is None:
        step = EtlJobStep(
            job_id=job_id,
            step_name=step_name,
            status="RUNNING",
            started_at=datetime.now(timezone.utc),
        )
        session.add(step)
    else:
        step.status = "RUNNING"
        step.started_at = datetime.now(timezone.utc)
        step.error_message = None
    session.flush()
    return step


def mark_step_done(session: Session, job_id: str, step_name: str) -> None:
    step = session.query(EtlJobStep).filter_by(
        job_id=job_id, step_name=step_name
    ).first()
    if step:
        step.status = "DONE"
        step.finished_at = datetime.now(timezone.utc)
        session.flush()


def mark_step_failed(session: Session, job_id: str, step_name: str, error: str) -> None:
    step = session.query(EtlJobStep).filter_by(
        job_id=job_id, step_name=step_name
    ).first()
    if step:
        step.status = "FAILED"
        step.finished_at = datetime.now(timezone.utc)
        step.error_message = error
        session.flush()
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/test_checkpoint.py -v
```
Expected: 3 PASSED

**Step 5: Commit**

```bash
git add worker/steps/checkpoint.py tests/unit/test_checkpoint.py
git commit -m "feat: add idempotent checkpoint system for ETL steps"
```

---

## Task 8: Checker Service

**Files:**
- Create: `checker/checker.py`
- Create: `tests/unit/test_checker.py`

**Step 1: Write the failing tests**

`tests/unit/test_checker.py`:
```python
import pytest
from unittest.mock import patch, MagicMock
from datetime import date, datetime, timezone


def make_settings(**kwargs):
    s = MagicMock()
    s.ETL_SOURCE_API_URL = "http://example.com/file"
    s.ETL_SOURCE_API_KEY = ""
    s.BAD_ROW_THRESHOLD_PCT = 5.0
    for k, v in kwargs.items():
        setattr(s, k, v)
    return s


def test_compute_hash_is_deterministic():
    from checker.checker import compute_sha256
    data = b"hello world"
    assert compute_sha256(data) == compute_sha256(data)
    assert len(compute_sha256(data)) == 64


def test_is_hash_duplicate_returns_true_when_exists():
    from shared.models import EtlFile
    existing = EtlFile(hash_sha256="abc", file_date=date(2026, 2, 27))
    session = MagicMock()
    session.query().filter_by().first.return_value = existing

    from checker.checker import is_hash_duplicate
    assert is_hash_duplicate(session, "abc", date(2026, 2, 27)) is True


def test_is_hash_duplicate_returns_false_when_new():
    session = MagicMock()
    session.query().filter_by().first.return_value = None

    from checker.checker import is_hash_duplicate
    assert is_hash_duplicate(session, "newhash", date(2026, 2, 27)) is False
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_checker.py -v
```
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write implementation**

`checker/checker.py`:
```python
import hashlib
import uuid
from datetime import date, datetime, timezone

import httpx
from celery import current_app
from sqlalchemy.orm import Session

from checker.celery_app import app
from shared.config import get_settings
from shared.db import get_db_session
from shared.minio_client import MinioClient
from shared.models import EtlFile


def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def is_hash_duplicate(session: Session, hash_sha256: str, file_date: date) -> bool:
    existing = session.query(EtlFile).filter_by(
        hash_sha256=hash_sha256, file_date=file_date
    ).first()
    return existing is not None


def _build_minio_path(file_date: date, filename: str) -> str:
    return f"{file_date.year}/{file_date.month:02d}/{file_date.day:02d}/{filename}"


def _send_alert(event_type: str, severity: str, message: str, metadata: dict):
    from notifier.tasks import dispatch_notification
    dispatch_notification.apply_async(
        kwargs={
            "event_type": event_type,
            "severity": severity,
            "message": message,
            "metadata": metadata,
        },
        queue="notification_jobs",
    )


def _enqueue_etl_job(file_id: str):
    from worker.tasks import run_etl
    run_etl.apply_async(
        kwargs={"job_id": None, "file_id": file_id},
        queue="etl_jobs",
    )


@app.task(name="checker.checker.run_daily", bind=True)
def run_daily(self):
    settings = get_settings()
    today = date.today()

    # 1. Download file from external API
    try:
        headers = {}
        if settings.ETL_SOURCE_API_KEY:
            headers["Authorization"] = f"Bearer {settings.ETL_SOURCE_API_KEY}"
        response = httpx.get(settings.ETL_SOURCE_API_URL, headers=headers, timeout=60)
        response.raise_for_status()
        file_bytes = response.content
        filename = f"source_{today.isoformat()}.xlsx"
    except Exception as exc:
        _send_alert(
            event_type="FILE_MISSING",
            severity="CRITICAL",
            message=f"Failed to download file from API: {exc}",
            metadata={"file_date": today.isoformat()},
        )
        return

    # 2. Compute hash and check for duplicates
    hash_sha256 = compute_sha256(file_bytes)

    with get_db_session() as session:
        if is_hash_duplicate(session, hash_sha256, today):
            _send_alert(
                event_type="HASH_REPEAT",
                severity="WARNING",
                message=f"File for {today} has same hash as previous — skipping",
                metadata={"file_date": today.isoformat(), "hash": hash_sha256},
            )
            return

        # 3. Upload to MinIO
        minio = MinioClient()
        minio_path = _build_minio_path(today, filename)
        minio.upload_file(file_bytes, minio_path)

        # 4. Save etl_file record
        etl_file = EtlFile(
            id=str(uuid.uuid4()),
            file_date=today,
            source_url=settings.ETL_SOURCE_API_URL,
            filename=filename,
            hash_sha256=hash_sha256,
            minio_path=minio_path,
            downloaded_at=datetime.now(timezone.utc),
            is_valid=True,
            is_processed=False,
        )
        session.add(etl_file)
        session.flush()
        file_id = etl_file.id

    # 5. Enqueue ETL job
    _enqueue_etl_job(file_id)
```

**Step 4: Run tests to verify they pass**

```bash
pip install httpx
pytest tests/unit/test_checker.py -v
```
Expected: 3 PASSED

**Step 5: Commit**

```bash
git add checker/checker.py tests/unit/test_checker.py
git commit -m "feat: add checker service with hash dedup and file routing"
```

---

## Task 9: ETL Worker — Main Task + Retry Logic

**Files:**
- Create: `worker/tasks.py`
- Create: `tests/unit/test_worker_task.py`

**Step 1: Write the failing tests**

`tests/unit/test_worker_task.py`:
```python
import pytest
from unittest.mock import patch, MagicMock, call
from datetime import date


def test_run_etl_marks_job_done_on_success():
    with patch("worker.tasks.get_db_session") as mock_db, \
         patch("worker.tasks.run_extract") as mock_extract, \
         patch("worker.tasks.run_validate") as mock_validate, \
         patch("worker.tasks.run_clean") as mock_clean, \
         patch("worker.tasks.run_enrich") as mock_enrich, \
         patch("worker.tasks.run_stage") as mock_stage, \
         patch("worker.tasks.run_upsert") as mock_upsert:

        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        from shared.models import EtlJobRun, EtlFile
        mock_job = EtlJobRun(id="job-1", file_id="file-1", status="QUEUED", max_retries=3)
        mock_file = EtlFile(id="file-1", minio_path="2026/02/27/f.xlsx")
        mock_session.query().filter_by().first.side_effect = [mock_job, mock_file]

        from worker.tasks import run_etl
        run_etl.__wrapped__(MagicMock(), job_id="job-1", file_id=None)

        assert mock_job.status == "DONE"


def test_exponential_backoff_delays():
    from worker.tasks import compute_retry_delay
    assert compute_retry_delay(0) == 300
    assert compute_retry_delay(1) == 600
    assert compute_retry_delay(2) == 1200
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_worker_task.py -v
```
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write implementation**

`worker/tasks.py`:
```python
import uuid
from datetime import datetime, timezone

from worker.celery_app import app
from shared.db import get_db_session
from shared.models import EtlFile, EtlJobRun
from worker.steps.extract import run_extract
from worker.steps.validate import run_validate
from worker.steps.clean import run_clean
from worker.steps.enrich import run_enrich
from worker.steps.stage import run_stage
from worker.steps.upsert import run_upsert


def compute_retry_delay(retry_number: int) -> int:
    """Exponential backoff: 5min, 10min, 20min"""
    return 300 * (2 ** retry_number)


def _send_dead_alert(job_id: str, step_name: str, retry_count: int):
    from notifier.tasks import dispatch_notification
    dispatch_notification.apply_async(
        kwargs={
            "event_type": "ETL_DEAD",
            "severity": "CRITICAL",
            "message": f"Job {job_id} failed after {retry_count} retries at step {step_name}",
            "metadata": {
                "job_id": job_id,
                "step": step_name,
                "retry_count": retry_count,
            },
        },
        queue="notification_jobs",
    )


@app.task(name="worker.tasks.run_etl", bind=True, queue="etl_jobs")
def run_etl(self, job_id: str | None, file_id: str | None):
    with get_db_session() as session:
        # Resolve job
        if job_id:
            job = session.query(EtlJobRun).filter_by(id=job_id).first()
        else:
            # Create new job for this file
            etl_file = session.query(EtlFile).filter_by(id=file_id).first()
            job = EtlJobRun(
                id=str(uuid.uuid4()),
                file_id=etl_file.id,
                status="RUNNING",
                triggered_by="scheduler",
                started_at=datetime.now(timezone.utc),
                max_retries=3,
            )
            session.add(job)
            session.flush()
            job_id = job.id

        job.status = "RUNNING"
        job.started_at = job.started_at or datetime.now(timezone.utc)
        etl_file = session.query(EtlFile).filter_by(id=job.file_id).first()
        current_step = "unknown"

        try:
            current_step = "extract"
            run_extract(session, job_id, etl_file)

            current_step = "validate"
            run_validate(session, job_id, etl_file)

            current_step = "clean"
            run_clean(session, job_id)

            current_step = "enrich"
            run_enrich(session, job_id)

            current_step = "stage"
            run_stage(session, job_id)

            current_step = "upsert"
            run_upsert(session, job_id)

            job.status = "DONE"
            job.finished_at = datetime.now(timezone.utc)
            etl_file.is_processed = True

        except Exception as exc:
            from worker.steps.checkpoint import mark_step_failed
            mark_step_failed(session, job_id, current_step, str(exc))

            retry_count = self.request.retries
            job.retry_count = retry_count + 1
            job.last_retry_at = datetime.now(timezone.utc)

            if retry_count >= job.max_retries:
                job.status = "DEAD"
                job.error_message = str(exc)
                job.finished_at = datetime.now(timezone.utc)
                _send_dead_alert(job_id, current_step, retry_count)
                return

            job.status = "RETRYING"
            delay = compute_retry_delay(retry_count)
            raise self.retry(exc=exc, countdown=delay)
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/test_worker_task.py -v
```
Expected: 2 PASSED

**Step 5: Commit**

```bash
git add worker/tasks.py tests/unit/test_worker_task.py
git commit -m "feat: add ETL worker main task with exponential backoff retry"
```

---

## Task 10: ETL Steps — Extract, Validate, Clean

**Files:**
- Create: `worker/steps/extract.py`
- Create: `worker/steps/validate.py`
- Create: `worker/steps/clean.py`
- Create: `tests/unit/test_etl_steps.py`

**Step 1: Write the failing tests**

`tests/unit/test_etl_steps.py`:
```python
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
import io


def make_session_mock(step_exists=False):
    session = MagicMock()
    from shared.models import EtlJobStep
    existing = EtlJobStep(status="DONE") if step_exists else None
    session.query().filter_by().first.return_value = existing
    return session


def make_file_mock(minio_path="2026/02/27/f.xlsx"):
    f = MagicMock()
    f.minio_path = minio_path
    return f


def test_extract_skips_when_done():
    session = make_session_mock(step_exists=True)
    from worker.steps.extract import run_extract
    # Should not raise or call minio
    with patch("worker.steps.extract.MinioClient") as mock_minio:
        run_extract(session, "job-1", make_file_mock())
        mock_minio.assert_not_called()


def test_validate_aborts_when_threshold_exceeded():
    session = make_session_mock(step_exists=False)
    df = pd.DataFrame({"col1": [None] * 10, "col2": [None] * 10})

    with patch("worker.steps.validate.is_step_done", return_value=False), \
         patch("worker.steps.validate.begin_step"), \
         patch("worker.steps.validate.get_cached_dataframe", return_value=df), \
         patch("worker.steps.validate.get_settings") as mock_settings:
        mock_settings.return_value.BAD_ROW_THRESHOLD_PCT = 5.0
        from worker.steps.validate import run_validate
        with pytest.raises(ValueError, match="threshold"):
            run_validate(session, "job-1", make_file_mock())


def test_clean_skips_when_done():
    with patch("worker.steps.clean.is_step_done", return_value=True):
        from worker.steps.clean import run_clean
        session = MagicMock()
        run_clean(session, "job-1")  # should not raise
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_etl_steps.py -v
```
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write implementations**

`worker/steps/extract.py`:
```python
from sqlalchemy.orm import Session
from shared.models import EtlFile
from shared.minio_client import MinioClient
from worker.steps.checkpoint import is_step_done, begin_step, mark_step_done

# In-memory cache per task execution (cleared between tasks by Celery)
_dataframe_cache: dict = {}


def get_cached_dataframe(job_id: str):
    return _dataframe_cache.get(job_id)


def set_cached_dataframe(job_id: str, df):
    _dataframe_cache[job_id] = df


def clear_cached_dataframe(job_id: str):
    _dataframe_cache.pop(job_id, None)


def run_extract(session: Session, job_id: str, etl_file: EtlFile):
    if is_step_done(session, job_id, "extract"):
        return
    begin_step(session, job_id, "extract")

    import pandas as pd
    import io

    minio = MinioClient()
    file_bytes = minio.download_file(etl_file.minio_path)
    df = pd.read_excel(io.BytesIO(file_bytes))
    set_cached_dataframe(job_id, df)
    mark_step_done(session, job_id, "extract")
```

`worker/steps/validate.py`:
```python
import uuid
from sqlalchemy.orm import Session
from shared.models import EtlJobRun, EtlBadRow
from shared.config import get_settings
from worker.steps.checkpoint import is_step_done, begin_step, mark_step_done
from worker.steps.extract import get_cached_dataframe

# Required columns — adjust per entity
REQUIRED_COLUMNS: list[str] = []  # Set per deployment


def run_validate(session: Session, job_id: str, etl_file):
    if is_step_done(session, job_id, "validate"):
        return
    begin_step(session, job_id, "validate")

    settings = get_settings()
    df = get_cached_dataframe(job_id)
    if df is None:
        raise RuntimeError("No dataframe in cache — extract must run first")

    bad_rows = []
    for idx, row in df.iterrows():
        reasons = []
        if row.isnull().all():
            reasons.append("all_null_row")
        if reasons:
            bad_rows.append(EtlBadRow(
                id=str(uuid.uuid4()),
                job_id=job_id,
                row_number=int(idx),
                raw_data=row.to_dict(),
                reason="; ".join(reasons),
            ))

    total = len(df)
    bad_count = len(bad_rows)
    threshold_pct = settings.BAD_ROW_THRESHOLD_PCT

    # Save bad rows
    for br in bad_rows:
        session.merge(br)

    # Update job metrics
    job = session.query(EtlJobRun).filter_by(id=job_id).first()
    if job:
        job.rows_total = total
        job.rows_bad = bad_count
        job.rows_ok = total - bad_count
    session.flush()

    if total > 0 and (bad_count / total * 100) > threshold_pct:
        raise ValueError(
            f"Bad row threshold exceeded: {bad_count}/{total} "
            f"({bad_count/total*100:.1f}%) > {threshold_pct}%"
        )

    mark_step_done(session, job_id, "validate")
```

`worker/steps/clean.py`:
```python
import pandas as pd
from sqlalchemy.orm import Session
from worker.steps.checkpoint import is_step_done, begin_step, mark_step_done
from worker.steps.extract import get_cached_dataframe, set_cached_dataframe


def run_clean(session: Session, job_id: str):
    if is_step_done(session, job_id, "clean"):
        return
    begin_step(session, job_id, "clean")

    df = get_cached_dataframe(job_id)
    if df is None:
        raise RuntimeError("No dataframe in cache")

    # Normalize strings
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip() if hasattr(df[col], 'str') else df[col]

    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    set_cached_dataframe(job_id, df)
    mark_step_done(session, job_id, "clean")
```

**Step 4: Run tests to verify they pass**

```bash
pip install pandas openpyxl
pytest tests/unit/test_etl_steps.py -v
```
Expected: 3 PASSED

**Step 5: Commit**

```bash
git add worker/steps/extract.py worker/steps/validate.py worker/steps/clean.py tests/unit/test_etl_steps.py
git commit -m "feat: add ETL steps — extract, validate, clean"
```

---

## Task 11: ETL Steps — Enrich, Stage, Upsert

**Files:**
- Create: `worker/steps/enrich.py`
- Create: `worker/steps/stage.py`
- Create: `worker/steps/upsert.py`

**Step 1: Write minimal implementations**

> Note: Enrich and Stage/Upsert are entity-specific. These are the structural shells to be populated when the actual data entity is defined.

`worker/steps/enrich.py`:
```python
from sqlalchemy.orm import Session
from worker.steps.checkpoint import is_step_done, begin_step, mark_step_done
from worker.steps.extract import get_cached_dataframe, set_cached_dataframe


def run_enrich(session: Session, job_id: str):
    """
    Enrichment step: joins with reference tables, computes derived fields.
    Extend this function with entity-specific enrichment logic.
    Idempotent by nature (pure computation).
    """
    if is_step_done(session, job_id, "enrich"):
        return
    begin_step(session, job_id, "enrich")

    # No enrichment rules defined — pass through
    mark_step_done(session, job_id, "enrich")
```

`worker/steps/stage.py`:
```python
from sqlalchemy.orm import Session
from sqlalchemy import text
from worker.steps.checkpoint import is_step_done, begin_step, mark_step_done
from worker.steps.extract import get_cached_dataframe


# STAGING_TABLE must be configured per entity deployment
STAGING_TABLE = "staging_registro"


def run_stage(session: Session, job_id: str):
    if is_step_done(session, job_id, "stage"):
        return
    begin_step(session, job_id, "stage")

    df = get_cached_dataframe(job_id)
    if df is None:
        raise RuntimeError("No dataframe in cache")

    # Clear previous partial load for this job (idempotent)
    session.execute(
        text(f"DELETE FROM {STAGING_TABLE} WHERE etl_job_id = :job_id"),
        {"job_id": job_id}
    )

    # Insert rows
    df_to_insert = df.copy()
    df_to_insert["etl_job_id"] = job_id
    import pandas as pd
    from datetime import datetime, timezone
    df_to_insert["loaded_at"] = datetime.now(timezone.utc)

    df_to_insert.to_sql(
        STAGING_TABLE,
        session.get_bind(),
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    mark_step_done(session, job_id, "stage")
```

`worker/steps/upsert.py`:
```python
from sqlalchemy.orm import Session
from sqlalchemy import text
from worker.steps.checkpoint import is_step_done, begin_step, mark_step_done

# Entity-specific — adjust table names and conflict key per deployment
STAGING_TABLE = "staging_registro"
FINAL_TABLE = "final_registro"
CONFLICT_KEY = "id"   # natural key of the entity


def run_upsert(session: Session, job_id: str):
    if is_step_done(session, job_id, "upsert"):
        return
    begin_step(session, job_id, "upsert")

    # Get columns from staging (excluding control columns)
    result = session.execute(
        text(f"SELECT column_name FROM information_schema.columns "
             f"WHERE table_name = '{STAGING_TABLE}'")
    )
    all_cols = [row[0] for row in result if row[0] not in ("etl_job_id", "loaded_at")]
    update_cols = [c for c in all_cols if c != CONFLICT_KEY]
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    cols_str = ", ".join(all_cols)

    upsert_sql = f"""
        INSERT INTO {FINAL_TABLE} ({cols_str})
        SELECT {cols_str} FROM {STAGING_TABLE} WHERE etl_job_id = :job_id
        ON CONFLICT ({CONFLICT_KEY}) DO UPDATE SET {set_clause}
    """
    session.execute(text(upsert_sql), {"job_id": job_id})
    mark_step_done(session, job_id, "upsert")
```

**Step 2: Run existing test suite to verify no regressions**

```bash
pytest tests/unit/ -v
```
Expected: All previous tests still PASS

**Step 3: Commit**

```bash
git add worker/steps/enrich.py worker/steps/stage.py worker/steps/upsert.py
git commit -m "feat: add ETL steps — enrich, stage, upsert (entity-configurable)"
```

---

## Task 12: Notification Deduplication

**Files:**
- Create: `notifier/dedup.py`
- Create: `tests/unit/test_dedup.py`

**Step 1: Write the failing tests**

`tests/unit/test_dedup.py`:
```python
import pytest


def test_dedup_key_with_job_id():
    from notifier.dedup import build_dedup_key
    key = build_dedup_key("ETL_DEAD", {"job_id": "abc-123"})
    assert key == "job:abc-123:ETL_DEAD"


def test_dedup_key_file_missing():
    from notifier.dedup import build_dedup_key
    key = build_dedup_key("FILE_MISSING", {"file_date": "2026-02-27"})
    assert key == "file_date:2026-02-27:FILE_MISSING"


def test_dedup_key_schema_error_with_version():
    from notifier.dedup import build_dedup_key
    key = build_dedup_key("SCHEMA_ERROR", {"file_date": "2026-02-27", "schema_version": "v2"})
    assert key == "file_date:2026-02-27:SCHEMA_ERROR:v2"


def test_dedup_key_hash_repeat():
    from notifier.dedup import build_dedup_key
    key = build_dedup_key("HASH_REPEAT", {"file_date": "2026-02-27"})
    assert key == "file_date:2026-02-27:HASH_REPEAT"
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_dedup.py -v
```
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write implementation**

`notifier/dedup.py`:
```python
def build_dedup_key(event_type: str, metadata: dict) -> str:
    """
    Build a deterministic dedup key for alert events.
    Covers all event types, with and without job_id.
    """
    job_id = metadata.get("job_id")
    file_date = metadata.get("file_date", "unknown")
    schema_v = metadata.get("schema_version", "")

    if job_id:
        return f"job:{job_id}:{event_type}"
    elif schema_v:
        return f"file_date:{file_date}:{event_type}:{schema_v}"
    else:
        return f"file_date:{file_date}:{event_type}"
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/test_dedup.py -v
```
Expected: 4 PASSED

**Step 5: Commit**

```bash
git add notifier/dedup.py tests/unit/test_dedup.py
git commit -m "feat: add deterministic dedup_key builder for alert events"
```

---

## Task 13: Notification Strategies

**Files:**
- Create: `notifier/strategies/base.py`
- Create: `notifier/strategies/telegram.py`
- Create: `notifier/strategies/email_smtp.py`
- Create: `notifier/strategies/flag_file.py`
- Create: `tests/unit/test_strategies.py`

**Step 1: Write the failing tests**

`tests/unit/test_strategies.py`:
```python
import pytest
from unittest.mock import patch, MagicMock
import os
import tempfile


def test_flag_file_creates_file(tmp_path):
    from notifier.strategies.flag_file import FlagFileStrategy
    strategy = FlagFileStrategy(flag_dir=str(tmp_path))
    strategy.send(
        event_type="FILE_MISSING",
        severity="CRITICAL",
        message="File missing",
        metadata={"file_date": "2026-02-27"}
    )
    files = list(tmp_path.glob("*.flag"))
    assert len(files) == 1
    assert "CRITICAL" in files[0].name


def test_telegram_send_calls_api():
    with patch("notifier.strategies.telegram.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_httpx.post.return_value = mock_response

        from notifier.strategies.telegram import TelegramStrategy
        strategy = TelegramStrategy(bot_token="TOKEN", chat_id="123")
        strategy.send(
            event_type="ETL_DEAD",
            severity="CRITICAL",
            message="Job failed",
            metadata={}
        )
        mock_httpx.post.assert_called_once()
        call_url = mock_httpx.post.call_args[0][0]
        assert "TOKEN" in call_url
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_strategies.py -v
```
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write implementations**

`notifier/strategies/base.py`:
```python
from abc import ABC, abstractmethod


class NotificationStrategy(ABC):
    @abstractmethod
    def send(self, event_type: str, severity: str, message: str, metadata: dict) -> None:
        ...
```

`notifier/strategies/telegram.py`:
```python
import httpx
from notifier.strategies.base import NotificationStrategy

SEVERITY_EMOJI = {"WARNING": "⚠️", "CRITICAL": "🚨"}


class TelegramStrategy(NotificationStrategy):
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id

    def send(self, event_type: str, severity: str, message: str, metadata: dict) -> None:
        emoji = SEVERITY_EMOJI.get(severity, "ℹ️")
        text = f"{emoji} *{severity}* — {event_type}\n\n{message}"
        if metadata:
            details = "\n".join(f"• `{k}`: {v}" for k, v in metadata.items())
            text += f"\n\n{details}"

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        response = httpx.post(
            url,
            json={"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=15,
        )
        response.raise_for_status()
```

`notifier/strategies/email_smtp.py`:
```python
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from notifier.strategies.base import NotificationStrategy


class EmailSMTPStrategy(NotificationStrategy):
    def __init__(self, host: str, port: int, user: str, password: str, recipient: str = ""):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.recipient = recipient or user

    def send(self, event_type: str, severity: str, message: str, metadata: dict) -> None:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[{severity}] ETL Alert — {event_type}"
        msg["From"] = self.user
        msg["To"] = self.recipient

        body = f"<h2>{severity}: {event_type}</h2><p>{message}</p>"
        if metadata:
            rows = "".join(f"<tr><td><b>{k}</b></td><td>{v}</td></tr>" for k, v in metadata.items())
            body += f"<table>{rows}</table>"

        msg.attach(MIMEText(body, "html"))

        with smtplib.SMTP(self.host, self.port) as server:
            server.starttls()
            server.login(self.user, self.password)
            server.sendmail(self.user, self.recipient, msg.as_string())
```

`notifier/strategies/flag_file.py`:
```python
import os
from datetime import datetime, timezone
from notifier.strategies.base import NotificationStrategy


class FlagFileStrategy(NotificationStrategy):
    def __init__(self, flag_dir: str = "/app/alerts"):
        self.flag_dir = flag_dir
        os.makedirs(flag_dir, exist_ok=True)

    def send(self, event_type: str, severity: str, message: str, metadata: dict) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{ts}_{severity}_{event_type}.flag"
        path = os.path.join(self.flag_dir, filename)
        content = f"{severity}|{event_type}|{message}\n{metadata}"
        with open(path, "w") as f:
            f.write(content)
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/test_strategies.py -v
```
Expected: 2 PASSED

**Step 5: Commit**

```bash
git add notifier/strategies/ tests/unit/test_strategies.py
git commit -m "feat: add notification strategies — Telegram, Email SMTP, flag file"
```

---

## Task 14: Notification Worker Task

**Files:**
- Create: `notifier/tasks.py`
- Create: `tests/unit/test_notifier_task.py`

**Step 1: Write the failing tests**

`tests/unit/test_notifier_task.py`:
```python
import pytest
from unittest.mock import patch, MagicMock


def test_dispatch_skips_duplicate_event():
    with patch("notifier.tasks.get_db_session") as mock_db, \
         patch("notifier.tasks.build_dedup_key", return_value="job:abc:ETL_DEAD"):
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        from shared.models import AlertEvent
        existing = AlertEvent(id="existing-1", dedup_key="job:abc:ETL_DEAD")
        mock_session.query().filter_by().first.return_value = existing

        from notifier.tasks import dispatch_notification
        result = dispatch_notification.__wrapped__(
            MagicMock(),
            event_type="ETL_DEAD",
            severity="CRITICAL",
            message="msg",
            metadata={"job_id": "abc"},
        )
        # Should return early — no channels dispatched
        assert result is None


def test_dispatch_creates_event_for_new_alert():
    with patch("notifier.tasks.get_db_session") as mock_db, \
         patch("notifier.tasks.build_dedup_key", return_value="file_date:2026:FILE_MISSING"), \
         patch("notifier.tasks._dispatch_channel") as mock_dispatch:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.query().filter_by().first.return_value = None

        from notifier.tasks import dispatch_notification
        dispatch_notification.__wrapped__(
            MagicMock(),
            event_type="FILE_MISSING",
            severity="CRITICAL",
            message="missing",
            metadata={"file_date": "2026-02-27"},
        )
        mock_session.add.assert_called()
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_notifier_task.py -v
```
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write implementation**

`notifier/tasks.py`:
```python
import uuid
from datetime import datetime, timezone

from notifier.celery_app import app
from notifier.dedup import build_dedup_key
from notifier.strategies.telegram import TelegramStrategy
from notifier.strategies.email_smtp import EmailSMTPStrategy
from notifier.strategies.flag_file import FlagFileStrategy
from shared.config import get_settings
from shared.db import get_db_session
from shared.models import AlertEvent, AlertEventChannel


def _get_active_strategies():
    settings = get_settings()
    strategies = []
    if settings.TELEGRAM_BOT_TOKEN:
        strategies.append(("telegram", TelegramStrategy(
            bot_token=settings.TELEGRAM_BOT_TOKEN,
            chat_id=settings.TELEGRAM_CHAT_ID,
        )))
    if settings.SMTP_USER:
        strategies.append(("email", EmailSMTPStrategy(
            host=settings.SMTP_HOST,
            port=settings.SMTP_PORT,
            user=settings.SMTP_USER,
            password=settings.SMTP_PASSWORD,
        )))
    strategies.append(("flag_file", FlagFileStrategy(
        flag_dir=settings.FLAG_FILE_DIR
    )))
    return strategies


def _dispatch_channel(channel_id: str, strategy, event_type, severity, message, metadata):
    """Attempt delivery for a single channel. Schedules retry on failure."""
    retry_channel.apply_async(
        kwargs={"channel_id": channel_id},
        queue="notification_jobs",
    )


@app.task(name="notifier.tasks.dispatch_notification", bind=True, queue="notification_jobs")
def dispatch_notification(self, event_type: str, severity: str, message: str, metadata: dict):
    dedup_key = build_dedup_key(event_type, metadata)

    with get_db_session() as session:
        existing = session.query(AlertEvent).filter_by(dedup_key=dedup_key).first()
        if existing:
            return  # Already processed — deduplicated

        alert = AlertEvent(
            id=str(uuid.uuid4()),
            dedup_key=dedup_key,
            event_type=event_type,
            severity=severity,
            message=message,
            metadata_=metadata,
            created_at=datetime.now(timezone.utc),
        )
        session.add(alert)
        session.flush()

        strategies = _get_active_strategies()
        channel_records = []
        for channel_name, strategy in strategies:
            channel = AlertEventChannel(
                id=str(uuid.uuid4()),
                alert_id=alert.id,
                channel=channel_name,
                status="RETRYING",
                retry_count=0,
                max_retries=3,
            )
            session.add(channel)
            session.flush()
            channel_records.append((channel.id, strategy))

    # Dispatch each channel as independent task
    for channel_id, strategy in channel_records:
        retry_channel.apply_async(
            kwargs={"channel_id": channel_id},
            queue="notification_jobs",
        )


@app.task(name="notifier.tasks.retry_channel", bind=True, queue="notification_jobs")
def retry_channel(self, channel_id: str):
    with get_db_session() as session:
        channel = session.query(AlertEventChannel).filter_by(id=channel_id).first()
        if not channel or channel.status == "SENT":
            return

        alert = session.query(AlertEvent).filter_by(id=channel.alert_id).first()
        strategies = dict(_get_active_strategies())
        strategy = strategies.get(channel.channel)

        if strategy is None:
            channel.status = "FAILED"
            channel.error_message = f"Unknown channel: {channel.channel}"
            return

        try:
            strategy.send(
                event_type=alert.event_type,
                severity=alert.severity,
                message=alert.message,
                metadata=alert.metadata_ or {},
            )
            channel.status = "SENT"
            channel.sent_at = datetime.now(timezone.utc)
        except Exception as exc:
            channel.retry_count += 1
            channel.last_retry_at = datetime.now(timezone.utc)
            channel.error_message = str(exc)

            if channel.retry_count >= channel.max_retries:
                channel.status = "FAILED"
                return

            delay = 60 * (2 ** (channel.retry_count - 1))
            from datetime import timedelta
            channel.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
            raise self.retry(exc=exc, countdown=delay)
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/test_notifier_task.py -v
```
Expected: 2 PASSED

**Step 5: Commit**

```bash
git add notifier/tasks.py tests/unit/test_notifier_task.py
git commit -m "feat: add notification worker with dedup, per-channel retry"
```

---

## Task 15: FastAPI App — Main + Health Endpoints

**Files:**
- Create: `api/main.py`
- Create: `tests/unit/test_api_health.py`

**Step 1: Write the failing tests**

`tests/unit/test_api_health.py`:
```python
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    from api.main import app
    return TestClient(app)


def test_health_always_returns_200(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_returns_503_when_db_fails(client):
    with patch("api.main.get_engine") as mock_engine:
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(side_effect=Exception("db down"))
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_engine.return_value.connect.return_value = mock_conn
        response = client.get("/ready")
        assert response.status_code == 503
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_api_health.py -v
```
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write implementation**

`api/main.py`:
```python
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import redis as redis_lib

from shared.db import get_engine
from shared.config import get_settings
from api.routes import files, jobs, alerts

app = FastAPI(title="ETL System API", version="1.0.0")

app.include_router(files.router, prefix="/v1")
app.include_router(jobs.router, prefix="/v1")
app.include_router(alerts.router, prefix="/v1")


@app.get("/health")
def health():
    """Liveness probe — always 200."""
    return {"status": "ok"}


@app.get("/ready")
def ready():
    """Readiness probe — checks all dependencies."""
    settings = get_settings()
    status = {"postgres": "ok", "redis": "ok", "minio": "ok", "ready": True}
    http_status = 200

    # Check PostgreSQL
    try:
        engine = get_engine()
        with engine.connect() as conn:
            from sqlalchemy import text
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        status["postgres"] = f"error: {exc}"
        status["ready"] = False
        http_status = 503

    # Check Redis
    try:
        r = redis_lib.from_url(settings.REDIS_URL, socket_timeout=3)
        r.ping()
    except Exception as exc:
        status["redis"] = f"error: {exc}"
        status["ready"] = False
        http_status = 503

    # Check MinIO
    try:
        from shared.minio_client import MinioClient
        MinioClient()
    except Exception as exc:
        status["minio"] = f"error: {exc}"
        status["ready"] = False
        http_status = 503

    return JSONResponse(content=status, status_code=http_status)
```

**Step 4: Create empty route files (needed for import)**

```python
# api/routes/files.py  — placeholder
from fastapi import APIRouter
router = APIRouter(prefix="/files", tags=["files"])

# api/routes/jobs.py  — placeholder
from fastapi import APIRouter
router = APIRouter(prefix="/jobs", tags=["jobs"])

# api/routes/alerts.py  — placeholder
from fastapi import APIRouter
router = APIRouter(prefix="/alerts", tags=["alerts"])
```

**Step 5: Run tests to verify they pass**

```bash
pip install fastapi uvicorn httpx
pytest tests/unit/test_api_health.py -v
```
Expected: 2 PASSED

**Step 6: Commit**

```bash
git add api/main.py api/routes/ tests/unit/test_api_health.py
git commit -m "feat: add FastAPI app with /health and /ready endpoints"
```

---

## Task 16: API Routes — Files + Jobs + Alerts

**Files:**
- Modify: `api/routes/files.py`
- Modify: `api/routes/jobs.py`
- Modify: `api/routes/alerts.py`
- Create: `api/schemas/files.py`
- Create: `api/schemas/jobs.py`
- Create: `api/schemas/alerts.py`
- Create: `tests/unit/test_api_routes.py`

**Step 1: Create schemas**

`api/schemas/files.py`:
```python
from pydantic import BaseModel
from datetime import date, datetime
from typing import Optional


class FileOut(BaseModel):
    id: str
    file_date: date
    filename: Optional[str]
    hash_sha256: str
    is_valid: bool
    is_processed: bool
    downloaded_at: Optional[datetime]

    model_config = {"from_attributes": True}


class FileListOut(BaseModel):
    items: list[FileOut]
    total: int
    limit: int
    offset: int
```

`api/schemas/jobs.py`:
```python
from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class JobRunRequest(BaseModel):
    file_id: str


class JobRunResponse(BaseModel):
    job_id: str
    status: str


class StepOut(BaseModel):
    step_name: str
    status: str
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    error_message: Optional[str]

    model_config = {"from_attributes": True}


class JobOut(BaseModel):
    id: str
    status: str
    triggered_by: str
    rows_total: Optional[int]
    rows_ok: Optional[int]
    rows_bad: Optional[int]
    retry_count: int
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    steps: list[StepOut] = []

    model_config = {"from_attributes": True}
```

`api/schemas/alerts.py`:
```python
from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class ChannelOut(BaseModel):
    channel: str
    status: str
    sent_at: Optional[datetime]
    retry_count: int
    error_message: Optional[str]

    model_config = {"from_attributes": True}


class AlertOut(BaseModel):
    id: str
    event_type: str
    severity: str
    message: str
    created_at: datetime
    channels: list[ChannelOut] = []

    model_config = {"from_attributes": True}
```

**Step 2: Write the failing tests**

`tests/unit/test_api_routes.py`:
```python
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    from api.main import app
    return TestClient(app)


def test_get_files_returns_list(client):
    with patch("api.routes.files.get_db_session") as mock_db:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.query().count.return_value = 0
        mock_session.query().order_by().offset().limit().all.return_value = []

        response = client.get("/v1/files")
        assert response.status_code == 200
        assert "items" in response.json()


def test_post_jobs_run_queues_job(client):
    with patch("api.routes.jobs.get_db_session") as mock_db, \
         patch("api.routes.jobs.run_etl") as mock_task:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)
        from shared.models import EtlFile
        mock_session.query().filter_by().first.return_value = EtlFile(id="f-1")
        mock_task.apply_async.return_value.id = "task-1"

        response = client.post("/v1/jobs/run", json={"file_id": "f-1"})
        assert response.status_code == 200
        assert response.json()["status"] == "QUEUED"
```

**Step 3: Run test to verify it fails**

```bash
pytest tests/unit/test_api_routes.py -v
```
Expected: FAIL (routes not implemented yet)

**Step 4: Implement routes**

`api/routes/files.py`:
```python
import uuid
from datetime import datetime, timezone
from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from shared.db import get_db_session
from shared.minio_client import MinioClient
from shared.models import EtlFile
from shared.config import get_settings
from api.schemas.files import FileOut, FileListOut

router = APIRouter(prefix="/files", tags=["files"])


@router.get("", response_model=FileListOut)
def list_files(limit: int = Query(20, le=100), offset: int = Query(0)):
    with get_db_session() as session:
        total = session.query(EtlFile).count()
        items = session.query(EtlFile).order_by(EtlFile.downloaded_at.desc()).offset(offset).limit(limit).all()
        return FileListOut(
            items=[FileOut.model_validate(f) for f in items],
            total=total, limit=limit, offset=offset
        )


@router.get("/{file_id}", response_model=FileOut)
def get_file(file_id: str):
    with get_db_session() as session:
        f = session.query(EtlFile).filter_by(id=file_id).first()
        if not f:
            raise HTTPException(status_code=404, detail="File not found")
        return FileOut.model_validate(f)


@router.post("/upload", response_model=FileOut)
def upload_file(file: UploadFile = File(...)):
    file_bytes = file.file.read()
    import hashlib
    from datetime import date
    hash_sha256 = hashlib.sha256(file_bytes).hexdigest()
    today = date.today()
    minio = MinioClient()
    path = f"{today.year}/{today.month:02d}/{today.day:02d}/{file.filename}"
    minio.upload_file(file_bytes, path)
    with get_db_session() as session:
        etl_file = EtlFile(
            id=str(uuid.uuid4()),
            file_date=today,
            filename=file.filename,
            hash_sha256=hash_sha256,
            minio_path=path,
            downloaded_at=datetime.now(timezone.utc),
            is_valid=True,
        )
        session.add(etl_file)
        session.flush()
        return FileOut.model_validate(etl_file)


@router.post("/sync")
def sync_file():
    """Trigger manual download from external API (runs checker task)."""
    from checker.checker import run_daily
    task = run_daily.apply_async(queue="etl_jobs")
    return {"task_id": task.id, "status": "QUEUED"}
```

`api/routes/jobs.py`:
```python
import uuid
from fastapi import APIRouter, HTTPException, Query
from shared.db import get_db_session
from shared.models import EtlFile, EtlJobRun
from api.schemas.jobs import JobRunRequest, JobRunResponse, JobOut
from worker.tasks import run_etl

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("/run", response_model=JobRunResponse)
def run_job(req: JobRunRequest):
    with get_db_session() as session:
        f = session.query(EtlFile).filter_by(id=req.file_id).first()
        if not f:
            raise HTTPException(status_code=404, detail="File not found")
    task = run_etl.apply_async(
        kwargs={"job_id": None, "file_id": req.file_id},
        queue="etl_jobs",
    )
    return JobRunResponse(job_id=task.id, status="QUEUED")


@router.post("/reprocess/{file_id}", response_model=JobRunResponse)
def reprocess(file_id: str):
    task = run_etl.apply_async(
        kwargs={"job_id": None, "file_id": file_id},
        queue="etl_jobs",
    )
    return JobRunResponse(job_id=task.id, status="QUEUED")


@router.get("", response_model=list[JobOut])
def list_jobs(
    status: str | None = None,
    limit: int = Query(20, le=100),
    offset: int = Query(0)
):
    with get_db_session() as session:
        q = session.query(EtlJobRun)
        if status:
            q = q.filter(EtlJobRun.status == status)
        jobs = q.order_by(EtlJobRun.started_at.desc()).offset(offset).limit(limit).all()
        return [JobOut.model_validate(j) for j in jobs]


@router.get("/{job_id}", response_model=JobOut)
def get_job(job_id: str):
    with get_db_session() as session:
        j = session.query(EtlJobRun).filter_by(id=job_id).first()
        if not j:
            raise HTTPException(status_code=404, detail="Job not found")
        return JobOut.model_validate(j)
```

`api/routes/alerts.py`:
```python
from fastapi import APIRouter, HTTPException, Query
from shared.db import get_db_session
from shared.models import AlertEvent
from api.schemas.alerts import AlertOut

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("", response_model=list[AlertOut])
def list_alerts(
    severity: str | None = None,
    limit: int = Query(20, le=100),
    offset: int = Query(0)
):
    with get_db_session() as session:
        q = session.query(AlertEvent)
        if severity:
            q = q.filter(AlertEvent.severity == severity)
        alerts = q.order_by(AlertEvent.created_at.desc()).offset(offset).limit(limit).all()
        return [AlertOut.model_validate(a) for a in alerts]


@router.get("/{alert_id}", response_model=AlertOut)
def get_alert(alert_id: str):
    with get_db_session() as session:
        a = session.query(AlertEvent).filter_by(id=alert_id).first()
        if not a:
            raise HTTPException(status_code=404, detail="Alert not found")
        return AlertOut.model_validate(a)
```

**Step 5: Run tests to verify they pass**

```bash
pytest tests/unit/test_api_routes.py -v
```
Expected: 2 PASSED

**Step 6: Run full test suite**

```bash
pytest tests/unit/ -v
```
Expected: All tests PASS

**Step 7: Commit**

```bash
git add api/routes/ api/schemas/ tests/unit/test_api_routes.py
git commit -m "feat: add API routes for files, jobs, alerts with pagination"
```

---

## Task 17: Local Watcher (Outside Docker)

**Files:**
- Create: `local_watcher/watcher.py`
- Create: `tests/unit/test_watcher.py`

**Step 1: Write the failing test**

`tests/unit/test_watcher.py`:
```python
import pytest
import os
import tempfile
from pathlib import Path


def test_watcher_detects_flag_file(tmp_path):
    from local_watcher.watcher import process_flag_file
    flag = tmp_path / "20260227_120000_CRITICAL_FILE_MISSING.flag"
    flag.write_text("CRITICAL|FILE_MISSING|File not found\n{}")

    with pytest.raises(SystemExit):
        # Mocked toast: if winotify not available, just logs
        pass

    result = process_flag_file(str(flag))
    assert result["severity"] == "CRITICAL"
    assert result["event_type"] == "FILE_MISSING"


def test_parse_flag_filename():
    from local_watcher.watcher import parse_flag_filename
    result = parse_flag_filename("20260227_120000_CRITICAL_ETL_DEAD.flag")
    assert result["severity"] == "CRITICAL"
    assert result["event_type"] == "ETL_DEAD"
```

**Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_watcher.py -v
```
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write implementation**

`local_watcher/watcher.py`:
```python
"""
local_watcher.py — runs OUTSIDE Docker on the Windows host.
Monitors the alerts volume directory for .flag files and triggers local notifications.

Usage:
    python -m local_watcher.watcher --dir C:/path/to/alerts

Run as Windows service or via Task Scheduler.
"""
import os
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("local_watcher")


def parse_flag_filename(filename: str) -> dict:
    """Parse: YYYYMMDD_HHMMSS_SEVERITY_EVENT_TYPE.flag"""
    name = Path(filename).stem  # strip .flag
    parts = name.split("_", 3)  # ts_date, ts_time, severity, event_type
    if len(parts) < 4:
        return {"severity": "UNKNOWN", "event_type": "UNKNOWN"}
    return {"severity": parts[2], "event_type": parts[3]}


def process_flag_file(path: str) -> dict:
    content = Path(path).read_text()
    first_line = content.split("\n")[0]
    parts = first_line.split("|", 2)
    severity = parts[0] if len(parts) > 0 else "UNKNOWN"
    event_type = parts[1] if len(parts) > 1 else "UNKNOWN"
    message = parts[2] if len(parts) > 2 else ""

    logger.warning(f"ALERT [{severity}] {event_type}: {message}")

    # Try Windows toast notification
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
        logger.info("winotify not available — skipping toast")
    except Exception as exc:
        logger.error(f"Toast failed: {exc}")

    # Archive processed flag
    archive_path = path.replace(".flag", ".processed")
    os.rename(path, archive_path)

    return {"severity": severity, "event_type": event_type, "message": message}


def watch(flag_dir: str, poll_interval: int = 5):
    logger.info(f"Watching {flag_dir} for .flag files every {poll_interval}s")
    os.makedirs(flag_dir, exist_ok=True)
    while True:
        for fname in os.listdir(flag_dir):
            if fname.endswith(".flag"):
                process_flag_file(os.path.join(flag_dir, fname))
        time.sleep(poll_interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL local alert watcher")
    parser.add_argument("--dir", default="./alerts", help="Directory to watch")
    parser.add_argument("--interval", type=int, default=5, help="Poll interval in seconds")
    args = parser.parse_args()
    watch(args.dir, args.interval)
```

**Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/test_watcher.py -v
```
Expected: 2 PASSED

**Step 5: Commit**

```bash
git add local_watcher/watcher.py tests/unit/test_watcher.py
git commit -m "feat: add local watcher for Windows toast alerts via flag files"
```

---

## Task 18: Full Test Suite + Docker Smoke Test

**Files:**
- Create: `tests/integration/test_smoke.py`

**Step 1: Run all unit tests**

```bash
pytest tests/unit/ -v --tb=short
```
Expected: All tests PASS. If any fail, fix them before proceeding.

**Step 2: Create smoke test**

`tests/integration/test_smoke.py`:
```python
"""
Integration smoke test — requires docker-compose services running.
Run with: pytest tests/integration/ -v -m integration
"""
import pytest
import httpx
import os


SMOKE_BASE_URL = os.getenv("SMOKE_API_URL", "http://localhost:8000")


@pytest.mark.integration
def test_health_endpoint():
    response = httpx.get(f"{SMOKE_BASE_URL}/health", timeout=5)
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.integration
def test_ready_endpoint():
    response = httpx.get(f"{SMOKE_BASE_URL}/ready", timeout=10)
    assert response.status_code == 200
    assert response.json()["ready"] is True


@pytest.mark.integration
def test_list_files_empty():
    response = httpx.get(f"{SMOKE_BASE_URL}/v1/files", timeout=5)
    assert response.status_code == 200
    assert response.json()["total"] == 0


@pytest.mark.integration
def test_list_jobs_empty():
    response = httpx.get(f"{SMOKE_BASE_URL}/v1/jobs", timeout=5)
    assert response.status_code == 200
    assert isinstance(response.json(), list)
```

**Step 3: Add pytest.ini for markers**

`pytest.ini`:
```ini
[pytest]
asyncio_mode = auto
markers =
    integration: integration tests requiring docker-compose
```

**Step 4: Build and start all services**

```bash
cd /c/Users/MB\ NEGOCIOS/etl-system
cp .env.example .env
# Edit .env with your actual values (POSTGRES_PASSWORD, MINIO_SECRET_KEY, etc.)
docker-compose build
docker-compose up -d
```

Wait ~30 seconds for all healthchecks to pass:

```bash
docker-compose ps
```
Expected: All services show `healthy` or `running`.

**Step 5: Apply migrations**

```bash
docker-compose exec api alembic upgrade head
```
Expected: "Running upgrade -> xxxx"

**Step 6: Run smoke tests**

```bash
pytest tests/integration/ -v -m integration
```
Expected: 4 PASSED

**Step 7: Final commit**

```bash
git add tests/integration/ pytest.ini
git commit -m "test: add integration smoke tests and pytest config"
```

---

## Task 19: README and Operational Guide

**Files:**
- Create: `README.md`

**Step 1: Create `README.md`**

```markdown
# ETL System

Automated ETL with daily scheduling, multicanal alerts, idempotent pipeline.

## Quick Start

```bash
cp .env.example .env
# Edit .env with real values
docker-compose build
docker-compose up -d
docker-compose exec api alembic upgrade head
```

## Services

| Service | URL |
|---|---|
| API | http://localhost:8000/docs |
| MinIO Console | http://localhost:9001 |

## API Reference

See http://localhost:8000/docs (Swagger UI auto-generated).

## Local Watcher (Windows)

```bash
pip install -r requirements/local_watcher.txt
python -m local_watcher.watcher --dir C:/path/to/alerts/volume
```

Map the `alerts_data` Docker volume to a Windows path in docker-compose.yml.

## Configuring the Entity

Update these files with your actual data entity:

- `worker/steps/validate.py` — `REQUIRED_COLUMNS` list
- `worker/steps/stage.py` — `STAGING_TABLE` name
- `worker/steps/upsert.py` — `FINAL_TABLE`, `CONFLICT_KEY`
- Add staging/final table migrations via Alembic

## Running Tests

```bash
# Unit tests (no Docker required)
pytest tests/unit/ -v

# Integration tests (requires docker-compose up)
pytest tests/integration/ -v -m integration
```
```

**Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add README with quick start and operational guide"
```

---

## Summary

| Task | Component | Tests |
|---|---|---|
| 1 | Scaffold (requirements, compose, dockerfiles) | — |
| 2 | Shared config (pydantic-settings) | 2 unit |
| 3 | SQLAlchemy models | 2 unit |
| 4 | DB session + Alembic migrations | — |
| 5 | MinIO client | 2 unit |
| 6 | Celery apps (worker/checker/notifier) | — |
| 7 | Checkpoint system | 3 unit |
| 8 | Checker service | 3 unit |
| 9 | ETL worker main task + retry | 2 unit |
| 10 | ETL steps: extract, validate, clean | 3 unit |
| 11 | ETL steps: enrich, stage, upsert | regression |
| 12 | Notification dedup key | 4 unit |
| 13 | Notification strategies | 2 unit |
| 14 | Notification worker task | 2 unit |
| 15 | FastAPI + /health /ready | 2 unit |
| 16 | API routes (files, jobs, alerts) | 2 unit |
| 17 | Local watcher | 2 unit |
| 18 | Docker build + smoke test | 4 integration |
| 19 | README | — |

**Total unit tests: ~31 | Integration: 4**
