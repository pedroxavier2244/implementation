"""
Fixtures de integração para testes e2e.

Requer Docker Engine rodando. Sobe PostgreSQL via testcontainers.
MinIO é mockado via moto (mock_s3). Redis via fakeredis.
"""
import os
import re

import fakeredis
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from testcontainers.postgres import PostgresContainer


# ── monkeypatch com escopo session ────────────────────────────────────────────

@pytest.fixture(scope="session")
def monkeypatch_session():
    """monkeypatch com escopo de session (não existe nativamente no pytest)."""
    from _pytest.monkeypatch import MonkeyPatch
    mp = MonkeyPatch()
    yield mp
    mp.undo()


# ── PostgreSQL ────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def pg_container():
    """Sobe container PostgreSQL para a sessão de testes inteira."""
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture(scope="session")
def pg_url(pg_container):
    """URL de conexão ao PostgreSQL de teste (driver psycopg2)."""
    url = pg_container.get_connection_url()
    # testcontainers pode retornar postgresql+psycopg2 ou postgresql — normaliza
    if "psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://")
    return url


@pytest.fixture(scope="session")
def run_migrations(pg_url, test_env):
    """Roda alembic upgrade head no banco de teste."""
    from alembic import command
    from alembic.config import Config as AlembicConfig
    import os

    alembic_cfg = AlembicConfig(os.path.join(os.path.dirname(__file__), "..", "..", "alembic.ini"))
    alembic_cfg.set_main_option("sqlalchemy.url", pg_url)
    command.upgrade(alembic_cfg, "head")
    return pg_url


# ── MinIO (moto mock_s3) ──────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def aws_mock():
    """Ativa mock S3 da moto para toda a sessão."""
    with mock_aws():
        import boto3
        s3 = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        s3.create_bucket(Bucket="etl-files")
        yield s3


# ── Redis (fakeredis) ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def fake_redis_server():
    """Servidor fakeredis para a sessão."""
    return fakeredis.FakeServer()


# ── Patch de configuração e caches ───────────────────────────────────────────

@pytest.fixture(scope="session")
def test_env(pg_url, fake_redis_server, aws_mock):
    """
    Sobrescreve variáveis de ambiente para apontar para os serviços de teste.
    Limpa caches lru_cache de get_settings() e get_engine().
    """
    m = re.match(
        r"postgresql\+psycopg2://(?P<user>[^:]+):(?P<pw>[^@]+)@(?P<host>[^:]+):(?P<port>\d+)/(?P<db>.+)",
        pg_url,
    )
    assert m, f"URL inesperada: {pg_url}"

    env_overrides = {
        "POSTGRES_HOST": m.group("host"),
        "POSTGRES_PORT": m.group("port"),
        "POSTGRES_DB": m.group("db"),
        "POSTGRES_USER": m.group("user"),
        "POSTGRES_PASSWORD": m.group("pw"),
        "REDIS_URL": "redis://localhost:6379/0",
        "MINIO_ENDPOINT": "s3.amazonaws.com",
        "MINIO_ACCESS_KEY": "test",
        "MINIO_SECRET_KEY": "test",
        "MINIO_BUCKET": "etl-files",
        "MINIO_SECURE": "false",
        "CNPJ_VERIFY_BATCH_SIZE": "0",
    }

    original = {k: os.environ.get(k) for k in env_overrides}
    os.environ.update(env_overrides)

    from shared.config import get_settings
    from shared.db import get_engine
    get_settings.cache_clear()
    get_engine.cache_clear()

    yield env_overrides

    for k, v in original.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    get_settings.cache_clear()
    get_engine.cache_clear()


# ── Patch do MinioClient ──────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def patch_minio(aws_mock, test_env):
    """
    Substitui MinioClient por uma implementação em memória para evitar
    problemas de assinatura com endpoint customizado.
    """
    from unittest.mock import patch

    _store: dict[str, bytes] = {}

    class _FakeMinioClient:
        def __init__(self):
            self.bucket = "etl-files"

        def upload_file(self, file_bytes: bytes, object_name: str) -> str:
            _store[object_name] = file_bytes
            return object_name

        def download_file(self, object_name: str) -> bytes:
            if object_name not in _store:
                raise KeyError(f"Object not found: {object_name}")
            return _store[object_name]

        def object_exists(self, object_name: str) -> bool:
            return object_name in _store

    with patch("shared.minio_client.MinioClient", _FakeMinioClient):
        yield


# ── Patch do Celery (modo eager) ──────────────────────────────────────────────

@pytest.fixture(scope="session")
def patch_celery_eager(fake_redis_server):
    """Faz tasks Celery rodarem sincronamente (sem broker real)."""
    from unittest.mock import patch, MagicMock
    import fakeredis

    fake_conn = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=False)

    from worker.celery_app import app as celery_app
    celery_app.conf.update(
        task_always_eager=True,
        task_eager_propagates=True,
    )

    from worker.tasks import run_etl as _run_etl_task

    def _smart_enqueue(task_name, args=None, kwargs=None, queue=None):
        if task_name == "worker.tasks.run_etl":
            result = _run_etl_task.apply(kwargs=kwargs or {})
            mock = MagicMock()
            mock.id = result.id or "eager-task-id"
            return mock
        mock = MagicMock()
        mock.id = "mock-notification-task"
        return mock

    with patch("redis.from_url", return_value=fake_conn):
        with patch("shared.celery_dispatch.enqueue_task", side_effect=_smart_enqueue):
            with patch("api.routes.jobs.enqueue_task", side_effect=_smart_enqueue):
                with patch("api.routes.files.enqueue_task", side_effect=_smart_enqueue):
                    yield


# ── Patch da BrasilAPI ────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def patch_brasilapi():
    """Mock da BrasilAPI para evitar HTTP externo."""
    from unittest.mock import patch

    with patch("shared.brasilapi.fetch_cnpj", return_value=None):
        yield


# ── Cliente FastAPI ───────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def client(run_migrations, test_env, patch_minio, patch_celery_eager, patch_brasilapi):
    """TestClient FastAPI com toda a infraestrutura de teste ativa."""
    from api.main import app
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
