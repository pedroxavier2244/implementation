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
        # Reimport to get fresh Settings instance
        import importlib
        import shared.config
        importlib.reload(shared.config)
        from shared.config import Settings
        s = Settings(_env_file=None)
        assert s.POSTGRES_HOST == "localhost"
        assert s.REDIS_URL == "redis://localhost:6379/0"
        assert s.BAD_ROW_THRESHOLD_PCT == 5.0  # default value


def test_settings_database_url():
    env = {
        "POSTGRES_HOST": "pg",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "db",
        "POSTGRES_USER": "u",
        "POSTGRES_PASSWORD": "p",
        "REDIS_URL": "redis://r:6379/0",
        "MINIO_ENDPOINT": "m:9000",
        "MINIO_ACCESS_KEY": "k",
        "MINIO_SECRET_KEY": "s",
        "MINIO_BUCKET": "b",
        "ETL_SOURCE_API_URL": "http://x.com",
    }
    with patch.dict(os.environ, env, clear=True):
        import importlib
        import shared.config
        importlib.reload(shared.config)
        from shared.config import Settings
        s = Settings(_env_file=None)
        assert s.database_url == "postgresql+psycopg2://u:p@pg:5432/db"
        assert s.celery_broker_url == "redis://r:6379/0"


def test_settings_defaults():
    env = {
        "POSTGRES_HOST": "h",
        "POSTGRES_DB": "d",
        "POSTGRES_USER": "u",
        "POSTGRES_PASSWORD": "p",
        "REDIS_URL": "redis://r:6379/0",
        "MINIO_ENDPOINT": "m:9000",
        "MINIO_ACCESS_KEY": "k",
        "MINIO_SECRET_KEY": "s",
        "MINIO_BUCKET": "b",
        "ETL_SOURCE_API_URL": "http://x.com",
    }
    with patch.dict(os.environ, env, clear=True):
        import importlib
        import shared.config
        importlib.reload(shared.config)
        from shared.config import Settings
        s = Settings(_env_file=None)
        assert s.POSTGRES_PORT == 5432
        assert s.MINIO_SECURE == False
        assert s.BAD_ROW_THRESHOLD_PCT == 5.0
        assert s.MAX_RETRIES == 3


def test_cnpj_settings_defaults():
    env = {
        "POSTGRES_HOST": "h",
        "POSTGRES_DB": "d",
        "POSTGRES_USER": "u",
        "POSTGRES_PASSWORD": "p",
        "REDIS_URL": "redis://r:6379/0",
        "MINIO_ENDPOINT": "m:9000",
        "MINIO_ACCESS_KEY": "k",
        "MINIO_SECRET_KEY": "s",
        "MINIO_BUCKET": "b",
        "ETL_SOURCE_API_URL": "http://x.com",
    }
    with patch.dict(os.environ, env, clear=True):
        import importlib
        import shared.config
        importlib.reload(shared.config)
        from shared.config import Settings
        s = Settings(_env_file=None)
        assert s.CNPJ_CACHE_TTL_DAYS == 30
        assert s.BRASILAPI_TIMEOUT == 10
