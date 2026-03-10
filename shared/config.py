from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # PostgreSQL
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "etl_db"
    POSTGRES_USER: str = "etl_user"
    POSTGRES_PASSWORD: str = "change_me"
    POSTGRES_SSLMODE: str = "disable"  # "require" para Neon/produção
    DB_POOL_SIZE: int = 5
    DB_MAX_OVERFLOW: int = 0
    DB_POOL_TIMEOUT: int = 30

    # Redis
    REDIS_URL: str = "redis://redis:6379/0"

    # MinIO
    MINIO_ENDPOINT: str = "minio:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "change_me"
    MINIO_BUCKET: str = "etl-files"
    MINIO_SECURE: bool = False

    # ETL Config
    BAD_ROW_THRESHOLD_PCT: float = 5.0
    MAX_RETRIES: int = 3

    # CNPJ Verification
    CNPJ_CACHE_TTL_DAYS: int = 30
    BRASILAPI_TIMEOUT: int = 10
    CNPJ_VERIFY_BATCH_SIZE: int = 300  # max CNPJs verified per job run

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
            f"?sslmode={self.POSTGRES_SSLMODE}"
        )

    @property
    def celery_broker_url(self) -> str:
        return self.REDIS_URL


@lru_cache
def get_settings() -> Settings:
    return Settings()
