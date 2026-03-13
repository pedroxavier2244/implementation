import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Boolean, Integer, Text, DateTime,
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
    triggered_by = Column(String(20), nullable=False)
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



class CnpjRfCache(Base):
    __tablename__ = "cnpj_rf_cache"

    cnpj               = Column(Text, primary_key=True)
    razao_social       = Column(Text)
    nome_fantasia      = Column(Text)
    situacao_cadastral = Column(Text)
    descricao_situacao = Column(Text)
    cnae_fiscal        = Column(Text)
    cnae_descricao     = Column(Text)
    natureza_juridica  = Column(Text)
    capital_social     = Column(Text)
    porte              = Column(Text)
    uf                 = Column(Text)
    municipio          = Column(Text)
    email              = Column(Text)
    data_inicio_ativ   = Column(Text)
    last_checked_at    = Column(DateTime(timezone=True))


class CnpjDivergencia(Base):
    __tablename__ = "cnpj_divergencia"

    id       = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    job_id   = Column(String(36), ForeignKey("etl_job_run.id"), nullable=False)
    cnpj     = Column(Text, nullable=False)
    campo    = Column(Text, nullable=False)
    valor_c6 = Column(Text)
    valor_rf = Column(Text)
    found_at = Column(DateTime(timezone=True), default=utcnow)


# Strategic indexes
Index("idx_job_status",     EtlJobRun.status)
Index("idx_job_file_id",    EtlJobRun.file_id)
Index("idx_file_date",      EtlFile.file_date)
