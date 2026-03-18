import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Boolean, Integer, BigInteger, Text, DateTime,
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


class AlertEvent(Base):
    __tablename__ = "alert_event"

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    dedup_key = Column(Text, nullable=False, unique=True)
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
    status = Column(String(20), nullable=False)
    sent_at = Column(DateTime(timezone=True))
    error_message = Column(Text)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    last_retry_at = Column(DateTime(timezone=True))
    next_retry_at = Column(DateTime(timezone=True))

    alert = relationship("AlertEvent", back_populates="channels")


class AnalyticsIndicatorSnapshot(Base):
    __tablename__ = "analytics_indicator_snapshot"

    indicator = Column(Text, primary_key=True)
    reference_date = Column(Date, primary_key=True)
    total = Column(BigInteger, nullable=False, default=0)
    source_sheet = Column(Text)
    source_column = Column(Text)
    job_id = Column(String(36), ForeignKey("etl_job_run.id"), nullable=False)
    file_id = Column(String(36), ForeignKey("etl_file.id"))
    loaded_at = Column(DateTime(timezone=True), default=utcnow)


class VisaoClienteChangeHistory(Base):
    __tablename__ = "visao_cliente_change_history"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    documento = Column(Text, nullable=False)
    etl_job_id = Column(String(36), ForeignKey("etl_job_run.id"), nullable=False)
    file_id = Column(String(36), ForeignKey("etl_file.id"))
    data_base = Column(Text)
    change_type = Column(String(20), nullable=False)
    field_name = Column(Text)
    old_value = Column(Text)
    new_value = Column(Text)
    changed_at = Column(DateTime(timezone=True), default=utcnow)



# Strategic indexes
Index("idx_job_status",  EtlJobRun.status)
Index("idx_job_file_id", EtlJobRun.file_id)
Index("idx_file_date",   EtlFile.file_date)
Index("idx_alert_severity", AlertEvent.severity)
Index("idx_visao_cliente_change_history_documento_changed_at", VisaoClienteChangeHistory.documento, VisaoClienteChangeHistory.changed_at)
Index("idx_visao_cliente_change_history_etl_job_id", VisaoClienteChangeHistory.etl_job_id)
Index("idx_visao_cliente_change_history_file_id", VisaoClienteChangeHistory.file_id)
