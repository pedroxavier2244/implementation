import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import IntegrityError
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
    engine = create_engine("sqlite:///:memory:")
    from shared.models import Base, EtlFile
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        import uuid
        from datetime import date, datetime, timezone
        f1 = EtlFile(
            id=str(uuid.uuid4()),
            file_date=date(2026, 2, 27),
            hash_sha256="abc123",
            downloaded_at=datetime.now(timezone.utc),
        )
        s.add(f1)
        s.commit()
        f2 = EtlFile(
            id=str(uuid.uuid4()),
            file_date=date(2026, 2, 27),
            hash_sha256="abc123",
            downloaded_at=datetime.now(timezone.utc),
        )
        s.add(f2)
        with pytest.raises(IntegrityError):
            s.commit()


def test_etl_job_run_relationships():
    engine = create_engine("sqlite:///:memory:")
    from shared.models import Base, EtlFile, EtlJobRun, EtlJobStep
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        import uuid
        from datetime import date, datetime, timezone
        f = EtlFile(
            id=str(uuid.uuid4()),
            file_date=date(2026, 2, 27),
            hash_sha256="xyz999",
            downloaded_at=datetime.now(timezone.utc),
        )
        j = EtlJobRun(
            id=str(uuid.uuid4()),
            file_id=f.id,
            status="QUEUED",
            triggered_by="scheduler",  # now required since no default
        )
        step = EtlJobStep(
            id=str(uuid.uuid4()),
            job_id=j.id,
            step_name="extract",
            status="DONE",
        )
        # Use ORM relationship to append step
        j.steps.append(step)
        f.jobs.append(j)
        s.add(f)
        s.commit()
        # Reload and verify via relationships
        s.expire_all()
        loaded_file = s.get(EtlFile, f.id)
        assert len(loaded_file.jobs) == 1
        loaded_job = loaded_file.jobs[0]
        assert loaded_job.status == "QUEUED"
        assert len(loaded_job.steps) == 1
        assert loaded_job.steps[0].step_name == "extract"


def test_alert_event_dedup_key_unique():
    engine = create_engine("sqlite:///:memory:")
    from shared.models import Base, AlertEvent
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        import uuid
        a1 = AlertEvent(
            id=str(uuid.uuid4()),
            dedup_key="job:abc:ETL_DEAD",
            event_type="ETL_DEAD",
            severity="CRITICAL",
            message="test",
        )
        s.add(a1)
        s.commit()
        a2 = AlertEvent(
            id=str(uuid.uuid4()),
            dedup_key="job:abc:ETL_DEAD",  # same dedup_key
            event_type="ETL_DEAD",
            severity="CRITICAL",
            message="test2",
        )
        s.add(a2)
        with pytest.raises(IntegrityError):
            s.commit()


def test_metadata_column_name_is_metadata_not_metadata_underscore():
    engine = create_engine("sqlite:///:memory:")
    from shared.models import Base
    Base.metadata.create_all(engine)
    inspector = inspect(engine)
    cols = {c["name"] for c in inspector.get_columns("alert_event")}
    assert "metadata" in cols, "DB column should be named 'metadata'"
    assert "metadata_" not in cols, "DB column should NOT be named 'metadata_'"
