from datetime import datetime, timezone

from sqlalchemy.orm import Session

from shared.models import EtlJobStep


def is_step_done(session: Session, job_id: str, step_name: str) -> bool:
    step = session.query(EtlJobStep).filter_by(job_id=job_id, step_name=step_name).first()
    return step is not None and step.status == "DONE"


def begin_step(session: Session, job_id: str, step_name: str) -> EtlJobStep:
    step = session.query(EtlJobStep).filter_by(job_id=job_id, step_name=step_name).first()
    now = datetime.now(timezone.utc)
    if step is None:
        step = EtlJobStep(
            job_id=job_id,
            step_name=step_name,
            status="RUNNING",
            started_at=now,
        )
        session.add(step)
    else:
        step.status = "RUNNING"
        step.started_at = now
        step.finished_at = None
        step.error_message = None
    session.flush()
    return step


def mark_step_done(session: Session, job_id: str, step_name: str) -> None:
    step = session.query(EtlJobStep).filter_by(job_id=job_id, step_name=step_name).first()
    if step is None:
        step = begin_step(session, job_id, step_name)
    step.status = "DONE"
    step.finished_at = datetime.now(timezone.utc)
    session.flush()


def mark_step_failed(session: Session, job_id: str, step_name: str, error: str) -> None:
    step = session.query(EtlJobStep).filter_by(job_id=job_id, step_name=step_name).first()
    if step is None:
        step = begin_step(session, job_id, step_name)
    step.status = "FAILED"
    step.finished_at = datetime.now(timezone.utc)
    step.error_message = error
    session.flush()
