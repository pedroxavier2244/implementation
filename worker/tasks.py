import uuid
from datetime import datetime, timezone

from shared.celery_dispatch import enqueue_task
from shared.db import get_db_session
from shared.models import EtlFile, EtlJobRun
from worker.celery_app import app
from worker.steps.checkpoint import mark_step_failed
from worker.steps.clean import run_clean
from worker.steps.enrich import run_enrich
from worker.steps.extract import clear_cached_dataframe, run_extract
from worker.steps.stage import run_stage
from worker.steps.cnpj_verify import run_cnpj_verify
from worker.steps.upsert import run_upsert
from worker.steps.validate import run_validate


def compute_retry_delay(retry_number: int) -> int:
    return 300 * (2 ** retry_number)


def _send_dead_alert(job_id: str, step_name: str, retry_count: int) -> None:
    enqueue_task(
        "notifier.tasks.dispatch_notification",
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
        if job_id:
            job = session.query(EtlJobRun).filter_by(id=job_id).first()
            if job is None:
                raise ValueError(f"Job not found: {job_id}")
        else:
            etl_file = session.query(EtlFile).filter_by(id=file_id).first()
            if etl_file is None:
                raise ValueError(f"File not found: {file_id}")

            job = EtlJobRun(
                id=str(uuid.uuid4()),
                file_id=etl_file.id,
                status="RUNNING",
                triggered_by="scheduler",
                started_at=datetime.now(timezone.utc),
                max_retries=3,
            )
            session.add(job)
            session.commit()
            job_id = job.id

        job.status = "RUNNING"
        if job.started_at is None:
            job.started_at = datetime.now(timezone.utc)

        etl_file = session.query(EtlFile).filter_by(id=job.file_id).first()
        if etl_file is None:
            raise ValueError(f"File not found: {job.file_id}")

        current_step = "unknown"
        try:
            current_step = "extract"
            run_extract(session, job_id, etl_file)

            current_step = "clean"
            run_clean(session, job_id)

            current_step = "enrich"
            run_enrich(session, job_id)

            current_step = "validate"
            run_validate(session, job_id, etl_file)

            current_step = "stage"
            run_stage(session, job_id)

            current_step = "upsert"
            run_upsert(session, job_id)

            current_step = "cnpj_verify"
            run_cnpj_verify(session, job_id)

            job.status = "DONE"
            job.finished_at = datetime.now(timezone.utc)
            etl_file.is_processed = True
            clear_cached_dataframe(job_id)

        except Exception as exc:
            # Clear failed transaction state before persisting failure metadata.
            session.rollback()
            mark_step_failed(session, job_id, current_step, str(exc))

            retry_count = self.request.retries
            job.retry_count = retry_count + 1
            job.last_retry_at = datetime.now(timezone.utc)

            if retry_count >= (job.max_retries or 3):
                job.status = "DEAD"
                job.error_message = str(exc)
                job.finished_at = datetime.now(timezone.utc)
                clear_cached_dataframe(job_id)
                _send_dead_alert(job_id, current_step, retry_count)
                return

            job.status = "RETRYING"
            delay = compute_retry_delay(retry_count)
            raise self.retry(exc=exc, countdown=delay)
