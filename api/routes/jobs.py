from fastapi import APIRouter, HTTPException, Query

from api.schemas.jobs import JobOut, JobRunRequest, JobRunResponse
from shared.celery_dispatch import enqueue_task
from shared.db import get_db_session
from shared.models import EtlFile, EtlJobRun

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("/run", response_model=JobRunResponse)
def run_job(request: JobRunRequest):
    with get_db_session() as session:
        etl_file = session.query(EtlFile).filter_by(id=request.file_id).first()
        if etl_file is None:
            raise HTTPException(status_code=404, detail="File not found")

        active_job = (
            session.query(EtlJobRun)
            .filter(
                EtlJobRun.file_id == request.file_id,
                EtlJobRun.status.in_(["QUEUED", "RUNNING", "RETRYING"]),
            )
            .first()
        )
        if active_job:
            return JobRunResponse(job_id=active_job.id, status=active_job.status)

    task = enqueue_task(
        "worker.tasks.run_etl",
        kwargs={"job_id": None, "file_id": request.file_id},
        queue="etl_jobs",
    )
    return JobRunResponse(job_id=task.id, status="QUEUED")


@router.post("/reprocess/{file_id}", response_model=JobRunResponse)
def reprocess(file_id: str):
    task = enqueue_task(
        "worker.tasks.run_etl",
        kwargs={"job_id": None, "file_id": file_id},
        queue="etl_jobs",
    )
    return JobRunResponse(job_id=task.id, status="QUEUED")


@router.get("", response_model=list[JobOut])
def list_jobs(status: str | None = None, limit: int = Query(20, le=100), offset: int = Query(0)):
    with get_db_session() as session:
        query = session.query(EtlJobRun)
        if status:
            query = query.filter(EtlJobRun.status == status)
        items = query.order_by(EtlJobRun.started_at.desc()).offset(offset).limit(limit).all()
        return [JobOut.model_validate(item) for item in items]


@router.get("/{job_id}", response_model=JobOut)
def get_job(job_id: str):
    with get_db_session() as session:
        job = session.query(EtlJobRun).filter_by(id=job_id).first()
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return JobOut.model_validate(job)
