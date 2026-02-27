import hashlib
import uuid
from datetime import date, datetime, timezone

from fastapi import APIRouter, File, HTTPException, Query, UploadFile

from api.schemas.files import FileListOut, FileOut
from shared.celery_dispatch import enqueue_task
from shared.db import get_db_session
from shared.minio_client import MinioClient
from shared.models import EtlFile

router = APIRouter(prefix="/files", tags=["files"])


@router.get("", response_model=FileListOut)
def list_files(limit: int = Query(20, le=100), offset: int = Query(0)):
    with get_db_session() as session:
        total = session.query(EtlFile).count()
        items = (
            session.query(EtlFile)
            .order_by(EtlFile.downloaded_at.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )
        return FileListOut(
            items=[FileOut.model_validate(item) for item in items],
            total=total,
            limit=limit,
            offset=offset,
        )


@router.get("/{file_id}", response_model=FileOut)
def get_file(file_id: str):
    with get_db_session() as session:
        etl_file = session.query(EtlFile).filter_by(id=file_id).first()
        if etl_file is None:
            raise HTTPException(status_code=404, detail="File not found")
        return FileOut.model_validate(etl_file)


@router.post("/upload", response_model=FileOut)
def upload_file(file: UploadFile = File(...)):
    file_bytes = file.file.read()
    file_hash = hashlib.sha256(file_bytes).hexdigest()
    today = date.today()

    minio = MinioClient()
    minio_path = f"{today.year}/{today.month:02d}/{today.day:02d}/{file.filename}"
    minio.upload_file(file_bytes, minio_path)

    with get_db_session() as session:
        etl_file = EtlFile(
            id=str(uuid.uuid4()),
            file_date=today,
            filename=file.filename,
            hash_sha256=file_hash,
            minio_path=minio_path,
            downloaded_at=datetime.now(timezone.utc),
            is_valid=True,
            is_processed=False,
        )
        session.add(etl_file)
        session.flush()
        return FileOut.model_validate(etl_file)


@router.post("/sync")
def sync_file():
    task = enqueue_task("checker.checker.run_daily", queue="celery")
    return {"task_id": task.id, "status": "QUEUED"}
