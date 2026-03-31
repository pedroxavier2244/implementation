import hashlib
import io
import json
import logging
import re
import uuid
from datetime import date, datetime, timezone

from shared.config import get_settings
from shared.db import get_db_session
from shared.minio_client import MinioClient
from shared.models import EtlFile
from worker.celery_app import app

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def _get_drive_service():
    """Build Google Drive API service using service account credentials."""
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError as e:
        raise RuntimeError(
            "Google API packages not installed. Add google-api-python-client and "
            "google-auth to requirements/worker.txt"
        ) from e

    settings = get_settings()
    creds_value = settings.GOOGLE_SERVICE_ACCOUNT_JSON
    if not creds_value:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON not configured in .env")

    if creds_value.strip().startswith("{"):
        # JSON content passed directly
        info = json.loads(creds_value)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        # File path passed
        creds = service_account.Credentials.from_service_account_file(creds_value, scopes=SCOPES)

    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _parse_date_from_filename(filename: str) -> date:
    """Extract date from filename like 'Relatório de Produção - 28.03.26.xlsx'"""
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{2})", filename)
    if m:
        day, month, year_short = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(2000 + year_short, month, day)
        except ValueError:
            pass
    return date.today()


@app.task(name="checker.checker.run_daily", bind=True, queue="etl_jobs")
def run_daily(self):
    """
    Lists .xlsx files in the configured Google Drive folder,
    downloads any that haven't been processed yet, saves them to MinIO,
    and triggers an ETL job for each.
    """
    settings = get_settings()
    folder_id = settings.GOOGLE_DRIVE_FOLDER_ID

    if not folder_id:
        logger.warning("GOOGLE_DRIVE_FOLDER_ID not configured — skipping Drive sync")
        return {"status": "skipped", "reason": "GOOGLE_DRIVE_FOLDER_ID not set"}

    if not settings.GOOGLE_SERVICE_ACCOUNT_JSON:
        logger.warning("GOOGLE_SERVICE_ACCOUNT_JSON not configured — skipping Drive sync")
        return {"status": "skipped", "reason": "GOOGLE_SERVICE_ACCOUNT_JSON not set"}

    logger.info("Starting Drive sync for folder %s", folder_id)
    service = _get_drive_service()

    # List all .xlsx files in the folder (not trashed)
    query = (
        f"'{folder_id}' in parents "
        "and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' "
        "and trashed=false"
    )
    results = (
        service.files()
        .list(
            q=query,
            fields="files(id, name, md5Checksum, modifiedTime, size)",
            orderBy="modifiedTime asc",
            pageSize=100,
        )
        .execute()
    )

    drive_files = results.get("files", [])
    logger.info("Found %d .xlsx file(s) in Drive folder", len(drive_files))

    imported = 0
    skipped = 0
    errors = 0

    for drive_file in drive_files:
        gdrive_id = drive_file["id"]
        filename = drive_file["name"]
        source_url = f"gdrive:{gdrive_id}"

        try:
            # Check if already imported by Drive file ID
            with get_db_session() as session:
                existing = (
                    session.query(EtlFile)
                    .filter(EtlFile.source_url == source_url)
                    .first()
                )
                if existing:
                    logger.debug("Already imported (Drive ID match): %s", filename)
                    skipped += 1
                    continue

            # Download from Drive
            logger.info("Downloading from Drive: %s (%s)", filename, gdrive_id)
            try:
                from googleapiclient.http import MediaIoBaseDownload
            except ImportError as e:
                raise RuntimeError("googleapiclient not installed") from e

            request = service.files().get_media(fileId=gdrive_id)
            buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(buffer, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

            file_bytes = buffer.getvalue()
            sha256 = hashlib.sha256(file_bytes).hexdigest()
            logger.info("Downloaded %d bytes (sha256=%s...)", len(file_bytes), sha256[:8])

            # Secondary dedup: same content hash already in DB
            with get_db_session() as session:
                existing_hash = (
                    session.query(EtlFile)
                    .filter(EtlFile.hash_sha256 == sha256)
                    .first()
                )
                if existing_hash:
                    logger.info(
                        "Already imported (hash match): %s -> existing file %s",
                        filename,
                        existing_hash.id,
                    )
                    skipped += 1
                    continue

            # Upload to MinIO
            file_date = _parse_date_from_filename(filename)
            minio_path = (
                f"{file_date.year}/{file_date.month:02d}/{file_date.day:02d}/{filename}"
            )
            minio = MinioClient()
            minio.upload_file(file_bytes, minio_path)
            logger.info("Uploaded to MinIO: %s", minio_path)

            # Create EtlFile record and trigger ETL job
            with get_db_session() as session:
                etl_file = EtlFile(
                    id=str(uuid.uuid4()),
                    file_date=file_date,
                    source_url=source_url,
                    filename=filename,
                    hash_sha256=sha256,
                    minio_path=minio_path,
                    downloaded_at=datetime.now(timezone.utc),
                    is_valid=True,
                    is_processed=False,
                )
                session.add(etl_file)
                session.flush()
                file_db_id = etl_file.id

            from worker.tasks import run_etl

            run_etl.apply_async(
                kwargs={"job_id": None, "file_id": file_db_id},
                queue="etl_jobs",
            )
            logger.info("ETL job queued for: %s (file_id=%s)", filename, file_db_id)
            imported += 1

        except Exception as exc:
            logger.exception("Error processing Drive file %s: %s", filename, exc)
            errors += 1

    result = {"status": "done", "imported": imported, "skipped": skipped, "errors": errors}
    logger.info("Drive sync complete: %s", result)
    return result
