import hashlib
import html
import re
from urllib.parse import urljoin
import uuid
from datetime import date, datetime, timezone

from sqlalchemy.orm import Session

from checker.celery_app import app
from shared.celery_dispatch import enqueue_task
from shared.config import get_settings
from shared.db import get_db_session
from shared.minio_client import MinioClient
from shared.models import EtlFile


def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def is_hash_duplicate(session: Session, hash_sha256: str, file_date: date) -> bool:
    existing = session.query(EtlFile).filter_by(hash_sha256=hash_sha256, file_date=file_date).first()
    return existing is not None


def _build_minio_path(file_date: date, filename: str) -> str:
    return f"{file_date.year}/{file_date.month:02d}/{file_date.day:02d}/{filename}"


def _extract_drive_folder_id(url: str) -> str | None:
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url)
    return match.group(1) if match else None


def _extract_drive_file_id(url: str) -> str | None:
    match = re.search(r"/file/d/([a-zA-Z0-9_-]+)", url)
    if match:
        return match.group(1)
    match = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    return match.group(1) if match else None


def _strip_html(raw_text: str) -> str:
    no_tags = re.sub(r"<[^>]+>", "", raw_text)
    return html.unescape(no_tags).strip()


def _looks_like_html(content: bytes, content_type: str | None = None) -> bool:
    lowered_ct = (content_type or "").lower()
    if "text/html" in lowered_ct or "application/xhtml" in lowered_ct:
        return True
    head = content[:512].lstrip().lower()
    return head.startswith(b"<!doctype html") or head.startswith(b"<html")


def _extract_drive_confirm_token(page_html: str) -> str | None:
    hidden_match = re.search(r'name="confirm"\s+value="([^"]+)"', page_html)
    if hidden_match:
        return hidden_match.group(1)

    query_match = re.search(r"[?&]confirm=([0-9A-Za-z_-]+)", page_html)
    if query_match:
        return query_match.group(1)

    return None


def _extract_drive_download_form(page_html: str) -> tuple[str | None, dict[str, str]]:
    form_match = re.search(r'<form[^>]+id="download-form"[^>]+action="([^"]+)"[^>]*>(.*?)</form>', page_html, flags=re.S)
    if not form_match:
        return None, {}

    action = html.unescape(form_match.group(1))
    form_body = form_match.group(2)
    inputs = re.findall(r'<input[^>]+name="([^"]+)"[^>]+value="([^"]*)"', form_body)
    params = {name: html.unescape(value) for name, value in inputs}
    return action, params


def _download_drive_file_bytes(file_id: str) -> bytes:
    import httpx

    default_url = "https://drive.google.com/uc"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }

    with httpx.Client(timeout=120, follow_redirects=True, headers=headers) as client:
        response = client.get(default_url, params={"export": "download", "id": file_id})
        response.raise_for_status()
        if not _looks_like_html(response.content, response.headers.get("content-type")):
            return response.content

        html_page = response.text
        confirm = _extract_drive_confirm_token(html_page)
        if confirm:
            confirmed = client.get(default_url, params={"export": "download", "id": file_id, "confirm": confirm})
            confirmed.raise_for_status()
            if not _looks_like_html(confirmed.content, confirmed.headers.get("content-type")):
                return confirmed.content

        form_action, form_params = _extract_drive_download_form(html_page)
        if form_action:
            form_url = urljoin("https://drive.google.com", form_action)
            if "id" not in form_params:
                form_params["id"] = file_id
            fallback = client.get(form_url, params=form_params)
            fallback.raise_for_status()
            if not _looks_like_html(fallback.content, fallback.headers.get("content-type")):
                return fallback.content

    raise RuntimeError("Google Drive returned HTML instead of spreadsheet bytes")


def _validate_downloaded_content(file_bytes: bytes, filename: str) -> None:
    lowered = (filename or "").lower()
    if _looks_like_html(file_bytes):
        raise RuntimeError(f"Downloaded content for '{filename}' is HTML, not a spreadsheet")

    # XLSX is a zip payload and should start with PK.
    if lowered.endswith(".xlsx") and not file_bytes.startswith(b"PK"):
        raise RuntimeError(f"Downloaded .xlsx file '{filename}' is not a valid XLSX payload")


def _parse_drive_modified_label(label: str, today: date) -> date:
    cleaned = label.strip()
    for fmt in ("%b %d, %Y", "%b %d %Y", "%b %d"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            if fmt == "%b %d":
                parsed = parsed.replace(year=today.year)
            return parsed.date()
        except ValueError:
            continue
    return date.min


def _download_from_drive_folder(folder_url: str, today: date) -> tuple[bytes, str]:
    import httpx

    folder_id = _extract_drive_folder_id(folder_url)
    if not folder_id:
        raise ValueError("Invalid Google Drive folder URL")

    embedded_url = f"https://drive.google.com/embeddedfolderview?id={folder_id}#list"
    response = httpx.get(embedded_url, timeout=60, follow_redirects=True)
    response.raise_for_status()
    page = response.text

    pattern = (
        r'id="entry-([^"]+)".*?'
        r'flip-entry-title">(.*?)</div>.*?'
        r'flip-entry-last-modified"><div>(.*?)</div>'
    )
    matches = re.findall(pattern, page, flags=re.S)
    if not matches:
        raise RuntimeError("No files found in Google Drive folder page")

    candidates = []
    for file_id, raw_title, raw_modified in matches:
        title = _strip_html(raw_title)
        modified_label = _strip_html(raw_modified)
        lowered = title.lower()
        if not (lowered.endswith(".xlsx") or lowered.endswith(".xls") or lowered.endswith(".csv")):
            continue
        modified_date = _parse_drive_modified_label(modified_label, today)
        candidates.append((modified_date, file_id, title))

    if not candidates:
        raise RuntimeError("No spreadsheet file (.xlsx/.xls/.csv) found in Google Drive folder")

    candidates.sort(key=lambda item: item[0], reverse=True)
    _, latest_file_id, latest_name = candidates[0]

    file_bytes = _download_drive_file_bytes(latest_file_id)
    _validate_downloaded_content(file_bytes, latest_name)
    return file_bytes, latest_name


def _download_from_drive_file(file_url: str) -> tuple[bytes, str]:
    import httpx

    file_id = _extract_drive_file_id(file_url)
    if not file_id:
        raise ValueError("Invalid Google Drive file URL")

    view_url = f"https://drive.google.com/file/d/{file_id}/view"
    filename = f"{file_id}.xlsx"
    try:
        view_response = httpx.get(view_url, timeout=30, follow_redirects=True)
        if view_response.status_code == 200:
            match = re.search(r"<title>(.*?) - Google Drive</title>", view_response.text, flags=re.S)
            if match:
                filename = _strip_html(match.group(1))
    except Exception:
        pass

    file_bytes = _download_drive_file_bytes(file_id)
    _validate_downloaded_content(file_bytes, filename)
    return file_bytes, filename


def _download_source_file(source_url: str, today: date, api_key: str) -> tuple[bytes, str]:
    import httpx

    if "drive.google.com" in source_url:
        if "/folders/" in source_url:
            return _download_from_drive_folder(source_url, today)
        return _download_from_drive_file(source_url)

    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    response = httpx.get(source_url, headers=headers, timeout=60)
    response.raise_for_status()
    filename = f"source_{today.isoformat()}.xlsx"
    _validate_downloaded_content(response.content, filename)
    return response.content, filename


def _send_alert(event_type: str, severity: str, message: str, metadata: dict) -> None:
    enqueue_task(
        "notifier.tasks.dispatch_notification",
        kwargs={
            "event_type": event_type,
            "severity": severity,
            "message": message,
            "metadata": metadata,
        },
        queue="notification_jobs",
    )


def _enqueue_etl_job(file_id: str) -> None:
    enqueue_task(
        "worker.tasks.run_etl",
        kwargs={"job_id": None, "file_id": file_id},
        queue="etl_jobs",
    )


@app.task(name="checker.checker.run_daily", bind=True)
def run_daily(self):
    settings = get_settings()
    today = date.today()

    try:
        file_bytes, filename = _download_source_file(
            source_url=settings.ETL_SOURCE_API_URL,
            today=today,
            api_key=settings.ETL_SOURCE_API_KEY,
        )
    except Exception as exc:
        _send_alert(
            event_type="FILE_MISSING",
            severity="CRITICAL",
            message=f"Failed to download file from API: {exc}",
            metadata={"file_date": today.isoformat()},
        )
        return

    hash_sha256 = compute_sha256(file_bytes)

    with get_db_session() as session:
        if is_hash_duplicate(session, hash_sha256, today):
            _send_alert(
                event_type="HASH_REPEAT",
                severity="WARNING",
                message=f"File for {today} has same hash as previous, skipping",
                metadata={"file_date": today.isoformat(), "hash": hash_sha256},
            )
            return

        minio = MinioClient()
        minio_path = _build_minio_path(today, filename)
        minio.upload_file(file_bytes, minio_path)

        etl_file = EtlFile(
            id=str(uuid.uuid4()),
            file_date=today,
            source_url=settings.ETL_SOURCE_API_URL,
            filename=filename,
            hash_sha256=hash_sha256,
            minio_path=minio_path,
            downloaded_at=datetime.now(timezone.utc),
            is_valid=True,
            is_processed=False,
        )
        session.add(etl_file)
        session.flush()
        file_id = etl_file.id

    _enqueue_etl_job(file_id)
