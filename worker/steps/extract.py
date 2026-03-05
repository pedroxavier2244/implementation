import io
import unicodedata

from sqlalchemy.orm import Session

from shared.minio_client import MinioClient
from shared.visao_cliente_schema import SOURCE_SHEET_NAME
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done

_dataframe_cache: dict[str, object] = {}
_workbook_cache: dict[str, dict[str, object]] = {}


def get_cached_dataframe(job_id: str):
    return _dataframe_cache.get(job_id)


def set_cached_dataframe(job_id: str, dataframe) -> None:
    _dataframe_cache[job_id] = dataframe


def get_cached_workbook(job_id: str):
    return _workbook_cache.get(job_id)


def set_cached_workbook(job_id: str, workbook) -> None:
    _workbook_cache[job_id] = workbook


def clear_cached_dataframe(job_id: str) -> None:
    _dataframe_cache.pop(job_id, None)
    _workbook_cache.pop(job_id, None)


def _normalize_name(value: str) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
    return " ".join(text.lower().split())


def _resolve_sheet_name(workbook: dict[str, object], candidates: list[str]) -> str | None:
    normalized_map = {_normalize_name(sheet_name): sheet_name for sheet_name in workbook.keys()}
    for candidate in candidates:
        found = normalized_map.get(_normalize_name(candidate))
        if found:
            return found

    for normalized, original in normalized_map.items():
        if "visao" in normalized and "cliente" in normalized:
            return original
    return None


def run_extract(session: Session, job_id: str, etl_file) -> None:
    if is_step_done(session, job_id, "extract"):
        return
    begin_step(session, job_id, "extract")

    import pandas as pd

    minio = MinioClient()
    file_bytes = minio.download_file(etl_file.minio_path)
    workbook = pd.read_excel(io.BytesIO(file_bytes), sheet_name=None)

    sheet_name = _resolve_sheet_name(
        workbook,
        [
            SOURCE_SHEET_NAME,
            "Visao Cliente",
            "Visao_Cliente",
            "VisaoCliente",
        ],
    )
    if sheet_name is None:
        available = ", ".join(workbook.keys())
        raise ValueError(f"Sheet 'Visao Cliente' not found. Available sheets: {available}")

    dataframe = workbook[sheet_name]
    set_cached_workbook(job_id, workbook)
    set_cached_dataframe(job_id, dataframe)

    mark_step_done(session, job_id, "extract")
