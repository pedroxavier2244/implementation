import io

from sqlalchemy.orm import Session

from shared.minio_client import MinioClient
from shared.visao_cliente_schema import SOURCE_SHEET_NAME
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done

_dataframe_cache: dict[str, object] = {}


def get_cached_dataframe(job_id: str):
    return _dataframe_cache.get(job_id)


def set_cached_dataframe(job_id: str, dataframe) -> None:
    _dataframe_cache[job_id] = dataframe


def clear_cached_dataframe(job_id: str) -> None:
    _dataframe_cache.pop(job_id, None)


def run_extract(session: Session, job_id: str, etl_file) -> None:
    if is_step_done(session, job_id, "extract"):
        return
    begin_step(session, job_id, "extract")

    import pandas as pd

    minio = MinioClient()
    file_bytes = minio.download_file(etl_file.minio_path)
    dataframe = pd.read_excel(io.BytesIO(file_bytes), sheet_name=SOURCE_SHEET_NAME)
    set_cached_dataframe(job_id, dataframe)

    mark_step_done(session, job_id, "extract")
