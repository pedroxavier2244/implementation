import uuid

from sqlalchemy.orm import Session

from shared.config import get_settings
from shared.models import EtlBadRow, EtlJobRun
from shared.visao_cliente_schema import REQUIRED_COLUMNS, normalize_column_name
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done
from worker.steps.extract import get_cached_dataframe


def _missing_required_columns(columns: list[str]) -> list[str]:
    if not REQUIRED_COLUMNS:
        return []
    normalized = {normalize_column_name(c) for c in columns}
    return [col for col in REQUIRED_COLUMNS if col.lower() not in normalized]


def run_validate(session: Session, job_id: str, etl_file) -> None:
    if is_step_done(session, job_id, "validate"):
        return
    begin_step(session, job_id, "validate")

    settings = get_settings()
    dataframe = get_cached_dataframe(job_id)
    if dataframe is None:
        raise RuntimeError("No dataframe in cache, extract must run first")

    missing_columns = _missing_required_columns(list(dataframe.columns))
    if missing_columns:
        raise ValueError(f"Schema validation failed, missing columns: {missing_columns}")

    bad_rows: list[EtlBadRow] = []
    for idx, row in dataframe.iterrows():
        if row.isnull().all():
            bad_rows.append(
                EtlBadRow(
                    id=str(uuid.uuid4()),
                    job_id=job_id,
                    row_number=int(idx),
                    raw_data=row.to_dict(),
                    reason="all_null_row",
                )
            )

    total = len(dataframe)
    bad_count = len(bad_rows)

    for bad in bad_rows:
        session.merge(bad)

    job = session.query(EtlJobRun).filter_by(id=job_id).first()
    if job is not None:
        job.rows_total = total
        job.rows_bad = bad_count
        job.rows_ok = total - bad_count

    session.flush()

    if total > 0 and (bad_count / total * 100) > settings.BAD_ROW_THRESHOLD_PCT:
        raise ValueError(
            f"Bad row threshold exceeded: {bad_count}/{total} "
            f"({(bad_count / total) * 100:.1f}%) > {settings.BAD_ROW_THRESHOLD_PCT}%"
        )

    mark_step_done(session, job_id, "validate")
