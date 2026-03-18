from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.visao_cliente_schema import STAGING_TABLE_NAME
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done
from worker.steps.extract import get_cached_dataframe

STAGING_TABLE = STAGING_TABLE_NAME


def run_stage(session: Session, job_id: str) -> None:
    if is_step_done(session, job_id, "stage"):
        return
    begin_step(session, job_id, "stage")

    dataframe = get_cached_dataframe(job_id)
    if dataframe is None:
        raise RuntimeError("No dataframe in cache")

    session.execute(
        text(f"DELETE FROM {STAGING_TABLE} WHERE etl_job_id = :job_id"),
        {"job_id": job_id},
    )

    df_to_insert = dataframe.copy()
    df_to_insert["etl_job_id"] = job_id
    df_to_insert["loaded_at"] = datetime.now(timezone.utc)
    df_to_insert.to_sql(
        STAGING_TABLE,
        con=session.get_bind(),
        schema="etl",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )

    mark_step_done(session, job_id, "stage")
