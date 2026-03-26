from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.visao_cliente_schema import STAGING_TABLE_NAME
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done
from worker.steps.extract import get_cached_dataframe

STAGING_TABLE = STAGING_TABLE_NAME


_STAGE_BATCH_SIZE = 5_000


def run_stage(session: Session, job_id: str) -> None:
    if is_step_done(session, job_id, "stage"):
        return
    begin_step(session, job_id, "stage")
    session.commit()

    dataframe = get_cached_dataframe(job_id)
    if dataframe is None:
        raise RuntimeError("No dataframe in cache")

    session.execute(
        text(f"DELETE FROM etl.{STAGING_TABLE} WHERE etl_job_id = :job_id"),
        {"job_id": job_id},
    )
    session.commit()

    df_to_insert = dataframe.copy()
    df_to_insert["etl_job_id"] = job_id
    df_to_insert["loaded_at"] = datetime.now(timezone.utc)

    # Insere em lotes com commit a cada lote — evita transações longas que
    # o Neon encerra por timeout de SSL em arquivos grandes.
    engine = session.get_bind()
    with engine.connect() as conn:
        for start in range(0, len(df_to_insert), _STAGE_BATCH_SIZE):
            chunk = df_to_insert.iloc[start:start + _STAGE_BATCH_SIZE]
            chunk.to_sql(
                STAGING_TABLE,
                con=conn,
                schema="etl",
                if_exists="append",
                index=False,
                method="multi",
            )
            conn.commit()

    mark_step_done(session, job_id, "stage")
