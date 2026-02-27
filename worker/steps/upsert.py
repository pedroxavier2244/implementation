from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.visao_cliente_schema import FINAL_TABLE_NAME, STAGING_TABLE_NAME, UPSERT_CONFLICT_COLUMNS
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done

STAGING_TABLE = STAGING_TABLE_NAME
FINAL_TABLE = FINAL_TABLE_NAME
CONFLICT_COLUMNS = UPSERT_CONFLICT_COLUMNS


def run_upsert(session: Session, job_id: str) -> None:
    if is_step_done(session, job_id, "upsert"):
        return
    begin_step(session, job_id, "upsert")

    result = session.execute(
        text(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_name = :table_name "
            "ORDER BY ordinal_position"
        ),
        {"table_name": STAGING_TABLE},
    )
    all_columns = [row[0] for row in result if row[0] not in ("etl_job_id", "loaded_at")]

    if not all_columns:
        raise RuntimeError(f"No columns found in staging table '{STAGING_TABLE}'")

    conflict_columns = [col for col in CONFLICT_COLUMNS if col in all_columns]
    if not conflict_columns:
        raise RuntimeError("No conflict columns found in staging schema for UPSERT")

    update_columns = [col for col in all_columns if col not in conflict_columns]
    set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
    columns_sql = ", ".join(all_columns)
    conflict_sql = ", ".join(conflict_columns)
    source_select_sql = f"""
        SELECT {columns_sql}
        FROM (
            SELECT
                {columns_sql},
                ROW_NUMBER() OVER (
                    PARTITION BY cd_cpf_cnpj_cliente
                    ORDER BY data_base DESC NULLS LAST
                ) AS __rn
            FROM {STAGING_TABLE}
            WHERE etl_job_id = :job_id
        ) ranked_source
        WHERE __rn = 1
    """
    incoming_is_newer = (
        f"COALESCE(EXCLUDED.data_base, '') >= COALESCE({FINAL_TABLE}.data_base, '')"
        if "data_base" in all_columns
        else None
    )

    if update_columns:
        upsert_sql = f"""
            INSERT INTO {FINAL_TABLE} ({columns_sql})
            {source_select_sql}
            ON CONFLICT ({conflict_sql}) DO UPDATE SET {set_clause}
            {f"WHERE {incoming_is_newer}" if incoming_is_newer else ""}
        """
    else:
        upsert_sql = f"""
            INSERT INTO {FINAL_TABLE} ({columns_sql})
            {source_select_sql}
            ON CONFLICT ({conflict_sql}) DO NOTHING
        """

    session.execute(text(upsert_sql), {"job_id": job_id})
    mark_step_done(session, job_id, "upsert")
