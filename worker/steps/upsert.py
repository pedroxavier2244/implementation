from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.visao_cliente_schema import (
    FINAL_TABLE_NAME,
    STAGING_TABLE_NAME,
    UPSERT_CONFLICT_COLUMNS,
    UPSERT_CONFLICT_WHERE,
)
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done


def _numeric_sql_from_text(column_name: str) -> str:
    cleaned = f"regexp_replace(COALESCE({column_name}, ''), '[^0-9,.-]', '', 'g')"
    return (
        "NULLIF("
        "CASE "
        f"WHEN {cleaned} LIKE '%,%' AND {cleaned} LIKE '%.%' "
        f"THEN REPLACE(REPLACE({cleaned}, '.', ''), ',', '.') "
        f"WHEN {cleaned} LIKE '%,%' THEN REPLACE({cleaned}, ',', '.') "
        f"ELSE {cleaned} "
        "END, "
        "''"
        ")::numeric"
    )


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
        {"table_name": STAGING_TABLE_NAME},
    )
    all_columns = [row[0] for row in result if row[0] not in ("etl_job_id", "loaded_at")]

    if not all_columns:
        raise RuntimeError(f"No columns found in staging table '{STAGING_TABLE_NAME}'")

    conflict_columns = [col for col in UPSERT_CONFLICT_COLUMNS if col in all_columns]
    if not conflict_columns:
        raise RuntimeError("No conflict columns found in staging schema for UPSERT")

    update_columns = [col for col in all_columns if col not in conflict_columns]
    set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_columns)
    columns_sql = ", ".join(all_columns)
    conflict_sql = ", ".join(conflict_columns)
    conflict_target = f"({conflict_sql})"
    if UPSERT_CONFLICT_WHERE:
        conflict_target = f"{conflict_target} WHERE {UPSERT_CONFLICT_WHERE}"
    source_select_sql = f"""
        SELECT {columns_sql}
        FROM (
            SELECT
                {columns_sql},
                ROW_NUMBER() OVER (
                    PARTITION BY cd_cpf_cnpj_cliente
                    ORDER BY data_base DESC NULLS LAST
                ) AS __rn
            FROM {STAGING_TABLE_NAME}
            WHERE etl_job_id = :job_id
        ) ranked_source
        WHERE __rn = 1
    """
    incoming_is_newer = (
        f"COALESCE(EXCLUDED.data_base, '') >= COALESCE({FINAL_TABLE_NAME}.data_base, '')"
        if "data_base" in all_columns
        else None
    )

    if update_columns:
        upsert_sql = f"""
            INSERT INTO {FINAL_TABLE_NAME} ({columns_sql})
            {source_select_sql}
            ON CONFLICT {conflict_target} DO UPDATE SET {set_clause}
            {f"WHERE {incoming_is_newer}" if incoming_is_newer else ""}
        """
    else:
        upsert_sql = f"""
            INSERT INTO {FINAL_TABLE_NAME} ({columns_sql})
            {source_select_sql}
            ON CONFLICT {conflict_target} DO NOTHING
        """

    session.execute(text(upsert_sql), {"job_id": job_id})

    if "nivel_cartao" in all_columns and "nivel_conta" in all_columns:
        limite_cartao_num = _numeric_sql_from_text("f.limite_cartao")
        limite_conta_num = _numeric_sql_from_text("f.limite_conta")
        level_backfill_sql = f"""
            UPDATE {FINAL_TABLE_NAME} AS f
            SET
                nivel_cartao = CASE
                    WHEN {limite_cartao_num} IS NULL OR {limite_cartao_num} <= 0 THEN 'Sem Cartao'
                    WHEN {limite_cartao_num} <= 1000 THEN 'Baixo'
                    WHEN {limite_cartao_num} <= 5000 THEN 'Medio'
                    ELSE 'Alto'
                END,
                nivel_conta = CASE
                    WHEN {limite_conta_num} IS NULL OR {limite_conta_num} <= 0 THEN 'Sem Conta'
                    WHEN {limite_conta_num} <= 1000 THEN 'Baixo'
                    WHEN {limite_conta_num} <= 3000 THEN 'Medio'
                    ELSE 'Alto'
                END
            WHERE EXISTS (
                SELECT 1
                FROM {STAGING_TABLE_NAME} AS s
                WHERE s.etl_job_id = :job_id
                  AND s.cd_cpf_cnpj_cliente = f.cd_cpf_cnpj_cliente
            )
        """
        session.execute(text(level_backfill_sql), {"job_id": job_id})

    mark_step_done(session, job_id, "upsert")
