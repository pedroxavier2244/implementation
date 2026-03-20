from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.visao_cliente_schema import (
    FINAL_TABLE_NAME,
    STAGING_TABLE_NAME,
    UPSERT_CONFLICT_COLUMNS,
    UPSERT_CONFLICT_WHERE,
)
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done

STAGING_TABLE = STAGING_TABLE_NAME
FINAL_TABLE = FINAL_TABLE_NAME
HISTORY_TABLE = "visao_cliente_change_history"
CONFLICT_COLUMNS = UPSERT_CONFLICT_COLUMNS
CONFLICT_WHERE = UPSERT_CONFLICT_WHERE


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


def _jsonb_payload_sql(alias: str, columns: list[str]) -> str:
    if not columns:
        return "'{}'::jsonb"

    chunks: list[str] = []
    # PostgreSQL limita chamadas de funcao a 100 argumentos.
    # Cada coluna consome 2 argumentos no jsonb_build_object.
    for index in range(0, len(columns), 40):
        payload_parts: list[str] = []
        for column in columns[index:index + 40]:
            payload_parts.append(f"'{column}'")
            payload_parts.append(f"{alias}.{column}")
        chunks.append(f"jsonb_build_object({', '.join(payload_parts)})")
    return " || ".join(chunks)


def _insert_change_history(
    session: Session,
    job_id: str,
    source_select_sql: str,
    history_columns: list[str],
    source_is_newer_sql: str | None,
) -> None:
    insert_new_rows_sql = f"""
        INSERT INTO {HISTORY_TABLE} (
            documento,
            etl_job_id,
            file_id,
            data_base,
            change_type,
            field_name,
            old_value,
            new_value,
            changed_at
        )
        SELECT
            s.cd_cpf_cnpj_cliente,
            :job_id,
            j.file_id,
            s.data_base,
            'INSERT',
            NULL,
            NULL,
            NULL,
            CURRENT_TIMESTAMP
        FROM ({source_select_sql}) AS s
        JOIN etl_job_run AS j
          ON j.id = :job_id
        LEFT JOIN {FINAL_TABLE} AS f
          ON f.cd_cpf_cnpj_cliente = s.cd_cpf_cnpj_cliente
        WHERE s.cd_cpf_cnpj_cliente IS NOT NULL
          AND f.cd_cpf_cnpj_cliente IS NULL
    """
    session.execute(text(insert_new_rows_sql), {"job_id": job_id})

    if not history_columns:
        return

    source_payload_sql = _jsonb_payload_sql("s", history_columns)
    final_payload_sql = _jsonb_payload_sql("f", history_columns)
    newer_condition = source_is_newer_sql or "TRUE"

    insert_updated_fields_sql = f"""
        WITH changed_rows AS (
            SELECT
                s.cd_cpf_cnpj_cliente AS documento,
                :job_id AS etl_job_id,
                j.file_id AS file_id,
                s.data_base AS data_base,
                {source_payload_sql} AS source_payload,
                {final_payload_sql} AS final_payload
            FROM ({source_select_sql}) AS s
            JOIN etl_job_run AS j
              ON j.id = :job_id
            JOIN {FINAL_TABLE} AS f
              ON f.cd_cpf_cnpj_cliente = s.cd_cpf_cnpj_cliente
            WHERE s.cd_cpf_cnpj_cliente IS NOT NULL
              AND {newer_condition}
        )
        INSERT INTO {HISTORY_TABLE} (
            documento,
            etl_job_id,
            file_id,
            data_base,
            change_type,
            field_name,
            old_value,
            new_value,
            changed_at
        )
        SELECT
            documento,
            etl_job_id,
            file_id,
            data_base,
            'UPDATE',
            diff.key,
            final_payload ->> diff.key,
            source_payload ->> diff.key,
            CURRENT_TIMESTAMP
        FROM changed_rows
        CROSS JOIN LATERAL jsonb_object_keys(source_payload || final_payload) AS diff(key)
        WHERE (final_payload ->> diff.key) IS DISTINCT FROM (source_payload ->> diff.key)
    """
    session.execute(text(insert_updated_fields_sql), {"job_id": job_id})


def run_upsert(session: Session, job_id: str) -> None:
    if is_step_done(session, job_id, "upsert"):
        return
    begin_step(session, job_id, "upsert")

    result = session.execute(
        text(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_name = :table_name "
            "  AND table_schema = 'etl' "
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
    conflict_target = f"({conflict_sql})"
    if CONFLICT_WHERE:
        conflict_target = f"{conflict_target} WHERE {CONFLICT_WHERE}"

    # Materializa a source deduplicada em tabela temporária.
    # ROW_NUMBER() é calculado uma única vez e indexado — evita recomputar
    # a window function em cada uma das 3 queries subsequentes.
    session.execute(
        text(f"""
            CREATE TEMP TABLE _upsert_source AS
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
        """),
        {"job_id": job_id},
    )
    session.execute(text("CREATE INDEX ON _upsert_source (cd_cpf_cnpj_cliente)"))

    source_select_sql = "SELECT * FROM _upsert_source"

    incoming_is_newer = (
        f"COALESCE(EXCLUDED.data_base, '') >= COALESCE({FINAL_TABLE}.data_base, '')"
        if "data_base" in all_columns
        else None
    )
    source_is_newer = (
        "COALESCE(s.data_base, '') >= COALESCE(f.data_base, '')"
        if "data_base" in all_columns
        else None
    )
    history_columns = [col for col in update_columns if col != "data_base"]

    _insert_change_history(
        session,
        job_id=job_id,
        source_select_sql=source_select_sql,
        history_columns=history_columns,
        source_is_newer_sql=source_is_newer,
    )

    if update_columns:
        upsert_sql = f"""
            INSERT INTO {FINAL_TABLE} ({columns_sql})
            SELECT {columns_sql} FROM _upsert_source
            ON CONFLICT {conflict_target} DO UPDATE SET {set_clause}
            {f"WHERE {incoming_is_newer}" if incoming_is_newer else ""}
        """
    else:
        upsert_sql = f"""
            INSERT INTO {FINAL_TABLE} ({columns_sql})
            SELECT {columns_sql} FROM _upsert_source
            ON CONFLICT {conflict_target} DO NOTHING
        """

    session.execute(text(upsert_sql))

    mark_step_done(session, job_id, "upsert")
