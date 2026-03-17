"""
Backfill da tabela visao_cliente_change_history a partir da staging_visao_cliente.

Uso:
    python scripts/backfill_change_history.py --dry-run
    python scripts/backfill_change_history.py
    python scripts/backfill_change_history.py --truncate-first

Regras:
- Processa apenas jobs com step "upsert" marcado como DONE.
- Replica a regra do upsert online: um snapshot so altera o estado quando
  data_base >= data_base atualmente aplicada para o documento.
- Gera um evento INSERT na primeira aparicao do documento.
- Gera eventos UPDATE por campo alterado nas cargas subsequentes.

Observacao:
- Por seguranca, o script aborta se a tabela de historico nao estiver vazia,
  a menos que voce use --truncate-first.
"""
from __future__ import annotations

import argparse
import time
from dataclasses import dataclass

from sqlalchemy import text

from shared.db import get_db_session
from shared.visao_cliente_schema import STAGING_TABLE_NAME, UPSERT_CONFLICT_COLUMNS

HISTORY_TABLE = "visao_cliente_change_history"
TEMP_STATE_TABLE = "tmp_visao_cliente_backfill_state"
TEMP_SOURCE_TABLE = "tmp_visao_cliente_backfill_source"


@dataclass
class JobRow:
    job_id: str
    file_id: str | None
    filename: str | None
    file_date: str | None
    sort_ts: str | None


def _jsonb_payload_sql(alias: str, columns: list[str]) -> str:
    if not columns:
        return "'{}'::jsonb"

    chunks: list[str] = []
    # PostgreSQL aceita no maximo 100 argumentos por funcao.
    # Cada par chave/valor consome 2 argumentos em jsonb_build_object.
    for index in range(0, len(columns), 40):
        payload_parts: list[str] = []
        for column in columns[index:index + 40]:
            payload_parts.append(f"'{column}'")
            payload_parts.append(f"{alias}.{column}")
        chunks.append(f"jsonb_build_object({', '.join(payload_parts)})")
    return " || ".join(chunks)


def _load_staging_columns(session) -> list[str]:
    rows = session.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :table_name
            ORDER BY ordinal_position
            """
        ),
        {"table_name": STAGING_TABLE_NAME},
    ).scalars().all()
    return [column for column in rows if column not in ("etl_job_id", "loaded_at")]


def _load_jobs(session) -> list[JobRow]:
    rows = session.execute(
        text(
            f"""
            SELECT
                j.id AS job_id,
                j.file_id,
                f.filename,
                f.file_date::text AS file_date,
                COALESCE(j.started_at, s.finished_at, s.started_at, j.finished_at)::text AS sort_ts
            FROM etl_job_run AS j
            JOIN etl_job_step AS s
              ON s.job_id = j.id
             AND s.step_name = 'upsert'
             AND s.status = 'DONE'
            LEFT JOIN etl_file AS f
              ON f.id = j.file_id
            WHERE EXISTS (
                SELECT 1
                FROM {STAGING_TABLE_NAME} AS st
                WHERE st.etl_job_id = j.id
            )
            ORDER BY
                COALESCE(j.started_at, s.finished_at, s.started_at, j.finished_at) ASC NULLS LAST,
                j.id ASC
            """
        )
    ).mappings().all()
    return [
        JobRow(
            job_id=row["job_id"],
            file_id=row["file_id"],
            filename=row["filename"],
            file_date=row["file_date"],
            sort_ts=row["sort_ts"],
        )
        for row in rows
    ]


def _history_table_exists(session) -> bool:
    return session.execute(
        text("SELECT to_regclass(:table_name)"),
        {"table_name": HISTORY_TABLE},
    ).scalar_one() is not None


def _history_count(session) -> int:
    return int(session.execute(text(f"SELECT COUNT(*) FROM {HISTORY_TABLE}")).scalar_one())


def _prepare_history_table(session, truncate_first: bool) -> None:
    existing_rows = _history_count(session)
    if existing_rows == 0:
        return

    if not truncate_first:
        raise RuntimeError(
            f"A tabela {HISTORY_TABLE} ja possui {existing_rows} linhas. "
            "Use --truncate-first para recriar o historico do zero."
        )

    session.execute(text(f"TRUNCATE TABLE {HISTORY_TABLE} RESTART IDENTITY"))
    session.commit()


def _create_temp_state_table(session, columns_sql: str) -> None:
    session.execute(text(f"DROP TABLE IF EXISTS {TEMP_STATE_TABLE}"))
    session.execute(
        text(
            f"""
            CREATE TEMP TABLE {TEMP_STATE_TABLE} AS
            SELECT {columns_sql}
            FROM {STAGING_TABLE_NAME}
            WHERE 1 = 0
            """
        )
    )
    session.execute(
        text(
            f"""
            ALTER TABLE {TEMP_STATE_TABLE}
            ADD PRIMARY KEY (cd_cpf_cnpj_cliente)
            """
        )
    )


def _prepare_source_table(session, job_id: str, columns_sql: str) -> int:
    session.execute(text(f"DROP TABLE IF EXISTS {TEMP_SOURCE_TABLE}"))
    session.execute(
        text(
            f"""
            CREATE TEMP TABLE {TEMP_SOURCE_TABLE} AS
            SELECT {columns_sql}, loaded_at
            FROM (
                SELECT
                    {columns_sql},
                    loaded_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY cd_cpf_cnpj_cliente
                        ORDER BY data_base DESC NULLS LAST, loaded_at DESC NULLS LAST
                    ) AS __rn
                FROM {STAGING_TABLE_NAME}
                WHERE etl_job_id = :job_id
            ) AS ranked
            WHERE __rn = 1
              AND cd_cpf_cnpj_cliente IS NOT NULL
            """
        ),
        {"job_id": job_id},
    )
    session.execute(
        text(
            f"""
            CREATE INDEX idx_{TEMP_SOURCE_TABLE}_documento
            ON {TEMP_SOURCE_TABLE} (cd_cpf_cnpj_cliente)
            """
        )
    )
    session.execute(text(f"ANALYZE {TEMP_SOURCE_TABLE}"))
    return int(session.execute(text(f"SELECT COUNT(*) FROM {TEMP_SOURCE_TABLE}")).scalar_one())


def _insert_new_document_events(session, job: JobRow) -> int:
    result = session.execute(
        text(
            f"""
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
                :file_id,
                s.data_base,
                'INSERT',
                NULL,
                NULL,
                NULL,
                s.loaded_at
            FROM {TEMP_SOURCE_TABLE} AS s
            LEFT JOIN {TEMP_STATE_TABLE} AS st
              ON st.cd_cpf_cnpj_cliente = s.cd_cpf_cnpj_cliente
            WHERE st.cd_cpf_cnpj_cliente IS NULL
            """
        ),
        {"job_id": job.job_id, "file_id": job.file_id},
    )
    return int(result.rowcount or 0)


def _insert_updated_field_events(session, job: JobRow, history_columns: list[str]) -> int:
    if not history_columns:
        return 0

    source_payload_sql = _jsonb_payload_sql("s", history_columns)
    state_payload_sql = _jsonb_payload_sql("st", history_columns)

    result = session.execute(
        text(
            f"""
            WITH candidate_rows AS (
                SELECT
                    s.cd_cpf_cnpj_cliente AS documento,
                    s.data_base AS data_base,
                    s.loaded_at AS changed_at,
                    :job_id AS etl_job_id,
                    :file_id AS file_id,
                    {source_payload_sql} AS source_payload,
                    {state_payload_sql} AS state_payload
                FROM {TEMP_SOURCE_TABLE} AS s
                JOIN {TEMP_STATE_TABLE} AS st
                  ON st.cd_cpf_cnpj_cliente = s.cd_cpf_cnpj_cliente
                WHERE COALESCE(s.data_base, '') >= COALESCE(st.data_base, '')
            ),
            candidate_updates AS (
                SELECT *
                FROM candidate_rows
                WHERE source_payload IS DISTINCT FROM state_payload
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
                state_payload ->> diff.key,
                source_payload ->> diff.key,
                changed_at
            FROM candidate_updates
            CROSS JOIN LATERAL jsonb_object_keys(source_payload || state_payload) AS diff(key)
            WHERE (state_payload ->> diff.key) IS DISTINCT FROM (source_payload ->> diff.key)
            """
        ),
        {"job_id": job.job_id, "file_id": job.file_id},
    )
    return int(result.rowcount or 0)


def _upsert_temp_state(session, columns_sql: str, update_columns: list[str]) -> None:
    set_clause = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
    session.execute(
        text(
            f"""
            INSERT INTO {TEMP_STATE_TABLE} ({columns_sql})
            SELECT {columns_sql}
            FROM {TEMP_SOURCE_TABLE}
            ON CONFLICT (cd_cpf_cnpj_cliente) DO UPDATE
            SET {set_clause}
            WHERE COALESCE(EXCLUDED.data_base, '') >= COALESCE({TEMP_STATE_TABLE}.data_base, '')
            """
        )
    )


def _delete_job_history(session, job_id: str) -> None:
    session.execute(
        text(f"DELETE FROM {HISTORY_TABLE} WHERE etl_job_id = :job_id"),
        {"job_id": job_id},
    )


def run_backfill(truncate_first: bool, dry_run: bool) -> None:
    with get_db_session() as session:
        if not _history_table_exists(session):
            raise RuntimeError(
                f"A tabela {HISTORY_TABLE} nao existe. Aplique a migration do ETL antes do backfill."
            )

        all_columns = _load_staging_columns(session)
        if not all_columns:
            raise RuntimeError("Nenhuma coluna encontrada em staging_visao_cliente.")

        conflict_columns = [column for column in UPSERT_CONFLICT_COLUMNS if column in all_columns]
        if not conflict_columns:
            raise RuntimeError("Nao foi possivel identificar a coluna de conflito do documento.")

        update_columns = [column for column in all_columns if column not in conflict_columns]
        history_columns = [column for column in update_columns if column != "data_base"]
        columns_sql = ", ".join(all_columns)

        jobs = _load_jobs(session)
        if not jobs:
            print("Nenhum job elegivel para backfill foi encontrado.")
            return

        print(
            f"Jobs elegiveis para backfill: {len(jobs)} | "
            f"colunas comparadas por UPDATE: {len(history_columns)}"
        )
        if dry_run:
            for index, job in enumerate(jobs, start=1):
                print(
                    f"[{index:02d}/{len(jobs):02d}] job_id={job.job_id} "
                    f"file_date={job.file_date} filename={job.filename!r} sort_ts={job.sort_ts}"
                )
            return

        _prepare_history_table(session, truncate_first=truncate_first)
        _create_temp_state_table(session, columns_sql=columns_sql)
        session.commit()

        started = time.perf_counter()
        total_insert_events = 0
        total_update_events = 0

        for index, job in enumerate(jobs, start=1):
            job_started = time.perf_counter()
            _delete_job_history(session, job.job_id)
            source_rows = _prepare_source_table(session, job_id=job.job_id, columns_sql=columns_sql)

            if source_rows == 0:
                session.commit()
                print(f"[{index:02d}/{len(jobs):02d}] job_id={job.job_id} sem snapshots elegiveis.")
                continue

            insert_events = _insert_new_document_events(session, job)
            update_events = _insert_updated_field_events(session, job, history_columns=history_columns)
            _upsert_temp_state(session, columns_sql=columns_sql, update_columns=update_columns)
            session.commit()

            total_insert_events += insert_events
            total_update_events += update_events
            elapsed = time.perf_counter() - job_started
            print(
                f"[{index:02d}/{len(jobs):02d}] job_id={job.job_id} "
                f"file_date={job.file_date} rows={source_rows} "
                f"inserts={insert_events} updates={update_events} "
                f"elapsed={elapsed:.1f}s"
            )

        total_elapsed = time.perf_counter() - started
        final_rows = _history_count(session)
        print(
            f"Backfill concluido em {total_elapsed:.1f}s | "
            f"eventos INSERT={total_insert_events} | "
            f"eventos UPDATE={total_update_events} | "
            f"linhas finais={final_rows}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill do historico persistido da visao cliente.")
    parser.add_argument(
        "--truncate-first",
        action="store_true",
        help="apaga o historico atual antes de reconstruir do zero",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="lista os jobs elegiveis sem gravar nada",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_backfill(truncate_first=args.truncate_first, dry_run=args.dry_run)
