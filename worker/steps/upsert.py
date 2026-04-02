import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.config import get_settings
from shared.visao_cliente_schema import (
    FINAL_TABLE_NAME,
    STAGING_TABLE_NAME,
    STAGING_TO_CRM_COLUMN_MAP,
)
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done

logger = logging.getLogger(__name__)

STAGING_TABLE = STAGING_TABLE_NAME
PROJETINHO_PAI = FINAL_TABLE_NAME  # public.projetinho_pai


def run_upsert(session: Session, job_id: str, historico_only: bool = False) -> None:
    if is_step_done(session, job_id, "upsert"):
        return
    begin_step(session, job_id, "upsert")
    session.commit()

    # Colunas do staging (snake_case), excluindo metadados do ETL
    result = session.execute(
        text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = :table_name AND table_schema = 'etl' "
            "ORDER BY ordinal_position"
        ),
        {"table_name": STAGING_TABLE},
    )
    staging_cols = [row[0] for row in result if row[0] not in ("etl_job_id", "loaded_at")]

    if not staging_cols:
        raise RuntimeError(f"No columns found in staging table '{STAGING_TABLE}'")

    # Mapeamento snake_case → UPPERCASE (colunas do projetinho_pai)
    crm_cols = [STAGING_TO_CRM_COLUMN_MAP.get(c, c.upper()) for c in staging_cols]

    # Cria tabela temporária deduplicada: 1 linha por CNPJ, DATA_BASE mais recente
    staging_cols_sql = ", ".join(staging_cols)
    session.execute(text("DROP TABLE IF EXISTS _upsert_source"))
    session.execute(
        text(f"""
            CREATE TEMP TABLE _upsert_source AS
            SELECT {staging_cols_sql}
            FROM (
                SELECT {staging_cols_sql},
                       ROW_NUMBER() OVER (
                           PARTITION BY cd_cpf_cnpj_cliente
                           ORDER BY data_base DESC NULLS LAST
                       ) AS __rn
                FROM etl.{STAGING_TABLE}
                WHERE etl_job_id = :job_id
            ) ranked
            WHERE __rn = 1
        """),
        {"job_id": job_id},
    )
    session.execute(text("CREATE INDEX ON _upsert_source (cd_cpf_cnpj_cliente)"))
    session.commit()

    # Modo historico_only: insere direto em historico sem tocar em projetinho_pai
    if historico_only:
        _insert_historico_only(session, staging_cols, crm_cols, job_id)
        _prune_historico(session)
        mark_step_done(session, job_id, "upsert")
        return

    # Arquiva DATA_BASE atual de projetinho_pai → historico ANTES do upsert
    # (preserva snapshot do estado anterior, igual ao pipeline.js)
    existing = session.execute(
        text(f'SELECT DISTINCT "DATA_BASE" FROM {PROJETINHO_PAI} WHERE "DATA_BASE" IS NOT NULL')
    ).fetchall()
    for (old_db,) in existing:
        logger.info("Arquivando projetinho_pai DATA_BASE=%s → historico", old_db)
        session.execute(
            text("SELECT arquivar_por_data_base(:data_base, :data_ref)"),
            {"data_base": old_db, "data_ref": old_db},
        )
    if existing:
        session.commit()
        logger.info("Arquivamento concluido: %d DATA_BASE(s) → historico", len(existing))

    # UPSERT: INSERT ... ON CONFLICT (CD_CPF_CNPJ_CLIENTE) DO UPDATE
    # Mantém o id original de cada cliente — lead_atribuicoes continua válido
    cols_insert = ", ".join(f'"{c}"' for c in crm_cols)
    cols_select = ", ".join(staging_cols)
    update_set = ", ".join(
        f'"{c}" = EXCLUDED."{c}"'
        for c in crm_cols
        if c != "CD_CPF_CNPJ_CLIENTE"
    )
    result = session.execute(
        text(f"""
            INSERT INTO {PROJETINHO_PAI} ({cols_insert})
            SELECT {cols_select}
            FROM _upsert_source
            WHERE cd_cpf_cnpj_cliente IS NOT NULL
            ON CONFLICT ("CD_CPF_CNPJ_CLIENTE") DO UPDATE SET {update_set}
        """)
    )
    session.commit()
    logger.info("Upsert: %d registros em %s (IDs preservados)", result.rowcount, PROJETINHO_PAI)

    _prune_historico(session)

    mark_step_done(session, job_id, "upsert")


def _insert_historico_only(
    session: Session,
    staging_cols: list[str],
    crm_cols: list[str],
    job_id: str,
) -> None:
    """Insere dados da staging direto em historico, usando DATA_BASE como DATA_REFERENCIA."""
    cols_insert = ", ".join(f'"{c}"' for c in crm_cols) + ', "DATA_REFERENCIA"'
    cols_select = ", ".join(staging_cols) + ", data_base"
    result = session.execute(
        text(f"""
            INSERT INTO historico ({cols_insert})
            SELECT {cols_select}
            FROM _upsert_source
            WHERE cd_cpf_cnpj_cliente IS NOT NULL
        """)
    )
    session.commit()
    logger.info(
        "historico_only: inseridos %d registros em historico (DATA_REFERENCIA=DATA_BASE)",
        result.rowcount,
    )


def _prune_historico(session: Session) -> None:
    """Remove datas mais antigas do historico, mantendo apenas HISTORICO_MAX_DATES datas distintas."""
    max_dates = get_settings().HISTORICO_MAX_DATES
    rows = session.execute(
        text(
            'SELECT DISTINCT "DATA_REFERENCIA" FROM historico '
            'WHERE "DATA_REFERENCIA" IS NOT NULL '
            'ORDER BY "DATA_REFERENCIA" DESC'
        )
    ).fetchall()

    dates = [r[0] for r in rows]
    if len(dates) <= max_dates:
        logger.info("Historico com %d data(s) — dentro do limite de %d.", len(dates), max_dates)
        return

    to_delete = dates[max_dates:]
    logger.info("Podando historico: removendo %d data(s) antiga(s): %s", len(to_delete), to_delete)
    for date in to_delete:
        session.execute(
            text('DELETE FROM historico WHERE "DATA_REFERENCIA" = :d'),
            {"d": date},
        )
    session.commit()
    logger.info("Historico agora tem %d data(s).", max_dates)
