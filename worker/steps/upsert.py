from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.config import get_settings
from shared.logging_config import get_logger
from shared.visao_cliente_schema import (
    FINAL_TABLE_NAME,
    STAGING_TABLE_NAME,
    STAGING_TO_CRM_COLUMN_MAP,
)
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done

logger = get_logger(__name__)

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

    # Cria backup de projetinho_pai antes de qualquer alteração — permite rollback rápido
    from datetime import date as _date
    backup_table = f"projetinho_pai_backup_{_date.today().strftime('%Y%m%d')}"
    session.execute(text(f"DROP TABLE IF EXISTS public.{backup_table}"))
    session.execute(text(f"CREATE TABLE public.{backup_table} AS SELECT * FROM {PROJETINHO_PAI}"))
    session.commit()
    logger.info("Backup criado: %s", backup_table,
                extra={"job_id": job_id, "step": "upsert", "event": "backup_created",
                       "backup_table": backup_table})

    # Arquiva DATA_BASE atual de projetinho_pai → historico ANTES do upsert
    # (preserva snapshot do estado anterior, igual ao pipeline.js)
    # IMPORTANTE: arquivamos SEM deletar de projetinho_pai para que o upsert
    # possa usar ON CONFLICT e preservar os IDs originais dos leads.
    existing = session.execute(
        text(f'SELECT DISTINCT "DATA_BASE" FROM {PROJETINHO_PAI} WHERE "DATA_BASE" IS NOT NULL')
    ).fetchall()
    if existing:
        # Obtém a lista de colunas de projetinho_pai (excluindo id, igual à função SQL)
        col_rows = session.execute(
            text("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'projetinho_pai'
                  AND column_name != 'id'
                ORDER BY ordinal_position
            """)
        ).fetchall()
        col_list = ", ".join(f'"{r[0]}"' for r in col_rows)

        for (old_db,) in existing:
            logger.info("Arquivando projetinho_pai DATA_BASE=%s → historico", old_db,
                        extra={"job_id": job_id, "step": "upsert", "event": "archive_data_base"})
            # Copia para historico sem deletar (preserva IDs para o upsert a seguir)
            session.execute(
                text(f"""
                    INSERT INTO historico ({col_list}, "DATA_REFERENCIA")
                    SELECT {col_list}, "DATA_BASE"
                    FROM {PROJETINHO_PAI}
                    WHERE "DATA_BASE" = :data_base
                """),
                {"data_base": old_db},
            )
        session.commit()
        logger.info("Arquivamento concluido: %d DATA_BASE(s) → historico", len(existing),
                    extra={"job_id": job_id, "step": "upsert", "event": "archive_done"})

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
    logger.info("Upsert: %d registros em %s (IDs preservados)", result.rowcount, PROJETINHO_PAI, extra={"job_id": job_id, "step": "upsert", "event": "upsert_done"})

    # Remove de projetinho_pai os CNPJs que não estão no arquivo novo.
    # Esses clientes saíram da carteira — seus dados já foram arquivados em historico acima.
    removed = session.execute(
        text(f"""
            DELETE FROM {PROJETINHO_PAI}
            WHERE "CD_CPF_CNPJ_CLIENTE" NOT IN (
                SELECT cd_cpf_cnpj_cliente FROM _upsert_source
                WHERE cd_cpf_cnpj_cliente IS NOT NULL
            )
        """)
    )
    if removed.rowcount:
        logger.info("Removidos %d registros de %s (nao presentes no arquivo novo)",
                    removed.rowcount, PROJETINHO_PAI,
                    extra={"job_id": job_id, "step": "upsert", "event": "removed_stale"})
    session.commit()

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
        extra={"job_id": job_id, "step": "upsert", "event": "upsert_done"},
    )


def _ensure_arquivo_partition(session: Session, data_ref_str: str) -> None:
    """Cria partição mensal em historico_arquivo se ainda não existir."""
    from datetime import datetime

    # DATA_REFERENCIA pode ser DD/MM/YYYY ou YYYY-MM-DD
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
        try:
            d = datetime.strptime(str(data_ref_str).strip()[:10], fmt[:len(fmt.replace("%H:%M:%S","").strip())])
            break
        except ValueError:
            continue
    else:
        logger.warning("DATA_REFERENCIA com formato desconhecido: %s — partição não criada", data_ref_str, extra={"step": "upsert", "event": "partition_format_error"})
        return

    year, month = d.year, d.month
    partition_name = f"historico_arquivo_{year}_{month:02d}"
    from_date = f"{year}-{month:02d}-01"
    to_year, to_month = (year + 1, 1) if month == 12 else (year, month + 1)
    to_date = f"{to_year}-{to_month:02d}-01"

    session.execute(
        text(f"""
            CREATE TABLE IF NOT EXISTS public.{partition_name}
                PARTITION OF public.historico_arquivo
                FOR VALUES FROM ('{from_date}') TO ('{to_date}')
        """)
    )


def _prune_historico(session: Session) -> None:
    """Move datas antigas do historico → historico_arquivo (particionado por mês), mantendo
    apenas HISTORICO_MAX_DATES datas distintas no historico ativo."""
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
        logger.info("Historico com %d data(s) — dentro do limite de %d.", len(dates), max_dates, extra={"step": "upsert", "event": "prune_skip"})
        return

    to_archive = dates[max_dates:]
    logger.info("Arquivando %d data(s) antiga(s) → historico_arquivo: %s", len(to_archive), to_archive, extra={"step": "upsert", "event": "prune_archive"})

    for date in to_archive:
        # Garante que a partição mensal existe
        _ensure_arquivo_partition(session, date)

        # Move para historico_arquivo — data_ref_date calculada no Python (evita
        # DatetimeFieldOverflow quando DATA_REFERENCIA está em formato YYYY-MM-DD ou timestamp)
        from datetime import datetime as _dt
        data_ref_date = None
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"):
            try:
                data_ref_date = _dt.strptime(str(date).strip()[:19], fmt).date()
                break
            except ValueError:
                continue

        result = session.execute(
            text("""
                INSERT INTO public.historico_arquivo
                SELECT *, :data_ref_date AS data_ref_date
                FROM public.historico
                WHERE "DATA_REFERENCIA" = :d
                ON CONFLICT DO NOTHING
            """),
            {"d": date, "data_ref_date": data_ref_date},
        )
        archived = result.rowcount

        # Remove do historico ativo
        session.execute(
            text('DELETE FROM historico WHERE "DATA_REFERENCIA" = :d'),
            {"d": date},
        )
        logger.info("DATA_REFERENCIA=%s: %d linha(s) movidas para historico_arquivo", date, archived, extra={"step": "upsert", "event": "prune_moved"})

    session.commit()
    logger.info("Historico ativo agora tem %d data(s). Arquivo total preservado.", max_dates, extra={"step": "upsert", "event": "prune_done"})
