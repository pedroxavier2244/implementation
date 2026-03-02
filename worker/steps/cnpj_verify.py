import logging
import time
import uuid
from datetime import datetime, timezone, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.brasilapi import fetch_cnpj, compare_fields
from shared.config import get_settings
from shared.models import CnpjRfCache, CnpjDivergencia
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done

logger = logging.getLogger(__name__)

# Campos da BrasilAPI -> colunas rf_* em final_visao_cliente
RF_COLUMN_MAP = {
    "razao_social":      "rf_razao_social",
    "natureza_juridica": "rf_natureza_juridica",
    "capital_social":    "rf_capital_social",
    "porte":             "rf_porte_empresa",
    "nome_fantasia":     "rf_nome_fantasia",
    "situacao_cadastral":"rf_situacao_cadastral",
    "data_inicio_ativ":  "rf_data_inicio_ativ",
    "cnae_fiscal":       "rf_cnae_principal",
    "uf":                "rf_uf",
    "municipio":         "rf_municipio",
    "email":             "rf_email",
}

SLEEP_BETWEEN_REQUESTS = 0.35  # ~3 req/s


def _is_stale(last_checked_at: datetime | None, ttl_days: int) -> bool:
    if last_checked_at is None:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=ttl_days)
    return last_checked_at < cutoff


def _is_valid_cnpj(cnpj) -> bool:
    if not cnpj:
        return False
    digits = str(cnpj).strip()
    return len(digits) == 14 and digits.isdigit()


def _build_divergencias(
    job_id: str, cnpj: str, divergencias_raw: list[dict]
) -> list[CnpjDivergencia]:
    return [
        CnpjDivergencia(
            id=str(uuid.uuid4()),
            job_id=job_id,
            cnpj=cnpj,
            campo=d["campo"],
            valor_c6=d.get("valor_c6"),
            valor_rf=d.get("valor_rf"),
        )
        for d in divergencias_raw
    ]


def _get_cnpjs_for_job(session: Session, job_id: str) -> list[str]:
    """Retorna CNPJs distintos e validos do staging para o job atual."""
    rows = session.execute(
        text(
            "SELECT DISTINCT cd_cpf_cnpj_cliente "
            "FROM staging_visao_cliente "
            "WHERE etl_job_id = :job_id "
            "  AND cd_cpf_cnpj_cliente IS NOT NULL"
        ),
        {"job_id": job_id},
    ).fetchall()
    return [r[0] for r in rows if _is_valid_cnpj(r[0])]


def _get_cache(session: Session, cnpj: str) -> CnpjRfCache | None:
    return session.query(CnpjRfCache).filter_by(cnpj=cnpj).first()


def _get_c6_row(session: Session, cnpj: str) -> dict:
    row = session.execute(
        text(
            "SELECT nome_cliente, uf, cidade, ramo_atuacao "
            "FROM final_visao_cliente "
            "WHERE cd_cpf_cnpj_cliente = :cnpj "
            "LIMIT 1"
        ),
        {"cnpj": cnpj},
    ).fetchone()
    if row is None:
        return {}
    return {
        "nome_cliente": row[0],
        "uf": row[1],
        "cidade": row[2],
        "ramo_atuacao": row[3],
    }


def _update_final_rf_columns(session: Session, cnpj: str, rf_data: dict) -> None:
    set_parts = ", ".join(
        f"{rf_col} = :{rf_col}" for rf_col in RF_COLUMN_MAP.values()
    )
    params = {"cnpj": cnpj}
    for api_field, rf_col in RF_COLUMN_MAP.items():
        params[rf_col] = rf_data.get(api_field)

    session.execute(
        text(
            f"UPDATE final_visao_cliente SET {set_parts} "
            "WHERE cd_cpf_cnpj_cliente = :cnpj"
        ),
        params,
    )


def run_cnpj_verify(session: Session, job_id: str) -> None:
    if is_step_done(session, job_id, "cnpj_verify"):
        return
    begin_step(session, job_id, "cnpj_verify")

    settings = get_settings()
    ttl_days = settings.CNPJ_CACHE_TTL_DAYS
    timeout = settings.BRASILAPI_TIMEOUT

    cnpjs = _get_cnpjs_for_job(session, job_id)
    logger.info("cnpj_verify: %d CNPJs no job %s", len(cnpjs), job_id)

    all_divergencias: list[CnpjDivergencia] = []
    checked = 0

    for cnpj in cnpjs:
        cache = _get_cache(session, cnpj)
        if not _is_stale(getattr(cache, "last_checked_at", None), ttl_days):
            continue  # cache ainda valido

        try:
            rf_data = fetch_cnpj(cnpj, timeout=timeout)
        except Exception as exc:
            logger.warning("cnpj_verify: erro ao buscar %s: %s", cnpj, exc)
            continue

        now = datetime.now(timezone.utc)

        if rf_data is None:
            # CNPJ nao encontrado — registra no cache para nao tentar de novo
            if cache is None:
                cache = CnpjRfCache(cnpj=cnpj)
                session.add(cache)
            cache.situacao_cadastral = "NAO_ENCONTRADO"
            cache.last_checked_at = now
            session.flush()
            checked += 1
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            continue

        # Salvar/atualizar cache
        if cache is None:
            cache = CnpjRfCache(cnpj=cnpj)
            session.add(cache)

        for api_field in RF_COLUMN_MAP:
            setattr(cache, api_field, rf_data.get(api_field))
        cache.cnae_descricao = rf_data.get("cnae_descricao")
        cache.last_checked_at = now
        session.flush()

        # Atualizar colunas rf_* na tabela final
        _update_final_rf_columns(session, cnpj, rf_data)

        # Comparar com dados do C6 Bank
        c6_row = _get_c6_row(session, cnpj)
        raw_divs = compare_fields(c6_row, rf_data)
        if raw_divs:
            records = _build_divergencias(job_id, cnpj, raw_divs)
            all_divergencias.extend(records)
            for rec in records:
                session.add(rec)

        session.flush()
        checked += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    logger.info(
        "cnpj_verify: %d CNPJs verificados, %d divergencias encontradas",
        checked, len(all_divergencias),
    )

    if all_divergencias:
        _send_divergencia_alert(job_id, all_divergencias)

    mark_step_done(session, job_id, "cnpj_verify")


def _send_divergencia_alert(job_id: str, divergencias: list[CnpjDivergencia]) -> None:
    try:
        from shared.celery_dispatch import enqueue_task
        exemplos = [
            f"{d.cnpj}: {d.campo} (C6={d.valor_c6!r} / RF={d.valor_rf!r})"
            for d in divergencias[:5]
        ]
        enqueue_task(
            "notifier.tasks.dispatch_notification",
            kwargs={
                "event_type": "CNPJ_DIVERGENCIA",
                "severity": "WARNING",
                "message": (
                    f"{len(divergencias)} divergencia(s) CNPJ encontrada(s) no job {job_id}"
                ),
                "metadata": {
                    "job_id": job_id,
                    "total_divergencias": len(divergencias),
                    "exemplos": exemplos,
                },
            },
            queue="notification_jobs",
        )
    except Exception as exc:
        logger.warning("cnpj_verify: falha ao enviar alerta: %s", exc)
