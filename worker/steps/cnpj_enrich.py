# worker/steps/cnpj_enrich.py
"""
Step de enriquecimento CNPJ.

Após o upsert, para cada CNPJ do arquivo atual:
1. Chama a API interna de CNPJs para obter cnae_fiscal
2. Formata como 'XXXX-X' e consulta CNAE_MAP local
3. Atualiza projetinho_pai: CNAE, RAMO_ATUACAO, FL_PROPENSAO_C6PAY

É best-effort: falhas de rede não abortam o job.
Não modifica FL_PROPENSAO_BOLCOB.
"""
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.cnpj_api import fetch_cnae_code
from shared.cnae_mapping import lookup
from shared.logging_config import get_logger

logger = get_logger(__name__)


def run_cnpj_enrich(session: Session, job_id: str) -> None:
    """Enriquece CNAE, RAMO_ATUACAO e FL_PROPENSAO_C6PAY para CNPJs do arquivo atual."""
    rows = session.execute(
        text("""
            SELECT DISTINCT s.cd_cpf_cnpj_cliente
            FROM etl.staging_visao_cliente s
            WHERE s.job_id = :job_id
              AND s.cd_cpf_cnpj_cliente IS NOT NULL
        """),
        {"job_id": job_id},
    ).fetchall()

    if not rows:
        logger.info(
            "cnpj_enrich: nenhum CNPJ no staging para job %s — pulando",
            job_id,
            extra={"job_id": job_id, "step": "cnpj_enrich", "event": "cnpj_enrich_skip"},
        )
        session.commit()
        return

    logger.info(
        "cnpj_enrich: enriquecendo %d CNPJs do job %s",
        len(rows), job_id,
        extra={"job_id": job_id, "step": "cnpj_enrich", "event": "cnpj_enrich_start"},
    )

    updated = 0
    skipped = 0

    for (cnpj,) in rows:
        digits = "".join(c for c in str(cnpj or "") if c.isdigit())
        if len(digits) != 14:
            skipped += 1
            continue

        cnae_code = fetch_cnae_code(digits)
        if cnae_code is None:
            skipped += 1
            continue

        ramo, c6pay = lookup(cnae_code)
        session.execute(
            text("""
                UPDATE public.projetinho_pai
                SET "CNAE" = :cnae,
                    "RAMO_ATUACAO" = :ramo,
                    "FL_PROPENSAO_C6PAY" = :c6pay
                WHERE "CD_CPF_CNPJ_CLIENTE" = :cnpj
            """),
            {"cnae": cnae_code, "ramo": ramo, "c6pay": c6pay, "cnpj": digits},
        )
        updated += 1

    session.commit()
    logger.info(
        "cnpj_enrich: %d atualizados, %d ignorados (CPF/API sem retorno)",
        updated, skipped,
        extra={
            "job_id": job_id, "step": "cnpj_enrich", "event": "cnpj_enrich_done",
            "updated": updated, "skipped": skipped,
        },
    )
