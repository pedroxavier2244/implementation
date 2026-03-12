"""Task Celery assíncrona para verificação de CNPJ.

F01 — QA fix: cnpj_verify foi extraído do pipeline principal (run_etl)
para esta task separada. O job ETL agora finaliza em ~5-8s sem aguardar
a chamada sequencial à BrasilAPI (que pode levar >105s para 300 CNPJs).

Fluxo:
    run_etl → (dispara) → run_cnpj_verify_async
                                ↓
                          Executa em background, atualiza final_visao_cliente
                          e registra divergências independentemente do job principal.
"""

import logging

from shared.db import get_db_session
from worker.celery_app import app
from worker.steps.cnpj_verify import run_cnpj_verify

logger = logging.getLogger(__name__)


@app.task(
    name="worker.tasks_cnpj.run_cnpj_verify_async",
    bind=True,
    queue="etl_jobs",
    max_retries=2,
    default_retry_delay=120,  # 2 min entre retries (BrasilAPI pode estar congestionada)
)
def run_cnpj_verify_async(self, job_id: str) -> None:
    """Executa a verificação de CNPJ de forma assíncrona, fora do pipeline principal."""
    logger.info("cnpj_verify_async: iniciando para job_id=%s", job_id)
    try:
        with get_db_session() as session:
            run_cnpj_verify(session, job_id)
        logger.info("cnpj_verify_async: concluído para job_id=%s", job_id)
    except Exception as exc:
        logger.warning(
            "cnpj_verify_async: erro no job %s — %s. Tentativa %d/%d",
            job_id, exc, self.request.retries + 1, self.max_retries + 1,
        )
        raise self.retry(exc=exc)
