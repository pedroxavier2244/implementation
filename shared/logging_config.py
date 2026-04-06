"""
Structured JSON logging para o ETL.

Cada linha de log é um objeto JSON com campos consistentes:
  ts          — ISO 8601 UTC
  level       — DEBUG | INFO | WARNING | ERROR | CRITICAL
  component   — módulo Python (ex: worker.steps.upsert)
  msg         — mensagem legível
  job_id      — ID do ETL job (quando disponível)
  step        — nome do step ETL (extract, clean, enrich, ...)
  event       — identificador de evento curto (ex: upsert_done, step_failed)
  + quaisquer campos extras passados via extra={}

No Loki, filtre por:
  {level="ERROR"}
  {component="worker.steps.upsert"}
  | json | job_id="xxx"
  | json | event="step_failed"
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any


class JSONFormatter(logging.Formatter):
    """Formata cada LogRecord como uma linha JSON."""

    # Campos extras conhecidos que são promovidos para o topo do JSON
    _KNOWN_EXTRA = (
        "job_id", "file_id", "step", "event",
        "rows", "rows_ok", "rows_bad", "duration_ms",
        "cnpj", "data_base", "data_referencia",
    )

    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3] + "Z",
            "level": record.levelname,
            "component": record.name,
            "msg": record.getMessage(),
        }

        # Campos extras conhecidos
        for key in self._KNOWN_EXTRA:
            val = getattr(record, key, None)
            if val is not None:
                data[key] = val

        # Exceção — inclui tipo e traceback
        if record.exc_info and record.exc_info[0] is not None:
            data["error_type"] = record.exc_info[0].__name__
            data["error"] = self.formatException(record.exc_info)

        return json.dumps(data, ensure_ascii=False)


class BoundLogger(logging.LoggerAdapter):
    """LoggerAdapter que injeta campos fixos (job_id, step, etc.) em todo log."""

    def process(self, msg: str, kwargs: dict) -> tuple[str, dict]:
        extra = kwargs.setdefault("extra", {})
        # extra do adapter tem menor prioridade que extra passado na chamada
        merged = {**self.extra, **extra}
        kwargs["extra"] = merged
        return msg, kwargs

    def bind(self, **fields) -> "BoundLogger":
        """Retorna novo BoundLogger com campos adicionais fixos."""
        return BoundLogger(self.logger, {**self.extra, **fields})


def get_logger(name: str, **bound_fields) -> BoundLogger:
    """Retorna um BoundLogger estruturado para o módulo dado."""
    return BoundLogger(logging.getLogger(name), bound_fields)


def setup_logging(level: str = "INFO") -> None:
    """
    Configura o root logger com JSONFormatter para stdout.
    Chame uma vez no startup do worker e da API.
    """
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    handler.setLevel(logging.DEBUG)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Silencia libs barulhentas
    for noisy in (
        "urllib3", "boto3", "botocore", "s3transfer",
        "paramiko", "googleapiclient", "google.auth",
        "httpx", "httpcore",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)
