import re

import pandas as pd

from sqlalchemy.orm import Session

from shared.logging_config import get_logger
from shared.visao_cliente_schema import normalize_column_name
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done
from worker.steps.extract import get_cached_dataframe, set_cached_dataframe

logger = get_logger(__name__)


def _normalize_document(value) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return None

    # Common Excel artifact for integer-like numeric cells.
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]

    digits = re.sub(r"[^0-9]", "", text)
    return digits or None


def _normalize_data_base(value):
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return None

    parsed = pd.to_datetime(value, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return text
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def run_clean(session: Session, job_id: str) -> None:
    if is_step_done(session, job_id, "clean"):
        return
    begin_step(session, job_id, "clean")
    logger.info("Starting clean", extra={"job_id": job_id, "step": "clean", "event": "clean_start"})

    dataframe = get_cached_dataframe(job_id)
    if dataframe is None:
        raise RuntimeError("No dataframe in cache")

    # F06: inclui string vazia "" para garantir que células em branco virem NULL
    _NULL_STRINGS = {"", "nan", "none", "nat", "None", "NaT", "NULL", "null", "<NA>"}
    _NULL_LOWER = {v.lower() for v in _NULL_STRINGS}
    # F07: artefato do Excel — inteiros lidos como float viram "1.0", "0.0" etc.
    #      Normaliza "N.0" → "N" para qualquer coluna de texto.
    _int_float_re = re.compile(r"^(-?\d+)\.0+$")
    for col in dataframe.select_dtypes(include="object").columns:
        dataframe[col] = (
            dataframe[col]
            .astype(str)
            .str.strip()
            .where(lambda s: ~s.str.lower().isin(_NULL_LOWER), other=None)
        )
    # Aplica normalização de float-inteiro apenas nas colunas que já viraram string
    for col in dataframe.select_dtypes(include="object").columns:
        dataframe[col] = dataframe[col].map(
            lambda v: _int_float_re.sub(r"\1", v) if isinstance(v, str) else v
        )

    dataframe.columns = [normalize_column_name(c) for c in dataframe.columns]

    for col in ("cd_cpf_cnpj_cliente", "cd_cpf_cnpj_parceiro", "cd_cpf_cnpj_consultor"):
        if col in dataframe.columns:
            dataframe[col] = dataframe[col].map(_normalize_document)
    if "data_base" in dataframe.columns:
        dataframe["data_base"] = dataframe["data_base"].map(_normalize_data_base)

    # Filtra: apenas PJ + LIBERADA + MES_REF_COMISS preenchido
    before = len(dataframe)
    if "tipo_pessoa" in dataframe.columns:
        dataframe = dataframe[dataframe["tipo_pessoa"].str.strip().str.upper() == "PJ"]
    if "status_cc" in dataframe.columns:
        dataframe = dataframe[dataframe["status_cc"].str.strip().str.upper() == "LIBERADA"]
    if "mes_ref_comiss" in dataframe.columns:
        dataframe = dataframe[
            dataframe["mes_ref_comiss"].notna()
            & (dataframe["mes_ref_comiss"].astype(str).str.strip() != "")
            & (dataframe["mes_ref_comiss"].astype(str).str.lower() != "nan")
        ]
    logger.info(
        "Filtro PJ+LIBERADA+MES_REF_COMISS: %d → %d linhas", before, len(dataframe),
        extra={"job_id": job_id, "step": "clean", "event": "filter_applied", "rows": len(dataframe)},
    )
    dataframe = dataframe.reset_index(drop=True)

    set_cached_dataframe(job_id, dataframe)

    mark_step_done(session, job_id, "clean")
    logger.info(
        "Clean done",
        extra={"job_id": job_id, "step": "clean", "event": "clean_done", "rows": len(dataframe)},
    )
