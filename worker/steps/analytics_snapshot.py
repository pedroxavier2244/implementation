import re
import unicodedata
from datetime import date, datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.analytics_snapshot_schema import TABLE_NAME
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done
from worker.steps.extract import get_cached_workbook


def _normalize_token(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    return re.sub(r"_+", "_", normalized).strip("_")


def _find_sheet(workbook: dict[str, object], candidates: list[str]) -> tuple[str, object] | tuple[None, None]:
    if not workbook:
        return None, None

    normalized_to_original = {_normalize_token(name): name for name in workbook.keys()}
    for candidate in candidates:
        normalized = _normalize_token(candidate)
        if normalized in normalized_to_original:
            original = normalized_to_original[normalized]
            return original, workbook[original]

    return None, None


def _find_column(dataframe, candidates: list[str]) -> str | None:
    normalized_map = {_normalize_token(col): col for col in dataframe.columns}
    for candidate in candidates:
        found = normalized_map.get(_normalize_token(candidate))
        if found:
            return found
    return None


def _sum_numeric(series) -> int:
    import pandas as pd

    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.replace({"": None, "nan": None, "none": None, "nat": None, "None": None})
    cleaned = cleaned.str.replace(r"[^0-9,.\-]", "", regex=True)

    has_comma = cleaned.str.contains(",", na=False)
    has_dot = cleaned.str.contains(r"\.", na=False)

    brl_mask = has_comma & has_dot
    cleaned.loc[brl_mask] = cleaned.loc[brl_mask].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)

    comma_only_mask = has_comma & ~has_dot
    cleaned.loc[comma_only_mask] = cleaned.loc[comma_only_mask].str.replace(",", ".", regex=False)

    numeric = pd.to_numeric(cleaned, errors="coerce").fillna(0)
    return int(numeric.sum())


def _require_sheet(workbook: dict[str, object], candidates: list[str], label: str):
    name, dataframe = _find_sheet(workbook, candidates)
    if dataframe is None:
        available = ", ".join(workbook.keys())
        raise ValueError(f"Sheet '{label}' not found. Available sheets: {available}")
    return name, dataframe


def _require_column(dataframe, candidates: list[str], label: str, sheet_name: str) -> str:
    column = _find_column(dataframe, candidates)
    if column is None:
        available = ", ".join(map(str, dataframe.columns))
        raise ValueError(f"Column '{label}' not found in sheet '{sheet_name}'. Available columns: {available}")
    return column


def run_analytics_snapshot(session: Session, job_id: str, etl_file) -> None:
    step_name = "analytics_snapshot"
    if is_step_done(session, job_id, step_name):
        return
    begin_step(session, job_id, step_name)

    workbook = get_cached_workbook(job_id)
    if workbook is None:
        raise RuntimeError("No workbook in cache, extract must run first")

    reference_date: date = etl_file.file_date or date.today()
    loaded_at = datetime.now(timezone.utc)

    abertura_sheet_name, df_abertura = _require_sheet(workbook, ["Abertura"], "Abertura")
    relacionamento_sheet_name, df_relacionamento = _require_sheet(workbook, ["Relacionamento"], "Relacionamento")

    total_abertas_col = _require_column(
        df_abertura,
        ["Total de Contas Abertas", "Total_de_Contas_Abertas"],
        "Total de Contas Abertas",
        abertura_sheet_name,
    )
    contas_qualificadas_col = _require_column(
        df_abertura,
        ["Contas Qualificadas", "Contas_Qualificadas"],
        "Contas Qualificadas",
        abertura_sheet_name,
    )
    maquinas_vendidas_col = _require_column(
        df_relacionamento,
        ["Maquinas Vendidas Relacionamento", "Maquinas_Vendidas_Relacionamento"],
        "Maquinas Vendidas Relacionamento",
        relacionamento_sheet_name,
    )

    metric_rows = [
        {
            "indicator": "contas-abertas",
            "reference_date": reference_date,
            "total": _sum_numeric(df_abertura[total_abertas_col]),
            "source_sheet": abertura_sheet_name,
            "source_column": total_abertas_col,
            "job_id": job_id,
            "file_id": etl_file.id,
            "loaded_at": loaded_at,
        },
        {
            "indicator": "contas-qualificadas",
            "reference_date": reference_date,
            "total": _sum_numeric(df_abertura[contas_qualificadas_col]),
            "source_sheet": abertura_sheet_name,
            "source_column": contas_qualificadas_col,
            "job_id": job_id,
            "file_id": etl_file.id,
            "loaded_at": loaded_at,
        },
        {
            "indicator": "instalacao-c6pay",
            "reference_date": reference_date,
            "total": _sum_numeric(df_relacionamento[maquinas_vendidas_col]),
            "source_sheet": relacionamento_sheet_name,
            "source_column": maquinas_vendidas_col,
            "job_id": job_id,
            "file_id": etl_file.id,
            "loaded_at": loaded_at,
        },
    ]

    upsert = text(
        f"""
        INSERT INTO {TABLE_NAME} (
            indicator, reference_date, total, source_sheet, source_column, job_id, file_id, loaded_at
        )
        VALUES (
            :indicator, :reference_date, :total, :source_sheet, :source_column, :job_id, :file_id, :loaded_at
        )
        ON CONFLICT (indicator, reference_date)
        DO UPDATE SET
            total = EXCLUDED.total,
            source_sheet = EXCLUDED.source_sheet,
            source_column = EXCLUDED.source_column,
            job_id = EXCLUDED.job_id,
            file_id = EXCLUDED.file_id,
            loaded_at = EXCLUDED.loaded_at
        """
    )
    for row in metric_rows:
        session.execute(upsert, row)

    mark_step_done(session, job_id, step_name)
