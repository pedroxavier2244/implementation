from datetime import date, datetime, timezone

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.analytics_snapshot_schema import TABLE_NAME
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done
from worker.steps.extract import get_cached_dataframe


def _to_numeric(series):
    s = pd.to_numeric(
        series.astype(str).str.replace(r"[^0-9.,\-]", "", regex=True).str.replace(",", ".", regex=False),
        errors="coerce",
    ).fillna(0)
    return s


def _in_reference_month(series, ref_date):
    dates = pd.to_datetime(series, errors="coerce", dayfirst=False)
    return (dates.dt.year == ref_date.year) & (dates.dt.month == ref_date.month)


def _str_eq(series, value):
    return series.astype(str).str.strip().str.upper() == value.upper()


def run_analytics_snapshot(session: Session, job_id: str, etl_file) -> None:
    step_name = "analytics_snapshot"
    if is_step_done(session, job_id, step_name):
        return
    begin_step(session, job_id, step_name)

    df = get_cached_dataframe(job_id)
    if df is None:
        raise RuntimeError("No dataframe in cache, extract must run first")

    reference_date: date = etl_file.file_date or date.today()
    loaded_at = datetime.now(timezone.utc)

    is_pj = _str_eq(df["tipo_pessoa"], "PJ")
    is_liberada = _str_eq(df["status_cc"], "LIBERADA")

    # contas-abertas: PJ + LIBERADA + dt_conta_criada in reference month
    in_ref_month_criada = _in_reference_month(df["dt_conta_criada"], reference_date)
    contas_abertas = int((is_pj & is_liberada & in_ref_month_criada).sum())

    # contas-qualificadas: PJ + LIBERADA + (ja_pago_comiss > 0 OR previsao_comiss > 0)
    ja_pago = _to_numeric(df["ja_pago_comiss"])
    previsao = _to_numeric(df["previsao_comiss"])
    has_commission = (ja_pago > 0) | (previsao > 0)
    contas_qualificadas = int((is_pj & is_liberada & has_commission).sum())

    # instalacao-c6pay: dt_install_maq not null + in reference month
    in_ref_month_install = _in_reference_month(df["dt_install_maq"], reference_date)
    install_not_null = pd.to_datetime(df["dt_install_maq"], errors="coerce").notna()
    instalacao_c6pay = int((install_not_null & in_ref_month_install).sum())

    source = "visao_cliente"

    metric_rows = [
        {
            "indicator": "contas-abertas",
            "reference_date": reference_date,
            "total": contas_abertas,
            "source_sheet": source,
            "source_column": "tipo_pessoa,status_cc,dt_conta_criada",
            "job_id": job_id,
            "file_id": etl_file.id,
            "loaded_at": loaded_at,
        },
        {
            "indicator": "contas-qualificadas",
            "reference_date": reference_date,
            "total": contas_qualificadas,
            "source_sheet": source,
            "source_column": "tipo_pessoa,status_cc,ja_pago_comiss,previsao_comiss",
            "job_id": job_id,
            "file_id": etl_file.id,
            "loaded_at": loaded_at,
        },
        {
            "indicator": "instalacao-c6pay",
            "reference_date": reference_date,
            "total": instalacao_c6pay,
            "source_sheet": source,
            "source_column": "dt_install_maq",
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
