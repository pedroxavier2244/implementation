from datetime import date, timedelta

from fastapi import APIRouter, Query
from sqlalchemy import text

from api.schemas.analytics import IndicatorDetailsOut, IndicatorSummaryOut, PeriodType
from shared.analytics_snapshot_schema import SNAPSHOT_INDICATORS, TABLE_NAME as SNAPSHOT_TABLE_NAME
from shared.db import get_db_session
from shared.visao_cliente_schema import FINAL_TABLE_NAME

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _date_expr(column: str) -> str:
    return f"""
        CASE
            WHEN NULLIF(BTRIM(COALESCE({column}, '')), '') IS NULL THEN NULL
            WHEN BTRIM({column}) ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}' THEN CAST(SUBSTRING(BTRIM({column}) FROM 1 FOR 10) AS DATE)
            WHEN BTRIM({column}) ~ '^\\d{{2}}/\\d{{2}}/\\d{{4}}$' THEN TO_DATE(BTRIM({column}), 'DD/MM/YYYY')
            WHEN BTRIM({column}) ~ '^\\d+(\\.0+)?$' THEN (DATE '1899-12-30' + CAST(SPLIT_PART(BTRIM({column}), '.', 1) AS INT))
            ELSE NULL
        END
    """


def _numeric_expr(column: str) -> str:
    base = f"NULLIF(BTRIM(COALESCE({column}, '')), '')"
    cleaned = f"regexp_replace({base}, '[^0-9,.-]', '', 'g')"
    normalized = (
        f"""
        CASE
            WHEN {cleaned} LIKE '%%,%%' AND {cleaned} LIKE '%%.%%' THEN REPLACE(REPLACE({cleaned}, '.', ''), ',', '.')
            WHEN {cleaned} LIKE '%%,%%' THEN REPLACE({cleaned}, ',', '.')
            ELSE {cleaned}
        END
        """
    )
    return f"CAST(NULLIF(({normalized}), '') AS NUMERIC)"


def _period_bounds(period: PeriodType, as_of: date) -> tuple[date, date]:
    if period == "daily":
        return as_of, as_of
    if period == "weekly":
        start = as_of - timedelta(days=as_of.weekday())
        end = start + timedelta(days=6)
        return start, end

    start = as_of.replace(day=1)
    if start.month == 12:
        next_month = start.replace(year=start.year + 1, month=1, day=1)
    else:
        next_month = start.replace(month=start.month + 1, day=1)
    end = next_month - timedelta(days=1)
    return start, end


COMMON_PJ_LIBERADA = """
    UPPER(BTRIM(COALESCE(tipo_pessoa, ''))) = 'PJ'
    AND UPPER(BTRIM(COALESCE(status_cc, ''))) = 'LIBERADA'
"""

QUALIFICACAO_C6PAY_WHERE_SQL = f"""
    {COMMON_PJ_LIBERADA}
    AND NULLIF(BTRIM(COALESCE(dt_cancelamento_maq, '')), '') IS NULL
    AND COALESCE({_numeric_expr('tpv_m0')}, 0) >= 5000
    AND COALESCE({_numeric_expr('tpv_m1')}, 0) < 5000
    AND COALESCE({_numeric_expr('tpv_m2')}, 0) < 5000
    AND {_date_expr('dt_install_maq')} >= :instalacao_limite
"""


def _summary_snapshot_payload(indicator: str, period: PeriodType, as_of: date) -> IndicatorSummaryOut:
    period_start, period_end = _period_bounds(period, as_of)
    query = text(
        f"""
        SELECT COALESCE(SUM(total), 0)::bigint AS total
        FROM {SNAPSHOT_TABLE_NAME}
        WHERE indicator = :indicator
          AND reference_date BETWEEN :period_start AND :period_end
        """
    )
    with get_db_session() as session:
        total = int(
            session.execute(
                query,
                {"indicator": indicator, "period_start": period_start, "period_end": period_end},
            ).scalar()
            or 0
        )

    return IndicatorSummaryOut(
        indicator=indicator,
        period=period,
        as_of=as_of,
        period_start=period_start,
        period_end=period_end,
        total=total,
    )


def _details_snapshot_payload(
    indicator: str,
    period: PeriodType,
    as_of: date,
    limit: int,
    offset: int,
) -> IndicatorDetailsOut:
    period_start, period_end = _period_bounds(period, as_of)
    sum_query = text(
        f"""
        SELECT COALESCE(SUM(total), 0)::bigint AS total
        FROM {SNAPSHOT_TABLE_NAME}
        WHERE indicator = :indicator
          AND reference_date BETWEEN :period_start AND :period_end
        """
    )
    rows_query = text(
        f"""
        SELECT
            indicator,
            reference_date,
            total,
            source_sheet,
            source_column,
            file_id,
            job_id,
            loaded_at
        FROM {SNAPSHOT_TABLE_NAME}
        WHERE indicator = :indicator
          AND reference_date BETWEEN :period_start AND :period_end
        ORDER BY reference_date DESC
        LIMIT :limit OFFSET :offset
        """
    )

    with get_db_session() as session:
        total = int(
            session.execute(
                sum_query,
                {"indicator": indicator, "period_start": period_start, "period_end": period_end},
            ).scalar()
            or 0
        )
        rows = session.execute(
            rows_query,
            {
                "indicator": indicator,
                "period_start": period_start,
                "period_end": period_end,
                "limit": limit,
                "offset": offset,
            },
        ).mappings().all()

    return IndicatorDetailsOut(
        indicator=indicator,
        period=period,
        as_of=as_of,
        period_start=period_start,
        period_end=period_end,
        total=total,
        limit=limit,
        offset=offset,
        items=[dict(row) for row in rows],
    )


def _summary_qualificacao_c6pay_payload(period: PeriodType, as_of: date) -> IndicatorSummaryOut:
    period_start, period_end = _period_bounds(period, as_of)
    instalacao_limite = as_of - timedelta(days=90)
    data_base_sql = _date_expr("data_base")

    query = text(
        f"""
        SELECT COUNT(*)::int AS total
        FROM {FINAL_TABLE_NAME}
        WHERE {QUALIFICACAO_C6PAY_WHERE_SQL}
          AND {data_base_sql} BETWEEN :period_start AND :period_end
        """
    )
    with get_db_session() as session:
        total = int(
            session.execute(
                query,
                {
                    "period_start": period_start,
                    "period_end": period_end,
                    "instalacao_limite": instalacao_limite,
                },
            ).scalar()
            or 0
        )

    return IndicatorSummaryOut(
        indicator="qualificacao-c6pay",
        period=period,
        as_of=as_of,
        period_start=period_start,
        period_end=period_end,
        total=total,
    )


def _details_qualificacao_c6pay_payload(period: PeriodType, as_of: date, limit: int, offset: int) -> IndicatorDetailsOut:
    period_start, period_end = _period_bounds(period, as_of)
    instalacao_limite = as_of - timedelta(days=90)
    data_base_sql = _date_expr("data_base")

    query = text(
        f"""
        SELECT *, COUNT(*) OVER() AS __total
        FROM {FINAL_TABLE_NAME}
        WHERE {QUALIFICACAO_C6PAY_WHERE_SQL}
          AND {data_base_sql} BETWEEN :period_start AND :period_end
        ORDER BY {data_base_sql} DESC NULLS LAST, data_base DESC NULLS LAST
        LIMIT :limit OFFSET :offset
        """
    )
    with get_db_session() as session:
        rows = session.execute(
            query,
            {
                "period_start": period_start,
                "period_end": period_end,
                "instalacao_limite": instalacao_limite,
                "limit": limit,
                "offset": offset,
            },
        ).mappings().all()

    total = int(rows[0]["__total"]) if rows else 0
    items = []
    for row in rows:
        item = dict(row)
        item.pop("__total", None)
        items.append(item)

    return IndicatorDetailsOut(
        indicator="qualificacao-c6pay",
        period=period,
        as_of=as_of,
        period_start=period_start,
        period_end=period_end,
        total=total,
        limit=limit,
        offset=offset,
        items=items,
    )


def _summary_payload(indicator: str, period: PeriodType, as_of: date):
    if indicator in SNAPSHOT_INDICATORS:
        return _summary_snapshot_payload(indicator, period, as_of)
    return _summary_qualificacao_c6pay_payload(period, as_of)


def _details_payload(indicator: str, period: PeriodType, as_of: date, limit: int, offset: int):
    if indicator in SNAPSHOT_INDICATORS:
        return _details_snapshot_payload(indicator, period, as_of, limit, offset)
    return _details_qualificacao_c6pay_payload(period, as_of, limit, offset)


def _as_of_date(as_of: date | None) -> date:
    return as_of or date.today()


@router.get("/contas-abertas/summary", response_model=IndicatorSummaryOut)
def contas_abertas_summary(
    period: PeriodType = Query("daily"),
    as_of: date | None = Query(None),
):
    return _summary_payload("contas-abertas", period, _as_of_date(as_of))


@router.get("/contas-abertas/details", response_model=IndicatorDetailsOut)
def contas_abertas_details(
    period: PeriodType = Query("daily"),
    as_of: date | None = Query(None),
    limit: int = Query(20, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    return _details_payload("contas-abertas", period, _as_of_date(as_of), limit, offset)


@router.get("/qualificacao-c6pay/summary", response_model=IndicatorSummaryOut)
def qualificacao_c6pay_summary(
    period: PeriodType = Query("daily"),
    as_of: date | None = Query(None),
):
    return _summary_payload("qualificacao-c6pay", period, _as_of_date(as_of))


@router.get("/qualificacao-c6pay/details", response_model=IndicatorDetailsOut)
def qualificacao_c6pay_details(
    period: PeriodType = Query("daily"),
    as_of: date | None = Query(None),
    limit: int = Query(20, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    return _details_payload("qualificacao-c6pay", period, _as_of_date(as_of), limit, offset)


@router.get("/instalacao-c6pay/summary", response_model=IndicatorSummaryOut)
def instalacao_c6pay_summary(
    period: PeriodType = Query("daily"),
    as_of: date | None = Query(None),
):
    return _summary_payload("instalacao-c6pay", period, _as_of_date(as_of))


@router.get("/instalacao-c6pay/details", response_model=IndicatorDetailsOut)
def instalacao_c6pay_details(
    period: PeriodType = Query("daily"),
    as_of: date | None = Query(None),
    limit: int = Query(20, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    return _details_payload("instalacao-c6pay", period, _as_of_date(as_of), limit, offset)


@router.get("/contas-qualificadas/summary", response_model=IndicatorSummaryOut)
def contas_qualificadas_summary(
    period: PeriodType = Query("daily"),
    as_of: date | None = Query(None),
):
    return _summary_payload("contas-qualificadas", period, _as_of_date(as_of))


@router.get("/contas-qualificadas/details", response_model=IndicatorDetailsOut)
def contas_qualificadas_details(
    period: PeriodType = Query("daily"),
    as_of: date | None = Query(None),
    limit: int = Query(20, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    return _details_payload("contas-qualificadas", period, _as_of_date(as_of), limit, offset)
