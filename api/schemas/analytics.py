from datetime import date
from typing import Any, Literal

from pydantic import BaseModel

PeriodType = Literal["daily", "weekly", "monthly"]


class IndicatorSummaryOut(BaseModel):
    indicator: str
    period: PeriodType
    as_of: date
    period_start: date
    period_end: date
    total: int


class IndicatorDetailsOut(BaseModel):
    indicator: str
    period: PeriodType
    as_of: date
    period_start: date
    period_end: date
    total: int
    limit: int
    offset: int
    items: list[dict[str, Any]]
