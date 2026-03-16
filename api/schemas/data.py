from datetime import date, datetime
from typing import Any

from pydantic import BaseModel


class VisaoClienteSearchOut(BaseModel):
    documento_consultado: str
    total: int
    limit: int
    offset: int
    items: list[dict[str, Any]]


class SnapshotItem(BaseModel):
    data_base: date | None
    carregado_em: datetime | None
    etl_job_id: str | None
    campos_alterados: dict[str, dict[str, Any]] | None
    dados: dict[str, Any]


class VisaoClienteHistoricoOut(BaseModel):
    documento_consultado: str
    total_snapshots: int
    limit: int
    offset: int
    snapshots: list[SnapshotItem]


class ChangeHistoryItem(BaseModel):
    id: int
    data_base: str | None
    changed_at: datetime | None
    etl_job_id: str
    file_id: str | None
    file_date: date | None
    filename: str | None
    change_type: str
    field_name: str | None
    old_value: str | None
    new_value: str | None


class VisaoClienteChangeHistoryOut(BaseModel):
    documento_consultado: str
    total_eventos: int
    limit: int
    offset: int
    items: list[ChangeHistoryItem]
