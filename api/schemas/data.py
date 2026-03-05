from typing import Any

from pydantic import BaseModel


class VisaoClienteSearchOut(BaseModel):
    documento_consultado: str
    total: int
    limit: int
    offset: int
    items: list[dict[str, Any]]


class SnapshotItem(BaseModel):
    data_base: str | None
    carregado_em: str | None
    etl_job_id: str | None
    campos_alterados: dict[str, dict[str, Any]] | None
    dados: dict[str, Any]


class VisaoClienteHistoricoOut(BaseModel):
    documento_consultado: str
    total_snapshots: int
    limit: int
    offset: int
    snapshots: list[SnapshotItem]
