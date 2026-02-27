from typing import Any

from pydantic import BaseModel


class VisaoClienteSearchOut(BaseModel):
    documento_consultado: str
    total: int
    limit: int
    offset: int
    items: list[dict[str, Any]]
