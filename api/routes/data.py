import re

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from api.schemas.data import VisaoClienteSearchOut
from shared.db import get_db_session
from shared.visao_cliente_schema import FINAL_TABLE_NAME

router = APIRouter(prefix="/data", tags=["data"])


def _only_digits(value: str) -> str:
    return re.sub(r"[^0-9]", "", value or "")


@router.get("/visao-cliente", response_model=VisaoClienteSearchOut)
def get_visao_cliente_by_documento(
    documento: str = Query(..., description="CPF/CNPJ com ou sem pontuacao"),
    limit: int = Query(1, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    documento_consultado = _only_digits(documento)
    if not documento_consultado:
        raise HTTPException(status_code=400, detail="documento must contain digits")

    with get_db_session() as session:
        rows = session.execute(
            text(
                f"""
                SELECT *, COUNT(*) OVER() AS __total
                FROM {FINAL_TABLE_NAME}
                WHERE cd_cpf_cnpj_cliente = :documento
                ORDER BY data_base DESC NULLS LAST
                LIMIT :limit OFFSET :offset
                """
            ),
            {"documento": documento_consultado, "limit": limit, "offset": offset},
        ).mappings().all()

        total = int(rows[0]["__total"]) if rows else 0

        # Backward compatibility for old rows that may still contain punctuation.
        if total == 0:
            rows = session.execute(
                text(
                    f"""
                    SELECT *, COUNT(*) OVER() AS __total
                    FROM {FINAL_TABLE_NAME}
                    WHERE regexp_replace(COALESCE(cd_cpf_cnpj_cliente, ''), '[^0-9]', '', 'g') = :documento
                    ORDER BY data_base DESC NULLS LAST
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {"documento": documento_consultado, "limit": limit, "offset": offset},
            ).mappings().all()
            total = int(rows[0]["__total"]) if rows else 0

    sanitized_rows = []
    for row in rows:
        item = dict(row)
        item.pop("__total", None)
        sanitized_rows.append(item)

    return VisaoClienteSearchOut(
        documento_consultado=documento_consultado,
        total=total,
        limit=limit,
        offset=offset,
        items=sanitized_rows,
    )
