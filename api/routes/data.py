import re
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from api.schemas.data import (
    ChangeHistoryItem,
    SnapshotItem,
    VisaoClienteChangeHistoryOut,
    VisaoClienteHistoricoOut,
    VisaoClienteSearchOut,
)
from shared.db import get_db_session
from shared.visao_cliente_schema import FINAL_TABLE_NAME, REQUIRED_COLUMNS, STAGING_TABLE_NAME

_DIFF_IGNORE_FIELDS = frozenset({"etl_job_id", "loaded_at", "__total"})


def _compute_diff(
    anterior: dict | None, atual: dict
) -> dict[str, dict[str, str | None]] | None:
    """Returns fields that changed between two snapshots. None if it's the first snapshot."""
    if anterior is None:
        return None
    diff = {}
    for key, val_atual in atual.items():
        if key in _DIFF_IGNORE_FIELDS:
            continue
        val_anterior = anterior.get(key)
        # Ignore fields where both are None
        if val_anterior is None and val_atual is None:
            continue
        if str(val_anterior) != str(val_atual):
            diff[key] = {
                "de": str(val_anterior) if val_anterior is not None else None,
                "para": str(val_atual) if val_atual is not None else None,
            }
    return diff if diff else None


router = APIRouter(prefix="/data", tags=["data"])

OUTPUT_COLUMNS = tuple(REQUIRED_COLUMNS)
CHANGE_HISTORY_TABLE = "visao_cliente_change_history"


def _only_digits(value: str) -> str:
    return re.sub(r"[^0-9]", "", value or "")


def _is_cnpj(documento: str) -> bool:
    return len(documento) == 14


def _normalize_output_item(item: dict) -> dict:
    normalized = {column: None for column in OUTPUT_COLUMNS}
    normalized.update(item)
    return normalized


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
        sanitized_rows.append(_normalize_output_item(item))

    return VisaoClienteSearchOut(
        documento_consultado=documento_consultado,
        total=total,
        limit=limit,
        offset=offset,
        items=sanitized_rows,
    )


@router.get("/visao-cliente/historico", response_model=VisaoClienteHistoricoOut)
def get_visao_cliente_historico(
    documento: str = Query(..., description="CPF/CNPJ com ou sem pontuacao"),
    limit: int = Query(50, ge=1, le=500),
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
                FROM {STAGING_TABLE_NAME}
                WHERE cd_cpf_cnpj_cliente = :documento
                ORDER BY data_base ASC NULLS LAST, loaded_at ASC
                LIMIT :limit OFFSET :offset
                """
            ),
            {"documento": documento_consultado, "limit": limit, "offset": offset},
        ).mappings().all()

    total = int(rows[0]["__total"]) if rows else 0

    snapshots = []
    anterior: dict | None = None
    for row in rows:
        row_dict = dict(row)
        row_dict.pop("__total", None)

        diff = _compute_diff(anterior, row_dict)
        dados = {k: v for k, v in row_dict.items() if k not in ("etl_job_id", "loaded_at")}

        snapshots.append(SnapshotItem(
            data_base=row_dict.get("data_base"),
            carregado_em=row_dict.get("loaded_at"),
            etl_job_id=str(row_dict["etl_job_id"]) if row_dict.get("etl_job_id") else None,
            campos_alterados=diff,
            dados=dados,
        ))
        anterior = row_dict

    return VisaoClienteHistoricoOut(
        documento_consultado=documento_consultado,
        total_snapshots=total,
        limit=limit,
        offset=offset,
        snapshots=snapshots,
    )


@router.get("/visao-cliente/historico-alteracoes", response_model=VisaoClienteChangeHistoryOut)
def get_visao_cliente_historico_alteracoes(
    documento: str = Query(..., description="CPF/CNPJ com ou sem pontuacao"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    documento_consultado = _only_digits(documento)
    if not documento_consultado:
        raise HTTPException(status_code=400, detail="documento must contain digits")

    with get_db_session() as session:
        rows = session.execute(
            text(
                f"""
                SELECT
                    h.id,
                    h.data_base,
                    h.changed_at,
                    h.etl_job_id,
                    h.file_id,
                    f.file_date,
                    f.filename,
                    h.change_type,
                    h.field_name,
                    h.old_value,
                    h.new_value,
                    COUNT(*) OVER() AS __total
                FROM {CHANGE_HISTORY_TABLE} AS h
                LEFT JOIN etl_file AS f
                  ON f.id = h.file_id
                WHERE h.documento = :documento
                ORDER BY h.changed_at ASC NULLS LAST, h.id ASC
                LIMIT :limit OFFSET :offset
                """
            ),
            {"documento": documento_consultado, "limit": limit, "offset": offset},
        ).mappings().all()

    total = int(rows[0]["__total"]) if rows else 0
    items = []
    for row in rows:
        item = dict(row)
        item.pop("__total", None)
        items.append(ChangeHistoryItem.model_validate(item))

    return VisaoClienteChangeHistoryOut(
        documento_consultado=documento_consultado,
        total_eventos=total,
        limit=limit,
        offset=offset,
        items=items,
    )
