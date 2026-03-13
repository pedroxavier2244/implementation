import re
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from api.schemas.data import SnapshotItem, VisaoClienteHistoricoOut, VisaoClienteSearchOut
from shared.brasilapi import fetch_cnpj
from shared.config import get_settings
from shared.db import get_db_session
from shared.models import CnpjRfCache
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

RF_FINAL_COLUMNS = (
    "rf_razao_social",
    "rf_natureza_juridica",
    "rf_capital_social",
    "rf_porte_empresa",
    "rf_nome_fantasia",
    "rf_situacao_cadastral",
    "rf_data_inicio_ativ",
    "rf_cnae_principal",
    "rf_uf",
    "rf_municipio",
    "rf_email",
)

RF_EXTRA_COLUMNS = (
    "nome_fantasia",
    "situacao_cadastral",
    "descricao_situacao",
    "cnae_fiscal",
    "cnae_descricao",
    "natureza_juridica",
    "capital_social",
    "porte",
    "data_inicio_ativ",
    "data_source",
)

OUTPUT_COLUMNS = tuple(dict.fromkeys([*REQUIRED_COLUMNS, *RF_FINAL_COLUMNS, *RF_EXTRA_COLUMNS]))


def _only_digits(value: str) -> str:
    return re.sub(r"[^0-9]", "", value or "")


def _is_cnpj(documento: str) -> bool:
    return len(documento) == 14


def _build_rf_item(documento: str, rf_data: dict, source: str) -> dict:
    item = {
        "data_source": source,
        "cd_cpf_cnpj_cliente": documento,
        "nome_cliente": rf_data.get("razao_social"),
        "nome_fantasia": rf_data.get("nome_fantasia"),
        "situacao_cadastral": rf_data.get("situacao_cadastral"),
        "descricao_situacao": rf_data.get("descricao_situacao"),
        "cnae_fiscal": rf_data.get("cnae_fiscal"),
        "cnae_descricao": rf_data.get("cnae_descricao"),
        "natureza_juridica": rf_data.get("natureza_juridica"),
        "capital_social": rf_data.get("capital_social"),
        "porte": rf_data.get("porte"),
        "uf": rf_data.get("uf"),
        "cidade": rf_data.get("municipio"),
        "email": rf_data.get("email"),
        "data_inicio_ativ": rf_data.get("data_inicio_ativ"),
        "rf_razao_social": rf_data.get("razao_social"),
        "rf_natureza_juridica": rf_data.get("natureza_juridica"),
        "rf_capital_social": rf_data.get("capital_social"),
        "rf_porte_empresa": rf_data.get("porte"),
        "rf_nome_fantasia": rf_data.get("nome_fantasia"),
        "rf_situacao_cadastral": rf_data.get("situacao_cadastral"),
        "rf_data_inicio_ativ": rf_data.get("data_inicio_ativ"),
        "rf_cnae_principal": rf_data.get("cnae_fiscal"),
        "rf_uf": rf_data.get("uf"),
        "rf_municipio": rf_data.get("municipio"),
        "rf_email": rf_data.get("email"),
    }
    return _normalize_output_item(item)


def _cache_to_rf_dict(cache: CnpjRfCache) -> dict:
    return {
        "razao_social": cache.razao_social or "",
        "nome_fantasia": cache.nome_fantasia or "",
        "situacao_cadastral": cache.situacao_cadastral or "",
        "descricao_situacao": cache.descricao_situacao or "",
        "cnae_fiscal": cache.cnae_fiscal or "",
        "cnae_descricao": cache.cnae_descricao or "",
        "natureza_juridica": cache.natureza_juridica or "",
        "capital_social": cache.capital_social or "",
        "porte": cache.porte or "",
        "uf": cache.uf or "",
        "municipio": cache.municipio or "",
        "email": cache.email or "",
        "data_inicio_ativ": cache.data_inicio_ativ or "",
    }


def _is_cache_fresh(last_checked_at, ttl_days: int) -> bool:
    if last_checked_at is None:
        return False
    return last_checked_at >= datetime.now(timezone.utc) - timedelta(days=ttl_days)


def _update_rf_cache(session, documento: str, rf_data: dict) -> None:
    cache = session.query(CnpjRfCache).filter_by(cnpj=documento).first()
    if cache is None:
        cache = CnpjRfCache(cnpj=documento)
        session.add(cache)

    cache.razao_social = rf_data.get("razao_social")
    cache.nome_fantasia = rf_data.get("nome_fantasia")
    cache.situacao_cadastral = rf_data.get("situacao_cadastral")
    cache.descricao_situacao = rf_data.get("descricao_situacao")
    cache.cnae_fiscal = rf_data.get("cnae_fiscal")
    cache.cnae_descricao = rf_data.get("cnae_descricao")
    cache.natureza_juridica = rf_data.get("natureza_juridica")
    cache.capital_social = rf_data.get("capital_social")
    cache.porte = rf_data.get("porte")
    cache.uf = rf_data.get("uf")
    cache.municipio = rf_data.get("municipio")
    cache.email = rf_data.get("email")
    cache.data_inicio_ativ = rf_data.get("data_inicio_ativ")
    cache.last_checked_at = datetime.now(timezone.utc)


def _normalize_output_item(item: dict) -> dict:
    normalized = {column: None for column in OUTPUT_COLUMNS}
    normalized.update(item)
    return normalized


@router.get("/visao-cliente", response_model=VisaoClienteSearchOut)
def get_visao_cliente_by_documento(
    documento: str = Query(..., description="CPF/CNPJ com ou sem pontuacao"),
    limit: int = Query(1, ge=1, le=500),
    offset: int = Query(0, ge=0),
    fallback_rf: bool = Query(
        True,
        description="Quando nao encontrar no banco local e for CNPJ, busca na Receita Federal (BrasilAPI)",
    ),
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

        if total == 0 and fallback_rf and _is_cnpj(documento_consultado):
            settings = get_settings()
            ttl_days = settings.CNPJ_CACHE_TTL_DAYS

            cache = session.query(CnpjRfCache).filter_by(cnpj=documento_consultado).first()
            if cache and _is_cache_fresh(cache.last_checked_at, ttl_days) and (cache.situacao_cadastral or "").upper() != "NAO_ENCONTRADO":
                rows = [_build_rf_item(documento_consultado, _cache_to_rf_dict(cache), "receita_federal_cache")]
                total = 1
            else:
                rf_data = fetch_cnpj(documento_consultado, timeout=settings.CNPJ_API_TIMEOUT)
                if rf_data:
                    _update_rf_cache(session, documento_consultado, rf_data)
                    rows = [_build_rf_item(documento_consultado, rf_data, "receita_federal_brasilapi")]
                    total = 1

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
