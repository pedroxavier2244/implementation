import re
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query

from shared.brasilapi import fetch_cnpj
from shared.config import get_settings
from shared.db import get_db_session
from shared.models import CnpjRfCache, CnpjDivergencia

router = APIRouter(prefix="/cnpj", tags=["cnpj"])


@router.get("/divergencias/list")
def list_divergencias(
    cnpj: str | None = Query(default=None),
    campo: str | None = Query(default=None),
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0),
):
    """Lista divergencias entre C6 Bank e Receita Federal."""
    with get_db_session() as session:
        query = session.query(CnpjDivergencia)
        if cnpj:
            query = query.filter(CnpjDivergencia.cnpj == cnpj)
        if campo:
            query = query.filter(CnpjDivergencia.campo == campo)
        total = query.count()
        items = query.order_by(CnpjDivergencia.found_at.desc()).offset(offset).limit(limit).all()
        return {
            "total": total,
            "items": [
                {
                    "id":       item.id,
                    "job_id":   item.job_id,
                    "cnpj":     item.cnpj,
                    "campo":    item.campo,
                    "valor_c6": item.valor_c6,
                    "valor_rf": item.valor_rf,
                    "found_at": item.found_at.isoformat() if item.found_at else None,
                }
                for item in items
            ],
        }


@router.get("/{cnpj}")
def get_cnpj_cache(
    cnpj: str,
    fallback_live: bool = Query(
        True,
        description="Se nao existir no cache local, consulta Receita Federal (BrasilAPI)",
    ),
):
    """Retorna dados da Receita Federal via cache local, com fallback opcional em tempo real."""
    cnpj_digits = re.sub(r"[^0-9]", "", cnpj or "")
    if len(cnpj_digits) != 14:
        raise HTTPException(status_code=400, detail="cnpj must have 14 digits")

    with get_db_session() as session:
        cache = session.query(CnpjRfCache).filter_by(cnpj=cnpj_digits).first()
        source = "cache"

        if cache is None and fallback_live:
            settings = get_settings()
            live_data = fetch_cnpj(cnpj_digits, timeout=settings.CNPJ_API_TIMEOUT, api_key=settings.CNPJ_API_KEY)
            if live_data:
                cache = CnpjRfCache(cnpj=cnpj_digits)
                cache.razao_social = live_data.get("razao_social")
                cache.nome_fantasia = live_data.get("nome_fantasia")
                cache.situacao_cadastral = live_data.get("situacao_cadastral")
                cache.descricao_situacao = live_data.get("descricao_situacao")
                cache.cnae_fiscal = live_data.get("cnae_fiscal")
                cache.cnae_descricao = live_data.get("cnae_descricao")
                cache.natureza_juridica = live_data.get("natureza_juridica")
                cache.capital_social = live_data.get("capital_social")
                cache.porte = live_data.get("porte")
                cache.uf = live_data.get("uf")
                cache.municipio = live_data.get("municipio")
                cache.email = live_data.get("email")
                cache.data_inicio_ativ = live_data.get("data_inicio_ativ")
                cache.last_checked_at = datetime.now(timezone.utc)
                session.add(cache)
                session.flush()
                source = "receita_federal_api"

        if cache is None:
            raise HTTPException(status_code=404, detail="CNPJ nao encontrado no cache e na API CNPJ")

        return {
            "data_source":         source,
            "cnpj":               cache.cnpj,
            "razao_social":       cache.razao_social,
            "nome_fantasia":      cache.nome_fantasia,
            "situacao_cadastral": cache.situacao_cadastral,
            "descricao_situacao": cache.descricao_situacao,
            "cnae_fiscal":        cache.cnae_fiscal,
            "cnae_descricao":     cache.cnae_descricao,
            "natureza_juridica":  cache.natureza_juridica,
            "capital_social":     cache.capital_social,
            "porte":              cache.porte,
            "uf":                 cache.uf,
            "municipio":          cache.municipio,
            "email":              cache.email,
            "data_inicio_ativ":   cache.data_inicio_ativ,
            "last_checked_at":    cache.last_checked_at.isoformat() if cache.last_checked_at else None,
        }
