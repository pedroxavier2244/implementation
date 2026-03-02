from fastapi import APIRouter, HTTPException, Query

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
def get_cnpj_cache(cnpj: str):
    """Retorna os dados da Receita Federal em cache para um CNPJ."""
    with get_db_session() as session:
        cache = session.query(CnpjRfCache).filter_by(cnpj=cnpj).first()
        if cache is None:
            raise HTTPException(status_code=404, detail="CNPJ nao encontrado no cache")
        return {
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
