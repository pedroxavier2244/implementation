import unicodedata
import re
import logging

import httpx

logger = logging.getLogger(__name__)

BRASILAPI_URL = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"

# Mapeamento C6 Bank -> BrasilAPI para comparacao de campos
FIELD_MAP = [
    ("nome_cliente", "razao_social"),
    ("uf",           "uf"),
    ("cidade",       "municipio"),
    ("ramo_atuacao", "cnae_descricao"),
]


def normalize_for_comparison(value) -> str:
    """Remove acentos, pontuacao e converte para maiusculas para comparacao."""
    if not value:
        return ""
    text = str(value).strip()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip().upper()
    return text


def fetch_cnpj(cnpj: str, timeout: int = 10) -> dict | None:
    """
    Consulta a BrasilAPI para um CNPJ.
    Retorna dict com campos normalizados para TEXT, ou None se nao encontrado/erro.
    """
    url = BRASILAPI_URL.format(cnpj=cnpj)
    try:
        response = httpx.get(url, timeout=timeout)
        if response.status_code == 404:
            logger.warning("CNPJ nao encontrado na BrasilAPI: %s", cnpj)
            return None
        if response.status_code != 200:
            logger.warning("BrasilAPI retornou status %s para CNPJ %s", response.status_code, cnpj)
            return None

        data = response.json()
        return {
            "razao_social":        str(data.get("razao_social") or ""),
            "nome_fantasia":       str(data.get("nome_fantasia") or ""),
            "situacao_cadastral":  str(data.get("situacao_cadastral") or ""),
            "descricao_situacao":  str(data.get("descricao_situacao_cadastral") or ""),
            "cnae_fiscal":         str(data.get("cnae_fiscal") or ""),
            "cnae_descricao":      str(data.get("cnae_fiscal_descricao") or ""),
            "natureza_juridica":   str(data.get("natureza_juridica") or ""),
            "capital_social":      str(data.get("capital_social") or ""),
            "porte":               str(data.get("porte") or ""),
            "uf":                  str(data.get("uf") or ""),
            "municipio":           str(data.get("municipio") or ""),
            "email":               str(data.get("email") or ""),
            "data_inicio_ativ":    str(data.get("data_inicio_atividade") or ""),
        }
    except Exception as exc:
        logger.warning("Erro ao consultar BrasilAPI para CNPJ %s: %s", cnpj, exc)
        return None


def compare_fields(c6_row: dict, rf_data: dict) -> list[dict]:
    """
    Compara campos do C6 Bank com dados da Receita Federal.
    Retorna lista de divergencias: [{"campo": str, "valor_c6": str, "valor_rf": str}]
    """
    divergencias = []
    for c6_field, rf_field in FIELD_MAP:
        c6_val = normalize_for_comparison(c6_row.get(c6_field))
        rf_val = normalize_for_comparison(rf_data.get(rf_field))
        if c6_val and rf_val and c6_val != rf_val:
            divergencias.append({
                "campo":    c6_field,
                "valor_c6": str(c6_row.get(c6_field) or ""),
                "valor_rf": str(rf_data.get(rf_field) or ""),
            })
    return divergencias
