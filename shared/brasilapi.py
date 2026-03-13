import unicodedata
import re
import logging

import httpx

from shared.config import get_settings

logger = logging.getLogger(__name__)

# Mapeamento C6 Bank -> API CNPJ para comparacao de campos
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


def fetch_cnpj(cnpj: str, timeout: int = 10, api_key: str = "") -> dict | None:
    """
    Consulta a API CNPJ para um CNPJ.
    Retorna dict com campos normalizados para TEXT, ou None se nao encontrado/erro.
    """
    base_url = get_settings().CNPJ_API_URL.rstrip("/")
    url = f"{base_url}/cnpj/{cnpj}"
    headers = {"X-API-Key": api_key} if api_key else {}
    try:
        response = httpx.get(url, headers=headers, timeout=timeout)
        if response.status_code == 401:
            logger.error("API CNPJ: chave de acesso invalida ou ausente (401)")
            return None
        if response.status_code == 404:
            logger.warning("CNPJ nao encontrado na API CNPJ: %s", cnpj)
            return None
        if response.status_code == 429:
            logger.warning("API CNPJ: limite de requisicoes atingido (429) para CNPJ %s", cnpj)
            return None
        if response.status_code != 200:
            logger.warning("API CNPJ retornou status %s para CNPJ %s", response.status_code, cnpj)
            return None

        data = response.json()
        return {
            "razao_social":        str(data.get("razao_social") or ""),
            "nome_fantasia":       str(data.get("nome_fantasia") or ""),
            "situacao_cadastral":  str(data.get("situacao_cadastral") or ""),
            # descricao_situacao: tenta campo curto e longo para compatibilidade
            "descricao_situacao":  str(data.get("descricao_situacao") or data.get("descricao_situacao_cadastral") or ""),
            "cnae_fiscal":         str(data.get("cnae_fiscal") or ""),
            # cnae_descricao: tenta campo curto e longo para compatibilidade
            "cnae_descricao":      str(data.get("cnae_descricao") or data.get("cnae_fiscal_descricao") or ""),
            "natureza_juridica":   str(data.get("natureza_juridica") or ""),
            "capital_social":      str(data.get("capital_social") or ""),
            "porte":               str(data.get("porte") or ""),
            "uf":                  str(data.get("uf") or ""),
            "municipio":           str(data.get("municipio") or ""),
            "email":               str(data.get("email") or ""),
            "data_inicio_ativ":    str(data.get("data_inicio_atividade") or data.get("data_inicio_ativ") or ""),
        }
    except Exception as exc:
        logger.warning("Erro ao consultar API CNPJ para CNPJ %s: %s", cnpj, exc)
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
        # Only flag confirmed mismatches where both sides have data.
        # If one side is empty, we cannot determine the direction of the divergence.
        if c6_val and rf_val and c6_val != rf_val:
            divergencias.append({
                "campo":    c6_field,
                "valor_c6": str(c6_row.get(c6_field) or ""),
                "valor_rf": str(rf_data.get(rf_field) or ""),
            })
    return divergencias
