# shared/cnpj_api.py
"""Cliente HTTP para a API interna de CNPJs (Receita Federal).

Endpoint: GET {CNPJ_API_URL}/cnpj/{cnpj}
Auth: X-API-Key header
Retorna cnae_fiscal (int) ou None em caso de erro/nao encontrado.
"""
import requests
from shared.config import get_settings
from shared.logging_config import get_logger

logger = get_logger(__name__)


def fetch_cnae_code(cnpj: str, timeout: int = 10) -> str | None:
    """Retorna o codigo CNAE formatado ('XXXX-X') para um CNPJ ou None.

    Ignora CPFs (< 14 digitos). Falhas de rede retornam None silenciosamente.
    Conversao: cnae_fiscal int -> zfill(7) -> 'XXXX-X' (ex: 4711301 -> '4711-3').
    """
    digits = "".join(c for c in str(cnpj or "") if c.isdigit())
    if len(digits) != 14:
        return None

    settings = get_settings()
    url = f"{settings.CNPJ_API_URL}/cnpj/{digits}"
    headers = {"X-API-Key": settings.CNPJ_API_KEY}

    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        cnae_fiscal = data.get("cnae_fiscal")
        if not cnae_fiscal:
            return None
        code = str(int(cnae_fiscal)).zfill(7)
        return f"{code[:4]}-{code[4]}"
    except Exception as exc:
        logger.warning(
            "cnpj_api: erro ao buscar CNPJ %s: %s", digits, exc,
            extra={"event": "cnpj_api_error", "cnpj": digits},
        )
        return None
