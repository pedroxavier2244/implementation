from unittest.mock import MagicMock, patch
import pytest


def test_fetch_cnpj_returns_mapped_dict():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "cnpj": "11222333000181",
        "razao_social": "EMPRESA TESTE LTDA",
        "nome_fantasia": "TESTE",
        "situacao_cadastral": "02",
        "descricao_situacao": "ATIVA",
        "cnae_fiscal": "6201501",
        "cnae_descricao": "Desenvolvimento de programas",
        "natureza_juridica": "Sociedade Empresaria Limitada",
        "capital_social": "10000.0",
        "porte": "01",
        "uf": "SP",
        "municipio": "SAO PAULO",
        "email": "contato@teste.com",
        "data_inicio_atividade": "2020-01-15",
    }

    with patch("httpx.get", return_value=mock_response):
        from shared.brasilapi import fetch_cnpj
        result = fetch_cnpj("11222333000181", timeout=5, api_key="test-key")

    assert result["razao_social"] == "EMPRESA TESTE LTDA"
    assert result["situacao_cadastral"] == "02"
    assert result["cnae_fiscal"] == "6201501"
    assert result["capital_social"] == "10000.0"
    assert result["descricao_situacao"] == "ATIVA"
    assert result["cnae_descricao"] == "Desenvolvimento de programas"
    assert result["data_inicio_ativ"] == "2020-01-15"


def test_fetch_cnpj_returns_none_on_404():
    mock_response = MagicMock()
    mock_response.status_code = 404

    with patch("httpx.get", return_value=mock_response):
        from shared.brasilapi import fetch_cnpj
        result = fetch_cnpj("00000000000000", timeout=5, api_key="test-key")

    assert result is None


def test_fetch_cnpj_returns_none_on_401():
    mock_response = MagicMock()
    mock_response.status_code = 401

    with patch("httpx.get", return_value=mock_response):
        from shared.brasilapi import fetch_cnpj
        result = fetch_cnpj("11222333000181", timeout=5, api_key="chave-invalida")

    assert result is None


def test_fetch_cnpj_returns_none_on_429():
    mock_response = MagicMock()
    mock_response.status_code = 429

    with patch("httpx.get", return_value=mock_response):
        from shared.brasilapi import fetch_cnpj
        result = fetch_cnpj("11222333000181", timeout=5, api_key="test-key")

    assert result is None


def test_fetch_cnpj_returns_none_on_exception():
    with patch("httpx.get", side_effect=Exception("timeout")):
        from shared.brasilapi import fetch_cnpj
        result = fetch_cnpj("11222333000181", timeout=5, api_key="test-key")

    assert result is None


def test_fetch_cnpj_sends_auth_header():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "razao_social": "EMPRESA TESTE LTDA",
        "situacao_cadastral": "02",
    }

    with patch("httpx.get", return_value=mock_response) as mock_get:
        from shared.brasilapi import fetch_cnpj
        fetch_cnpj("11222333000181", timeout=5, api_key="minha-chave")

    call_kwargs = mock_get.call_args
    assert call_kwargs.kwargs["headers"] == {"X-API-Key": "minha-chave"}


def test_fetch_cnpj_fallback_field_names():
    """Testa compatibilidade com nomes de campo alternativos (fallback)."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "razao_social": "EMPRESA LTDA",
        "descricao_situacao_cadastral": "ATIVA",  # nome alternativo
        "cnae_fiscal_descricao": "Comercio",      # nome alternativo
        "data_inicio_ativ": "2021-06-01",          # nome alternativo
    }

    with patch("httpx.get", return_value=mock_response):
        from shared.brasilapi import fetch_cnpj
        result = fetch_cnpj("11222333000181", timeout=5, api_key="test-key")

    assert result["descricao_situacao"] == "ATIVA"
    assert result["cnae_descricao"] == "Comercio"
    assert result["data_inicio_ativ"] == "2021-06-01"


def test_normalize_for_comparison_removes_accents_and_punctuation():
    from shared.brasilapi import normalize_for_comparison
    assert normalize_for_comparison("Sao Paulo") == "SAO PAULO"
    assert normalize_for_comparison("LTDA.") == "LTDA"
    assert normalize_for_comparison("S/A") == "S A"
    assert normalize_for_comparison(None) == ""
    assert normalize_for_comparison("") == ""


def test_compare_fields_detects_divergence():
    from shared.brasilapi import compare_fields
    c6_data = {"nome_cliente": "EMPRESA VELHA SA", "uf": "SP", "cidade": "SAO PAULO"}
    rf_data = {"razao_social": "EMPRESA NOVA SA", "uf": "RJ", "municipio": "RIO DE JANEIRO"}

    divergencias = compare_fields(c6_data, rf_data)
    assert len(divergencias) == 3
    campos = [d["campo"] for d in divergencias]
    assert "nome_cliente" in campos
    assert "uf" in campos
    assert "cidade" in campos


def test_compare_fields_no_divergence_when_equal():
    from shared.brasilapi import compare_fields
    c6_data = {"nome_cliente": "EMPRESA TESTE LTDA", "uf": "SP", "cidade": "SAO PAULO"}
    rf_data = {"razao_social": "EMPRESA TESTE LTDA", "uf": "SP", "municipio": "SAO PAULO"}

    divergencias = compare_fields(c6_data, rf_data)
    assert divergencias == []
