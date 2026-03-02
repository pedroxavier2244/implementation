from unittest.mock import MagicMock, patch
import pytest


def test_fetch_cnpj_returns_mapped_dict():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "cnpj": "11222333000181",
        "razao_social": "EMPRESA TESTE LTDA",
        "nome_fantasia": "TESTE",
        "situacao_cadastral": 2,
        "descricao_situacao_cadastral": "ATIVA",
        "cnae_fiscal": 6201501,
        "cnae_fiscal_descricao": "Desenvolvimento de programas",
        "natureza_juridica": "Sociedade Empresaria Limitada",
        "capital_social": 10000.0,
        "porte": "ME",
        "uf": "SP",
        "municipio": "SAO PAULO",
        "email": "contato@teste.com",
        "data_inicio_atividade": "2020-01-15",
    }

    with patch("httpx.get", return_value=mock_response):
        from shared.brasilapi import fetch_cnpj
        result = fetch_cnpj("11222333000181", timeout=5)

    assert result["razao_social"] == "EMPRESA TESTE LTDA"
    assert result["situacao_cadastral"] == "2"
    assert result["cnae_fiscal"] == "6201501"
    assert result["capital_social"] == "10000.0"


def test_fetch_cnpj_returns_none_on_404():
    mock_response = MagicMock()
    mock_response.status_code = 404

    with patch("httpx.get", return_value=mock_response):
        from shared.brasilapi import fetch_cnpj
        result = fetch_cnpj("00000000000000", timeout=5)

    assert result is None


def test_fetch_cnpj_returns_none_on_exception():
    with patch("httpx.get", side_effect=Exception("timeout")):
        from shared.brasilapi import fetch_cnpj
        result = fetch_cnpj("11222333000181", timeout=5)

    assert result is None


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
