# tests/unit/test_cnpj_api.py
from unittest.mock import patch, MagicMock


def test_fetch_cnae_code_returns_formatted_code_for_valid_cnpj():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"cnae_fiscal": 4711301}
    with patch("shared.cnpj_api.requests.get", return_value=mock_resp):
        from shared.cnpj_api import fetch_cnae_code
        result = fetch_cnae_code("12345678000195")
    assert result == "4711-3"


def test_fetch_cnae_code_zero_padded_cnae():
    """CNAE que comeca com zero: cnae_fiscal=162800 -> '0162-8'"""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"cnae_fiscal": 162800}
    with patch("shared.cnpj_api.requests.get", return_value=mock_resp):
        from shared.cnpj_api import fetch_cnae_code
        result = fetch_cnae_code("12345678000195")
    assert result == "0162-8"


def test_fetch_cnae_code_returns_none_for_404():
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    with patch("shared.cnpj_api.requests.get", return_value=mock_resp):
        from shared.cnpj_api import fetch_cnae_code
        result = fetch_cnae_code("00000000000000")
    assert result is None


def test_fetch_cnae_code_returns_none_on_network_error():
    with patch("shared.cnpj_api.requests.get", side_effect=Exception("timeout")):
        from shared.cnpj_api import fetch_cnae_code
        result = fetch_cnae_code("12345678000195")
    assert result is None


def test_fetch_cnae_code_skips_cpf_11_digits():
    with patch("shared.cnpj_api.requests.get") as mock_get:
        from shared.cnpj_api import fetch_cnae_code
        result = fetch_cnae_code("12345678901")
    mock_get.assert_not_called()
    assert result is None


def test_fetch_cnae_code_strips_formatting_from_cnpj():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"cnae_fiscal": 4711301}
    with patch("shared.cnpj_api.requests.get", return_value=mock_resp) as mock_get:
        from shared.cnpj_api import fetch_cnae_code
        fetch_cnae_code("12.345.678/0001-95")
    called_url = mock_get.call_args[0][0]
    assert "12345678000195" in called_url


def test_fetch_cnae_code_returns_none_when_cnae_fiscal_missing():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {}
    with patch("shared.cnpj_api.requests.get", return_value=mock_resp):
        from shared.cnpj_api import fetch_cnae_code
        result = fetch_cnae_code("12345678000195")
    assert result is None
