from datetime import date
from unittest.mock import MagicMock

import pytest

from checker.checker import (
    _extract_drive_confirm_token,
    _extract_drive_download_form,
    _looks_like_html,
    _validate_downloaded_content,
    compute_sha256,
    is_hash_duplicate,
)
from shared.models import EtlFile


def test_compute_hash_is_deterministic():
    data = b"hello world"
    assert compute_sha256(data) == compute_sha256(data)
    assert len(compute_sha256(data)) == 64


def test_is_hash_duplicate_returns_true_when_exists():
    existing = EtlFile(hash_sha256="abc", file_date=date(2026, 2, 27))
    session = MagicMock()
    session.query().filter_by().first.return_value = existing
    assert is_hash_duplicate(session, "abc", date(2026, 2, 27)) is True


def test_is_hash_duplicate_returns_false_when_new():
    session = MagicMock()
    session.query().filter_by().first.return_value = None
    assert is_hash_duplicate(session, "newhash", date(2026, 2, 27)) is False


def test_extract_drive_confirm_token_from_hidden_input():
    page = '<form><input type="hidden" name="confirm" value="t1234"></form>'
    assert _extract_drive_confirm_token(page) == "t1234"


def test_extract_drive_confirm_token_from_query_string():
    page = '<a href="/uc?export=download&confirm=abc123&id=file123">download</a>'
    assert _extract_drive_confirm_token(page) == "abc123"


def test_extract_drive_download_form():
    page = (
        '<form id="download-form" action="https://drive.usercontent.google.com/download">'
        '<input type="hidden" name="id" value="file123">'
        '<input type="hidden" name="confirm" value="token123">'
        "</form>"
    )
    action, params = _extract_drive_download_form(page)
    assert action == "https://drive.usercontent.google.com/download"
    assert params["id"] == "file123"
    assert params["confirm"] == "token123"


def test_looks_like_html_detects_html_and_not_binary():
    assert _looks_like_html(b"<!DOCTYPE html><html></html>") is True
    assert _looks_like_html(b"PK\x03\x04binary-xlsx") is False


def test_validate_downloaded_content_rejects_html():
    with pytest.raises(RuntimeError, match="HTML"):
        _validate_downloaded_content(b"<html>blocked</html>", "arquivo.xlsx")


def test_validate_downloaded_content_rejects_invalid_xlsx():
    with pytest.raises(RuntimeError, match="valid XLSX"):
        _validate_downloaded_content(b"plain text", "arquivo.xlsx")
