from unittest.mock import MagicMock, patch

import shutil
import uuid
from pathlib import Path


def test_flag_file_creates_file():
    from notifier.strategies.flag_file import FlagFileStrategy

    base_dir = Path(".tmp_test_strategies") / str(uuid.uuid4())
    base_dir.mkdir(parents=True, exist_ok=True)
    try:
        strategy = FlagFileStrategy(flag_dir=str(base_dir))
        strategy.send(
            event_type="FILE_MISSING",
            severity="CRITICAL",
            message="File missing",
            metadata={"file_date": "2026-02-27"},
        )

        files = list(base_dir.glob("*.flag"))
        assert len(files) == 1
        assert "CRITICAL" in files[0].name
    finally:
        if base_dir.exists():
            shutil.rmtree(base_dir)


def test_telegram_send_calls_api():
    with patch("notifier.strategies.telegram.httpx") as mock_httpx:
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_httpx.post.return_value = mock_response

        from notifier.strategies.telegram import TelegramStrategy

        strategy = TelegramStrategy(bot_token="TOKEN", chat_id="123")
        strategy.send(
            event_type="ETL_DEAD",
            severity="CRITICAL",
            message="Job failed",
            metadata={},
        )
        mock_httpx.post.assert_called_once()
        call_url = mock_httpx.post.call_args[0][0]
        assert "TOKEN" in call_url
