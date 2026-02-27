import shutil
import uuid
from pathlib import Path

from local_watcher.watcher import parse_flag_filename, process_flag_file


def test_watcher_detects_flag_file():
    base_dir = Path(".tmp_test_watcher") / str(uuid.uuid4())
    base_dir.mkdir(parents=True, exist_ok=True)
    try:
        flag = base_dir / "20260227_120000_CRITICAL_FILE_MISSING.flag"
        flag.write_text("CRITICAL|FILE_MISSING|File not found\n{}", encoding="utf-8")

        result = process_flag_file(str(flag))
        assert result["severity"] == "CRITICAL"
        assert result["event_type"] == "FILE_MISSING"
    finally:
        if base_dir.exists():
            shutil.rmtree(base_dir)


def test_parse_flag_filename():
    result = parse_flag_filename("20260227_120000_CRITICAL_ETL_DEAD.flag")
    assert result["severity"] == "CRITICAL"
    assert result["event_type"] == "ETL_DEAD"
