from notifier.dedup import build_dedup_key


def test_dedup_key_with_job_id():
    key = build_dedup_key("ETL_DEAD", {"job_id": "abc-123"})
    assert key == "job:abc-123:ETL_DEAD"


def test_dedup_key_file_missing():
    key = build_dedup_key("FILE_MISSING", {"file_date": "2026-02-27"})
    assert key == "file_date:2026-02-27:FILE_MISSING"


def test_dedup_key_schema_error_with_version():
    key = build_dedup_key("SCHEMA_ERROR", {"file_date": "2026-02-27", "schema_version": "v2"})
    assert key == "file_date:2026-02-27:SCHEMA_ERROR:v2"


def test_dedup_key_hash_repeat():
    key = build_dedup_key("HASH_REPEAT", {"file_date": "2026-02-27"})
    assert key == "file_date:2026-02-27:HASH_REPEAT"
