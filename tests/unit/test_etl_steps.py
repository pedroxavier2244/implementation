from unittest.mock import MagicMock, patch

import pytest

pd = pytest.importorskip("pandas")


def make_session_mock(step_exists=False):
    session = MagicMock()
    from shared.models import EtlJobStep

    existing = EtlJobStep(status="DONE") if step_exists else None
    session.query().filter_by().first.return_value = existing
    return session


def make_file_mock(minio_path="2026/02/27/f.xlsx"):
    file_obj = MagicMock()
    file_obj.minio_path = minio_path
    return file_obj


def test_extract_skips_when_done():
    session = make_session_mock(step_exists=True)
    with patch("worker.steps.extract.MinioClient") as mock_minio:
        from worker.steps.extract import run_extract

        run_extract(session, "job-1", make_file_mock())
        mock_minio.assert_not_called()


def test_validate_aborts_when_threshold_exceeded():
    session = make_session_mock(step_exists=False)
    dataframe = pd.DataFrame({"col1": [None] * 10, "col2": [None] * 10})

    with patch("worker.steps.validate.is_step_done", return_value=False), patch("worker.steps.validate.begin_step"), patch(
        "worker.steps.validate.get_cached_dataframe", return_value=dataframe
    ), patch("worker.steps.validate.get_settings") as mock_settings:
        mock_settings.return_value.BAD_ROW_THRESHOLD_PCT = 5.0
        from worker.steps.validate import run_validate

        with pytest.raises(ValueError, match="threshold"):
            run_validate(session, "job-1", make_file_mock())


def test_clean_skips_when_done():
    with patch("worker.steps.clean.is_step_done", return_value=True):
        from worker.steps.clean import run_clean

        session = MagicMock()
        run_clean(session, "job-1")
