from unittest.mock import MagicMock, patch


def test_run_etl_marks_job_done_on_success():
    with patch("worker.tasks.get_db_session") as mock_db, patch("worker.tasks.run_extract") as mock_extract, patch(
        "worker.tasks.run_validate"
    ) as mock_validate, patch("worker.tasks.run_clean") as mock_clean, patch("worker.tasks.run_enrich") as mock_enrich, patch(
        "worker.tasks.run_stage"
    ) as mock_stage, patch("worker.tasks.run_upsert") as mock_upsert, patch(
        "worker.tasks_cnpj.run_cnpj_verify_async"
    ) as mock_cnpj_verify:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        from shared.models import EtlFile, EtlJobRun

        mock_job = EtlJobRun(id="job-1", file_id="file-1", status="QUEUED", triggered_by="scheduler", max_retries=3)
        mock_file = EtlFile(id="file-1", minio_path="2026/02/27/f.xlsx", hash_sha256="h", file_date=None)
        mock_session.query().filter_by().first.side_effect = [mock_job, mock_file]

        from worker.tasks import run_etl

        run_etl.__wrapped__.__func__(run_etl, job_id="job-1", file_id=None)

        assert mock_job.status == "DONE"
        mock_extract.assert_called_once()
        mock_validate.assert_called_once()
        mock_clean.assert_called_once()
        mock_enrich.assert_called_once()
        mock_stage.assert_called_once()
        mock_upsert.assert_called_once()
        mock_cnpj_verify.apply_async.assert_called_once()


def test_exponential_backoff_delays():
    from worker.tasks import compute_retry_delay

    assert compute_retry_delay(0) == 300
    assert compute_retry_delay(1) == 600
    assert compute_retry_delay(2) == 1200


def test_run_etl_rolls_back_session_before_marking_failure():
    with patch("worker.tasks.get_db_session") as mock_db, patch("worker.tasks.run_extract") as mock_extract, patch(
        "worker.tasks.run_clean"
    ) as mock_clean, patch("worker.tasks.run_enrich") as mock_enrich, patch(
        "worker.tasks.run_validate"
    ) as mock_validate, patch("worker.tasks.run_stage") as mock_stage, patch(
        "worker.tasks.run_upsert", side_effect=RuntimeError("upsert failed")
    ), patch(
        "worker.tasks_cnpj.run_cnpj_verify_async"
    ), patch("worker.tasks.mark_step_failed") as mock_mark_step_failed:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        from shared.models import EtlFile, EtlJobRun

        mock_job = EtlJobRun(id="job-1", file_id="file-1", status="QUEUED", triggered_by="scheduler", max_retries=3)
        mock_file = EtlFile(id="file-1", minio_path="2026/02/27/f.xlsx", hash_sha256="h", file_date=None)
        mock_session.query().filter_by().first.side_effect = [mock_job, mock_file]

        from worker.tasks import run_etl

        task_self = MagicMock()
        task_self.request = MagicMock(retries=0)
        task_self.retry.side_effect = RuntimeError("retry called")

        try:
            run_etl.__wrapped__.__func__(task_self, job_id="job-1", file_id=None)
        except RuntimeError as exc:
            assert str(exc) == "retry called"

        mock_extract.assert_called_once()
        mock_clean.assert_called_once()
        mock_enrich.assert_called_once()
        mock_validate.assert_called_once()
        mock_stage.assert_called_once()
        mock_session.rollback.assert_called_once()
        mock_mark_step_failed.assert_called_once_with(mock_session, "job-1", "upsert", "upsert failed")
