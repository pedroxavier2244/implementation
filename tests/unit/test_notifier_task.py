from unittest.mock import MagicMock, patch


def test_dispatch_skips_duplicate_event():
    with patch("notifier.tasks.get_db_session") as mock_db, patch(
        "notifier.tasks.build_dedup_key", return_value="job:abc:ETL_DEAD"
    ):
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        from shared.models import AlertEvent

        existing = AlertEvent(id="existing-1", dedup_key="job:abc:ETL_DEAD", event_type="ETL_DEAD", severity="CRITICAL")
        mock_session.query().filter_by().first.return_value = existing

        from notifier.tasks import dispatch_notification

        result = dispatch_notification.__wrapped__(
            MagicMock(),
            event_type="ETL_DEAD",
            severity="CRITICAL",
            message="msg",
            metadata={"job_id": "abc"},
        )
        assert result is None


def test_dispatch_creates_event_for_new_alert():
    with patch("notifier.tasks.get_db_session") as mock_db, patch(
        "notifier.tasks.build_dedup_key", return_value="file_date:2026:FILE_MISSING"
    ), patch("notifier.tasks.retry_channel") as mock_retry, patch(
        "notifier.tasks._get_active_strategies", return_value=[("flag_file", MagicMock())]
    ):
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.query().filter_by().first.return_value = None

        from notifier.tasks import dispatch_notification

        dispatch_notification.__wrapped__(
            MagicMock(),
            event_type="FILE_MISSING",
            severity="CRITICAL",
            message="missing",
            metadata={"file_date": "2026-02-27"},
        )
        mock_session.add.assert_called()
        mock_retry.apply_async.assert_called()
