from unittest.mock import MagicMock

from shared.models import EtlJobStep
from worker.steps.checkpoint import is_step_done, mark_step_done


def make_mock_session(existing_step=None):
    session = MagicMock()
    mock_query = MagicMock()
    mock_filter = MagicMock()
    mock_filter.first.return_value = existing_step
    mock_query.filter_by.return_value = mock_filter
    session.query.return_value = mock_query
    return session


def test_is_done_returns_true_when_step_completed():
    done_step = EtlJobStep(status="DONE")
    session = make_mock_session(existing_step=done_step)
    assert is_step_done(session, "job-1", "extract") is True


def test_is_done_returns_false_when_no_step():
    session = make_mock_session(existing_step=None)
    assert is_step_done(session, "job-1", "extract") is False


def test_mark_step_done_updates_existing():
    existing = EtlJobStep(id="s1", job_id="job-1", step_name="extract", status="RUNNING")
    session = make_mock_session(existing_step=existing)
    mark_step_done(session, "job-1", "extract")
    assert existing.status == "DONE"
    assert existing.finished_at is not None
    session.flush.assert_called_once()
