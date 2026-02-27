from sqlalchemy.orm import Session

from shared.visao_cliente_schema import REQUIRED_COLUMNS
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done
from worker.steps.extract import get_cached_dataframe, set_cached_dataframe


def run_enrich(session: Session, job_id: str) -> None:
    if is_step_done(session, job_id, "enrich"):
        return
    begin_step(session, job_id, "enrich")

    dataframe = get_cached_dataframe(job_id)
    if dataframe is None:
        raise RuntimeError("No dataframe in cache")

    for column in REQUIRED_COLUMNS:
        if column not in dataframe.columns:
            dataframe[column] = None

    # Keep output shape aligned with the target spreadsheet model.
    dataframe = dataframe[REQUIRED_COLUMNS]
    set_cached_dataframe(job_id, dataframe)

    mark_step_done(session, job_id, "enrich")
