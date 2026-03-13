from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class JobRunRequest(BaseModel):
    file_id: str


class JobRunResponse(BaseModel):
    job_id: str
    status: str


class StepOut(BaseModel):
    step_name: str
    status: str
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    error_message: Optional[str]

    model_config = {"from_attributes": True}


class JobOut(BaseModel):
    id: str
    file_id: Optional[str] = None
    status: str
    triggered_by: str
    rows_total: Optional[int]
    rows_ok: Optional[int]
    rows_bad: Optional[int]
    retry_count: int
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    steps: list[StepOut] = []

    model_config = {"from_attributes": True}
