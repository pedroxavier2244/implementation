from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class ChannelOut(BaseModel):
    channel: str
    status: str
    sent_at: Optional[datetime]
    retry_count: int
    error_message: Optional[str]

    model_config = {"from_attributes": True}


class AlertOut(BaseModel):
    id: str
    event_type: str
    severity: str
    message: str
    created_at: datetime
    channels: list[ChannelOut] = []

    model_config = {"from_attributes": True}
