from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class FileOut(BaseModel):
    id: str
    file_date: date
    filename: Optional[str]
    hash_sha256: str
    is_valid: bool
    is_processed: bool
    downloaded_at: Optional[datetime]

    model_config = {"from_attributes": True}


class FileListOut(BaseModel):
    items: list[FileOut]
    total: int
    limit: int
    offset: int
