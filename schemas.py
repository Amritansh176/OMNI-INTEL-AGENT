from pydantic import BaseModel, Field
from typing import List, Optional

class JobPayload(BaseModel):
    job_id: str
    pipeline: str
    target: str
    keywords: List[str] = Field(default_factory=list)
    tier_attempts: List[int] = Field(default_factory=list)
    max_depth: int = 1
    retry_count: int = 0
