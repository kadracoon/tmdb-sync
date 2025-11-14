from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class FrameReport(BaseModel):
    movie_id: int
    frame_path: str  # пример: "/xYz123.jpg"
    reason: Optional[str] = Field(default="not_a_scene", max_length=200)
    content_type: str = Field(..., pattern="^(movie|tv)$")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
