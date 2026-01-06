from datetime import datetime
from pydantic import BaseModel

class TimeSeriesPoint(BaseModel):
    timestamp: datetime
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None
