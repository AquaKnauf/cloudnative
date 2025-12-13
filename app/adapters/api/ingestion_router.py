from fastapi import APIRouter, Depends
from app.domain.services import MarketIngestionService
from app.adapters.data_sources.yfinance_adapter import YFinanceAdapter
from app.infrastructure.database import get_repo

router = APIRouter(prefix="/yahoo")

@router.get("/{series_id}/fetch_store")
async def ingest_yahoo(
    series_id: str,
    interval: str = "1h",
    period: str = "1mo",
    repo=Depends(get_repo)
):
    svc = MarketIngestionService(YFinanceAdapter(), repo)
    inserted = await svc.fetch_and_store(series_id, interval, period)
    return {"series_id": series_id, "inserted_rows": inserted}
