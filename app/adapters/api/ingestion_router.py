from fastapi import APIRouter, Depends
from app.domain.services import MarketIngestionService
from app.adapters.data_sources.yfinance_adapter import YFinanceAdapter
from app.infrastructure.database import get_repo

router = APIRouter(prefix="/yahoo")

@router.get("/{ticker}/fetch_store")
async def ingest_yahoo(
    ticker: str,
    interval: str = "1h",
    period: str = "1mo",
    repo=Depends(get_repo)
):
    svc = MarketIngestionService(YFinanceAdapter(), repo)
    inserted = await svc.fetch_and_store(ticker, interval, period)
    return {"ticker": ticker, "inserted_rows": inserted}
