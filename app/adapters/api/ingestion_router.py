from fastapi import APIRouter, Depends
import uuid  # Add this import
from app.domain.services import MarketIngestionService
from app.adapters.data_sources.yfinance_adapter import YFinanceAdapter
from app.infrastructure.database import get_repo
from app.infrastructure.kafka import send_completion_event
from shared import SimpleJobTracker

router = APIRouter(prefix="/yahoo")

@router.get("/{series_id}/fetch_store")
async def ingest_yahoo(
    series_id: str,
    interval: str = "5m",
    period: str = "5d",
    repo=Depends(get_repo)
):
    job_id = str(uuid.uuid4())
    preprocessing_config = {"interval": interval, "period": period}

    SimpleJobTracker.update_status(
                job_id=job_id,
                series_id=series_id,
                status='running',
                stage='ingestion'
            )
    
    svc = MarketIngestionService(YFinanceAdapter(), repo)
    
    inserted = await svc.fetch_and_store(series_id, interval, period)

    SimpleJobTracker.update_status(
                job_id=job_id,
                series_id=series_id,
                status='completed',
                stage='ingestion'
            )   
    # Send Kafka event with error handling
    try:
        await send_completion_event({
            'series_id': series_id,
            'job_id': job_id,
            'preprocessing_config': preprocessing_config
        })
    except Exception as e:

        SimpleJobTracker.update_status(
                job_id=job_id,
                series_id=series_id,
                status='failed',
                stage='ingestion'
            )
        
        print(f"Kafka send failed: {e}")
    
    return {"series_id": series_id, "job_id": job_id, "inserted_rows": inserted}

