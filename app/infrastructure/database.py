# app/infrastructure/database.py
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import text
from app.infrastructure.config import settings
from ..domain.models import TimeSeriesPoint

engine = create_async_engine(settings.DB_URL, echo=True)  # Enable echo for debugging
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

class MarketDataRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def insert_data(self, series_id: str, points: list[TimeSeriesPoint]):
        query = text("""
            INSERT INTO time_series_raw (
                series_id, timestamp, open, high, low, close, volume
            ) VALUES (:series_id, :timestamp, :open, :high, :low, :close, :volume)
            ON CONFLICT (series_id, timestamp) DO NOTHING
        """)
        
        # Batch insert for better performance
        values = [
            {
                "series_id": series_id,
                "timestamp": p.timestamp,
                "open": float(p.open),
                "high": float(p.high),
                "low": float(p.low),
                "close": float(p.close),
                "volume": float(p.volume)  # Changed to float to match your schema
            }
            for p in points
        ]
        
        if values:
            await self.session.execute(query, values)
            await self.session.commit()


async def get_repo():
    async with SessionLocal() as session:
        yield MarketDataRepository(session)