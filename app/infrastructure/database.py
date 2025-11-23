from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import text
from app.infrastructure.config import settings

engine = create_async_engine(settings.DB_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

class MarketDataRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def insert_data(self, ticker: str, df):
        query = text("""
            INSERT INTO market_data (ticker, timestamp, open, high, low, close, volume)
            VALUES (:ticker, :timestamp, :open, :high, :low, :close, :volume)
            ON CONFLICT (ticker, timestamp) DO NOTHING
        """)
        for idx, row in df.iterrows():
            await self.session.execute(query, {
                "ticker": ticker,
                "timestamp": row.name,
                "open": row["Open"],
                "high": row["High"],
                "low": row["Low"],
                "close": row["Close"],
                "volume": row["Volume"]
            })
        await self.session.commit()

async def get_repo():
    async with SessionLocal() as session:
        yield MarketDataRepository(session)
