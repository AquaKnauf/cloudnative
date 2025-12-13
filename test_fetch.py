import asyncio
from app.adapters.data_sources.yfinance_adapter import YFinanceAdapter
from app.domain.services import MarketIngestionService
from app.infrastructure.database import SessionLocal, MarketDataRepository

async def test_full_flow():
    # Test adapter
    print("1. Testing YFinance Adapter...")
    adapter = YFinanceAdapter()
    df = await adapter.fetch("AAPL", "1d", "5d")
    print(f"   ✓ Fetched {len(df)} rows")
    
    # Test service
    print("\n2. Testing Service...")
    async with SessionLocal() as session:
        repo = MarketDataRepository(session)
        service = MarketIngestionService(adapter, repo)
        
        points = await service.get_online_data("AAPL", "1d", "5d")
        print(f"   ✓ Created {len(points)} TimeSeriesPoint objects")
        
        # Test insert
        print("\n3. Testing Database Insert...")
        inserted = await service.fetch_and_store("AAPL", "1d", "5d")
        print(f"   ✓ Inserted {inserted} rows")

if __name__ == "__main__":
    asyncio.run(test_full_flow())