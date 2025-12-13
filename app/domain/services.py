import logging
from .ports import IMarketDataSource, IMarketIngestionService
from .models import TimeSeriesPoint
import pandas as pd

logger = logging.getLogger("market_ingestion")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

class MarketIngestionService(IMarketIngestionService):

    def __init__(self, data_source: IMarketDataSource, repo):
        self.data_source = data_source
        self.repo = repo

    async def get_online_data(self, series_id: str, interval: str, period: str):
        logger.info("Fetching online data for %s interval=%s period=%s", series_id, interval, period)
        df = await self.data_source.fetch(series_id, interval, period)
        df = df.dropna().reset_index()

        ts_col = None
        if "Datetime" in df.columns:
            df["Datetime"] = pd.to_datetime(df["Datetime"], utc=True)
            ts_col = "Datetime"
        elif "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], utc=True)
            ts_col = "Date"
        else:
            raise ValueError("No datetime column found in fetched data.")

        points = [
            TimeSeriesPoint(
                timestamp=row[ts_col],
                open=row["Open"],
                high=row["High"],
                low=row["Low"],
                close=row["Close"],
                volume=row["Volume"]
            )
            for _, row in df.iterrows()
        ]
        logger.info("Fetched %d points for %s", len(points), series_id)
        return points

    async def fetch_and_store(self, series_id: str, interval: str, period: str):
        points = await self.get_online_data(series_id, interval, period)
        if points:
            logger.info("Inserting %d points into DB for %s", len(points), series_id)
            await self.repo.insert_data(series_id, points)
        return len(points)
