from .ports import IMarketDataSource, IMarketIngestionService
from .models import TimeSeriesPoint
from sqlalchemy import text
import pandas as pd

class MarketIngestionService(IMarketIngestionService):

    def __init__(self, data_source: IMarketDataSource, repo):
        self.data_source = data_source
        self.repo = repo  # repo must have async insert_data(ticker, df) method

    async def get_online_data(self, ticker: str, interval: str, period: str):
        """
        Fetch online data, preprocess, convert to TimeSeriesPoint objects.
        Returns list of points (optional, e.g., for API response)
        """
        df = await self.data_source.fetch(ticker, interval, period)
        df = df.dropna().reset_index()

        # Ensure datetime index
        if "Datetime" in df.columns:
            df["Datetime"] = pd.to_datetime(df["Datetime"])
        elif "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
        else:
            raise ValueError("No datetime column found in fetched data.")

        # Convert to list of TimeSeriesPoint
        points = []
        for _, row in df.iterrows():
            points.append(TimeSeriesPoint(
                timestamp=row["Datetime"] if "Datetime" in df.columns else row["Date"],
                open=row["Open"],
                high=row["High"],
                low=row["Low"],
                close=row["Close"],
                volume=row["Volume"]
            ))

        return points

    async def fetch_and_store(self, ticker: str, interval: str, period: str):
        """
        Fetch online data and store it in DB (TimescaleDB/PostgreSQL).
        Returns number of inserted rows.
        """
        df = await self.data_source.fetch(ticker, interval, period)
        df = df.dropna().reset_index()
        # Ensure datetime index
        if "Datetime" in df.columns:
            df["Datetime"] = pd.to_datetime(df["Datetime"])
            df.set_index("Datetime", inplace=True)
        elif "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df.set_index("Date", inplace=True)
        else:
            raise ValueError("No datetime column found in fetched data.")

        # Insert into DB via repo
        await self.repo.insert_data(ticker, df)
        return len(df)

