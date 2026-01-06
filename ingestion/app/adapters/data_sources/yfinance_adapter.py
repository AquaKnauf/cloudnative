import yfinance as yf
from app.domain.ports import IMarketDataSource

class YFinanceAdapter(IMarketDataSource):

    async def fetch(self, series_id: str, interval: str, period: str):
        ticker_obj = yf.Ticker(series_id)
        df = ticker_obj.history(interval=interval, period=period)

        df.index.name = "Datetime"
        df = df.rename(columns=str.title)
        return df
