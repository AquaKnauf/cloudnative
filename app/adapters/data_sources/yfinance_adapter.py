import yfinance as yf
from app.domain.ports import IMarketDataSource

class YFinanceAdapter(IMarketDataSource):

    async def fetch(self, ticker: str, interval: str, period: str):
        ticker_obj = yf.Ticker(ticker)
        df = ticker_obj.history(interval=interval, period=period)

        df.index.name = "Datetime"
        df = df.rename(columns=str.title)
        return df
