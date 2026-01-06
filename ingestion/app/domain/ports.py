from abc import ABC, abstractmethod
from typing import List
from .models import TimeSeriesPoint

class IMarketDataSource(ABC):
    @abstractmethod
    async def fetch(self, ticker: str, interval: str, period: str):
        pass

class IMarketIngestionService(ABC):
    @abstractmethod
    async def get_online_data(self, ticker: str, interval: str, period: str):
        pass
