# app/infrastructure/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DB_URL: str = "postgresql+asyncpg://tsuser:ts_password@localhost:5432/time_series_raw"

settings = Settings()