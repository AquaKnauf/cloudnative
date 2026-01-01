# app/infrastructure/config.py
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_URL: str = "postgresql+asyncpg://tsuser:ts_passwordingestion-timescaledb:5432/timeseries"
    KAFKA_BOOTSTRAP_SERVERS: str = "kafka:9092"


settings = Settings()
