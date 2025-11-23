# app/infrastructure/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DB_URL: str = "postgresql+asyncpg://finance:finance@localhost:5432/finance_db"

settings = Settings()