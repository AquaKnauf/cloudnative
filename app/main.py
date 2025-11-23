from fastapi import FastAPI
from app.adapters.api.ingestion_router import router as yahoo_router

app = FastAPI(title="Direct Yahoo Finance Ingestion API")
app.include_router(yahoo_router)
