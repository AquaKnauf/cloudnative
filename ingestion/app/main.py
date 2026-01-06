# app/main.py
from fastapi import FastAPI
from app.adapters.api.ingestion_router import router as yahoo_router

app = FastAPI(title="Direct Yahoo Finance Ingestion API")

app.include_router(yahoo_router)

@app.get("/")
async def root():
    return {"message": "Yahoo Finance API", "version": "1.0"}