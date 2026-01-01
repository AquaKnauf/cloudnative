from kafka import KafkaProducer
import json
from app.infrastructure.config import settings
from typing import Dict, Any
import asyncio
from functools import partial

producer = KafkaProducer(
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS.split(','),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=3,
    acks='all',  # Wait for all replicas
    max_block_ms=5000  # Timeout if broker unreachable
)

async def send_completion_event(event_data: Dict[str, Any]):
    """Send completion message to data.ingestion.completed topic."""
    message = {
        'series_id': event_data.get('series_id'),
        'job_id': event_data.get('job_id'),
        'preprocessing_config': event_data.get('preprocessing_config', {})
    }
    
    # Run blocking operations in thread pool
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, partial(
        producer.send, 'data.ingestion.completed', value=message
    ))
    await loop.run_in_executor(None, producer.flush)
