# engines/celery/worker_tasks.py
import os
import pandas as pd
from celery import Celery
from engines.multiproc.process_mp import _process_chunk, THRESHOLDS

# --- instância única ---
broker_url     = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@broker:5672//")
result_backend = os.getenv("CELERY_RESULT_BACKEND", "rpc://")

celery = Celery(
    'climadata',
    broker=broker_url,
    backend=result_backend,
    task_serializer='pickle',
    result_serializer='pickle',
    accept_content=['pickle'],
    worker_prefetch_multiplier=1,
)
celery.conf.task_default_queue = "climadata"

@celery.task
def process_chunk_celery(parquet_path: str):
    """Task executada no worker Celery."""
    df = pd.read_parquet(parquet_path)
    anom, counts, normals_df = _process_chunk((df, THRESHOLDS))
    buf = normals_df.to_parquet(index=False)          # serializa pra bytes
    return {"anom": anom, "counts": counts, "normals": buf}
