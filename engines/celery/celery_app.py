from celery import Celery

celery_app = Celery(
    'climadata',
    broker='amqp://guest:guest@broker:5672//',
    backend='rpc://',          # simples para começar
    task_serializer='pickle',
    result_serializer='pickle',
    accept_content=['pickle'],
    worker_prefetch_multiplier=1,
)

