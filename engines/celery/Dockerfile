FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONPATH=/app

COPY weather                /app/weather
COPY engines/multiproc      /app/engines/multiproc
COPY engines/celery         /app/engines/celery

RUN pip install --no-cache-dir \
      pandas \
      pyarrow \
      numpy \
      celery==5.3.* \
      kombu==5.3.* \
      rabbitmq-admin==0.2

ENV PYTHONUNBUFFERED=1
ENV C_FORCE_ROOT=1

CMD sh -c 'celery -A engines.celery.worker_tasks worker --loglevel=info --concurrency=${WORKER_CONCURRENCY}'
