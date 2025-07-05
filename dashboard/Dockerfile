FROM python:3.12-slim

WORKDIR /app

# instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# -- instale a CLI Docker --
# instala o Docker CLI + plugin do Compose
RUN apt-get update && \
    apt-get install -y \
      docker.io \
      curl \
    && curl -SL https://github.com/docker/compose/releases/download/v2.17.3/docker-compose-linux-x86_64 \
         -o /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose \
    && rm -rf /var/lib/apt/lists/*
# o código chega via bind-mount, mas copiamos o app para rodar mesmo
COPY app.py .

# garante que “engines”, “controller”, etc., sejam importáveis
ENV PYTHONPATH=/app

CMD ["streamlit", "run", "app.py", "--server.port", "8501", "--server.address", "0.0.0.0"]
