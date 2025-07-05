# ClimaData/engines/spark/Dockerfile
FROM bitnami/spark:3.5

USER root
COPY . /app

# dependências
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip install --no-cache-dir pandas pyarrow

# --- ajuste crítico ---
# se não existir, cria a linha 'spark' para o UID 1001
RUN getent passwd 1001 || \
    echo 'spark:x:1001:0:Spark user:/home/spark:/usr/sbin/nologin' >> /etc/passwd
# ----------------------

# diretório Ivy gravável
RUN mkdir -p /opt/bitnami/spark/ivy && chmod 777 /opt/bitnami/spark/ivy && \
    echo "spark.jars.ivy=/opt/bitnami/spark/ivy" >> $SPARK_HOME/conf/spark-defaults.conf

WORKDIR /app
USER 1001
