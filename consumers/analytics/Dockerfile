FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
        aiokafka==0.11.0 \
        pandas==2.2.2 \
        psycopg2-binary==2.9.9

COPY sink.py ./sink.py

CMD ["python", "sink.py"]
