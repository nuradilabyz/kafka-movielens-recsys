FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
        aiokafka==0.11.0 \
        numpy==1.26.4 \
        psycopg2-binary==2.9.9 \
        qdrant-client==1.11.0

COPY main.py ./main.py

CMD ["python", "main.py"]
