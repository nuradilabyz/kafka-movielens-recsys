FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

RUN pip install --no-cache-dir \
        aiokafka==0.11.0 \
        pandas==2.2.2 \
        kafka-python==2.0.2 \
        lz4==4.3.3

COPY main.py ./main.py

CMD ["python", "main.py"]
