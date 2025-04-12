FROM python:3.11-slim

WORKDIR /app

COPY server/workers/aggregator.py .

ENTRYPOINT ["python3", "aggregator.py"]
