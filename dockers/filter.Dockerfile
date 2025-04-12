FROM python:3.11-slim

COPY server /app

WORKDIR /app

ENV PYTHONPATH=/app

ENTRYPOINT ["python3", "workers/filter.py"]
