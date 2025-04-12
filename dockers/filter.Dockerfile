FROM python:3.11-slim

WORKDIR /app

COPY server/workers/filter.py .

ENTRYPOINT ["python3", "filter.py"]
