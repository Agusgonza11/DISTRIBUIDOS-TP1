FROM python:3.11-slim

WORKDIR /app

COPY server/workers/joiner.py .

ENTRYPOINT ["python3", "joiner.py"]
