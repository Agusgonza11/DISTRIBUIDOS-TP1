FROM python:3.11-slim

WORKDIR /app

COPY server/workers/pnl.py .

ENTRYPOINT ["python3", "pnl.py"]
