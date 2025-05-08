FROM python:3.11-slim

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
#RUN pip install --no-cache-dir transformers torch huggingface_hub[hf_xet] #Descomentar


COPY server /app

WORKDIR /app

ENV PYTHONPATH=/app


ENTRYPOINT ["python3", "workers/pnl.py"]
