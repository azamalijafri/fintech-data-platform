FROM python:3.14-slim

WORKDIR /app

COPY services/ingest/consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir --no-deps .

COPY services/ingest/consumer/consumer.py .

CMD ["python", "-u", "consumer.py"]
