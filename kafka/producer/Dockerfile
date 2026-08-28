FROM python:3.14-slim

WORKDIR /app

COPY kafka/producer/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY kafka/producer/producer.py .

CMD ["python", "producer.py"]
