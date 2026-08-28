import csv
import hashlib
import json
import os
import time
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
DATASET_PATH = os.getenv("PAYSIM_FILE")
EVENT_DELAY_SECONDS = 1


def generate_event_id(row):
    identity = f"{row['step']}|{row['nameOrig']}|{row['nameDest']}"
    return hashlib.sha256(identity.encode()).hexdigest()


def create_event(row):
    return {
        "event_id": generate_event_id(row),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "step": int(row["step"]),
        "type": row["type"],
        "amount": float(row["amount"]),
        "nameOrig": row["nameOrig"],
        "oldbalanceOrg": float(row["oldbalanceOrg"]),
        "newbalanceOrig": float(row["newbalanceOrig"]),
        "nameDest": row["nameDest"],
        "oldbalanceDest": float(row["oldbalanceDest"]),
        "newbalanceDest": float(row["newbalanceDest"]),
        "isFraud": int(row["isFraud"]),
        "isFlaggedFraud": int(row["isFlaggedFraud"]),
    }


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

with open(DATASET_PATH, "r", newline="") as file:
    reader = csv.DictReader(file)

    for count, row in enumerate(reader, start=1):
        event = create_event(row)

        producer.send(
            KAFKA_TOPIC,
            key=row["nameOrig"],
            value=event,
        )

        print(
            f"Sent event {count}: "
            f"event_id={event['event_id']} "
            f"key={row['nameOrig']} "
            f"step={row['step']}"
        )

        producer.flush()

        if count == 10:
            break

        time.sleep(EVENT_DELAY_SECONDS)

producer.close()

print("Finished streaming 10 PaySim events.")