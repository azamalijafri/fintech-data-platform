"""Stream PaySim transaction rows into a Kafka topic as structured events."""

import csv
import json
import time

from kafka import KafkaProducer

from fintech.config import get_env, kafka_bootstrap_servers, kafka_topic
from fintech.events import build_event

EVENT_DELAY_SECONDS = float(get_env("EVENT_DELAY_SECONDS", "1"))
MAX_EVENTS = int(get_env("MAX_EVENTS", "10"))
DATASET_PATH = get_env("PAYSIM_FILE")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers(),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def stream(producer: KafkaProducer, dataset_path: str, max_events: int) -> None:
    with open(dataset_path, "r", newline="") as file:
        reader = csv.DictReader(file)
        for count, row in enumerate(reader, start=1):
            if count > max_events:
                break

            event = build_event(row)
            producer.send(
                kafka_topic(),
                key=row["nameOrig"],
                value=event,
            )
            producer.flush()

            print(
                f"Sent event {count}: "
                f"event_id={event['event_id']} "
                f"key={row['nameOrig']} "
                f"step={row['step']}"
            )
            time.sleep(EVENT_DELAY_SECONDS)


def main() -> None:
    producer = create_producer()
    try:
        stream(producer, DATASET_PATH, MAX_EVENTS)
    finally:
        producer.close()
    print(f"Finished streaming {MAX_EVENTS} PaySim events.")


if __name__ == "__main__":
    main()
