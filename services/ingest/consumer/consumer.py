"""Consume transaction events from Kafka and persist them to S3 as JSONL batches."""

import json
import time
import uuid
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

from fintech.config import get_env, kafka_bootstrap_servers, kafka_topic

CONSUMER_GROUP = get_env("KAFKA_CONSUMER_GROUP")
S3_BUCKET = get_env("S3_BUCKET")
S3_PREFIX = get_env("S3_PREFIX")

BATCH_SIZE = int(get_env("BATCH_SIZE", "100"))
BATCH_TIMEOUT_SECONDS = int(get_env("BATCH_TIMEOUT_SECONDS", "30"))


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        kafka_topic(),
        bootstrap_servers=kafka_bootstrap_servers(),
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )


def upload_batch(s3, messages: list) -> str:
    batch_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    s3_key = (
        f"{S3_PREFIX}/"
        f"{now:%Y/%m/%d/%H}/"
        f"batch-{batch_id}.jsonl"
    )

    lines = [json.dumps(message.value) for message in messages]
    body = "\n".join(lines) + "\n"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )
    return s3_key


def commit_batch(consumer: KafkaConsumer, messages: list) -> None:
    offsets: dict = {}
    for message in messages:
        topic_partition = TopicPartition(message.topic, message.partition)
        offsets[topic_partition] = max(
            offsets.get(topic_partition, -1),
            message.offset,
        )

    commit_offsets = {
        topic_partition: OffsetAndMetadata(offset + 1, None)
        for topic_partition, offset in offsets.items()
    }

    consumer.commit(offsets=commit_offsets)

    for topic_partition, metadata in commit_offsets.items():
        print(
            f"Committed offset: "
            f"partition={topic_partition.partition} "
            f"next_offset={metadata.offset}"
        )


def main() -> None:
    s3 = boto3.client("s3")
    consumer = create_consumer()

    print("Consumer started. Waiting for messages...")

    batch: list = []
    batch_started_at: float | None = None

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            for topic_partition, messages in records.items():
                for message in messages:
                    if not batch:
                        batch_started_at = time.monotonic()
                    batch.append(message)
                    print(
                        f"Buffered event: "
                        f"event_id={message.value['event_id']} "
                        f"partition={message.partition} "
                        f"offset={message.offset} "
                        f"batch_size={len(batch)}"
                    )

            if batch and batch_started_at is not None:
                elapsed = time.monotonic() - batch_started_at
                if len(batch) >= BATCH_SIZE or elapsed >= BATCH_TIMEOUT_SECONDS:
                    print(f"Flushing batch: events={len(batch)} age={elapsed:.1f}s")
                    s3_key = upload_batch(s3, batch)
                    print(f"S3 upload successful: events={len(batch)} s3_key={s3_key}")
                    commit_batch(consumer, batch)
                    batch.clear()
                    batch_started_at = None
    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
