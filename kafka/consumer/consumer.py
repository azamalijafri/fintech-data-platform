import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP")

S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")

BATCH_SIZE = 100
BATCH_TIMEOUT_SECONDS = 30


s3 = boto3.client("s3")


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=CONSUMER_GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    key_deserializer=lambda key: key.decode("utf-8") if key else None,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)


def upload_batch(messages):
    batch_id = uuid.uuid4().hex

    now = datetime.now(timezone.utc)

    s3_key = (
        f"{S3_PREFIX}/"
        f"{now:%Y/%m/%d/%H}/"
        f"batch-{batch_id}.jsonl"
    )

    lines = []

    for message in messages:
        lines.append(json.dumps(message.value))

    body = "\n".join(lines) + "\n"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    return s3_key


def commit_batch(messages):
    offsets = {}

    for message in messages:
        offsets[message.partition] = max(
            offsets.get(message.partition, -1),
            message.offset,
        )

    commit_offsets = {}

    for partition, offset in offsets.items():
        commit_offsets[partition] = OffsetAndMetadata(offset + 1, None)

    consumer.commit(
        offsets={
            message.topic_partition: metadata
            for message, metadata in []
        }
    )


print("Consumer started. Waiting for messages...")

batch = []
batch_started_at = None


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

        if batch:

            elapsed = time.monotonic() - batch_started_at

            if (
                len(batch) >= BATCH_SIZE
                or elapsed >= BATCH_TIMEOUT_SECONDS
            ):

                print(
                    f"Flushing batch: "
                    f"events={len(batch)} "
                    f"age={elapsed:.1f}s"
                )

                s3_key = upload_batch(batch)

                print(
                    f"S3 upload successful: "
                    f"events={len(batch)} "
                    f"s3_key={s3_key}"
                )

                offsets = {}

                for message in batch:
                    topic_partition = TopicPartition(
                        message.topic,
                        message.partition,
                    )

                    offsets[topic_partition] = max(
                        offsets.get(topic_partition, -1),
                        message.offset,
                    )

                commit_offsets = {
                    topic_partition: OffsetAndMetadata(
                        offset + 1,
                        None,
                    )
                    for topic_partition, offset in offsets.items()
                }

                consumer.commit(
                    offsets=commit_offsets
                )

                for topic_partition, metadata in commit_offsets.items():
                    print(
                        f"Committed offset: "
                        f"partition={topic_partition.partition} "
                        f"next_offset={metadata.offset}"
                    )

                batch.clear()
                batch_started_at = None

except KeyboardInterrupt:

    print("\nConsumer stopped.")

finally:

    consumer.close()