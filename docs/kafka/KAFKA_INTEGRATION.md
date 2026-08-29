# Kafka Integration

## Overview

Kafka is the event transport layer between the PaySim transaction producer and the downstream consumer. The producer publishes transaction events; the consumer reads them and persists them to the raw landing layer.

> **Key distinction:** Kafka owns transport and delivery — it guarantees a message _can_ be delivered, redelivered, and retained. The consumer owns the decision of when an event counts as successfully processed, by controlling offset commits. **Idempotency is what bridges the gap** between "persisted downstream" and "acknowledged to Kafka" — those two things are not atomic, so duplicate delivery is expected behavior, not a bug.

| Layer     | Component                                                     |
| --------- | ------------------------------------------------------------- |
| Transport | Kafka broker, `financial-transactions` topic (3 partitions)   |
| Producer  | Python process reading PaySim rows, simulating stream arrival |
| Consumer  | Python process in a consumer group, persisting to raw landing |
| Runtime   | Docker, all services on the same network                      |

---

## How it fits together

```
PaySim CSV (static dataset)
        │
        ▼
Producer  ──publish(key=nameOrig)──▶  Kafka topic: financial-transactions
                                            (3 partitions)
                                                  │
                                                  ▼
                                     Consumer (consumer group:
                                     financial-transactions-consumer)
                                                  │
                                     persist to raw landing layer
                                                  │
                                     ONLY ON SUCCESS ──▶ commit offset
```

Partitioning uses `nameOrig` as the message key, so all transactions for a given origin account land on the same partition and preserve order relative to each other. There's no ordering guarantee _across_ partitions/accounts.

---

## 1. Kafka Deployment

Kafka runs locally via Docker, as an independent long-running service — it doesn't depend on Airflow or any orchestrator for its own operation. Producer and consumer are separate long-running processes that connect to Kafka over the Docker network using the service hostname, not a host-specific address.

```env
KAFKA_BOOTSTRAP_SERVERS=<KAFKA_SERVICE>:<KAFKA_PORT>
KAFKA_TOPIC=financial-transactions
KAFKA_CONSUMER_GROUP=financial-transactions-consumer
```

Environment-specific connection values live in config, not in application code.

---

## 2. Topic

| Property      | Value                    |
| ------------- | ------------------------ |
| Name          | `financial-transactions` |
| Partitions    | 3                        |
| Partition key | `nameOrig`               |

The topic is the boundary between producer and consumer — producer writes, consumer reads. Kafka distributes records across partitions by key, so ordering is preserved **per key**, not globally.

---

## 3. Producer

Reads PaySim records, builds the event payload, and publishes to `financial-transactions` keyed on `nameOrig`.

```python
producer.produce(
    topic="financial-transactions",
    key=transaction["nameOrig"],
    value=message
)
```

Responsibilities:

- Read PaySim records
- Build the event payload (source attributes + platform metadata)
- Key by `nameOrig`
- Simulate stream arrival timing (PaySim is static, not a live feed)
- Handle producer delivery results

### Event payload

Two distinct layers in every message:

**PaySim source attributes** (as-is from the dataset):
`step`, `type`, `amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`, `nameDest`, `oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`

**Platform metadata** (generated, not from PaySim — kept separate from source fields):

| Field           | Purpose                                                                                                                                                       |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `event_id`      | Deterministic technical identifier (PaySim has no native transaction ID); used downstream for dedup/idempotency — never described as an original PaySim field |
| `event_time`    | Simulated event timestamp, independent of when the producer physically reads the row — this is what makes a static CSV behave like a live stream              |
| `ingested_at`   | When the platform ingested the record                                                                                                                         |
| `event_version` | Structure version of the event envelope (starts at `1`); tracks changes to the envelope shape independently of changes to the underlying PaySim attributes    |

### Streaming simulation

The producer doesn't dump the CSV into Kafka in one shot — it replays rows at a controlled rate to make the pipeline behave like it's consuming a live stream. Configurable behavior includes:

- `events_per_second`
- starting position
- event timestamp behavior
- replay behavior
- duplicate generation (for testing downstream idempotency deliberately)

---

## 4. Consumer

Subscribes to `financial-transactions` as part of consumer group `financial-transactions-consumer`, and continuously polls for new events.

Per message:

1. Receive the Kafka message
2. Deserialize the payload
3. Validate structure
4. Persist to the raw landing layer
5. **Commit the offset only after persistence succeeds**

```
Receive → Persist (success) → Commit offset
Receive → Persist (fails)   → Offset NOT committed → Kafka redelivers
```

This ordering is the entire reliability model: Kafka only considers a message "done" when the consumer says so, and the consumer only says so after the write actually lands.

---

## 5. Offsets, Restarts, and Duplicate Delivery

Kafka retains messages that haven't been acknowledged via a committed offset. If the consumer crashes before committing:

```
Message A → persisted → offset committed      (safe)
Message B → persisted → CRASH before commit   (at risk)
```

On restart, the consumer resumes from the last **committed** offset — meaning Message B gets redelivered even though it was already persisted once.

**This is expected, not a failure mode.** Duplicate delivery happens whenever a message is persisted successfully but the process fails before the commit — Kafka has no way to know the write already succeeded. Downstream persistence therefore relies on `event_id` to detect and collapse duplicates. This is what separates _Kafka's delivery guarantees_ (at-least-once) from _downstream correctness_ (exactly-once effect via idempotency).

---

## 6. Replay & Retention

Resetting a consumer group's offset lets previously published events be reconsumed — useful for testing consumer recovery, duplicate handling, idempotency, and reprocessing, without needing the producer to regenerate anything.

Retention and offsets are independent concepts:

- **Offset** = where a specific consumer group has progressed
- **Retention** = how long Kafka physically keeps a record, regardless of whether it's been consumed

A message can be fully processed and still sit in the topic for the rest of its retention window.

---

## 7. Producer/Consumer Independence

Producer and consumer are separate processes with no direct lifecycle coupling:

| Failure                                       | Effect                                                                                                |
| --------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Producer stops                                | No new events published; already-published messages remain available per retention                    |
| Consumer stops                                | Kafka retains unconsumed/uncommitted messages; consumer resumes from last committed offset on restart |
| Downstream persistence fails                  | Offset not committed → message redelivered                                                            |
| Consumer crashes after persist, before commit | Message redelivered → duplicate handled via `event_id`                                                |

---

## Verification

**List topics:**

```bash
docker exec <KAFKA_CONTAINER> \
  kafka-topics --bootstrap-server <KAFKA_BOOTSTRAP_SERVERS> --list
```

**Describe the topic (check partition count):**

```bash
docker exec <KAFKA_CONTAINER> \
  kafka-topics --bootstrap-server <KAFKA_BOOTSTRAP_SERVERS> \
  --describe --topic financial-transactions
```

**Tail messages for debugging (not the production path):**

```bash
docker exec -it <KAFKA_CONTAINER> \
  kafka-console-consumer \
  --bootstrap-server <KAFKA_BOOTSTRAP_SERVERS> \
  --topic financial-transactions --from-beginning
```

**Inspect consumer groups:**

```bash
docker exec <KAFKA_CONTAINER> \
  kafka-consumer-groups --bootstrap-server <KAFKA_BOOTSTRAP_SERVERS> --list
```

**Inspect this consumer group's assignments, offsets, and lag:**

```bash
docker exec <KAFKA_CONTAINER> \
  kafka-consumer-groups \
  --bootstrap-server <KAFKA_BOOTSTRAP_SERVERS> \
  --describe --group financial-transactions-consumer
```

---

## Component Responsibilities

| Component                | Responsibility                                                   |
| ------------------------ | ---------------------------------------------------------------- |
| Kafka broker             | Hosts the topic; manages partitions, records, offsets, retention |
| Topic                    | Destination for transaction events                               |
| Partitions               | Distribute events; preserve ordering within a partition          |
| Producer                 | Reads PaySim, builds events, publishes keyed by `nameOrig`       |
| Message key (`nameOrig`) | Determines partition placement / per-key ordering                |
| Consumer group           | Tracks processing position via offsets                           |
| Consumer                 | Reads events, persists downstream, controls offset commit        |
| Offset                   | Consumer group's committed processing position                   |
| `event_id`               | Deterministic identifier enabling downstream dedup/idempotency   |
| Docker                   | Runs Kafka, producer, and consumer as local services             |

---

## Architectural Flow (summary)

1. Producer reads a PaySim record, builds the event, publishes to `financial-transactions` keyed on `nameOrig`.
2. Kafka assigns it to one of 3 partitions based on the key.
3. Consumer (in its consumer group) polls and receives the event.
4. Consumer persists to the raw landing layer.
5. Only on successful persistence does the consumer commit the offset.
6. If persistence or the process fails before commit, Kafka redelivers the message.
7. `event_id` lets downstream processing absorb the redelivery without creating a duplicate logical record.
