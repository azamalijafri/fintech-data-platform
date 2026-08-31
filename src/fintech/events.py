"""Shared event envelope construction for the ingest layer.

The producer turns a raw PaySim row into a structured event envelope. Keeping
the envelope construction in the shared package guarantees the producer,
consumer, and any downstream consumers agree on the schema.
"""

import hashlib
from datetime import datetime, timezone
from typing import Any, Mapping

EVENT_VERSION = 1

# Source fields copied verbatim from the PaySim dataset row.
PAYSIM_FIELDS = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
]


def _event_id(row: Mapping[str, Any]) -> str:
    """Deterministic identifier derived from source attributes.

    PaySim has no native transaction id, so we derive a stable key from the
    columns that uniquely identify a row. This is used downstream for
    deduplication / idempotency.
    """
    identity = f"{row['step']}|{row['nameOrig']}|{row['nameDest']}"
    return hashlib.sha256(identity.encode()).hexdigest()


def build_event(row: Mapping[str, Any]) -> dict[str, Any]:
    """Build a canonical transaction event from a PaySim row."""
    event: dict[str, Any] = {
        "event_id": _event_id(row),
        "event_version": EVENT_VERSION,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    for field in PAYSIM_FIELDS:
        event[field] = row[field]
    return event
