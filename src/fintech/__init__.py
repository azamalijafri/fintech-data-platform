"""FinTech data platform core package.

Shared configuration and helpers used across the ingest services and plugins.
"""

from fintech.config import (
    get_env,
    kafka_bootstrap_servers,
    kafka_topic,
)

__all__ = [
    "get_env",
    "kafka_bootstrap_servers",
    "kafka_topic",
]
