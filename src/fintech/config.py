"""Environment-based configuration helpers shared across platform services.

All secrets and environment-specific values are read from environment variables
(loaded from a root ``.env`` file), never hard-coded in application code. This
keeps configuration externalized and consistent across producer, consumer, and
orchestration layers.
"""

import os
from functools import lru_cache
from typing import Any

from dotenv import load_dotenv

load_dotenv()


def get_env(key: str, default: str | None = None) -> str:
    """Return an environment variable, raising if it is missing and required."""
    value = os.getenv(key, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value


@lru_cache
def kafka_bootstrap_servers() -> str:
    return get_env("KAFKA_BOOTSTRAP_SERVERS")


@lru_cache
def kafka_topic() -> str:
    return get_env("KAFKA_TOPIC")
