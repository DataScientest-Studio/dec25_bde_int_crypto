from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_env_file(start: Path) -> Path | None:
    """
    Find the nearest .env by walking up parent dirs from `start`.
    This makes settings loading independent of the current working directory.
    """
    here = start.resolve()
    for parent in (here, *here.parents):
        candidate = parent / ".env"
        if candidate.is_file():
            return candidate
    return None


_ENV_FILE = _find_env_file(Path(__file__).parent)


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    kafka_broker: str = Field(
        default="redpanda-0:9092", validation_alias="KAFKA_BROKER"
    )
    kafka_topic: str = Field(default="binance-klines", validation_alias="KAFKA_TOPIC")


@lru_cache(maxsize=1)
def get_settings() -> KafkaSettings:
    # Centralized accessor prevents accidental import-time instantiation elsewhere.
    try:
        return KafkaSettings()
    except Exception as e:
        # Provide a clearer, actionable error than the default ValidationError alone.
        # This typically fails when .env wasn't found due to CWD differences.
        searched_from = str(Path(__file__).resolve())
        env_path = str(_ENV_FILE) if _ENV_FILE else "<not found>"
        raise RuntimeError(
            "Failed to load KafkaSettings. Missing required environment variables.\n"
            f"Looked for .env by walking up from: {searched_from}\n"
            f"Resolved env_file: {env_path}\n"
            "Expected variables: KAFKA_BROKER, KAFKA_TOPIC"
        ) from e
