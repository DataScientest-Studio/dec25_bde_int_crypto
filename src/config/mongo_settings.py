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


class MongoSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    mongodb_uri: str = Field(validation_alias="MONGODB_URI")
    mongodb_database: str = Field(validation_alias="MONGODB_DATABASE")
    mongodb_collection_historical: str = Field(validation_alias="MONGODB_COLLECTION_HISTORICAL")
    mongodb_collection_streaming: str = Field(validation_alias="MONGODB_COLLECTION_STREAMING")


@lru_cache(maxsize=1)
def get_settings() -> MongoSettings:
    # Centralized accessor prevents accidental import-time instantiation elsewhere.
    try:
        return MongoSettings()
    except Exception as e:
        # Provide a clearer, actionable error than the default ValidationError alone.
        # This typically fails when .env wasn't found due to CWD differences.
        searched_from = str(Path(__file__).resolve())
        env_path = str(_ENV_FILE) if _ENV_FILE else "<not found>"
        raise RuntimeError(
            "Failed to load MongoSettings. Missing required environment variables.\n"
            f"Looked for .env by walking up from: {searched_from}\n"
            f"Resolved env_file: {env_path}\n"
            "Expected variables: MONGODB_URI, MONGODB_DATABASE, "
            "MONGODB_COLLECTION_HISTORICAL, MONGODB_COLLECTION_STREAMING"
        ) from e
