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


class DataSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    data_dir: str = Field(default="data", validation_alias="DATA_DIR")
    raw_data_dirname: str = Field(
        default="raw_data", validation_alias="RAW_DATA_DIRNAME"
    )
    processed_data_dirname: str = Field(
        default="processed_data", validation_alias="PROCESSED_DATA_DIRNAME"
    )


@lru_cache(maxsize=1)
def get_settings() -> DataSettings:
    # Centralized accessor prevents accidental import-time instantiation elsewhere.
    try:
        return DataSettings()
    except Exception as e:
        # Provide a clearer, actionable error than the default ValidationError alone.
        # This typically fails when .env wasn't found due to CWD differences.
        searched_from = str(Path(__file__).resolve())
        env_path = str(_ENV_FILE) if _ENV_FILE else "<not found>"
        raise RuntimeError(
            "Failed to load DataSettings. Missing required environment variables.\n"
            f"Looked for .env by walking up from: {searched_from}\n"
            f"Resolved env_file: {env_path}\n"
            "Expected variables: DATA_DIR, RAW_DATA_DIRNAME, PROCESSED_DATA_DIRNAME"
        ) from e
