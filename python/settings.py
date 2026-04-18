"""Application settings loaded from environment variables with validation."""

from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Repository root (parent of ``python/``); shared ``data/`` lives here for multi-language layouts.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_PYTHON_DIR = Path(__file__).resolve().parent


class Settings(BaseSettings):
    """Base settings: external configuration via OS env / `.env` file."""

    model_config = SettingsConfigDict(
        env_file=(_REPO_ROOT / ".env", _PYTHON_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    port: int = Field(default=5000, ge=1, le=65535)
    flask_debug: bool = Field(default=False)
    host: str = Field(default="127.0.0.1")
    data_dir: Path = Field(
        default=Path("data"),
        description="Directory for CSV inputs and analytics_results.json (relative paths are under the repository root).",
    )

    @field_validator("data_dir", mode="before")
    @classmethod
    def resolve_data_dir(cls, value: object) -> Path:
        p = Path(value) if not isinstance(value, Path) else value
        if not p.is_absolute():
            return (_REPO_ROOT / p).resolve()
        return p.resolve()

    @field_validator("flask_debug", mode="before")
    @classmethod
    def parse_bool_env(cls, value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "on")
        return bool(value)


settings = Settings()
