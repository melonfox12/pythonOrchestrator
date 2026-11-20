"""
Load and validate environment variables for the orchestrator.

Why this exists:
  Every other module needs consistent, typed settings (URLs, caps, secrets).
  Centralizing env parsing avoids scattered os.getenv calls and catches
  misconfiguration at startup before any calls are placed.

Who calls it:
  main.py (lifespan) calls load_config() once at startup and passes Settings
  into loops, stores, and API handlers.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator


class Settings(BaseModel):
    """Validated configuration for the orchestrator process."""

    orchestrator_host: str = Field(default="0.0.0.0", description="Bind host for uvicorn")
    orchestrator_port: int = Field(default=8000, ge=1, le=65535)

    web_app_url: HttpUrl = Field(
        description="Base URL where signed events are POSTed (fake web app in dev)"
    )
    hmac_secret: str = Field(min_length=8, description="Shared secret for HMAC signing")

    global_concurrent_cap: int = Field(default=10, ge=1)
    heartbeat_timeout_secs: int = Field(default=120, ge=10)
    max_call_secs: int = Field(default=1800, ge=60)

    sqlite_path: Path = Field(default=Path("./data/orchestrator.db"))

    number_daily_cap_default: int = Field(default=75, ge=1)
    number_cooldown_secs: int = Field(default=600, ge=0)
    dial_spacing_min_secs: float = Field(default=20.0, ge=0)
    dial_spacing_max_secs: float = Field(default=30.0, ge=0)

    outbox_max_retries: int = Field(default=10, ge=1)
    outbox_initial_backoff_secs: float = Field(default=1.0, gt=0)
    outbox_max_backoff_secs: float = Field(default=300.0, gt=0)

    telephony_backend: Literal["fake"] = "fake"
    agent_backend: Literal["fake"] = "fake"

    fake_no_answer_prob: float = Field(default=0.55, ge=0, le=1)
    fake_voicemail_prob: float = Field(default=0.10, ge=0, le=1)
    fake_busy_prob: float = Field(default=0.05, ge=0, le=1)
    fake_answered_prob: float = Field(default=0.30, ge=0, le=1)

    fake_agent_min_duration_secs: int = Field(default=20, ge=1)
    fake_agent_max_duration_secs: int = Field(default=90, ge=1)

    log_level: str = Field(default="INFO")

    # Local calendar day for per-number daily caps (midnight reset)
    pool_timezone: str = Field(default="America/New_York")

    # HMAC requests older than this are rejected (commands and webhook-style callbacks)
    signature_max_age_secs: int = Field(default=300, ge=60)

    model_config = {"frozen": True}

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        """Accept common log level names in any casing."""
        upper = value.upper()
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if upper not in allowed:
            raise ValueError(f"log_level must be one of {sorted(allowed)}")
        return upper

    @model_validator(mode="after")
    def check_dial_spacing_and_probs(self) -> Settings:
        """Ensure spacing min <= max and fake telephony probabilities sum to ~1."""
        if self.dial_spacing_min_secs > self.dial_spacing_max_secs:
            raise ValueError("dial_spacing_min_secs cannot exceed dial_spacing_max_secs")

        prob_sum = (
            self.fake_no_answer_prob
            + self.fake_voicemail_prob
            + self.fake_busy_prob
            + self.fake_answered_prob
        )
        if abs(prob_sum - 1.0) > 0.001:
            raise ValueError(
                f"fake telephony probabilities must sum to 1.0 (got {prob_sum:.4f})"
            )

        if self.fake_agent_min_duration_secs > self.fake_agent_max_duration_secs:
            raise ValueError(
                "fake_agent_min_duration_secs cannot exceed fake_agent_max_duration_secs"
            )
        return self

    @property
    def sqlite_path_resolved(self) -> Path:
        """Return absolute path to the database file (parent dirs may be created later)."""
        return self.sqlite_path.expanduser().resolve()


def _env_bool(name: str, default: bool) -> bool:
    """Parse a boolean environment variable (not used yet; helper for future flags)."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _require_env(name: str) -> str:
    """Read a required environment variable or raise a clear startup error."""
    value = os.getenv(name)
    if value is None or not value.strip():
        raise ValueError(
            f"Missing required environment variable: {name}. "
            f"Copy .env.example to .env and set values before starting."
        )
    return value.strip()


def _optional_env(name: str, default: str) -> str:
    """Read an optional environment variable with a string default."""
    value = os.getenv(name)
    return default if value is None else value.strip()


def _optional_env_int(name: str, default: int) -> int:
    """Read an optional integer environment variable."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)


def _optional_env_float(name: str, default: float) -> float:
    """Read an optional float environment variable."""
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return float(raw)


def load_settings() -> Settings:
    """
    Build Settings from the current process environment.

    Raises ValueError with a readable message if required variables are missing
    or validation fails. Called once at startup from main.py lifespan.
    """
    data = {
        "orchestrator_host": _optional_env("ORCHESTRATOR_HOST", "0.0.0.0"),
        "orchestrator_port": _optional_env_int("ORCHESTRATOR_PORT", 8000),
        "web_app_url": _require_env("WEB_APP_URL"),
        "hmac_secret": _require_env("HMAC_SECRET"),
        "global_concurrent_cap": _optional_env_int("GLOBAL_CONCURRENT_CAP", 10),
        "heartbeat_timeout_secs": _optional_env_int("HEARTBEAT_TIMEOUT_SECS", 120),
        "max_call_secs": _optional_env_int("MAX_CALL_SECS", 1800),
        "sqlite_path": Path(_optional_env("SQLITE_PATH", "./data/orchestrator.db")),
        "number_daily_cap_default": _optional_env_int("NUMBER_DAILY_CAP_DEFAULT", 75),
        "number_cooldown_secs": _optional_env_int("NUMBER_COOLDOWN_SECS", 600),
        "dial_spacing_min_secs": _optional_env_float("DIAL_SPACING_MIN_SECS", 20.0),
        "dial_spacing_max_secs": _optional_env_float("DIAL_SPACING_MAX_SECS", 30.0),
        "outbox_max_retries": _optional_env_int("OUTBOX_MAX_RETRIES", 10),
        "outbox_initial_backoff_secs": _optional_env_float(
            "OUTBOX_INITIAL_BACKOFF_SECS", 1.0
        ),
        "outbox_max_backoff_secs": _optional_env_float("OUTBOX_MAX_BACKOFF_SECS", 300.0),
        "telephony_backend": _optional_env("TELEPHONY_BACKEND", "fake"),
        "agent_backend": _optional_env("AGENT_BACKEND", "fake"),
        "fake_no_answer_prob": _optional_env_float("FAKE_NO_ANSWER_PROB", 0.55),
        "fake_voicemail_prob": _optional_env_float("FAKE_VOICEMAIL_PROB", 0.10),
        "fake_busy_prob": _optional_env_float("FAKE_BUSY_PROB", 0.05),
        "fake_answered_prob": _optional_env_float("FAKE_ANSWERED_PROB", 0.30),
        "fake_agent_min_duration_secs": _optional_env_int(
            "FAKE_AGENT_MIN_DURATION_SECS", 20
        ),
        "fake_agent_max_duration_secs": _optional_env_int(
            "FAKE_AGENT_MAX_DURATION_SECS", 90
        ),
        "log_level": _optional_env("LOG_LEVEL", "INFO"),
        "pool_timezone": _optional_env("POOL_TIMEZONE", "America/New_York"),
    }

    if data["telephony_backend"] != "fake":
        raise ValueError(
            "Only TELEPHONY_BACKEND=fake is implemented in this phase."
        )
    if data["agent_backend"] != "fake":
        raise ValueError("Only AGENT_BACKEND=fake is implemented in this phase.")

    return Settings.model_validate(data)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return cached Settings loaded from the environment.

    Use in modules that need read-only config after startup; main should call
    load_settings() explicitly so errors surface during lifespan startup.
    """
    return load_settings()