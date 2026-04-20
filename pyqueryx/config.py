"""Configuration helpers for PyQueryX connections."""

from __future__ import annotations

import os
from dataclasses import dataclass, fields
from typing import Any, Dict, Mapping, Optional


@dataclass(frozen=True)
class DatabaseConfig:
    """Portable database configuration for :func:`pyqueryx.connect`."""

    db_type: str = "sqlite"
    database: Optional[str] = None
    host: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    port: Optional[int] = None
    timeout: Optional[float] = None
    autocommit: Optional[bool] = None
    echo: bool = False
    options: Optional[Dict[str, Any]] = None

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "DatabaseConfig":
        """Create a config object from a mapping, ignoring unknown keys."""
        allowed = {field.name for field in fields(cls)}
        kwargs = {key: value for key, value in values.items() if key in allowed}

        if kwargs.get("port") is not None:
            kwargs["port"] = int(kwargs["port"])

        if kwargs.get("timeout") is not None:
            kwargs["timeout"] = float(kwargs["timeout"])

        if "autocommit" in kwargs:
            kwargs["autocommit"] = _to_bool(kwargs["autocommit"])

        if "echo" in kwargs:
            kwargs["echo"] = _to_bool(kwargs["echo"])

        return cls(**kwargs)


def config_from_env(prefix: str = "PYQUERYX_") -> DatabaseConfig:
    """Load database configuration from environment variables.

    Supported names are ``PYQUERYX_DB_TYPE``, ``PYQUERYX_DATABASE``,
    ``PYQUERYX_HOST``, ``PYQUERYX_USER``, ``PYQUERYX_PASSWORD``,
    ``PYQUERYX_PORT``, ``PYQUERYX_TIMEOUT``, ``PYQUERYX_AUTOCOMMIT``, and
    ``PYQUERYX_ECHO``.
    """
    values = {
        "db_type": os.getenv(f"{prefix}DB_TYPE"),
        "database": os.getenv(f"{prefix}DATABASE"),
        "host": os.getenv(f"{prefix}HOST"),
        "user": os.getenv(f"{prefix}USER"),
        "password": os.getenv(f"{prefix}PASSWORD"),
        "port": os.getenv(f"{prefix}PORT"),
        "timeout": os.getenv(f"{prefix}TIMEOUT"),
        "autocommit": os.getenv(f"{prefix}AUTOCOMMIT"),
        "echo": os.getenv(f"{prefix}ECHO"),
    }
    return DatabaseConfig.from_mapping(
        {key: value for key, value in values.items() if value is not None}
    )


def _to_bool(value: Any) -> bool:
    """Convert common config values to bool."""
    if isinstance(value, bool):
        return value

    return str(value).strip().lower() in {"1", "true", "yes", "on"}
