"""A small, beginner-friendly SQL wrapper for Python DB-API drivers."""

from __future__ import annotations

import sqlite3
from typing import Optional

from .connection import EZConnection
from .exceptions import EZSQLError

__version__ = "0.1.0"

__all__ = ["connect", "EZConnection", "EZSQLError", "__version__"]


def connect(
    db_type: str = "sqlite",
    database: Optional[str] = None,
    host: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
) -> EZConnection:
    """Create and return an :class:`EZConnection`.

    Args:
        db_type: Database backend to use. Supported values are ``"sqlite"``
            and ``"postgres"``.
        database: SQLite file path, PostgreSQL database name, or ``None`` for
            an in-memory SQLite database.
        host: PostgreSQL host.
        user: PostgreSQL username.
        password: PostgreSQL password.
        port: PostgreSQL port.

    Raises:
        EZSQLError: If the database type is unsupported, a driver is missing,
            or the database connection fails.
    """
    normalized_db_type = db_type.lower().strip()

    if normalized_db_type == "sqlite":
        try:
            connection = sqlite3.connect(database or ":memory:")
        except sqlite3.Error as exc:
            raise EZSQLError(f"EZSQL Error: {exc}") from exc

        print("Connected to SQLite")
        return EZConnection(connection, normalized_db_type)

    if normalized_db_type in {"postgres", "postgresql"}:
        try:
            import psycopg2
        except ImportError as exc:
            raise EZSQLError(
                "EZSQL Error: PostgreSQL support requires psycopg2 to be installed."
            ) from exc

        connection_args = {
            "dbname": database,
            "host": host,
            "user": user,
            "password": password,
            "port": port,
        }
        connection_args = {
            key: value for key, value in connection_args.items() if value is not None
        }

        try:
            connection = psycopg2.connect(**connection_args)
        except psycopg2.Error as exc:
            raise EZSQLError(f"EZSQL Error: {exc}") from exc

        return EZConnection(connection, "postgres")

    raise EZSQLError(f"EZSQL Error: unsupported database type '{db_type}'.")
