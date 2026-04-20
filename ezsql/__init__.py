"""Backward-compatible import shim for PyQueryX.

Use ``from pyqueryx import connect`` in new code.
"""

from pyqueryx import (
    DatabaseConfig,
    EZConnection,
    EZSQLError,
    PyQueryXConnection,
    PyQueryXError,
    __version__,
    connect,
    connect_from_config,
    connect_from_env,
)

__all__ = [
    "connect",
    "connect_from_config",
    "connect_from_env",
    "DatabaseConfig",
    "PyQueryXConnection",
    "PyQueryXError",
    "EZConnection",
    "EZSQLError",
    "__version__",
]
