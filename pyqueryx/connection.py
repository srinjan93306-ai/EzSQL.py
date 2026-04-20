"""Connection wrapper used by PyQueryX."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterable, Iterator, List, Optional, Sequence, Tuple

from .exceptions import PyQueryXError
from .helpers import is_select_query


Params = Optional[Sequence[Any]]


class PyQueryXConnection:
    """Small wrapper around a Python DB-API connection."""

    def __init__(
        self,
        conn: Any,
        db_type: str,
        *,
        autocommit: Optional[bool] = None,
        echo: bool = False,
    ) -> None:
        """Store the raw DB-API connection and behavior flags."""
        self.conn = conn
        self.db_type = db_type
        self.autocommit = autocommit
        self.echo = echo
        self._transaction_depth = 0

        if autocommit is not None and hasattr(conn, "autocommit"):
            try:
                conn.autocommit = autocommit
            except Exception:
                pass

    def query(self, sql: str, params: Params = None) -> List[Tuple[Any, ...]]:
        """Run SQL and return all rows as a list of tuples."""
        cursor = None

        try:
            if self.echo:
                print(f"PyQueryX SQL: {sql}")

            cursor = self.conn.cursor()
            cursor.execute(sql, params or ())

            rows = cursor.fetchall() if cursor.description is not None else []

            if self._should_commit(sql):
                self.conn.commit()

            return rows
        except Exception as exc:
            self._rollback_safely()
            raise PyQueryXError(f"PyQueryX Error: {exc}") from exc
        finally:
            if cursor is not None:
                cursor.close()

    def execute(self, sql: str, params: Params = None) -> None:
        """Run SQL without returning rows."""
        self.query(sql, params)

    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        """Run one SQL statement for many parameter rows."""
        cursor = None

        try:
            if self.echo:
                print(f"PyQueryX SQL many: {sql}")

            cursor = self.conn.cursor()
            cursor.executemany(sql, rows)

            if self._should_commit(sql):
                self.conn.commit()
        except Exception as exc:
            self._rollback_safely()
            raise PyQueryXError(f"PyQueryX Error: {exc}") from exc
        finally:
            if cursor is not None:
                cursor.close()

    def one(self, sql: str, params: Params = None) -> Optional[Tuple[Any, ...]]:
        """Return one row or ``None``."""
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def scalar(self, sql: str, params: Params = None) -> Any:
        """Return the first column from the first row or ``None``."""
        row = self.one(sql, params)
        return row[0] if row else None

    @contextmanager
    def transaction(self) -> Iterator["PyQueryXConnection"]:
        """Run a block in a transaction, committing or rolling back."""
        try:
            self._transaction_depth += 1
            yield self
            if self._transaction_depth == 1:
                self.conn.commit()
        except Exception:
            self._rollback_safely()
            raise
        finally:
            self._transaction_depth -= 1

    def close(self) -> None:
        """Close the database connection safely."""
        try:
            self.conn.close()
        except Exception as exc:
            raise PyQueryXError(f"PyQueryX Error: {exc}") from exc

    def __enter__(self) -> "PyQueryXConnection":
        """Return this connection for ``with`` blocks."""
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        """Close the connection when leaving a ``with`` block."""
        self.close()

    def __repr__(self) -> str:
        """Return a readable representation for debugging."""
        return f"<PyQueryX Connection ({self.db_type})>"

    def _should_commit(self, sql: str) -> bool:
        """Return True when PyQueryX should auto-commit this statement."""
        return (
            self.autocommit is not False
            and self._transaction_depth == 0
            and not is_select_query(sql)
        )

    def _rollback_safely(self) -> None:
        """Rollback failed transactions when the driver supports it."""
        try:
            self.conn.rollback()
        except Exception:
            pass


EZConnection = PyQueryXConnection
