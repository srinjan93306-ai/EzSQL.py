"""Connection wrapper used by ezsql."""

from __future__ import annotations

from typing import Any, List, Tuple

from .exceptions import EZSQLError
from .helpers import is_select_query


class EZConnection:
    """Small wrapper around a Python DB-API connection.

    Users call :meth:`query` or :meth:`execute` directly without managing
    cursors, commits, or low-level DB-API details.
    """

    def __init__(self, conn: Any, db_type: str) -> None:
        """Store the raw DB-API connection and database type."""
        self.conn = conn
        self.db_type = db_type

    def query(self, sql: str) -> List[Tuple[Any, ...]]:
        """Run SQL and return all rows as a list of tuples.

        For statements that do not produce rows, this method commits the
        transaction and returns an empty list.

        Raises:
            EZSQLError: If the database driver raises an error.
        """
        cursor = None

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)

            rows = cursor.fetchall() if cursor.description is not None else []

            if not is_select_query(sql):
                self.conn.commit()

            return rows
        except Exception as exc:
            self._rollback_safely()
            raise EZSQLError(f"EZSQL Error: {exc}") from exc
        finally:
            if cursor is not None:
                cursor.close()

    def execute(self, sql: str) -> None:
        """Run SQL without returning rows.

        This is intended for statements such as ``CREATE TABLE``, ``INSERT``,
        ``UPDATE``, and ``DELETE``.

        Raises:
            EZSQLError: If the database driver raises an error.
        """
        self.query(sql)

    def close(self) -> None:
        """Close the database connection safely."""
        try:
            self.conn.close()
        except Exception as exc:
            raise EZSQLError(f"EZSQL Error: {exc}") from exc

    def __repr__(self) -> str:
        """Return a readable representation for debugging."""
        return f"<EZSQL Connection ({self.db_type})>"

    def _rollback_safely(self) -> None:
        """Rollback failed transactions when the driver supports it."""
        try:
            self.conn.rollback()
        except Exception:
            pass
