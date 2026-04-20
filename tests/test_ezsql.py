"""Tests for the ezsql public API."""

import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO

from ezsql import EZConnection, EZSQLError, connect
from ezsql.helpers import is_select_query


class EZSQLTests(unittest.TestCase):
    """SQLite-backed tests for ezsql behavior."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = os.path.join(self.temp_dir.name, "test.db")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def connect_sqlite(self) -> EZConnection:
        output = StringIO()

        with redirect_stdout(output):
            return connect("sqlite", database=self.database_path)

    def test_sqlite_usage_example(self) -> None:
        output = StringIO()

        with redirect_stdout(output):
            conn = connect("sqlite", database=self.database_path)

        self.assertIn("Connected to SQLite", output.getvalue())

        conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO users VALUES (1, 'Srinjan')")

        result = conn.query("SELECT * FROM users")

        self.assertEqual(result, [(1, "Srinjan")])
        conn.close()

    def test_execute_returns_none(self) -> None:
        conn = self.connect_sqlite()

        result = conn.execute("CREATE TABLE users (id INTEGER)")

        self.assertIsNone(result)
        conn.close()

    def test_query_returns_empty_list_for_write_query(self) -> None:
        conn = self.connect_sqlite()

        result = conn.query("CREATE TABLE users (id INTEGER)")

        self.assertEqual(result, [])
        conn.close()

    def test_repr_includes_database_type(self) -> None:
        conn = self.connect_sqlite()

        self.assertEqual(repr(conn), "<EZSQL Connection (sqlite)>")
        conn.close()

    def test_unsupported_database_type_raises_ezsql_error(self) -> None:
        with self.assertRaisesRegex(EZSQLError, "unsupported database type"):
            connect("mysql")

    def test_database_errors_are_wrapped(self) -> None:
        conn = self.connect_sqlite()

        with self.assertRaisesRegex(EZSQLError, "EZSQL Error:"):
            conn.query("SELECT * FROM missing_table")

        conn.close()

    def test_close_errors_are_wrapped(self) -> None:
        class BadConnection:
            def close(self) -> None:
                raise sqlite3.Error("close failed")

        conn = EZConnection(BadConnection(), "sqlite")

        with self.assertRaisesRegex(EZSQLError, "close failed"):
            conn.close()

    def test_is_select_query(self) -> None:
        self.assertTrue(is_select_query("SELECT * FROM users"))
        self.assertTrue(is_select_query("   select * from users"))
        self.assertFalse(is_select_query("INSERT INTO users VALUES (1)"))


if __name__ == "__main__":
    unittest.main()
