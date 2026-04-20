"""Basic PyQueryX usage example."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyqueryx import connect


def main() -> None:
    """Create a table, insert one row, read it, and close the connection."""
    conn = connect("sqlite", database="test.db")

    conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT)")
    conn.execute("DELETE FROM users")
    conn.execute("INSERT INTO users VALUES (?, ?)", (1, "Srinjan"))

    result = conn.query("SELECT * FROM users WHERE id = ?", (1,))
    print(result)

    conn.close()


if __name__ == "__main__":
    main()
