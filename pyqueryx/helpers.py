"""Helper functions used by PyQueryX."""


def is_select_query(sql: str) -> bool:
    """Return True when SQL starts with SELECT after leading whitespace."""
    return sql.lstrip().lower().startswith("select")
