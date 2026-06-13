"""Schema route labels for reports (e.g. demo-silver)."""

from __future__ import annotations


def table_schema_name(table_ref: str | None) -> str:
    """Return the schema/database segment from a table reference."""
    if not table_ref:
        return "unknown"
    parts = [part.strip() for part in str(table_ref).split(".") if part.strip()]
    if len(parts) >= 3:
        return parts[-2]
    if len(parts) == 2:
        return parts[0]
    return parts[0]


def feed_route_label(
    *,
    source_table_name: str | None = None,
    target_schema_name: str | None = None,
    target_table_path: str | None = None,
) -> str:
    """
    Build a route label ``{source_schema}-{target_schema}`` (e.g. ``demo-silver``).
    """
    source_schema = table_schema_name(source_table_name)
    if target_schema_name and str(target_schema_name).strip():
        target_schema = str(target_schema_name).strip()
    else:
        target_schema = table_schema_name(target_table_path)
    return f"{source_schema}-{target_schema}"
