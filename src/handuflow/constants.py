"""Shared constants for HanduFlow pipelines."""

INGESTION = "INGESTION"
WITHIN_UNITY_CATALOG = "WITHIN_UNITY_CATALOG"

# Deprecated; treated as ``INGESTION`` for backward compatibility.
SOURCE_TO_BRONZE = "SOURCE_TO_BRONZE"

ALLOWED_DATA_FLOW_DIRECTIONS = frozenset(
    {
        INGESTION,
        WITHIN_UNITY_CATALOG,
    }
)

SUPPORTED_LOAD_TYPES = frozenset(
    {
        "FULL_LOAD",
        "APPEND_LOAD",
        "INCREMENTAL_CDC",
        "SCD_TYPE_2",
        "API_EXTRACTOR",
        "STORAGE_FETCH",
    }
)


def is_ingestion_direction(direction: str | None) -> bool:
    """True for external ingest feeds (API, storage fetch, legacy SOURCE_TO_BRONZE)."""
    value = (direction or "").strip()
    return value in (INGESTION, SOURCE_TO_BRONZE)


def is_within_unity_catalog_direction(direction: str | None) -> bool:
    """True for feeds whose source and target tables live in Unity Catalog."""
    return not is_ingestion_direction(direction)
