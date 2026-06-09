"""Shared constants for HanduFlow pipelines."""

SOURCE_TO_BRONZE = "SOURCE_TO_BRONZE"

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
