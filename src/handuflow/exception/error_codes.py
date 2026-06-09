"""
HanduFlow error code registry.

Format: HF### — stable identifier for logs, reports, and APIs.
"""

from __future__ import annotations

from typing import TypedDict


class ErrorCodeEntry(TypedDict):
    description: str
    category: str


ERROR_CODES: dict[str, ErrorCodeEntry] = {
    # Validation (HF001–HF019)
    "HF001": {
        "description": "Feed specs JSON is invalid or not parseable",
        "category": "validation",
    },
    "HF002": {
        "description": "Master specs structure or required columns invalid",
        "category": "validation",
    },
    "HF003": {
        "description": "Master specs file missing or unreadable",
        "category": "validation",
    },
    "HF004": {
        "description": "Primary key validation failed",
        "category": "validation",
    },
    "HF005": {
        "description": "Composite key validation failed",
        "category": "validation",
    },
    "HF006": {
        "description": "Partition key validation failed",
        "category": "validation",
    },
    "HF007": {
        "description": "Column missing from selection query or schema",
        "category": "validation",
    },
    "HF008": {
        "description": "Standard checks structure invalid",
        "category": "validation",
    },
    "HF009": {
        "description": "Comprehensive checks structure invalid",
        "category": "validation",
    },
    "HF010": {
        "description": "Vacuum hours value invalid",
        "category": "validation",
    },
    "HF011": {
        "description": "Comprehensive checks dependency dataset invalid",
        "category": "validation",
    },
    "HF012": {
        "description": "Validation context or Spark table metadata error",
        "category": "validation",
    },
    "HF013": {
        "description": "System launch validation failed unexpectedly",
        "category": "validation",
    },
    # Configuration (HF020–HF029)
    "HF020": {
        "description": "config.ini validation failed (missing keys or sections)",
        "category": "configuration",
    },
    "HF021": {
        "description": "Configured path does not exist",
        "category": "configuration",
    },
    "HF022": {
        "description": "config.ini file not found or not parseable",
        "category": "configuration",
    },
    # Data load (HF030–HF049)
    "HF030": {
        "description": "Unsupported load_type for feed",
        "category": "data_load",
    },
    "HF031": {
        "description": "Feed specs JSON invalid at load dispatch",
        "category": "data_load",
    },
    "HF032": {
        "description": "No load handler registered for load_type",
        "category": "data_load",
    },
    "HF033": {
        "description": "Partition column preparation failed",
        "category": "data_load",
    },
    "HF034": {
        "description": "Schema mismatch or enforcement failed",
        "category": "data_load",
    },
    "HF035": {
        "description": "Row count verification failed",
        "category": "data_load",
    },
    "HF036": {
        "description": "Empty source sync blocked",
        "category": "data_load",
    },
    "HF037": {
        "description": "Primary key integrity check failed",
        "category": "data_load",
    },
    "HF038": {
        "description": "Target table missing after reported successful load",
        "category": "data_load",
    },
    "HF039": {
        "description": "Delta merge, staging, or partition layout error",
        "category": "data_load",
    },
    "HF040": {
        "description": "Unknown audit column load kind",
        "category": "data_load",
    },
    "HF041": {
        "description": "Append load strategy failed",
        "category": "data_load",
    },
    "HF042": {
        "description": "Incremental CDC load failed",
        "category": "data_load",
    },
    "HF043": {
        "description": "SCD Type 2 load failed",
        "category": "data_load",
    },
    "HF044": {
        "description": "Full load strategy failed",
        "category": "data_load",
    },
    "HF045": {
        "description": "Load type conflict on existing target table",
        "category": "data_load",
    },
    # Extraction (HF050–HF059)
    "HF050": {
        "description": "API extraction failed",
        "category": "extraction",
    },
    "HF051": {
        "description": "API response format unsupported",
        "category": "extraction",
    },
    "HF052": {
        "description": "API content-type mismatch",
        "category": "extraction",
    },
    "HF053": {
        "description": "API request failed after retries",
        "category": "extraction",
    },
    # Storage fetch (HF060–HF069)
    "HF060": {
        "description": "Storage fetch load failed",
        "category": "storage_fetch",
    },
    "HF061": {
        "description": "Invalid storage file_type",
        "category": "storage_fetch",
    },
    "HF062": {
        "description": "Invalid storage storage_type",
        "category": "storage_fetch",
    },
    # Data quality (HF070–HF079)
    "HF070": {
        "description": "Comprehensive DQ dependency table missing",
        "category": "data_quality",
    },
    "HF071": {
        "description": "Unsupported standard DQ check method",
        "category": "data_quality",
    },
    "HF072": {
        "description": "Invalid DQ check parameters",
        "category": "data_quality",
    },
    "HF073": {
        "description": "DQ executor runtime error",
        "category": "data_quality",
    },
    # Reporting (HF080–HF089)
    "HF080": {
        "description": "Result generation failed",
        "category": "reporting",
    },
    "HF081": {
        "description": "Excel report write failed",
        "category": "reporting",
    },
    "HF082": {
        "description": "Result segregation failed",
        "category": "reporting",
    },
    # Orchestration / system (HF090–HF099)
    "HF090": {
        "description": "Spark session is required but missing",
        "category": "system",
    },
    "HF091": {
        "description": "Unexpected orchestrator failure",
        "category": "system",
    },
    "HF092": {
        "description": "Pipeline phase failure",
        "category": "system",
    },
    "HF093": {
        "description": "Parallel feed dispatch failure",
        "category": "system",
    },
    "HF094": {
        "description": "Log archive or run summary failure",
        "category": "system",
    },
    "HF095": {
        "description": "System cleanup failure",
        "category": "system",
    },
    "HF096": {
        "description": "Lineage diagram generation failure",
        "category": "system",
    },
    "HF097": {
        "description": "System restore failure or invalid restore point",
        "category": "system",
    },
    "HF099": {
        "description": "Unknown or unclassified error",
        "category": "system",
    },
}

DEFAULT_ERROR_CODE = "HF099"

VALIDATION_RULE_CODES: dict[str, str] = {
    "ValidateFeedSpecsJSON": "HF001",
    "EnforceMasterSpecsStructure": "HF002",
    "ValidateMasterSpecs": "HF003",
    "PrimaryKey": "HF004",
    "CompositeKeysCheck": "HF005",
    "PartitionKeysCheck": "HF006",
    "ColumnExistsInSelection": "HF007",
    "EnforceStandardChecks": "HF008",
    "StandardCheckStructureCheck": "HF009",
    "VacuumHoursCheck": "HF010",
    "ComprehensiveChecksDependencyDatasetCheck": "HF011",
}


def get_error_description(code: str) -> str:
    entry = ERROR_CODES.get(code)
    if entry:
        return entry["description"]
    return ERROR_CODES[DEFAULT_ERROR_CODE]["description"]


def get_error_category(code: str) -> str:
    entry = ERROR_CODES.get(code)
    if entry:
        return entry["category"]
    return ERROR_CODES[DEFAULT_ERROR_CODE]["category"]


def format_error_label(code: str) -> str:
    return f"{code}: {get_error_description(code)}"
