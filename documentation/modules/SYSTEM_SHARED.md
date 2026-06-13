# `system_shared/` package

Shared utilities for system cleanup and restore operations.

**Import:** `from handuflow.system_shared import is_delta_table, collect_master_spec_table_entries`

---

## `system_shared/__init__.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Exports** | `is_delta_table`, `quote_table`, `TABLE_TYPE_SOURCE`, `TABLE_TYPE_TARGET`, `TABLE_TYPE_STAGING`, `collect_master_spec_table_entries`, `collect_cleanup_table_entries`, `collect_restore_point_table_entries`, `expected_table_set`, `expected_restore_table_set` |

---

## `system_shared/delta_utils.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Purpose** | Delta table detection and SQL identifier quoting. |

### Functions

| Name | Description |
|------|-------------|
| `is_delta_table(spark, table_name) -> bool` | Detect Delta format via `DESCRIBE DETAIL` (works on local Hive) |
| `quote_table(name) -> str` | Backtick-quote identifiers for Spark SQL |

**Dependencies:** None

**Used by:** `system_cleanup`, `system_restore`

---

## `system_shared/spec_tables.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Purpose** | Collect source/target and restore-scoped table names from master specs. |

### Constants

| Name | Description |
|------|-------------|
| `TABLE_TYPE_SOURCE` | `"SOURCE"` |
| `TABLE_TYPE_TARGET` | `"TARGET"` |
| `TABLE_TYPE_STAGING` | `"STAGING"` |

### Functions

| Name | Description |
|------|-------------|
| `master_specs_to_dataframe(specs)` | Normalize specs input to DataFrame |
| `collect_master_spec_table_entries(specs, config)` | Source + target `(table_name, table_type)` pairs |
| `collect_cleanup_table_entries(specs, config)` | Source + target + staging table names (post-run cleanup) |
| `expected_table_set(specs, config) -> set[str]` | Unique source and target table names |
| `TABLE_TYPE_SOURCE` | `"SOURCE"` |
| `TABLE_TYPE_TARGET` | `"TARGET"` |
| `TABLE_TYPE_STAGING` | `"STAGING"` |
| `collect_restore_point_table_entries(specs, config)` | Source + target + staging for `WITHIN_UNITY_CATALOG`; target only for `INGESTION` |
| `expected_restore_table_set(specs, config) -> set[str]` | Target tables required for a complete restore point |

Walks all active feeds via `CatalogResolver`. Restore scope includes `staging.t_full_*`, `staging.t_incr_*`, and `staging.t_incr_cdf_changes_*` when those tables exist.

**Dependencies:** `config.catalog_resolver`

**Used by:** `system_cleanup`, `system_restore`
