# `system_shared/` package

Shared utilities for system cleanup and restore operations.

**Import:** `from handuflow.system_shared import is_delta_table, collect_master_spec_table_entries`

---

## `system_shared/__init__.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Exports** | `is_delta_table`, `quote_table`, `TABLE_TYPE_SOURCE`, `TABLE_TYPE_TARGET`, `collect_master_spec_table_entries`, `expected_table_set` |

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
| **Purpose** | Collect source/target table names from master specs. |

### Constants

| Name | Description |
|------|-------------|
| `TABLE_TYPE_SOURCE` | `"SOURCE"` |
| `TABLE_TYPE_TARGET` | `"TARGET"` |

### Functions

| Name | Description |
|------|-------------|
| `master_specs_to_dataframe(specs)` | Normalize specs input to DataFrame |
| `collect_master_spec_table_entries(specs, catalog_resolver)` | List of `(table_name, table_type)` tuples |
| `expected_table_set(specs, catalog_resolver) -> set[str]` | Unique qualified table names |

Walks all active feeds, resolves bronze/silver/gold source and target tables via `CatalogResolver`.

**Dependencies:** `config.catalog_resolver`

**Used by:** `system_cleanup`, `system_restore`
