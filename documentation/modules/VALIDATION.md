# `validation/` package

Master-spec launch validation: rule engine, context, and individual checks (HF001–HF011).

**Import:** `from handuflow.validation import SystemLaunchValidator, Validator`

---

## `validation/__init__.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Exports** | `SystemLaunchValidator`, `Validator` |

---

## `validation/system_launch_validator.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Purpose** | Compose and run all launch validation rules against master specs. |

### Class: `SystemLaunchValidator`

| Method | Description |
|--------|-------------|
| `run()` | Execute all rules; raise `ValidationError` on failure |
| `get_validated_master_specs()` | Filtered active-feed DataFrame after success |

Rules executed in order (see validation rules below). On success, stores parsed master specs in `ValidationContext`.

**Dependencies:** `validator`, `validation_context`, `config.config_paths`, all `validation_rules.*`, `exception.validation_error`

---

## `validation/validator.py`

| | |
|---|---|
| **Visibility** | Semi-public |
| **Purpose** | Run a sequence of rules with optional fail-fast. |

### Class: `Validator`

| Method | Description |
|--------|-------------|
| `validate(rules, context, *, fail_fast=True)` | Run rules; return `ValidationResult` |

**Dependencies:** `exception.*`, `validation_rule`, `validation_context`, `validation_result`

---

## `validation/validation_rule.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Abstract base for validation rules. |

### Class: `ValidationRule`

| Method | Description |
|--------|-------------|
| `validate(context)` | Abstract: inspect context, call `fail()` on violation |
| `fail(message, error_code)` | Raise `ValidationError` with HF### code |

**Dependencies:** `exception.validation_error`, `exception.error_codes`, `validation_context`

---

## `validation/validation_context.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Mutable context passed through rules. |

### Class: `ValidationContext`

| Attribute / method | Description |
|--------------------|-------------|
| `spark` | SparkSession |
| `config` | Parsed config.ini |
| `file_hunt_path` | Base spec directory |
| `get_master_specs()` | Lazy-load Excel into pandas DataFrame |
| `_get_table_columns(table)` | Spark catalog column list |

**Dependencies:** `exception.validation_error`

---

## `validation/validation_result.py`

| | |
|---|---|
| **Visibility** | Internal |
| **Purpose** | Aggregated validation outcome. |

### Class: `ValidationResult`

| Field | Description |
|-------|-------------|
| `passed` | All rules passed |
| `score` | Pass ratio |
| `errors` | List of error messages |
| `results_df` | Per-rule results DataFrame |

**Dependencies:** `exception.validation_error`

---

## Validation rules (`validation_rules/`)

Each rule extends `ValidationRule` and maps to an HF### code.

| Module | Class | Code | Check |
|--------|-------|------|-------|
| `validate_feed_specs_json.py` | `ValidateFeedSpecsJSON` | HF001 | `feed_specs` column parses as valid JSON |
| `enforce_master_specs_structure.py` | `EnforceMasterSpecsStructure` | HF002 | Required Excel columns; active feeds; medallion keys |
| `validate_master_specs.py` | `ValidateMasterSpecs` | HF003 | `master_specs.xlsx` exists on disk |
| `primary_key.py` | `PrimaryKey` | HF004 | Non-bronze feeds have `primary_key` |
| `composite_keys_check.py` | `CompositeKeysCheck` | HF005 | `composite_key` columns exist in source |
| `partition_keys_check.py` | `PartitionKeysCheck` | HF006 | `partition_keys` columns exist in source |
| `column_exists_in_selection.py` | `ColumnExistsInSelection` | HF007 | `selection_schema` columns exist in source |
| `enforce_standard_checks.py` | `EnforceStandardChecks` | HF008 | `standard_checks` is a list |
| `standard_check_structure_check.py` | `StandardCheckStructureCheck` | HF009 | Each check has `check_sequence`, `column_name`, `threshold` |
| `vacuum_hours_check.py` | `VacuumHoursCheck` | HF010 | Integer `vacuum_hours` per medallion feed |
| `comprehensive_checks_dependency_dataset_check.py` | `ComprehensiveChecksDependencyDatasetCheck` | HF011 | Comprehensive check dependency tables exist in catalog |

### `enforce_master_specs_structure.py` helper

`validate_medallion_feed_spec_keys(feed_specs)` — validates required JSON keys for medallion feeds.
