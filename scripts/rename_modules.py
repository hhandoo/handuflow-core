#!/usr/bin/env python3
"""One-time migration: PascalCase module files -> snake_case (PEP 8)."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "handuflow"

# Explicit renames (relative to src/handuflow). Class names inside files unchanged.
RENAMES: dict[str, str] = {
    "orchestrator/Orchestrator.py": "orchestrator/orchestrator.py",
    "system_cleanup/SystemCleanup.py": "system_cleanup/cleanup.py",
    "system_restore/systemrestore.py": "system_restore/restore.py",
    "data_movement_controller/LoadDispatcher.py": "data_movement_controller/load_dispatcher.py",
    "data_movement_controller/DataLoadController.py": "data_movement_controller/data_load_controller.py",
    "data_movement_controller/BaseLoadStrategy.py": "data_movement_controller/base_load_strategy.py",
    "data_movement_controller/data_class/LoadConfig.py": "data_movement_controller/data_class/load_config.py",
    "data_movement_controller/data_class/LoadResult.py": "data_movement_controller/data_class/load_result.py",
    "data_movement_controller/load_types/APIExtractor.py": "data_movement_controller/load_types/api_extractor.py",
    "data_movement_controller/load_types/AppendLoad.py": "data_movement_controller/load_types/append_load.py",
    "data_movement_controller/load_types/FullLoad.py": "data_movement_controller/load_types/full_load.py",
    "data_movement_controller/load_types/IncrementalCDC.py": "data_movement_controller/load_types/incremental_cdc.py",
    "data_movement_controller/load_types/SCDType2.py": "data_movement_controller/load_types/scd_type_2.py",
    "data_movement_controller/load_types/StorageFetch.py": "data_movement_controller/load_types/storage_fetch.py",
    "config/LoggingConfig.py": "config/logging_config.py",
    "config/LoggingPrettyFormatter.py": "config/logging_pretty_formatter.py",
    "result_generator/ResultGenerator.py": "result_generator/result_generator.py",
    "data_flow_diagram_generator/DataFlowDiagramGenerator.py": "data_flow_diagram_generator/data_flow_diagram_generator.py",
    "data_quality/runner/FeedDataQualityRunner.py": "data_quality/runner/feed_data_quality_runner.py",
    "data_quality/executors/StandardDQExecutor.py": "data_quality/executors/standard_dq_executor.py",
    "data_quality/executors/ComprehensiveDQExecutor.py": "data_quality/executors/comprehensive_dq_executor.py",
    "data_quality/model/FeedDQSummaryRow.py": "data_quality/model/feed_dq_summary_row.py",
    "data_quality/report/DQExcelReportWriter.py": "data_quality/report/dq_excel_report_writer.py",
    "validation/SystemLaunchValidator.py": "validation/system_launch_validator.py",
    "validation/Validator.py": "validation/validator.py",
    "validation/ValidationRule.py": "validation/validation_rule.py",
    "validation/ValidationContext.py": "validation/validation_context.py",
    "validation/ValidationResult.py": "validation/validation_result.py",
    "validation/validation_rules/PrimaryKey.py": "validation/validation_rules/primary_key.py",
    "validation/validation_rules/CompositeKeysCheck.py": "validation/validation_rules/composite_keys_check.py",
    "validation/validation_rules/ColumnExistsInSelection.py": "validation/validation_rules/column_exists_in_selection.py",
    "validation/validation_rules/EnforceMasterSpecsStructure.py": "validation/validation_rules/enforce_master_specs_structure.py",
    "validation/validation_rules/EnforceStandardChecks.py": "validation/validation_rules/enforce_standard_checks.py",
    "validation/validation_rules/StandardCheckStructureCheck.py": "validation/validation_rules/standard_check_structure_check.py",
    "validation/validation_rules/ComprehensiveChecksDependencyDatasetCheck.py": "validation/validation_rules/comprehensive_checks_dependency_dataset_check.py",
    "validation/validation_rules/PartitionKeysCheck.py": "validation/validation_rules/partition_keys_check.py",
    "validation/validation_rules/VacuumHoursCheck.py": "validation/validation_rules/vacuum_hours_check.py",
    "validation/validation_rules/ValidateFeedSpecsJSON.py": "validation/validation_rules/validate_feed_specs_json.py",
    "validation/validation_rules/ValidateMasterSpecs.py": "validation/validation_rules/validate_master_specs.py",
    "exception/BaseException.py": "exception/base_exception.py",
    "exception/ConfigError.py": "exception/config_error.py",
    "exception/ValidationError.py": "exception/validation_error.py",
    "exception/DataLoadException.py": "exception/data_load_exception.py",
    "exception/DataQualityException.py": "exception/data_quality_exception.py",
    "exception/ExtractionException.py": "exception/extraction_exception.py",
    "exception/StorageFetchException.py": "exception/storage_fetch_exception.py",
    "exception/ResultGenerationException.py": "exception/result_generation_exception.py",
    "exception/SystemError.py": "exception/system_error.py",
}

DELETE_FILES = [
    SRC / "system_restore" / "SystemRestore.py",
]


def module_path_from_file(rel: str) -> str:
    """handuflow.orchestrator.Orchestrator from orchestrator/Orchestrator.py"""
    parts = Path(rel).with_suffix("").parts
    return "handuflow." + ".".join(parts)


def build_import_replacements() -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for old_rel, new_rel in RENAMES.items():
        old_mod = module_path_from_file(old_rel)
        new_mod = module_path_from_file(new_rel)
        pairs.append((old_mod, new_mod))
    # legacy alias module
    pairs.append(
        (
            "handuflow.system_restore.systemrestore",
            "handuflow.system_restore.restore",
        )
    )
    pairs.sort(key=lambda x: len(x[0]), reverse=True)
    return pairs


def rewrite_text(text: str, replacements: list[tuple[str, str]]) -> str:
    for old, new in replacements:
        text = text.replace(old, new)
    return text


def main() -> None:
    replacements = build_import_replacements()

    for old_rel, new_rel in RENAMES.items():
        old_path = SRC / old_rel
        new_path = SRC / new_rel
        if not old_path.exists():
            raise SystemExit(f"Missing file to rename: {old_path}")
        new_path.parent.mkdir(parents=True, exist_ok=True)
        old_path.rename(new_path)
        print(f"rename: {old_rel} -> {new_rel}")

    for path in DELETE_FILES:
        if path.exists():
            path.unlink()
            print(f"delete: {path.relative_to(ROOT)}")

    targets = list(ROOT.rglob("*.py")) + list(ROOT.rglob("*.md"))
    for path in targets:
        if "scripts/rename_modules.py" in str(path):
            continue
        original = path.read_text(encoding="utf-8")
        updated = rewrite_text(original, replacements)
        if updated != original:
            path.write_text(updated, encoding="utf-8")
            print(f"updated imports: {path.relative_to(ROOT)}")

    print("done")


if __name__ == "__main__":
    main()
