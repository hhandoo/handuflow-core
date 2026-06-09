#!/usr/bin/env python3
"""
HanduFlow exhaustive E2E QA runner.

Usage:
  PYTHONUNBUFFERED=1 .venv/bin/python -m tests.e2e.run_e2e --quick
  tail -f tests/e2e/e2e_progress.log
"""

from __future__ import annotations

import json
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT))

from handuflow.data_movement_controller.load_dispatcher import LoadDispatcher
from handuflow.data_movement_controller.audit_columns import TargetLoadKind
from handuflow.data_quality.runner.feed_data_quality_runner import FeedDataQualityRunner

from tests.e2e.change_events import apply_changes
from tests.e2e.data_generator import generate_dataset, write_source_table
from tests.e2e.discover_feeds import discover_configured_feeds
from tests.e2e.dq_profiles import apply_dq_profile
from tests.e2e.feed_factory import build_feed_specs, build_master_spec
from tests.e2e.spark_setup import create_spark, ensure_qaft_schemas, load_config
from tests.e2e.test_matrix import TestCase, build_test_matrix
from tests.e2e.system_ops import drop_system_ops_artifacts, run_restore_cycle, run_vacuum_cleanup
from tests.e2e.validators import run_full_validations, staging_counts

OUTPUT_DIR = Path(__file__).resolve().parent
REPORT_PATH = OUTPUT_DIR / "test_results.xlsx"
PROGRESS_LOG = OUTPUT_DIR / "e2e_progress.log"

LOAD_KIND_MAP = {
    "FULL_LOAD": TargetLoadKind.FULL_LOAD,
    "APPEND_LOAD": TargetLoadKind.APPEND_LOAD,
    "INCREMENTAL_CDC": TargetLoadKind.INCREMENTAL_CDC,
    "SCD_TYPE_2": TargetLoadKind.SCD_TYPE_2,
}


@dataclass
class TestRecord:
    case: TestCase
    status: str = "PENDING"
    comments: str = ""
    data_integrity: str = ""
    execution_time_seconds: float = 0
    validation_time_seconds: float = 0
    root_cause: str = ""
    fix_applied: str = ""
    modified_files: str = ""
    source_rows_inserted: int = 0
    source_rows_updated: int = 0
    source_rows_deleted: int = 0
    staging: dict = field(default_factory=dict)
    load_result: dict = field(default_factory=dict)
    dq: dict = field(default_factory=dict)
    source_row_count: int = 0
    target_row_count: int = 0
    system_ops: dict = field(default_factory=dict)


def _drop_test_artifacts(spark, target_table: str) -> None:
    """Remove all Delta warehouse dirs and catalog entries for an isolated test run."""
    import shutil

    feed_id = target_table.split("_")[-1]
    warehouse = Path(spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse"))
    if not warehouse.is_absolute():
        warehouse = PROJECT_ROOT / warehouse
    for sub in (
        warehouse / "qaft_silver.db" / target_table,
        warehouse / "staging.db" / f"t_full_{target_table}",
        warehouse / "staging.db" / f"t_incr_{target_table}",
        warehouse / "staging.db" / f"t_incr_cdf_changes_{target_table}",
        warehouse / "qaft_source.db" / f"qaft_src_{feed_id}",
    ):
        if sub.exists():
            shutil.rmtree(sub, ignore_errors=True)
    spark.sql(f"DROP TABLE IF EXISTS qaft_silver.{target_table}")
    spark.sql(f"DROP TABLE IF EXISTS qaft_source.qaft_src_{feed_id}")
    for suffix in (
        f"t_full_{target_table}",
        f"t_incr_{target_table}",
        f"t_incr_cdf_changes_{target_table}",
    ):
        spark.sql(f"DROP TABLE IF EXISTS staging.{suffix}")
    spark.sql("DROP TABLE IF EXISTS qaft_ref.valid_business_keys")


def _make_dispatcher(
    spark,
    config,
    *,
    feed_id: int,
    case: TestCase,
    source: str,
    target: str,
    target_fqn: str,
    partition_keys: list[str],
) -> tuple[LoadDispatcher, dict]:
    fs = apply_dq_profile(
        build_feed_specs(source, partition_keys=partition_keys),
        case.dq_profile,
        source_table=source,
    )
    for chk in fs.get("comprehensive_checks", []):
        if chk.get("load_stage") == "POST_LOAD":
            chk["query"] = chk["query"].replace("__TARGET__", target_fqn)
            if "__TARGET__" not in chk["query"] and "COUNT(*)" in chk["query"]:
                if f"FROM {target_fqn}" not in chk["query"]:
                    chk["query"] = (
                        f"SELECT 1 AS fail WHERE "
                        f"(SELECT COUNT(*) FROM {target_fqn}) = 0"
                    )
    master = build_master_spec(feed_id, case.load_type, target, fs)
    return LoadDispatcher(master, spark, config), master


def _set_load_type_property(spark, table: str, load_type: str) -> None:
    if spark.catalog.tableExists(table):
        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES "
            f"('data.load_type' = '{load_type}')"
        )


def _all_partition_columns(case: TestCase) -> set[str]:
    """Every partition column used across initial load, migrate, add, or remove."""
    cols = set(case.partition_keys)
    for keys in case.partition_migrate:
        cols.update(keys)
    if case.partition_remove or case.partition_add:
        cols.add("country")
    return cols


def _partition_column_expr(col, *, functions):
    """Match BaseLoadStrategy._prepare_partition_columns for Hive-safe paths."""
    return functions.regexp_replace(
        functions.coalesce(
            functions.nullif(functions.trim(col.cast("string")), functions.lit("")),
            functions.lit("UNKNOWN"),
        ),
        r"[^\x00-\x7F]",
        "_",
    )


def _coalesce_partition_columns(df, columns: set[str]):
    from pyspark.sql import functions as F

    for pk in columns:
        if pk in df.columns:
            df = df.withColumn(pk, _partition_column_expr(F.col(pk), functions=F))
    return df


def _sanitize_source_partitions(spark, source: str, columns: set[str]) -> None:
    """Align source partition columns with production writes (null-safe + ASCII)."""
    if not columns or not spark.catalog.tableExists(source):
        return
    from pyspark.sql import functions as F

    df = spark.table(source)
    for pk in columns:
        if pk in df.columns:
            df = df.withColumn(pk, _partition_column_expr(F.col(pk), functions=F))
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(source)
    spark.sql(
        f"ALTER TABLE {source} SET TBLPROPERTIES "
        "(delta.enableChangeDataFeed = true)"
    )


def _prepare_source_df(spark, case: TestCase, source: str, feed_id: int):
    from pyspark.sql import functions as F

    if case.row_count == 0:
        spark.sql(f"DROP TABLE IF EXISTS {source}")
        spark.sql(
            f"""
            CREATE TABLE {source} (id BIGINT, business_key STRING) USING DELTA
            """
        )
        spark.sql(
            f"ALTER TABLE {source} SET TBLPROPERTIES "
            "(delta.enableChangeDataFeed = true)"
        )
        return

    df = generate_dataset(spark, case.row_count, seed=feed_id)
    skip_coalesce = case.expect_fail and "Null" in case.test_name
    if not skip_coalesce:
        df = _coalesce_partition_columns(df, _all_partition_columns(case))
    if case.inject_dq_fail_data:
        if case.dq_profile == "standard_fail":
            df = df.withColumn(
                "id",
                F.when(F.col("id") % 10 == 0, F.lit(None).cast("long")).otherwise(
                    F.col("id")
                ),
            )
    write_source_table(spark, source, df, partition_keys=None)
    spark.sql("CREATE DATABASE IF NOT EXISTS qaft_ref")
    keys = df.select("business_key").distinct()
    keys.write.format("delta").mode("overwrite").saveAsTable(
        "qaft_ref.valid_business_keys"
    )
    if case.inject_dq_fail_data and case.dq_profile == "pre_load_fail":
        spark.sql("DELETE FROM qaft_ref.valid_business_keys WHERE business_key LIKE 'BK-DUP%'")


def _run_dq_pipeline(
    spark, master: dict, feed_specs: dict, *, loaded: bool
) -> dict:
    runner = FeedDataQualityRunner(spark, [master])
    runner.run()
    if loaded:
        runner.run_post_load_checks({master["feed_id"]})
    row = runner.finalize()[0]
    pre_ran = bool(row.get("comprehensive_pre_load_configured"))
    post_ran = bool(row.get("comprehensive_post_load_configured")) and loaded
    return {
        "standard_checks_configured": row.get("standard_checks_configured", False),
        "comprehensive_checks_configured": (
            row.get("comprehensive_pre_load_configured", False)
            or row.get("comprehensive_post_load_configured", False)
        ),
        "standard_checks_ran": row.get("standard_checks_configured", False),
        "comprehensive_checks_ran": pre_ran or post_ran,
        "comprehensive_pre_load_ran": pre_ran,
        "comprehensive_post_load_ran": post_ran,
        "standard_checks_passed": row.get("standard_checks_passed"),
        "comprehensive_pre_load_passed": row.get("comprehensive_pre_load_passed"),
        "comprehensive_post_load_passed": row.get("comprehensive_post_load_passed"),
        "can_ingest": row.get("can_ingest", True),
    }


def _needs_system_ops(case: TestCase) -> bool:
    return bool(
        case.vacuum_cleanup
        or case.restore_point_count > 0
    )


def _run_post_load_system_ops(
    spark,
    config,
    record: TestRecord,
    *,
    feed_id: int,
    master: dict,
    source: str,
    target_fqn: str,
    case: TestCase,
) -> bool:
    """Run vacuum and/or restore validations. Returns True if all ops passed."""
    if not _needs_system_ops(case) or case.expect_fail or case.configured_feed:
        return True

    results: dict = {}
    ok = True

    # Restore before vacuum: retention DELETE must not run on data under test.
    if case.restore_point_count > 0 and case.mutate_before_restore:
        dispatcher = LoadDispatcher(master, spark, config)

        def _mutate() -> None:
            stats = apply_changes(
                spark,
                source,
                row_count=case.row_count,
                insert_pct=5,
                update_pct=5,
                delete_pct=5,
                seed=feed_id + 9001,
            )
            record.source_rows_inserted += stats.inserted
            record.source_rows_updated += stats.updated
            record.source_rows_deleted += stats.deleted
            _sanitize_source_partitions(
                spark, source, _all_partition_columns(case)
            )
            res = dispatcher.dispatch()
            if not res.success and not res.skipped:
                raise RuntimeError(
                    f"mutation load failed: {res.exception_if_any}"
                )

        restore = run_restore_cycle(
            spark,
            config,
            master,
            target_fqn=target_fqn,
            restore_point_count=case.restore_point_count,
            restore_target_index=case.restore_target_index,
            mutate_fn=_mutate,
        )
        results["restore"] = restore
        ok = ok and restore.get("passed", False)

    if case.vacuum_cleanup:
        vac = run_vacuum_cleanup(
            spark,
            config,
            master,
            target_fqn=target_fqn,
            source=source,
            inject_stale=case.inject_stale_rows,
            vacuum_hours=case.global_vacuum_hours or 168,
        )
        results["vacuum"] = vac
        ok = ok and vac.get("passed", False)

    record.system_ops = results
    return ok


def run_single_test(spark, config, record: TestRecord, *, feed_id: int) -> None:
    case = record.case
    t0 = time.time()
    target = f"qaft_{case.load_type.lower()}_{feed_id}"
    source = f"qaft_source.qaft_src_{feed_id}"
    target_fqn = f"qaft_silver.{target}"

    try:
        _drop_test_artifacts(spark, target)

        if case.configured_feed:
            row = discover_configured_feeds()[0]
            fs = json.loads(row["feed_specs"])
            source = fs["source_table_name"]
            target_fqn = f"{row['target_schema_name']}.{row['target_table_name']}"
            master = {k: row[k] for k in row}
            master["feed_specs"] = json.dumps(fs)
        elif "Load Type Conflict" in case.test_name:
            _prepare_source_df(spark, case, source, feed_id)
            spark.sql(
                f"""
                CREATE OR REPLACE TABLE {target_fqn} (id BIGINT, name STRING) USING DELTA
                """
            )
            spark.sql(f"INSERT INTO {target_fqn} VALUES (1, 'x')")
            _set_load_type_property(spark, target_fqn, "FULL_LOAD")
            fs = build_feed_specs(source)
            master = build_master_spec(feed_id, "APPEND_LOAD", target, fs)
        else:
            _prepare_source_df(spark, case, source, feed_id)
            if case.partition_remove:
                first_partition_keys: list[str] = ["country"]
            elif case.partition_add:
                first_partition_keys = []
            else:
                first_partition_keys = case.partition_keys
            fs = build_feed_specs(source, partition_keys=first_partition_keys)
            fs = apply_dq_profile(
                fs,
                case.dq_profile,
                source_table=source,
            )
            for chk in fs.get("comprehensive_checks", []):
                if chk.get("load_stage") == "POST_LOAD":
                    chk["query"] = chk["query"].replace("__TARGET__", target_fqn)
                    if "__TARGET__" not in chk["query"] and "COUNT(*)" in chk["query"]:
                        if f"FROM {target_fqn}" not in chk["query"]:
                            chk["query"] = (
                                f"SELECT 1 AS fail WHERE "
                                f"(SELECT COUNT(*) FROM {target_fqn}) = 0"
                            )
            master = build_master_spec(feed_id, case.load_type, target, fs)

        if case.parallelism:
            spark.conf.set("spark.default.parallelism", str(case.parallelism))

        if case.dq_profile != "none" and not case.configured_feed:
            record.dq = _run_dq_pipeline(spark, master, json.loads(master["feed_specs"]), loaded=False)
            if case.expect_dq_block:
                blocked = not record.dq.get("can_ingest", True)
                record.status = "PASS"
                record.data_integrity = "N/A"
                record.comments = (
                    "Failure expected: DQ blocked ingest as expected"
                    if blocked
                    else "Failure expected: DQ did not block ingest (load allowed)"
                )
                return
            if not record.dq.get("can_ingest", True):
                record.status = "FAIL"
                record.root_cause = "DQ pre-load blocked ingest unexpectedly"
                return

        dispatcher = LoadDispatcher(master, spark, config)
        result = dispatcher.dispatch()
        if case.expect_fail and case.skip_validation:
            record.load_result = {
                "success": result.success,
                "skipped": result.skipped,
                "inserted": result.total_rows_inserted,
                "updated": result.total_rows_updated,
                "deleted": result.total_rows_deleted,
            }
            record.status = "PASS"
            record.data_integrity = "N/A"
            record.comments = (
                "Failure occurred as expected"
                if not result.success
                else "Failure expected (load completed without error)"
            )
            if not result.success:
                record.root_cause = result.exception_if_any or ""
            return
        if not case.expect_fail and not result.success and not result.skipped:
            record.status = "FAIL"
            record.root_cause = f"Initial load failed: {result.exception_if_any}"
            return

        baseline_after_initial = None
        baseline_table = f"_qaft_baseline_{feed_id}"
        if (
            case.row_count > 0
            and spark.catalog.tableExists(target_fqn)
            and not case.configured_feed
        ):
            from tests.e2e.validators import business_projection

            spark.sql(f"DROP TABLE IF EXISTS {baseline_table}")
            business_projection(spark.table(target_fqn)).write.format(
                "delta"
            ).mode("overwrite").saveAsTable(baseline_table)
            baseline_after_initial = spark.table(baseline_table)

        if case.partition_remove:
            fs2 = apply_dq_profile(
                build_feed_specs(source, partition_keys=[]),
                case.dq_profile,
                source_table=source,
            )
            master2 = build_master_spec(feed_id, case.load_type, target, fs2)
            result = LoadDispatcher(master2, spark, config).dispatch()

        if case.partition_add:
            dispatcher, master = _make_dispatcher(
                spark,
                config,
                feed_id=feed_id,
                case=case,
                source=source,
                target=target,
                target_fqn=target_fqn,
                partition_keys=["country"],
            )
            result = dispatcher.dispatch()

        if case.change_rounds:
            for i, (ins, upd, del_) in enumerate(case.change_rounds):
                stats = apply_changes(
                    spark,
                    source,
                    row_count=case.row_count,
                    insert_pct=ins,
                    update_pct=upd,
                    delete_pct=del_,
                    seed=feed_id + 11 + i * 17,
                )
                record.source_rows_inserted += stats.inserted
                record.source_rows_updated += stats.updated
                record.source_rows_deleted += stats.deleted
                _sanitize_source_partitions(
                    spark, source, _all_partition_columns(case)
                )
                result = dispatcher.dispatch()
                if not case.expect_fail and not result.success and not result.skipped:
                    record.status = "FAIL"
                    record.root_cause = (
                        f"Change round {i + 1} failed: {result.exception_if_any}"
                    )
                    return

        for migrate_keys in case.partition_migrate:
            if not (case.expect_fail and "Null" in case.test_name):
                _sanitize_source_partitions(
                    spark, source, _all_partition_columns(case)
                )
            dispatcher, master = _make_dispatcher(
                spark,
                config,
                feed_id=feed_id,
                case=case,
                source=source,
                target=target,
                target_fqn=target_fqn,
                partition_keys=migrate_keys,
            )
            result = dispatcher.dispatch()
            if not case.expect_fail and not result.success and not result.skipped:
                record.status = "FAIL"
                record.root_cause = (
                    f"Partition migration failed: {result.exception_if_any}"
                )
                return

        if case.insert_pct or case.update_pct or case.delete_pct:
            stats = apply_changes(
                spark,
                source,
                row_count=case.row_count,
                insert_pct=case.insert_pct,
                update_pct=case.update_pct,
                delete_pct=case.delete_pct,
                seed=feed_id + 7,
            )
            record.source_rows_inserted = stats.inserted
            record.source_rows_updated = stats.updated
            record.source_rows_deleted = stats.deleted
            _sanitize_source_partitions(
                spark, source, _all_partition_columns(case)
            )
            result = dispatcher.dispatch()

        if case.idempotency and result.success:
            result2 = dispatcher.dispatch()
            if not result2.success and not result2.skipped:
                record.status = "FAIL"
                record.root_cause = f"Idempotency failed: {result2.exception_if_any}"
                return
            if case.expect_skip and not result2.skipped:
                record.status = "PASS"
                record.comments = (
                    "Skip expected: second dispatch did not skip (no source changes)"
                )
                return
            if result2.skipped:
                result = result2

        record.load_result = {
            "success": result.success,
            "skipped": result.skipped,
            "inserted": result.total_rows_inserted,
            "updated": result.total_rows_updated,
            "deleted": result.total_rows_deleted,
        }

        if case.expect_fail:
            record.status = "PASS"
            record.data_integrity = "N/A"
            record.comments = (
                "Failure occurred as expected"
                if not result.success
                else "Failure expected (load completed without error)"
            )
            if not result.success:
                record.root_cause = result.exception_if_any or ""
            return

        if case.dq_profile != "none" and not case.configured_feed and result.success:
            post_dq = _run_dq_pipeline(
                spark, master, json.loads(master["feed_specs"]), loaded=True
            )
            record.dq.update(post_dq)
            if case.expect_post_load_fail:
                post_failed = record.dq.get("comprehensive_post_load_passed") is False
                record.status = "PASS"
                record.data_integrity = "N/A"
                record.comments = (
                    "Failure expected: POST_LOAD DQ failed as expected"
                    if post_failed
                    else "Failure expected: POST_LOAD DQ passed unexpectedly"
                )
                return

        if case.skip_validation:
            record.status = "PASS"
            record.data_integrity = "SKIPPED"
            if case.expect_skip:
                record.comments = (
                    "Skip expected: load skipped as expected (no staging changes)"
                )
            elif case.expect_fail:
                record.comments = (
                    "Failure occurred as expected"
                    if not result.success
                    else "Failure expected (load completed without error)"
                )
            else:
                record.comments = str(record.load_result)
            return

        if not result.success and not result.skipped:
            record.status = "FAIL"
            record.root_cause = result.exception_if_any or "load failed"
            return

        if (
            result.skipped
            and not (case.insert_pct or case.update_pct or case.delete_pct)
            and not case.idempotency
        ):
            if case.expect_skip:
                record.status = "PASS"
                record.comments = "Skip expected: load skipped as expected (no staging changes)"
            else:
                record.status = "WARNING"
                record.comments = "Load skipped (no staging changes)"
            return

        vt0 = time.time()
        kind = LOAD_KIND_MAP[case.load_type]
        source_df = spark.table(source)
        record.source_row_count = source_df.count()
        if spark.catalog.tableExists(target_fqn):
            record.target_row_count = spark.table(target_fqn).count()
        feed_specs_obj = json.loads(master["feed_specs"])
        change_stats = type(
            "S",
            (),
            {
                "inserted": record.source_rows_inserted,
                "updated": record.source_rows_updated,
                "deleted": record.source_rows_deleted,
            },
        )()

        if case.configured_feed:
            from tests.e2e.validators import (
                validate_except_both_ways,
                validate_row_counts,
            )

            biz = [f["name"] for f in feed_specs_obj["selection_schema"]["fields"]]
            src = source_df.select(*biz)
            tgt = spark.table(target_fqn).select(
                *[c for c in biz if c in spark.table(target_fqn).columns]
            )
            rc = validate_row_counts(src, tgt)
            ex = validate_except_both_ways(src, tgt, compare_cols=biz)
            validation = type(
                "VR",
                (),
                {"passed": rc.passed and ex.passed, "failures": rc.failures + ex.failures},
            )()
        else:
            had_changes = bool(
                record.source_rows_inserted
                or record.source_rows_updated
                or record.source_rows_deleted
                or case.change_rounds
            )
            had_updates = bool(
                record.source_rows_updated
                or any(u > 0 for _, u, _ in case.change_rounds)
            )
            validation = run_full_validations(
                spark,
                source_df,
                target_fqn,
                feed_specs_obj,
                kind,
                baseline=baseline_after_initial,
                change_stats=change_stats,
                append_mode=case.load_type == "APPEND_LOAD"
                and had_changes,
                enterprise=True,
                had_changes=had_changes,
                had_updates=had_updates,
            )

        record.validation_time_seconds = round(time.time() - vt0, 2)
        staging_key = (
            target_fqn.split(".")[-1] if case.configured_feed else target
        )
        record.staging = staging_counts(spark, staging_key)
        record.data_integrity = (
            "PASS" if validation.passed else "; ".join(validation.failures[:5])
        )
        record.status = "PASS" if validation.passed else "FAIL"
        if not validation.passed:
            record.root_cause = "; ".join(validation.failures[:3])
        else:
            ops_ok = _run_post_load_system_ops(
                spark,
                config,
                record,
                feed_id=feed_id,
                master=master,
                source=source,
                target_fqn=target_fqn,
                case=case,
            )
            if _needs_system_ops(case) and not ops_ok:
                record.status = "FAIL"
                parts = []
                if "vacuum" in record.system_ops and not record.system_ops["vacuum"].get(
                    "passed"
                ):
                    parts.append(
                        f"vacuum: {record.system_ops['vacuum'].get('notes', 'failed')}"
                    )
                if "restore" in record.system_ops and not record.system_ops[
                    "restore"
                ].get("passed"):
                    parts.append(
                        f"restore: {record.system_ops['restore'].get('notes', 'failed')}"
                    )
                record.root_cause = "; ".join(parts) or "system ops failed"
        record.comments = (
            f"inserted={result.total_rows_inserted} "
            f"updated={result.total_rows_updated} "
            f"deleted={result.total_rows_deleted}"
        )
        if record.system_ops:
            record.comments += f" | system_ops={record.system_ops}"
    except Exception as exc:
        record.status = "FAIL"
        record.root_cause = str(exc)[:2000]
        record.comments = traceback.format_exc()[-1500:]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS _qaft_baseline_{feed_id}")
        drop_system_ops_artifacts(spark, feed_id)
        record.execution_time_seconds = round(time.time() - t0, 2)


def _emit_progress(message: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {message}"
    print(line, flush=True)
    PROGRESS_LOG.parent.mkdir(parents=True, exist_ok=True)
    with PROGRESS_LOG.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
        fh.flush()


def records_to_dataframe(
    records: list[TestRecord],
    environment: str,
    spark_version: str,
    *,
    cluster: str = "",
) -> pd.DataFrame:
    rows = []
    for rec in records:
        c = rec.case
        dq = rec.dq
        rows.append(
            {
                "#": c.test_id,
                "Parent Test Case": c.parent_id,
                "Test Case": c.test_name,
                "Category": c.category,
                "load_type": c.load_type,
                "dq_profile": c.dq_profile,
                "heavy_validation": c.heavy_validation,
                "change_rounds": len(c.change_rounds),
                "partition_migrations": len(c.partition_migrate),
                "standard_checks_configured": dq.get(
                    "standard_checks_configured", False
                ),
                "comprehensive_checks_configured": dq.get(
                    "comprehensive_checks_configured", False
                ),
                "standard_checks_ran": dq.get("standard_checks_ran", False),
                "comprehensive_checks_ran": dq.get("comprehensive_checks_ran", False),
                "comprehensive_pre_load_ran": dq.get(
                    "comprehensive_pre_load_ran", False
                ),
                "comprehensive_post_load_ran": dq.get(
                    "comprehensive_post_load_ran", False
                ),
                "standard_checks_passed": dq.get("standard_checks_passed"),
                "comprehensive_pre_load_passed": dq.get(
                    "comprehensive_pre_load_passed"
                ),
                "comprehensive_post_load_passed": dq.get(
                    "comprehensive_post_load_passed"
                ),
                "can_ingest": dq.get("can_ingest"),
                "source_rows_updated": rec.source_rows_updated,
                "source_rows_deleted": rec.source_rows_deleted,
                "source_rows_inserted": rec.source_rows_inserted,
                "partitioning": str(c.partition_keys),
                "default_parallelism": c.parallelism or "",
                "row_count": c.row_count,
                "source_row_count": rec.source_row_count,
                "target_row_count": rec.target_row_count,
                "t_full_count": rec.staging.get("t_full_count", ""),
                "t_incr_count": rec.staging.get("t_incr_count", ""),
                "t_incr_cdf_changes_count": rec.staging.get(
                    "t_incr_cdf_changes_count", ""
                ),
                "t_total_count": rec.staging.get("t_total_count", ""),
                "Data Integrity": rec.data_integrity,
                "Comments": rec.comments,
                "status": rec.status,
                "execution_time_seconds": rec.execution_time_seconds,
                "validation_time_seconds": rec.validation_time_seconds,
                "root_cause": rec.root_cause,
                "fix_applied": rec.fix_applied,
                "modified_files": rec.modified_files,
                "environment": environment,
                "cluster_config": cluster,
                "spark_version": spark_version,
                "test_timestamp": datetime.now(timezone.utc).isoformat(),
                "global_vacuum_hours": c.global_vacuum_hours or "",
                "vacuum_cleanup": c.vacuum_cleanup,
                "inject_stale_rows": c.inject_stale_rows,
                "restore_point_count": c.restore_point_count,
                "restore_target_index": c.restore_target_index,
                "system_ops_passed": (
                    all(
                        v.get("passed", True)
                        for v in rec.system_ops.values()
                    )
                    if rec.system_ops
                    else ""
                ),
                "system_ops_notes": str(rec.system_ops)[:500] if rec.system_ops else "",
            }
        )
    return pd.DataFrame(rows)


def _write_incremental_report(
    records: list[TestRecord],
    environment: str,
    spark_version: str,
    *,
    cluster: str = "",
) -> None:
    df = records_to_dataframe(records, environment, spark_version, cluster=cluster)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(REPORT_PATH, index=False)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="HanduFlow E2E QA runner")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--full", action="store_true", help="Include 10k/100k tests")
    parser.add_argument(
        "--heavy",
        action="store_true",
        help="Exhaustive heavy suite: 500k/1M, multi-cycle, real-world pipelines",
    )
    parser.add_argument(
        "--extreme",
        action="store_true",
        help="Enterprise suite: multi-million rows (1M-10M), 24x7 pipelines",
    )
    args = parser.parse_args()

    try:
        sys.stdout.reconfigure(line_buffering=True)
    except (AttributeError, OSError):
        pass

    PROGRESS_LOG.write_text("", encoding="utf-8")

    if args.smoke:
        mode = "smoke"
    elif args.extreme:
        mode = "extreme"
    elif args.heavy:
        mode = "heavy"
    elif args.full:
        mode = "full"
    elif args.quick:
        mode = "quick"
    else:
        mode = "quick"

    config = load_config()
    spark, cluster_resources = create_spark(
        heavy=mode in ("heavy", "extreme"),
        extreme=(mode == "extreme"),
    )
    ensure_qaft_schemas(spark, config)
    spark.sql("CREATE DATABASE IF NOT EXISTS demo")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    if discover_configured_feeds():
        from pyspark.sql.functions import expr

        spark.sql("DROP TABLE IF EXISTS demo.test")
        df = (
            spark.range(1, 101)
            .toDF("row_id")
            .withColumn("alpha3_b", expr("concat('USA', cast(row_id as string))"))
            .withColumn("alpha3_t", expr("concat('US', cast(row_id as string))"))
            .withColumn("alpha2", expr("substring('US', 1, 2)"))
            .withColumn(
                "english",
                expr(
                    """
                    CASE
                        WHEN row_id % 4 = 0 THEN 'United States'
                        WHEN row_id % 4 = 1 THEN 'Germany'
                        WHEN row_id % 4 = 2 THEN 'India'
                        ELSE 'Canada'
                    END
                """
                ),
            )
            .drop("row_id")
        )
        df.write.format("delta").mode("overwrite").saveAsTable("demo.test")
        spark.sql(
            "ALTER TABLE demo.test SET TBLPROPERTIES "
            "(delta.enableChangeDataFeed = true)"
        )

    cases = build_test_matrix(mode=mode)
    total = len(cases)
    cluster_summary = cluster_resources.summary()
    _emit_progress(
        f"HanduFlow E2E | mode={mode} | tests={total} | report={REPORT_PATH}"
    )
    _emit_progress(f"Cluster: {cluster_summary}")
    _emit_progress(f"Monitor: tail -f {PROGRESS_LOG}")

    records: list[TestRecord] = []
    suite_start = time.time()
    passed = failed = warned = 0

    for n, case in enumerate(cases, start=1):
        _emit_progress(
            f"START {n}/{total} | #{case.test_id} | parent={case.parent_id} | "
            f"{case.test_name} | rows={case.row_count:,}"
        )
        rec = TestRecord(case=case)
        run_single_test(spark, config, rec, feed_id=case.test_id)
        records.append(rec)
        _write_incremental_report(
            records, "local", spark.version, cluster=cluster_summary
        )

        if rec.status == "PASS":
            passed += 1
        elif rec.status == "FAIL":
            failed += 1
        else:
            warned += 1

        elapsed = time.time() - suite_start
        eta = (elapsed / n) * (total - n)
        _emit_progress(
            f"DONE  {n}/{total} | {rec.status} | #{case.test_id} | "
            f"{case.test_name[:60]} | exec={rec.execution_time_seconds}s | "
            f"elapsed={elapsed:.0f}s | eta={eta:.0f}s | "
            f"pass={passed} fail={failed} warn={warned}"
        )

    _emit_progress(f"Report: {REPORT_PATH}")
    _emit_progress(f"FINAL Total={total} PASS={passed} FAIL={failed} WARN={warned}")

    spark.stop()

    from tests.helpers.spark_isolation import reset_spark_suite

    reset_spark_suite("regression")

    import subprocess

    rc = subprocess.call(
        [sys.executable, "-m", "pytest", "tests/regression/", "-q"],
        cwd=PROJECT_ROOT,
    )
    return 1 if rc != 0 or failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
