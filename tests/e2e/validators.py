"""E2E validation suite (12 checks)."""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from handuflow.data_movement_controller.audit_columns import (
    TARGET_ROW_HASH_COLUMN,
    AuditColumns,
    TargetLoadKind,
)

from tests.e2e.data_generator import BUSINESS_COLUMNS, business_projection


@dataclass
class ValidationResult:
    passed: bool
    details: dict = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)


def _hash_expr(columns: list[str]):
    """Business-row fingerprint (full column set) for FULL_LOAD / APPEND checks."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in columns]
    return F.sha2(F.concat_ws("|", *parts), 256)


def _key_columns(feed_specs: dict) -> list[str]:
    primary = feed_specs.get("primary_key")
    composite = feed_specs.get("composite_key") or []
    keys = [primary] if primary else []
    keys.extend(k for k in composite if k not in keys)
    return keys


def _production_hash_expr(feed_specs: dict):
    """Same formula HanduFlow uses for CDC / SCD ``_x_row_hash``."""
    keys = _key_columns(feed_specs)
    non_key = AuditColumns.non_key_business_columns(feed_specs, keys)
    return AuditColumns.row_hash_expr(non_key)


def _business_cols(df: DataFrame) -> list[str]:
    return [c for c in BUSINESS_COLUMNS if c in df.columns]


def validate_row_counts(source: DataFrame, target: DataFrame) -> ValidationResult:
    s = source.count()
    t = target.count()
    ok = s == t
    return ValidationResult(
        ok,
        {"source_count": s, "target_count": t},
        [] if ok else [f"row count mismatch source={s} target={t}"],
    )


def validate_except_both_ways(
    source: DataFrame, target: DataFrame, *, compare_cols: list[str] | None = None
) -> ValidationResult:
    cols = compare_cols or _business_cols(source)
    src = source.select(*[c for c in cols if c in source.columns]).distinct()
    tgt = target.select(*[c for c in cols if c in target.columns]).distinct()
    s_minus_t = src.exceptAll(tgt).count()
    t_minus_s = tgt.exceptAll(src).count()
    ok = s_minus_t == 0 and t_minus_s == 0
    failures = []
    if s_minus_t:
        failures.append(f"source EXCEPT target: {s_minus_t} rows")
    if t_minus_s:
        failures.append(f"target EXCEPT source: {t_minus_s} rows")
    return ValidationResult(
        ok,
        {"source_minus_target": s_minus_t, "target_minus_source": t_minus_s},
        failures,
    )


def validate_hashes(source: DataFrame, target: DataFrame, cols: list[str]) -> ValidationResult:
    src = source.withColumn("_h", _hash_expr(cols))
    tgt = target.withColumn("_h", _hash_expr(cols))
    s_total = src.count()
    t_total = tgt.count()
    s_dist = src.select("_h").distinct().count()
    t_dist = tgt.select("_h").distinct().count()
    s_counts = src.groupBy("_h").agg(F.count(F.lit(1)).alias("s_cnt"))
    t_counts = tgt.groupBy("_h").agg(F.count(F.lit(1)).alias("t_cnt"))
    mismatch = (
        s_counts.join(t_counts, on="_h", how="full_outer")
        .filter(
            F.coalesce(F.col("s_cnt"), F.lit(0))
            != F.coalesce(F.col("t_cnt"), F.lit(0))
        )
        .count()
    )
    ok = s_total == t_total and s_dist == t_dist and mismatch == 0
    return ValidationResult(
        ok,
        {
            "source_total": s_total,
            "target_total": t_total,
            "source_distinct_hashes": s_dist,
            "target_distinct_hashes": t_dist,
        },
        [] if ok else ["hash reconciliation mismatch"],
    )


def _agg_close(a, b, *, rtol: float = 1e-9, atol: float = 1e-6) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(float(a) - float(b)) <= atol + rtol * max(abs(float(a)), abs(float(b)))
    return a == b


def validate_aggregates(source: DataFrame, target: DataFrame) -> ValidationResult:
    failures = []
    details = {}
    for label, expr in [
        ("count_star", F.count(F.lit(1))),
        ("count_distinct_id", F.countDistinct("id")),
        ("sum_amount", F.sum("amount")),
        ("avg_amount", F.avg("amount")),
        ("min_amount", F.min("amount")),
        ("max_amount", F.max("amount")),
    ]:
        if "amount" in label and "amount" not in source.columns:
            continue
        s = source.agg(expr.alias("v")).collect()[0][0]
        t = target.agg(expr.alias("v")).collect()[0][0]
        details[f"source_{label}"] = s
        details[f"target_{label}"] = t
        if not _agg_close(s, t):
            failures.append(f"{label}: source={s} target={t}")
    return ValidationResult(len(failures) == 0, details, failures)


def validate_nulls(source: DataFrame, target: DataFrame) -> ValidationResult:
    failures = []
    details = {}
    for col in _business_cols(source):
        if col not in target.columns:
            continue
        s_null = source.filter(F.col(col).isNull()).count()
        t_null = target.filter(F.col(col).isNull()).count()
        details[col] = {"source": s_null, "target": t_null}
        if s_null != t_null:
            failures.append(f"nulls in {col}: source={s_null} target={t_null}")
    return ValidationResult(len(failures) == 0, details, failures)


def validate_duplicates(
    df: DataFrame, key_col: str = "id", *, allow_dupes: bool = False
) -> ValidationResult:
    total = df.count()
    distinct = df.select(key_col).distinct().count()
    ok = allow_dupes or total == distinct
    return ValidationResult(
        ok,
        {"count": total, "distinct_keys": distinct},
        [] if ok else [f"duplicate keys: count={total} distinct={distinct}"],
    )


def validate_schema(
    spark: SparkSession,
    table: str,
    feed_specs: dict,
    load_kind: TargetLoadKind,
) -> ValidationResult:
    try:
        AuditColumns.assert_target_schema(spark, table, feed_specs, load_kind)
        return ValidationResult(True, {"table": table}, [])
    except Exception as exc:
        return ValidationResult(False, {}, [str(exc)])


def validate_scd2(spark: SparkSession, table: str, business_key: str = "id") -> ValidationResult:
    df = spark.table(table)
    failures = []
    active = df.filter("_x_is_active = 1")
    active_per_key = active.groupBy(business_key).count().filter("count > 1").count()
    if active_per_key:
        failures.append(f"{active_per_key} keys with multiple active rows")
    overlap = (
        df.alias("a")
        .join(
            df.alias("b"),
            (F.col(f"a.{business_key}") == F.col(f"b.{business_key}"))
            & (F.col("a._x_surrogate_key") < F.col("b._x_surrogate_key"))
            & (F.col("a._x_date_from") < F.col("b._x_date_to"))
            & (F.col("b._x_date_from") < F.col("a._x_date_to")),
            "inner",
        )
        .count()
    )
    if overlap:
        failures.append(f"{overlap} overlapping SCD date ranges")
    null_active = df.filter("_x_is_active IS NULL").count()
    if null_active:
        failures.append(f"{null_active} rows with null _x_is_active")
    return ValidationResult(
        len(failures) == 0,
        {
            "active_rows": active.count(),
            "total_rows": df.count(),
        },
        failures,
    )


def validate_append_behavior(
    baseline_target: DataFrame,
    source_after: DataFrame,
    target: DataFrame,
    change_stats,
) -> ValidationResult:
    """APPEND: new inserts only; updates/deletes ignored."""
    failures = []
    base_cols = _business_cols(baseline_target)
    tgt = target.select(*base_cols)
    base_tgt = baseline_target.select(*base_cols)

    if change_stats.deleted:
        deleted_ids = (
            base_tgt.select("id")
            .join(source_after.select("id"), on="id", how="left_anti")
            .count()
        )
        still_in_target = (
            base_tgt.select("id")
            .join(source_after.select("id"), on="id", how="left_anti")
            .join(target.select("id"), on="id", how="inner")
            .count()
        )
        if still_in_target < deleted_ids:
            failures.append(
                f"deleted source rows missing from append target "
                f"({deleted_ids - still_in_target} missing)"
            )

    if change_stats.updated:
        updated_in_source = source_after.filter(F.col("name").contains("-UPDATED"))
        for row in updated_in_source.limit(20).collect():
            old = base_tgt.filter(F.col("id") == row.id).collect()
            cur = tgt.filter(F.col("id") == row.id).collect()
            if old and cur and cur[0].name != old[0].name:
                failures.append("append target reflects updates (should be unchanged)")
                break

    new_source_ids = source_after.select("id").join(
        base_tgt.select("id"), on="id", how="left_anti"
    )
    new_in_target = new_source_ids.join(target.select("id"), on="id", how="inner").count()
    new_count = new_source_ids.count()
    if new_in_target < new_count:
        failures.append(
            f"new source rows not appended: expected {new_count} found {new_in_target}"
        )

    if target.count() < baseline_target.count():
        failures.append("append target row count decreased (should never shrink)")

    return ValidationResult(
        len(failures) == 0,
        {
            "baseline_target": baseline_target.count(),
            "target": target.count(),
            "new_source_ids": new_count,
            "new_in_target": new_in_target,
            "inserted": change_stats.inserted,
            "updated": change_stats.updated,
            "deleted": change_stats.deleted,
        },
        failures,
    )


def validate_incremental_cdc_staging(
    spark: SparkSession,
    target_table: str,
    *,
    source_count: int,
    had_changes: bool,
) -> ValidationResult:
    """CDC must keep t_full = source; t_incr populated when source changed."""
    counts = staging_counts(spark, target_table)
    failures = []
    full_count = counts.get("t_full_count", 0)
    incr_count = counts.get("t_incr_count", 0)
    if full_count != source_count:
        failures.append(
            f"t_full count {full_count} != source count {source_count}"
        )
    if had_changes and incr_count == 0:
        failures.append("t_incr empty after source changes (CDC should capture delta)")
    if counts.get("t_incr_cdf_changes_count", 0) == 0 and had_changes:
        failures.append("t_incr_cdf_changes empty after source changes")
    return ValidationResult(len(failures) == 0, counts, failures)


def validate_append_staging(
    spark: SparkSession,
    target_table: str,
    *,
    had_changes: bool,
) -> ValidationResult:
    """Append load uses t_incr for inserts; t_full tracks full snapshot."""
    counts = staging_counts(spark, target_table)
    failures = []
    if counts.get("t_full_count", 0) == 0:
        failures.append("t_full must be populated for APPEND_LOAD")
    if had_changes and counts.get("t_incr_count", 0) == 0:
        failures.append("t_incr empty after source inserts (append should capture new rows)")
    return ValidationResult(len(failures) == 0, counts, failures)


def validate_scd_history_after_changes(
    spark: SparkSession,
    table: str,
    *,
    had_updates: bool,
) -> ValidationResult:
    """SCD Type 2 must retain history rows when attributes change."""
    df = spark.table(table)
    failures = []
    total = df.count()
    active = df.filter("_x_is_active = 1").count()
    inactive = df.filter("_x_is_active = 0").count()
    if had_updates and inactive == 0:
        failures.append("no inactive history rows after source updates")
    if active > total:
        failures.append(f"active rows ({active}) exceed total ({total})")
    return ValidationResult(
        len(failures) == 0,
        {"total": total, "active": active, "inactive": inactive},
        failures,
    )


def validate_partition_coverage(
    spark: SparkSession,
    table: str,
    partition_keys: list[str],
) -> ValidationResult:
    """Ensure partitioned tables have rows in every declared partition bucket."""
    if not partition_keys:
        return ValidationResult(True, {}, [])
    df = spark.table(table)
    failures = []
    for key in partition_keys:
        if key not in df.columns:
            failures.append(f"partition column {key} missing from {table}")
            continue
        nulls = df.filter(F.col(key).isNull()).count()
        if nulls:
            failures.append(f"{nulls} null values in partition column {key}")
        distinct_parts = df.select(key).distinct().count()
        if distinct_parts == 0:
            failures.append(f"no partition values for {key}")
    return ValidationResult(len(failures) == 0, {"partition_keys": partition_keys}, failures)


def validate_cdc_hash_column(
    spark: SparkSession,
    target_table: str,
    source: DataFrame,
    feed_specs: dict,
) -> ValidationResult:
    """CDC ``_x_row_hash`` must match production ``AuditColumns.row_hash_expr``."""
    key_col = feed_specs.get("primary_key") or "id"
    if TARGET_ROW_HASH_COLUMN not in spark.table(target_table).columns:
        return ValidationResult(
            False, {}, [f"{TARGET_ROW_HASH_COLUMN} missing from CDC target"]
        )
    tgt = spark.table(target_table)
    hash_expr = _production_hash_expr(feed_specs)
    src_h = source.withColumn("_h", hash_expr).select(
        key_col, F.col("_h").alias("src_h")
    )
    tgt_h = tgt.select(key_col, F.col(TARGET_ROW_HASH_COLUMN).alias("tgt_h"))
    joined = src_h.join(tgt_h, on=key_col, how="full_outer")
    mismatch = joined.filter(
        F.col("src_h").isNull()
        | F.col("tgt_h").isNull()
        | (F.col("src_h") != F.col("tgt_h"))
    ).count()
    return ValidationResult(
        mismatch == 0,
        {"hash_mismatches": mismatch},
        [] if mismatch == 0 else [f"{mismatch} rows with hash/key mismatch"],
    )


def validate_key_coverage(
    source: DataFrame,
    target: DataFrame,
    key_col: str = "id",
) -> ValidationResult:
    """Every source key must exist in target and vice-versa."""
    src_keys = source.select(key_col).distinct()
    tgt_keys = target.select(key_col).distinct()
    missing_in_tgt = src_keys.join(tgt_keys, on=key_col, how="left_anti").count()
    missing_in_src = tgt_keys.join(src_keys, on=key_col, how="left_anti").count()
    failures = []
    if missing_in_tgt:
        failures.append(f"{missing_in_tgt} source keys missing from target")
    if missing_in_src:
        failures.append(f"{missing_in_src} target keys missing from source")
    return ValidationResult(
        len(failures) == 0,
        {
            "missing_in_target": missing_in_tgt,
            "missing_in_source": missing_in_src,
        },
        failures,
    )


def validate_enterprise_integrity(
    spark: SparkSession,
    source: DataFrame,
    target_table: str,
    feed_specs: dict,
    load_kind: TargetLoadKind,
    *,
    had_changes: bool = False,
    had_updates: bool = False,
) -> ValidationResult:
    """Enterprise-grade integrity: staging rules, CDC hash, partitions, history."""
    failures: list[str] = []
    details: dict = {}
    partition_keys = list(feed_specs.get("partition_keys") or [])

    part = validate_partition_coverage(spark, target_table, partition_keys)
    details["partition_coverage"] = part.details
    failures.extend(part.failures)

    if load_kind == TargetLoadKind.INCREMENTAL_CDC:
        src_count = business_projection(source).count()
        cdc_stg = validate_incremental_cdc_staging(
            spark, target_table, source_count=src_count, had_changes=had_changes
        )
        details["cdc_staging"] = cdc_stg.details
        failures.extend(cdc_stg.failures)
        cdc_hash = validate_cdc_hash_column(
            spark, target_table, business_projection(source), feed_specs
        )
        details["cdc_hash"] = cdc_hash.details
        failures.extend(cdc_hash.failures)
        key_cov = validate_key_coverage(
            business_projection(source),
            spark.table(target_table),
            key_col=feed_specs.get("primary_key") or "id",
        )
        details["key_coverage"] = key_cov.details
        failures.extend(key_cov.failures)

    if load_kind == TargetLoadKind.APPEND_LOAD:
        app_stg = validate_append_staging(spark, target_table, had_changes=had_changes)
        details["append_staging"] = app_stg.details
        failures.extend(app_stg.failures)

    if load_kind == TargetLoadKind.SCD_TYPE_2:
        scd_hist = validate_scd_history_after_changes(
            spark, target_table, had_updates=had_updates
        )
        details["scd_history"] = scd_hist.details
        failures.extend(scd_hist.failures)

    if load_kind == TargetLoadKind.FULL_LOAD:
        fl_stg = validate_full_load_staging(
            spark,
            target_table,
            source_count=business_projection(source).count(),
        )
        details["full_load_staging"] = fl_stg.details
        failures.extend(fl_stg.failures)

    return ValidationResult(len(failures) == 0, details, failures)


def run_full_validations(
    spark: SparkSession,
    source: DataFrame,
    target_table: str,
    feed_specs: dict,
    load_kind: TargetLoadKind,
    *,
    baseline: DataFrame | None = None,
    change_stats=None,
    append_mode: bool = False,
    enterprise: bool = True,
    had_changes: bool = False,
    had_updates: bool = False,
) -> ValidationResult:
    target = spark.table(target_table)
    if load_kind == TargetLoadKind.INCREMENTAL_CDC:
        src = business_projection(source)
        tgt = target.drop(TARGET_ROW_HASH_COLUMN)
    elif load_kind == TargetLoadKind.SCD_TYPE_2:
        src = business_projection(source)
        tgt = target.filter("_x_is_active = 1").select(
            *[c for c in BUSINESS_COLUMNS if c in target.columns]
        )
    elif load_kind == TargetLoadKind.APPEND_LOAD and append_mode and baseline is not None:
        return validate_append_behavior(baseline, source, target, change_stats)
    else:
        src = business_projection(source)
        tgt = business_projection(target)

    checks = [
        ("row_count", validate_row_counts(src, tgt)),
        ("except", validate_except_both_ways(src, tgt)),
        ("hash", validate_hashes(src, tgt, _business_cols(src))),
        ("aggregates", validate_aggregates(src, tgt)),
        ("nulls", validate_nulls(src, tgt)),
        ("duplicates", validate_duplicates(tgt)),
        (
            "schema",
            validate_schema(spark, target_table, feed_specs, load_kind),
        ),
    ]
    if load_kind == TargetLoadKind.FULL_LOAD:
        checks.insert(
            0,
            (
                "full_load_staging",
                validate_full_load_staging(
                    spark,
                    target_table,
                    source_count=src.count(),
                ),
            ),
        )
    if load_kind == TargetLoadKind.SCD_TYPE_2:
        checks.append(("scd2", validate_scd2(spark, target_table)))

    key_col = feed_specs.get("primary_key") or "id"
    checks.append(("key_coverage", validate_key_coverage(src, tgt, key_col=key_col)))

    if load_kind == TargetLoadKind.INCREMENTAL_CDC:
        checks.append(
            (
                "cdc_hash",
                validate_cdc_hash_column(
                    spark, target_table, src, feed_specs
                ),
            )
        )
        if had_changes:
            checks.append(
                (
                    "cdc_staging",
                    validate_incremental_cdc_staging(
                        spark,
                        target_table,
                        source_count=src.count(),
                        had_changes=True,
                    ),
                )
            )

    if enterprise:
        checks.append(
            (
                "enterprise_integrity",
                validate_enterprise_integrity(
                    spark,
                    source,
                    target_table,
                    feed_specs,
                    load_kind,
                    had_changes=had_changes,
                    had_updates=had_updates,
                ),
            )
        )

    failures: list[str] = []
    details: dict = {}
    for name, result in checks:
        details[name] = result.details
        failures.extend([f"{name}: {f}" for f in result.failures])

    return ValidationResult(len(failures) == 0, details, failures)


def staging_counts(spark: SparkSession, target_table: str) -> dict:
    base = target_table.split(".")[-1]
    counts = {}
    for suffix, key in [
        (f"t_full_{base}", "t_full_count"),
        (f"t_incr_{base}", "t_incr_count"),
        (f"t_incr_cdf_changes_{base}", "t_incr_cdf_changes_count"),
    ]:
        name = f"staging.{suffix}"
        counts[key] = (
            spark.table(name).count() if spark.catalog.tableExists(name) else 0
        )
    counts["t_total_count"] = sum(counts.values())
    return counts


def validate_full_load_staging(
    spark: SparkSession,
    target_table: str,
    *,
    source_count: int,
) -> ValidationResult:
    """FULL_LOAD uses only t_full; staging row count must equal source."""
    counts = staging_counts(spark, target_table)
    failures = []
    if counts.get("t_incr_count", 0) != 0:
        failures.append(
            f"t_incr must be empty for FULL_LOAD (got {counts['t_incr_count']})"
        )
    if counts.get("t_incr_cdf_changes_count", 0) != 0:
        failures.append(
            "t_incr_cdf_changes must be empty for FULL_LOAD "
            f"(got {counts['t_incr_cdf_changes_count']})"
        )
    full_count = counts.get("t_full_count", 0)
    if full_count != source_count:
        failures.append(
            f"t_full count {full_count} != source count {source_count}"
        )
    return ValidationResult(
        len(failures) == 0,
        {**counts, "source_count": source_count},
        failures,
    )
