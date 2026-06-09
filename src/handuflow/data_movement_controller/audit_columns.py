"""
HanduFlow audit / lineage columns: staging vs target by load type.

Staging (``staging.t_full_*``, ``staging.t_incr_*``) may contain:
  _x_load_id, _x_row_hash, _x_commit_version, _x_commit_timestamp, _x_operation

Target tables only persist columns allowed for each ``load_type``.
"""

from __future__ import annotations

import logging
from enum import Enum

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

from handuflow.exception.data_load_exception import DataLoadException

logger = logging.getLogger(__name__)

# Written only to staging tables during MERGE / CDF — never to silver/gold targets.
STAGING_ONLY_COLUMNS = frozenset(
    {"_x_load_id", "_x_commit_version", "_x_commit_timestamp"}
)

# Present on incremental staging stream; used during MERGE, not stored on APPEND/FULL targets.
CDC_STREAM_COLUMN = "_x_operation"

# Row-change fingerprint stored on INCREMENTAL_CDC targets.
TARGET_ROW_HASH_COLUMN = "_x_row_hash"

# SCD Type 2 target metadata (in addition to business columns from selection_schema).
SCD_TARGET_COLUMNS = frozenset(
    {
        "_x_surrogate_key",
        TARGET_ROW_HASH_COLUMN,
        "_x_date_from",
        "_x_date_to",
        "_x_is_active",
        "_x_last_modification_timestamp",
        "_x_last_operation",
    }
)


class TargetLoadKind(str, Enum):
    FULL_LOAD = "FULL_LOAD"
    APPEND_LOAD = "APPEND_LOAD"
    INCREMENTAL_CDC = "INCREMENTAL_CDC"
    SCD_TYPE_2 = "SCD_TYPE_2"


class AuditColumns:
    """Strip staging noise and align DataFrames with per-load-type target schemas."""

    @staticmethod
    def business_columns(feed_specs: dict) -> list[str]:
        return [
            f.name
            for f in StructType.fromJson(feed_specs["selection_schema"]).fields
        ]

    @staticmethod
    def non_key_business_columns(feed_specs: dict, key_columns: list[str]) -> list[str]:
        keys = set(key_columns)
        return [c for c in AuditColumns.business_columns(feed_specs) if c not in keys]

    @staticmethod
    def drop_staging_only(df: DataFrame) -> DataFrame:
        drop = [c for c in STAGING_ONLY_COLUMNS if c in df.columns]
        if drop:
            logger.info("Dropping staging-only columns: %s", drop)
            df = df.drop(*drop)
        return df

    @staticmethod
    def drop_columns_present(df: DataFrame, columns: frozenset[str] | set[str]) -> DataFrame:
        drop = [c for c in columns if c in df.columns]
        return df.drop(*drop) if drop else df

    @staticmethod
    def row_hash_expr(columns: list[str]):
        if not columns:
            return F.lit("")
        return F.sha2(
            F.concat_ws("||", *[F.col(c).cast("string") for c in columns]),
            256,
        )

    @staticmethod
    def enforce_business_schema(df: DataFrame, feed_specs: dict) -> DataFrame:
        """Project to ``selection_schema`` only (no audit columns)."""
        from handuflow.data_movement_controller.load_integrity import (
            LoadIntegrityVerifier,
        )

        schema = StructType.fromJson(feed_specs["selection_schema"])
        business = [f.name for f in schema.fields if f.name in df.columns]
        subset = df.select(*business) if business else df
        return LoadIntegrityVerifier.enforce_schema(subset, schema)

    @staticmethod
    def expected_target_columns(feed_specs: dict, load_kind: TargetLoadKind) -> list[str]:
        business = AuditColumns.business_columns(feed_specs)
        if load_kind == TargetLoadKind.FULL_LOAD:
            return business
        if load_kind == TargetLoadKind.APPEND_LOAD:
            return business
        if load_kind == TargetLoadKind.INCREMENTAL_CDC:
            return business + [TARGET_ROW_HASH_COLUMN]
        if load_kind == TargetLoadKind.SCD_TYPE_2:
            return business + sorted(SCD_TARGET_COLUMNS)
        raise DataLoadException(
            message=f"Unknown load kind: {load_kind}",
            error_code="HF040",
        )

    @staticmethod
    def assert_target_schema(
        spark: SparkSession,
        table_name: str,
        feed_specs: dict,
        load_kind: TargetLoadKind,
    ) -> None:
        if not spark.catalog.tableExists(table_name):
            return
        actual = sorted(spark.table(table_name).columns)
        expected = sorted(AuditColumns.expected_target_columns(feed_specs, load_kind))
        if actual != expected:
            raise DataLoadException(
                message=(
                    f"Target table {table_name} columns {actual} do not match "
                    f"expected for {load_kind.value}: {expected}. "
                    "Drop the table or align load_type with how the table was created."
                ),
                original_exception=None,
            )

    @staticmethod
    def ensure_row_hash_on_target(
        spark: SparkSession,
        table_name: str,
        key_columns: list[str],
        hash_columns: list[str],
    ) -> None:
        if not spark.catalog.tableExists(table_name):
            return
        if TARGET_ROW_HASH_COLUMN in spark.table(table_name).columns:
            return
        logger.info("Adding %s to target table %s", TARGET_ROW_HASH_COLUMN, table_name)
        spark.sql(
            f"ALTER TABLE {table_name} ADD COLUMNS ({TARGET_ROW_HASH_COLUMN} STRING)"
        )
        hash_expr = AuditColumns.row_hash_expr(hash_columns)
        key_condition = " AND ".join([f"t.{k} = s.{k}" for k in key_columns])
        src = spark.table(table_name).withColumn("_x_row_hash_new", hash_expr)
        (
            DeltaTable.forName(spark, table_name)
            .alias("t")
            .merge(src.alias("s"), key_condition)
            .whenMatchedUpdate(set={TARGET_ROW_HASH_COLUMN: "s._x_row_hash_new"})
            .execute()
        )

    # --- Per load type: staging → target-ready DataFrame ---

    @staticmethod
    def prepare_full_load_snapshot(
        staging_full_df: DataFrame, feed_specs: dict
    ) -> DataFrame:
        df = AuditColumns.drop_staging_only(staging_full_df)
        df = AuditColumns.drop_columns_present(
            df, {TARGET_ROW_HASH_COLUMN, CDC_STREAM_COLUMN}
        )
        return AuditColumns.enforce_business_schema(df, feed_specs)

    @staticmethod
    def prepare_append_inserts(
        staging_incr_df: DataFrame, feed_specs: dict
    ) -> DataFrame:
        df = AuditColumns.drop_staging_only(staging_incr_df)
        if CDC_STREAM_COLUMN in df.columns:
            df = df.filter(f"{CDC_STREAM_COLUMN} = 'insert'")
        df = AuditColumns.drop_columns_present(
            df,
            STAGING_ONLY_COLUMNS
            | {TARGET_ROW_HASH_COLUMN, CDC_STREAM_COLUMN, "_x_commit_version", "_x_commit_timestamp"},
        )
        return AuditColumns.enforce_business_schema(df, feed_specs)

    @staticmethod
    def prepare_cdc_stream(
        staging_incr_df: DataFrame,
        feed_specs: dict,
        key_columns: list[str],
    ) -> DataFrame:
        """Business + ``_x_operation`` + ``_x_row_hash`` for CDC MERGE source."""
        df = AuditColumns.drop_staging_only(staging_incr_df)
        df = AuditColumns.drop_columns_present(
            df,
            {TARGET_ROW_HASH_COLUMN, "_x_commit_version", "_x_commit_timestamp"},
        )
        non_key = AuditColumns.non_key_business_columns(feed_specs, key_columns)
        df = df.withColumn(TARGET_ROW_HASH_COLUMN, AuditColumns.row_hash_expr(non_key))
        business = AuditColumns.business_columns(feed_specs)
        merge_cols = business + [CDC_STREAM_COLUMN, TARGET_ROW_HASH_COLUMN]
        missing = [c for c in merge_cols if c not in df.columns]
        if missing:
            raise DataLoadException(
                message=f"CDC stream missing columns after staging cleanup: {missing}",
                original_exception=None,
            )
        schema = StructType.fromJson(feed_specs["selection_schema"])
        from handuflow.data_movement_controller.load_integrity import (
            LoadIntegrityVerifier,
        )

        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(
                    field.name, F.col(field.name).cast(field.dataType)
                )
        return df.select(*business, CDC_STREAM_COLUMN, TARGET_ROW_HASH_COLUMN)

    @staticmethod
    def prepare_cdc_initial_table(
        cdc_stream_df: DataFrame, feed_specs: dict
    ) -> DataFrame:
        """First CDC create: business + ``_x_row_hash`` only."""
        business = AuditColumns.business_columns(feed_specs)
        return cdc_stream_df.select(*business, TARGET_ROW_HASH_COLUMN)

    @staticmethod
    def prepare_cdc_full_snapshot(
        staging_full_df: DataFrame,
        feed_specs: dict,
        key_columns: list[str],
    ) -> DataFrame:
        """Full staging snapshot as CDC target layout (business + row hash)."""
        df = AuditColumns.drop_staging_only(staging_full_df)
        df = AuditColumns.drop_columns_present(
            df,
            {
                CDC_STREAM_COLUMN,
                TARGET_ROW_HASH_COLUMN,
                "_x_commit_version",
                "_x_commit_timestamp",
            },
        )
        non_key = AuditColumns.non_key_business_columns(feed_specs, key_columns)
        df = df.withColumn(
            TARGET_ROW_HASH_COLUMN, AuditColumns.row_hash_expr(non_key)
        )
        business = AuditColumns.business_columns(feed_specs)
        return df.select(*business, TARGET_ROW_HASH_COLUMN)

    @staticmethod
    def prepare_scd_stream(
        staging_incr_df: DataFrame,
        feed_specs: dict,
        key_columns: list[str],
    ) -> DataFrame:
        """Incremental stream with SCD metadata columns (keeps ``_x_operation`` until merge)."""
        df = AuditColumns.drop_staging_only(staging_incr_df)
        df = AuditColumns.drop_columns_present(
            df, {TARGET_ROW_HASH_COLUMN, "_x_commit_version", "_x_commit_timestamp"}
        )
        non_key = AuditColumns.non_key_business_columns(feed_specs, key_columns)
        df = df.withColumn(TARGET_ROW_HASH_COLUMN, AuditColumns.row_hash_expr(non_key))
        df = df.withColumn("_x_date_from", F.current_timestamp())
        df = df.withColumn(
            "_x_date_to", F.to_timestamp(F.lit("9999-12-31 23:59:59"))
        )
        df = df.withColumn("_x_is_active", F.lit(1))
        df = df.withColumn("_x_last_modification_timestamp", F.current_timestamp())
        if CDC_STREAM_COLUMN in df.columns:
            df = df.withColumn("_x_last_operation", F.col(CDC_STREAM_COLUMN))
        return df

    @staticmethod
    def scd_business_subset_enforced(
        df: DataFrame, feed_specs: dict
    ) -> DataFrame:
        """Cast business columns to selection_schema; keep SCD / stream audit columns."""
        business = AuditColumns.business_columns(feed_specs)
        schema = StructType.fromJson(feed_specs["selection_schema"])
        keep = [
            c
            for c in df.columns
            if c in business
            or c in SCD_TARGET_COLUMNS
            or c == CDC_STREAM_COLUMN
        ]
        result = df.select(*keep)
        for field in schema.fields:
            if field.name in result.columns:
                result = result.withColumn(
                    field.name, F.col(field.name).cast(field.dataType)
                )
        return result

    @staticmethod
    def merge_update_columns(incr_columns: list[str]) -> list[str]:
        """Columns allowed in CDC MERGE UPDATE set (business + row hash)."""
        skip = {
            CDC_STREAM_COLUMN,
            *STAGING_ONLY_COLUMNS,
            "_x_commit_version",
            "_x_commit_timestamp",
        }
        return [c for c in incr_columns if c not in skip]

    @staticmethod
    def merge_insert_values(incr_columns: list[str]) -> list[str]:
        skip = {
            CDC_STREAM_COLUMN,
            *STAGING_ONLY_COLUMNS,
            "_x_commit_version",
            "_x_commit_timestamp",
        }
        return [c for c in incr_columns if c not in skip]
