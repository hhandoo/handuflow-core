# inbuilt
import logging

# external
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

# internal
from handuflow.exception.data_load_exception import DataLoadException

logger = logging.getLogger(__name__)

from handuflow.data_movement_controller.audit_columns import (
    STAGING_ONLY_COLUMNS,
    TARGET_ROW_HASH_COLUMN,
)

# Backward-compatible aliases
TARGET_CDC_METADATA = TARGET_ROW_HASH_COLUMN


class LoadIntegrityVerifier:
    """Post-load checks to detect missed or corrupt data before a feed is marked successful."""

    @staticmethod
    def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
        """
        Cast and project to target schema; add missing columns as null-typed columns.
        """
        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(
                    field.name, F.col(field.name).cast(field.dataType)
                )
            else:
                df = df.withColumn(
                    field.name, F.lit(None).cast(field.dataType)
                )
        return df.select([f.name for f in schema.fields])

    @staticmethod
    def require_primary_keys(feed_specs: dict, target_table: str) -> list[str]:
        primary_key = feed_specs.get("primary_key")
        composite_keys = feed_specs.get("composite_key") or []
        all_keys = [primary_key] if primary_key else []
        all_keys.extend(k for k in composite_keys if k not in all_keys)
        if not all_keys:
            raise DataLoadException(
                message=(
                    f"Primary or composite keys are required for keyed loads on "
                    f"{target_table} to prevent duplicate or lost rows."
                ),
                error_code="HF037",
            )
        return all_keys

    @staticmethod
    def verify_row_count(
        spark: SparkSession,
        table_name: str,
        *,
        expected: int | None = None,
        minimum: int | None = None,
        maximum: int | None = None,
    ) -> int:
        if not spark.catalog.tableExists(table_name):
            raise DataLoadException(
                message=f"Post-load verification failed: table does not exist: {table_name}",
                error_code="HF038",
            )
        actual = spark.table(table_name).count()
        logger.info(
            "Post-load row count verify | table=%s actual=%s expected=%s minimum=%s",
            table_name,
            actual,
            expected,
            minimum,
        )
        if expected is not None and actual != expected:
            raise DataLoadException(
                message=(
                    f"Row count mismatch on {table_name}: expected={expected}, actual={actual}"
                ),
                error_code="HF035",
            )
        if minimum is not None and actual < minimum:
            raise DataLoadException(
                message=(
                    f"Row count below minimum on {table_name}: minimum={minimum}, actual={actual}"
                ),
                error_code="HF035",
            )
        if maximum is not None and actual > maximum:
            raise DataLoadException(
                message=(
                    f"Row count above maximum on {table_name}: maximum={maximum}, actual={actual}"
                ),
                error_code="HF035",
            )
        logger.info("Row count verified for %s: %s rows", table_name, actual)
        return actual

    @staticmethod
    def verify_primary_keys_not_null(
        spark: SparkSession,
        table_name: str,
        key_columns: list[str],
    ) -> None:
        if not key_columns:
            return
        table = spark.table(table_name)
        null_filter = " OR ".join(f"{c} IS NULL" for c in key_columns)
        nulls = table.filter(null_filter).limit(1).count()
        if nulls > 0:
            raise DataLoadException(
                message=(
                    f"Primary key integrity failed on {table_name}: "
                    f"null values found in keys {key_columns}"
                ),
                error_code="HF037",
            )

    @staticmethod
    def verify_source_not_empty_for_sync(
        df: DataFrame,
        *,
        operation: str,
        allow_empty: bool = False,
    ) -> int:
        """
        Block full-sync MERGE paths when the source snapshot is empty (prevents mass DELETE).
        """
        count = df.count()
        if count == 0 and not allow_empty:
            raise DataLoadException(
                message=(
                    f"{operation} aborted: source snapshot is empty. "
                    "Refusing to sync to avoid deleting all target rows."
                ),
                error_code="HF036",
            )
        return count

    @staticmethod
    def business_columns_from_feed_specs(feed_specs: dict) -> list[str]:
        from handuflow.data_movement_controller.audit_columns import AuditColumns

        return AuditColumns.business_columns(feed_specs)

    @staticmethod
    def non_key_business_columns(
        feed_specs: dict, key_columns: list[str]
    ) -> list[str]:
        from handuflow.data_movement_controller.audit_columns import AuditColumns

        return AuditColumns.non_key_business_columns(feed_specs, key_columns)

    @staticmethod
    def ensure_row_hash_on_target(
        spark: SparkSession,
        table_name: str,
        key_columns: list[str],
        hash_columns: list[str],
    ) -> None:
        from handuflow.data_movement_controller.audit_columns import AuditColumns

        AuditColumns.ensure_row_hash_on_target(
            spark, table_name, key_columns, hash_columns
        )

    @staticmethod
    def sanitize_sql_identifier(value: str, prefix: str = "id") -> str:
        cleaned = "".join(c if c.isalnum() or c == "_" else "_" for c in str(value))
        if not cleaned or cleaned[0].isdigit():
            cleaned = f"{prefix}_{cleaned}"
        return cleaned
