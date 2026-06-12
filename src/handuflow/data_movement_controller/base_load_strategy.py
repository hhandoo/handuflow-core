# inbuilt
import logging
import re
import shutil
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import urlparse

# external
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, TimestampType, StructType
from delta.tables import DeltaTable
from pyspark.sql import DataFrame

# internal
from handuflow.config.catalog_resolver import CatalogResolver
from handuflow.data_movement_controller.data_class.load_config import LoadConfig
from handuflow.data_movement_controller.data_class.load_result import LoadResult
from handuflow.data_movement_controller.load_integrity import LoadIntegrityVerifier
from handuflow.exception.data_load_exception import DataLoadException


class BaseLoadStrategy(ABC):
    """
    Template method for data loads without retries.
    Subclasses must implement extract and load.
    """

    def __init__(self, config: LoadConfig, spark: SparkSession):
        self.config = config
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        self._catalog = CatalogResolver(
            self.config.target_unity_catalog, config=self.config.config
        )
        self._current_target_table_name = self._catalog.target_table(
            self.config.target_schema_name,
            self.config.target_table_name,
        )
        self._staging_schema = self._catalog.staging_schema()
        self._full_load_staging_changed = True

        self.logger.info(
            "Target table=%s staging=%s runtime=%s",
            self._current_target_table_name,
            self._staging_schema,
            "local" if self._catalog.is_local else "unity_catalog",
        )

    def _normalize_column_names(self, df: DataFrame) -> DataFrame:
        for col in df.columns:
            clean = col.strip().lower()
            clean = re.sub(r"[^a-z0-9_]", "_", clean)
            clean = re.sub(r"_+", "_", clean)
            clean = clean.strip("_")
            if clean != col:
                df = df.withColumnRenamed(col, clean)
        return df

    def _enforce_schema(self, df: DataFrame, schema: StructType) -> DataFrame:
        self.logger.info("Enforcing target schema on extracted DataFrame...")
        self.logger.info("Target Schema: %s", schema.simpleString())
        return LoadIntegrityVerifier.enforce_schema(df, schema)

    def _partition_keys(self) -> list[str]:
        return list(self.config.feed_specs.get("partition_keys") or [])

    def _prepare_partition_columns(self, df: DataFrame) -> DataFrame:
        """Coalesce null/blank partition values before partitioned Delta writes."""
        for pk in self._partition_keys():
            if pk in df.columns:
                df = df.withColumn(
                    pk,
                    F.regexp_replace(
                        F.coalesce(
                            F.nullif(F.trim(F.col(pk).cast("string")), F.lit("")),
                            F.lit("UNKNOWN"),
                        ),
                        r"[^\x00-\x7F]",
                        "_",
                    ),
                )
        return df

    def _validate_partition_columns_not_null(self, df: DataFrame) -> None:
        keys = self._partition_keys()
        if not keys:
            return
        missing = [k for k in keys if k not in df.columns]
        if missing:
            raise DataLoadException(
                message=(
                    f"Partition columns missing from load DataFrame: {missing}"
                ),
                error_code="HF033",
            )
        bad_filter = " OR ".join(
            f"({c} IS NULL OR trim(cast({c} as string)) = '')" for c in keys
        )
        if df.filter(bad_filter).limit(1).count() > 0:
            raise DataLoadException(
                message=(
                    f"Null values in partition columns {keys}; "
                    "cannot write partitioned Delta table."
                ),
                error_code="HF033",
            )

    def _delta_partition_columns_mismatch(self, table_name: str) -> bool:
        """True when an existing Delta table's partition columns differ from feed keys."""
        keys = self._partition_keys()
        if not self.spark.catalog.tableExists(table_name):
            return False
        details = self.spark.sql(f"DESCRIBE DETAIL {table_name}").first()
        if details is None:
            return False
        existing = list(details["partitionColumns"])
        return set(existing) != set(keys)

    def _target_partition_mismatch(self, table_name: str) -> bool:
        """True when target Delta partitions differ from feed (including removal)."""
        return self._delta_partition_columns_mismatch(table_name)

    def _resolve_table_storage_path(self, table_name: str) -> Path:
        """Hive-style warehouse path for a catalog table name (db.table)."""
        db_name, short_name = table_name.split(".", 1)
        warehouse = self.spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
        warehouse_path = Path(urlparse(str(warehouse)).path or warehouse).resolve()
        return warehouse_path / f"{db_name}.db" / short_name

    def _purge_delta_table(self, table_name: str) -> None:
        """Drop catalog entry and remove on-disk Delta files (avoids corrupt layouts)."""
        location: str | None = None
        if self.spark.catalog.tableExists(table_name):
            try:
                rows = self.spark.sql(f"DESCRIBE DETAIL {table_name}").collect()
                if rows:
                    location = rows[0]["location"]
            except Exception:
                location = None
        self.logger.info("Dropping table %s", table_name)
        self.spark.catalog.clearCache()
        self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        if location:
            path = Path(urlparse(str(location)).path)
            if path.exists():
                shutil.rmtree(path, ignore_errors=True)
        elif "." in table_name:
            db_name, short_name = table_name.split(".", 1)
            warehouse = Path(
                self.spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
            ).resolve()
            table_dir = warehouse / f"{db_name}.db" / short_name
            if table_dir.exists():
                shutil.rmtree(table_dir, ignore_errors=True)

    def _drop_table_if_exists(self, table_name: str) -> None:
        self._purge_delta_table(table_name)

    def _rebuild_target_partition_layout(
        self,
        table_name: str,
        rebuild_df: DataFrame,
        *,
        load_type: str,
    ) -> bool:
        """
        Overwrite an existing target when feed ``partition_keys`` differ from the
        table layout (including removing partitions). Returns True if rebuilt.
        """
        if not self.spark.catalog.tableExists(table_name):
            return False
        if not self._target_partition_mismatch(table_name):
            return False
        self.logger.info(
            "%s: partition scheme changed for %s (partition_keys=%s); "
            "rebuilding target",
            load_type,
            table_name,
            self._partition_keys(),
        )
        # Snapshot before drop/write: rebuild_df is often a lazy scan of table_name.
        rebuild_df = self._prepare_partition_columns(rebuild_df)
        tmp_table = f"_handuflow_rebuild_{uuid.uuid4().hex[:12]}"
        rebuild_df.write.format("delta").mode("overwrite").saveAsTable(tmp_table)
        try:
            materialized = self._prepare_partition_columns(self.spark.table(tmp_table))
            row_count = materialized.count()
            self._purge_delta_table(table_name)
            if "." in table_name:
                db_name, short_name = table_name.split(".", 1)
                warehouse = Path(
                    self.spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
                ).resolve()
                table_dir = warehouse / f"{db_name}.db" / short_name
                if table_dir.exists():
                    shutil.rmtree(table_dir, ignore_errors=True)
            self._write_delta_table(
                materialized,
                table_name,
                mode="overwrite",
                overwrite_schema=True,
            )
            self.logger.info(
                "%s: rebuilt %s with %s rows (partition_keys=%s)",
                load_type,
                table_name,
                row_count,
                self._partition_keys(),
            )
        finally:
            self.spark.sql(f"DROP TABLE IF EXISTS {tmp_table}")
        return True

    def _staging_partition_rebuild_needed(
        self, full_table: str, incr_table: str, all_changes_table: str
    ) -> bool:
        for name in (full_table, incr_table, all_changes_table):
            if self.spark.catalog.tableExists(name) and self._delta_partition_columns_mismatch(
                name
            ):
                return True
        return False

    def _drop_staging_tables(self, *table_names: str) -> None:
        for name in table_names:
            self._purge_delta_table(name)

    def _overwrite_delta_table(
        self,
        df: DataFrame,
        table_name: str,
        *,
        overwrite_schema: bool = False,
        extra_options: dict[str, str] | None = None,
    ) -> None:
        """
        Drop + path overwrite + CREATE TABLE.

        Avoids Spark V2 saveAsTable(overwrite) truncate-in-batch-mode failures
        on local Hive/Delta catalogs.
        """
        keys = self._partition_keys()
        self._drop_table_if_exists(table_name)
        table_path = self._resolve_table_storage_path(table_name)
        table_path.parent.mkdir(parents=True, exist_ok=True)
        writer = df.write.format("delta").mode("overwrite")
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        for key, value in (extra_options or {}).items():
            writer = writer.option(key, value)
        if keys:
            writer = writer.partitionBy(*keys)
        writer.save(str(table_path))
        location = table_path.resolve().as_uri()
        self.spark.sql(
            f"CREATE TABLE {table_name} USING DELTA LOCATION '{location}'"
        )

    def _write_staging_delta(
        self, df: DataFrame, table_name: str, mode: str
    ) -> None:
        """Write a staging Delta table honoring feed partition_keys."""
        keys = self._partition_keys()
        mismatch = self._delta_partition_columns_mismatch(table_name)
        if keys:
            df = self._prepare_partition_columns(df)
            self._validate_partition_columns_not_null(df)
        if mode == "overwrite":
            self.logger.info(
                "Recreating staging table %s (drop + path write; feed keys=%s)",
                table_name,
                keys,
            )
            self._overwrite_delta_table(
                df,
                table_name,
                overwrite_schema=mismatch,
            )
            return
        writer = df.write.format("delta").mode(mode)
        if mismatch:
            writer = writer.option("overwriteSchema", "true")
        if keys:
            writer = writer.partitionBy(*keys)
        writer.saveAsTable(table_name)

    def _write_delta_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str,
        *,
        overwrite_schema: bool = False,
    ) -> None:
        """Write to a target Delta table with feed ``partition_keys`` applied."""
        keys = self._partition_keys()
        partition_mismatch = self._delta_partition_columns_mismatch(table_name)
        if partition_mismatch:
            if mode != "overwrite":
                raise DataLoadException(
                    message=(
                        f"Partition scheme for {table_name} does not match "
                        f"feed partition_keys {keys}. Rebuild the target with "
                        "the load strategy's partition rebuild path or drop the "
                        "table manually."
                    ),
                    error_code="HF033",
                )
            self.logger.info(
                "Target partition scheme changed for %s (feed keys=%s); "
                "dropping table before overwrite",
                table_name,
                keys,
            )
            overwrite_schema = True
        if keys:
            df = self._prepare_partition_columns(df)
            self._validate_partition_columns_not_null(df)
        if mode == "overwrite":
            if keys:
                self.logger.info(
                    "Writing %s with partitionBy(%s)", table_name, keys
                )
            else:
                self.logger.info("Writing %s without partitioning", table_name)
            self._overwrite_delta_table(
                df,
                table_name,
                overwrite_schema=overwrite_schema or partition_mismatch,
                extra_options={
                    "delta.autoOptimize.autoCompact": "false",
                    "delta.autoOptimize.optimizeWrite": "false",
                }
                if overwrite_schema
                else None,
            )
            return
        writer = df.write.format("delta").mode(mode)
        if overwrite_schema:
            writer = (
                writer.option("overwriteSchema", "true")
                .option("delta.autoOptimize.autoCompact", "false")
                .option("delta.autoOptimize.optimizeWrite", "false")
            )
        if keys:
            self.logger.info(
                "Writing %s with partitionBy(%s)", table_name, keys
            )
            writer = writer.partitionBy(*keys)
        else:
            self.logger.info("Writing %s without partitioning", table_name)
        writer.saveAsTable(table_name)

    def _post_load_verify(
        self,
        *,
        expected_row_count: int | None = None,
        minimum_row_count: int | None = None,
        verify_keys: bool = True,
    ) -> int:
        """Raise DataLoadException if the target table fails integrity checks."""
        keys = []
        if verify_keys:
            try:
                keys = LoadIntegrityVerifier.require_primary_keys(
                    self.config.feed_specs, self._current_target_table_name
                )
            except DataLoadException:
                keys = []
        count = LoadIntegrityVerifier.verify_row_count(
            self.spark,
            self._current_target_table_name,
            expected=expected_row_count,
            minimum=minimum_row_count,
        )
        if keys:
            self.logger.info(
                "Post-load primary key verify | table=%s keys=%s",
                self._current_target_table_name,
                keys,
            )
            LoadIntegrityVerifier.verify_primary_keys_not_null(
                self.spark, self._current_target_table_name, keys
            )
        self.logger.info(
            "Post-load verify OK | table=%s row_count=%s",
            self._current_target_table_name,
            count,
        )
        return count

    def execute(self) -> LoadResult:
        """
        Orchestrates the load lifecycle:
        """
        try:
            result = self.load()
            return result
        except Exception as e:
            raise DataLoadException(
                message="Somethine went wrong while executing data load",
                original_exception=e,
            )

    @abstractmethod
    def load(self) -> LoadResult:
        """W
        Core load logic implemented by subclass.W
        Should return LoadResult on success.
        """

    def __get_max_table_version(self, table_path_or_name: str) -> int:
        """
        Returns the latest version number of a Delta table.

        Args:
            table_path_or_name (str): Path (e.g. '/mnt/data/mytable')
                                    or table name (e.g. 'db.mytable')
        Returns:
            int: Latest delta table version number
        """
        delta_tbl = (
            DeltaTable.forName(self.spark, table_path_or_name)
            if not table_path_or_name.startswith("/")
            else DeltaTable.forPath(self.spark, table_path_or_name)
        )

        history_df = delta_tbl.history(1)  # get only the latest record
        return history_df.collect()[0]["version"]

    def _create_full_load_staging_layer(self) -> bool:
        """
        FULL_LOAD staging keeps only ``t_full`` as a full source snapshot.

        When the source Delta version changes (or staging partition layout
        changes), ``t_full`` is overwritten from the current source. Incremental
        staging tables are not created.
        """
        spark = self.spark
        staging_schema = self._staging_schema
        full_table = f"{staging_schema}.t_full_{self.config.target_table_name}"
        incr_table = f"{staging_schema}.t_incr_{self.config.target_table_name}"
        all_changes_table = (
            f"{staging_schema}.t_incr_cdf_changes_{self.config.target_table_name}"
        )

        self.logger.info(
            (
                "\n=== FULL_LOAD Staging ===\n"
                f"FULL Table: [{full_table}]\n"
                "INCR/CDF tables: not used for FULL_LOAD\n"
                f"partition_keys={self._partition_keys()}\n"
                "========================="
            )
        )

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {staging_schema}")
        self._drop_staging_tables(incr_table, all_changes_table)

        try:
            df = (
                spark.sql(self.config.feed_specs["selection_query"])
                if self.config.feed_specs["selection_query"]
                else spark.read.table(self.config.feed_specs["source_table_name"])
            )
            latest_source_version = -9999
            if self.config.feed_specs["source_table_name"]:
                src_history = spark.sql(
                    f"DESCRIBE HISTORY {self.config.feed_specs['source_table_name']}"
                )
                agg_result = src_history.agg({"version": "max"}).first()
                if agg_result and agg_result[0] is not None:
                    latest_source_version = int(agg_result[0])
                else:
                    latest_source_version = None
            if df is None or len(df.columns) == 0:
                self.logger.info("Source empty. Skipping FULL_LOAD staging.")
                return False

            source_row_count = LoadIntegrityVerifier.verify_source_not_empty_for_sync(
                df,
                operation="FULL_LOAD staging snapshot",
                allow_empty=self.config.feed_specs.get("allow_empty_source", False),
            )
            self.logger.info("FULL_LOAD staging source row count: %s", source_row_count)

            full_exists = spark.catalog.tableExists(full_table)
            partition_mismatch = (
                full_exists and self._delta_partition_columns_mismatch(full_table)
            )
            if partition_mismatch:
                self.logger.info(
                    "FULL_LOAD staging partition scheme changed; rebuilding %s",
                    full_table,
                )
                self._drop_staging_tables(full_table)
                full_exists = False

            if full_exists:
                props = spark.sql(f"SHOW TBLPROPERTIES {full_table}")
                row = (
                    props.filter("key = '_x_latest_source_version'")
                    .select("value")
                    .first()
                )
                version_prop = int(row[0]) if row is not None else None
                if (
                    version_prop is not None
                    and version_prop == latest_source_version
                    and not partition_mismatch
                ):
                    self.logger.info(
                        "Source unchanged; reusing FULL_LOAD staging snapshot."
                    )
                    self._full_load_staging_changed = False
                    self._current_staging_table_df = spark.read.table(full_table)
                    return True

            load_id = str(uuid.uuid4())
            df_cols = df.columns
            df = df.withColumn(
                "_x_row_hash",
                F.sha2(
                    F.concat_ws("||", *[F.col(c).cast("string") for c in df_cols]), 256
                ),
            ).withColumn("_x_load_id", F.lit(load_id))

            if self._partition_keys():
                df = self._prepare_partition_columns(df)
                self._validate_partition_columns_not_null(df)

            self._full_load_staging_changed = True
            self._write_staging_delta(df, full_table, mode="overwrite")
            if (
                self.config.feed_specs["selection_query"] == ""
                or self.config.feed_specs["selection_query"] is None
            ):
                spark.sql(
                    f"""ALTER TABLE {full_table} SET TBLPROPERTIES (
                    '_x_latest_source_version' = '{latest_source_version}',
                    'delta.autoOptimize.autoCompact' = 'false',
                    'delta.autoOptimize.optimizeWrite' = 'false'
                )"""
                )
            self._current_staging_table_df = spark.read.table(full_table)
            self.logger.info(
                "FULL_LOAD staging snapshot written (%s rows).", source_row_count
            )
            return True
        except Exception as e:
            raise DataLoadException(
                message=(
                    "Error in FULL_LOAD staging for "
                    f"{self.config.feed_specs['source_table_name']}"
                ),
                original_exception=e,
            )

    def _create_staging_layer(self) -> bool:
        """
        Staging Layer (MERGE + CDC) with partitioning and schema alignment:
        - FULL table is updated using MERGE on _x_row_hash.
        - INCR table contains only true inserts/updates/deletes from CDF.
        - Partitioning handled via self.config.feed_specs['partition_keys'].
        """
        if self.config.master_specs.get("load_type") == "FULL_LOAD":
            return self._create_full_load_staging_layer()

        spark = self.spark
        staging_schema = self._staging_schema
        full_table = f"{staging_schema}.t_full_{self.config.target_table_name}"
        incr_table = f"{staging_schema}.t_incr_{self.config.target_table_name}"
        all_changes_table = (
            f"{staging_schema}.t_incr_cdf_changes_{self.config.target_table_name}"
        )

        self.logger.info(
            (
                "\n=== Staging Layer Creation ===\n"
                "Function: [_create_staging_layer]\n"
                "Operation: MERGE + CDC with partitioning and schema alignment\n"
                f"FULL Table: [{full_table}]\n"
                f"INCR Table: [{incr_table}]\n"
                f"CDF Table: [{all_changes_table}]\n"
                f"Current Partitioning Scheme: {self.config.feed_specs['partition_keys']}\n"
                "================================"
            )
        )

        self.logger.info(f"Creating Schema [{staging_schema}] if it doesn't exist.")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {staging_schema}")
        try:
            df = (
                spark.sql(self.config.feed_specs["selection_query"])
                if self.config.feed_specs["selection_query"]
                else spark.read.table(self.config.feed_specs["source_table_name"])
            )
            latest_source_version = -9999
            if self.config.feed_specs["source_table_name"]:
                src_history = spark.sql(
                    f"DESCRIBE HISTORY {self.config.feed_specs['source_table_name']}"
                )
                agg_result = src_history.agg({"version": "max"}).first()

                if agg_result and agg_result[0] is not None:
                    latest_source_version = int(agg_result[0])
                else:
                    latest_source_version = None
            else:
                self.logger.info("Source Data is in the form of a query:")
                self.logger.info(self.config.feed_specs["selection_query"])
            if df is None or len(df.columns) == 0:
                self.logger.info("Source empty. Skipping staging.")
                return False
            source_row_count = LoadIntegrityVerifier.verify_source_not_empty_for_sync(
                df,
                operation="Staging MERGE",
                allow_empty=self.config.feed_specs.get("allow_empty_source", False),
            )
            self.logger.info("Staging source row count: %s", source_row_count)
            full_exists = spark.catalog.tableExists(full_table)
            partition_mismatch = self._staging_partition_rebuild_needed(
                full_table, incr_table, all_changes_table
            )
            if partition_mismatch:
                self.logger.info(
                    "Staging partition scheme changed; rebuilding staging tables "
                    "(partition_keys=%s)",
                    self._partition_keys(),
                )
            version_prop = None
            if full_exists:
                props = spark.sql(f"SHOW TBLPROPERTIES {full_table}")
                row = (
                    props.filter("key = '_x_latest_source_version'")
                    .select("value")
                    .first()
                )
                if row is not None:
                    version_prop = int(row[0])
                self.logger.info(
                    "Latest source version: %s, Latest table version: %s",
                    latest_source_version,
                    version_prop,
                )
                if version_prop == latest_source_version:
                    if partition_mismatch:
                        self._drop_staging_tables(
                            full_table, incr_table, all_changes_table
                        )
                        full_exists = False
                    else:
                        self.logger.warning("No new data to load.")
                        return False

            df_cols = df.columns
            load_id = str(uuid.uuid4())
            self.logger.info(f"Current _x_load_id: {load_id}")
            df = df.withColumn(
                "_x_row_hash",
                F.sha2(
                    F.concat_ws("||", *[F.col(c).cast("string") for c in df_cols]), 256
                ),
            ).withColumn("_x_load_id", F.lit(load_id))

            if self._partition_keys():
                df = self._prepare_partition_columns(df)
                try:
                    self._validate_partition_columns_not_null(df)
                except DataLoadException:
                    self.logger.error(
                        "Null or missing values in partition columns %s",
                        self._partition_keys(),
                    )
                    return False
            if full_exists == False or partition_mismatch == True:
                self._write_staging_delta(df, full_table, mode="overwrite")
                if (
                    self.config.feed_specs["selection_query"] == ""
                    or self.config.feed_specs["selection_query"] == None
                ):
                    self.logger.info(
                        f"Updating _x_latest_source_version in full table [{full_table}] with {latest_source_version}"
                    )
                    spark.sql(
                        f"""ALTER TABLE {full_table} SET TBLPROPERTIES (
                        '_x_latest_source_version' = '{latest_source_version}',
                        'delta.autoOptimize.autoCompact' = 'false',
                        'delta.autoOptimize.optimizeWrite' = 'false'
                    )"""
                    )
                spark.sql(
                    f"ALTER TABLE {full_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
                )
                incr_df = (
                    df.withColumn("_x_operation", F.lit("insert"))
                    .withColumn(
                        "_x_commit_version",
                        F.lit(self.__get_max_table_version(full_table)).cast(
                            LongType()
                        ),
                    )
                    .withColumn(
                        "_x_commit_timestamp",
                        F.lit(F.current_timestamp()).cast(TimestampType()),
                    )
                )
                self._write_staging_delta(incr_df, incr_table, mode="overwrite")
                self._write_staging_delta(
                    incr_df, all_changes_table, mode="overwrite"
                )
                self._current_staging_table_df = df
                self._current_staging_incremental_table_df = incr_df
                self.logger.info(
                    f"First load completed (FULL + INCR) | PARTION REBUILD: [{partition_mismatch}]"
                )
                return True
            current_version = self.__get_max_table_version(full_table)
            safe_feed_id = LoadIntegrityVerifier.sanitize_sql_identifier(
                self.config.master_specs["feed_id"], prefix="feed"
            )
            inc_data = f"incoming_data_{safe_feed_id}"
            df.createOrReplaceTempView(inc_data)
            primary_key = self.config.feed_specs.get("primary_key")
            composite_keys = self.config.feed_specs.get("composite_key", [])
            all_keys = [primary_key] if primary_key else []
            all_keys.extend([k for k in composite_keys if k not in all_keys])
            if not all_keys:
                raise DataLoadException(
                    message=(
                        f"Staging MERGE on {full_table} requires primary_key or "
                        "composite_key in feed_specs."
                    ),
                    original_exception=None,
                )
            allow_deletes = self.config.feed_specs.get("allow_unmatched_deletes", False)
            merge_condition = " AND ".join([f"tgt.{c} = src.{c}" for c in all_keys])
            data_cols = [c for c in df.columns if c not in composite_keys]
            data_cols_ins = [c for c in df.columns]
            set_clause = ", ".join([f"tgt.{c} = src.{c}" for c in data_cols])
            insert_cols = ", ".join(data_cols_ins)
            insert_vals = ", ".join([f"src.{c}" for c in data_cols_ins])
            self.logger.info(f"Target Staging Table: [{full_table}]")
            self.logger.info(f"Merge Condition: [{merge_condition}]")
            self.logger.info(f"Set Clause: [{set_clause}]")
            self.logger.info(f"Insert clause: [{data_cols_ins}]")
            delete_clause = (
                "WHEN NOT MATCHED BY SOURCE THEN\n                DELETE"
                if allow_deletes
                else ""
            )
            if allow_deletes:
                self.logger.warning(
                    "allow_unmatched_deletes=true: staging MERGE may DELETE target rows "
                    "not present in source."
                )
            merge_query = f"""

            MERGE INTO 
                {full_table} AS tgt
            USING 
                {inc_data} AS src
            ON 
                {merge_condition}
            WHEN MATCHED AND tgt._x_row_hash != src._x_row_hash THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
            {delete_clause}

            """
            self.logger.info(merge_query)
            spark.sql(merge_query)
            new_version = self.__get_max_table_version(full_table)
            self.logger.info(
                f"Current Version: [{current_version}], New Version after Merge: [{new_version}]"
            )
            cdf_df = (
                spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", current_version + 1)
                .option("endingVersion", new_version)
                .table(full_table)
            )
            update_pre = cdf_df.filter("_change_type = 'update_preimage'")
            update_post = cdf_df.filter("_change_type = 'update_postimage'")
            true_updates = (
                (
                    update_post.alias("post")
                    .join(update_pre.alias("pre"), on=all_keys, how="left")
                    .filter("post._x_row_hash != pre._x_row_hash")
                )
                .select("post.*")
                .withColumn("_x_operation", F.lit("update"))
            )

            true_inserts = cdf_df.filter("_change_type = 'insert'").withColumn(
                "_x_operation", F.lit("insert")
            )
            true_deletes = cdf_df.filter("_change_type = 'delete'").withColumn(
                "_x_operation", F.lit("delete")
            )
            true_updates = true_updates.drop("_change_type").filter(
                f"_x_load_id = '{load_id}'"
            )
            true_inserts = true_inserts.drop("_change_type").filter(
                f"_x_load_id = '{load_id}'"
            )
            true_deletes = true_deletes.drop("_change_type")

            incr_df = (
                (true_inserts.unionByName(true_updates).unionByName(true_deletes))
                .withColumnRenamed("_commit_version", "_x_commit_version")
                .withColumnRenamed("_commit_timestamp", "_x_commit_timestamp")
            )
            self._write_staging_delta(incr_df, incr_table, mode="overwrite")
            # Keep cdf-changes staging aligned with incr (_x_operation), not raw CDF.
            self._write_staging_delta(incr_df, all_changes_table, mode="overwrite")
            if self.config.feed_specs["selection_query"]:
                spark.sql(
                    f"""
                    ALTER TABLE {full_table}
                    SET TBLPROPERTIES ('_x_latest_source_version' = '{latest_source_version}')
                """
                )
            self.logger.info(
                f"INCR updated using MERGE + TRUE CDC logic (Δ {current_version} → {new_version}). "
                f"Affected Records: {incr_df.count()}"
            )
            if (
                self.config.feed_specs["selection_query"] == ""
                or self.config.feed_specs["selection_query"] is None
            ):
                self.logger.info(
                    f"Updating _x_latest_source_version in full table [{full_table}] with {latest_source_version}"
                )
                spark.sql(
                    f"""ALTER TABLE {full_table} SET TBLPROPERTIES ('_x_latest_source_version' = '{latest_source_version}')"""
                )
            self._current_staging_table_df = spark.read.table(full_table)
            self._current_staging_incremental_table_df = spark.read.table(incr_table)
            self.logger.info(
                f"INCR updated using MERGE + CDF (Δ {current_version} → {new_version}). Affected Records {incr_df.count()}"
            )
            return True
        except Exception as e:
            raise DataLoadException(
                message=f"Error in staging layer for {self.config.feed_specs['source_table_name']}",
                original_exception=e,
            )

    def _enforce_load_type_consistency(self) -> None:
        """
        Enforces that a target Delta table can only ever be loaded with a single load_type.
        Once a load_type has been applied, switching to another type is disallowed.
        This is enforced using Delta table properties.
        """
        target_table = self._current_target_table_name
        current_type = self.config.master_specs["load_type"]
        try:
            if self.spark.catalog.tableExists(target_table):
                props_df = self.spark.sql(f"SHOW TBLPROPERTIES {target_table}")
                existing_type_row = (
                    props_df.filter(F.col("key") == "data.load_type")
                    .select("value")
                    .collect()
                )

                if existing_type_row:
                    existing_type = existing_type_row[0]["value"]
                    if existing_type.upper() != current_type:
                        raise DataLoadException(
                            message=(
                                f"Load type conflict for {target_table}. "
                                f"Existing load_type: '{existing_type}', "
                                f"Attempted: '{current_type}'. "
                                f"Switching load types is not permitted."
                            ),
                            error_code="HF045",
                        )
                    else:
                        self.logger.info(
                            f"Verified consistent load_type '{existing_type}' for {target_table}."
                        )
                else:
                    self.spark.sql(
                        f"ALTER TABLE {target_table} SET TBLPROPERTIES ('data.load_type' = '{current_type}')"
                    )
                    self.logger.info(
                        f"Registered load_type '{current_type}' for existing table {target_table}."
                    )
            else:
                self.logger.info(
                    f"Target table {target_table} not found yet — will set load_type on creation."
                )

        except Exception as e:
            raise DataLoadException(
                message="Something went wrong while enforcing load type consistency",
                original_exception=e,
            )
