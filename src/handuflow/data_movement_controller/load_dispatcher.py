# inbuilt
import json
import time
import logging
import configparser

# external
from pyspark.sql import SparkSession

# internal
from handuflow.constants import SUPPORTED_LOAD_TYPES
from handuflow.config.catalog_resolver import CatalogResolver
from handuflow.data_movement_controller.load_types.full_load import FullLoad
from handuflow.data_movement_controller.load_types.append_load import AppendLoad
from handuflow.data_movement_controller.load_types.incremental_cdc import IncrementalCDC
from handuflow.data_movement_controller.load_types.scd_type_2 import SCDType2
from handuflow.data_movement_controller.load_types.api_extractor import APIExtractor
from handuflow.data_movement_controller.load_types.storage_fetch import StorageFetch
from handuflow.data_movement_controller.data_class.load_config import LoadConfig
from handuflow.config.run_logger import log_step
from handuflow.data_movement_controller.data_class.load_result import LoadResult
from handuflow.exception.data_load_exception import DataLoadException
from handuflow.exception.error_handler import exception_message, resolve_error_code, wrap_exception


class LoadDispatcher:
    """Dispatches a single master-spec row to the appropriate load strategy."""

    _LOAD_TYPE_MAP = {
        "FULL_LOAD": FullLoad,
        "APPEND_LOAD": AppendLoad,
        "INCREMENTAL_CDC": IncrementalCDC,
        "SCD_TYPE_2": SCDType2,
        "API_EXTRACTOR": APIExtractor,
        "STORAGE_FETCH": StorageFetch,
    }

    def __init__(
        self,
        master_spec: dict,
        spark: SparkSession,
        config: configparser.ConfigParser,
    ) -> None:
        self.master_spec = master_spec
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _format_duration(seconds: float) -> str:
        seconds = round(seconds, 2)
        hours, remainder = divmod(seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        parts = []
        if hours > 0:
            parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
        if secs > 0 or not parts:
            parts.append(f"{secs} second{'s' if secs != 1 else ''}")
        return ", ".join(parts[:-1]) + (
            " and " + parts[-1] if len(parts) > 1 else parts[0]
        )

    def _parse_feed_specs(self, feed_id) -> dict:
        raw = self.master_spec.get("feed_specs", "{}")
        try:
            return json.loads(raw) if isinstance(raw, str) else raw
        except json.JSONDecodeError as exc:
            raise DataLoadException(
                message=f"feed_id={feed_id}: invalid feed_specs JSON",
                error_code="HF031",
                feed_id=feed_id,
                original_exception=exc,
            ) from exc

    def dispatch(self) -> LoadResult:
        feed_id = self.master_spec.get("feed_id")
        load_type = self.master_spec.get("load_type", "")
        start_time = time.time()
        exception_if_any: str | None = None
        error_code: str | None = None
        load_result: LoadResult | None = None
        target_path = ""

        try:
            if load_type not in SUPPORTED_LOAD_TYPES:
                raise DataLoadException(
                    message=(
                        f"feed_id={feed_id}: unsupported load_type={load_type!r}. "
                        f"Supported: {sorted(SUPPORTED_LOAD_TYPES)}"
                    ),
                    error_code="HF030",
                    feed_id=feed_id,
                )

            load_class = self._LOAD_TYPE_MAP.get(load_type)
            if load_class is None:
                raise DataLoadException(
                    message=f"feed_id={feed_id}: no handler for load_type={load_type!r}",
                    error_code="HF032",
                    feed_id=feed_id,
                )

            log_step(
                self.logger,
                "load.dispatch",
                status="START",
                feed_id=feed_id,
                load_type=load_type,
                feed_type=self.master_spec.get("feed_type"),
                direction=self.master_spec.get("data_flow_direction"),
                target_schema=self.master_spec.get("target_schema_name"),
                target_table=self.master_spec.get("target_table_name"),
            )

            load_config = LoadConfig(
                config=self.config,
                master_specs=self.master_spec,
                feed_specs=self._parse_feed_specs(feed_id),
                target_unity_catalog=self.master_spec.get("target_unity_catalog", ""),
                target_schema_name=self.master_spec.get("target_schema_name", ""),
                target_table_name=self.master_spec.get("target_table_name", ""),
            )
            catalog = CatalogResolver(
                load_config.target_unity_catalog, config=self.config
            )
            target_path = catalog.target_table(
                load_config.target_schema_name,
                load_config.target_table_name,
            )

            self.logger.info(
                "Invoking load strategy %s for feed_id=%s target=%s",
                load_class.__name__,
                feed_id,
                target_path,
            )
            loader = load_class(config=load_config, spark=self.spark)
            load_result = loader.load()
            if (
                load_result.success
                and not load_result.skipped
                and not self.spark.catalog.tableExists(target_path)
            ):
                load_result.success = False
                err = DataLoadException(
                    message=f"Load reported success but target table missing: {target_path}",
                    error_code="HF038",
                    feed_id=feed_id,
                )
                exception_if_any = exception_message(err)
                error_code = "HF038"
                self.logger.error(
                    "Post-load verification failed feed_id=%s: target missing %s",
                    feed_id,
                    target_path,
                )
            elif load_result.skipped:
                self.logger.warning(
                    "Load skipped feed_id=%s target=%s (no new data or staging empty)",
                    feed_id,
                    target_path,
                )
        except Exception as exc:
            wrapped = wrap_exception(exc, feed_id=feed_id)
            exception_if_any = exception_message(wrapped)
            error_code = resolve_error_code(wrapped)
            self.logger.error(
                "Feed load failed | feed_id=%s load_type=%s error_code=%s error=%s",
                feed_id,
                load_type,
                error_code,
                wrapped.short_message(),
                exc_info=True,
            )
            load_result = LoadResult(
                feed_id=feed_id,
                success=False,
                total_rows_inserted=0,
                total_rows_deleted=0,
                total_rows_updated=0,
            )

        if load_result is None:
            load_result = LoadResult(
                feed_id=feed_id,
                success=False,
                total_rows_inserted=0,
                total_rows_deleted=0,
                total_rows_updated=0,
            )

        end_time = time.time()
        load_result.start_epoch = start_time
        load_result.end_epoch = end_time
        load_result.total_human_readable_time = self._format_duration(
            end_time - start_time
        )
        load_result.exception_if_any = exception_if_any
        load_result.error_code = error_code
        load_result.source_table_path = self.master_spec.get("source_unity_catalog", "")
        load_result.target_table_path = target_path
        load_result.data_flow_direction = self.master_spec.get(
            "data_flow_direction", ""
        )
        log_step(
            self.logger,
            "load.dispatch",
            status="OK" if load_result.success else "FAIL",
            feed_id=feed_id,
            load_type=load_type,
            target=load_result.target_table_path,
            skipped=load_result.skipped,
            inserted=load_result.total_rows_inserted,
            updated=load_result.total_rows_updated,
            deleted=load_result.total_rows_deleted,
            duration=load_result.total_human_readable_time,
            error_code=error_code,
        )
        return load_result
