# inbuilt
from __future__ import annotations

import uuid
import logging
import configparser
from typing import TYPE_CHECKING

# external
import pandas as pd
from pyspark.sql import SparkSession

# internal
from handuflow.constants import (
    is_ingestion_direction,
    is_within_unity_catalog_direction,
)
from handuflow.config.logging_config import LoggingConfig
from handuflow.config.config_paths import cfg_get
from handuflow.config.spark_session import is_databricks_runtime
from handuflow.config.validate import validate_handuflow_config
from handuflow.config.run_logger import log_step
from handuflow.orchestrator.run_guard import run_phase
from handuflow.orchestrator.result import RunResult, RunStatus
from handuflow.result_generator.result_generator import ResultGenerator
from handuflow.validation.system_launch_validator import SystemLaunchValidator
from handuflow.data_quality.runner.feed_data_quality_runner import FeedDataQualityRunner
from handuflow.data_movement_controller.data_load_controller import DataLoadController
from handuflow.data_movement_controller.data_class.load_result import LoadResult
from handuflow.system_cleanup.cleanup import SystemCleanup
from handuflow.data_flow_diagram_generator.data_flow_diagram_generator import (
    DataFlowDiagramGenerator,
)
from handuflow.exception.system_error import SystemError
from handuflow.exception.error_handler import exception_to_record, wrap_exception

if TYPE_CHECKING:
    pass


class Orchestrator:
    """
    Main entry point for a HanduFlow batch run.

    Validates configuration and master specs, runs bronze ingest, data quality,
    medallion loads, reporting, and lineage. Individual feed failures are isolated;
    logs and cleanup always run in ``finally``.

    Parameters
    ----------
    spark:
        Active Spark session created and configured by the caller (Delta, Hive/UC, etc.).
    config:
        Parsed ``config.ini`` (see README configuration reference).
    validate_config:
        When True, validate ``config.ini`` and required paths before starting.
    """

    PRODUCT_TAGLINE = "HanduFlow - Reliable data movement and evolution at scale"

    def __init__(
        self,
        spark: SparkSession,
        config: configparser.ConfigParser,
        *,
        validate_config: bool = True,
    ) -> None:
        if validate_config:
            validate_handuflow_config(
                config,
                check_paths_exist=not is_databricks_runtime(),
            )
        self.config = config
        self.run_id = uuid.uuid4().hex
        self.logging_config = LoggingConfig(run_id=self.run_id, config=config)
        self.logging_config.configure()
        self.logger = logging.getLogger(__name__)
        self.logger.info(self.PRODUCT_TAGLINE)
        if spark is None:
            raise SystemError(
                message="spark is required; create a SparkSession before calling Orchestrator.",
                error_code="HF090",
            )
        self.spark = spark
        self.logger.info("Using Spark app: %s", spark.sparkContext.appName)
        self.file_hunt_path = cfg_get(config, "file_hunt_path")
        self.system_run_report = pd.DataFrame()
        self.validated_master_specs: pd.DataFrame | None = None
        self._lineage_specs_snapshot: pd.DataFrame | None = None
        self._phase_errors: list[dict] = []
        self._last_load_results: list[LoadResult] = []
        self._last_dq_manifest: list[dict] = []
        self.logger.info("Run id: %s", self.run_id)

    @property
    def my_LoggingConfig(self) -> LoggingConfig:
        """Backward-compatible alias for :attr:`logging_config`."""
        return self.logging_config

    def run(self) -> RunResult:
        """
        Execute the full pipeline.

        Returns
        -------
        RunResult
            ``str(result)`` equals ``result.status.value`` for backward compatibility.
        """
        status = RunStatus.FAILED
        self._phase_errors = []
        self._last_load_results = []
        archived_log: str | None = None

        dq_manifest: list[dict] = []

        try:
            log_step(self.logger, "run", status="START", run_id=self.run_id)
            self.logger.warning(
                "INGESTION feeds load external data with reduced validation."
            )
            ready = self._system_prerequisites()
            if not ready:
                status = RunStatus.VALIDATION_FAILED
                self.logger.error(
                    "System is not ready; downstream feeds will not be processed."
                )
                self._emit_run_report(feed_manifest=[], load_results=[])
                return self._build_run_result(status, archived_log)

            status = RunStatus.COMPLETED_WITH_ERRORS

            run_phase(
                "validate_and_load",
                self._phase_errors,
                lambda: self._validate_and_load(dq_manifest_holder=dq_manifest),
                reraise=False,
            )
            run_phase(
                "lineage_diagram",
                self._phase_errors,
                self._generate_lineage_diagram,
                reraise=False,
            )

            if not self._phase_errors:
                status = RunStatus.COMPLETED
            log_step(
                self.logger, "run", status="OK", run_id=self.run_id, batch_status=status.value
            )

        except Exception as exc:
            wrapped = wrap_exception(exc, error_code="HF091")
            log_step(
                self.logger,
                "run",
                status="FAIL",
                run_id=self.run_id,
                error_code=wrapped.error_code,
                error_type=type(exc).__name__,
                error=wrapped.short_message(),
                exc_info=True,
            )
            self.logger.critical(
                "Unexpected orchestrator failure: %s", wrapped, exc_info=True
            )
            self._phase_errors.append(
                exception_to_record(wrapped, phase="orchestrator", error_code="HF091")
            )
            status = RunStatus.FAILED

        finally:
            archived_log = self._finalize_run(status)

        return self._build_run_result(status, archived_log)

    def _build_run_result(
        self, status: RunStatus, archived_log: str | None
    ) -> RunResult:
        message = ""
        if status == RunStatus.VALIDATION_FAILED:
            message = "Master specs or environment validation failed."
        elif status == RunStatus.COMPLETED_WITH_ERRORS:
            message = (
                f"Run completed with {len(self._phase_errors)} phase error(s). "
                "See logs and outbound Excel report."
            )
        return RunResult(
            status=status,
            run_id=self.run_id,
            load_results=list(self._last_load_results),
            phase_errors=list(self._phase_errors),
            archived_log_path=archived_log,
            message=message,
            master_specs=(
                self.validated_master_specs.copy()
                if self.validated_master_specs is not None
                else None
            ),
        )

    def _finalize_run(self, run_status: RunStatus) -> str | None:
        archived: str | None = None
        try:
            self.logging_config.write_run_summary(
                run_status=run_status.value,
                errors=self._phase_errors,
                extra_lines=[f"run_id={self.run_id}"],
                load_results=self._last_load_results,
                dq_summary=getattr(self, "_last_dq_manifest", None),
            )
        except Exception as exc:
            wrapped = wrap_exception(exc, error_code="HF094")
            self.logger.error(
                "Failed to write run summary [%s]: %s",
                wrapped.error_code,
                wrapped.short_message(),
                exc_info=True,
            )

        try:
            self.logger.warning(
                "Archiving logs; messages after this may not appear in the run log file."
            )
            self.logger.info("==FINAL LOG==")
            archived = self.logging_config.move_logs_to_final_location()
            if archived:
                self.logger.info("Archived log file: %s", archived)
        except Exception as exc:
            wrapped = wrap_exception(exc, error_code="HF094")
            self.logger.error(
                "Failed to archive logs [%s]: %s",
                wrapped.error_code,
                wrapped.short_message(),
                exc_info=True,
            )

        try:
            specs = self._lineage_specs_snapshot
            if specs is None:
                specs = pd.DataFrame()
            SystemCleanup(
                config=self.config,
                master_specs=specs,
                spark=self.spark,
            ).run()
        except Exception as exc:
            wrapped = wrap_exception(exc, error_code="HF095")
            self.logger.error(
                "Shutdown cleanup failed [%s]: %s",
                wrapped.error_code,
                wrapped.short_message(),
                exc_info=True,
            )

        self.logger.info("Thanks for using HanduFlow.")
        return archived

    def _system_prerequisites(self) -> bool:
        log_step(
            self.logger,
            "system_validation",
            status="START",
            file_hunt_path=self.file_hunt_path,
        )
        validator = SystemLaunchValidator(
            file_hunt_path=self.file_hunt_path,
            spark=self.spark,
            config=self.config,
        )
        validation_result = validator.run()
        log_step(
            self.logger,
            "system_validation",
            status="OK" if validation_result.passed else "FAIL",
            passed_rules=validation_result.passed_rules,
            total_rules=validation_result.total_rules,
        )
        self.validated_master_specs = validator.get_validated_master_specs()
        self._lineage_specs_snapshot = (
            self.validated_master_specs.copy()
            if self.validated_master_specs is not None
            else None
        )
        self.system_run_report = validation_result.results_df
        return validation_result.passed

    def _generate_lineage_diagram(self) -> None:
        if self._lineage_specs_snapshot is None:
            self.logger.warning("Skipping lineage: no validated master specs.")
            return
        DataFlowDiagramGenerator(
            validated_dataframe=self._lineage_specs_snapshot,
            config=self.config,
            run_id=self.run_id,
        ).run()

    def _validate_and_load(self, dq_manifest_holder: list[dict] | None = None) -> None:
        if self.validated_master_specs is None:
            log_step(self.logger, "validate_and_load", status="FAIL", reason="no_master_specs")
            self._emit_run_report(feed_manifest=[], load_results=[])
            return

        load_results: list[LoadResult] = []
        extraction_df = self.validated_master_specs[
            self.validated_master_specs["data_flow_direction"].map(is_ingestion_direction)
        ]
        if not extraction_df.empty:
            log_step(
                self.logger,
                "ingestion",
                status="START",
                feed_count=len(extraction_df),
            )
            controller = DataLoadController(
                allowed_df=extraction_df, spark=self.spark, config=self.config
            )
            controller.run()
            load_results.extend(controller.get_load_results())
            log_step(
                self.logger,
                "ingestion",
                status="OK",
                feed_count=len(extraction_df),
            )

        self.validated_master_specs = self.validated_master_specs[
            self.validated_master_specs["data_flow_direction"].map(
                is_within_unity_catalog_direction
            )
        ]

        if self.validated_master_specs.empty:
            log_step(
                self.logger,
                "unity_catalog_pipeline",
                status="SKIP",
                reason="no_within_unity_catalog_feeds",
            )
            self._last_load_results = load_results
            self._last_dq_manifest = []
            self._emit_run_report(feed_manifest=[], load_results=load_results)
            return

        catalog_count = len(self.validated_master_specs)
        log_step(
            self.logger,
            "unity_catalog_pipeline",
            status="START",
            feed_count=catalog_count,
        )

        dq_runner = FeedDataQualityRunner(
            self.spark,
            self.validated_master_specs.to_dict(orient="records"),
        )
        run_phase(
            "data_quality_pre_load",
            self._phase_errors,
            dq_runner.run,
            reraise=False,
            feed_count=catalog_count,
        )

        pre_load_manifest = dq_runner.finalize()
        ingestible_feed_ids = [
            entry["feed_id"]
            for entry in pre_load_manifest
            if entry.get("can_ingest") is True
        ]
        blocked_feed_ids = [
            entry["feed_id"]
            for entry in pre_load_manifest
            if entry.get("can_ingest") is not True
        ]
        self.logger.info("Feeds approved for ingest: %s", ingestible_feed_ids)
        for entry in pre_load_manifest:
            if entry.get("can_ingest") is True:
                continue
            self.logger.warning(
                "Feed blocked from load (pre-load DQ) | feed_id=%s reason=%s "
                "standard_passed=%s pre_load_passed=%s",
                entry.get("feed_id"),
                entry.get("ingest_block_reason"),
                entry.get("standard_checks_passed"),
                entry.get("comprehensive_pre_load_passed"),
            )
        if blocked_feed_ids:
            self.logger.warning(
                "Feeds not loaded due to pre-load DQ gate: %s",
                blocked_feed_ids,
            )

        loaded_feed_ids: set = set()
        if ingestible_feed_ids:
            allowed_df = self.validated_master_specs[
                self.validated_master_specs["feed_id"].isin(ingestible_feed_ids)
            ]
            controller = DataLoadController(
                allowed_df=allowed_df, spark=self.spark, config=self.config
            )
            run_phase(
                "unity_catalog_load",
                self._phase_errors,
                controller.run,
                reraise=False,
            )
            load_results.extend(controller.get_load_results())
            loaded_feed_ids = {
                r.feed_id
                for r in controller.get_load_results()
                if r.success and not r.skipped
            }

        run_phase(
            "data_quality_post_load",
            self._phase_errors,
            lambda: dq_runner.run_post_load_checks(loaded_feed_ids),
            reraise=False,
        )

        final_manifest = dq_runner.finalize()
        self._last_load_results = load_results
        self._last_dq_manifest = final_manifest
        if dq_manifest_holder is not None:
            dq_manifest_holder.clear()
            dq_manifest_holder.extend(final_manifest)
        self._emit_run_report(
            feed_manifest=final_manifest,
            load_results=load_results,
        )
        log_step(
            self.logger,
            "unity_catalog_pipeline",
            status="OK",
            loaded_feeds=len(loaded_feed_ids),
            skipped_feeds=len(blocked_feed_ids),
        )

    def _emit_run_report(
        self,
        feed_manifest: list[dict],
        load_results: list[LoadResult],
    ) -> None:
        run_phase(
            "result_generator",
            self._phase_errors,
            lambda: ResultGenerator(
                feed_manifest or [],
                file_hunt_path=self.file_hunt_path,
                run_id=self.run_id,
                config=self.config,
                system_report=self.system_run_report,
                load_results=load_results,
            ).run(),
            reraise=False,
        )
