# inbuilt
import json
import logging
import traceback

# external
from pyspark.sql import SparkSession

# internal
from handuflow.config.run_logger import log_feed_event, log_step
from handuflow.data_quality.executors.standard_dq_executor import StandardDQExecutor
from handuflow.data_quality.executors.comprehensive_dq_executor import ComprehensiveDQExecutor
from handuflow.exception.error_handler import exception_to_record, resolve_error_code

logger = logging.getLogger(__name__)

PRE_LOAD_STAGE = "PRE_LOAD"
POST_LOAD_STAGE = "POST_LOAD"


class FeedDataQualityRunner:
    """
    Per-feed data quality: standard checks and PRE_LOAD comprehensive checks gate ingest;
    POST_LOAD comprehensive checks run after load and are report-only.
    """

    def __init__(self, spark: SparkSession, master_specs: list[dict]):
        self.spark = spark
        self.master_specs = master_specs
        self.standard_checker = StandardDQExecutor(spark)
        self.comprehensive_checker = ComprehensiveDQExecutor(spark)
        self.results: list[dict] = []
        self.logger = logger

    def run(self) -> None:
        """Run standard + PRE_LOAD comprehensive checks for every feed (isolated per feed)."""
        log_step(
            self.logger,
            "dq.pre_load",
            status="START",
            feed_count=len(self.master_specs),
        )
        self.results = []
        for feed_row in self.master_specs:
            feed_id = feed_row.get("feed_id")
            try:
                log_feed_event(
                    self.logger,
                    "dq.pre_load.feed",
                    feed_id=feed_id,
                    feed_name=feed_row.get("feed_name"),
                    status="START",
                )
                result = self._run_pre_load_for_feed(feed_row)
                log_feed_event(
                    self.logger,
                    "dq.pre_load.feed",
                    feed_id=feed_id,
                    feed_name=feed_row.get("feed_name"),
                    status="OK" if result.get("can_ingest") else "FAIL",
                    can_ingest=result.get("can_ingest"),
                    standard_passed=result.get("standard_checks_passed"),
                    pre_load_passed=result.get("comprehensive_pre_load_passed"),
                )
            except Exception as exc:
                self.logger.error(
                    "Pre-load DQ failed for feed_id=%s: %s",
                    feed_id,
                    exc,
                    exc_info=True,
                )
                result = self._failed_feed_result(feed_row, error=exc)
            self.results.append(self._attach_master_columns(result, feed_row))

        ingestible = sum(1 for r in self.results if r.get("can_ingest"))
        log_step(
            self.logger,
            "dq.pre_load",
            status="OK",
            feed_count=len(self.results),
            can_ingest_count=ingestible,
            blocked_count=len(self.results) - ingestible,
        )

    def run_post_load_checks(self, loaded_feed_ids: set) -> None:
        """
        Run POST_LOAD comprehensive checks for loaded feeds; record NOT_RUN for others.
        Failures are reported only — they do not roll back loads.
        """
        log_step(
            self.logger,
            "dq.post_load",
            status="START",
            loaded_feed_count=len(loaded_feed_ids),
            loaded_feed_ids=sorted(loaded_feed_ids),
        )
        for row in self.results:
            feed_id = row.get("feed_id")
            feed = self._feed_dict_for_id(feed_id)
            post_checks = self._comprehensive_checks_for_stage(feed, POST_LOAD_STAGE)
            if not post_checks:
                row["comprehensive_post_load_passed"] = None
                self.logger.info(
                    "feed_id=%s: no POST_LOAD comprehensive_checks configured.",
                    feed_id,
                )
                continue

            if feed_id not in loaded_feed_ids:
                row["comprehensive_results"] = list(row.get("comprehensive_results") or [])
                row["comprehensive_results"].extend(
                    self._not_run_post_load_results(feed, post_checks)
                )
                row["comprehensive_post_load_passed"] = None
                row["post_load_skipped_reason"] = (
                    "Feed was not loaded (pre-load DQ or load failed)."
                )
                log_feed_event(
                    self.logger,
                    "dq.post_load.feed",
                    feed_id=feed_id,
                    status="SKIP",
                    reason=row["post_load_skipped_reason"],
                    check_count=len(post_checks),
                )
                continue

            try:
                log_feed_event(
                    self.logger,
                    "dq.post_load.feed",
                    feed_id=feed_id,
                    status="START",
                    check_count=len(post_checks),
                )
                has_errors, check_results = self.comprehensive_checker.run(
                    feed, is_post_load=True
                )
                existing = list(row.get("comprehensive_results") or [])
                existing.extend(check_results)
                row["comprehensive_results"] = existing
                row["comprehensive_post_load_passed"] = not has_errors
                log_feed_event(
                    self.logger,
                    "dq.post_load.feed",
                    feed_id=feed_id,
                    status="OK" if not has_errors else "FAIL",
                    post_load_passed=not has_errors,
                )
            except Exception as exc:
                self.logger.error(
                    "Post-load DQ failed for feed_id=%s: %s",
                    feed_id,
                    exc,
                    exc_info=True,
                )
                existing = list(row.get("comprehensive_results") or [])
                existing.extend(
                    self._error_post_load_results(feed, post_checks, exc)
                )
                row["comprehensive_results"] = existing
                row["comprehensive_post_load_passed"] = False

        log_step(self.logger, "dq.post_load", status="OK")

    def finalize(self) -> list[dict]:
        return self.results

    def _finalize(self) -> list[dict]:
        return self.finalize()

    def _run_pre_load_for_feed(self, feed_row: dict) -> dict:
        feed = json.loads(feed_row["feed_specs"])
        table = feed.get("source_table_name", "")
        standard_checks = feed.get("standard_checks") or []
        pre_load_checks = self._comprehensive_checks_for_stage(feed, PRE_LOAD_STAGE)
        feed_id = feed_row.get("feed_id")

        result = {
            "check_table_name": table,
            "standard_checks_configured": len(standard_checks) > 0,
            "comprehensive_pre_load_configured": len(pre_load_checks) > 0,
            "comprehensive_post_load_configured": len(
                self._comprehensive_checks_for_stage(feed, POST_LOAD_STAGE)
            )
            > 0,
            "standard_checks_result": [],
            "comprehensive_results": [],
            "standard_checks_passed": None,
            "comprehensive_pre_load_passed": None,
            "comprehensive_post_load_passed": None,
            "can_ingest": True,
        }

        if standard_checks:
            self.logger.info(
                "feed_id=%s: running %s standard check(s) on %s",
                feed_id,
                len(standard_checks),
                table,
            )
            result.update(self._run_standard_checks(feed, feed_id=feed_id))
        else:
            self.logger.info(
                "feed_id=%s: no standard_checks configured; skipping standard DQ.",
                feed_id,
            )
            result["standard_checks_passed"] = None

        if pre_load_checks:
            self.logger.info(
                "feed_id=%s: running %s PRE_LOAD comprehensive check(s)",
                feed_id,
                len(pre_load_checks),
            )
            has_errors, check_results = self.comprehensive_checker.run(
                feed, is_post_load=False
            )
            result["comprehensive_results"] = check_results
            result["comprehensive_pre_load_passed"] = not has_errors
        else:
            self.logger.info(
                "feed_id=%s: no PRE_LOAD comprehensive_checks; skipping.",
                feed_id,
            )
            result["comprehensive_pre_load_passed"] = None

        result["can_ingest"] = self._can_ingest(result)
        if not result["can_ingest"]:
            self.logger.warning(
                "feed_id=%s blocked from ingest | standard_passed=%s pre_load_passed=%s",
                feed_id,
                result.get("standard_checks_passed"),
                result.get("comprehensive_pre_load_passed"),
            )
        return result

    def _run_standard_checks(self, feed: dict, *, feed_id) -> dict:
        table = feed["source_table_name"]
        self.logger.info("feed_id=%s: reading table %s for standard checks", feed_id, table)
        df = self.spark.table(table)
        total_count = df.count()
        self.logger.info("feed_id=%s: source row count=%s", feed_id, total_count)
        passed = True
        check_results = []
        for idx, check in enumerate(feed.get("standard_checks", [])):
            column = check.get("column_name")
            threshold = check.get("threshold", 0)
            for method in check.get("check_sequence", []):
                self.logger.info(
                    "feed_id=%s: standard check [%s] method=%s column=%s threshold=%s",
                    feed_id,
                    idx,
                    method,
                    column,
                    threshold,
                )
                ok = self.standard_checker.run_check(
                    method_name=method,
                    df=df,
                    column=column,
                    total_count=total_count,
                    threshold=threshold,
                )
                if not ok["passed"]:
                    self.logger.error(
                        "feed_id=%s: standard check FAILED method=%s column=%s "
                        "bad_count=%s total=%s failure_pct=%s",
                        feed_id,
                        method,
                        column,
                        ok.get("bad_count"),
                        ok.get("total_count"),
                        ok.get("failure_percentage"),
                    )
                passed = passed and bool(ok["passed"])
                check_results.append(ok)
        return {
            "standard_checks_passed": passed,
            "standard_checks_result": check_results,
        }

    @staticmethod
    def _can_ingest(result: dict) -> bool:
        if result.get("standard_checks_configured") and not result.get(
            "standard_checks_passed"
        ):
            return False
        if result.get("comprehensive_pre_load_configured") and result.get(
            "comprehensive_pre_load_passed"
        ) is False:
            return False
        return True

    @staticmethod
    def _comprehensive_checks_for_stage(feed: dict, stage: str) -> list[dict]:
        checks = []
        for check in feed.get("comprehensive_checks") or []:
            load_stage = (check.get("load_stage") or PRE_LOAD_STAGE).upper()
            if load_stage == stage:
                checks.append(check)
        return checks

    def _feed_dict_for_id(self, feed_id) -> dict:
        for feed_row in self.master_specs:
            if feed_row.get("feed_id") == feed_id:
                return json.loads(feed_row["feed_specs"])
        return {}

    @staticmethod
    def _not_run_post_load_results(feed: dict, post_checks: list[dict]) -> list[dict]:
        return [
            {
                "table": feed.get("source_table_name"),
                "check_name": c.get("check_name"),
                "load_stage": POST_LOAD_STAGE,
                "query": c.get("query"),
                "failed_records": 0,
                "threshold": c.get("threshold", 0),
                "status": "NOT_RUN",
                "severity": c.get("severity", ""),
                "did_check_pass": None,
                "message": "Feed was not loaded; post-load check skipped.",
            }
            for c in post_checks
        ]

    @staticmethod
    def _error_post_load_results(
        feed: dict, post_checks: list[dict], exc: Exception
    ) -> list[dict]:
        return [
            {
                "table": feed.get("source_table_name"),
                "check_name": c.get("check_name"),
                "load_stage": POST_LOAD_STAGE,
                "query": c.get("query"),
                "failed_records": 0,
                "threshold": c.get("threshold", 0),
                "status": "ERROR",
                "severity": c.get("severity", ""),
                "did_check_pass": False,
                "error": str(exc),
            }
            for c in post_checks
        ]

    def _attach_master_columns(self, result: dict, feed_row: dict) -> dict:
        for col in (
            "feed_id",
            "system_name",
            "subsystem_name",
            "category",
            "sub_category",
            "data_flow_direction",
            "residing_layer",
            "feed_name",
            "feed_type",
            "load_type",
            "target_table_name",
        ):
            result[col] = feed_row.get(col)
        return result

    @staticmethod
    def _failed_feed_result(feed_row: dict, error: Exception) -> dict:
        table = ""
        try:
            table = json.loads(feed_row.get("feed_specs", "{}")).get(
                "source_table_name", ""
            )
        except json.JSONDecodeError:
            pass
        record = exception_to_record(
            error,
            feed_id=feed_row.get("feed_id"),
            error_code=resolve_error_code(error),
        )
        return {
            "check_table_name": table,
            "standard_checks_configured": False,
            "comprehensive_pre_load_configured": False,
            "comprehensive_post_load_configured": False,
            "standard_checks_passed": False,
            "standard_checks_result": [],
            "comprehensive_pre_load_passed": False,
            "comprehensive_post_load_passed": None,
            "comprehensive_results": [],
            "can_ingest": False,
            "dq_error": record.get("message") or str(error),
            "dq_error_code": record.get("error_code"),
            "dq_traceback": record.get("traceback") or traceback.format_exc(),
        }

    def adhoc_post_load(self) -> None:
        """Backward-compatible alias; prefer :meth:`run_post_load_checks`."""
        loaded = {r["feed_id"] for r in self.results if r.get("can_ingest")}
        self.run_post_load_checks(loaded)
