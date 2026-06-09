# inbuilt
import logging

# internal
from handuflow.exception.data_quality_exception import DataQualityException

logger = logging.getLogger(__name__)

PRE_LOAD = "PRE_LOAD"
POST_LOAD = "POST_LOAD"


class ComprehensiveDQExecutor:
    """SQL-based comprehensive checks; rows returned over threshold mean failure."""

    def __init__(self, spark):
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    def run(self, feed_spec: dict, is_post_load: bool = False) -> tuple[bool, list[dict]]:
        stage_label = POST_LOAD if is_post_load else PRE_LOAD
        has_errors = False
        check_result: list[dict] = []
        self.logger.info(
            "Running %s comprehensive DQ checks on %s",
            stage_label,
            feed_spec.get("source_table_name"),
        )

        for check in feed_spec.get("comprehensive_checks", []):
            check_stage = (check.get("load_stage") or PRE_LOAD).upper()
            if is_post_load and check_stage == PRE_LOAD:
                continue
            if not is_post_load and check_stage == POST_LOAD:
                continue

            try:
                entry = self._run_single_check(feed_spec, check)
                check_result.append(entry)
                if entry["status"] in ("FAILED", "ERROR"):
                    has_errors = True
            except Exception as exc:
                self.logger.error(
                    "Comprehensive check %s failed: %s",
                    check.get("check_name"),
                    exc,
                    exc_info=True,
                )
                check_result.append(self._failed_check_entry(feed_spec, check, exc))
                has_errors = True

        return has_errors, check_result

    def _run_single_check(self, feed_spec: dict, check: dict) -> dict:
        dependency_ds = check.get("dependency_dataset") or []
        for dds in dependency_ds:
            if not self.spark.catalog.tableExists(dds):
                raise DataQualityException(
                    message=f"Dependency table not found: {dds}",
                    error_code="HF070",
                    original_exception=None,
                )

        query = check.get("query")
        severity = (check.get("severity") or "ERROR").upper()
        threshold = check.get("threshold", 0)
        load_stage = (check.get("load_stage") or PRE_LOAD).upper()

        self.logger.info(
            "Executing %s check [%s] on %s",
            load_stage,
            check.get("check_name"),
            feed_spec.get("source_table_name"),
        )
        df = self.spark.sql(query)
        count = df.count()
        status = "PASSED"
        did_check_pass = True
        if count > threshold:
            status = "FAILED"
            did_check_pass = False
            self.logger.error(
                "Comprehensive check FAILED | stage=%s name=%s table=%s "
                "failed_records=%s threshold=%s",
                load_stage,
                check.get("check_name"),
                feed_spec.get("source_table_name"),
                count,
                threshold,
            )
        else:
            self.logger.info(
                "Comprehensive check PASSED | stage=%s name=%s failed_records=%s",
                load_stage,
                check.get("check_name"),
                count,
            )

        return {
            "table": feed_spec.get("source_table_name"),
            "check_name": check.get("check_name"),
            "load_stage": load_stage,
            "query": query,
            "failed_records": count,
            "threshold": threshold,
            "status": status,
            "severity": severity,
            "did_check_pass": did_check_pass,
        }

    @staticmethod
    def _failed_check_entry(feed_spec: dict, check: dict, exc: Exception) -> dict:
        return {
            "table": feed_spec.get("source_table_name"),
            "check_name": check.get("check_name"),
            "load_stage": (check.get("load_stage") or PRE_LOAD).upper(),
            "query": check.get("query"),
            "failed_records": 0,
            "threshold": check.get("threshold", 0),
            "status": "ERROR",
            "severity": (check.get("severity") or "ERROR").upper(),
            "did_check_pass": False,
            "error": str(exc),
        }
