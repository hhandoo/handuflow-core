import logging
from functools import reduce
import pyspark.sql.functions as F

from handuflow.exception.data_quality_exception import DataQualityException

logger = logging.getLogger(__name__)


class StandardDQExecutor:
    """
    Standalone standard data quality checks.
    All checks return a uniform result structure.
    """

    DEFAULT_PARAMS = {
        "threshold": 0.0,
        "allowed_values": [],
        "range": {"min": float("-inf"), "max": float("inf")},
    }

    def __init__(self, spark):
        self.spark = spark

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run_check(
        self,
        method_name: str,
        df,
        column=None,
        total_count: int = 0,
        **params,
    ) -> dict:
        """
        Executes a single standard DQ check.
        Returns a uniform result dictionary.
        """

        if not hasattr(self, method_name):
            raise DataQualityException(
                message=f"Unsupported check method: {method_name}",
                error_code="HF071",
            )

        final_params = {**self.DEFAULT_PARAMS, **params}
        self._validate_parameters(final_params)

        method = getattr(self, method_name)

        logger.info(
            "Executing %s with %s on %s",
            method_name,
            self._format_context(method_name, final_params),
            column
        )

        bad_count = method(
            df=df,
            column=column,
            total_count=total_count,
            **final_params,
        )

        threshold = final_params.get("threshold", 0.0)
        passed = self._is_within_threshold(bad_count, total_count, threshold)

        result = {
            "check_name": method_name,
            "column": column,
            "passed": passed,
            "bad_count": bad_count,
            "total_count": total_count,
            "failure_percentage": (
                (float(bad_count) / float(total_count))*float(100) if total_count > 0 else 0.0
            ),
            "failure_ratio": (
                float(bad_count) / float(total_count) if total_count > 0 else 0.0
            ),
            "threshold": threshold,
            "threshold_percentage": float(threshold)*float(100),
        }

        if passed:
            logger.info("%s PASSED on %s", method_name, column)
        else:
            logger.error("%s FAILED on %s", method_name, column)

        return result

    # ------------------------------------------------------------------
    # Parameter Validation
    # ------------------------------------------------------------------
    def _validate_parameters(self, params: dict):
        if not isinstance(params["threshold"], (int, float)):
            raise DataQualityException(
                message="Threshold must be numeric.",
                error_code="HF072",
            )
        if not isinstance(params["allowed_values"], list):
            raise DataQualityException(
                message="Allowed values must be a list.",
                error_code="HF072",
            )
        if not all(
            isinstance(params["range"][k], (int, float)) for k in ("min", "max")
        ):
            raise DataQualityException(
                message="Range bounds must be numeric.",
                error_code="HF072",
            )

    # ------------------------------------------------------------------
    # Logging Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _format_context(method_name: str, params: dict) -> str:
        if method_name == "_check_value_range":
            r = params["range"]
            return f"range=[{r['min']}, {r['max']}]"
        if method_name == "_check_allowed_values":
            vals = params["allowed_values"]
            preview = vals[:5]
            more = "..." if len(vals) > 5 else ""
            return f"allowed_values={preview}{more}"
        return f"threshold={params.get('threshold')}"

    # ------------------------------------------------------------------
    # Shared Helper
    # ------------------------------------------------------------------
    @staticmethod
    def _is_within_threshold(bad_count, total_count, threshold) -> bool:
        return total_count == 0 or (bad_count / total_count) <= threshold

    # ------------------------------------------------------------------
    # CHECK FUNCTIONS
    # Each check RETURNS bad_count (int)
    # ------------------------------------------------------------------
    def _check_nulls(self, df, column, total_count, **_):
        return df.filter(F.col(column).isNull()).count()

    def _check_value_range(self, df, column, total_count, range, **_):
        return df.filter(
            (F.col(column) < range["min"]) | (F.col(column) > range["max"])
        ).count()

    def _check_allowed_values(self, df, column, total_count, allowed_values, **_):
        return df.filter(~F.col(column).isin(allowed_values)).count()

    def _check_duplicates(self, df, column=None, total_count=0, **_):
        cols = [column] if column else df.columns
        return df.groupBy(cols).count().filter(F.col("count") > 1).count()

    def _check_composite_key(self, df, column, total_count, **_):
        cols = column if isinstance(column, list) else [column]

        nulls = df.filter(
            reduce(lambda x, y: x | y, [F.col(c).isNull() for c in cols])
        ).count()

        dups = df.groupBy(cols).count().filter(F.col("count") > 1).count()

        return nulls + dups

    def _check_primary_key(self, df, column, total_count, **_):
        cols = [column] if isinstance(column, str) else column

        nulls = df.filter(
            reduce(lambda x, y: x | y, [F.col(c).isNull() for c in cols])
        ).count()

        dups = df.groupBy(cols).count().filter(F.col("count") > 1).count()

        return nulls + dups
