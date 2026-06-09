# inbuilt
import os
import logging
import configparser
from concurrent.futures import ThreadPoolExecutor, as_completed


# external
import pandas as pd
from pyspark.sql import SparkSession

# internal
from handuflow.config.config_paths import cfg_get_int
from handuflow.config.run_logger import log_feed_event, log_step
from handuflow.data_movement_controller.load_dispatcher import LoadDispatcher
from handuflow.data_movement_controller.data_class.load_result import LoadResult
from handuflow.exception.error_handler import exception_message, wrap_exception


class DataLoadController:

    def __init__(
        self,
        spark: SparkSession,
        allowed_df: pd.DataFrame,
        config: configparser.ConfigParser,
    ) -> None:
        self.logger = logging.getLogger(__name__)
        self.logger.info("Data Load Controller has been initialized...")
        self.master_specs_df = allowed_df
        self.spark = spark
        self.load_results_list = []
        self.config = config
        self.max_workers = max(
            1,
            cfg_get_int(config, "max_concurrent_batches", 4, section="DEFAULT"),
        )

    def run(self):
        log_step(
            self.logger,
            "data_load_controller",
            status="START",
            feed_count=len(self.master_specs_df),
        )
        self.__prepare()
        self.load_results_list = self.__execute()
        log_step(
            self.logger,
            "data_load_controller",
            status="OK",
            feed_count=len(self.load_results_list),
        )

    def get_load_results(self) -> list[LoadResult]:
        return self.load_results_list

    def __run_group(self, group) -> list[LoadResult]:
        results = []

        feeds = group["feeds_in_group"]
        group_id = group["parallelism_group_number"]

        log_step(
            self.logger,
            f"parallelism_group.{group_id}",
            status="START",
            feed_count=len(feeds),
        )

        if len(feeds) > 1:
            workers = min(len(feeds), self.max_workers)
            with ThreadPoolExecutor(max_workers=workers) as executor:

                futures = {
                    executor.submit(self._dispatch_single_feed, feed): feed
                    for feed in feeds
                }

                for future in as_completed(futures):
                    feed = futures[future]
                    try:
                        results.append(future.result())
                    except Exception as exc:
                        wrapped = wrap_exception(
                            exc,
                            error_code="HF093",
                            feed_id=feed.get("feed_id"),
                        )
                        log_feed_event(
                            self.logger,
                            "feed.load",
                            feed_id=feed.get("feed_id"),
                            feed_name=feed.get("feed_name"),
                            load_type=feed.get("load_type"),
                            status="FAIL",
                            error_type=type(exc).__name__,
                            error_code=wrapped.error_code,
                            error=wrapped.short_message(),
                        )
                        self.logger.error(
                            "Unexpected error in parallel feed execution feed_id=%s: %s",
                            feed.get("feed_id"),
                            wrapped,
                            exc_info=True,
                        )
                        results.append(
                            LoadResult(
                                feed_id=feed.get("feed_id"),
                                success=False,
                                exception_if_any=exception_message(wrapped),
                                error_code=wrapped.error_code,
                            )
                        )

        else:
            results.append(self._dispatch_single_feed(feeds[0]))

        log_step(
            self.logger,
            f"parallelism_group.{group_id}",
            status="OK",
            feed_count=len(results),
        )
        return results

    def _dispatch_single_feed(self, feed: dict) -> LoadResult:
        feed_id = feed.get("feed_id")
        log_feed_event(
            self.logger,
            "feed.load",
            feed_id=feed_id,
            feed_name=feed.get("feed_name"),
            load_type=feed.get("load_type"),
            status="START",
            target_table=feed.get("target_table_name"),
        )
        result = LoadDispatcher(
            master_spec=feed, spark=self.spark, config=self.config
        ).dispatch()
        status = "OK" if result.success else "FAIL"
        log_feed_event(
            self.logger,
            "feed.load",
            feed_id=feed_id,
            feed_name=feed.get("feed_name"),
            load_type=feed.get("load_type"),
            status=status,
            target=result.target_table_path,
            rows_inserted=result.total_rows_inserted,
            rows_updated=result.total_rows_updated,
            rows_deleted=result.total_rows_deleted,
            duration=result.total_human_readable_time,
            error=result.exception_if_any,
        )
        return result

    def __execute(self) -> list[LoadResult]:
        results = []

        for group in self.execution_groups:
            results.extend(self.__run_group(group))

        self.__log_load_summary(results)
        return results

    def __log_load_summary(self, results: list[LoadResult]) -> None:
        if not results:
            return
        succeeded = sum(1 for r in results if r.success)
        failed = len(results) - succeeded
        self.logger.info(
            "Load batch summary: total=%s succeeded=%s failed=%s",
            len(results),
            succeeded,
            failed,
        )
        for result in results:
            if result.success:
                self.logger.info(
                    "Feed load succeeded | feed_id=%s target=%s inserted=%s updated=%s deleted=%s duration=%s",
                    result.feed_id,
                    result.target_table_path,
                    result.total_rows_inserted,
                    result.total_rows_updated,
                    result.total_rows_deleted,
                    result.total_human_readable_time,
                )
                continue
            self.logger.error(
                "Feed load failed | feed_id=%s target=%s error=%s",
                result.feed_id,
                result.target_table_path,
                result.exception_if_any,
                exc_info=result.exception_if_any is not None,
            )

    def __prepare(self):
        self.logger.info("Loading validated master specs...")
        self.logger.info("Building ordered execution groups...")

        # Sort first to guarantee execution order
        grouped = self.master_specs_df.sort_values("parallelism_group_number").groupby(
            "parallelism_group_number", sort=False
        )

        self.execution_groups = []

        for key, group in grouped:
            self.logger.info(f"Execution group {key}, Total Feeds: {len(group)}")

            self.execution_groups.append(
                {
                    "parallelism_group_number": key,
                    "feeds_in_group": group.to_dict(orient="records"),
                }
            )
