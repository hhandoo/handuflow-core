# inbuilt
import os
import uuid
import time
import random
import logging
import requests
from io import BytesIO
from requests.exceptions import RequestException

# external
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

# internal
from handuflow.config.catalog_resolver import CatalogResolver
from handuflow.config.config_paths import dmc_temp_dir
from handuflow.data_movement_controller.base_load_strategy import BaseLoadStrategy
from handuflow.data_movement_controller.data_class.load_config import LoadConfig
from handuflow.data_movement_controller.data_class.load_result import LoadResult
from handuflow.exception.extraction_exception import ExtractionException


class APIExtractor(BaseLoadStrategy):

    def __init__(self, config: LoadConfig, spark: SparkSession) -> None:
        super().__init__(config=config, spark=spark)
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.spark = spark
        catalog = CatalogResolver(
            self.config.target_unity_catalog, config=self.config.config
        )
        self.__bronze_schema = catalog.bronze_schema()
        
        self.logger.warning('API Extractor will always dump data in bronze schema as per medallion architecture.')

    def load(self) -> LoadResult:
        try:
            result_df = self.__extract()
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.__bronze_schema}")
            feed_temp = (
                f"{self.__bronze_schema}."
                f"{self.config.master_specs['target_table_name']}"
            )
            self.logger.info(f"Creating bronze table: {feed_temp}")
            schema = StructType.fromJson(self.config.feed_specs['selection_schema'])
            result_df = self._enforce_schema(result_df, schema)
            (
                result_df.write.
                format("delta")
                .mode("overwrite")
                .saveAsTable(feed_temp)
            )
            return LoadResult(
                feed_id = self.config.master_specs['feed_id'],
                success=True, 
                total_rows_inserted=result_df.count(),
                total_rows_updated=0,
                total_rows_deleted=0
            )
        except ExtractionException:
            raise
        except Exception as e:
            raise ExtractionException(
                message=(
                    f"Feed ID: {self.config.master_specs['feed_id']}, "
                    f"Error during API extraction for {self._current_target_table_name}: {e}"
                ),
                error_code="HF050",
                feed_id=self.config.master_specs.get("feed_id"),
                original_exception=e,
            )
    
    def __validate_content_type(self, response: requests.Response, expected: str) -> None:
        content_type = response.headers.get("Content-Type", "").lower()
        if expected == "json" and "application/json" not in content_type:
            raise ExtractionException(
                message=f"Expected JSON but got {content_type}",
                error_code="HF052",
                feed_id=self.config.master_specs.get("feed_id"),
            )
        if expected == "parquet":
            if not content_type.startswith("application/parquet"):
                raise ExtractionException(
                    message=f"Expected Parquet but got {content_type}",
                    error_code="HF052",
                    feed_id=self.config.master_specs.get("feed_id"),
                )

    def __extract(self) -> DataFrame:
        try:
            cfg = self.config.feed_specs["ingestion_config"]
            response_type = cfg["response"]["format"].lower()
            if response_type == "parquet":
                return self.__extract_parquet()
            raise ExtractionException(
                message=(
                    f"Unsupported API response format: {response_type!r}. "
                    "Only 'parquet' is implemented."
                ),
                error_code="HF051",
                feed_id=self.config.master_specs.get("feed_id"),
            )
        except ExtractionException:
            raise
        except Exception as exc:
            raise ExtractionException(
                message="Something went wrong while running API Extractor",
                original_exception=exc,
            )
        
    def __extract_parquet(self) -> DataFrame:
        response = self.__fetch_response()
        parquet_bytes = BytesIO(response.content)
        tmp_dir = os.path.join(
            dmc_temp_dir(self.config.config),
            f"api_parquet_mem_{uuid.uuid4().hex}",
        )
        os.makedirs(tmp_dir, exist_ok=True)
        file_path = f"{tmp_dir}/data.parquet"
        with open(file_path, "wb") as f:
            f.write(parquet_bytes.getbuffer())
        self.logger.info("Parquet loaded in memory at %s", file_path)
        # spark_path = file_path.replace("/dbfs", "dbfs:")
        self.logger.info("Reading Parquet into Spark from %s", file_path)
        return self.spark.read.parquet(file_path)

    def __fetch_response(self) -> requests.Response:
        cfg = self.config.feed_specs['ingestion_config']
        retry_cfg = cfg.get("retry", {})
        max_attempts = retry_cfg.get("max_attempts", 3)
        backoff_factor = retry_cfg.get("backoff_factor", 2.0)
        max_backoff = retry_cfg.get("max_backoff", 60)
        retry_statuses = set(
            retry_cfg.get("retry_statuses", [429, 500, 502, 503, 504])
        )
        expected_type = cfg.get("response", {}).get("format", "json")
        last_exception = None
        for attempt in range(1, max_attempts + 1):
            try:
                self.logger.info(
                    "API request attempt %s/%s: %s",
                    attempt, max_attempts, cfg['request']["url"]
                )
                resp = cfg['request']
                response = requests.request(
                    method=resp.get("method", "GET"),
                    url=resp["url"],
                    headers=resp.get("headers"),
                    params=resp.get("params"),
                    json=resp.get("body"),
                    timeout=resp.get("timeout", 30),
                    stream=(expected_type == "parquet")
                )
                self.__validate_content_type(response, expected_type)
                if response.status_code < 400:
                    return response
                if response.status_code in retry_statuses:
                    self.__sleep_before_retry(
                        attempt, backoff_factor, max_backoff, response
                    )
                    continue
                response.raise_for_status()
            except RequestException as exc:
                last_exception = exc
                self.logger.warning(
                    "Request error on attempt %s/%s: %s",
                    attempt, max_attempts, exc
                )
                if attempt >= max_attempts:
                    break
                self.__sleep_before_retry(attempt, backoff_factor, max_backoff)

        if last_exception:
            raise ExtractionException(
                message="API request failed after retries",
                error_code="HF053",
                feed_id=self.config.master_specs.get("feed_id"),
                original_exception=last_exception,
            )
        raise ExtractionException(
            message="API request failed after retries",
            error_code="HF053",
            feed_id=self.config.master_specs.get("feed_id"),
        )

    
    def __sleep_before_retry(
        self,
        attempt: int,
        backoff_factor: float,
        max_backoff: int,
        response: requests.Response | None = None
    ) -> None:
        retry_after = None
        if response is not None:
            retry_after = response.headers.get("Retry-After")
        if retry_after:
            sleep_time = min(int(retry_after), max_backoff)
        else:
            sleep_time = min(
                backoff_factor ** attempt + random.uniform(0, 1),
                max_backoff
            )
        self.logger.warning(
            "Retrying in %.2f seconds (attempt %s)",
            sleep_time, attempt
        )
        time.sleep(sleep_time)

