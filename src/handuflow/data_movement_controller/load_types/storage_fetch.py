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
from pyspark.sql.functions import input_file_name

# internal
from handuflow.config.catalog_resolver import CatalogResolver
from handuflow.data_movement_controller.base_load_strategy import BaseLoadStrategy
from handuflow.data_movement_controller.data_class.load_config import LoadConfig
from handuflow.data_movement_controller.data_class.load_result import LoadResult
from handuflow.exception.storage_fetch_exception import StorageFetchException


class StorageFetch(BaseLoadStrategy):

    def __init__(self, config: LoadConfig, spark: SparkSession) -> None:
        super().__init__(config=config, spark=spark)
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.spark = spark
        self.file_type = self.config.feed_specs['storage_config']['file_type']
        self.lookup_directory = self.config.feed_specs['storage_config']['lookup_directory']
        catalog = CatalogResolver(
            self.config.target_unity_catalog, config=self.config.config
        )
        self.__bronze_schema = catalog.bronze_schema()
        self.logger.warning('Storage Fetch will always dump data in bronze schema as per medallion architecture.')

    def load(self) -> LoadResult:
        try:

            results_df = self.__load_file_to_dataframe()
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.__bronze_schema}")
            feed_temp = (
                f"{self.__bronze_schema}."
                f"{self.config.master_specs['target_table_name']}"
            )
            self.logger.info(f"Creating bronze table: {feed_temp}")
            

            (
                results_df.write.
                format("delta")
                .mode("overwrite")
                .saveAsTable(feed_temp)
            )
            return LoadResult(
                feed_id = self.config.master_specs['feed_id'],
                success=True, 
                total_rows_inserted=results_df.count(),
                total_rows_updated=0,
                total_rows_deleted=0
            )
        except Exception as e:
            raise StorageFetchException(
                message=f"Feed ID: {self.config.master_specs['feed_id']}, Error during FULL LOAD for {self._current_target_table_name}: {str(e)}",
                original_exception=e
            )
    
    def __iterate_over_latest_medallion_directory(self, base_path) -> str:
        """
        Returns the maximum integer directory under base_path.
        Ignores files.
        """
        max_dir = float('-inf')
        for item in os.listdir(base_path):
            if max_dir < int(item):
                max_dir = int(item)
        return str(max_dir)
    
    def __load_file_to_dataframe(self) -> DataFrame:
        file_path = self.__build_file_destination_directory(self.lookup_directory)
        self.logger.info(f"Fetching data from path: {file_path}")

        if self.file_type == 'xml':
            df = (
                self.spark.read
                .format("xml")
                .option(
                    "rowTag",
                    self.config.feed_specs['storage_config']['xml_row_tag']
                )
                .load(file_path)
            )

        elif self.file_type == 'json':
            df = (
                self.spark.read
                .format("json")
                .load(file_path)
            )

        elif self.file_type == "parquet":
            df = self.spark.read.format("parquet").load(file_path)

        else:
            raise StorageFetchException(
                message="Invalid/missing value for [file_type] parameter in feed specs",
                error_code="HF061",
            )
        
        schema = StructType.fromJson(self.config.feed_specs['selection_schema'])
        df = self._enforce_schema(df, schema)
        df = df.withColumn("_x_source_file", input_file_name())

        return df
        
    def __build_file_destination_directory(self, base_path_prefix: str) -> str:
        storage_type = self.config.feed_specs['storage_config']['storage_type']
        is_multi_file = self.config.feed_specs['storage_config']['is_multi_file']
        inside_timestamp_dir = self.config.feed_specs['storage_config']['inside_timestamp_dir']
        file_name = self.config.feed_specs['storage_config']['file_name']

        if storage_type == 'MEDALLION':
            current_year = self.__iterate_over_latest_medallion_directory(base_path_prefix)
            current_month = self.__iterate_over_latest_medallion_directory(os.path.join(base_path_prefix, current_year))
            current_day = self.__iterate_over_latest_medallion_directory(os.path.join(base_path_prefix, current_year, current_month))
            latest_timestamp = self.__iterate_over_latest_medallion_directory(os.path.join(base_path_prefix, current_year, current_month, current_day))
            if is_multi_file == True:
                return f"{base_path_prefix}/{current_year}/{current_month}/{current_day}/{latest_timestamp}/{inside_timestamp_dir}/*.{self.file_type}"
            else:
                return f"{base_path_prefix}/{current_year}/{current_month}/{current_day}/{latest_timestamp}/{inside_timestamp_dir}/{file_name}"

        elif storage_type == 'STANDARD':
            if is_multi_file == True:
                return f"{base_path_prefix}/*.{self.file_type}"
            else:
                return f"{base_path_prefix}/{file_name}"
        else:
            raise StorageFetchException(
                message="Invalid/missing value for [storage_type] parameter in feed specs",
                error_code="HF062",
            )