from pyspark.sql import SparkSession
import pandas as pd
from handuflow.exception.validation_error import ValidationError

class ValidationContext:
    spark: SparkSession
    file_hunt_path: str
    raw_json: str
    master_specs_dataframe: pd.DataFrame
    mdf_feed_specs_array: list[dict] | None
    master_spec_name:str

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def get_master_specs(self) -> pd.DataFrame:
        return self.master_specs_dataframe
    
    def _get_table_columns(self, data: dict, rule_name: str) -> set:
        if "source_table_name" not in data:
            raise ValidationError(
                message="Missing 'source_table_name'",
                error_code="HF007",
                original_exception=None,
            )

        table_name = data["source_table_name"]

        try:
            df = self.spark.table(table_name)
            return {field.name for field in df.schema.fields}
        except Exception as e:
            raise ValidationError(
                message=f"Unable to read schema for table '{table_name}' ",
                error_code="HF012",
                original_exception=e,
            )

