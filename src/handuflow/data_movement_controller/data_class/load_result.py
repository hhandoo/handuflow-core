# inbuilt
from dataclasses import dataclass, field
from typing import Optional

# external
from pyspark.sql import DataFrame


@dataclass
class LoadResult:
    feed_id: int
    success: bool
    skipped: bool = False
    start_epoch: float = 0.0
    end_epoch: float = 0.0
    total_human_readable_time: str = ""
    source_table_path: str = ""
    target_table_path: str = ""
    data_flow_direction: str = ""
    total_rows_inserted: int = 0
    total_rows_updated: int = 0
    total_rows_deleted: int = 0
    exception_if_any: Optional[str] = None
    error_code: Optional[str] = None

