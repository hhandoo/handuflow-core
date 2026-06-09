"""Isolated Spark warehouse + Derby metastore paths per test suite."""

from __future__ import annotations

import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def spark_data_dirs(suite: str) -> tuple[Path, Path]:
    """Return (warehouse_dir, metastore_dir) under tests/.spark/<suite>/."""
    base = PROJECT_ROOT / "tests" / ".spark" / suite
    base.mkdir(parents=True, exist_ok=True)
    return base / "warehouse", base / "metastore_db"


def reset_spark_suite(suite: str) -> None:
    """Delete warehouse + metastore for a suite (e.g. before regression after e2e)."""
    base = PROJECT_ROOT / "tests" / ".spark" / suite
    if base.exists():
        shutil.rmtree(base)


def with_isolated_hive(builder, warehouse: Path, metastore: Path):
    """Pin Hive/Derby to dedicated dirs so suites do not contend on metastore_db/."""
    warehouse.mkdir(parents=True, exist_ok=True)
    metastore.parent.mkdir(parents=True, exist_ok=True)
    return (
        builder.config("spark.sql.warehouse.dir", str(warehouse.resolve()))
        .config("spark.hadoop.hive.metastore.warehouse.dir", str(warehouse.resolve()))
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore.resolve()};create=true",
        )
    )
