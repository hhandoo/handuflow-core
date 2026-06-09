"""Partition mismatch detection (no Spark session)."""

from unittest.mock import MagicMock

from handuflow.data_movement_controller.load_types.full_load import FullLoad


def _strategy_with_partitions(existing: list[str], feed_keys: list[str]) -> FullLoad:
    strategy = FullLoad.__new__(FullLoad)
    strategy.spark = MagicMock()
    strategy.spark.catalog.tableExists.return_value = True
    strategy.spark.sql.return_value.first.return_value = {
        "partitionColumns": existing,
    }
    strategy.config = MagicMock()
    strategy.config.feed_specs = {"partition_keys": feed_keys}
    return strategy


def test_target_mismatch_when_removing_partitions():
    strategy = _strategy_with_partitions(["english"], [])
    assert strategy._target_partition_mismatch("silver.t_test") is True


def test_target_mismatch_when_adding_partitions():
    strategy = _strategy_with_partitions([], ["english"])
    assert strategy._target_partition_mismatch("silver.t_test") is True


def test_target_no_mismatch_when_both_unpartitioned():
    strategy = _strategy_with_partitions([], [])
    assert strategy._target_partition_mismatch("silver.t_test") is False
