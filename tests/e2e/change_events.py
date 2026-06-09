"""Apply inserts / updates / deletes to Delta source tables (Spark-native at scale)."""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from tests.e2e.data_generator import BUSINESS_COLUMNS, generate_dataset

# Above this row count, use hash-based Spark SQL mutations (no driver collect).
_SPARK_NATIVE_THRESHOLD = 10_000


@dataclass
class ChangeStats:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0


@dataclass
class CumulativeChangeStats:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    rounds: int = 0
    per_round: list[ChangeStats] = field(default_factory=list)

    def add(self, stats: ChangeStats) -> None:
        self.inserted += stats.inserted
        self.updated += stats.updated
        self.deleted += stats.deleted
        self.rounds += 1
        self.per_round.append(stats)


def _hash_predicate(seed: int, pct: float, *, offset: int = 0) -> str:
    """Deterministic row selection: ~pct% of rows match."""
    if pct <= 0:
        return "1=0"
    bucket = min(99, max(1, int(pct)))
    return f"pmod(abs(hash(id, {seed + offset})), 100) < {bucket}"


def _apply_changes_spark_native(
    spark: SparkSession,
    source_table: str,
    *,
    insert_pct: float,
    update_pct: float,
    delete_pct: float,
    seed: int,
) -> ChangeStats:
    base = spark.table(source_table)
    n = base.count()
    if n == 0:
        return ChangeStats()

    del_pred = _hash_predicate(seed, delete_pct)
    upd_pred = _hash_predicate(seed, update_pct, offset=1)

    del_count = base.filter(del_pred).count() if delete_pct else 0
    if del_count:
        spark.sql(f"DELETE FROM {source_table} WHERE {del_pred}")

    upd_count = (
        spark.table(source_table).filter(upd_pred).count() if update_pct else 0
    )
    if upd_count:
        spark.sql(
            f"""
            UPDATE {source_table}
            SET name = concat(name, '-UPDATED'),
                modified_date = current_date(),
                amount = coalesce(amount, 0) + 1.0
            WHERE {upd_pred}
            """
        )

    ins_n = max(0, int(n * insert_pct / 100))
    if ins_n:
        max_id_row = spark.table(source_table).agg(F.max("id").alias("m")).collect()
        max_id = max_id_row[0]["m"] or 0
        new_df = generate_dataset(spark, ins_n, seed=seed + 1)
        new_df = (
            new_df.withColumn("id", F.col("id") + F.lit(max_id))
            .withColumn(
                "business_key",
                F.concat(F.lit("BK-NEW-"), F.col("id").cast("string")),
            )
        )
        new_df.write.format("delta").mode("append").saveAsTable(source_table)

    return ChangeStats(inserted=ins_n, updated=upd_count, deleted=del_count)


def _apply_changes_exact(
    spark: SparkSession,
    source_table: str,
    *,
    insert_pct: float,
    update_pct: float,
    delete_pct: float,
    seed: int,
) -> ChangeStats:
    """Exact-percentage mutations for smaller datasets."""
    rng = random.Random(seed)
    base = spark.table(source_table)
    ids = [r.id for r in base.select("id").collect()]
    if not ids:
        return ChangeStats()

    n = len(ids)
    ins_n = max(0, int(n * insert_pct / 100))
    upd_n = max(0, int(n * update_pct / 100))
    del_n = max(0, int(n * delete_pct / 100))

    del_ids = set(rng.sample(ids, min(del_n, n)))
    upd_ids = set(
        rng.sample([i for i in ids if i not in del_ids], min(upd_n, n - len(del_ids)))
    )
    stats = ChangeStats(deleted=len(del_ids), updated=len(upd_ids))

    if del_ids:
        spark.sql(
            f"DELETE FROM {source_table} WHERE id IN ({','.join(str(i) for i in del_ids)})"
        )

    if upd_ids:
        spark.sql(
            f"""
            UPDATE {source_table}
            SET name = concat(name, '-UPDATED'),
                modified_date = current_date(),
                amount = coalesce(amount, 0) + 1.0
            WHERE id IN ({','.join(str(i) for i in upd_ids)})
            """
        )

    if ins_n:
        max_id = max(ids)
        new_df = generate_dataset(spark, ins_n, seed=seed + 1)
        new_df = (
            new_df.withColumn("id", F.col("id") + F.lit(max_id))
            .withColumn(
                "business_key",
                F.concat(F.lit("BK-NEW-"), F.col("id").cast("string")),
            )
        )
        new_df.write.format("delta").mode("append").saveAsTable(source_table)
        stats.inserted = ins_n

    return stats


def apply_changes(
    spark: SparkSession,
    source_table: str,
    *,
    row_count: int,
    insert_pct: float,
    update_pct: float,
    delete_pct: float,
    seed: int = 99,
) -> ChangeStats:
    """Mutate source; uses Spark-native ops for multi-million row tables."""
    if row_count >= _SPARK_NATIVE_THRESHOLD:
        return _apply_changes_spark_native(
            spark,
            source_table,
            insert_pct=insert_pct,
            update_pct=update_pct,
            delete_pct=delete_pct,
            seed=seed,
        )
    return _apply_changes_exact(
        spark,
        source_table,
        insert_pct=insert_pct,
        update_pct=update_pct,
        delete_pct=delete_pct,
        seed=seed,
    )


def apply_change_rounds(
    spark: SparkSession,
    source_table: str,
    *,
    row_count: int,
    rounds: list[tuple[float, float, float]],
    base_seed: int = 99,
) -> CumulativeChangeStats:
    cumulative = CumulativeChangeStats()
    for i, (ins, upd, del_) in enumerate(rounds):
        stats = apply_changes(
            spark,
            source_table,
            row_count=row_count,
            insert_pct=ins,
            update_pct=upd,
            delete_pct=del_,
            seed=base_seed + i * 17,
        )
        cumulative.add(stats)
    return cumulative


def capture_baseline(df: DataFrame) -> DataFrame:
    cols = [c for c in BUSINESS_COLUMNS if c in df.columns]
    return df.select(*cols)
