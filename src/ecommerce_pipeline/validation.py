"""Reusable structural and reconciliation checks for the pipeline."""

from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_required_columns(df: DataFrame, required_columns: Iterable[str]) -> None:
    """Raise a clear error when a source is missing mandatory columns."""
    required = set(required_columns)
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def validate_unique_key(df: DataFrame, key_columns: str | list[str]) -> None:
    """Raise when a key is null or duplicated.

    This is called before dimension joins so accidental one-to-many joins are
    detected before they multiply order rows.
    """
    keys = [key_columns] if isinstance(key_columns, str) else key_columns
    null_condition = F.lit(False)
    for key in keys:
        null_condition = null_condition | F.col(key).isNull()

    if df.filter(null_condition).limit(1).count() > 0:
        raise ValueError(f"Null value found in key: {', '.join(keys)}")

    duplicate_exists = (
        df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count() > 0
    )
    if duplicate_exists:
        raise ValueError(f"Duplicate value found for key: {', '.join(keys)}")


def validate_row_count(source_df: DataFrame, target_df: DataFrame) -> None:
    source_count = source_df.count()
    target_count = target_df.count()
    if source_count != target_count:
        raise ValueError(
            f"Row-count reconciliation failed: source={source_count}, target={target_count}"
        )


def validate_profit_reconciliation(
    detail_df: DataFrame,
    aggregate_df: DataFrame,
    detail_column: str = "profit",
    aggregate_column: str = "total_profit",
) -> None:
    detail_total = detail_df.agg(F.sum(detail_column).alias("total")).first()["total"]
    aggregate_total = aggregate_df.agg(F.sum(aggregate_column).alias("total")).first()[
        "total"
    ]

    detail_total = detail_total or Decimal("0.00")
    aggregate_total = aggregate_total or Decimal("0.00")
    if detail_total != aggregate_total:
        raise ValueError(
            "Profit reconciliation failed: "
            f"detail={detail_total}, aggregate={aggregate_total}"
        )
