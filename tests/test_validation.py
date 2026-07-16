from decimal import Decimal

import pytest
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType

from ecommerce_pipeline.validation import (
    validate_profit_reconciliation,
    validate_required_columns,
    validate_row_count,
    validate_unique_key,
)


@pytest.mark.unit
def test_required_columns_pass_when_present(spark):
    df = spark.createDataFrame([(1, "A")], ["id", "name"])
    validate_required_columns(df, {"id", "name"})


@pytest.mark.data_quality
def test_required_columns_report_all_missing_fields(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(ValueError, match="country, name"):
        validate_required_columns(df, {"id", "name", "country"})


@pytest.mark.unit
def test_unique_key_passes_for_unique_values(spark):
    validate_unique_key(spark.createDataFrame([(1,), (2,)], ["id"]), "id")


@pytest.mark.data_quality
def test_unique_key_rejects_duplicates(spark):
    with pytest.raises(ValueError, match="Duplicate"):
        validate_unique_key(spark.createDataFrame([(1,), (1,)], ["id"]), "id")


@pytest.mark.data_quality
def test_unique_key_rejects_nulls(spark):
    schema = StructType([StructField("id", LongType(), True)])
    with pytest.raises(ValueError, match="Null"):
        validate_unique_key(spark.createDataFrame([(None,)], schema), "id")


@pytest.mark.integration
def test_row_count_reconciliation_passes(spark):
    source = spark.createDataFrame([(1,), (2,)], ["id"])
    target = spark.createDataFrame([(1,), (2,)], ["id"])
    validate_row_count(source, target)


@pytest.mark.data_quality
def test_row_count_reconciliation_fails(spark):
    source = spark.createDataFrame([(1,), (2,)], ["id"])
    target = spark.createDataFrame([(1,)], ["id"])
    with pytest.raises(ValueError, match="source=2, target=1"):
        validate_row_count(source, target)


@pytest.mark.data_quality
def test_profit_reconciliation_detects_difference(spark):
    detail_schema = StructType(
        [StructField("profit", DecimalType(18, 2), False)]
    )
    aggregate_schema = StructType(
        [StructField("total_profit", DecimalType(18, 2), False)]
    )
    detail = spark.createDataFrame([(Decimal("10.00"),)], detail_schema)
    aggregate = spark.createDataFrame([(Decimal("9.00"),)], aggregate_schema)
    with pytest.raises(ValueError, match="Profit reconciliation failed"):
        validate_profit_reconciliation(detail, aggregate)
