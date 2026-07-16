from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ecommerce-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


CUSTOMER_TEST_SCHEMA = StructType(
    [
        StructField("Customer ID", StringType(), True),
        StructField("Customer Name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("Segment", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal Code", StringType(), True),
        StructField("Region", StringType(), True),
    ]
)

PRODUCT_TEST_SCHEMA = StructType(
    [
        StructField("Product ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True),
        StructField("Product Name", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Price per product", StringType(), True),
    ]
)

ORDER_TEST_SCHEMA = StructType(
    [
        StructField("Row ID", LongType(), True),
        StructField("Order ID", StringType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Ship Date", StringType(), True),
        StructField("Ship Mode", StringType(), True),
        StructField("Customer ID", StringType(), True),
        StructField("Product ID", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Price", StringType(), True),
        StructField("Discount", StringType(), True),
        StructField("Profit", StringType(), True),
    ]
)

ENRICHED_TEST_SCHEMA = StructType(
    [
        StructField("row_id", LongType(), False),
        StructField("order_year", IntegerType(), False),
        StructField("product_category", StringType(), False),
        StructField("product_sub_category", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("profit", DecimalType(18, 2), False),
    ]
)


def _merge(defaults: dict, overrides: dict) -> dict:
    row = defaults.copy()
    row.update(overrides)
    return row


@pytest.fixture
def make_customer_df(spark):
    defaults = {
        "Customer ID": "C-001",
        "Customer Name": "Alice Brown",
        "email": "ALICE@EXAMPLE.COM",
        "phone": "555-111-2222",
        "address": "1 Main Street",
        "Segment": "Consumer",
        "Country": "United States",
        "City": "Boston",
        "State": "Massachusetts",
        "Postal Code": "02108",
        "Region": "East",
    }

    def _factory(*rows, **overrides):
        if rows and overrides:
            raise ValueError("Use positional row dictionaries or keyword overrides, not both")
        prepared = [_merge(defaults, row) for row in rows] if rows else [_merge(defaults, overrides)]
        return spark.createDataFrame(prepared, CUSTOMER_TEST_SCHEMA)

    return _factory


@pytest.fixture
def make_product_df(spark):
    defaults = {
        "Product ID": "P-001",
        "Category": "Furniture",
        "Sub-Category": "Chairs",
        "Product Name": "Task Chair",
        "State": "New York",
        "Price per product": "25.5000",
    }

    def _factory(*rows, **overrides):
        if rows and overrides:
            raise ValueError("Use positional row dictionaries or keyword overrides, not both")
        prepared = [_merge(defaults, row) for row in rows] if rows else [_merge(defaults, overrides)]
        return spark.createDataFrame(prepared, PRODUCT_TEST_SCHEMA)

    return _factory


@pytest.fixture
def make_order_df(spark):
    defaults = {
        "Row ID": 1,
        "Order ID": "CA-2017-100001",
        "Order Date": "1/2/2017",
        "Ship Date": "3/2/2017",
        "Ship Mode": "Standard Class",
        "Customer ID": "C-001",
        "Product ID": "P-001",
        "Quantity": 2,
        "Price": "51.00",
        "Discount": "0.10",
        "Profit": "10.235",
    }

    def _factory(*rows, **overrides):
        if rows and overrides:
            raise ValueError("Use positional row dictionaries or keyword overrides, not both")
        prepared = [_merge(defaults, row) for row in rows] if rows else [_merge(defaults, overrides)]
        return spark.createDataFrame(prepared, ORDER_TEST_SCHEMA)

    return _factory


@pytest.fixture
def make_enriched_df(spark):
    def _factory(rows):
        prepared = [
            (
                row["row_id"],
                row["order_year"],
                row["product_category"],
                row["product_sub_category"],
                row["customer_id"],
                row["customer_name"],
                Decimal(str(row["profit"])),
            )
            for row in rows
        ]
        return spark.createDataFrame(prepared, ENRICHED_TEST_SCHEMA)

    return _factory
