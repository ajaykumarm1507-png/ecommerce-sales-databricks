"""Spark schemas used by the ingestion notebooks.

The pipeline uses explicit schemas so that a source change fails clearly instead of
silently changing the target table structure.
"""

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

CUSTOMER_RAW_SCHEMA = StructType(
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

PRODUCT_RAW_SCHEMA = StructType(
    [
        StructField("Product ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub-Category", StringType(), True),
        StructField("Product Name", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Price per product", DecimalType(18, 4), True),
    ]
)

ORDER_RAW_SCHEMA = StructType(
    [
        StructField("Row ID", LongType(), True),
        StructField("Order ID", StringType(), True),
        StructField("Order Date", StringType(), True),
        StructField("Ship Date", StringType(), True),
        StructField("Ship Mode", StringType(), True),
        StructField("Customer ID", StringType(), True),
        StructField("Product ID", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("Price", DecimalType(18, 2), True),
        StructField("Discount", DecimalType(5, 2), True),
        StructField("Profit", DecimalType(18, 2), True),
    ]
)

CUSTOMER_REQUIRED_COLUMNS = {
    "Customer ID",
    "Customer Name",
    "email",
    "phone",
    "Country",
    "Postal Code",
}

PRODUCT_REQUIRED_COLUMNS = {
    "Product ID",
    "Category",
    "Sub-Category",
    "Product Name",
    "Price per product",
}

ORDER_REQUIRED_COLUMNS = {
    "Row ID",
    "Order ID",
    "Order Date",
    "Ship Date",
    "Ship Mode",
    "Customer ID",
    "Product ID",
    "Quantity",
    "Price",
    "Discount",
    "Profit",
}
