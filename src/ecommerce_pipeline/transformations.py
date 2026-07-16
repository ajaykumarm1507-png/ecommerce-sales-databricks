"""Pure PySpark transformations used by notebooks and PyTest tests."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from ecommerce_pipeline.schemas import (
    CUSTOMER_REQUIRED_COLUMNS,
    ORDER_REQUIRED_COLUMNS,
    PRODUCT_REQUIRED_COLUMNS,
)
from ecommerce_pipeline.validation import validate_required_columns

MONEY_TYPE = DecimalType(18, 2)
PRODUCT_PRICE_TYPE = DecimalType(18, 4)
DISCOUNT_TYPE = DecimalType(5, 2)


def _trimmed(column_name: str):
    return F.trim(F.col(column_name).cast("string"))


def clean_customers(raw_df: DataFrame) -> DataFrame:
    """Clean and consolidate customer records.

    The source customer ID is the business key. Name corrections are conservative:
    numeric and control characters are removed, spacing is normalized, and the raw
    value is retained for traceability.
    """
    validate_required_columns(raw_df, CUSTOMER_REQUIRED_COLUMNS)

    customer_name_raw = _trimmed("Customer Name")
    cleaned_name = F.trim(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(customer_name_raw, r"[^A-Za-z '-]", " "),
                r"\s+",
                " ",
            ),
            r"\s*-\s*",
            "-",
        )
    )

    postal_code = F.regexp_replace(_trimmed("Postal Code"), r"\.0$", "")
    normalized_postal_code = F.when(
        postal_code.rlike(r"^[0-9]+$") & (F.length(postal_code) < 5),
        F.lpad(postal_code, 5, "0"),
    ).otherwise(postal_code)

    email = F.lower(_trimmed("email"))
    valid_email = email.rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    phone = F.when(
        _trimmed("phone").isin("", "#ERROR!"), F.lit(None).cast("string")
    ).otherwise(_trimmed("phone"))

    prepared = raw_df.select(
        F.upper(_trimmed("Customer ID")).alias("customer_id"),
        customer_name_raw.alias("customer_name_raw"),
        F.when(cleaned_name.isNull() | (cleaned_name == ""), F.lit("Unknown"))
        .otherwise(cleaned_name)
        .alias("customer_name"),
        email.alias("email"),
        phone.alias("phone"),
        _trimmed("address").alias("address"),
        _trimmed("Segment").alias("segment"),
        _trimmed("Country").alias("country"),
        _trimmed("City").alias("city"),
        _trimmed("State").alias("state"),
        normalized_postal_code.alias("postal_code"),
        _trimmed("Region").alias("region"),
        F.when(customer_name_raw.isNull() | (customer_name_raw == ""), "MISSING")
        .when(cleaned_name != customer_name_raw, "CLEANED")
        .otherwise("VALID")
        .alias("customer_name_quality_flag"),
        F.when(valid_email, "VALID").otherwise("INVALID").alias(
            "email_quality_flag"
        ),
        F.when(phone.isNull(), "INVALID").otherwise("VALID").alias(
            "phone_quality_flag"
        ),
    )

    # Prefer the most complete row if a duplicate customer arrives later.
    completeness_score = sum(
        F.when(F.col(c).isNotNull() & (F.col(c) != ""), 1).otherwise(0)
        for c in [
            "customer_name",
            "email",
            "phone",
            "address",
            "country",
            "city",
            "state",
            "postal_code",
        ]
    )
    window = Window.partitionBy("customer_id").orderBy(
        F.desc(completeness_score), F.asc("customer_name"), F.asc("email")
    )
    counts = Window.partitionBy("customer_id")

    return (
        prepared.withColumn("source_record_count", F.count("*").over(counts))
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def clean_products(raw_df: DataFrame) -> DataFrame:
    """Create one deterministic product record per Product ID.

    Product IDs are duplicated in the supplied file. Category and sub-category are
    consistent for those IDs, while product name, state and unit price may differ.
    Consolidating before the order join prevents row multiplication.
    """
    validate_required_columns(raw_df, PRODUCT_REQUIRED_COLUMNS)

    prepared = raw_df.select(
        F.upper(_trimmed("Product ID")).alias("product_id"),
        _trimmed("Category").alias("category"),
        _trimmed("Sub-Category").alias("sub_category"),
        _trimmed("Product Name").alias("product_name"),
        _trimmed("State").alias("state"),
        F.col("Price per product").cast(PRODUCT_PRICE_TYPE).alias("unit_price"),
    ).filter(F.col("product_id").isNotNull() & (F.col("product_id") != ""))

    return (
        prepared.groupBy("product_id")
        .agg(
            F.min("category").alias("category"),
            F.min("sub_category").alias("sub_category"),
            F.min("product_name").alias("canonical_product_name"),
            F.count("*").alias("source_record_count"),
            F.countDistinct("product_name").alias("product_name_count"),
            F.countDistinct("category").alias("category_count"),
            F.countDistinct("sub_category").alias("sub_category_count"),
            F.countDistinct("state").alias("state_count"),
            F.min("unit_price").cast(PRODUCT_PRICE_TYPE).alias("min_unit_price"),
            F.max("unit_price").cast(PRODUCT_PRICE_TYPE).alias("max_unit_price"),
        )
        .withColumn(
            "product_name_conflict_flag", F.col("product_name_count") > F.lit(1)
        )
        .withColumn("category_conflict_flag", F.col("category_count") > F.lit(1))
        .withColumn(
            "sub_category_conflict_flag", F.col("sub_category_count") > F.lit(1)
        )
    )


def clean_orders(raw_df: DataFrame) -> DataFrame:
    """Standardize order data and attach row-level quality results."""
    validate_required_columns(raw_df, ORDER_REQUIRED_COLUMNS)

    normalized = raw_df.select(
        F.col("Row ID").cast("long").alias("row_id"),
        _trimmed("Order ID").alias("order_id"),
        F.try_to_timestamp(_trimmed("Order Date"), F.lit("d/M/yyyy")).cast("date").alias("order_date"),
        F.try_to_timestamp(_trimmed("Ship Date"), F.lit("d/M/yyyy")).cast("date").alias("ship_date"),
        _trimmed("Ship Mode").alias("ship_mode"),
        F.upper(_trimmed("Customer ID")).alias("customer_id"),
        F.upper(_trimmed("Product ID")).alias("product_id"),
        F.col("Quantity").cast("int").alias("quantity"),
        F.col("Price").cast(MONEY_TYPE).alias("price"),
        F.col("Discount").cast(DISCOUNT_TYPE).alias("discount"),
        F.round(F.col("Profit").cast("decimal(18,4)"), 2)
        .cast(MONEY_TYPE)
        .alias("profit"),
    )

    row_id_count = F.count("*").over(Window.partitionBy("row_id"))
    with_counts = normalized.withColumn("_row_id_count", row_id_count)

    errors = F.concat_ws(
        "|",
        F.when(F.col("row_id").isNull(), "MISSING_ROW_ID"),
        F.when(F.col("row_id").isNotNull() & (F.col("_row_id_count") > 1), "DUPLICATE_ROW_ID"),
        F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), "MISSING_ORDER_ID"),
        F.when(F.col("order_date").isNull(), "INVALID_ORDER_DATE"),
        F.when(F.col("ship_date").isNull(), "INVALID_SHIP_DATE"),
        F.when(
            F.col("order_date").isNotNull()
            & F.col("ship_date").isNotNull()
            & (F.col("ship_date") < F.col("order_date")),
            "SHIP_DATE_BEFORE_ORDER_DATE",
        ),
        F.when(F.col("ship_mode").isNull() | (F.col("ship_mode") == ""), "MISSING_SHIP_MODE"),
        F.when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "MISSING_CUSTOMER_ID"),
        F.when(F.col("product_id").isNull() | (F.col("product_id") == ""), "MISSING_PRODUCT_ID"),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "INVALID_QUANTITY"),
        F.when(F.col("price").isNull() | (F.col("price") < 0), "INVALID_PRICE"),
        F.when(
            F.col("discount").isNull()
            | (F.col("discount") < 0)
            | (F.col("discount") > 1),
            "INVALID_DISCOUNT",
        ),
        F.when(F.col("profit").isNull(), "MISSING_PROFIT"),
    )

    return (
        with_counts.withColumn("order_year", F.year("order_date"))
        .withColumn("dq_error_reason", errors)
        .withColumn("is_valid", F.col("dq_error_reason") == "")
        .drop("_row_id_count")
    )


def split_valid_invalid_orders(cleaned_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = cleaned_df.filter(F.col("is_valid")).drop("is_valid", "dq_error_reason")
    invalid = cleaned_df.filter(~F.col("is_valid")).drop("is_valid")
    return valid, invalid


def enrich_orders(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
    use_broadcast: bool = True,
) -> DataFrame:
    """Enrich order lines while retaining unmatched business transactions."""
    customer_lookup = customers_df.select(
        "customer_id", "customer_name", F.col("country").alias("customer_country")
    )
    product_lookup = products_df.select(
        "product_id", "category", "sub_category"
    )

    if use_broadcast:
        customer_lookup = F.broadcast(customer_lookup)
        product_lookup = F.broadcast(product_lookup)

    joined = (
        orders_df.alias("o")
        .join(customer_lookup.alias("c"), on="customer_id", how="left")
        .join(product_lookup.alias("p"), on="product_id", how="left")
    )

    return joined.select(
        "row_id",
        "order_id",
        "order_date",
        "ship_date",
        "order_year",
        "ship_mode",
        "customer_id",
        F.coalesce(F.col("customer_name"), F.lit("Unknown")).alias("customer_name"),
        F.coalesce(F.col("customer_country"), F.lit("Unknown")).alias(
            "customer_country"
        ),
        "product_id",
        F.coalesce(F.col("category"), F.lit("Unknown")).alias(
            "product_category"
        ),
        F.coalesce(F.col("sub_category"), F.lit("Unknown")).alias(
            "product_sub_category"
        ),
        "quantity",
        "price",
        "discount",
        F.round(F.col("profit"), 2).cast(MONEY_TYPE).alias("profit"),
        F.when(F.col("customer_name").isNull(), "MISSING")
        .otherwise("MATCHED")
        .alias("customer_lookup_status"),
        F.when(F.col("category").isNull(), "MISSING")
        .otherwise("MATCHED")
        .alias("product_lookup_status"),
    )


def aggregate_profit(enriched_df: DataFrame) -> DataFrame:
    """Aggregate profit at the exact grain requested by the assignment."""
    grouping_columns = [
        "order_year",
        "product_category",
        "product_sub_category",
        "customer_id",
        "customer_name",
    ]
    return (
        enriched_df.groupBy(*grouping_columns)
        .agg(
            F.round(F.sum("profit"), 2).cast(MONEY_TYPE).alias("total_profit"),
            F.count("*").alias("order_line_count"),
        )
        .select(*grouping_columns, "total_profit", "order_line_count")
    )
