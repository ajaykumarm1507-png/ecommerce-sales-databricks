# Databricks notebook source
# MAGIC %md
# MAGIC # Transform, enrich and aggregate
# MAGIC Cleans the raw data, creates enriched tables, and builds the requested profit aggregate.

# COMMAND ----------

from pyspark.sql import functions as F

from ecommerce_pipeline.io import overwrite_delta_table
from ecommerce_pipeline.transformations import (
    aggregate_profit,
    clean_customers,
    clean_orders,
    clean_products,
    enrich_orders,
    split_valid_invalid_orders,
)
from ecommerce_pipeline.validation import (
    validate_profit_reconciliation,
    validate_row_count,
    validate_unique_key,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
catalog = dbutils.widgets.get("catalog")
raw_schema = f"{catalog}.ecommerce_raw"
sales_schema = f"{catalog}.ecommerce_sales"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {sales_schema}")

customers_raw = spark.table(f"{raw_schema}.customers_raw")
products_raw = spark.table(f"{raw_schema}.products_raw")
orders_raw = spark.table(f"{raw_schema}.orders_raw")

# COMMAND ----------

customers_enriched = clean_customers(customers_raw)
products_enriched = clean_products(products_raw)
orders_checked = clean_orders(orders_raw)
orders_clean, orders_quarantine = split_valid_invalid_orders(orders_checked)
orders_quarantine = orders_quarantine.withColumn("rejected_timestamp", F.current_timestamp())

validate_unique_key(customers_enriched, "customer_id")
validate_unique_key(products_enriched, "product_id")

order_enriched = enrich_orders(
    orders_clean, customers_enriched, products_enriched, use_broadcast=True
)
validate_row_count(orders_clean, order_enriched)

profit_aggregate = aggregate_profit(order_enriched)
validate_profit_reconciliation(order_enriched, profit_aggregate)

# COMMAND ----------

overwrite_delta_table(customers_enriched, f"{sales_schema}.customers_enriched")
overwrite_delta_table(products_enriched, f"{sales_schema}.products_enriched")
overwrite_delta_table(orders_clean, f"{sales_schema}.orders_clean")
overwrite_delta_table(orders_quarantine, f"{sales_schema}.orders_quarantine")
overwrite_delta_table(order_enriched, f"{sales_schema}.order_enriched")
overwrite_delta_table(profit_aggregate, f"{sales_schema}.profit_aggregate")

print(
    {
        "customers_enriched": customers_enriched.count(),
        "products_enriched": products_enriched.count(),
        "orders_clean": orders_clean.count(),
        "orders_quarantine": orders_quarantine.count(),
        "order_enriched": order_enriched.count(),
        "profit_aggregate": profit_aggregate.count(),
    }
)
