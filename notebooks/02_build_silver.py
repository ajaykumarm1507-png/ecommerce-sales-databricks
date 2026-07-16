# Databricks notebook source
# MAGIC %md
# MAGIC # Silver tables
# MAGIC Cleans customers and products, validates orders, and writes rejected order rows separately.

# COMMAND ----------

from pyspark.sql import functions as F

from ecommerce_pipeline.io import overwrite_delta_table
from ecommerce_pipeline.transformations import (
    clean_customers,
    clean_orders,
    clean_products,
    split_valid_invalid_orders,
)
from ecommerce_pipeline.validation import validate_unique_key

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
catalog = dbutils.widgets.get("catalog")
bronze_schema = f"{catalog}.ecommerce_bronze"
silver_schema = f"{catalog}.ecommerce_silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")

customers_raw = spark.table(f"{bronze_schema}.customers_raw")
products_raw = spark.table(f"{bronze_schema}.products_raw")
orders_raw = spark.table(f"{bronze_schema}.orders_raw")

# COMMAND ----------

customers_enriched = clean_customers(customers_raw)
products_enriched = clean_products(products_raw)
orders_checked = clean_orders(orders_raw)
orders_clean, orders_quarantine = split_valid_invalid_orders(orders_checked)
orders_quarantine = orders_quarantine.withColumn("rejected_timestamp", F.current_timestamp())

# Fail before the Gold join if a dimension key is not unique.
validate_unique_key(customers_enriched, "customer_id")
validate_unique_key(products_enriched, "product_id")

# COMMAND ----------

overwrite_delta_table(customers_enriched, f"{silver_schema}.customers_enriched")
overwrite_delta_table(products_enriched, f"{silver_schema}.products_enriched")
overwrite_delta_table(orders_clean, f"{silver_schema}.orders_clean")
overwrite_delta_table(orders_quarantine, f"{silver_schema}.orders_quarantine")

print(
    {
        "customers_enriched": customers_enriched.count(),
        "products_enriched": products_enriched.count(),
        "orders_clean": orders_clean.count(),
        "orders_quarantine": orders_quarantine.count(),
    }
)
