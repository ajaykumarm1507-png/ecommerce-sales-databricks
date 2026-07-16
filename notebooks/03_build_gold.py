# Databricks notebook source
# MAGIC %md
# MAGIC # Gold tables
# MAGIC Creates the enriched order-line table and the requested profit aggregate.

# COMMAND ----------

from ecommerce_pipeline.io import overwrite_delta_table
from ecommerce_pipeline.transformations import aggregate_profit, enrich_orders
from ecommerce_pipeline.validation import (
    validate_profit_reconciliation,
    validate_row_count,
)

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
catalog = dbutils.widgets.get("catalog")
silver_schema = f"{catalog}.ecommerce_silver"
gold_schema = f"{catalog}.ecommerce_gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")

orders = spark.table(f"{silver_schema}.orders_clean")
customers = spark.table(f"{silver_schema}.customers_enriched")
products = spark.table(f"{silver_schema}.products_enriched")

# COMMAND ----------

order_enriched = enrich_orders(orders, customers, products, use_broadcast=True)
validate_row_count(orders, order_enriched)

profit_aggregate = aggregate_profit(order_enriched)
validate_profit_reconciliation(order_enriched, profit_aggregate)

# COMMAND ----------

overwrite_delta_table(order_enriched, f"{gold_schema}.order_enriched")
overwrite_delta_table(profit_aggregate, f"{gold_schema}.profit_aggregate")

print(
    {
        "order_enriched": order_enriched.count(),
        "profit_aggregate": profit_aggregate.count(),
    }
)
