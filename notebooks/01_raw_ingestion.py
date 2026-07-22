# Databricks notebook source
# MAGIC %md
# MAGIC # Raw ingestion
# MAGIC Reads the supplied Excel, CSV and JSON files using explicit schemas and adds ingestion metadata.

# COMMAND ----------

from uuid import uuid4
from pyspark.sql import functions as F

from ecommerce_pipeline.schemas import (
    CUSTOMER_RAW_SCHEMA,
    ORDER_RAW_SCHEMA,
    PRODUCT_RAW_SCHEMA,
)
from ecommerce_pipeline.io import overwrite_delta_table

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
dbutils.widgets.text("source_path", "/Volumes/main/ecommerce_landing/source", "Source folder")

catalog = dbutils.widgets.get("catalog")
source_path = dbutils.widgets.get("source_path").rstrip("/")
raw_schema = f"{catalog}.ecommerce_raw"
pipeline_run_id = str(uuid4())

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {raw_schema}")

# COMMAND ----------

# Install a spark-excel Maven library compatible with the Databricks Runtime on the cluster.
customers_raw = (
    spark.read.format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("dataAddress", "'Sheet1'!A1")
    .schema(CUSTOMER_RAW_SCHEMA)
    .load(f"{source_path}/Customer.xlsx")
)

products_raw = (
    spark.read.option("header", "true")
    .schema(PRODUCT_RAW_SCHEMA)
    .csv(f"{source_path}/Products.csv")
)

orders_raw = (
    spark.read.option("multiLine", "true")
    .schema(ORDER_RAW_SCHEMA)
    .json(f"{source_path}/Orders.json")
)

# COMMAND ----------

def add_ingestion_metadata(df, source_file):
    return (
        df.withColumn("_source_file", F.lit(source_file))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
    )

customers_raw = add_ingestion_metadata(customers_raw, "Customer.xlsx")
products_raw = add_ingestion_metadata(products_raw, "Products.csv")
orders_raw = add_ingestion_metadata(orders_raw, "Orders.json")

# COMMAND ----------

overwrite_delta_table(customers_raw, f"{raw_schema}.customers_raw")
overwrite_delta_table(products_raw, f"{raw_schema}.products_raw")
overwrite_delta_table(orders_raw, f"{raw_schema}.orders_raw")

print(
    {
        "customers_raw": customers_raw.count(),
        "products_raw": products_raw.count(),
        "orders_raw": orders_raw.count(),
        "pipeline_run_id": pipeline_run_id,
    }
)
