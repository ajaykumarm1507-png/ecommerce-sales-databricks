# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce pipeline setup
# MAGIC Creates the schemas used for raw and sales tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
dbutils.widgets.text("source_path", "/Volumes/main/ecommerce_landing/source", "Source folder")

catalog = dbutils.widgets.get("catalog")
source_path = dbutils.widgets.get("source_path").rstrip("/")

raw_schema = f"{catalog}.ecommerce_raw"
sales_schema = f"{catalog}.ecommerce_sales"

for schema_name in [raw_schema, sales_schema]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

print(f"Source path: {source_path}")
print(f"Raw: {raw_schema}")
print(f"Sales: {sales_schema}")
