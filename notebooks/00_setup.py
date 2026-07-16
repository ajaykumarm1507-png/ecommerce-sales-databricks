# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce pipeline setup
# MAGIC Creates the schemas used by the Bronze, Silver and Gold layers.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
dbutils.widgets.text("source_path", "/Volumes/main/ecommerce_landing/source", "Source folder")

catalog = dbutils.widgets.get("catalog")
source_path = dbutils.widgets.get("source_path").rstrip("/")

bronze_schema = f"{catalog}.ecommerce_bronze"
silver_schema = f"{catalog}.ecommerce_silver"
gold_schema = f"{catalog}.ecommerce_gold"

for schema_name in [bronze_schema, silver_schema, gold_schema]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

print(f"Source path: {source_path}")
print(f"Bronze: {bronze_schema}")
print(f"Silver: {silver_schema}")
print(f"Gold: {gold_schema}")
