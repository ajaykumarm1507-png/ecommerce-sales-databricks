"""Small I/O helpers used by Databricks notebooks."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def create_database_if_needed(spark: SparkSession, database_name: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")


def overwrite_delta_table(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
