# Databricks notebook source
# MAGIC %md
# MAGIC # Run PyTest suite
# MAGIC Run this notebook from a Databricks Repo checkout after installing the development dependencies.

# COMMAND ----------

# MAGIC %pip install -e ".[dev]"

# COMMAND ----------

# MAGIC %sh
# MAGIC pytest -q --cov=ecommerce_pipeline --cov-report=term-missing
