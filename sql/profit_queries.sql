-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Required SQL outputs
-- MAGIC The transformation pipeline is PySpark. SQL is used only for the requested reporting outputs.

-- COMMAND ----------

-- Profit by Year
SELECT
    order_year,
    ROUND(SUM(total_profit), 2) AS total_profit
FROM main.ecommerce_sales.profit_aggregate
GROUP BY order_year
ORDER BY order_year;

-- COMMAND ----------

-- Profit by Year + Product Category
SELECT
    order_year,
    product_category,
    ROUND(SUM(total_profit), 2) AS total_profit
FROM main.ecommerce_sales.profit_aggregate
GROUP BY order_year, product_category
ORDER BY order_year, product_category;

-- COMMAND ----------

-- Profit by Customer
SELECT
    customer_id,
    customer_name,
    ROUND(SUM(total_profit), 2) AS total_profit
FROM main.ecommerce_sales.profit_aggregate
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC;

-- COMMAND ----------

-- Profit by Customer + Year
SELECT
    customer_id,
    customer_name,
    order_year,
    ROUND(SUM(total_profit), 2) AS total_profit
FROM main.ecommerce_sales.profit_aggregate
GROUP BY customer_id, customer_name, order_year
ORDER BY customer_id, order_year;
