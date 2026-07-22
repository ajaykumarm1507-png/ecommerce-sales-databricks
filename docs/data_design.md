# Data design

The design keeps the flow simple: raw source tables first, then one sales schema for transformation, enrichment and aggregation.

## Raw Tables

Raw tables keep the source columns as they arrive. Three metadata columns are added:

- `_source_file`
- `_ingestion_timestamp`
- `_pipeline_run_id`

Tables in `ecommerce_raw`:

- `customers_raw`
- `products_raw`
- `orders_raw`

## Transformation And Enrichment Tables

Transformation and enrichment are handled together in the same notebook and schema.

Tables in `ecommerce_sales`:

- `customers_enriched`
- `products_enriched`
- `orders_clean`
- `orders_quarantine`
- `order_enriched`
- `profit_aggregate`

`customers_enriched` contains one row per customer. It cleans names, email, phone and postal code values and keeps simple quality flags.

`products_enriched` contains one row per product. Duplicate product IDs are combined before the order join.

`orders_clean` contains valid, typed order lines. Invalid rows go to `orders_quarantine` with `dq_error_reason`.

`order_enriched` contains order information, profit rounded to two decimal places, customer name and country, and product category and sub-category.

`profit_aggregate` is grouped by:

```text
order_year
product_category
product_sub_category
customer_id
customer_name
```

## Join Choices

Customer and product tables are small, so they are broadcast before the joins.

Both joins are left joins. An order is kept even when its customer or product lookup is missing. Missing text values are set to `Unknown`, and a lookup status is added.

Customer and product keys are checked for duplicates before the join. This avoids increasing the number of order rows by mistake.

## Write Choice

The input files are full extracts and are small enough for this task. Delta tables are written in overwrite mode so a rerun gives the same result.

The order file contains 9,994 rows. Partitioning data of this size would create many small files, so no partition column is used.
