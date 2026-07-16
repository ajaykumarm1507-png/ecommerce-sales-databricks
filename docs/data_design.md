# Data design

The pipeline uses three layers: raw, cleaned and gold.

## Raw layer

The raw tables keep the source columns as they are. Three metadata columns are added:

- `_source_file`
- `_ingestion_timestamp`
- `_pipeline_run_id`

Tables:

- `customers_raw`
- `products_raw`
- `orders_raw`

## Cleaned layer

`customers_enriched` contains one row per customer. It cleans names, email, phone and postal code values and keeps simple quality flags.

`products_enriched` contains one row per product. The product file has duplicate product IDs, so they are combined before the order join. Conflict flags show when duplicate rows have different values.

`orders_clean` contains valid, typed order lines. Invalid rows go to `orders_quarantine` with `dq_error_reason`.

## Gold layer

`order_enriched` is the main order-line table. It includes:

- Order details
- Profit rounded to two decimal places
- Customer name and country
- Product category and sub-category
- Customer and product lookup status

`profit_aggregate` is grouped by:

```text
order_year
product_category
product_sub_category
customer_id
customer_name
```

## Join choices

Customer and product tables are small, so they are broadcast before the joins.

Both joins are left joins. An order is kept even when its customer or product lookup is missing. Missing text values are set to `Unknown`, and a lookup status is added.

Customer and product keys are checked for duplicates before the join. This avoids increasing the number of order rows by mistake.

## Write choice

The input files are full extracts and are small enough for this task. Delta tables are written in overwrite mode so a rerun gives the same result.

For a regular production feed, I would change raw ingestion to Auto Loader and use Delta `MERGE` for changed records.

## Partitioning

The order file contains 9,994 rows. Partitioning data of this size would create many small files, so no partition column is used.
