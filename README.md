# E-commerce Sales Processing – Databricks and PySpark

This repository contains the solution for the Lead Data Engineer take-home task. The pipeline loads the three supplied source files, creates cleaned customer and product dimensions, builds an enriched order-line table, and produces the requested profit aggregates.

The implementation is deliberately small and testable. Databricks notebooks orchestrate the work, while the transformation logic is kept in normal Python modules so it can be tested with PyTest without writing data to Delta tables.

## Data flow

```text
Customer.xlsx ─┐
Products.csv ──┼── Bronze raw tables
Orders.json ───┘          │
                          ▼
                    Silver tables
             customers_enriched
             products_enriched
             orders_clean / orders_quarantine
                          │
                          ▼
                     Gold tables
             order_enriched
             profit_aggregate
```

## Table grain

| Table | Grain |
|---|---|
| `customers_enriched` | One row per `customer_id` |
| `products_enriched` | One row per `product_id` |
| `orders_clean` | One row per source order line (`row_id`) |
| `order_enriched` | One enriched source order line (`row_id`) |
| `profit_aggregate` | Year + category + sub-category + customer ID + customer name |

`Order ID` is not used as the order-line key because a single order can contain multiple product lines. `Row ID` is unique in the supplied order file.

## Important data decisions

### Product duplicates

The product file contains 1,851 rows but only 1,818 unique Product IDs. A direct join would duplicate some order lines. The product transformation first creates one deterministic record per Product ID and retains conflict indicators such as `product_name_conflict_flag`.

### Missing product lookups

Orders reference 44 Product IDs that are not present in the product file. These affect 204 order lines. The pipeline uses a left join so the sales records are not lost. Missing category and sub-category values are written as `Unknown`, with `product_lookup_status = 'MISSING'`.

### Customer quality issues

The customer file contains missing names, digits and unwanted characters in names, `#ERROR!` phone values, and postal codes where leading zeroes were lost. The cleaning rules keep the original name, standardize the usable value, and add quality flags instead of silently hiding the issue.

### Money and profit

Price and profit use decimal types. Negative profit is valid because it represents a loss. It is not rejected or replaced with zero. Profit is rounded to two decimal places in the clean order transformation and again at the aggregate boundary.

## Project structure

```text
src/ecommerce_pipeline/     reusable PySpark transformations and validations
notebooks/                  Databricks orchestration notebooks
sql/                        requested SQL outputs
tests/                      PyTest unit, data-quality and integration tests
docs/                       design notes, source profile and test strategy
.github/workflows/          GitHub Actions test workflow
```

## Databricks execution

1. Upload the three source files to a Unity Catalog Volume, for example:

   ```text
   /Volumes/main/ecommerce_landing/source/
   ```

2. Install a `spark-excel` Maven library that matches the Databricks Runtime. The ingestion notebook uses the `com.crealytics.spark.excel` format because the customer source is an `.xlsx` file.

3. Add this repository as a Databricks Repo or install the package from the repository root:

   ```python
   %pip install -e .
   ```

4. Run the notebooks in order:

   ```text
   00_setup
   01_raw_ingestion
   02_build_silver
   03_build_gold
   04_sql_outputs
   ```

The default catalog is `main`. Change the notebook widget when another catalog is required.

## Running tests

```bash
pip install -e ".[dev]"
pytest -q
```

Useful subsets:

```bash
pytest -q -m unit
pytest -q -m data_quality
pytest -q -m integration
pytest -q --cov=ecommerce_pipeline --cov-report=term-missing
```

The tests use small in-memory Spark DataFrames. They do not depend on the source files, Databricks paths, or Delta tables, which keeps failures focused on the transformation being tested.

## Main validations

- Mandatory source columns are present.
- Customer and product dimension keys are unique before the Gold join.
- Invalid order rows are sent to `orders_quarantine` with a clear error reason.
- Duplicate `row_id` values are rejected.
- Ship date cannot be before order date.
- Quantity must be greater than zero.
- Price cannot be null or negative.
- Discount must be between zero and one.
- Negative profit is accepted.
- The enriched order count must match the valid order count.
- Duplicate product source records must not multiply order rows.
- Total profit in the aggregate must reconcile to total profit in the enriched table.

## Supplied-data reconciliation values

These values are useful when checking a full Databricks run:

| Check | Expected |
|---|---:|
| Customer source rows | 793 |
| Product source rows | 1,851 |
| Consolidated products | 1,818 |
| Order source rows | 9,994 |
| Enriched order rows | 9,994, provided no source order is rejected |
| Aggregate rows | 8,129 |
| Total profit | 278,417.03 |

Profit by year:

| Year | Profit |
|---:|---:|
| 2014 | 39,185.75 |
| 2015 | 63,073.09 |
| 2016 | 65,073.23 |
| 2017 | 111,084.96 |

## Assumptions

- The supplied files are full snapshots, so the take-home implementation uses deterministic overwrite writes.
- `Row ID` is the order-line business key.
- Customer ID and Product ID comparisons are trimmed and converted to upper case.
- The supplied US postal codes are represented as five-character strings after cleaning.
- Customer and product lookups use left joins to retain valid orders.
- In a production incremental design, Bronze ingestion would use Auto Loader and Silver/Gold tables would use Delta `MERGE` with the documented keys.
