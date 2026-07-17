# E-commerce Sales Pipeline

This project is for the Lead Data Engineer assignment. It uses the three files shared in the Drive link:

- `Customer.xlsx`
- `Products.csv`
- `Orders.json`

All transformation logic is written in PySpark. SQL is used only for the final output queries requested in the task.

## Scope

The pipeline covers these requirements:

| Requirement | Output |
|---|---|
| Raw ingestion | `customers_raw`, `products_raw`, `orders_raw` |
| Customer enrichment | `customers_enriched` |
| Product enrichment | `products_enriched` |
| Order enrichment | `order_enriched` |
| Profit aggregate | `profit_aggregate` |
| SQL outputs | Queries in `sql/profit_queries.sql` |

## Data Flow

```text
Source files
    |
    v
Raw tables
    |
    v
Cleaned customer, product and order tables
    |
    v
Enriched order table
    |
    v
Profit aggregate table
```

## Main Tables

| Table | Grain |
|---|---|
| `customers_raw` | One row from `Customer.xlsx` |
| `products_raw` | One row from `Products.csv` |
| `orders_raw` | One row from `Orders.json` |
| `customers_enriched` | One customer |
| `products_enriched` | One product |
| `order_enriched` | One order line with customer and product details |
| `profit_aggregate` | Year, product category, product sub-category and customer |

`Row ID` is used as the order-line key because one `Order ID` can contain multiple products.

## Enriched Order Table

`order_enriched` includes:

- Order ID, row ID, order date, ship date and ship mode
- Profit rounded to two decimal places
- Customer name and country
- Product category and sub-category
- Lookup status for customer and product joins

## SQL Outputs

The SQL file contains only the requested reporting queries:

- Profit by year
- Profit by year and product category
- Profit by customer
- Profit by customer and year

## Data Checks

The pipeline handles common data issues found in the files:

- Required source columns are checked before processing.
- Customer and product keys are standardized before joins.
- Duplicate product IDs are reduced to one product record before joining with orders.
- Invalid order rows are separated into `orders_quarantine` with an error reason.
- Orders are kept even when the product lookup is missing.
- Profit is stored as a decimal value and rounded to two decimal places.
- Negative profit is kept because it represents a loss.

## Tests

pytest test cases are included for the PySpark logic. The tests use small in-memory Spark DataFrames and cover:

- Schema and required column checks
- Customer and product cleaning
- Duplicate key handling
- Order date, quantity, price and discount rules
- Profit rounding
- Missing customer and product lookups
- Enriched order row-count checks
- Aggregate profit totals
- Positive and negative scenarios

Test files are under `tests/`.

## Project Structure

```text
notebooks/                  Databricks notebooks for the pipeline steps
src/ecommerce_pipeline/     PySpark transformation and validation code
tests/                      pytest test cases
sql/                        final SQL output queries
docs/                       short design and test notes
```

## Assumptions

- The input files are treated as full extracts.
- Raw tables keep the original source columns.
- Customer ID and Product ID are trimmed and converted to upper case before joining.
- Customer and product joins are left joins so valid orders are not dropped.
- The data volume is small, so no partitioning is added for this assignment.
