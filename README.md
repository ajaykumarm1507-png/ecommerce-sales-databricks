# E-commerce Sales Pipeline

This project processes the three files provided for the Lead Data Engineer task:

- `Customer.xlsx`
- `Products.csv`
- `Orders.json`

The data processing is written in PySpark and runs from Databricks notebooks. SQL is used only for the final reporting queries.

## What the pipeline does

1. Loads each source file into a raw Delta table.
2. Cleans the customer and product data.
3. Creates an enriched order table with customer and product details.
4. Creates a profit aggregate by year, category, sub-category and customer.
5. Runs the four requested SQL queries.

The SQL outputs are:

- Profit by year
- Profit by year and product category
- Profit by customer
- Profit by customer and year

## Data flow

```text
Customer.xlsx ─┐
Products.csv ──┼── Raw tables
Orders.json ───┘       |
                       v
                 Cleaned tables
              customers_enriched
              products_enriched
              orders_clean
                       |
                       v
                  Gold tables
                order_enriched
                profit_aggregate
```

## Table grain

| Table | One row represents |
|---|---|
| `customers_enriched` | One customer |
| `products_enriched` | One product |
| `orders_clean` | One order line |
| `order_enriched` | One enriched order line |
| `profit_aggregate` | One year, category, sub-category and customer combination |

`Row ID` is used as the order-line key. One `Order ID` can have more than one product, so it is not unique.

## Data issues handled

- Some product IDs occur more than once. Products are reduced to one row per ID before joining to orders.
- Some orders have product IDs that are not in the product file. These orders are kept and the product fields are set to `Unknown`.
- Invalid order rows are written to `orders_quarantine` with the reason.
- Customer names, phone numbers and postal codes are cleaned where needed.
- Profit uses a decimal type and is rounded to two decimal places.
- Negative profit is kept because it represents a loss.

More details are in [docs/data_design.md](docs/data_design.md).

## Project structure

```text
notebooks/                  Databricks notebooks
src/ecommerce_pipeline/     PySpark transformations and checks
tests/                      pytest test cases
sql/                        SQL output queries
docs/                       Data design and test notes
.github/workflows/          Test workflow
```

## Run in Databricks

1. Upload the three source files to a Unity Catalog Volume.
2. Install a `spark-excel` library that matches the Databricks Runtime.
3. Add this repository to Databricks Repos.
4. Install the project from the repository root:

   ```python
   %pip install -e .
   ```

5. Run the notebooks in this order:

   ```text
   00_setup
   01_raw_ingestion
   02_build_silver
   03_build_gold
   04_sql_outputs
   ```

The default source path is `/Volumes/main/ecommerce_landing/source`. It can be changed through the notebook widget.

## Run tests

```bash
pip install -e ".[dev]"
pytest -q
```

Run a specific test group:

```bash
pytest -q -m unit
pytest -q -m data_quality
pytest -q -m integration
```

The tests create small Spark DataFrames in memory. This keeps them fast and makes each test independent of Databricks and the source file location.

The test suite covers:

- Required columns and schema checks
- Customer and product cleaning
- Valid and invalid order records
- Duplicate keys
- Date, quantity, price and discount rules
- Missing customer and product lookups
- Profit rounding and negative profit
- Row counts after joins
- Profit totals after aggregation

See [docs/test_strategy.md](docs/test_strategy.md) for the test split.

## Results for the provided files

| Check | Result |
|---|---:|
| Customers | 793 |
| Product rows | 1,851 |
| Unique products | 1,818 |
| Order lines | 9,994 |
| Aggregate rows | 8,129 |
| Total profit | 278,417.03 |

The source files are not committed to GitHub. They should be downloaded from the Drive link in the task document.

## Assumptions

- The three input files are full extracts, so the tables use overwrite mode.
- Customer ID and Product ID are trimmed and converted to upper case before joining.
- Customer and product joins are left joins so valid orders are not dropped.
- The supplied data is small, so the output is not partitioned.
