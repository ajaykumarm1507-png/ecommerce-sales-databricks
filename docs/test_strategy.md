# PyTest strategy

The task places strong emphasis on test-driven development, so the tests are separated by business area and marked by purpose.

## Test categories

### Unit

Tests one transformation rule at a time using small in-memory Spark DataFrames. Examples include date conversion, profit rounding, postal-code normalization, product consolidation, and aggregate calculations.

### Data quality

Tests invalid and edge-case records. Examples include missing IDs, duplicate Row IDs, invalid dates, non-positive quantity, negative price, invalid discount, missing lookups, and corrupted customer attributes.

### Integration

Tests interactions across transformations. Examples include product consolidation followed by order enrichment, preservation of order count through joins, and reconciliation between detail and aggregate profit.

## Coverage by area

| Area | Main scenarios |
|---|---|
| Structural validation | Missing columns, null keys, duplicate keys |
| Customers | Missing name, corrupted name, invalid phone, email flag, postal code, duplicate customer |
| Products | Duplicate IDs, deterministic name, conflict flags, price range, missing key |
| Orders | Date parsing, profit rounding, negative profit, invalid quantity/price/discount, duplicate Row ID, quarantine split |
| Enrichment | Matched values, missing customer/product, left-join behavior, row-count preservation, no join multiplication |
| Aggregation | Exact sums, losses, year grain, same name/different IDs, decimal precision, detail-to-aggregate reconciliation |

## Why tests do not read the source files

Unit tests should fail because business logic is wrong, not because a file path or Excel connector is unavailable. Each test creates only the minimum DataFrame needed for the scenario. File-reading and Delta-writing behavior remains in the Databricks notebooks.

## Commands

```bash
pytest -q
pytest -q -m data_quality
pytest -q --cov=ecommerce_pipeline --cov-report=term-missing
```
