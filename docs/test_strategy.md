# Test approach

pytest is used for the transformation and validation logic. Each test creates a small Spark DataFrame with only the rows needed for that case.

## Test groups

### Unit tests

These test one rule at a time, such as:

- Date conversion
- Customer name cleaning
- Postal code formatting
- Product duplicate handling
- Profit rounding
- Aggregate totals

### Data quality tests

These cover bad or unexpected records:

- Missing required columns
- Null and duplicate keys
- Invalid dates
- Zero or negative quantity
- Negative price
- Discount outside the range 0 to 1
- Missing customer or product lookup
- Corrupted customer values

Negative profit is tested as a valid value because it represents a loss.

### Integration tests

These check more than one step together:

- Product cleanup followed by order enrichment
- Order count before and after joins
- Duplicate products do not duplicate order rows
- Detail profit matches aggregate profit
- Broadcast and normal joins return the same result

## Test files

| File | Area |
|---|---|
| `test_customers.py` | Customer cleaning |
| `test_products.py` | Product cleaning and duplicates |
| `test_orders.py` | Order rules and quarantine |
| `test_enrichment.py` | Customer and product joins |
| `test_aggregations.py` | Profit aggregation |
| `test_validation.py` | Schema, key and total checks |

## Commands

```bash
pytest -q
pytest -q -m unit
pytest -q -m data_quality
pytest -q -m integration
pytest -q --cov=ecommerce_pipeline --cov-report=term-missing
```

File ingestion and Delta writes stay in the Databricks notebooks. The unit tests focus on the PySpark business logic, so they do not depend on a Drive path or a running Databricks workspace.
