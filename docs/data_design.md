# Data design

## Layers

### Bronze

Bronze tables keep the source column names and values and add:

- `_source_file`
- `_ingestion_timestamp`
- `_pipeline_run_id`

The Bronze layer is intentionally light. Business cleaning is performed in Silver.

### Silver

`customers_enriched` cleans customer details and provides quality flags.

`products_enriched` consolidates duplicate Product IDs before they are used in a join. The chosen product name is deterministic, and conflict counts remain available for review.

`orders_clean` contains valid, typed order lines. `orders_quarantine` contains invalid order lines with `dq_error_reason` and a rejection timestamp.

### Gold

`order_enriched` is the master order-line table. It contains order information, two-decimal profit, customer name and country, and product category and sub-category.

`profit_aggregate` contains the requested aggregation grain:

```text
order_year
product_category
product_sub_category
customer_id
customer_name
```

## Join design

The customer and product tables are dimension-sized and are broadcast in the Gold transformation. Only required columns are selected before joining.

Both joins are left joins. Missing dimension data is surfaced through lookup-status columns rather than causing an otherwise valid order line to disappear.

## Write strategy

The assignment files are full snapshots. Tables are therefore written using Delta overwrite with `overwriteSchema=true`. This makes reruns idempotent for the supplied use case.

A production incremental extension would use:

- Auto Loader for new files
- Delta `MERGE` by `customer_id`, `product_id`, and `row_id`
- a run-control table for watermarks and processing status
- scheduled compaction based on actual file size and query patterns

## Performance

The supplied data is small and is not partitioned. Partitioning 9,994 order rows would create unnecessary small files. At larger scale, the order tables could use liquid clustering on `order_date`, `customer_id`, or another frequently filtered column after observing the real workload.
