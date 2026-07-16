from decimal import Decimal

import pytest

from ecommerce_pipeline.transformations import aggregate_profit
from ecommerce_pipeline.validation import validate_profit_reconciliation


@pytest.fixture
def profit_rows():
    return [
        {
            "row_id": 1,
            "order_year": 2016,
            "product_category": "Furniture",
            "product_sub_category": "Chairs",
            "customer_id": "C-001",
            "customer_name": "Customer A",
            "profit": "10.50",
        },
        {
            "row_id": 2,
            "order_year": 2016,
            "product_category": "Furniture",
            "product_sub_category": "Chairs",
            "customer_id": "C-001",
            "customer_name": "Customer A",
            "profit": "-2.25",
        },
        {
            "row_id": 3,
            "order_year": 2016,
            "product_category": "Technology",
            "product_sub_category": "Phones",
            "customer_id": "C-002",
            "customer_name": "Customer B",
            "profit": "5.00",
        },
    ]


@pytest.mark.unit
def test_profit_is_aggregated_at_requested_grain(make_enriched_df, profit_rows):
    result = aggregate_profit(make_enriched_df(profit_rows))
    furniture = result.filter("product_category = 'Furniture'").first()
    assert furniture.total_profit == Decimal("8.25")
    assert furniture.order_line_count == 2


@pytest.mark.unit
def test_negative_profit_reduces_total(make_enriched_df, profit_rows):
    result = aggregate_profit(make_enriched_df(profit_rows))
    assert result.filter("customer_id = 'C-001'").first().total_profit == Decimal(
        "8.25"
    )


@pytest.mark.unit
def test_different_categories_create_separate_rows(make_enriched_df, profit_rows):
    assert aggregate_profit(make_enriched_df(profit_rows)).count() == 2


@pytest.mark.data_quality
def test_same_customer_name_with_different_ids_is_not_combined(make_enriched_df):
    rows = [
        {
            "row_id": 1,
            "order_year": 2017,
            "product_category": "Technology",
            "product_sub_category": "Phones",
            "customer_id": "C-001",
            "customer_name": "Same Name",
            "profit": "1.00",
        },
        {
            "row_id": 2,
            "order_year": 2017,
            "product_category": "Technology",
            "product_sub_category": "Phones",
            "customer_id": "C-002",
            "customer_name": "Same Name",
            "profit": "2.00",
        },
    ]
    assert aggregate_profit(make_enriched_df(rows)).count() == 2


@pytest.mark.integration
def test_aggregate_profit_reconciles_to_detail(make_enriched_df, profit_rows):
    detail = make_enriched_df(profit_rows)
    aggregate = aggregate_profit(detail)
    validate_profit_reconciliation(detail, aggregate)


@pytest.mark.unit
def test_aggregation_is_deterministic(make_enriched_df, profit_rows):
    detail = make_enriched_df(profit_rows)
    first = sorted(aggregate_profit(detail).collect(), key=lambda r: tuple(r))
    second = sorted(aggregate_profit(detail).collect(), key=lambda r: tuple(r))
    assert first == second


@pytest.mark.unit
def test_year_is_part_of_aggregate_grain(make_enriched_df):
    base = {
        "product_category": "Furniture",
        "product_sub_category": "Chairs",
        "customer_id": "C-001",
        "customer_name": "Customer A",
        "profit": "4.00",
    }
    rows = [
        {**base, "row_id": 1, "order_year": 2016},
        {**base, "row_id": 2, "order_year": 2017},
    ]
    assert aggregate_profit(make_enriched_df(rows)).count() == 2


@pytest.mark.unit
def test_final_profit_uses_two_decimal_places(make_enriched_df):
    rows = [
        {
            "row_id": 1,
            "order_year": 2017,
            "product_category": "Furniture",
            "product_sub_category": "Chairs",
            "customer_id": "C-001",
            "customer_name": "Customer A",
            "profit": "1.10",
        },
        {
            "row_id": 2,
            "order_year": 2017,
            "product_category": "Furniture",
            "product_sub_category": "Chairs",
            "customer_id": "C-001",
            "customer_name": "Customer A",
            "profit": "2.20",
        },
    ]
    assert aggregate_profit(make_enriched_df(rows)).first().total_profit == Decimal(
        "3.30"
    )
