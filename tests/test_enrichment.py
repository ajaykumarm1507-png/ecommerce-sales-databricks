from decimal import Decimal

import pytest

from ecommerce_pipeline.transformations import (
    clean_customers,
    clean_orders,
    clean_products,
    enrich_orders,
    split_valid_invalid_orders,
)


def _valid_orders(raw):
    valid, _ = split_valid_invalid_orders(clean_orders(raw))
    return valid


@pytest.mark.integration
def test_order_is_enriched_with_customer_and_product(
    make_order_df, make_customer_df, make_product_df
):
    result = enrich_orders(
        _valid_orders(make_order_df()),
        clean_customers(make_customer_df()),
        clean_products(make_product_df()),
        use_broadcast=False,
    ).first()
    assert result.customer_name == "Alice Brown"
    assert result.customer_country == "United States"
    assert result.product_category == "Furniture"
    assert result.product_sub_category == "Chairs"
    assert result.customer_lookup_status == "MATCHED"
    assert result.product_lookup_status == "MATCHED"


@pytest.mark.data_quality
def test_missing_product_does_not_remove_order(
    make_order_df, make_customer_df, make_product_df
):
    result = enrich_orders(
        _valid_orders(make_order_df(**{"Product ID": "P-MISSING"})),
        clean_customers(make_customer_df()),
        clean_products(make_product_df()),
        use_broadcast=False,
    ).first()
    assert result.product_category == "Unknown"
    assert result.product_sub_category == "Unknown"
    assert result.product_lookup_status == "MISSING"


@pytest.mark.data_quality
def test_missing_customer_does_not_remove_order(
    make_order_df, make_customer_df, make_product_df
):
    result = enrich_orders(
        _valid_orders(make_order_df(**{"Customer ID": "C-MISSING"})),
        clean_customers(make_customer_df()),
        clean_products(make_product_df()),
        use_broadcast=False,
    ).first()
    assert result.customer_name == "Unknown"
    assert result.customer_country == "Unknown"
    assert result.customer_lookup_status == "MISSING"


@pytest.mark.integration
def test_order_count_is_preserved(make_order_df, make_customer_df, make_product_df):
    orders = _valid_orders(
        make_order_df(
            {"Row ID": 1, "Order ID": "ORDER-1"},
            {"Row ID": 2, "Order ID": "ORDER-2"},
        )
    )
    enriched = enrich_orders(
        orders,
        clean_customers(make_customer_df()),
        clean_products(make_product_df()),
        use_broadcast=False,
    )
    assert enriched.count() == orders.count()


@pytest.mark.integration
def test_duplicate_product_source_does_not_multiply_orders(
    make_order_df, make_customer_df, make_product_df
):
    products = clean_products(
        make_product_df(
            {"Product ID": "P-001", "Product Name": "Chair A"},
            {"Product ID": "P-001", "Product Name": "Chair B"},
        )
    )
    orders = _valid_orders(make_order_df())
    result = enrich_orders(
        orders,
        clean_customers(make_customer_df()),
        products,
        use_broadcast=False,
    )
    assert result.count() == 1
    assert result.select("row_id").distinct().count() == 1


@pytest.mark.unit
def test_profit_remains_decimal_with_two_places(
    make_order_df, make_customer_df, make_product_df
):
    result = enrich_orders(
        _valid_orders(make_order_df(**{"Profit": "10.235"})),
        clean_customers(make_customer_df()),
        clean_products(make_product_df()),
        use_broadcast=False,
    ).first()
    assert result.profit == Decimal("10.24")


@pytest.mark.unit
def test_negative_profit_remains_unchanged(
    make_order_df, make_customer_df, make_product_df
):
    result = enrich_orders(
        _valid_orders(make_order_df(**{"Profit": "-14.92"})),
        clean_customers(make_customer_df()),
        clean_products(make_product_df()),
        use_broadcast=False,
    ).first()
    assert result.profit == Decimal("-14.92")


@pytest.mark.integration
def test_broadcast_path_returns_same_business_result(
    make_order_df, make_customer_df, make_product_df
):
    orders = _valid_orders(make_order_df())
    customers = clean_customers(make_customer_df())
    products = clean_products(make_product_df())
    regular = enrich_orders(orders, customers, products, use_broadcast=False).first()
    broadcast = enrich_orders(orders, customers, products, use_broadcast=True).first()
    assert regular.asDict() == broadcast.asDict()
