from decimal import Decimal

import pytest

from ecommerce_pipeline.transformations import clean_orders, split_valid_invalid_orders


@pytest.mark.unit
def test_valid_order_is_parsed_and_rounded(make_order_df):
    result = clean_orders(make_order_df()).first()
    assert str(result.order_date) == "2017-02-01"
    assert str(result.ship_date) == "2017-02-03"
    assert result.order_year == 2017
    assert result.profit == Decimal("10.24")
    assert result.is_valid is True


@pytest.mark.data_quality
def test_negative_profit_is_valid(make_order_df):
    result = clean_orders(make_order_df(**{"Profit": "-14.92"})).first()
    assert result.profit == Decimal("-14.92")
    assert result.is_valid is True


@pytest.mark.data_quality
@pytest.mark.parametrize(
    "changes, expected_error",
    [
        ({"Row ID": None}, "MISSING_ROW_ID"),
        ({"Order ID": None}, "MISSING_ORDER_ID"),
        ({"Order Date": "31/15/2017"}, "INVALID_ORDER_DATE"),
        ({"Ship Date": "31/15/2017"}, "INVALID_SHIP_DATE"),
        ({"Ship Date": "31/1/2017"}, "SHIP_DATE_BEFORE_ORDER_DATE"),
        ({"Ship Mode": None}, "MISSING_SHIP_MODE"),
        ({"Customer ID": None}, "MISSING_CUSTOMER_ID"),
        ({"Product ID": None}, "MISSING_PRODUCT_ID"),
        ({"Quantity": 0}, "INVALID_QUANTITY"),
        ({"Quantity": -1}, "INVALID_QUANTITY"),
        ({"Price": None}, "INVALID_PRICE"),
        ({"Price": "-0.01"}, "INVALID_PRICE"),
        ({"Discount": None}, "INVALID_DISCOUNT"),
        ({"Discount": "-0.01"}, "INVALID_DISCOUNT"),
        ({"Discount": "1.01"}, "INVALID_DISCOUNT"),
        ({"Profit": None}, "MISSING_PROFIT"),
    ],
)
def test_invalid_orders_receive_reason(make_order_df, changes, expected_error):
    result = clean_orders(make_order_df(**changes)).first()
    assert result.is_valid is False
    assert expected_error in result.dq_error_reason


@pytest.mark.data_quality
def test_duplicate_row_id_is_rejected(make_order_df):
    raw = make_order_df(
        {"Row ID": 1, "Order ID": "ORDER-1"},
        {"Row ID": 1, "Order ID": "ORDER-2"},
    )
    results = clean_orders(raw).collect()
    assert len(results) == 2
    assert all("DUPLICATE_ROW_ID" in row.dq_error_reason for row in results)


@pytest.mark.unit
def test_split_valid_and_invalid_orders(make_order_df):
    raw = make_order_df(
        {"Row ID": 1, "Order ID": "ORDER-1"},
        {"Row ID": 2, "Order ID": "ORDER-2", "Quantity": 0},
    )
    valid, invalid = split_valid_invalid_orders(clean_orders(raw))
    assert valid.count() == 1
    assert invalid.count() == 1
    assert invalid.first().dq_error_reason == "INVALID_QUANTITY"


@pytest.mark.unit
def test_identifiers_are_trimmed_and_uppercased(make_order_df):
    row = clean_orders(
        make_order_df(
            **{
                "Customer ID": " c-001 ",
                "Product ID": " p-001 ",
                "Order ID": " ORDER-1 ",
            }
        )
    ).first()
    assert row.customer_id == "C-001"
    assert row.product_id == "P-001"
    assert row.order_id == "ORDER-1"


@pytest.mark.data_quality
def test_multiple_errors_are_retained(make_order_df):
    result = clean_orders(
        make_order_df(**{"Quantity": 0, "Discount": "1.5", "Price": "-1"})
    ).first()
    assert result.dq_error_reason == "INVALID_QUANTITY|INVALID_PRICE|INVALID_DISCOUNT"


@pytest.mark.data_quality
def test_missing_required_order_column_raises(make_order_df):
    with pytest.raises(ValueError, match="Profit"):
        clean_orders(make_order_df().drop("Profit"))
