from decimal import Decimal

import pytest

from ecommerce_pipeline.transformations import clean_products


@pytest.mark.unit
def test_single_product_is_standardized(make_product_df):
    result = clean_products(make_product_df()).first()
    assert result.product_id == "P-001"
    assert result.category == "Furniture"
    assert result.sub_category == "Chairs"
    assert result.min_unit_price == Decimal("25.5000")


@pytest.mark.data_quality
def test_duplicate_product_ids_are_consolidated(make_product_df):
    raw = make_product_df(
        {"Product ID": "P-009", "Product Name": "Chair B"},
        {"Product ID": "P-009", "Product Name": "Chair A"},
    )
    result = clean_products(raw)
    assert result.count() == 1
    assert result.first().source_record_count == 2


@pytest.mark.unit
def test_canonical_product_name_is_deterministic(make_product_df):
    raw = make_product_df(
        {"Product ID": "P-009", "Product Name": "Chair B"},
        {"Product ID": "P-009", "Product Name": "Chair A"},
    )
    assert clean_products(raw).first().canonical_product_name == "Chair A"


@pytest.mark.data_quality
def test_product_name_conflict_is_flagged(make_product_df):
    raw = make_product_df(
        {"Product ID": "P-009", "Product Name": "Chair A"},
        {"Product ID": "P-009", "Product Name": "Chair B"},
    )
    assert clean_products(raw).first().product_name_conflict_flag is True


@pytest.mark.data_quality
def test_consistent_product_name_is_not_flagged(make_product_df):
    raw = make_product_df(
        {"Product ID": "P-009", "Product Name": "Chair A"},
        {"Product ID": "P-009", "Product Name": "Chair A", "State": "Texas"},
    )
    assert clean_products(raw).first().product_name_conflict_flag is False


@pytest.mark.unit
def test_product_price_range_is_retained(make_product_df):
    raw = make_product_df(
        {"Product ID": "P-009", "Price per product": "10.1000"},
        {"Product ID": "P-009", "Price per product": "15.9000"},
    )
    result = clean_products(raw).first()
    assert result.min_unit_price == Decimal("10.1000")
    assert result.max_unit_price == Decimal("15.9000")


@pytest.mark.data_quality
def test_category_conflict_is_visible(make_product_df):
    raw = make_product_df(
        {"Product ID": "P-009", "Category": "Furniture"},
        {"Product ID": "P-009", "Category": "Technology"},
    )
    assert clean_products(raw).first().category_conflict_flag is True


@pytest.mark.data_quality
def test_sub_category_conflict_is_visible(make_product_df):
    raw = make_product_df(
        {"Product ID": "P-009", "Sub-Category": "Chairs"},
        {"Product ID": "P-009", "Sub-Category": "Tables"},
    )
    assert clean_products(raw).first().sub_category_conflict_flag is True


@pytest.mark.data_quality
def test_null_product_id_is_excluded(make_product_df):
    raw = make_product_df({"Product ID": None}, {"Product ID": "P-002"})
    result = clean_products(raw)
    assert result.count() == 1
    assert result.first().product_id == "P-002"


@pytest.mark.data_quality
def test_missing_required_product_column_raises(make_product_df):
    with pytest.raises(ValueError, match="Sub-Category"):
        clean_products(make_product_df().drop("Sub-Category"))
