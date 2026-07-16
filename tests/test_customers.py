import pytest

from ecommerce_pipeline.transformations import clean_customers


@pytest.mark.unit
def test_valid_customer_is_standardized(make_customer_df):
    result = clean_customers(make_customer_df()).first()
    assert result.customer_id == "C-001"
    assert result.customer_name == "Alice Brown"
    assert result.email == "alice@example.com"
    assert result.customer_name_quality_flag == "VALID"


@pytest.mark.data_quality
def test_missing_customer_name_defaults_to_unknown(make_customer_df):
    result = clean_customers(make_customer_df(**{"Customer Name": None})).first()
    assert result.customer_name == "Unknown"
    assert result.customer_name_quality_flag == "MISSING"


@pytest.mark.data_quality
@pytest.mark.parametrize(
    "raw_name, expected_name",
    [
        ("Gary567 Hansen", "Gary Hansen"),
        ("  Tracy   P 908765oddar ", "Tracy P oddar"),
        ("Beth Tho098-.,;;mpson", "Beth Tho-mpson"),
    ],
)
def test_corrupted_names_are_cleaned(make_customer_df, raw_name, expected_name):
    result = clean_customers(make_customer_df(**{"Customer Name": raw_name})).first()
    assert result.customer_name == expected_name
    assert result.customer_name_quality_flag == "CLEANED"


@pytest.mark.data_quality
def test_phone_error_becomes_null(make_customer_df):
    result = clean_customers(make_customer_df(phone="#ERROR!")).first()
    assert result.phone is None
    assert result.phone_quality_flag == "INVALID"


@pytest.mark.data_quality
def test_invalid_email_is_flagged(make_customer_df):
    result = clean_customers(make_customer_df(email="not-an-email")).first()
    assert result.email_quality_flag == "INVALID"


@pytest.mark.unit
def test_postal_code_is_left_padded(make_customer_df):
    result = clean_customers(make_customer_df(**{"Postal Code": "2108"})).first()
    assert result.postal_code == "02108"


@pytest.mark.unit
def test_postal_code_decimal_suffix_is_removed(make_customer_df):
    result = clean_customers(make_customer_df(**{"Postal Code": "2108.0"})).first()
    assert result.postal_code == "02108"


@pytest.mark.data_quality
def test_duplicate_customer_uses_more_complete_record(make_customer_df):
    raw = make_customer_df(
        {"Customer ID": "C-009", "Customer Name": None, "phone": "#ERROR!"},
        {
            "Customer ID": "C-009",
            "Customer Name": "Complete Customer",
            "phone": "555-333-4444",
        },
    )
    result = clean_customers(raw).first()
    assert result.customer_name == "Complete Customer"
    assert result.phone == "555-333-4444"
    assert result.source_record_count == 2


@pytest.mark.unit
def test_duplicate_customer_produces_one_record(make_customer_df):
    raw = make_customer_df(
        {"Customer ID": "C-009", "Customer Name": "One"},
        {"Customer ID": "C-009", "Customer Name": "Two"},
    )
    assert clean_customers(raw).count() == 1


@pytest.mark.data_quality
def test_missing_required_customer_column_raises(make_customer_df):
    raw = make_customer_df().drop("Customer ID")
    with pytest.raises(ValueError, match="Customer ID"):
        clean_customers(raw)
