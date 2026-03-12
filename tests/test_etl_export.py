"""Tests for ETL export script"""

from scripts.etl_export import (
    extract_active_customers_with_orders,
    generate_summary,
    transform_data,
)


def test_extract_only_active_customers(seeded_session):
    """Only active customers should be extracted"""
    rows = extract_active_customers_with_orders(seeded_session)
    statuses = {row["customer_id"] for row in rows}

    # Check we got records
    assert len(rows) > 0

    # Verify no suspended/archived customers (4, 9 suspended; 6, 12 archived)
    assert 4 not in statuses
    assert 9 not in statuses
    assert 6 not in statuses
    assert 12 not in statuses


def test_transform_concatenates_names(seeded_session):
    """Transform should combine first_name and surname into name"""
    rows = extract_active_customers_with_orders(seeded_session)
    transformed = transform_data(rows)

    for row in transformed:
        assert "name" in row
        assert " " in row["name"]  # full name has a space
        assert "first_name" not in row
        assert "surname" not in row


def test_transform_calculates_total_value(seeded_session):
    """Transform should calculate total_value = quantity * unit_price"""
    rows = extract_active_customers_with_orders(seeded_session)
    transformed = transform_data(rows)

    for row in transformed:
        expected = round(row["quantity"] * row["unit_price"], 2)
        assert row["total_value"] == expected


def test_generate_summary(seeded_session):
    """Summary should contain correct keys and positive values"""
    rows = extract_active_customers_with_orders(seeded_session)
    transformed = transform_data(rows)
    summary = generate_summary(transformed)

    assert "active_customers_with_orders" in summary
    assert "total_orders" in summary
    assert "total_order_value" in summary
    assert summary["total_orders"] == len(transformed)
    assert summary["total_order_value"] > 0
