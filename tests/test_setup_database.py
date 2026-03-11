"""Tests for database setup script"""

import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError

from app.models import Customer, Order
from scripts.setup_database import load_customers, load_orders, verify_data


def test_tables_exist(test_engine) -> None:
    """Tables should exist after creation"""
    inspector = inspect(test_engine)
    tables = inspector.get_table_names()
    assert "customers" in tables
    assert "orders" in tables


def test_load_customers(test_session) -> None:
    """Should load all customers from JSON"""
    count = load_customers(test_session)
    test_session.commit()
    assert count == 15
    assert test_session.query(Customer).count() == 15


def test_load_customers_is_repeatable(test_session) -> None:
    """Loading customers twice should not create duplicates"""
    load_customers(test_session)
    load_customers(test_session)
    test_session.commit()
    assert test_session.query(Customer).count() == 15


def test_load_orders(test_session) -> None:
    """Should load all orders from JSON"""
    load_customers(test_session)
    count = load_orders(test_session)
    test_session.commit()
    assert count == 25
    assert test_session.query(Order).count() == 25


def test_verify_data(test_session) -> None:
    """verify_data should return correct counts"""
    load_customers(test_session)
    load_orders(test_session)
    test_session.commit()

    summary = verify_data(test_session)
    assert summary["total_customers"] == 15
    assert summary["active_customers"] == 11
    assert summary["total_orders"] == 25
