"""Tests for the REST API"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base, get_db
from app.main import app
from scripts.setup_database import load_customers, load_orders


@pytest.fixture
def client():
    """Create a test client with an in-memory database"""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(bind=engine)
    test_session_factory = sessionmaker(bind=engine)

    # Seed data using a shared connection
    connection = engine.connect()
    session = test_session_factory(bind=connection)
    load_customers(session)
    load_orders(session)
    session.commit()

    def override_get_db():
        db = test_session_factory(bind=connection)
        try:
            yield db
        finally:
            pass  # don't close, reuse the connection

    app.dependency_overrides[get_db] = override_get_db
    yield TestClient(app)
    app.dependency_overrides.clear()
    session.close()
    connection.close()
    engine.dispose()


def test_get_existing_customer(client) -> None:
    """Return customer with orders"""
    response = client.get("/customers/1")
    assert response.status_code == 200
    data = response.json()
    assert data["customer"]["customer_id"] == 1
    assert data["customer"]["first_name"] == "Emma"
    assert data["order_count"] >= 1


def test_get_customer_not_found(client) -> None:
    """Return 404 for non-existent customer"""
    response = client.get("/customers/999")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"].lower()


def test_order_has_total_value(client) -> None:
    """Each order includes calculated total_value"""
    response = client.get("/customers/1")
    data = response.json()
    for order in data["orders"]:
        expected = round(order["quantity"] * order["unit_price"], 2)
        assert order["total_value"] == expected


def test_health_check(client) -> None:
    """Health endpoint returns status"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
