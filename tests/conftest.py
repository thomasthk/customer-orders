"""Shared test fixtures"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base


@pytest.fixture
def test_engine():
    """Create an in-memory SQLite engine for testing"""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture
def test_session(test_engine):
    """Create a test database session"""
    test_session_factory = sessionmaker(bind=test_engine)
    session = test_session_factory()
    yield session
    session.close()


@pytest.fixture
def seeded_session(test_session):
    """Test session pre-loaded with sample data"""
    from scripts.setup_database import load_customers, load_orders

    load_customers(test_session)
    load_orders(test_session)
    test_session.commit()
    yield test_session
