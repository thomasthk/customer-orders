"""
Task 2: REST API

Returns customer and order data for a single customer by ID.

Usage:
    uvicorn app.main:app --reload

Endpoints:
    GET /customers/{id}  - Get customer with orders
    GET /health          - Health check
    GET /docs            - API documentation (auto-generated)
"""

import logging

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Customer, Order
from app.schemas import (
    CustomerWithOrdersResponse,
    HealthResponse,
    OrderResponse,
    RootResponse,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Customer Orders API",
    description="API to retrieve customer and order information",
    version="1.0.0",
)


@app.get("/", response_model=RootResponse)
def root() -> dict:
    """Root endpoint with API information"""
    return {
        "message": "Customer Orders API",
        "endpoints": {
            "get_customer": "/customers/{customer_id}",
            "health": "/health",
            "documentation": "/docs",
        },
    }


@app.get("/customers/{customer_id}", response_model=CustomerWithOrdersResponse)
def get_customer_with_orders(customer_id: int, db: Session = Depends(get_db)) -> dict:
    """Get a customer and their orders by customer ID"""
    customer = db.query(Customer).filter(Customer.customer_id == customer_id).first()

    if not customer:
        logger.warning("Customer %d not found", customer_id)
        raise HTTPException(
            status_code=404,
            detail=f"Customer with id {customer_id} not found",
        )

    orders = (
        db.query(Order)
        .filter(Order.customer_id == customer_id)
        .order_by(Order.order_date.desc())
        .all()
    )

    order_responses = [
        OrderResponse(
            order_id=order.order_id,
            product_name=order.product_name,
            quantity=order.quantity,
            unit_price=order.unit_price,
            order_date=str(order.order_date),
            total_value=round(order.quantity * order.unit_price, 2),
        )
        for order in orders
    ]

    logger.info("Retrieved customer %d with %d orders", customer_id, len(orders))

    return {
        "customer": customer,
        "orders": order_responses,
        "order_count": len(orders),
    }


@app.get("/health", response_model=HealthResponse)
def health_check(db: Session = Depends(get_db)) -> dict:
    """Health check endpoint"""
    try:
        db.query(Customer).count()
        return {"status": "healthy"}
    except Exception as e:
        logger.error("Health check failed: %s", e)
        raise HTTPException(status_code=500, detail="Database unavailable")
