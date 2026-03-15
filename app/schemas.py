"""Pydantic schemas for API response models"""

from pydantic import BaseModel


class OrderResponse(BaseModel):
    """Single order in API responses"""

    order_id: int
    product_name: str
    quantity: int
    unit_price: float
    order_date: str
    total_value: float

    model_config = {"from_attributes": True}


class CustomerResponse(BaseModel):
    """Customer data in API responses"""

    customer_id: int
    first_name: str
    surname: str
    email: str
    status: str

    model_config = {"from_attributes": True}


class CustomerWithOrdersResponse(BaseModel):
    """Customer with their orders"""

    customer: CustomerResponse
    orders: list[OrderResponse]
    order_count: int


class HealthResponse(BaseModel):
    """Health check response"""

    status: str


class RootResponse(BaseModel):
    """Root endpoint response"""

    message: str
    endpoints: dict[str, str]
