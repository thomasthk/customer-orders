"""
Task 3: ETL Export Script

Extracts active customers with orders, transforms data, exports to CSV.
Could be run on a schedule as a batch job.

Usage:
    python -m scripts.etl_export
"""

import csv
import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.models import Customer, Order

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(settings.output_dir)

CSV_FIELDNAMES = [
    "customer_id", "name", "email", "order_id",
    "product_name", "quantity", "unit_price", "total_value", "order_date",
]


def extract_active_customers_with_orders(session: Session) -> list[dict]:
    """Extract all active customers with their orders"""
    results = (
        session.query(Customer, Order)
        .join(Order, Customer.customer_id == Order.customer_id)
        .filter(Customer.status == "active")
        .order_by(Customer.customer_id, Order.order_date)
        .all()
    )

    rows = []
    for customer, order in results:
        rows.append({
            "customer_id": customer.customer_id,
            "first_name": customer.first_name,
            "surname": customer.surname,
            "email": customer.email,
            "order_id": order.order_id,
            "product_name": order.product_name,
            "quantity": order.quantity,
            "unit_price": order.unit_price,
            "order_date": str(order.order_date),
        })

    logger.info("Extracted %d records", len(rows))
    return rows


def transform_data(rows: list[dict]) -> list[dict]:
    """Concatenate names and calculate total value"""
    transformed = []

    for row in rows:
        full_name = f"{row['first_name']} {row['surname']}"
        total_value = round(row["quantity"] * row["unit_price"], 2)

        transformed.append({
            "customer_id": row["customer_id"],
            "name": full_name,
            "email": row["email"],
            "order_id": row["order_id"],
            "product_name": row["product_name"],
            "quantity": row["quantity"],
            "unit_price": row["unit_price"],
            "total_value": total_value,
            "order_date": row["order_date"],
        })

    logger.info("Transformed %d records", len(transformed))
    return transformed


def export_to_csv(data: list[dict], output_dir: Path = OUTPUT_DIR) -> Path:
    """Write transformed data to timestamped CSV file"""
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"active_customers_orders_{timestamp}.csv"
    filepath = output_dir / filename

    with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(data)

    logger.info("Exported to %s", filepath)
    return filepath


def generate_summary(data: list[dict]) -> dict:
    """Summary statistics"""
    if not data:
        logger.warning("No data")
        return {}

    unique_customers = len(set(row["customer_id"] for row in data))
    total_orders = len(data)
    total_order_value = sum(row["total_value"] for row in data)

    summary = {
        "active_customers_with_orders": unique_customers,
        "total_orders": total_orders,
        "total_order_value": round(total_order_value, 2),
    }

    logger.info("Export summary: %s", summary)
    return summary


def main() -> None:
    """Main ETL function"""
    logger.info("Starting ETL process")

    session = SessionLocal()

    try:
        raw_data = extract_active_customers_with_orders(session)

        if not raw_data:
            logger.warning("No active customers with orders found")
            return

        transformed_data = transform_data(raw_data)
        export_to_csv(transformed_data)
        generate_summary(transformed_data)

        logger.info("ETL process complete")

    except Exception as e:
        logger.error("ETL process failed: %s", e)
        raise

    finally:
        session.close()


if __name__ == "__main__":
    main()
