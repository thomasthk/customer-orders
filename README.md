# Customer Orders API

A Python application with database operations, REST API, and ETL processing.

---

## How to Run

Requires **Python 3.12+**. On Windows, use `python` / `pip` instead of `python3` / `pip3`.

```bash
# Clone and set up
git clone https://github.com/thomasthk/customer-orders-api.git
cd customer-orders-api
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip3 install -r requirements.txt
cp .env.example .env

# Task 1 — Set up database
python3 -m scripts.setup_database

# Task 2 — Start API
python3 -m uvicorn app.main:app --reload
# Visit http://127.0.0.1:8000/customers/1 or http://127.0.0.1:8000/docs

# Task 3 — Run ETL export
python3 -m scripts.etl_export
# Output CSV in output/ folder

# Run tests
python3 -m pytest tests/ -v
```

## Choices and Reasoning

- **SQLite** — Easy to setup database. No external database server needed.
- **SQLAlchemy ORM** — Provides type-safe models and relationships. Makes the code more readable and maintainable.
- **FastAPI** — Python web framework with automatic OpenAPI docs, built-in validation via Pydantic, and clear error handling.
- **Pydantic** — Enforces response schemas so the API always returns predictable, typed JSON. Also used for loading environment variables via pydantic-settings.
- **pytest** — Python testing framework. Tests use in-memory SQLite for speed and isolation.
- **ruff** — all-in-one linter. Configured in pyproject.toml to keep tooling centralised.
- **Environment variables (.env)** — Keeps configuration separate from code, following the twelve-factor app pattern.

## Application Flow

1. **Database setup** (`scripts/setup_database.py`) — Creates tables from ORM models, loads customers and orders from JSON files using merge for repeatability.
2. **REST API** (`app/main.py`) — Serves customer data with orders via GET endpoints. Uses dependency injection for database sessions.
3. **ETL export** (`scripts/etl_export.py`) — Extracts active customers with orders from the database, calculates total order values, and exports to a timestamped CSV.

## What I Would Improve

- **Database migrations** — Use Alembic to manage schema changes instead of recreating tables.
- **Authentication** — Add API key or OAuth to protect endpoints.
- **Pagination** — Add limit/offset to prevent large responses as data grows.
- **Docker** — Containerise the application for consistent deployment across environments.
- **CI/CD pipeline** — Add GitHub Actions for automated linting, testing, and deployment.
- **API versioning** — Prefix routes with `/v1/` to allow future breaking changes.
- **Health endpoint** — Remove database row counts from the response to avoid leaking internal metrics.
