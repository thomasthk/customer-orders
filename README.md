# Customer Orders API

A Python application covering database setup, REST API, and an ETL export pipeline.

---

## How to Run

Requires **Python 3.12+**.

### macOS / Linux

```bash
git clone https://github.com/thomasthk/customer-orders-api.git
cd customer-orders-api
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# Task 1 — Set up database
python -m scripts.setup_database

# Task 2 — Start API
python -m uvicorn app.main:app --reload
# Visit http://127.0.0.1:8000/customers/1 or http://127.0.0.1:8000/docs

# Task 3 — Run ETL export
python -m scripts.etl_export
# Output CSV in output/ folder

# Run tests
python -m pytest tests/ -v
```

### Windows

```bash
git clone https://github.com/thomasthk/customer-orders-api.git
cd customer-orders-api
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env

python -m scripts.setup_database
python -m uvicorn app.main:app --reload
python -m scripts.etl_export
python -m pytest tests/ -v
```

## Choices and Reasoning

### Database — SQLite

I chose SQLite because it is simple to set up and requires no external server, which keeps the setup minimal. The database file is created automatically by the setup script. Since SQLAlchemy handles the differences between database engines, switching to PostgreSQL or MySQL later would only require changing the connection string in `.env`.

### ORM — SQLAlchemy

SQLAlchemy provides defined models with relationships and constraints rather than raw SQL strings. This makes the code easier to follow and catches issues like invalid status values or negative quantities at the database level through `CheckConstraint`. I used `session.merge()` for data loading to handle repeatability. It performs an upsert, so running the setup script multiple times will not create duplicates.

### API Framework — FastAPI

I chose FastAPI as it generates interactive API documentation automatically at `/docs`, has built-in request validation via Pydantic, and uses Python type hints for both validation and documentation. The dependency injection system (`Depends(get_db)`) also made it straightforward to swap in a test database during testing.

### Validation — Pydantic

Pydantic schemas define the return of every API response, ensuring the output is always predictable and typed. I also used `pydantic-settings` to load configs from environment variables, keeping settings separate from code.

### Testing — pytest

Tests run against an in-memory SQLite database so they are fast and do not touch the real database. I wrote tests covering all three tasks: database setup (including a repeatability test), API endpoints (including 404 handling), and the ETL transform logic.

### Linting — ruff

Ruff is a single fast linter. It is configured in `pyproject.toml` to keep all tooling configuration in one place.

### Configuration — Environment Variables

The `.env` file keeps paths and the database URL out of the codebase. A `.env.example` is included so that the expected variables are clear without exposing actual values.

## Application Flow

### 1. Database Setup (`scripts/setup_database.py`)

The setup script reads sample data from JSON files in the `data/` directory and loads it into an SQLite database.

- **Table creation** — `Base.metadata.create_all()` creates the `customers` and `orders` tables from the ORM models. If the tables already exist, it won't create again.
- **Data loading** — Each record is loaded using `session.merge()`, which inserts new rows or updates existing ones by primary key. This makes the script safe to run repeatedly.
- **Verification** — After loading, the script logs a summary of row counts to confirm the data was loaded correctly.

### 2. REST API (`app/main.py`)

- A request to `GET /customers/{id}` queries the `customers` table, then fetches the related orders sorted by date. The response includes the customer details, a list of orders (each with a calculated `total_value`), and an `order_count`.
- If the customer ID does not exist, the API returns a `404` with a descriptive message.
- A `GET /health` endpoint confirms the database connection is working.
- Database sessions are managed through FastAPI's dependency injection (`get_db`), which ensures each request receives its own session that is closed after the response.

### 3. ETL Export (`scripts/etl_export.py`)

The ETL script is designed to run as a standalone batch job.

- **Extract** — Queries the database with a JOIN between customers and orders, filtered to `status = 'active'` only. Suspended and archived customers are excluded.
- **Transform** — Concatenates `first_name` and `surname` into a single `name` field, and calculates `total_value` as `quantity × unit_price` for each order.
- **Export** — Writes the results to a timestamped CSV file in the `output/` directory and each run produces a separate file with a timestamp.

## What I Would Improve

- **Authentication** — Add API key or OAuth middleware to protect the endpoints before exposing them beyond localhost.
- **Database migrations** — Use Alembic to manage schema changes incrementally rather than recreating tables, which would be essential once there is real data to preserve.
- **Schema validation on import** — The setup script trusts the JSON files fully. Adding validation (e.g. checking for duplicate emails or missing fields) before loading would make it safer to use with data from other sources.
- **ETL error handling** — If a single row fails during the transform step, the entire export fails. Adding per-row error handling with logging would allow partial exports and make it easier to identify bad data.
- **Health endpoint** — The `/health` response currently includes `customer_count`, which leaks information. In production it should return only the service status and keep internal information behind an authenticated admin endpoint.
- **Structured logging** — Currently logs go to the console only. In production I would write to a file or a centralised logging service, with structured JSON output so logs can be filtered and searched.
- **Pagination** — Add `limit` and `offset` query parameters to the customer endpoint so the API handles larger datasets without returning everything in a single response.
- **A list endpoint** — The API only supports lookup by ID. I would add `GET /customers` with optional filtering by status (e.g. `?status=active`), since the ETL already distinguishes active customers and a list view would be a natural next feature for anyone consuming this API.
- **CORS middleware** — The API has no CORS configuration, so a frontend hosted on a different origin would be blocked from calling it. Adding FastAPI's `CORSMiddleware` with an allowed origins list would resolve this.
- **Docker** — Containerise the application so it can be run consistently regardless of the local Python version or operating system.
- **ETL output cleanup** — Each run creates a new timestamped CSV, but old files are never removed. A retention policy or an option to overwrite would prevent the output directory from growing indefinitely.
- **Continuous deployment** — Extend the CI pipeline to deploy automatically on merge to main.
- **API versioning** — Prefix routes with `/v1/` to allow breaking changes in future without disrupting existing consumers.
