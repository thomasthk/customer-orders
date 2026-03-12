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
