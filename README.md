# Data Engineering Project

A small end-to-end data engineering project built with Python, SQLite, FastAPI, and a JavaScript dashboard.

It includes:
- Synthetic data generation
- Batch ETL pipeline (extract, transform, load)
- SQLite data warehouse
- REST API for analytics
- Browser dashboard

## Tech Stack

- Python 3.10+
- FastAPI + Uvicorn
- SQLite
- Pandas
- Faker
- HTML/CSS/JavaScript

## Project Structure

```text
src/
  etl/         # data generation + ETL pipeline scripts
  api/         # FastAPI app
  database/    # schema and SQL files
frontend/      # dashboard UI
data/
  raw/         # source CSV files
  processed/   # logs/reports
  warehouse.db # SQLite warehouse
run.py         # command runner
```

## Setup

1. Create virtual environment

```bash
python -m venv .venv
```

2. Activate it

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

3. Install dependencies

```bash
pip install -r requirements.txt
```

## Run

Use the helper script:

```bash
python run.py generate
python run.py etl
python run.py serve
python run.py all
```

- Dashboard: http://localhost:8000
- API docs: http://localhost:8000/docs

## Main API Endpoints

- `/api/overview`
- `/api/sales/monthly`
- `/api/sales/by-category`
- `/api/sales/by-channel`
- `/api/sales/by-country`
- `/api/products/top?limit=10`
- `/api/activity/engagement`

Advanced endpoints are also available under `/api/advanced/*`.

## Notes

- Run `python run.py generate` before ETL if you want fresh synthetic data.
- Run `python run.py etl` before starting the API to ensure warehouse tables are populated.
- If port 8000 is busy, stop the running process or change the Uvicorn port in `run.py`.
