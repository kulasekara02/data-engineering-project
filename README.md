# Data Engineering Project

A comprehensive end-to-end data engineering platform built with Python, SQLite, FastAPI, and a JavaScript dashboard. Features synthetic data generation, batch ETL, real-time stream processing, data quality auditing, SCD Type 2 tracking, live API ingestion, DAG-based orchestration, and an interactive analytics dashboard.

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| **Backend** | Python 3.10+, FastAPI, Uvicorn, Pandas, Faker, Pydantic |
| **Database** | SQLite (star schema) |
| **Frontend** | HTML5, CSS3, JavaScript, Chart.js |
| **DevOps** | Docker, Kubernetes (blue-green deployment), GitHub Actions |
| **Monitoring** | Prometheus, Grafana |
| **Testing** | pytest, flake8, black, isort, bandit |

## Project Structure

```text
data-engineering-project/
├── src/
│   ├── api/
│   │   └── main.py                  # FastAPI app with 30+ endpoints
│   ├── etl/
│   │   ├── generate_data.py         # Synthetic data generation
│   │   ├── etl_pipeline.py          # Extract / Transform / Load
│   │   ├── data_quality.py          # Data quality audit framework
│   │   ├── pipeline_orchestrator.py # DAG-based orchestration
│   │   ├── scd_handler.py           # SCD Type 2 dimension tracking
│   │   ├── stream_processor.py      # Real-time streaming simulation
│   │   └── api_ingestion.py         # Public API data ingestion
│   └── database/
│       ├── schema.sql               # Star schema + materialized views
│       ├── queries.sql              # Analytical queries
│       └── advanced_queries.sql     # Advanced analytics queries
├── frontend/
│   ├── index.html                   # 6-tab interactive dashboard
│   ├── js/dashboard.js
│   └── css/style.css
├── k8s/                             # Kubernetes blue-green manifests
├── monitoring/                      # Prometheus configuration
├── tests/                           # Unit and integration tests
├── data/
│   ├── raw/                         # Source CSV files + API responses
│   ├── processed/                   # Logs, lineage, quality reports
│   └── warehouse.db                 # SQLite data warehouse
├── Dockerfile                       # Multi-stage production image
├── docker-compose.yml               # App + Prometheus + Grafana
├── run.py                           # Command runner
├── requirements.txt
└── pyproject.toml
```

## Features

### ETL Pipeline
Extract, transform, and load synthetic data into a SQLite star schema with dimension tables (customers, products, dates, channels) and fact tables (sales, user activity).

### Data Quality Auditing
Validation engine covering completeness, uniqueness, referential integrity, statistical outlier detection, temporal consistency, and business rule enforcement. Generates JSON quality reports with severity levels.

### Pipeline Orchestrator
DAG-based execution engine with topological sorting, configurable retry logic with exponential backoff, status tracking, and lineage recording.

### SCD Type 2 Handling
Slowly Changing Dimension implementation with historical tracking, surrogate keys, effective date ranges, version control, and a full audit trail.

### Stream Processing
Real-time streaming simulation with tumbling and sliding window aggregations, watermark-based late event detection, and exactly-once semantics.

### Live API Ingestion
Fetches and loads data from public APIs: CoinGecko (crypto prices), REST Countries, Open-Meteo (weather), and Exchange Rates.

### Interactive Dashboard
Six-tab frontend with Chart.js visualizations:
1. **Overview** - KPI cards and sales/engagement charts
2. **Advanced Analytics** - Cohort analysis, RFM segmentation, Pareto, CLV
3. **Live API Data** - Real-time crypto, weather, country, and exchange rate data
4. **Stream Processing** - Event stream simulation with windowed metrics
5. **Data Quality** - Audit results with pass/warn/fail indicators
6. **Pipeline Lineage** - DAG visualization and task execution timeline

### REST API
30+ analytics endpoints under `/api/` covering sales, products, customers, activity, advanced analytics, and live data.

## Setup

### Local Development

1. Create and activate a virtual environment

```bash
python -m venv .venv
```

Windows PowerShell:
```powershell
.\.venv\Scripts\Activate.ps1
```

Linux / macOS:
```bash
source .venv/bin/activate
```

2. Install dependencies

```bash
pip install -r requirements.txt
```

### Docker

```bash
docker compose up --build
```

Or with the monitoring stack:

```bash
docker compose --profile monitoring up --build
```

## Usage

Use the command runner:

```bash
python run.py generate    # Generate synthetic data (500 customers, 50 products, 5000 sales)
python run.py etl         # Run the ETL pipeline
python run.py quality     # Run data quality audit
python run.py scd         # SCD Type 2 processing demo
python run.py stream      # Stream processing simulation
python run.py ingest      # Fetch data from public APIs
python run.py pipeline    # Full DAG orchestration
python run.py serve       # Start API server
python run.py all         # Full pipeline + serve
```

- **Dashboard:** http://localhost:8000
- **API Docs:** http://localhost:8000/docs
- **Prometheus:** http://localhost:9090 (with monitoring profile)
- **Grafana:** http://localhost:3000 (with monitoring profile)

## API Endpoints

### Sales Analytics
- `GET /api/sales/monthly` - Revenue and profit trends
- `GET /api/sales/by-category` - Category breakdown
- `GET /api/sales/by-channel` - Channel performance
- `GET /api/sales/by-country` - Geographic distribution
- `GET /api/sales/daily-pattern` - Day-of-week analysis

### Product Analytics
- `GET /api/products/top?limit=10` - Top products by revenue
- `GET /api/products/by-category` - Category inventory
- `GET /api/products/margin-analysis` - Profitability metrics

### Customer Analytics
- `GET /api/customers/segments` - RFM segmentation
- `GET /api/customers/lifetime-value` - CLV predictions
- `GET /api/customers/by-country` - Geographic distribution

### Advanced Analytics
- `GET /api/advanced/cohort-analysis` - Cohort retention
- `GET /api/advanced/attribution` - Channel attribution
- `GET /api/advanced/forecast` - Trend forecasting
- `GET /api/advanced/pareto` - 80/20 analysis
- `GET /api/advanced/churn-risk` - Churn prediction

### Live Data
- `GET /api/live/crypto` - Cryptocurrency prices
- `GET /api/live/weather` - Current weather data
- `GET /api/live/countries` - Country information
- `GET /api/live/exchange-rates` - Currency exchange rates

## Data Architecture

**Star Schema** with:
- **Dimensions:** `dim_customers` (500), `dim_products` (50), `dim_date`, `dim_channels` (6)
- **Facts:** `fact_sales` (5,000 transactions), `fact_user_activity` (8,000 records)
- **Views:** `v_monthly_sales`, `v_customer_summary`, `v_product_performance`
- **SCD Tables:** `dim_customers_history`, `dim_products_history`, `scd_change_log`

## CI/CD

- **CI Pipeline** - Lint, test, security scan, Docker build on every push
- **PR Quality Gate** - Automated checks on pull requests
- **Data Quality** - Scheduled daily audit at 6 AM UTC
- **Blue-Green Deployment** - Kubernetes deployment via GHCR with zero-downtime traffic switching

## Notes

- Run `python run.py generate` before ETL if you want fresh synthetic data.
- Run `python run.py etl` before starting the API to ensure warehouse tables are populated.
- If port 8000 is busy, stop the running process or change the Uvicorn port in `run.py`.
