"""
DATA API - FastAPI application serving warehouse data.
Provides REST endpoints for the JavaScript dashboard.
"""

import os
import sqlite3
from contextlib import contextmanager

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "..")
DB_PATH = os.path.join(BASE_DIR, "data", "warehouse.db")
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")

app = FastAPI(
    title="Data Engineering API", description="REST API for sales & analytics data warehouse", version="1.0.0"
)

# Allow frontend to access the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@contextmanager
def get_db():
    """Database connection context manager."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def rows_to_dicts(rows):
    return [dict(row) for row in rows]


# --- API ENDPOINTS ---


@app.get("/api/overview")
def get_overview():
    """Dashboard overview with key metrics."""
    with get_db() as conn:
        cur = conn.cursor()
        stats = {}
        stats["total_revenue"] = cur.execute("SELECT ROUND(SUM(total_amount),2) FROM fact_sales").fetchone()[0]
        stats["total_profit"] = cur.execute("SELECT ROUND(SUM(profit),2) FROM fact_sales").fetchone()[0]
        stats["total_orders"] = cur.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
        stats["total_customers"] = cur.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
        stats["total_products"] = cur.execute("SELECT COUNT(*) FROM dim_products").fetchone()[0]
        stats["avg_order_value"] = cur.execute("SELECT ROUND(AVG(total_amount),2) FROM fact_sales").fetchone()[0]
        stats["total_sessions"] = cur.execute("SELECT COUNT(*) FROM fact_user_activity").fetchone()[0]
        stats["bounce_rate"] = cur.execute(
            "SELECT ROUND(SUM(bounce)*100.0/COUNT(*),1) FROM fact_user_activity"
        ).fetchone()[0]
        return stats


@app.get("/api/sales/monthly")
def get_monthly_sales():
    """Monthly sales trend data."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT d.year, d.month, d.month_name,
                   COUNT(s.sale_id) AS orders,
                   ROUND(SUM(s.total_amount),2) AS revenue,
                   ROUND(SUM(s.profit),2) AS profit,
                   COUNT(DISTINCT s.customer_id) AS unique_customers
            FROM fact_sales s
            JOIN dim_date d ON s.date_id = d.date_id
            GROUP BY d.year, d.month
            ORDER BY d.year, d.month
        """).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/sales/by-category")
def get_sales_by_category():
    """Sales breakdown by product category."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT p.category,
                   COUNT(s.sale_id) AS orders,
                   ROUND(SUM(s.total_amount),2) AS revenue,
                   ROUND(SUM(s.profit),2) AS profit,
                   ROUND(SUM(s.profit)/SUM(s.total_amount)*100,1) AS margin_pct
            FROM fact_sales s
            JOIN dim_products p ON s.product_id = p.product_id
            GROUP BY p.category
            ORDER BY revenue DESC
        """).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/sales/by-channel")
def get_sales_by_channel():
    """Sales breakdown by channel."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT ch.channel_name, ch.channel_type,
                   COUNT(s.sale_id) AS orders,
                   ROUND(SUM(s.total_amount),2) AS revenue,
                   ROUND(AVG(s.total_amount),2) AS avg_order_value
            FROM fact_sales s
            JOIN dim_channels ch ON s.channel_id = ch.channel_id
            GROUP BY ch.channel_id
            ORDER BY revenue DESC
        """).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/sales/by-country")
def get_sales_by_country():
    """Sales by customer country."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT c.country,
                   COUNT(DISTINCT c.customer_id) AS customers,
                   COUNT(s.sale_id) AS orders,
                   ROUND(SUM(s.total_amount),2) AS revenue
            FROM fact_sales s
            JOIN dim_customers c ON s.customer_id = c.customer_id
            GROUP BY c.country
            ORDER BY revenue DESC
        """).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/sales/daily-pattern")
def get_daily_pattern():
    """Sales pattern by day of week."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT d.day_name, d.day_of_week,
                   COUNT(s.sale_id) AS orders,
                   ROUND(SUM(s.total_amount),2) AS revenue,
                   ROUND(AVG(s.total_amount),2) AS avg_order
            FROM fact_sales s
            JOIN dim_date d ON s.date_id = d.date_id
            GROUP BY d.day_of_week
            ORDER BY d.day_of_week
        """).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/products/top")
def get_top_products(limit: int = Query(default=10, le=50)):
    """Top products by revenue."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT p.product_name, p.category,
                   SUM(s.quantity) AS units_sold,
                   ROUND(SUM(s.total_amount),2) AS revenue,
                   ROUND(SUM(s.profit),2) AS profit
            FROM fact_sales s
            JOIN dim_products p ON s.product_id = p.product_id
            GROUP BY p.product_id
            ORDER BY revenue DESC
            LIMIT ?
        """,
            (limit,),
        ).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/customers/segments")
def get_customer_segments():
    """Customer segmentation by lifetime value."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                CASE
                    WHEN lv >= 5000 THEN 'Platinum'
                    WHEN lv >= 2000 THEN 'Gold'
                    WHEN lv >= 500 THEN 'Silver'
                    ELSE 'Bronze'
                END AS segment,
                COUNT(*) AS customer_count,
                ROUND(AVG(lv),2) AS avg_ltv,
                ROUND(AVG(orders),1) AS avg_orders
            FROM (
                SELECT c.customer_id,
                       COALESCE(SUM(s.total_amount),0) AS lv,
                       COUNT(s.sale_id) AS orders
                FROM dim_customers c
                LEFT JOIN fact_sales s ON c.customer_id = s.customer_id
                GROUP BY c.customer_id
            )
            GROUP BY segment
            ORDER BY avg_ltv DESC
        """).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/activity/engagement")
def get_engagement():
    """User engagement metrics by channel."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT ch.channel_name,
                   COUNT(a.activity_id) AS sessions,
                   ROUND(AVG(a.session_duration_sec),0) AS avg_duration_sec,
                   ROUND(AVG(a.pages_viewed),1) AS avg_pages,
                   ROUND(SUM(a.bounce)*100.0/COUNT(*),1) AS bounce_rate_pct
            FROM fact_user_activity a
            JOIN dim_channels ch ON a.channel_id = ch.channel_id
            GROUP BY ch.channel_id
            ORDER BY sessions DESC
        """).fetchall()
        return rows_to_dicts(rows)


# --- ADVANCED ENDPOINTS (PhD Level) ---


@app.get("/api/advanced/monthly-trend")
def get_monthly_trend_advanced():
    """Monthly trend with cumulative revenue, moving average, and MoM change."""
    with get_db() as conn:
        rows = conn.execute("""
            WITH monthly AS (
                SELECT d.year, d.month, d.month_name,
                       ROUND(SUM(s.total_amount),2) AS revenue,
                       ROUND(SUM(s.profit),2) AS profit
                FROM fact_sales s
                JOIN dim_date d ON s.date_id = d.date_id
                GROUP BY d.year, d.month
            )
            SELECT year, month, month_name, revenue, profit,
                ROUND(SUM(revenue) OVER (ORDER BY year, month), 2) AS cumulative_revenue,
                ROUND(AVG(revenue) OVER (
                    ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ), 2) AS moving_avg_3m,
                ROUND(revenue - LAG(revenue) OVER (ORDER BY year, month), 2) AS mom_change,
                ROUND((revenue - LAG(revenue) OVER (ORDER BY year, month))
                    / NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0) * 100, 1) AS mom_pct_change
            FROM monthly
        """).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/advanced/rfm")
def get_rfm_analysis():
    """RFM (Recency, Frequency, Monetary) customer segmentation."""
    with get_db() as conn:
        rows = conn.execute("""
            WITH rfm_raw AS (
                SELECT c.customer_id,
                    c.first_name || ' ' || c.last_name AS customer_name,
                    c.country,
                    julianday('2024-12-31') - julianday(MAX(d.full_date)) AS recency_days,
                    COUNT(DISTINCT s.sale_id) AS frequency,
                    ROUND(SUM(s.total_amount), 2) AS monetary
                FROM dim_customers c
                JOIN fact_sales s ON c.customer_id = s.customer_id
                JOIN dim_date d ON s.date_id = d.date_id
                GROUP BY c.customer_id
            ),
            rfm_scored AS (
                SELECT *, NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
                    NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
                    NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
                FROM rfm_raw
            )
            SELECT customer_id, customer_name, country,
                ROUND(recency_days,0) AS recency_days, frequency, monetary,
                r_score, f_score, m_score,
                r_score + f_score + m_score AS rfm_total,
                CASE
                    WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
                    WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
                    WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
                    WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
                    WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
                    ELSE 'Potential Loyalists'
                END AS rfm_segment
            FROM rfm_scored
            ORDER BY rfm_total DESC
        """).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/advanced/rfm-summary")
def get_rfm_summary():
    """RFM segment summary counts and averages."""
    with get_db() as conn:
        rows = conn.execute("""
            WITH rfm_raw AS (
                SELECT c.customer_id,
                    julianday('2024-12-31') - julianday(MAX(d.full_date)) AS recency_days,
                    COUNT(DISTINCT s.sale_id) AS frequency,
                    ROUND(SUM(s.total_amount), 2) AS monetary
                FROM dim_customers c
                JOIN fact_sales s ON c.customer_id = s.customer_id
                JOIN dim_date d ON s.date_id = d.date_id
                GROUP BY c.customer_id
            ),
            rfm_scored AS (
                SELECT *, NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
                    NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
                    NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
                FROM rfm_raw
            ),
            rfm_segmented AS (
                SELECT *,
                    CASE
                        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
                        WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
                        WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
                        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
                        WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
                        ELSE 'Potential Loyalists'
                    END AS rfm_segment
                FROM rfm_scored
            )
            SELECT rfm_segment, COUNT(*) AS customer_count,
                ROUND(AVG(monetary),2) AS avg_monetary,
                ROUND(AVG(frequency),1) AS avg_frequency,
                ROUND(AVG(recency_days),0) AS avg_recency_days
            FROM rfm_segmented
            GROUP BY rfm_segment
            ORDER BY avg_monetary DESC
        """).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/advanced/pareto")
def get_pareto_analysis():
    """Pareto (80/20) analysis of customer revenue contribution."""
    with get_db() as conn:
        rows = conn.execute("""
            WITH customer_revenue AS (
                SELECT c.customer_id,
                    c.first_name || ' ' || c.last_name AS name,
                    ROUND(SUM(s.total_amount), 2) AS revenue
                FROM fact_sales s
                JOIN dim_customers c ON s.customer_id = c.customer_id
                GROUP BY c.customer_id
            ),
            ranked AS (
                SELECT *,
                    SUM(revenue) OVER (ORDER BY revenue DESC) AS cumulative_revenue,
                    SUM(revenue) OVER () AS total_revenue,
                    ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rank_num,
                    COUNT(*) OVER () AS total_customers
                FROM customer_revenue
            )
            SELECT rank_num, name, revenue,
                ROUND(cumulative_revenue, 2) AS cumulative_revenue,
                ROUND(cumulative_revenue / total_revenue * 100, 1) AS cumulative_pct,
                ROUND(rank_num * 100.0 / total_customers, 1) AS customer_percentile
            FROM ranked
            ORDER BY rank_num
        """).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/advanced/clv")
def get_clv_prediction():
    """Customer Lifetime Value prediction (24-month)."""
    with get_db() as conn:
        rows = conn.execute("""
            WITH customer_metrics AS (
                SELECT c.customer_id,
                    c.first_name || ' ' || c.last_name AS name,
                    c.country,
                    COUNT(DISTINCT s.sale_id) AS total_purchases,
                    ROUND(SUM(s.total_amount), 2) AS total_spent,
                    ROUND(AVG(s.total_amount), 2) AS avg_purchase_value,
                    COUNT(DISTINCT strftime('%Y-%m', d.full_date)) AS active_months
                FROM dim_customers c
                JOIN fact_sales s ON c.customer_id = s.customer_id
                JOIN dim_date d ON s.date_id = d.date_id
                GROUP BY c.customer_id
            )
            SELECT customer_id, name, country, total_purchases, total_spent,
                avg_purchase_value, active_months,
                ROUND(total_purchases * 1.0 / NULLIF(active_months, 0), 2) AS purchase_freq_monthly,
                ROUND(avg_purchase_value * (total_purchases * 1.0 / NULLIF(active_months, 0)) * 24, 2) AS predicted_clv_24m
            FROM customer_metrics
            WHERE total_purchases > 1
            ORDER BY predicted_clv_24m DESC
            LIMIT 50
        """).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/advanced/channel-attribution")
def get_channel_attribution():
    """Channel attribution analysis with revenue share and efficiency ranking."""
    with get_db() as conn:
        rows = conn.execute("""
            WITH channel_stats AS (
                SELECT ch.channel_name, ch.channel_type,
                    COUNT(s.sale_id) AS conversions,
                    ROUND(SUM(s.total_amount), 2) AS attributed_revenue,
                    COUNT(DISTINCT s.customer_id) AS unique_customers,
                    ROUND(AVG(s.total_amount), 2) AS avg_order_value,
                    ROUND(SUM(s.profit), 2) AS attributed_profit
                FROM fact_sales s
                JOIN dim_channels ch ON s.channel_id = ch.channel_id
                GROUP BY ch.channel_id
            )
            SELECT *,
                ROUND(attributed_revenue * 100.0 / SUM(attributed_revenue) OVER (), 1) AS revenue_share_pct,
                ROUND(conversions * 100.0 / SUM(conversions) OVER (), 1) AS conversion_share_pct,
                ROUND(attributed_profit / NULLIF(attributed_revenue, 0) * 100, 1) AS profit_margin_pct
            FROM channel_stats
            ORDER BY attributed_revenue DESC
        """).fetchall()
        return rows_to_dicts(rows)


@app.get("/api/advanced/stream-windows")
def get_stream_windows():
    """Real-time streaming window aggregations."""
    with get_db() as conn:
        try:
            rows = conn.execute("""
                SELECT * FROM stream_windows ORDER BY window_start DESC LIMIT 50
            """).fetchall()
            return rows_to_dicts(rows)
        except Exception:
            return []


@app.get("/api/advanced/data-quality")
def get_data_quality():
    """Latest data quality report."""
    import glob
    import json

    report_dir = os.path.join(BASE_DIR, "data", "processed", "quality_reports")
    reports = sorted(glob.glob(os.path.join(report_dir, "*.json")))
    if not reports:
        return {"message": "No quality reports found. Run the pipeline first."}
    with open(reports[-1], "r") as f:
        return json.load(f)


@app.get("/api/advanced/scd-history/{customer_id}")
def get_scd_history(customer_id: int):
    """SCD Type 2 history for a specific customer."""
    with get_db() as conn:
        try:
            rows = conn.execute(
                """
                SELECT * FROM dim_customers_history
                WHERE customer_id = ? ORDER BY version
            """,
                (customer_id,),
            ).fetchall()
            return rows_to_dicts(rows)
        except Exception:
            return []


@app.get("/api/advanced/lineage")
def get_pipeline_lineage():
    """Pipeline execution lineage and DAG history."""
    import json

    lineage_path = os.path.join(BASE_DIR, "data", "processed", "pipeline_lineage.json")
    if os.path.exists(lineage_path):
        with open(lineage_path, "r") as f:
            return json.load(f)
    return {"message": "No lineage data found. Run the pipeline first."}


# --- PUBLIC API DATA ENDPOINTS ---


@app.get("/api/live/crypto")
def get_crypto_prices():
    """Live cryptocurrency prices from CoinGecko."""
    with get_db() as conn:
        try:
            rows = conn.execute("""
                SELECT name, symbol, current_price_usd, market_cap, total_volume,
                       price_change_24h, price_change_pct_24h, high_24h, low_24h,
                       circulating_supply, rank, last_updated
                FROM api_crypto_prices ORDER BY rank LIMIT 30
            """).fetchall()
            return rows_to_dicts(rows)
        except Exception:
            return []


@app.get("/api/live/crypto/summary")
def get_crypto_summary():
    """Crypto market summary stats."""
    with get_db() as conn:
        try:
            row = conn.execute("""
                SELECT COUNT(*) as total_coins,
                       ROUND(SUM(market_cap), 0) as total_market_cap,
                       ROUND(SUM(total_volume), 0) as total_volume_24h,
                       ROUND(AVG(price_change_pct_24h), 2) as avg_change_pct,
                       SUM(CASE WHEN price_change_pct_24h > 0 THEN 1 ELSE 0 END) as gainers,
                       SUM(CASE WHEN price_change_pct_24h <= 0 THEN 1 ELSE 0 END) as losers
                FROM api_crypto_prices
            """).fetchone()
            return dict(row)
        except Exception:
            return {}


@app.get("/api/live/countries")
def get_countries():
    """Country data from REST Countries API."""
    with get_db() as conn:
        try:
            rows = conn.execute("""
                SELECT name, capital, region, subregion, population, area,
                       languages, currencies, flag_emoji, latitude, longitude
                FROM api_countries
                WHERE population > 0
                ORDER BY population DESC
            """).fetchall()
            return rows_to_dicts(rows)
        except Exception:
            return []


@app.get("/api/live/countries/by-region")
def get_countries_by_region():
    """Countries aggregated by region."""
    with get_db() as conn:
        try:
            rows = conn.execute("""
                SELECT region, COUNT(*) as country_count,
                       SUM(population) as total_population,
                       ROUND(SUM(area), 0) as total_area,
                       ROUND(AVG(population), 0) as avg_population
                FROM api_countries
                WHERE region != '' AND population > 0
                GROUP BY region
                ORDER BY total_population DESC
            """).fetchall()
            return rows_to_dicts(rows)
        except Exception:
            return []


@app.get("/api/live/weather")
def get_weather():
    """Current weather for major cities."""
    with get_db() as conn:
        try:
            rows = conn.execute("""
                SELECT city, country, temperature_c, wind_speed_kmh,
                       humidity_pct, weather_desc, measured_at
                FROM api_weather ORDER BY city
            """).fetchall()
            return rows_to_dicts(rows)
        except Exception:
            return []


@app.get("/api/live/exchange-rates")
def get_exchange_rates():
    """USD exchange rates."""
    with get_db() as conn:
        try:
            rows = conn.execute("""
                SELECT target_currency, rate, rate_date
                FROM api_exchange_rates ORDER BY target_currency
            """).fetchall()
            return rows_to_dicts(rows)
        except Exception:
            return []


@app.get("/api/live/github")
def get_github_repos():
    """Trending GitHub repositories."""
    with get_db() as conn:
        try:
            rows = conn.execute("""
                SELECT repo_name, full_name, description, language,
                       stars, forks, open_issues, watchers, html_url
                FROM api_github_trending ORDER BY stars DESC
            """).fetchall()
            return rows_to_dicts(rows)
        except Exception:
            return []


@app.get("/api/live/github/by-language")
def get_github_by_language():
    """GitHub repos grouped by language."""
    with get_db() as conn:
        try:
            rows = conn.execute("""
                SELECT COALESCE(language, 'Unknown') as language,
                       COUNT(*) as repo_count,
                       SUM(stars) as total_stars,
                       SUM(forks) as total_forks,
                       ROUND(AVG(stars), 0) as avg_stars
                FROM api_github_trending
                GROUP BY language
                ORDER BY total_stars DESC
            """).fetchall()
            return rows_to_dicts(rows)
        except Exception:
            return []


@app.get("/api/live/ingestion-log")
def get_ingestion_log():
    """API ingestion history log."""
    with get_db() as conn:
        try:
            rows = conn.execute("""
                SELECT source, status, records_count, error_message,
                       started_at, completed_at
                FROM api_ingestion_log ORDER BY started_at DESC
            """).fetchall()
            return rows_to_dicts(rows)
        except Exception:
            return []


# Serve frontend
app.mount("/static", StaticFiles(directory=os.path.join(FRONTEND_DIR)), name="static")


@app.get("/")
def serve_dashboard():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))
