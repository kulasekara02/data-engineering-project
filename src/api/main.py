"""
DATA API - FastAPI application serving warehouse data.
Provides REST endpoints for the JavaScript dashboard.
"""

import os
import sqlite3
from contextlib import contextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = os.path.join(os.path.dirname(__file__), '..', '..')
DB_PATH = os.path.join(BASE_DIR, 'data', 'warehouse.db')
FRONTEND_DIR = os.path.join(BASE_DIR, 'frontend')

app = FastAPI(
    title="Data Engineering API",
    description="REST API for sales & analytics data warehouse",
    version="1.0.0"
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
        stats['total_revenue'] = cur.execute("SELECT ROUND(SUM(total_amount),2) FROM fact_sales").fetchone()[0]
        stats['total_profit'] = cur.execute("SELECT ROUND(SUM(profit),2) FROM fact_sales").fetchone()[0]
        stats['total_orders'] = cur.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
        stats['total_customers'] = cur.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
        stats['total_products'] = cur.execute("SELECT COUNT(*) FROM dim_products").fetchone()[0]
        stats['avg_order_value'] = cur.execute("SELECT ROUND(AVG(total_amount),2) FROM fact_sales").fetchone()[0]
        stats['total_sessions'] = cur.execute("SELECT COUNT(*) FROM fact_user_activity").fetchone()[0]
        stats['bounce_rate'] = cur.execute(
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
        rows = conn.execute("""
            SELECT p.product_name, p.category,
                   SUM(s.quantity) AS units_sold,
                   ROUND(SUM(s.total_amount),2) AS revenue,
                   ROUND(SUM(s.profit),2) AS profit
            FROM fact_sales s
            JOIN dim_products p ON s.product_id = p.product_id
            GROUP BY p.product_id
            ORDER BY revenue DESC
            LIMIT ?
        """, (limit,)).fetchall()
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


# Serve frontend
app.mount("/static", StaticFiles(directory=os.path.join(FRONTEND_DIR)), name="static")


@app.get("/")
def serve_dashboard():
    return FileResponse(os.path.join(FRONTEND_DIR, 'index.html'))
