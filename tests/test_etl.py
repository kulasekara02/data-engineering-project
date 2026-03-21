"""Tests for ETL pipeline components."""

import os
import sqlite3
import pytest

BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
DB_PATH = os.path.join(BASE_DIR, 'data', 'warehouse.db')


@pytest.fixture
def db_conn():
    """Database connection fixture."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


class TestDataGeneration:
    def test_raw_files_exist(self):
        raw_dir = os.path.join(BASE_DIR, 'data', 'raw')
        expected = ['dim_customers.csv', 'dim_products.csv', 'dim_dates.csv',
                    'dim_channels.csv', 'fact_sales.csv', 'fact_user_activity.csv']
        for f in expected:
            assert os.path.exists(os.path.join(raw_dir, f)), f"Missing: {f}"

    def test_database_exists(self):
        assert os.path.exists(DB_PATH), "Warehouse database not found"


class TestWarehouseSchema:
    def test_dimension_tables_exist(self, db_conn):
        tables = ['dim_customers', 'dim_products', 'dim_date', 'dim_channels']
        for table in tables:
            count = db_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert count > 0, f"Table {table} is empty"

    def test_fact_tables_exist(self, db_conn):
        tables = ['fact_sales', 'fact_user_activity']
        for table in tables:
            count = db_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert count > 0, f"Table {table} is empty"

    def test_customer_count(self, db_conn):
        count = db_conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
        assert count == 500

    def test_product_count(self, db_conn):
        count = db_conn.execute("SELECT COUNT(*) FROM dim_products").fetchone()[0]
        assert count == 50

    def test_sales_count(self, db_conn):
        count = db_conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
        assert count == 5000

    def test_activity_count(self, db_conn):
        count = db_conn.execute("SELECT COUNT(*) FROM fact_user_activity").fetchone()[0]
        assert count == 8000


class TestDataIntegrity:
    def test_no_orphan_sales(self, db_conn):
        orphans = db_conn.execute("""
            SELECT COUNT(*) FROM fact_sales s
            LEFT JOIN dim_customers c ON s.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """).fetchone()[0]
        assert orphans == 0, f"Found {orphans} orphan sales records"

    def test_no_negative_amounts(self, db_conn):
        negatives = db_conn.execute(
            "SELECT COUNT(*) FROM fact_sales WHERE total_amount < 0"
        ).fetchone()[0]
        assert negatives == 0, f"Found {negatives} negative sale amounts"

    def test_positive_quantities(self, db_conn):
        zeros = db_conn.execute(
            "SELECT COUNT(*) FROM fact_sales WHERE quantity <= 0"
        ).fetchone()[0]
        assert zeros == 0

    def test_valid_emails(self, db_conn):
        invalid = db_conn.execute(
            "SELECT COUNT(*) FROM dim_customers WHERE email NOT LIKE '%@%'"
        ).fetchone()[0]
        assert invalid == 0

    def test_unique_emails(self, db_conn):
        total = db_conn.execute("SELECT COUNT(email) FROM dim_customers").fetchone()[0]
        distinct = db_conn.execute("SELECT COUNT(DISTINCT email) FROM dim_customers").fetchone()[0]
        assert total == distinct, "Duplicate emails found"

    def test_revenue_is_positive(self, db_conn):
        revenue = db_conn.execute("SELECT SUM(total_amount) FROM fact_sales").fetchone()[0]
        assert revenue > 0

    def test_all_dates_covered(self, db_conn):
        date_count = db_conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
        assert date_count == 731  # 2 years of dates
