"""Tests for FastAPI endpoints."""

import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from fastapi.testclient import TestClient
from src.api.main import app

client = TestClient(app)


class TestOverviewAPI:
    def test_overview_returns_200(self):
        response = client.get("/api/overview")
        assert response.status_code == 200

    def test_overview_has_required_fields(self):
        data = client.get("/api/overview").json()
        required = ['total_revenue', 'total_profit', 'total_orders',
                    'total_customers', 'total_products', 'avg_order_value']
        for field in required:
            assert field in data, f"Missing field: {field}"

    def test_overview_values_positive(self):
        data = client.get("/api/overview").json()
        assert data['total_revenue'] > 0
        assert data['total_orders'] > 0
        assert data['total_customers'] > 0


class TestSalesAPI:
    def test_monthly_sales(self):
        response = client.get("/api/sales/monthly")
        assert response.status_code == 200
        data = response.json()
        assert len(data) > 0
        assert 'revenue' in data[0]

    def test_sales_by_category(self):
        response = client.get("/api/sales/by-category")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 5  # 5 categories

    def test_sales_by_channel(self):
        response = client.get("/api/sales/by-channel")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 6  # 6 channels

    def test_sales_by_country(self):
        response = client.get("/api/sales/by-country")
        assert response.status_code == 200
        assert len(response.json()) > 0

    def test_daily_pattern(self):
        response = client.get("/api/sales/daily-pattern")
        assert response.status_code == 200
        assert len(response.json()) == 7  # 7 days


class TestProductsAPI:
    def test_top_products_default(self):
        response = client.get("/api/products/top")
        assert response.status_code == 200
        assert len(response.json()) == 10

    def test_top_products_custom_limit(self):
        response = client.get("/api/products/top?limit=5")
        assert response.status_code == 200
        assert len(response.json()) == 5


class TestAdvancedAPI:
    def test_monthly_trend_advanced(self):
        response = client.get("/api/advanced/monthly-trend")
        assert response.status_code == 200
        data = response.json()
        assert 'cumulative_revenue' in data[0]
        assert 'moving_avg_3m' in data[0]

    def test_rfm_analysis(self):
        response = client.get("/api/advanced/rfm-summary")
        assert response.status_code == 200
        data = response.json()
        assert len(data) > 0
        assert 'rfm_segment' in data[0]

    def test_pareto_analysis(self):
        response = client.get("/api/advanced/pareto")
        assert response.status_code == 200
        data = response.json()
        assert len(data) > 0
        assert 'cumulative_pct' in data[0]

    def test_customer_segments(self):
        response = client.get("/api/customers/segments")
        assert response.status_code == 200
        assert len(response.json()) > 0

    def test_engagement(self):
        response = client.get("/api/activity/engagement")
        assert response.status_code == 200


class TestDashboard:
    def test_dashboard_loads(self):
        response = client.get("/")
        assert response.status_code == 200
        assert b"Data Engineering Platform" in response.content
