-- ============================================
-- DATA WAREHOUSE SCHEMA (Star Schema Design)
-- ============================================

-- DIMENSION TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    country TEXT NOT NULL,
    city TEXT NOT NULL,
    age_group TEXT NOT NULL,
    signup_date TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    subcategory TEXT NOT NULL,
    unit_price REAL NOT NULL,
    cost_price REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date TEXT NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    week INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_channels (
    channel_id INTEGER PRIMARY KEY,
    channel_name TEXT NOT NULL,
    channel_type TEXT NOT NULL
);

-- FACT TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    discount REAL DEFAULT 0,
    total_amount REAL NOT NULL,
    profit REAL NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (channel_id) REFERENCES dim_channels(channel_id)
);

CREATE TABLE IF NOT EXISTS fact_user_activity (
    activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    date_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    session_duration_sec INTEGER NOT NULL,
    pages_viewed INTEGER NOT NULL,
    actions_taken INTEGER NOT NULL,
    bounce INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (channel_id) REFERENCES dim_channels(channel_id)
);

-- INDEXES FOR PERFORMANCE
-- ============================================

CREATE INDEX IF NOT EXISTS idx_sales_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_product ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_sales_date ON fact_sales(date_id);
CREATE INDEX IF NOT EXISTS idx_activity_customer ON fact_user_activity(customer_id);
CREATE INDEX IF NOT EXISTS idx_activity_date ON fact_user_activity(date_id);

-- ANALYTICS VIEWS
-- ============================================

CREATE VIEW IF NOT EXISTS v_monthly_sales AS
SELECT
    d.year,
    d.month,
    d.month_name,
    p.category,
    ch.channel_name,
    COUNT(s.sale_id) AS total_orders,
    SUM(s.quantity) AS total_units,
    ROUND(SUM(s.total_amount), 2) AS total_revenue,
    ROUND(SUM(s.profit), 2) AS total_profit,
    ROUND(AVG(s.total_amount), 2) AS avg_order_value
FROM fact_sales s
JOIN dim_date d ON s.date_id = d.date_id
JOIN dim_products p ON s.product_id = p.product_id
JOIN dim_channels ch ON s.channel_id = ch.channel_id
GROUP BY d.year, d.month, d.month_name, p.category, ch.channel_name;

CREATE VIEW IF NOT EXISTS v_customer_summary AS
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS full_name,
    c.country,
    c.age_group,
    COUNT(DISTINCT s.sale_id) AS total_orders,
    ROUND(SUM(s.total_amount), 2) AS lifetime_value,
    ROUND(AVG(a.session_duration_sec), 0) AS avg_session_sec,
    ROUND(AVG(a.pages_viewed), 1) AS avg_pages_viewed
FROM dim_customers c
LEFT JOIN fact_sales s ON c.customer_id = s.customer_id
LEFT JOIN fact_user_activity a ON c.customer_id = a.customer_id
GROUP BY c.customer_id;

CREATE VIEW IF NOT EXISTS v_product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    COUNT(s.sale_id) AS times_sold,
    SUM(s.quantity) AS total_units_sold,
    ROUND(SUM(s.total_amount), 2) AS total_revenue,
    ROUND(SUM(s.profit), 2) AS total_profit,
    ROUND(SUM(s.profit) / NULLIF(SUM(s.total_amount), 0) * 100, 1) AS profit_margin_pct
FROM dim_products p
LEFT JOIN fact_sales s ON p.product_id = s.product_id
GROUP BY p.product_id;
