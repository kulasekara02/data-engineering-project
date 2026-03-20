-- ============================================
-- ANALYTICAL QUERIES FOR DATA ENGINEERING
-- ============================================

-- 1. Top 10 Products by Revenue
SELECT
    p.product_name,
    p.category,
    SUM(s.quantity) AS units_sold,
    ROUND(SUM(s.total_amount), 2) AS revenue,
    ROUND(SUM(s.profit), 2) AS profit
FROM fact_sales s
JOIN dim_products p ON s.product_id = p.product_id
GROUP BY p.product_id
ORDER BY revenue DESC
LIMIT 10;

-- 2. Monthly Revenue Trend
SELECT
    d.year,
    d.month_name,
    ROUND(SUM(s.total_amount), 2) AS revenue,
    ROUND(SUM(s.profit), 2) AS profit,
    COUNT(DISTINCT s.customer_id) AS unique_customers
FROM fact_sales s
JOIN dim_date d ON s.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 3. Sales by Channel
SELECT
    ch.channel_name,
    ch.channel_type,
    COUNT(s.sale_id) AS orders,
    ROUND(SUM(s.total_amount), 2) AS revenue,
    ROUND(AVG(s.total_amount), 2) AS avg_order_value
FROM fact_sales s
JOIN dim_channels ch ON s.channel_id = ch.channel_id
GROUP BY ch.channel_id
ORDER BY revenue DESC;

-- 4. Customer Segmentation by Lifetime Value
SELECT
    CASE
        WHEN lifetime_value >= 5000 THEN 'Platinum'
        WHEN lifetime_value >= 2000 THEN 'Gold'
        WHEN lifetime_value >= 500 THEN 'Silver'
        ELSE 'Bronze'
    END AS segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(lifetime_value), 2) AS avg_ltv,
    ROUND(AVG(total_orders), 1) AS avg_orders
FROM v_customer_summary
GROUP BY segment
ORDER BY avg_ltv DESC;

-- 5. Revenue by Country (Top 10)
SELECT
    c.country,
    COUNT(DISTINCT c.customer_id) AS customers,
    COUNT(s.sale_id) AS orders,
    ROUND(SUM(s.total_amount), 2) AS revenue
FROM fact_sales s
JOIN dim_customers c ON s.customer_id = c.customer_id
GROUP BY c.country
ORDER BY revenue DESC
LIMIT 10;

-- 6. Day-of-Week Sales Pattern
SELECT
    d.day_name,
    d.day_of_week,
    COUNT(s.sale_id) AS orders,
    ROUND(SUM(s.total_amount), 2) AS revenue,
    ROUND(AVG(s.total_amount), 2) AS avg_order
FROM fact_sales s
JOIN dim_date d ON s.date_id = d.date_id
GROUP BY d.day_of_week
ORDER BY d.day_of_week;

-- 7. Category Performance with Profit Margin
SELECT
    p.category,
    COUNT(s.sale_id) AS orders,
    ROUND(SUM(s.total_amount), 2) AS revenue,
    ROUND(SUM(s.profit), 2) AS profit,
    ROUND(SUM(s.profit) / SUM(s.total_amount) * 100, 1) AS margin_pct
FROM fact_sales s
JOIN dim_products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC;

-- 8. User Engagement Metrics by Channel
SELECT
    ch.channel_name,
    COUNT(a.activity_id) AS sessions,
    ROUND(AVG(a.session_duration_sec), 0) AS avg_duration_sec,
    ROUND(AVG(a.pages_viewed), 1) AS avg_pages,
    ROUND(SUM(a.bounce) * 100.0 / COUNT(a.activity_id), 1) AS bounce_rate_pct
FROM fact_user_activity a
JOIN dim_channels ch ON a.channel_id = ch.channel_id
GROUP BY ch.channel_id
ORDER BY sessions DESC;
