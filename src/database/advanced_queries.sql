-- ============================================
-- ADVANCED ANALYTICAL QUERIES (PhD Level)
-- Window Functions, CTEs, Recursive Queries,
-- Statistical Analysis, Cohort Analysis
-- ============================================

-- 1. RUNNING TOTAL & MOVING AVERAGE (Window Functions)
-- Calculate 3-month moving average revenue with running total
WITH monthly AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        ROUND(SUM(s.total_amount), 2) AS revenue,
        ROUND(SUM(s.profit), 2) AS profit
    FROM fact_sales s
    JOIN dim_date d ON s.date_id = d.date_id
    GROUP BY d.year, d.month
)
SELECT
    year, month, month_name, revenue, profit,
    SUM(revenue) OVER (ORDER BY year, month) AS cumulative_revenue,
    ROUND(AVG(revenue) OVER (
        ORDER BY year, month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS moving_avg_3m,
    ROUND(revenue - LAG(revenue) OVER (ORDER BY year, month), 2) AS mom_change,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY year, month))
        / NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0) * 100, 1) AS mom_pct_change,
    RANK() OVER (ORDER BY revenue DESC) AS revenue_rank
FROM monthly;


-- 2. CUSTOMER COHORT ANALYSIS
-- Retention analysis by signup month cohort
WITH customer_cohort AS (
    SELECT
        c.customer_id,
        strftime('%Y-%m', c.signup_date) AS cohort_month,
        MIN(d.full_date) AS first_purchase_date
    FROM dim_customers c
    JOIN fact_sales s ON c.customer_id = s.customer_id
    JOIN dim_date d ON s.date_id = d.date_id
    GROUP BY c.customer_id
),
cohort_activity AS (
    SELECT
        cc.cohort_month,
        CAST((julianday(d.full_date) - julianday(cc.first_purchase_date)) / 30 AS INTEGER) AS months_since_first,
        COUNT(DISTINCT cc.customer_id) AS active_customers
    FROM customer_cohort cc
    JOIN fact_sales s ON cc.customer_id = s.customer_id
    JOIN dim_date d ON s.date_id = d.date_id
    GROUP BY cc.cohort_month, months_since_first
),
cohort_sizes AS (
    SELECT cohort_month, MAX(active_customers) AS cohort_size
    FROM cohort_activity
    WHERE months_since_first = 0
    GROUP BY cohort_month
)
SELECT
    ca.cohort_month,
    cs.cohort_size,
    ca.months_since_first,
    ca.active_customers,
    ROUND(ca.active_customers * 100.0 / cs.cohort_size, 1) AS retention_pct
FROM cohort_activity ca
JOIN cohort_sizes cs ON ca.cohort_month = cs.cohort_month
WHERE ca.months_since_first <= 12
ORDER BY ca.cohort_month, ca.months_since_first;


-- 3. RFM ANALYSIS (Recency, Frequency, Monetary)
-- Customer segmentation using RFM scoring
WITH rfm_raw AS (
    SELECT
        c.customer_id,
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
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
    FROM rfm_raw
)
SELECT *,
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
ORDER BY rfm_total DESC;


-- 4. MARKET BASKET ANALYSIS (Association Rules)
-- Find products frequently bought together
WITH order_pairs AS (
    SELECT
        s1.customer_id,
        s1.date_id,
        p1.product_name AS product_a,
        p2.product_name AS product_b,
        p1.category AS category_a,
        p2.category AS category_b
    FROM fact_sales s1
    JOIN fact_sales s2 ON s1.customer_id = s2.customer_id
        AND s1.date_id = s2.date_id
        AND s1.product_id < s2.product_id
    JOIN dim_products p1 ON s1.product_id = p1.product_id
    JOIN dim_products p2 ON s2.product_id = p2.product_id
),
pair_counts AS (
    SELECT
        product_a, product_b,
        category_a, category_b,
        COUNT(*) AS co_occurrence,
        COUNT(DISTINCT customer_id) AS unique_customers
    FROM order_pairs
    GROUP BY product_a, product_b
    HAVING COUNT(*) >= 3
)
SELECT *,
    ROUND(co_occurrence * 100.0 / (SELECT COUNT(DISTINCT sale_id) FROM fact_sales), 2) AS support_pct
FROM pair_counts
ORDER BY co_occurrence DESC
LIMIT 20;


-- 5. PARETO ANALYSIS (80/20 Rule)
-- Identify the top customers contributing to 80% of revenue
WITH customer_revenue AS (
    SELECT
        c.customer_id,
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
SELECT *,
    ROUND(cumulative_revenue / total_revenue * 100, 1) AS cumulative_pct,
    ROUND(rank_num * 100.0 / total_customers, 1) AS customer_percentile,
    CASE WHEN cumulative_revenue / total_revenue <= 0.8 THEN 'Top 80% Revenue' ELSE 'Bottom 20% Revenue' END AS pareto_group
FROM ranked
ORDER BY rank_num;


-- 6. TIME-SERIES DECOMPOSITION
-- Decompose sales into trend, seasonality, and residual components
WITH daily_sales AS (
    SELECT
        d.full_date,
        d.day_of_week,
        d.month,
        ROUND(SUM(s.total_amount), 2) AS daily_revenue
    FROM fact_sales s
    JOIN dim_date d ON s.date_id = d.date_id
    GROUP BY d.full_date
),
trend AS (
    SELECT full_date, daily_revenue, day_of_week, month,
        ROUND(AVG(daily_revenue) OVER (
            ORDER BY full_date
            ROWS BETWEEN 15 PRECEDING AND 15 FOLLOWING
        ), 2) AS trend_component
    FROM daily_sales
),
seasonality AS (
    SELECT
        day_of_week,
        ROUND(AVG(daily_revenue), 2) AS seasonal_component
    FROM daily_sales
    GROUP BY day_of_week
)
SELECT
    t.full_date,
    t.daily_revenue,
    t.trend_component,
    s.seasonal_component,
    ROUND(t.daily_revenue - t.trend_component - s.seasonal_component, 2) AS residual
FROM trend t
JOIN seasonality s ON t.day_of_week = s.day_of_week
ORDER BY t.full_date;


-- 7. CUSTOMER LIFETIME VALUE PREDICTION (Simple)
-- CLV estimation using historical purchase patterns
WITH customer_metrics AS (
    SELECT
        c.customer_id,
        COUNT(DISTINCT s.sale_id) AS total_purchases,
        ROUND(SUM(s.total_amount), 2) AS total_spent,
        ROUND(AVG(s.total_amount), 2) AS avg_purchase_value,
        MIN(d.full_date) AS first_purchase,
        MAX(d.full_date) AS last_purchase,
        julianday(MAX(d.full_date)) - julianday(MIN(d.full_date)) AS customer_lifespan_days,
        COUNT(DISTINCT strftime('%Y-%m', d.full_date)) AS active_months
    FROM dim_customers c
    JOIN fact_sales s ON c.customer_id = s.customer_id
    JOIN dim_date d ON s.date_id = d.date_id
    GROUP BY c.customer_id
)
SELECT
    customer_id,
    total_purchases,
    total_spent,
    avg_purchase_value,
    active_months,
    ROUND(customer_lifespan_days, 0) AS lifespan_days,
    ROUND(total_purchases * 1.0 / NULLIF(active_months, 0), 2) AS purchase_frequency_per_month,
    -- Simple CLV = Avg Purchase Value * Purchase Frequency * Estimated Lifespan (24 months)
    ROUND(avg_purchase_value * (total_purchases * 1.0 / NULLIF(active_months, 0)) * 24, 2) AS predicted_clv_24m
FROM customer_metrics
WHERE total_purchases > 1
ORDER BY predicted_clv_24m DESC;


-- 8. CHANNEL ATTRIBUTION (Last-Touch)
-- Attribution analysis showing channel effectiveness
WITH channel_stats AS (
    SELECT
        ch.channel_name,
        ch.channel_type,
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
    ROUND(attributed_profit / NULLIF(attributed_revenue, 0) * 100, 1) AS profit_margin_pct,
    RANK() OVER (ORDER BY attributed_revenue DESC) AS revenue_rank,
    RANK() OVER (ORDER BY attributed_profit / NULLIF(attributed_revenue, 0) DESC) AS efficiency_rank
FROM channel_stats
ORDER BY attributed_revenue DESC;
