-- ============================================================
-- FILE: create_tables.sql
-- Redshift DDL for Real-Time Sales Pipeline
-- Run this in Redshift Query Editor before starting the pipeline
-- ============================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS sales;

-- Main transactions table
CREATE TABLE IF NOT EXISTS sales.sales_transactions (
    transaction_id   VARCHAR(50)     NOT NULL,
    store_id         VARCHAR(20)     NOT NULL,
    product_id       VARCHAR(20)     NOT NULL,
    product_name     VARCHAR(200)    NOT NULL,
    category         VARCHAR(100),
    quantity         INTEGER         NOT NULL,
    unit_price       DECIMAL(10, 2)  NOT NULL,
    total_amount     DECIMAL(12, 2)  NOT NULL,
    discount_amount  DECIMAL(10, 2)  DEFAULT 0.00,
    payment_method   VARCHAR(30),
    customer_id      VARCHAR(50)     DEFAULT 'GUEST',
    region           VARCHAR(50),
    event_timestamp  TIMESTAMP       NOT NULL,
    ingested_at      TIMESTAMP       DEFAULT GETDATE(),

    PRIMARY KEY (transaction_id)
)
DISTKEY(store_id)
SORTKEY(event_timestamp);


-- ============================================================
-- FILE: queries.sql
-- Sample Analytical Queries for QuickSight / Reporting
-- ============================================================

-- 1. Total revenue by hour (last 24 hours)
SELECT
    DATE_TRUNC('hour', event_timestamp)  AS sale_hour,
    COUNT(*)                              AS total_transactions,
    SUM(total_amount)                     AS total_revenue,
    AVG(total_amount)                     AS avg_order_value
FROM sales.sales_transactions
WHERE event_timestamp >= DATEADD(hour, -24, GETDATE())
GROUP BY 1
ORDER BY 1;


-- 2. Top 10 best-selling products today
SELECT
    product_name,
    category,
    SUM(quantity)      AS units_sold,
    SUM(total_amount)  AS revenue
FROM sales.sales_transactions
WHERE DATE(event_timestamp) = CURRENT_DATE
GROUP BY product_name, category
ORDER BY units_sold DESC
LIMIT 10;


-- 3. Revenue by region
SELECT
    region,
    COUNT(DISTINCT store_id)  AS store_count,
    SUM(total_amount)         AS total_revenue
FROM sales.sales_transactions
WHERE event_timestamp >= DATEADD(day, -7, GETDATE())
GROUP BY region
ORDER BY total_revenue DESC;


-- 4. Payment method breakdown
SELECT
    payment_method,
    COUNT(*)            AS transaction_count,
    SUM(total_amount)   AS total_revenue,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2
    )                   AS pct_of_total
FROM sales.sales_transactions
WHERE DATE(event_timestamp) = CURRENT_DATE
GROUP BY payment_method
ORDER BY transaction_count DESC;


-- 5. Hourly transaction volume (real-time monitoring)
SELECT
    DATE_TRUNC('minute', event_timestamp)  AS minute_bucket,
    COUNT(*)                                AS transactions,
    SUM(total_amount)                       AS revenue
FROM sales.sales_transactions
WHERE event_timestamp >= DATEADD(hour, -1, GETDATE())
GROUP BY 1
ORDER BY 1;
