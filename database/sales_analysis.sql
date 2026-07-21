-- ==========================================================
-- Project : Supermarket Data Engineering Project
-- File    : sales_analysis.sql
-- Purpose : Business SQL queries for sales performance analysis
-- Author  : Rathish
-- ==========================================================

-- ==========================================================
-- Question 1
-- Which store has the highest total revenue in 2024?
-- ==========================================================

SELECT 
        store_id,
        store_name,
        location,
        total_revenue 
from gold_sales_performance
ORDER BY total_revenue DESC
LIMIT 1;


-- ==========================================================
-- Question 2
-- Business Question:
-- What are the Top 5 best-selling products by quantity sold?
--
-- Objective:
-- Identify the top 5 products with the highest total quantity sold
-- to understand customer demand and product popularity.
-- ==========================================================

SELECT Product_id,
       Product_name,
       total_quantity_sold
FROM gold_product_performance
WHERE top_5_best_seller = 'Yes'
ORDER BY total_quantity_sold DESC;


-- ==========================================================
-- Question 3
-- Business Question:
-- Rank all stores based on total revenue and transaction count.
--
-- Objective:
-- Evaluate store performance by ranking stores according to
-- their revenue and number of transactions.
-- ==========================================================
SELECT
    store_id,
    store_name,
    total_revenue,
    total_transactions,
    DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY total_transactions DESC) AS transaction_rank
FROM gold_sales_performance
ORDER BY revenue_rank;


-- ==========================================================
-- Question 4
-- Business Question:
-- Which is the most profitable product in each store?
--
-- Objective:
-- Identify the highest revenue-generating product for every
-- store to understand product profitability at each location.
-- ==========================================================

WITH product_profit AS (
    SELECT
        store_id,
        store_name,
        product_id,
        product_name,
        total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY total_revenue DESC
        ) AS rn
    FROM gold_product_performance
)
SELECT
    store_id,
    store_name,
    product_id,
    product_name,
    total_revenue
FROM product_profit
WHERE rn = 1
ORDER BY total_revenue DESC;



-- ==========================================================
-- Question 5
-- Business Question:
-- What is the month-on-month sales trend for each store?
--
-- Objective:
-- Analyze monthly sales trends for each store to identify
-- seasonal patterns and revenue growth.
-- ==========================================================

SELECT
    store_name,
    sales_year,
    sales_month,
    total_revenue
FROM gold_monthly_sales_performance
ORDER BY
    store_name,
    sales_year,
    sales_month;


-- ==========================================================
-- Question 6
-- Business Question:
-- Which store has the most diverse product sales?
--
-- Objective:
-- Identify the store that sold the highest number of
-- unique products.
-- ==========================================================

SELECT
    store_id,
    store_name,
    location,
    distinct_products_sold
FROM gold_sales_performance
ORDER BY distinct_products_sold DESC
LIMIT 1;