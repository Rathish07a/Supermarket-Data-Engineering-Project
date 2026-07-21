-- ==========================================================
-- Project : Supermarket Data Engineering Project
-- File    : inventory_analysis.sql
-- Purpose : Business SQL queries for inventory optimization
-- Author  : Rathish
-- ==========================================================

-- ==========================================================
-- Question 7
-- Business Question:
-- Which products are out of stock or have low stock
-- (less than 50 units)?
--
-- Objective:
-- Identify products that require immediate replenishment
-- to avoid stock shortages.
-- ==========================================================

SELECT
    product_id,
    product_name,
    category,
    quantity_on_hand,
    stock_status
FROM gold_inventory_analysis
WHERE quantity_on_hand < 50
ORDER BY quantity_on_hand ASC;


-- ==========================================================
-- Question 8
-- Business Question:
-- Which products have the highest sales velocity?
--
-- Objective:
-- Identify fast-moving products to support inventory
-- planning and replenishment decisions.
-- ==========================================================

SELECT
    product_id,
    product_name,
    category,
    quantity_on_hand,
    total_quantity_sold,
    sales_velocity
FROM gold_inventory_analysis
WHERE sales_velocity = 'Fast Moving'
ORDER BY total_quantity_sold DESC;


-- ==========================================================
-- Question 9
-- Business Question:
-- Which products are slow-moving and overstocked?
--
-- Objective:
-- Identify products that have high inventory but low demand
-- so that inventory carrying costs can be reduced.
-- ==========================================================

SELECT
    product_id,
    product_name,
    category,
    quantity_on_hand,
    total_quantity_sold,
    sales_velocity,
    overstock_candidate
FROM gold_inventory_analysis
WHERE overstock_candidate = 'Yes'
ORDER BY quantity_on_hand DESC;


-- ==========================================================
-- Question 10
-- Business Question:
-- Which products need immediate replenishment?
--
-- Objective:
-- Identify products whose current stock is below the
-- calculated reorder point.
-- ==========================================================

SELECT
    product_id,
    product_name,
    category,
    quantity_on_hand,
    reorder_point,
    stock_status
FROM gold_inventory_analysis
WHERE quantity_on_hand <= reorder_point
ORDER BY quantity_on_hand ASC;


-- ==========================================================
-- Question 11
-- Business Question:
-- What is the average delivery time for each supplier?
--
-- Objective:
-- Analyze supplier delivery performance to identify
-- suppliers with the shortest and longest delivery times.
-- ==========================================================

SELECT
    supplier_id,
    supplier_name,
    average_delivery_days
FROM gold_supplier_performance
ORDER BY average_delivery_days ASC;


-- ==========================================================
-- Question 12
-- Business Question:
-- Which suppliers are the most reliable?
--
-- Objective:
-- Identify suppliers with the best on-time delivery
-- performance and highest supplier ratings.
-- ==========================================================

SELECT
    supplier_id,
    supplier_name,
    total_orders,
    on_time_deliveries,
    late_deliveries,
    on_time_delivery_percentage,
    supplier_rating,
    supplier_rank
FROM gold_supplier_performance
ORDER BY supplier_rank ASC;

