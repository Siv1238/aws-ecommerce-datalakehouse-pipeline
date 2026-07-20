--------------------------------------------------------------------------------
-- QUERY 1: WINDOW FUNCTIONS QUERY
-- Top 3 products per category based on total sales using Row Number, Rank, and Dense Rank
--------------------------------------------------------------------------------
SELECT 
    category,
    product_name,
    total_sales,
    row_num,
    rank_val,
    dense_rank_val
FROM ecommerce_db.processed
WHERE row_num <= 3
ORDER BY category, row_num;


--------------------------------------------------------------------------------
-- QUERY 2: CATEGORY-WISE SALES AGGREGATION QUERY
-- Summary report showing total unique customers, total revenue, and average order value per category
--------------------------------------------------------------------------------
SELECT 
    category,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(total_sales) AS category_revenue,
    AVG(total_sales) AS avg_order_value
FROM ecommerce_db.processed
GROUP BY category
ORDER BY SUM(total_sales) DESC;


--------------------------------------------------------------------------------
-- QUERY 3: OVERALL DATA LAKE VALIDATION QUERY
-- Safely inspecting all columns and first 20 records from the processed Parquet table
--------------------------------------------------------------------------------
SELECT * 
FROM ecommerce_db.processed 
LIMIT 20;