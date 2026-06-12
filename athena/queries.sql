-- Sample analytical queries against the lakehouse silver zone.
-- Workgroup: lakehouse_wg   Database: lakehouse_dwh
--
-- Run from the AWS console (Athena → Editor → pick workgroup lakehouse_wg)
-- or from the CLI:
--   aws athena start-query-execution \
--       --query-string "$(awk 'NR>=N1 && NR<=N2' athena/queries.sql)" \
--       --work-group lakehouse_wg \
--       --query-execution-context Database=lakehouse_dwh

-------------------------------------------------------------------------------
-- 1. Row counts per Delta table
-------------------------------------------------------------------------------
SELECT 'products'    AS dataset, COUNT(*) AS n FROM lakehouse_dwh.products
UNION ALL
SELECT 'orders'      AS dataset, COUNT(*) AS n FROM lakehouse_dwh.orders
UNION ALL
SELECT 'order_items' AS dataset, COUNT(*) AS n FROM lakehouse_dwh.order_items;

-------------------------------------------------------------------------------
-- 2. Daily revenue
--    Sum of total_amount across orders, grouped by order_date.
-------------------------------------------------------------------------------
SELECT
    order_date,
    COUNT(*)                              AS orders_placed,
    ROUND(SUM(total_amount), 2)           AS revenue,
    ROUND(AVG(total_amount), 2)           AS avg_order_value
FROM lakehouse_dwh.orders
GROUP BY order_date
ORDER BY order_date;

-------------------------------------------------------------------------------
-- 3. Top 10 products by order_item count
--    Join through products to surface product_name and department.
-------------------------------------------------------------------------------
SELECT
    p.product_id,
    p.product_name,
    p.department,
    COUNT(*) AS times_ordered
FROM       lakehouse_dwh.order_items oi
INNER JOIN lakehouse_dwh.products    p
        ON oi.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.department
ORDER BY times_ordered DESC
LIMIT 10;

-------------------------------------------------------------------------------
-- 4. Department-level daily revenue
--    Allocates an order's total_amount across its line items in proportion
--    to the line count, then sums per department per day. (The sample data
--    doesn't include line-item prices, so equal-weight allocation is the
--    honest approximation.)
-------------------------------------------------------------------------------
WITH lines_per_order AS (
    SELECT order_id, COUNT(*) AS n_lines
    FROM lakehouse_dwh.order_items
    GROUP BY order_id
),
line_revenue AS (
    SELECT
        oi.order_date,
        oi.product_id,
        o.total_amount / NULLIF(lpo.n_lines, 0) AS line_amount
    FROM       lakehouse_dwh.order_items oi
    INNER JOIN lakehouse_dwh.orders      o   ON oi.order_id = o.order_id
    INNER JOIN lines_per_order           lpo ON oi.order_id = lpo.order_id
)
SELECT
    lr.order_date,
    p.department,
    ROUND(SUM(lr.line_amount), 2) AS revenue
FROM       line_revenue           lr
INNER JOIN lakehouse_dwh.products p ON lr.product_id = p.product_id
GROUP BY lr.order_date, p.department
ORDER BY lr.order_date, revenue DESC;

-------------------------------------------------------------------------------
-- 5. Reorder rate per department
--    Fraction of order_items in each department where `reordered = true`.
-------------------------------------------------------------------------------
SELECT
    p.department,
    COUNT(*)                                                       AS items_sold,
    SUM(CASE WHEN oi.reordered THEN 1 ELSE 0 END)                  AS reorders,
    ROUND(
        SUM(CASE WHEN oi.reordered THEN 1 ELSE 0 END) * 1.0 / COUNT(*),
        3
    )                                                              AS reorder_rate
FROM       lakehouse_dwh.order_items oi
INNER JOIN lakehouse_dwh.products    p ON oi.product_id = p.product_id
GROUP BY p.department
ORDER BY reorder_rate DESC;
