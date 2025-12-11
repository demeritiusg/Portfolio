INSERT INTO analytics.sales_summary
SELECT customer_id, SUM(sales_amount) AS total_sales
FROM raw.sales
GROUP BY customer_id;
